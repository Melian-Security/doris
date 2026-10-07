// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format/arrow/arrow_stream_reader.h"

#include <algorithm>

#include "arrow/io/buffered.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "exec/common/arrow_column_to_doris_column.h"
#include "format/arrow/arrow_pip_input_stream.h"
#include "io/fs/stream_load_pipe.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeProfile;
} // namespace doris

namespace doris {

ArrowStreamReader::ArrowStreamReader(RuntimeState* state, RuntimeProfile* profile,
                                     ScannerCounter* counter, const TFileScanRangeParams& params,
                                     const TFileRangeDesc& range,
                                     const std::vector<SlotDescriptor*>& file_slot_descs,
                                     io::IOContext* io_ctx)
        : _state(state),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _io_ctx(io_ctx),
          _file_reader(nullptr) {
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctzz);
}

ArrowStreamReader::~ArrowStreamReader() = default;

Status ArrowStreamReader::init_reader() {
    io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(FileFactory::create_pipe_reader(_range.load_id, &file_reader, _state, false));
    return init_reader(std::move(file_reader));
}

Status ArrowStreamReader::init_reader(io::FileReaderSPtr file_reader) {
    _file_reader = _io_ctx && _io_ctx->file_reader_stats
                           ? std::make_shared<io::TracingFileReader>(std::move(file_reader),
                                                                     _io_ctx->file_reader_stats)
                           : file_reader;
    _pip_stream = ArrowPipInputStream::create_unique(_file_reader);
    return Status::OK();
}

Status ArrowStreamReader::_open_stream(bool* opened) {
    bool has_next = false;
    RETURN_IF_ERROR(_pip_stream->HasNext(&has_next));
    if (!has_next) {
        *opened = false;
        return Status::OK();
    }
    auto res_open = arrow::ipc::RecordBatchStreamReader::Open(
            _pip_stream.get(), arrow::ipc::IpcReadOptions::Defaults());
    if (!res_open.ok()) {
        LOG(WARNING) << "failed to open stream reader: " << res_open.status().message();
        return Status::InternalError("failed to open stream reader: {}",
                                     res_open.status().message());
    }
    _batch_reader = std::move(res_open).ValueUnsafe();
    *opened = true;
    return Status::OK();
}

Status ArrowStreamReader::_convert_batch(const arrow::RecordBatch& batch, int64_t start,
                                         int64_t end, Block* block) {
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    const int num_columns = batch.num_columns();
    for (int c = 0; c < num_columns; ++c) {
        const arrow::Array* column = batch.column(c).get();
        const std::string& column_name = batch.schema()->field(c)->name();
        try {
            const auto& column_name_in_block = columns_guard.get_name_by_position(c);
            if (column_name_in_block != column_name) {
                return Status::InternalError("Column name mismatch: expected {}, got {}",
                                             column_name_in_block, column_name);
            }
            RETURN_IF_ERROR(
                    columns_guard.get_datatype_by_position(c)->get_serde()->read_column_from_arrow(
                            *columns[c], column, start, end, _ctzz));
        } catch (Exception& e) {
            return Status::InternalError("Failed to convert from arrow to block: {}", e.what());
        }
    }
    return Status::OK();
}

Status ArrowStreamReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (!config::enable_arrow_stream_load_streaming_read) {
        return _read_whole_stream(block, read_rows, eof);
    }
    *read_rows = 0;
    const int64_t block_rows = config::arrow_stream_load_block_rows > 0
                                       ? config::arrow_stream_load_block_rows
                                       : std::max(_state->batch_size(), 1);
    while (static_cast<int64_t>(*read_rows) < block_rows) {
        if (_batch == nullptr) {
            if (_batch_reader == nullptr) {
                bool opened = false;
                RETURN_IF_ERROR(_open_stream(&opened));
                if (!opened) {
                    break;
                }
            }
            std::shared_ptr<arrow::RecordBatch> batch;
            auto st = _batch_reader->ReadNext(&batch);
            if (!st.ok()) {
                LOG(WARNING) << "failed to read batch: " << st.message();
                return Status::InternalError("failed to read batch: {}", st.message());
            }
            if (batch == nullptr) {
                // End of this IPC stream; the body may carry another one.
                _batch_reader.reset();
                continue;
            }
            if (batch->num_rows() == 0) {
                continue;
            }
            _batch = std::move(batch);
            _batch_offset = 0;
        }
        const int64_t end = std::min(_batch->num_rows(),
                                     _batch_offset + block_rows - static_cast<int64_t>(*read_rows));
        RETURN_IF_ERROR(_convert_batch(*_batch, _batch_offset, end, block));
        *read_rows += end - _batch_offset;
        _batch_offset = end;
        if (_batch_offset == _batch->num_rows()) {
            // Drop the decoded Arrow buffers before the next batch is decompressed.
            _batch.reset();
        }
    }
    *eof = (*read_rows == 0);
    return Status::OK();
}

Status ArrowStreamReader::_read_whole_stream(Block* block, size_t* read_rows, bool* eof) {
    bool opened = false;
    RETURN_IF_ERROR(_open_stream(&opened));
    if (!opened) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    auto reader = std::move(_batch_reader);

    // get arrow data from reader
    arrow::Result<arrow::RecordBatchVector> res_reader = reader->ToRecordBatches();
    if (!res_reader.ok()) {
        LOG(WARNING) << "failed to read batch: " << res_reader.status().message();
        return Status::InternalError("failed to read batch: {}", res_reader.status().message());
    }
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches =
            std::move(res_reader).ValueUnsafe();

    // convert arrow batch to block
    for (const auto& batch : out_batches) {
        RETURN_IF_ERROR(_convert_batch(*batch, 0, batch->num_rows(), block));
        *read_rows += batch->num_rows();
    }

    *eof = (*read_rows == 0);
    return Status::OK();
}

Status ArrowStreamReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                                      std::unordered_set<std::string>* missing_cols) {
    for (const auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), load_source_type(slot->type()));
    }
    return Status::OK();
}

// A legacy Variant column arrives as one JSON document per row. The reader keeps it as text and
// the scanner's CAST(varchar AS variant) wraps the text as the root of a scalar variant, as it
// does for JSON and CSV loads. The tablet sink then redistributes a plain string column, and the
// segment writer parses each document once per tablet with the column's storage parse settings.
// Parsing in the reader instead builds up to variant_max_subcolumns_count subcolumns per block,
// which the sink copies to each tablet row by row and the segment writer cannot re-stage.
DataTypePtr ArrowStreamReader::load_source_type(const DataTypePtr& slot_type) {
    if (typeid_cast<const DataTypeVariant*>(remove_nullable(slot_type).get()) != nullptr) {
        return std::make_shared<DataTypeString>();
    }
    return slot_type;
}

#include "common/compile_check_end.h"
} // namespace doris
