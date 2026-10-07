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

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cctz/time_zone.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "format/arrow/arrow_pip_input_stream.h"
#include "format/file_reader/new_plain_text_line_reader.h"
#include "format/generic_reader.h"
#include "io/file_factory.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace arrow {
class RecordBatch;
namespace ipc {
class RecordBatchStreamReader;
} // namespace ipc
} // namespace arrow

namespace doris {
namespace io {
class FileSystem;
struct IOContext;
} // namespace io

#include "common/compile_check_begin.h"

struct ScannerCounter;
class Block;

class ArrowStreamReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(ArrowStreamReader);

public:
    ArrowStreamReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                      const TFileScanRangeParams& params, const TFileRangeDesc& range,
                      const std::vector<SlotDescriptor*>& file_slot_descs, io::IOContext* io_ctx);

    ~ArrowStreamReader() override;

    Status init_reader();
    // Reads the stream from file_reader instead of the load's stream load pipe.
    Status init_reader(io::FileReaderSPtr file_reader);

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    // The type the reader reads a load column's Arrow data into; the scanner then casts it to
    // the slot type.
    static DataTypePtr load_source_type(const DataTypePtr& slot_type);

private:
    Status _open_stream(bool* opened);
    Status _convert_batch(const arrow::RecordBatch& batch, int64_t start, int64_t end,
                          Block* block);
    Status _read_whole_stream(Block* block, size_t* read_rows, bool* eof);

    RuntimeState* _state;
    const TFileRangeDesc& _range;
    const std::vector<SlotDescriptor*>& _file_slot_descs;
    io::IOContext* _io_ctx;
    io::FileReaderSPtr _file_reader;
    std::unique_ptr<doris::ArrowPipInputStream> _pip_stream;
    // Open IPC stream and the record batch being converted, kept across get_next_block calls.
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> _batch_reader;
    std::shared_ptr<arrow::RecordBatch> _batch;
    int64_t _batch_offset = 0;
    cctz::time_zone _ctzz;
};
#include "common/compile_check_end.h"
} // namespace doris
