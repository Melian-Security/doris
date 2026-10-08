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

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/util/compression.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format/arrow/arrow_stream_reader.h"
#include "io/fs/stream_load_pipe.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {

std::shared_ptr<arrow::Schema> test_schema() {
    return arrow::schema({arrow::field("id", arrow::int32()), arrow::field("doc", arrow::utf8())});
}

std::shared_ptr<arrow::RecordBatch> make_batch(int32_t first, int32_t rows) {
    arrow::Int32Builder ids;
    arrow::StringBuilder docs;
    for (int32_t i = first; i < first + rows; ++i) {
        EXPECT_TRUE(ids.Append(i).ok());
        EXPECT_TRUE(
                (i % 7 == 0 ? docs.AppendNull() : docs.Append("{\"i\":" + std::to_string(i) + "}"))
                        .ok());
    }
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> doc_array;
    EXPECT_TRUE(ids.Finish(&id_array).ok());
    EXPECT_TRUE(docs.Finish(&doc_array).ok());
    return arrow::RecordBatch::Make(test_schema(), rows, {id_array, doc_array});
}

// One IPC stream of ZSTD-compressed batches with the given row counts.
std::shared_ptr<io::StreamLoadPipe> make_pipe(const std::vector<int32_t>& batch_rows) {
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto options = arrow::ipc::IpcWriteOptions::Defaults();
    options.codec = arrow::util::Codec::Create(arrow::Compression::ZSTD).ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(sink, test_schema(), options).ValueOrDie();
    int32_t first = 0;
    for (int32_t rows : batch_rows) {
        EXPECT_TRUE(writer->WriteRecordBatch(*make_batch(first, rows)).ok());
        first += rows;
    }
    EXPECT_TRUE(writer->Close().ok());
    auto body = sink->Finish().ValueOrDie();
    auto pipe = std::make_shared<io::StreamLoadPipe>(body->size() + 1024, 64 * 1024, body->size());
    EXPECT_TRUE(pipe->append(reinterpret_cast<const char*>(body->data()), body->size()).ok());
    EXPECT_TRUE(pipe->finish().ok());
    return pipe;
}

Block make_block() {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "id"});
    auto doc_type = make_nullable(std::make_shared<DataTypeString>());
    block.insert({doc_type->create_column(), doc_type, "doc"});
    return block;
}

class ArrowStreamReaderStreamingTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_streaming = config::enable_arrow_stream_load_streaming_read;
        _saved_rows = config::arrow_stream_load_block_rows;
        _saved_inline = config::arrow_stream_load_decompress_inline;
    }
    void TearDown() override {
        config::enable_arrow_stream_load_streaming_read = _saved_streaming;
        config::arrow_stream_load_block_rows = _saved_rows;
        config::arrow_stream_load_decompress_inline = _saved_inline;
    }

    // Reads the whole pipe and returns the row count of each returned block. Every row is
    // checked against what make_batch wrote, in order.
    std::vector<size_t> read_all(const std::shared_ptr<io::StreamLoadPipe>& pipe) {
        RuntimeState state;
        TFileScanRangeParams params;
        TFileRangeDesc range;
        std::vector<SlotDescriptor*> slots;
        ArrowStreamReader reader(&state, nullptr, nullptr, params, range, slots, nullptr);
        EXPECT_TRUE(reader.init_reader(pipe).ok());
        std::vector<size_t> block_rows;
        int32_t next = 0;
        while (true) {
            Block block = make_block();
            size_t read_rows = 0;
            bool eof = false;
            Status st = reader.get_next_block(&block, &read_rows, &eof);
            EXPECT_TRUE(st.ok()) << st.to_string();
            if (!st.ok() || eof) {
                break;
            }
            EXPECT_EQ(block.rows(), read_rows);
            const auto& ids = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
            const auto& docs = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            for (size_t r = 0; r < read_rows; ++r, ++next) {
                EXPECT_EQ(ids.get_element(r), next);
                EXPECT_EQ(docs.is_null_at(r), next % 7 == 0);
                if (next % 7 != 0) {
                    EXPECT_EQ(docs.get_nested_column().get_data_at(r).to_string(),
                              "{\"i\":" + std::to_string(next) + "}");
                }
            }
            block_rows.push_back(read_rows);
        }
        _total_rows = next;
        return block_rows;
    }

    bool _saved_streaming = true;
    int32_t _saved_rows = 0;
    bool _saved_inline = true;
    int32_t _total_rows = 0;
};

} // namespace

TEST_F(ArrowStreamReaderStreamingTest, SplitsOneLargeBatchIntoBlocks) {
    config::enable_arrow_stream_load_streaming_read = true;
    config::arrow_stream_load_block_rows = 1000;
    auto blocks = read_all(make_pipe({4500}));
    EXPECT_EQ(_total_rows, 4500);
    EXPECT_EQ(blocks, (std::vector<size_t> {1000, 1000, 1000, 1000, 500}));
}

TEST_F(ArrowStreamReaderStreamingTest, FillsBlocksAcrossSmallBatches) {
    config::enable_arrow_stream_load_streaming_read = true;
    config::arrow_stream_load_block_rows = 1000;
    auto blocks = read_all(make_pipe({300, 300, 0, 300, 300, 50}));
    EXPECT_EQ(_total_rows, 1250);
    EXPECT_EQ(blocks, (std::vector<size_t> {1000, 250}));
}

TEST_F(ArrowStreamReaderStreamingTest, UsesBatchSizeWhenRowsUnset) {
    config::enable_arrow_stream_load_streaming_read = true;
    config::arrow_stream_load_block_rows = 0;
    RuntimeState state;
    const auto batch_size = static_cast<size_t>(state.batch_size());
    auto blocks = read_all(make_pipe({static_cast<int32_t>(batch_size * 2 + 3)}));
    EXPECT_EQ(blocks, (std::vector<size_t> {batch_size, batch_size, 3}));
}

TEST_F(ArrowStreamReaderStreamingTest, LegacyModeReturnsWholeStream) {
    config::enable_arrow_stream_load_streaming_read = false;
    auto blocks = read_all(make_pipe({300, 700, 2000}));
    EXPECT_EQ(_total_rows, 3000);
    EXPECT_EQ(blocks, (std::vector<size_t> {3000}));
}

TEST_F(ArrowStreamReaderStreamingTest, EmptyStreamIsEof) {
    config::enable_arrow_stream_load_streaming_read = true;
    auto blocks = read_all(make_pipe({}));
    EXPECT_TRUE(blocks.empty());
    EXPECT_EQ(_total_rows, 0);
}

TEST_F(ArrowStreamReaderStreamingTest, InlineAndPooledDecompressionReadTheSameRows) {
    config::enable_arrow_stream_load_streaming_read = true;
    config::arrow_stream_load_block_rows = 1000;
    for (bool decompress_inline : {true, false}) {
        config::arrow_stream_load_decompress_inline = decompress_inline;
        auto blocks = read_all(make_pipe({300, 2500, 0, 700}));
        EXPECT_EQ(_total_rows, 3500) << "inline=" << decompress_inline;
        EXPECT_EQ(blocks, (std::vector<size_t> {1000, 1000, 1000, 500}))
                << "inline=" << decompress_inline;
    }
}

} // namespace doris
