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

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/olap_define.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/segment_creator.h"
#include "storage/segment/segment.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema.h"
#include "util/block_compression.h"
#include "util/faststring.h"

namespace doris {

static const std::string kLoadCompressionDir = "./ut_dir/load_segment_compression_test";

// Load segments may use a cheaper page codec than the table's, while compaction output keeps the
// table codec; readers take the codec from each column's meta.
class LoadSegmentCompressionTest : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kLoadCompressionDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kLoadCompressionDir).ok());
        auto engine = std::make_unique<StorageEngine>(EngineOptions {});
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _saved_threshold = config::segment_compression_threshold_kb;
        _saved_type = config::load_segment_compression_type;
        _saved_level = config::zstd_compression_level;
        _saved_load_level = config::load_segment_zstd_compression_level;
        // Compress every segment, however small the test block is.
        config::segment_compression_threshold_kb = 0;
    }

    void TearDown() override {
        config::segment_compression_threshold_kb = _saved_threshold;
        config::load_segment_compression_type = _saved_type;
        config::zstd_compression_level = _saved_level;
        config::load_segment_zstd_compression_level = _saved_load_level;
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kLoadCompressionDir).ok());
    }

    static TabletSchemaSPtr create_schema() {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(1024);
        schema_pb.set_compression_type(segment_v2::ZSTD);
        schema_pb.set_next_column_unique_id(3);
        ColumnPB* key = schema_pb.add_column();
        key->set_unique_id(1);
        key->set_name("k1");
        key->set_type("BIGINT");
        key->set_is_key(true);
        key->set_length(8);
        key->set_index_length(8);
        key->set_is_nullable(false);
        ColumnPB* value = schema_pb.add_column();
        value->set_unique_id(2);
        value->set_name("v1");
        value->set_type("VARCHAR");
        value->set_is_key(false);
        value->set_length(65533);
        value->set_index_length(16);
        value->set_is_nullable(true);
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);
        return schema;
    }

    static std::string value_at(int64_t row) {
        return "user-" + std::to_string(row % 97) + "@host-" + std::to_string(row % 13) +
               ".example";
    }

    static Block make_block(const TabletSchemaSPtr& schema, int64_t first_row, int64_t rows) {
        Block block = schema->create_block();
        auto columns = std::move(block).mutate_columns();
        for (int64_t row = first_row; row < first_row + rows; ++row) {
            columns[0]->insert_data(reinterpret_cast<const char*>(&row), sizeof(row));
            std::string value = value_at(row);
            columns[1]->insert_data(value.data(), value.size());
        }
        block.set_columns(std::move(columns));
        return block;
    }

    RowsetSharedPtr write_rowset(const TabletSchemaSPtr& schema, DataWriteType write_type,
                                 int64_t version, int64_t first_row, int64_t rows) {
        RowsetWriterContext context;
        context.rowset_id = _engine->next_rowset_id();
        context.rowset_type = BETA_ROWSET;
        context.rowset_state = VISIBLE;
        context.tablet_schema = schema;
        context.tablet_path = kLoadCompressionDir;
        context.version = {version, version};
        context.segments_overlap = NONOVERLAPPING;
        context.max_rows_per_segment = UINT32_MAX;
        context.write_type = write_type;
        auto writer = RowsetFactory::create_rowset_writer(*_engine, context, false);
        EXPECT_TRUE(writer.has_value()) << writer.error();
        Block block = make_block(schema, first_row, rows);
        // flush_single_block is the memtable flush entry point.
        EXPECT_TRUE(writer.value()->flush_single_block(&block).ok());
        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer.value()->build(rowset).ok());
        return rowset;
    }

    // Codec recorded for every column (and every child column such as the null map) of every
    // segment of the rowset.
    static std::vector<segment_v2::CompressionTypePB> column_codecs(const RowsetSharedPtr& rowset) {
        std::vector<segment_v2::SegmentSharedPtr> segments;
        EXPECT_TRUE(std::static_pointer_cast<BetaRowset>(rowset)->load_segments(&segments).ok());
        std::vector<segment_v2::CompressionTypePB> codecs;
        for (const auto& segment : segments) {
            std::shared_ptr<segment_v2::SegmentFooterPB> footer;
            OlapReaderStatistics stats;
            EXPECT_TRUE(segment->_get_segment_footer(footer, &stats).ok());
            for (const auto& column : footer->columns()) {
                codecs.push_back(column.compression());
                for (const auto& child : column.children_columns()) {
                    codecs.push_back(child.compression());
                }
            }
        }
        return codecs;
    }

    static std::vector<std::tuple<int64_t, std::string>> read_rows(const TabletSchemaSPtr& schema,
                                                                   const RowsetSharedPtr& rowset) {
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = schema;
        reader_context.need_ordered_result = false;
        std::vector<uint32_t> return_columns = {0, 1};
        reader_context.return_columns = &return_columns;
        RowsetReaderSharedPtr reader;
        EXPECT_TRUE(rowset->create_reader(&reader).ok());
        EXPECT_TRUE(reader->init(&reader_context).ok());
        std::vector<std::tuple<int64_t, std::string>> rows;
        Status st;
        do {
            Block block = schema->create_block();
            st = reader->next_batch(&block);
            for (size_t i = 0; i < block.rows(); ++i) {
                rows.emplace_back(block.get_by_position(0).column->get_int(i),
                                  block.get_by_position(1).column->get_data_at(i).to_string());
            }
        } while (st.ok());
        EXPECT_TRUE(st.is<ErrorCode::END_OF_FILE>()) << st;
        return rows;
    }

    static void expect_rows(const std::vector<std::tuple<int64_t, std::string>>& rows,
                            int64_t first_row, int64_t count) {
        ASSERT_EQ(rows.size(), count);
        for (int64_t i = 0; i < count; ++i) {
            EXPECT_EQ(std::get<0>(rows[i]), first_row + i);
            EXPECT_EQ(std::get<1>(rows[i]), value_at(first_row + i));
        }
    }

    StorageEngine* _engine = nullptr;
    int32_t _saved_threshold = 0;
    std::string _saved_type;
    int32_t _saved_level = 0;
    int32_t _saved_load_level = 0;
};

TEST_F(LoadSegmentCompressionTest, override_applies_to_loads_only) {
    config::load_segment_compression_type = "";
    EXPECT_EQ(load_segment_compression_type(DataWriteType::TYPE_DIRECT),
              segment_v2::UNKNOWN_COMPRESSION);

    config::load_segment_compression_type = "lz4";
    EXPECT_EQ(load_segment_compression_type(DataWriteType::TYPE_DIRECT), segment_v2::LZ4);
    EXPECT_EQ(load_segment_compression_type(DataWriteType::TYPE_COMPACTION),
              segment_v2::UNKNOWN_COMPRESSION);
    EXPECT_EQ(load_segment_compression_type(DataWriteType::TYPE_SCHEMA_CHANGE),
              segment_v2::UNKNOWN_COMPRESSION);

    config::load_segment_compression_type = "ZSTD";
    EXPECT_EQ(load_segment_compression_type(DataWriteType::TYPE_DIRECT), segment_v2::ZSTD);
    config::load_segment_compression_type = "no_compression";
    EXPECT_EQ(load_segment_compression_type(DataWriteType::TYPE_DIRECT),
              segment_v2::NO_COMPRESSION);
    // Unparsable names and DEFAULT_COMPRESSION keep the table codec.
    config::load_segment_compression_type = "brotli";
    EXPECT_EQ(load_segment_compression_type(DataWriteType::TYPE_DIRECT),
              segment_v2::UNKNOWN_COMPRESSION);
    config::load_segment_compression_type = "DEFAULT_COMPRESSION";
    EXPECT_EQ(load_segment_compression_type(DataWriteType::TYPE_DIRECT),
              segment_v2::UNKNOWN_COMPRESSION);

    config::load_segment_zstd_compression_level = 1;
    EXPECT_EQ(load_segment_zstd_compression_level(DataWriteType::TYPE_DIRECT), 1);
    EXPECT_EQ(load_segment_zstd_compression_level(DataWriteType::TYPE_COMPACTION), 0);
}

TEST_F(LoadSegmentCompressionTest, load_uses_override_and_compaction_keeps_table_codec) {
    auto schema = create_schema();
    config::load_segment_compression_type = "LZ4";
    auto loaded = write_rowset(schema, DataWriteType::TYPE_DIRECT, 2, 0, 5000);
    auto compacted = write_rowset(schema, DataWriteType::TYPE_COMPACTION, 3, 5000, 5000);
    config::load_segment_compression_type = "";
    auto default_load = write_rowset(schema, DataWriteType::TYPE_DIRECT, 4, 10000, 5000);

    auto load_codecs = column_codecs(loaded);
    ASSERT_FALSE(load_codecs.empty());
    for (auto codec : load_codecs) {
        EXPECT_EQ(codec, segment_v2::LZ4);
    }
    for (auto codec : column_codecs(compacted)) {
        EXPECT_EQ(codec, segment_v2::ZSTD);
    }
    for (auto codec : column_codecs(default_load)) {
        EXPECT_EQ(codec, segment_v2::ZSTD);
    }

    // One tablet may now hold rowsets in both codecs; each reads back intact.
    expect_rows(read_rows(schema, loaded), 0, 5000);
    expect_rows(read_rows(schema, compacted), 5000, 5000);
    expect_rows(read_rows(schema, default_load), 10000, 5000);
}

TEST_F(LoadSegmentCompressionTest, load_zstd_level_changes_output_not_codec) {
    auto schema = create_schema();
    config::load_segment_compression_type = "";
    config::load_segment_zstd_compression_level = 19;
    auto high = write_rowset(schema, DataWriteType::TYPE_DIRECT, 2, 0, 20000);
    config::load_segment_zstd_compression_level = 1;
    auto low = write_rowset(schema, DataWriteType::TYPE_DIRECT, 3, 0, 20000);

    for (auto codec : column_codecs(high)) {
        EXPECT_EQ(codec, segment_v2::ZSTD);
    }
    for (auto codec : column_codecs(low)) {
        EXPECT_EQ(codec, segment_v2::ZSTD);
    }
    EXPECT_NE(high->data_disk_size(), low->data_disk_size());
    expect_rows(read_rows(schema, high), 0, 20000);
    expect_rows(read_rows(schema, low), 0, 20000);
    // The scope ends with the flush; nothing leaks into later work on this thread.
    EXPECT_EQ(current_zstd_compression_level(), config::zstd_compression_level);
}

TEST(ZstdCompressionLevelTest, scoped_level_nests_and_restores) {
    const int32_t saved = config::zstd_compression_level;
    config::zstd_compression_level = 3;
    EXPECT_EQ(current_zstd_compression_level(), 3);
    {
        ScopedZstdCompressionLevel outer(1);
        EXPECT_EQ(current_zstd_compression_level(), 1);
        {
            ScopedZstdCompressionLevel keep(0);
            EXPECT_EQ(current_zstd_compression_level(), 1);
            ScopedZstdCompressionLevel inner(9);
            EXPECT_EQ(current_zstd_compression_level(), 9);
        }
        EXPECT_EQ(current_zstd_compression_level(), 1);
    }
    EXPECT_EQ(current_zstd_compression_level(), 3);
    config::zstd_compression_level = 5;
    EXPECT_EQ(current_zstd_compression_level(), 5);
    config::zstd_compression_level = saved;
}

TEST(ZstdCompressionLevelTest, level_controls_ratio_and_output_round_trips) {
    std::string input;
    for (int i = 0; i < 20000; ++i) {
        input += "event=" + std::to_string(i % 251) + " src=10.0." + std::to_string(i % 17) +
                 ".1 status=" + std::to_string(200 + i % 7) + ";";
    }
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(segment_v2::ZSTD, &codec).ok());
    auto compress_at = [&](int level) {
        ScopedZstdCompressionLevel scope(level);
        faststring out;
        EXPECT_TRUE(codec->compress(Slice(input), &out).ok());
        std::string decompressed(input.size(), '\0');
        Slice result(decompressed.data(), decompressed.size());
        EXPECT_TRUE(codec->decompress(Slice(out.data(), out.size()), &result).ok());
        EXPECT_EQ(result.size, input.size());
        EXPECT_EQ(decompressed, input);
        return out.size();
    };
    const size_t fast = compress_at(1);
    const size_t strong = compress_at(19);
    EXPECT_LT(strong, fast);
}

} // namespace doris
