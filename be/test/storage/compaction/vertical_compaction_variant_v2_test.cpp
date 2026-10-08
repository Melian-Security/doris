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

// Cumulative vertical compaction of DUPLICATE rowsets loaded through the Variant V2 writer must
// return exactly the rows of its inputs, whichever copy strategy it uses.

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "exec/common/variant_util.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/data_dir.h"
#include "storage/merger.h"
#include "storage/predicate/column_predicate.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/segment/segment_loader.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

constexpr std::string_view kTestDir = "./ut_dir/vertical_compaction_variant_v2_test";

template <typename T>
class ScopedConfig {
public:
    ScopedConfig(T* field, T value) : _field(field), _old(*field) { *_field = std::move(value); }
    ~ScopedConfig() { *_field = std::move(_old); }
    ScopedConfig(const ScopedConfig&) = delete;
    ScopedConfig& operator=(const ScopedConfig&) = delete;

private:
    T* _field;
    T _old;
};

// One generated input row; nullopt event is SQL NULL.
struct InputRow {
    int64_t ts;
    std::string org;
    std::optional<std::string> event;
};

std::string quoted(const std::string& value) {
    return "\"" + value + "\"";
}

// Events exercising typed leaves, nested objects, arrays, JSON nulls, a path whose type differs
// between rowsets (`conflict`) and within a rowset (`mixed`), and a per-row long tail of paths.
std::string event_json(int64_t rowset, int64_t row, size_t tail_paths, std::mt19937_64& rng) {
    std::string json = "{\"id\":" + std::to_string(row);
    json += ",\"name\":" + quoted("n" + std::to_string(rng() % 50));
    json += ",\"src\":{\"ip\":" + quoted("10.0." + std::to_string(rng() % 256) + ".1") +
            ",\"port\":" + std::to_string(rng() % 65536) + "}";
    json += ",\"conflict\":" +
            (rowset % 2 == 0 ? std::to_string(rng() % 100) : quoted("s" + std::to_string(row)));
    json += ",\"mixed\":";
    switch (row % 4) {
    case 0:
        json += std::to_string(row);
        break;
    case 1:
        json += std::to_string(row) + ".5";
        break;
    case 2:
        json += "true";
        break;
    default:
        json += quoted("m" + std::to_string(row));
        break;
    }
    json += ",\"arr\":[" + std::to_string(row) + "," + std::to_string(row + 1) + "]";
    if (row % 3 == 0) {
        json += ",\"maybe\":null";
    } else {
        json += ",\"maybe\":{\"v\":" + std::to_string(row) + "}";
    }
    for (size_t i = 0; i < tail_paths; ++i) {
        json += ",\"tail_" + std::to_string(rowset) + "_" + std::to_string((row * 7 + i) % 997) +
                "\":" + std::to_string(i);
    }
    json += "}";
    return json;
}

} // namespace

class VerticalCompactionVariantV2Test : public testing::Test {
protected:
    void SetUp() override {
        _root = std::filesystem::absolute(std::string(kTestDir)).string();
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_root).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_root).ok());
        auto engine = std::make_unique<StorageEngine>(EngineOptions {});
        _engine = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine, _root);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        _tablet.reset();
        _data_dir.reset();
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_root).ok());
    }

    void create_tablet(int64_t tablet_id, int32_t max_subcolumns) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_compression_type(segment_v2::CompressionTypePB::ZSTD);
        schema_pb.set_num_short_key_columns(1);
        ColumnPB* key = schema_pb.add_column();
        key->set_unique_id(0);
        key->set_name("ts");
        key->set_type("BIGINT");
        key->set_is_key(true);
        key->set_is_nullable(false);
        key->set_length(8);
        ColumnPB* org = schema_pb.add_column();
        org->set_unique_id(1);
        org->set_name("org");
        org->set_type("STRING");
        org->set_is_key(false);
        org->set_is_nullable(false);
        org->set_length(2147483643);
        org->set_aggregation("NONE");
        ColumnPB* event = schema_pb.add_column();
        event->set_unique_id(2);
        event->set_name("event");
        event->set_type("VARIANT");
        event->set_is_key(false);
        event->set_is_nullable(true);
        event->set_aggregation("NONE");
        event->set_variant_max_subcolumns_count(max_subcolumns);
        event->set_variant_enable_typed_paths_to_sparse(false);
        event->set_variant_max_sparse_column_statistics_size(10000);
        event->set_variant_sparse_hash_shard_count(1);
        _schema = std::make_shared<TabletSchema>();
        _schema->init_from_pb(schema_pb);

        TabletMetaSharedPtr tablet_meta(new TabletMeta(_schema));
        tablet_meta->_tablet_id = tablet_id;
        _tablet = std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir.get());
        ASSERT_TRUE(_tablet->init().ok());
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    }

    // Rows of `num_rowsets` loads. Interleaved keys make every output run one row long;
    // otherwise each load owns one contiguous key range.
    std::vector<std::vector<InputRow>> make_rows(int64_t num_rowsets, int64_t rows_per_rowset,
                                                 bool interleave, size_t tail_paths) {
        std::vector<std::vector<InputRow>> rowsets(num_rowsets);
        std::mt19937_64 rng(42);
        for (int64_t r = 0; r < num_rowsets; ++r) {
            for (int64_t row = 0; row < rows_per_rowset; ++row) {
                InputRow input;
                input.ts = interleave ? row * num_rowsets + r : r * rows_per_rowset + row;
                input.org = "org-" + std::to_string(row % 5);
                if (row % 11 != 5) {
                    input.event = event_json(r, row, tail_paths, rng);
                }
                rowsets[r].push_back(std::move(input));
            }
        }
        return rowsets;
    }

    RowsetSharedPtr write_rowset(const std::vector<InputRow>& rows, int64_t version) {
        RowsetWriterContext ctx;
        RowsetId rowset_id;
        rowset_id.init(_tablet->tablet_id() * 100 + version);
        ctx.rowset_id = rowset_id;
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _data_dir.get();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = _schema;
        ctx.tablet_path = _tablet->tablet_path();
        ctx.tablet_id = _tablet->tablet_id();
        ctx.tablet = _tablet;
        ctx.version = Version(version, version);
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = 1 << 20;
        ctx.write_type = DataWriteType::TYPE_DIRECT;
        auto writer_result = RowsetFactory::create_rowset_writer(*_engine, ctx, false);
        EXPECT_TRUE(writer_result.has_value()) << writer_result.error();
        auto writer = std::move(writer_result).value();

        auto keys = ColumnInt64::create();
        auto orgs = ColumnString::create();
        auto values = ColumnVariantV2::create();
        auto nulls = ColumnUInt8::create();
        DataTypeVariantV2SerDe serde;
        DataTypeSerDe::FormatOptions options;
        for (const auto& row : rows) {
            keys->insert_value(row.ts);
            orgs->insert_data(row.org.data(), row.org.size());
            if (row.event.has_value()) {
                Slice slice(row.event->data(), row.event->size());
                EXPECT_TRUE(serde.deserialize_one_cell_from_json(*values, slice, options).ok());
                nulls->insert_value(0);
            } else {
                values->insert_default();
                nulls->insert_value(1);
            }
        }
        Block block;
        block.insert({std::move(keys), std::make_shared<DataTypeInt64>(), "ts"});
        block.insert({std::move(orgs), std::make_shared<DataTypeString>(), "org"});
        block.insert({ColumnNullable::create(std::move(values), std::move(nulls)),
                      make_nullable(std::make_shared<DataTypeVariantV2>(
                              _schema->column(2).variant_max_subcolumns_count())),
                      "event"});
        Status st = writer->add_block(&block);
        EXPECT_TRUE(st.ok()) << st.to_string();
        st = writer->flush();
        EXPECT_TRUE(st.ok()) << st.to_string();
        RowsetSharedPtr rowset;
        st = writer->build(rowset);
        EXPECT_TRUE(st.ok()) << st.to_string();
        return rowset;
    }

    RowsetSharedPtr compact(const std::vector<RowsetSharedPtr>& inputs, int64_t sequence) {
        std::vector<RowsetReaderSharedPtr> readers;
        for (const auto& rowset : inputs) {
            RowsetReaderSharedPtr reader;
            EXPECT_TRUE(rowset->create_reader(&reader).ok());
            readers.push_back(std::move(reader));
        }
        auto schema = std::make_shared<TabletSchema>(*_schema);
        Status st = variant_util::VariantCompactionUtil::get_extended_compaction_schema(inputs,
                                                                                       schema);
        EXPECT_TRUE(st.ok()) << st.to_string();

        RowsetWriterContext ctx;
        RowsetId rowset_id;
        rowset_id.init(_tablet->tablet_id() * 100 + 50 + sequence);
        ctx.rowset_id = rowset_id;
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _data_dir.get();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = schema;
        ctx.tablet_path = _tablet->tablet_path();
        ctx.tablet_id = _tablet->tablet_id();
        ctx.tablet = _tablet;
        ctx.version = Version(inputs.front()->start_version(), inputs.back()->end_version());
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.write_type = DataWriteType::TYPE_COMPACTION;
        ctx.compaction_type = ReaderType::READER_CUMULATIVE_COMPACTION;
        auto writer_result = RowsetFactory::create_rowset_writer(*_engine, ctx, true);
        EXPECT_TRUE(writer_result.has_value()) << writer_result.error();
        auto writer = std::move(writer_result).value();

        Merger::Statistics stats;
        st = Merger::vertical_merge_rowsets(_tablet, ReaderType::READER_CUMULATIVE_COMPACTION,
                                            *schema, readers, writer.get(), 1 << 20,
                                            static_cast<int64_t>(inputs.size()), &stats);
        EXPECT_TRUE(st.ok()) << st.to_string();
        RowsetSharedPtr output;
        st = writer->build(output);
        EXPECT_TRUE(st.ok()) << st.to_string();
        // The persisted schema never carries the temporary extracted columns.
        output->rowset_meta()->set_tablet_schema(schema->copy_without_variant_extracted_columns());
        RowsetSharedPtr reloaded;
        EXPECT_TRUE(RowsetFactory::create_rowset(output->rowset_meta()->tablet_schema(),
                                                 _tablet->tablet_path(), output->rowset_meta(),
                                                 &reloaded)
                            .ok());
        return reloaded;
    }

    using Row = std::tuple<int64_t, std::string, std::optional<std::string>>;

    // All rows of `rowsets` read back as (ts, org, event JSON) through a Variant V2 scan.
    std::vector<Row> read_rows(const std::vector<RowsetSharedPtr>& rowsets) {
        // TabletSchema copies share their TabletColumn objects, so the scan schema is rebuilt
        // from its PB rather than copied before marking the column as Variant V2.
        TabletSchemaPB schema_pb;
        _schema->to_schema_pb(&schema_pb);
        auto read_schema = std::make_shared<TabletSchema>();
        read_schema->init_from_pb(schema_pb);
        read_schema->mutable_column(2).set_variant_is_v2(true);
        std::vector<uint32_t> return_columns {0, 1, 2};
        std::vector<std::shared_ptr<ColumnPredicate>> predicates;
        std::vector<Row> rows;
        for (const auto& rowset : rowsets) {
            RowsetReaderSharedPtr reader;
            EXPECT_TRUE(rowset->create_reader(&reader).ok());
            OlapReaderStatistics stats;
            RowsetReaderContext ctx;
            ctx.reader_type = ReaderType::READER_QUERY;
            ctx.tablet_schema = read_schema;
            ctx.need_ordered_result = false;
            ctx.return_columns = &return_columns;
            ctx.predicates = &predicates;
            ctx.stats = &stats;
            Status st = reader->init(&ctx);
            EXPECT_TRUE(st.ok()) << st.to_string();
            while (true) {
                Block block = read_schema->create_block_by_cids(return_columns);
                st = reader->next_batch(&block);
                if (st.is<ErrorCode::END_OF_FILE>()) {
                    break;
                }
                EXPECT_TRUE(st.ok()) << st.to_string();
                if (!st.ok()) {
                    break;
                }
                append_rows(block, &rows);
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    static void append_rows(const Block& block, std::vector<Row>* rows) {
        const auto keys = block.get_by_position(0).column->convert_to_full_column_if_const();
        const auto orgs = block.get_by_position(1).column->convert_to_full_column_if_const();
        const auto events = block.get_by_position(2).column->convert_to_full_column_if_const();
        const auto& nullable = assert_cast<const ColumnNullable&>(*events);
        const auto* variant = check_and_get_column<ColumnVariantV2>(nullable.get_nested_column());
        ASSERT_NE(variant, nullptr) << events->get_name();
        DataTypeVariantV2SerDe serde;
        DataTypeSerDe::FormatOptions options;
        for (size_t row = 0; row < block.rows(); ++row) {
            std::optional<std::string> json;
            if (!nullable.is_null_at(row)) {
                auto output = ColumnString::create();
                BufferWritable writer(*output);
                ASSERT_TRUE(serde.serialize_one_cell_to_json(*variant, row, writer, options).ok());
                writer.commit();
                json = output->get_data_at(0).to_string();
            }
            rows->emplace_back(assert_cast<const ColumnInt64&>(*keys).get_element(row),
                               orgs->get_data_at(row).to_string(), std::move(json));
        }
    }

    // Loads, compacts and checks the output rows against the input rows. Returns the inputs'
    // rows so callers can compare several compactions of the same data.
    std::vector<Row> check_compaction(int64_t tablet_id, int32_t max_subcolumns,
                                      int64_t num_rowsets, int64_t rows_per_rowset,
                                      bool interleave, size_t tail_paths) {
        create_tablet(tablet_id, max_subcolumns);
        const auto generated = make_rows(num_rowsets, rows_per_rowset, interleave, tail_paths);
        std::vector<RowsetSharedPtr> inputs;
        for (int64_t r = 0; r < num_rowsets; ++r) {
            inputs.push_back(write_rowset(generated[r], r + 2));
        }
        const auto expected = read_rows(inputs);
        EXPECT_EQ(expected.size(), static_cast<size_t>(num_rowsets * rows_per_rowset));

        RowsetSharedPtr output = compact(inputs, 0);
        EXPECT_NE(output, nullptr);
        if (output == nullptr) {
            return expected;
        }
        EXPECT_EQ(output->num_rows(), expected.size());
        const auto actual = read_rows({output});
        EXPECT_EQ(actual.size(), expected.size());
        size_t mismatches = 0;
        for (size_t i = 0; i < std::min(actual.size(), expected.size()); ++i) {
            if (actual[i] != expected[i] && mismatches++ < 3) {
                ADD_FAILURE() << "row " << i << " ts=" << std::get<0>(expected[i])
                              << "\n expected: " << std::get<2>(expected[i]).value_or("NULL")
                              << "\n actual:   " << std::get<2>(actual[i]).value_or("NULL");
            }
        }
        EXPECT_EQ(mismatches, 0);
        return expected;
    }

    std::string _root;
    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    TabletSchemaSPtr _schema;
    TabletSharedPtr _tablet;
};

TEST_F(VerticalCompactionVariantV2Test, InterleavedRowsGatherCopy) {
    ScopedConfig gather(&config::enable_vertical_compaction_gather_copy, true);
    check_compaction(810001, 2048, 6, 3000, true, 0);
}

TEST_F(VerticalCompactionVariantV2Test, InterleavedRowsRangeCopy) {
    ScopedConfig gather(&config::enable_vertical_compaction_gather_copy, false);
    check_compaction(810002, 2048, 6, 3000, true, 0);
}

TEST_F(VerticalCompactionVariantV2Test, ContiguousRowsUseRangeCopy) {
    ScopedConfig gather(&config::enable_vertical_compaction_gather_copy, true);
    check_compaction(810003, 2048, 4, 5000, false, 0);
}

TEST_F(VerticalCompactionVariantV2Test, SparsePathsBeyondSubcolumnLimit) {
    // Four materialized subcolumns: every other path is stored in the sparse column.
    ScopedConfig gather(&config::enable_vertical_compaction_gather_copy, true);
    check_compaction(810004, 4, 5, 2000, true, 3);
}

TEST_F(VerticalCompactionVariantV2Test, MoreThan2048DistinctPaths) {
    // 3 rowsets x 997 tail paths each exceed the 2048-subcolumn budget, so compaction chooses
    // which paths stay materialized and moves the rest to the sparse column.
    ScopedConfig gather(&config::enable_vertical_compaction_gather_copy, true);
    check_compaction(810005, 2048, 3, 1200, true, 6);
}

} // namespace doris
