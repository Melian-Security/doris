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
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "load/memtable/memtable.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/desc_tbl_builder.h"

namespace doris {
namespace {

class ScopedReleaseInputColumns {
public:
    explicit ScopedReleaseInputColumns(bool value)
            : _old(config::memtable_flush_release_input_columns) {
        config::memtable_flush_release_input_columns = value;
    }
    ~ScopedReleaseInputColumns() { config::memtable_flush_release_input_columns = _old; }

private:
    bool _old;
};

struct Row {
    int32_t key;
    std::optional<std::string> document;
};

std::vector<Row> make_rows(size_t count, uint32_t seed) {
    std::mt19937 rng(seed);
    // Distinct sort values, so the flush order of every row is fully determined.
    std::vector<int32_t> sort_values(count);
    std::iota(sort_values.begin(), sort_values.end(), 0);
    std::shuffle(sort_values.begin(), sort_values.end(), rng);
    std::vector<Row> rows;
    rows.reserve(count);
    for (size_t row = 0; row < count; ++row) {
        std::optional<std::string> document;
        if (rng() % 10 != 0) {
            document = R"({"seq":)" + std::to_string(row) + R"(,"host":"h)" +
                       std::to_string(rng() % 7) + R"(","tags":["a",)" + std::to_string(rng() % 3) +
                       "]}";
        }
        rows.push_back({sort_values[row], std::move(document)});
    }
    return rows;
}

std::string json_at(const IColumn& variant, size_t row) {
    DataTypeVariantV2SerDe serde;
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    const Status status = serde.serialize_one_cell_to_json(variant, row, writer, options);
    EXPECT_TRUE(status.ok()) << status;
    writer.commit();
    return output->get_data_at(0).to_string();
}

std::string canonical_json(const std::string& document) {
    DataTypeVariantV2SerDe serde;
    auto variant = ColumnVariantV2::create();
    Slice slice(document.data(), document.size());
    const Status status =
            serde.deserialize_one_cell_from_json(*variant, slice, DataTypeSerDe::FormatOptions {});
    EXPECT_TRUE(status.ok()) << status;
    return json_at(*variant, 0);
}

class MemTableFlushReleaseTest : public ::testing::Test {
protected:
    void SetUp() override {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        ColumnPB* sort_column = schema_pb.add_column();
        sort_column->set_unique_id(1);
        sort_column->set_name("k");
        sort_column->set_type("INT");
        sort_column->set_is_key(true);
        sort_column->set_is_nullable(false);
        ColumnPB* value = schema_pb.add_column();
        value->set_unique_id(2);
        value->set_name("v");
        value->set_type("VARIANT");
        value->set_is_key(false);
        value->set_is_nullable(true);
        value->set_variant_max_subcolumns_count(2048);
        _schema = std::make_shared<TabletSchema>();
        _schema->init_from_pb(schema_pb);

        _sort_type = std::make_shared<DataTypeInt32>();
        _value_type = make_nullable(std::make_shared<DataTypeVariantV2>(2048));
        DescriptorTblBuilder builder(&_pool);
        builder.declare_tuple() << TupleDescBuilder::SlotType {_sort_type, "k"}
                                << TupleDescBuilder::SlotType {_value_type, "v"};
        DescriptorTbl* desc_tbl = builder.build();
        _tuple_desc = desc_tbl->get_tuple_descriptor(0);
        _slots = _tuple_desc->slots();

        _tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::LOAD,
                                                    "UT-MemTableFlushReleaseTest");
        _resource_ctx = ResourceContext::create_shared();
        _resource_ctx->memory_context()->set_mem_tracker(_tracker);
    }

    Block make_block(const std::vector<Row>& rows) const {
        DataTypeVariantV2SerDe serde;
        auto sort_values = ColumnInt32::create();
        auto variants = ColumnVariantV2::create();
        auto nulls = ColumnUInt8::create();
        for (const auto& row : rows) {
            sort_values->insert_value(row.key);
            const std::string text = row.document.value_or("null");
            Slice slice(text.data(), text.size());
            const Status status = serde.deserialize_one_cell_from_json(
                    *variants, slice, DataTypeSerDe::FormatOptions {});
            EXPECT_TRUE(status.ok()) << status;
            nulls->insert_value(row.document.has_value() ? 0 : 1);
        }
        Block block;
        block.insert({std::move(sort_values), _sort_type, "k"});
        block.insert(
                {ColumnNullable::create(std::move(variants), std::move(nulls)), _value_type, "v"});
        return block;
    }

    // Inserts the rows in batches the way the tablet sink hands them over, then flushes.
    std::unique_ptr<Block> insert_and_flush(const std::vector<Row>& rows, size_t batch_rows) {
        MemTable memtable(1, _schema, &_slots, _tuple_desc, false, nullptr, _resource_ctx);
        for (size_t begin = 0; begin < rows.size(); begin += batch_rows) {
            const size_t end = std::min(rows.size(), begin + batch_rows);
            Block block = make_block(std::vector<Row>(rows.begin() + begin, rows.begin() + end));
            DorisVector<uint32_t> row_idxs(end - begin);
            std::iota(row_idxs.begin(), row_idxs.end(), 0);
            const Status status = memtable.insert(&block, row_idxs);
            EXPECT_TRUE(status.ok()) << status;
        }
        std::unique_ptr<Block> flushed;
        const Status status = memtable.to_block(&flushed);
        EXPECT_TRUE(status.ok()) << status;
        return flushed;
    }

    // DUP KEY flush output: rows ordered by the sort column.
    static void expect_sorted_output(const Block& block, const std::vector<Row>& rows) {
        std::vector<size_t> order(rows.size());
        std::iota(order.begin(), order.end(), 0);
        std::stable_sort(order.begin(), order.end(), [&](size_t left, size_t right) {
            return rows[left].key < rows[right].key;
        });
        ASSERT_EQ(block.rows(), rows.size());
        const auto& sort_values = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
        const auto& values = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        for (size_t row = 0; row < order.size(); ++row) {
            const Row& expected = rows[order[row]];
            ASSERT_EQ(sort_values.get_element(row), expected.key) << row;
            ASSERT_EQ(values.is_null_at(row), !expected.document.has_value()) << row;
            if (expected.document.has_value()) {
                ASSERT_EQ(json_at(values.get_nested_column(), row),
                          canonical_json(*expected.document))
                        << row;
            }
        }
    }

    ObjectPool _pool;
    TabletSchemaSPtr _schema;
    DataTypePtr _sort_type;
    DataTypePtr _value_type;
    TupleDescriptor* _tuple_desc = nullptr;
    std::vector<SlotDescriptor*> _slots;
    std::shared_ptr<MemTrackerLimiter> _tracker;
    std::shared_ptr<ResourceContext> _resource_ctx;
};

TEST_F(MemTableFlushReleaseTest, UnsortedRowsFlushInOrderWithAndWithoutRelease) {
    const auto rows = make_rows(3000, 11);
    for (const bool release : {true, false}) {
        SCOPED_TRACE(release);
        ScopedReleaseInputColumns scoped(release);
        const auto flushed = insert_and_flush(rows, 512);
        ASSERT_NE(flushed, nullptr);
        expect_sorted_output(*flushed, rows);
    }
}

TEST_F(MemTableFlushReleaseTest, SortedRowsAreMovedIntoTheOutput) {
    auto rows = make_rows(2000, 12);
    std::stable_sort(rows.begin(), rows.end(),
                     [](const Row& left, const Row& right) { return left.key < right.key; });
    for (const bool release : {true, false}) {
        SCOPED_TRACE(release);
        ScopedReleaseInputColumns scoped(release);
        const auto flushed = insert_and_flush(rows, 300);
        ASSERT_NE(flushed, nullptr);
        expect_sorted_output(*flushed, rows);
    }
}

} // namespace
} // namespace doris
