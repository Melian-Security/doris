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
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_variant_serde.h"
#include "core/string_buffer.hpp"
#include "exec/common/variant_util.h"
#include "exprs/function/simple_function_factory.h"
#include "format/arrow/arrow_stream_reader.h"
#include "runtime/runtime_state.h"
#include "util/json/json_parser.h"

namespace doris {
namespace {

std::shared_ptr<arrow::Array> make_utf8(const std::vector<std::optional<std::string>>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        EXPECT_TRUE((value.has_value() ? builder.Append(*value) : builder.AppendNull()).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

std::string json_at(const IColumn& column, size_t row) {
    DataTypeVariantSerDe serde;
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    EXPECT_TRUE(serde.serialize_one_cell_to_json(column, row, writer, options).ok());
    writer.commit();
    return output->get_data_at(0).to_string();
}

} // namespace

TEST(ArrowStreamReaderVariantTest, LoadReadsLegacyVariantAsText) {
    auto variant = std::make_shared<DataTypeVariant>(2048);
    EXPECT_EQ(ArrowStreamReader::load_source_type(make_nullable(variant))->get_primitive_type(),
              TYPE_STRING);
    EXPECT_EQ(ArrowStreamReader::load_source_type(variant)->get_primitive_type(), TYPE_STRING);

    DataTypePtr variant_v2 = std::make_shared<DataTypeVariantV2>();
    EXPECT_EQ(ArrowStreamReader::load_source_type(variant_v2), variant_v2);
    DataTypePtr int_type = make_nullable(std::make_shared<DataTypeInt32>());
    EXPECT_EQ(ArrowStreamReader::load_source_type(int_type), int_type);
}

// The load path for a legacy Variant Arrow column: the reader reads text, the scanner casts it to
// the slot type, the tablet sink redistributes the rows and the segment writer parses them. Each
// row must parse to what the JSON cell path builds for the same document.
TEST(ArrowStreamReaderVariantTest, LegacyVariantLoadPathMatchesJsonCells) {
    auto slot_type = make_nullable(std::make_shared<DataTypeVariant>(2048));
    auto source_type = make_nullable(ArrowStreamReader::load_source_type(slot_type));
    constexpr size_t num_rows = 3000;
    constexpr size_t batch_rows = 1024;
    std::vector<std::optional<std::string>> documents;
    for (size_t i = 0; i < num_rows; ++i) {
        if (i % 11 == 0) {
            documents.emplace_back(std::nullopt);
            continue;
        }
        std::string doc = "{\"a\":" + (i % 7 == 0 ? "\"t" + std::to_string(i) + "\"" : std::to_string(i));
        doc += ",\"p" + std::to_string(i % 2500) + "\":" + std::to_string(i);
        if (i % 5 == 0) {
            doc += ",\"n\":{\"x\":" + std::to_string(i) + ",\"y\":[1,\"v\"]}";
        }
        doc += "}";
        documents.emplace_back(std::move(doc));
    }

    // Reader: the Arrow batches of one stream land in one source column.
    auto source = source_type->create_column();
    for (size_t begin = 0; begin < num_rows; begin += batch_rows) {
        const size_t end = std::min(num_rows, begin + batch_rows);
        auto array = make_utf8(std::vector<std::optional<std::string>>(
                documents.begin() + begin, documents.begin() + end));
        ASSERT_TRUE(source_type->get_serde()
                            ->read_column_from_arrow(*source, array.get(), 0, end - begin,
                                                     cctz::utc_time_zone())
                            .ok());
    }

    // Scanner: CAST(source AS slot type), then copy into the output block column.
    ColumnWithTypeAndName source_column {std::move(source), source_type, "v"};
    ColumnsWithTypeAndName arguments {source_column, {nullptr, slot_type, "cast_to"}};
    auto cast = SimpleFunctionFactory::instance().get_function("CAST", arguments, slot_type, {});
    ASSERT_NE(cast, nullptr);
    Block cast_block;
    cast_block.insert(source_column);
    RuntimeState state;
    auto ctx = FunctionContext::create_context(&state, {}, {});
    ASSERT_TRUE(cast->execute(ctx.get(), cast_block, {0}, 0, num_rows).ok());
    auto output = slot_type->create_column();
    output->insert_range_from(*cast_block.get_by_position(0).column, 0, num_rows);

    // Sink: redistribute the rows in a shuffled order.
    std::vector<uint32_t> selector(num_rows);
    std::iota(selector.begin(), selector.end(), 0);
    std::mt19937 rng(42);
    std::shuffle(selector.begin(), selector.end(), rng);
    auto tablet_column = output->clone_empty();
    tablet_column->insert_indices_from(*output, selector.data(), selector.data() + num_rows);

    // Segment writer: parse the redistributed text.
    Block tablet_block;
    tablet_block.insert({std::move(tablet_column), slot_type, "v"});
    ASSERT_TRUE(variant_util::parse_and_materialize_variant_columns(tablet_block, {0},
                                                                    {ParseConfig {}})
                        .ok());
    const auto& parsed = assert_cast<const ColumnNullable&>(*tablet_block.get_by_position(0).column);
    ASSERT_EQ(parsed.size(), num_rows);

    DataTypeVariantSerDe serde;
    DataTypeSerDe::FormatOptions options;
    for (size_t k = 0; k < num_rows; ++k) {
        const auto& document = documents[selector[k]];
        ASSERT_EQ(parsed.is_null_at(k), !document.has_value()) << k;
        if (!document.has_value()) {
            continue;
        }
        auto expected = ColumnVariant::create(2048, false);
        Slice slice(document->data(), document->size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*expected, slice, options).ok());
        expected->finalize();
        ASSERT_EQ(json_at(parsed.get_nested_column(), k), json_at(*expected, 0)) << *document;
    }
}

} // namespace doris
