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
#include <arrow/builder.h>
#include <arrow/type.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_variant_serde.h"
#include "core/string_buffer.hpp"

namespace doris {
namespace {

class ScopedInvalidJsonMode {
public:
    explicit ScopedInvalidJsonMode(bool value)
            : _old(config::variant_throw_exeception_on_invalid_json) {
        config::variant_throw_exeception_on_invalid_json = value;
    }
    ~ScopedInvalidJsonMode() { config::variant_throw_exeception_on_invalid_json = _old; }

private:
    bool _old;
};

std::shared_ptr<arrow::Array> make_arrow_strings(
        const std::shared_ptr<arrow::DataType>& type,
        const std::vector<std::optional<std::string>>& values) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    EXPECT_TRUE(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder).ok());
    for (const auto& value : values) {
        arrow::Status status;
        if (!value.has_value()) {
            status = builder->AppendNull();
        } else if (type->id() == arrow::Type::STRING) {
            status = static_cast<arrow::StringBuilder&>(*builder).Append(*value);
        } else if (type->id() == arrow::Type::LARGE_STRING) {
            status = static_cast<arrow::LargeStringBuilder&>(*builder).Append(*value);
        } else if (type->id() == arrow::Type::BINARY) {
            status = static_cast<arrow::BinaryBuilder&>(*builder).Append(*value);
        } else {
            status = static_cast<arrow::LargeBinaryBuilder&>(*builder).Append(*value);
        }
        EXPECT_TRUE(status.ok()) << status.ToString();
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder->Finish(&array).ok());
    return array;
}

std::string json_at(const DataTypeVariantSerDe& serde, const IColumn& column, size_t row) {
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    const Status status = serde.serialize_one_cell_to_json(column, row, writer, options);
    EXPECT_TRUE(status.ok()) << status;
    if (!status.ok()) {
        return {};
    }
    writer.commit();
    return output->get_data_at(0).to_string();
}

// Loads each document through the JSON cell path, the reference the Arrow path must match.
MutableColumnPtr load_through_json(const DataTypeVariantSerDe& serde,
                                   const std::vector<std::string>& documents) {
    auto column = ColumnVariant::create(0, false);
    DataTypeSerDe::FormatOptions options;
    for (const auto& document : documents) {
        Slice slice(document.data(), document.size());
        EXPECT_TRUE(serde.deserialize_one_cell_from_json(*column, slice, options).ok());
    }
    return column;
}

} // namespace

TEST(DataTypeVariantSerdeArrowTest, NullArrowInputIsRejected) {
    DataTypeVariantSerDe serde;
    auto column = ColumnVariant::create(0, false);
    EXPECT_EQ(serde.read_column_from_arrow(*column, nullptr, 0, 0, cctz::utc_time_zone()).code(),
              ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), 0);
}

TEST(DataTypeVariantSerdeArrowTest, ArrowStringTypesMatchTheJsonCellPath) {
    DataTypeVariantSerDe serde;
    const std::vector<std::string> documents {R"({"a":{"b":1},"c":[1,2]})", R"({"a":{"b":"x"}})",
                                              "", "7", R"("s")"};
    auto expected = load_through_json(serde, documents);
    for (const auto& type :
         {arrow::utf8(), arrow::large_utf8(), arrow::binary(), arrow::large_binary()}) {
        SCOPED_TRACE(type->ToString());
        std::vector<std::optional<std::string>> values(documents.begin(), documents.end());
        auto array = make_arrow_strings(type, values);
        auto column = ColumnVariant::create(0, false);
        ASSERT_TRUE(serde.read_column_from_arrow(*column, array.get(), 0, array->length(),
                                                 cctz::utc_time_zone())
                            .ok());
        ASSERT_EQ(column->size(), documents.size());
        for (size_t row = 0; row < documents.size(); ++row) {
            EXPECT_EQ(json_at(serde, *column, row), json_at(serde, *expected, row));
        }
    }
}

TEST(DataTypeVariantSerdeArrowTest, ArrowReadsOnlyTheRequestedRange) {
    DataTypeVariantSerDe serde;
    auto array = make_arrow_strings(arrow::utf8(), {R"({"skip":1})", R"({"k":"v"})", "3"});
    auto expected = load_through_json(serde, {R"({"k":"v"})", "3"});
    auto column = ColumnVariant::create(0, false);
    ASSERT_TRUE(
            serde.read_column_from_arrow(*column, array.get(), 1, 3, cctz::utc_time_zone()).ok());
    ASSERT_EQ(column->size(), 2);
    EXPECT_EQ(json_at(serde, *column, 0), json_at(serde, *expected, 0));
    EXPECT_EQ(json_at(serde, *column, 1), json_at(serde, *expected, 1));
}

TEST(DataTypeVariantSerdeArrowTest, ArrowFollowsTheInvalidJsonPolicy) {
    DataTypeVariantSerDe serde;
    auto array = make_arrow_strings(arrow::utf8(), {"not-json"});
    {
        ScopedInvalidJsonMode mode(false);
        auto expected = load_through_json(serde, {"not-json"});
        auto column = ColumnVariant::create(0, false);
        ASSERT_TRUE(serde.read_column_from_arrow(*column, array.get(), 0, 1, cctz::utc_time_zone())
                            .ok());
        ASSERT_EQ(column->size(), 1);
        EXPECT_EQ(json_at(serde, *column, 0), json_at(serde, *expected, 0));
    }
    {
        ScopedInvalidJsonMode mode(true);
        auto column = ColumnVariant::create(0, false);
        EXPECT_EQ(serde.read_column_from_arrow(*column, array.get(), 0, 1, cctz::utc_time_zone())
                          .code(),
                  ErrorCode::INVALID_ARGUMENT);
    }
}

TEST(DataTypeVariantSerdeArrowTest, ArrowRejectsNonStringArrays) {
    DataTypeVariantSerDe serde;
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Append(1).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto column = ColumnVariant::create(0, false);
    EXPECT_EQ(
            serde.read_column_from_arrow(*column, array.get(), 0, 1, cctz::utc_time_zone()).code(),
            ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), 0);
}

TEST(DataTypeVariantSerdeArrowTest, NullableArrowMapsArrowNullToSqlNull) {
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantSerDe>());
    auto array = make_arrow_strings(arrow::utf8(), {R"({"x":1})", std::nullopt, R"({"x":2})"});
    MutableColumnPtr nested = ColumnVariant::create(0, false);
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    ASSERT_TRUE(
            nullable_serde.read_column_from_arrow(*outer, array.get(), 0, 3, cctz::utc_time_zone())
                    .ok());
    ASSERT_EQ(outer->size(), 3);
    EXPECT_FALSE(outer->is_null_at(0));
    EXPECT_TRUE(outer->is_null_at(1));
    EXPECT_FALSE(outer->is_null_at(2));
    EXPECT_EQ(outer->get_nested_column().size(), 3);
}

TEST(DataTypeVariantSerdeArrowTest, ArrowWriteThenReadRoundTrips) {
    DataTypeVariantSerDe serde;
    auto source = load_through_json(serde, {R"({"a":[1,2],"b":{"c":"d"}})", R"({"a":[3]})", "5"});
    arrow::StringBuilder builder;
    ASSERT_TRUE(serde.write_column_to_arrow(*source, nullptr, &builder, 0, source->size(),
                                            cctz::utc_time_zone())
                        .ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    auto column = ColumnVariant::create(0, false);
    ASSERT_TRUE(serde.read_column_from_arrow(*column, array.get(), 0, array->length(),
                                             cctz::utc_time_zone())
                        .ok());
    ASSERT_EQ(column->size(), source->size());
    for (size_t row = 0; row < source->size(); ++row) {
        EXPECT_EQ(json_at(serde, *column, row), json_at(serde, *source, row));
    }
}

} // namespace doris
