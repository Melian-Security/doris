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

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "gen_cpp/types.pb.h"

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

Status deserialize_json(const DataTypeVariantV2SerDe& serde, ColumnVariantV2& column,
                        std::string_view text) {
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    return serde.deserialize_one_cell_from_json(column, slice, options);
}

Status deserialize_csv(const DataTypeVariantV2SerDe& serde, ColumnVariantV2& column,
                       std::string_view text) {
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    return serde.deserialize_one_cell_from_csv(column, slice, options);
}

std::string json_at(const DataTypeVariantV2SerDe& serde, const IColumn& column, size_t row) {
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

ColumnVariantV2::MutablePtr typed_int(int32_t value) {
    auto nested = ColumnInt32::create();
    nested->insert_value(value);
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(0);
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(nested), std::move(nulls)),
            std::make_shared<DataTypeInt32>());
}

} // namespace

TEST(DataTypeVariantV2SerdeInputTest, JsonUsesT15ValidEmptyAndInvalidPolicies) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    {
        ScopedInvalidJsonMode mode(false);
        EXPECT_TRUE(deserialize_json(serde, *column, R"({"b":[1,null]})").ok());
        EXPECT_TRUE(deserialize_json(serde, *column, {}).ok());
        EXPECT_TRUE(deserialize_json(serde, *column, "not-json").ok());
    }
    ASSERT_EQ(column->size(), 3);
    EXPECT_EQ(json_at(serde, *column, 0), R"({"b":[1,null]})");
    EXPECT_EQ(json_at(serde, *column, 1), "{}");
    EXPECT_EQ(json_at(serde, *column, 2), R"("not-json")");

    const size_t before = column->size();
    {
        ScopedInvalidJsonMode mode(true);
        EXPECT_EQ(deserialize_json(serde, *column, "not-json").code(), ErrorCode::INVALID_ARGUMENT);
    }
    EXPECT_EQ(column->size(), before);

    const std::string invalid_utf8("\xC3\x28", 2);
    {
        ScopedInvalidJsonMode mode(false);
        EXPECT_EQ(deserialize_json(serde, *column, invalid_utf8).code(),
                  ErrorCode::INVALID_ARGUMENT);
    }
    EXPECT_EQ(column->size(), before);
}

TEST(DataTypeVariantV2SerdeInputTest, VectorFailureIsAtomicAndCounterIsNotAdvanced) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_json(serde, *column, R"({"sentinel":1})").ok());
    const size_t before = column->size();

    std::string valid = R"({"a":1})";
    std::string invalid = "{";
    std::vector<Slice> slices {{valid.data(), valid.size()}, {invalid.data(), invalid.size()}};
    uint64_t num_deserialized = 0;
    DataTypeSerDe::FormatOptions options;
    ScopedInvalidJsonMode mode(true);
    const Status status =
            serde.deserialize_column_from_json_vector(*column, slices, &num_deserialized, options);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), before);
    EXPECT_EQ(num_deserialized, 0);
    EXPECT_EQ(json_at(serde, *column, 0), R"({"sentinel":1})");
}

TEST(DataTypeVariantV2SerdeInputTest, EmptyVectorAndCounterOverflowDoNotMutateState) {
    DataTypeVariantV2SerDe serde;
    auto column = typed_int(1);
    DataTypeSerDe::FormatOptions options;
    std::vector<Slice> empty;
    uint64_t counter = 7;
    EXPECT_TRUE(serde.deserialize_column_from_json_vector(*column, empty, &counter, options).ok());
    EXPECT_EQ(counter, 7);
    EXPECT_EQ(column->size(), 1);
    EXPECT_TRUE(column->is_typed());

    std::string value = "2";
    std::vector<Slice> one {{value.data(), value.size()}};
    counter = std::numeric_limits<uint64_t>::max();
    EXPECT_EQ(serde.deserialize_column_from_json_vector(*column, one, &counter, options).code(),
              ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(counter, std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(column->size(), 1);
    EXPECT_TRUE(column->is_typed());
    EXPECT_EQ(json_at(serde, *column, 0), "1");
}

TEST(DataTypeVariantV2SerdeInputTest, SuccessfulVectorAdvancesExistingCounter) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    std::string first = "1";
    std::string second = R"({"a":2})";
    std::vector<Slice> slices {{first.data(), first.size()}, {second.data(), second.size()}};
    uint64_t counter = 5;
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(serde.deserialize_column_from_json_vector(*column, slices, &counter, options).ok());
    EXPECT_EQ(counter, 7);
    ASSERT_EQ(column->size(), 2);
    EXPECT_EQ(json_at(serde, *column, 0), "1");
    EXPECT_EQ(json_at(serde, *column, 1), R"({"a":2})");
}

TEST(DataTypeVariantV2SerdeInputTest, CsvIsAlwaysRawStringScalar) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_csv(serde, *column, R"({"a":1})").ok());
    ASSERT_TRUE(deserialize_csv(serde, *column, "null").ok());
    EXPECT_EQ(json_at(serde, *column, 0), R"("{\"a\":1}")");
    EXPECT_EQ(json_at(serde, *column, 1), R"("null")");
}

TEST(DataTypeVariantV2SerdeInputTest, CsvUnescapesStringScalarBeforeEncoding) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    std::string text = "left,right\"\"quoted\\\\path\nnext";
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    options.quote_char = '"';
    ASSERT_TRUE(serde.deserialize_one_cell_from_csv(*column, slice, options).ok());
    ASSERT_EQ(column->size(), 1);
    EXPECT_EQ(json_at(serde, *column, 0), R"("left,right\"quoted\\path\nnext")");
}

TEST(DataTypeVariantV2SerdeInputTest, NullableCsvPreservesSqlNullAndRawStringScalar) {
    DataTypeVariantV2SerDe serde;
    MutableColumnPtr nested = ColumnVariantV2::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    DataTypeSerDe::FormatOptions options;
    options.null_format = "NULL";
    options.null_len = 4;

    std::string sql_null = "NULL";
    Slice sql_null_slice(sql_null.data(), sql_null.size());
    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, sql_null_slice, options).ok());

    std::string raw_null = "null";
    Slice raw_null_slice(raw_null.data(), raw_null.size());
    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, raw_null_slice, options).ok());

    options.converted_from_string = true;
    std::string quoted_null_format = R"("NULL")";
    Slice quoted_null_format_slice(quoted_null_format.data(), quoted_null_format.size());
    ASSERT_TRUE(
            nullable_serde.deserialize_one_cell_from_csv(*outer, quoted_null_format_slice, options)
                    .ok());

    ASSERT_EQ(outer->size(), 3);
    EXPECT_TRUE(outer->is_null_at(0));
    EXPECT_FALSE(outer->is_null_at(1));
    EXPECT_FALSE(outer->is_null_at(2));
    EXPECT_EQ(json_at(serde, outer->get_nested_column(), 1), R"("null")");
    EXPECT_EQ(json_at(serde, outer->get_nested_column(), 2), R"("NULL")");
}

TEST(DataTypeVariantV2SerdeInputTest, NullableCsvNestedFailureAppendsOneSqlNull) {
    MutableColumnPtr nested = ColumnVariantV2::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    Slice invalid(static_cast<char*>(nullptr), 1);
    DataTypeSerDe::FormatOptions options;
    options.null_len = 0;
    options.escape_char = '\\';

    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, invalid, options).ok());
    ASSERT_EQ(outer->size(), 1);
    EXPECT_TRUE(outer->is_null_at(0));
    EXPECT_EQ(outer->get_nested_column().size(), 1);
}

TEST(DataTypeVariantV2SerdeInputTest, NullableCsvDispatchesForNonVariantNestedSerde) {
    MutableColumnPtr nested = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeStringSerDe>(TYPE_STRING));
    std::string text = R"(left""right\\tail)";
    Slice slice(text.data(), text.size());
    DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    options.quote_char = '"';

    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_csv(*outer, slice, options).ok());
    ASSERT_EQ(outer->size(), 1);
    EXPECT_FALSE(outer->is_null_at(0));
    EXPECT_EQ(assert_cast<const ColumnString&>(outer->get_nested_column()).get_data_at(0),
              StringRef("left\"right\\tail"));
}

TEST(DataTypeVariantV2SerdeInputTest, InvalidCsvInputDoesNotPublishARow) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_csv(serde, *column, "sentinel").ok());
    const size_t before = column->size();
    Slice invalid(static_cast<char*>(nullptr), 1);
    DataTypeSerDe::FormatOptions options;
    options.escape_char = '\\';
    EXPECT_EQ(serde.deserialize_one_cell_from_csv(*column, invalid, options).code(),
              ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), before);
    EXPECT_EQ(json_at(serde, *column, 0), R"("sentinel")");
}

TEST(DataTypeVariantV2SerdeInputTest, OuterSqlNullAndVariantNullRemainDistinct) {
    DataTypeVariantV2SerDe serde;
    auto variant_null = ColumnVariantV2::create();
    ASSERT_TRUE(deserialize_json(serde, *variant_null, "null").ok());
    EXPECT_EQ(json_at(serde, *variant_null, 0), "null");

    MutableColumnPtr nested = ColumnVariantV2::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    std::string sql_null = "\\N";
    Slice slice(sql_null.data(), sql_null.size());
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(nullable_serde.deserialize_one_cell_from_json(*outer, slice, options).ok());
    ASSERT_EQ(outer->size(), 1);
    EXPECT_TRUE(outer->is_null_at(0));
}

TEST(DataTypeVariantV2SerdeInputTest, PbKeepsExplicitGuardsAndNullArrowInputIsRejected) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    PValues values;
    EXPECT_EQ(serde.write_column_to_pb(*column, values, 0, 0).code(),
              ErrorCode::NOT_IMPLEMENTED_ERROR);
    EXPECT_EQ(serde.read_column_from_pb(*column, values).code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
    EXPECT_EQ(serde.read_column_from_arrow(*column, nullptr, 0, 0, cctz::utc_time_zone()).code(),
              ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), 0);
}

TEST(DataTypeVariantV2SerdeInputTest, ArrowStringTypesParseOneJsonDocumentPerRow) {
    DataTypeVariantV2SerDe serde;
    const std::vector<std::string> expected {R"({"a":{"b":1}})", "[1,2]", "{}", "null"};
    for (const auto& type :
         {arrow::utf8(), arrow::large_utf8(), arrow::binary(), arrow::large_binary()}) {
        SCOPED_TRACE(type->ToString());
        auto array = make_arrow_strings(type, {R"({"a":{"b":1}})", "[1,2]", "", std::nullopt});
        auto column = ColumnVariantV2::create();
        ASSERT_TRUE(serde.read_column_from_arrow(*column, array.get(), 0, array->length(),
                                                 cctz::utc_time_zone())
                            .ok());
        ASSERT_EQ(column->size(), expected.size());
        for (size_t row = 0; row < expected.size(); ++row) {
            EXPECT_EQ(json_at(serde, *column, row), expected[row]);
        }
    }
}

TEST(DataTypeVariantV2SerdeInputTest, ArrowReadsOnlyTheRequestedRange) {
    DataTypeVariantV2SerDe serde;
    auto array = make_arrow_strings(arrow::utf8(), {"1", R"({"k":"v"})", "3"});
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(
            serde.read_column_from_arrow(*column, array.get(), 1, 3, cctz::utc_time_zone()).ok());
    ASSERT_EQ(column->size(), 2);
    EXPECT_EQ(json_at(serde, *column, 0), R"({"k":"v"})");
    EXPECT_EQ(json_at(serde, *column, 1), "3");

    EXPECT_TRUE(
            serde.read_column_from_arrow(*column, array.get(), 2, 2, cctz::utc_time_zone()).ok());
    EXPECT_EQ(column->size(), 2);
}

TEST(DataTypeVariantV2SerdeInputTest, ArrowFollowsJsonInvalidPolicyAndFailureIsAtomic) {
    DataTypeVariantV2SerDe serde;
    auto array = make_arrow_strings(arrow::utf8(), {R"({"a":1})", "not-json"});
    auto column = ColumnVariantV2::create();
    {
        ScopedInvalidJsonMode mode(false);
        ASSERT_TRUE(serde.read_column_from_arrow(*column, array.get(), 0, 2, cctz::utc_time_zone())
                            .ok());
    }
    ASSERT_EQ(column->size(), 2);
    EXPECT_EQ(json_at(serde, *column, 1), R"("not-json")");

    const size_t before = column->size();
    {
        ScopedInvalidJsonMode mode(true);
        EXPECT_EQ(serde.read_column_from_arrow(*column, array.get(), 0, 2, cctz::utc_time_zone())
                          .code(),
                  ErrorCode::INVALID_ARGUMENT);
    }
    EXPECT_EQ(column->size(), before);
}

TEST(DataTypeVariantV2SerdeInputTest, ArrowRejectsNonStringArrays) {
    DataTypeVariantV2SerDe serde;
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Append(1).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto column = ColumnVariantV2::create();
    EXPECT_EQ(
            serde.read_column_from_arrow(*column, array.get(), 0, 1, cctz::utc_time_zone()).code(),
            ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), 0);
}

TEST(DataTypeVariantV2SerdeInputTest, NullableArrowKeepsSqlNullDistinctFromJsonNull) {
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    auto array = make_arrow_strings(arrow::utf8(), {"null", std::nullopt, R"({"x":true})"});
    MutableColumnPtr nested = ColumnVariantV2::create();
    auto null_map = ColumnUInt8::create();
    auto outer = ColumnNullable::create(std::move(nested), std::move(null_map));
    ASSERT_TRUE(
            nullable_serde.read_column_from_arrow(*outer, array.get(), 0, 3, cctz::utc_time_zone())
                    .ok());
    ASSERT_EQ(outer->size(), 3);
    EXPECT_FALSE(outer->is_null_at(0));
    EXPECT_TRUE(outer->is_null_at(1));
    EXPECT_FALSE(outer->is_null_at(2));
    DataTypeVariantV2SerDe serde;
    EXPECT_EQ(json_at(serde, outer->get_nested_column(), 0), "null");
    EXPECT_EQ(json_at(serde, outer->get_nested_column(), 2), R"({"x":true})");
}

TEST(DataTypeVariantV2SerdeInputTest, ArrowWriteThenReadRoundTrips) {
    DataTypeVariantV2SerDe serde;
    auto source = ColumnVariantV2::create();
    for (std::string_view json : {R"({"a":[1,{"b":"c"}],"d":null})", "42", R"("s")", "[]"}) {
        ASSERT_TRUE(deserialize_json(serde, *source, json).ok());
    }
    arrow::StringBuilder builder;
    ASSERT_TRUE(serde.write_column_to_arrow(*source, nullptr, &builder, 0, source->size(),
                                            cctz::utc_time_zone())
                        .ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(serde.read_column_from_arrow(*column, array.get(), 0, array->length(),
                                             cctz::utc_time_zone())
                        .ok());
    ASSERT_EQ(column->size(), source->size());
    for (size_t row = 0; row < source->size(); ++row) {
        EXPECT_EQ(json_at(serde, *column, row), json_at(serde, *source, row));
    }
}

// The tablet sink distributes a loaded block across tablets with insert_indices_from on the
// nullable Variant column, so a multi-batch Arrow read must leave a column every row of which
// can be selected in any order. The documents vary their paths and value types row by row.
TEST(DataTypeVariantV2SerdeInputTest, MultiBatchArrowReadSurvivesSinkRowSelection) {
    DataTypeVariantV2SerDe serde;
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    constexpr size_t num_rows = 10000;
    constexpr size_t batch_rows = 4096;
    std::vector<std::optional<std::string>> documents;
    documents.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        if (i % 11 == 0) {
            documents.emplace_back(std::nullopt);
            continue;
        }
        std::string doc = "{";
        doc += i % 7 == 0 ? "\"a\":\"text_" + std::to_string(i) + "\""
                          : "\"a\":" + std::to_string(i);
        if (i % 3 == 0) {
            doc += ",\"s\":\"" + std::string(i % 50, 'x') + "\"";
        }
        if (i % 5 == 0) {
            doc += ",\"n\":{\"x\":" + std::to_string(i) + ",\"y\":\"v" + std::to_string(i) + "\"}";
        }
        if (i > 8000) {
            doc += ",\"late\":true";
        }
        doc += "}";
        documents.emplace_back(std::move(doc));
    }

    MutableColumnPtr nested = ColumnVariantV2::create();
    auto outer = ColumnNullable::create(std::move(nested), ColumnUInt8::create());
    for (size_t begin = 0; begin < num_rows; begin += batch_rows) {
        const size_t end = std::min(num_rows, begin + batch_rows);
        std::vector<std::optional<std::string>> batch(documents.begin() + begin,
                                                      documents.begin() + end);
        auto array = make_arrow_strings(arrow::utf8(), batch);
        ASSERT_TRUE(nullable_serde
                            .read_column_from_arrow(*outer, array.get(), 0, end - begin,
                                                    cctz::utc_time_zone())
                            .ok());
    }
    ASSERT_EQ(outer->size(), num_rows);
    ASSERT_EQ(outer->get_nested_column().size(), num_rows);

    std::vector<std::string> expected(num_rows);
    for (size_t row = 0; row < num_rows; ++row) {
        if (documents[row].has_value()) {
            expected[row] = json_at(serde, outer->get_nested_column(), row);
        }
    }

    std::vector<uint32_t> selector(num_rows);
    std::iota(selector.begin(), selector.end(), 0);
    std::mt19937 rng(42);
    std::shuffle(selector.begin(), selector.end(), rng);
    auto target = outer->clone_empty();
    target->insert_indices_from(*outer, selector.data(), selector.data() + selector.size());
    ASSERT_EQ(target->size(), num_rows);
    const auto& target_nullable = assert_cast<const ColumnNullable&>(*target);
    for (size_t k = 0; k < num_rows; ++k) {
        const size_t row = selector[k];
        ASSERT_EQ(target_nullable.is_null_at(k), !documents[row].has_value()) << row;
        if (documents[row].has_value()) {
            ASSERT_EQ(json_at(serde, target_nullable.get_nested_column(), k), expected[row]) << row;
        }
    }
}

namespace {

class ScopedBoundedColumnGrowth {
public:
    explicit ScopedBoundedColumnGrowth(bool value)
            : _old(config::variant_v2_bounded_column_growth) {
        config::variant_v2_bounded_column_growth = value;
    }
    ~ScopedBoundedColumnGrowth() { config::variant_v2_bounded_column_growth = _old; }

private:
    bool _old;
};

// Log-shaped documents of about 600 bytes, so a few thousand rows cross the 1 MB threshold where
// bounded growth starts.
std::vector<std::optional<std::string>> log_documents(size_t rows, uint32_t seed) {
    std::mt19937 rng(seed);
    std::vector<std::optional<std::string>> documents;
    documents.reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (rng() % 50 == 0) {
            documents.emplace_back(std::nullopt);
            continue;
        }
        std::string document =
                R"({"time":)" + std::to_string(1760000000000 + row) + R"(,"severity_id":)" +
                std::to_string(rng() % 6) + R"(,"message":"request )" + std::to_string(rng()) +
                R"( served","src":{"ip":"10.0.)" + std::to_string(rng() % 256) + R"(.7","port":)" +
                std::to_string(rng() % 65536) + R"(},"labels":["edge","waf"],"unmapped":{)";
        for (int field = 0; field < 12; ++field) {
            document += (field == 0 ? "" : ",");
            document += R"("vendor_field_)" + std::to_string(rng() % 40) + "_" +
                        std::to_string(field) + R"(":"value-)" + std::to_string(rng()) + R"(")";
        }
        document += "}}";
        documents.emplace_back(std::move(document));
    }
    return documents;
}

ColumnVariantV2::MutablePtr read_arrow_batch(const std::vector<std::optional<std::string>>& docs) {
    DataTypeVariantV2SerDe serde;
    auto array = make_arrow_strings(arrow::utf8(), docs);
    auto column = ColumnVariantV2::create();
    const Status status = serde.read_column_from_arrow(*column, array.get(), 0, array->length(),
                                                       cctz::utc_time_zone());
    EXPECT_TRUE(status.ok()) << status;
    return column;
}

void expect_same_rows(const ColumnVariantV2& actual, const ColumnVariantV2& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t row = 0; row < actual.size(); ++row) {
        const VariantRef left = actual.get_value_ref(row);
        const VariantRef right = expected.get_value_ref(row);
        ASSERT_EQ(StringRef(left.metadata.data, left.metadata.size),
                  StringRef(right.metadata.data, right.metadata.size))
                << row;
        ASSERT_EQ(left.value, right.value) << row;
    }
}

} // namespace

TEST(DataTypeVariantV2SerdeArrowMemoryTest, ArrowBatchFillsAnEmptyColumnExactly) {
    const auto documents = log_documents(4096, 7);
    ColumnVariantV2::MutablePtr bounded;
    {
        ScopedBoundedColumnGrowth growth(true);
        bounded = read_arrow_batch(documents);
    }
    ColumnVariantV2::MutablePtr power_of_two;
    {
        ScopedBoundedColumnGrowth growth(false);
        power_of_two = read_arrow_batch(documents);
    }
    ASSERT_GT(bounded->byte_size(), size_t {1} << 20);
    expect_same_rows(*bounded, *power_of_two);
    // Only PODArray padding and the batch metadata dictionary sit above the encoded bytes.
    EXPECT_LE(bounded->allocated_bytes(), bounded->byte_size() + (64U << 10));
    EXPECT_LE(bounded->allocated_bytes(), power_of_two->allocated_bytes());
}

TEST(DataTypeVariantV2SerdeArrowMemoryTest, MemtableStyleAppendsGrowByAQuarter) {
    // MemTable::insert gathers each sink batch into one column with insert_indices_from, and the
    // flush gathers that column once more into sorted order.
    auto run = [](bool bounded_growth, size_t* max_allocated_over_bytes_permille) {
        ScopedBoundedColumnGrowth growth(bounded_growth);
        auto memtable = ColumnVariantV2::create();
        *max_allocated_over_bytes_permille = 0;
        for (uint32_t batch = 0; batch < 24; ++batch) {
            auto source = read_arrow_batch(log_documents(1024, batch));
            std::vector<uint32_t> rows(source->size());
            std::iota(rows.begin(), rows.end(), 0);
            memtable->insert_indices_from(*source, rows.data(), rows.data() + rows.size());
            if (memtable->byte_size() > (size_t {4} << 20)) {
                *max_allocated_over_bytes_permille =
                        std::max(*max_allocated_over_bytes_permille,
                                 memtable->allocated_bytes() * 1000 / memtable->byte_size());
            }
        }
        std::vector<uint32_t> sorted(memtable->size());
        std::iota(sorted.rbegin(), sorted.rend(), 0);
        auto flushed = ColumnVariantV2::create();
        flushed->insert_indices_from(*memtable, sorted.data(), sorted.data() + sorted.size());
        return std::make_pair(std::move(memtable), std::move(flushed));
    };
    size_t bounded_permille = 0;
    size_t power_of_two_permille = 0;
    auto [bounded, bounded_flushed] = run(true, &bounded_permille);
    auto [power_of_two, power_of_two_flushed] = run(false, &power_of_two_permille);

    expect_same_rows(*bounded, *power_of_two);
    expect_same_rows(*bounded_flushed, *power_of_two_flushed);
    EXPECT_LE(bounded_permille, 1260);
    EXPECT_LT(bounded_permille, power_of_two_permille);
    EXPECT_LE(bounded_flushed->allocated_bytes(),
              bounded_flushed->byte_size() + bounded_flushed->byte_size() / 50);
}

} // namespace doris
