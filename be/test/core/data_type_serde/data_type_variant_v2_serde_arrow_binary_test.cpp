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

// Stream Load reads a Variant column from Arrow either as JSON text or as Parquet Variant binary
// in struct<metadata: binary, value: binary>. These tests pin that both shapes load the same
// Variant for the same document.
//
// JSON to Variant typing rules of JsonStringToVariantEncoder, which a binary producer must follow
// to store what a JSON text load stores. JsonEncoderTypingRules pins the bytes.
//
// Numbers (exprs/function/parse/variant_string_parse.cpp JsonTreeCollector::collect). simdjson
// classifies each number; the encoder never emits FLOAT or DECIMAL4/8, and never a decimal for a
// number written with a fraction or an exponent.
// - Integer literal that fits int64: the narrowest of INT8 (id 3), INT16 (4), INT32 (5), INT64 (6)
//   holding it (core/value/variant/variant_scalar.cpp minimum_integer_width). -0 is INT8 0.
// - Integer literal in (INT64_MAX, UINT64_MAX]: DECIMAL16 (id 10) with scale 0
//   (VariantBatchBuilder::Row::add_largeint): header 0x28, scale byte 0x00, 16-byte little-endian
//   two's-complement unscaled value.
// - Any number with a fraction or an exponent: DOUBLE (id 7), the IEEE-754 bits of simdjson's
//   correctly rounded parse, 8 bytes little-endian. -0.0 keeps its sign bit. 1.0 stays DOUBLE.
// - An integer literal outside [INT64_MIN, UINT64_MAX], or a number that overflows a double
//   (1e400): simdjson rejects the document, so the WHOLE document, wherever the number sits,
//   is stored as one string holding the raw input text (the invalid-JSON fallback below).
// Other scalars: null 0x00, true 0x04, false 0x08. A string of at most 63 UTF-8 bytes is a short
// string, header (length << 2) | 1, else primitive STRING (id 16), header 0x40, then a 4-byte
// little-endian length (variant_scalar.cpp VariantScalarRef::write_physical). Escapes are decoded;
// strings are not normalized.
//
// Containers (core/value/variant/variant_batch_builder.cpp plan_node / write_node). Integer fields
// use the narrowest of 1, 2, 3 or 4 bytes (variant_parquet_encoding.h
// variant_minimum_unsigned_width).
// - Object: header 0x02 | (offset_width - 1) << 2 | (id_width - 1) << 4 | is_large << 6.
//   is_large when it has more than 255 fields (then a 4-byte count). Fields are ordered by key
//   bytes, which is also field-id order; values are written in that same order. id_width covers
//   the largest field id used; offset_width covers the total child value bytes. A repeated key
//   is an error ("Duplicate Variant object key"), unless
//   variant_enable_duplicate_json_path_check is on and the first occurrence wins.
//   {} is 02 00 00.
// - Array: header 0x03 | (offset_width - 1) << 2 | is_large << 4, elements in JSON order.
//   [] is 03 00 00.
//
// Metadata (VariantMetadataBuilder::seal): one dictionary per encoder batch, not per row. It holds
// every key of every row of the batch, deduplicated and sorted by bytes, with the sorted_strings
// bit always set: header 0x11 | (offset_width - 1) << 6, where offset_width covers
// max(key count, total key bytes); then the count, count + 1 offsets and the key bytes. A batch
// with no keys has 11 00 00. Because field ids and id_width come from the batch dictionary, a
// value's bytes depend on the other rows of its batch; per-row dictionaries are equally valid and
// load as the same Variant.
//
// Inputs (JsonStringToVariantEncoder add_json_row): an empty document is {}; an unparsable one is
// the raw text as a string, or an error when variant_throw_exeception_on_invalid_json is set.
// Nesting is limited to 128 levels and keys to variant_max_json_key_length (255) bytes.

#include <arrow/array/array_nested.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/extension_type.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_canonical.h"
#include "exprs/function/parse/variant_string_parse.h"

namespace doris {
namespace {

using EncodedRow = std::pair<std::string, std::string>; // metadata, value
using Names = std::vector<std::string>;

// Documents that exercise every JSON-to-Variant typing branch: integer widths and their edges,
// integers past int64, decimals, exponents, doubles, strings on both sides of the short-string
// limit, escapes, and object key ordering.
const std::vector<std::string>& tricky_documents() {
    static const std::vector<std::string> documents = {
            R"({"b":1,"a":{"z":[1,2,{"k":null}],"y":"s"},"c":[true,false,null]})",
            "{}",
            "[]",
            "[[],{},[{}]]",
            "0",
            "-0",
            "1",
            "-1",
            "127",
            "128",
            "-128",
            "-129",
            "32767",
            "32768",
            "-32768",
            "-32769",
            "2147483647",
            "2147483648",
            "-2147483648",
            "-2147483649",
            "9223372036854775807",
            "9223372036854775808",
            "-9223372036854775808",
            "-9223372036854775809",
            "18446744073709551615",
            "18446744073709551616",
            "123456789012345678901234567890",
            "-99999999999999999999999999999999999999",
            "100000000000000000000000000000000000000",
            "1.5",
            "-0.0",
            "0.1",
            "1.0",
            "100.000",
            "0.000001",
            "123456789.123456789",
            "3.14159265358979323846264338327950288",
            "1e3",
            "1E-2",
            "-2.5e+10",
            "6.02214076e23",
            "1.7976931348623157e308",
            "5e-324",
            "true",
            "false",
            "null",
            R"("")",
            R"("short")",
            R"("a string that is longer than sixty-three bytes, so it is a long primitive string")",
            R"("é中😀 \"q\" \\ \/ \b\f\n\r\t é中😀")",
            R"({"":0,"é":1,"A":2,"a":3,"aa":4,"b":{"a":[1.25,-3,"x"]}})",
            R"({"n":{"n":{"n":{"n":{"n":[[[[1]]]]}}}}})",
            R"([1,-1,300,-70000,5000000000,1.5,1e2,"s",null,true,{"k":"v"}])",
    };
    return documents;
}

std::string json_at(const IColumn& column, size_t row) {
    DataTypeVariantV2SerDe serde;
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

EncodedRow row_bytes(const ColumnVariantV2& column, size_t row) {
    const VariantRef ref = column.get_value_ref(row);
    return {std::string(ref.metadata.data, ref.metadata.size),
            std::string(ref.value.data, ref.value.size)};
}

std::shared_ptr<arrow::Array> make_utf8(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

// The JSON text load path: one encoder batch, so every row shares one metadata dictionary.
ColumnVariantV2::MutablePtr read_json_text(const std::vector<std::string>& documents) {
    DataTypeVariantV2SerDe serde;
    auto column = ColumnVariantV2::create();
    auto array = make_utf8(documents);
    const Status status = serde.read_column_from_arrow(*column, array.get(), 0, array->length(),
                                                       cctz::utc_time_zone());
    EXPECT_TRUE(status.ok()) << status;
    return column;
}

// Each document encoded on its own, so each row carries its own minimal dictionary.
ColumnVariantV2::MutablePtr encode_per_row(const std::vector<std::string>& documents) {
    auto column = ColumnVariantV2::create();
    for (const auto& document : documents) {
        JsonStringToVariantEncoder encoder;
        encoder.add_json({document.data(), document.size()});
        VariantBatchBuilder block = encoder.finish_batch();
        column->insert_encoded_batch(block);
    }
    return column;
}

EncodedRow encode_one(std::string_view document) {
    auto column = encode_per_row({std::string(document)});
    return row_bytes(*column, 0);
}

// Emits a column through the existing binary Variant Arrow writers. value_first selects
// struct<value, metadata> (the Paimon writer) or struct<metadata, value> (the Iceberg writer).
std::shared_ptr<arrow::Array> write_binary_struct(const ColumnVariantV2& column, bool value_first) {
    auto type = value_first ? arrow::struct_({arrow::field("value", arrow::binary()),
                                              arrow::field("metadata", arrow::binary())})
                            : arrow::struct_({arrow::field("metadata", arrow::binary()),
                                              arrow::field("value", arrow::binary())});
    std::unique_ptr<arrow::ArrayBuilder> builder;
    EXPECT_TRUE(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder).ok());
    DataTypeVariantV2SerDe serde;
    const Status status = serde.write_column_to_arrow(column, nullptr, builder.get(), 0,
                                                      column.size(), cctz::utc_time_zone());
    EXPECT_TRUE(status.ok()) << status;
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder->Finish(&array).ok());
    return array;
}

struct StructShape {
    bool metadata_first = true;
    bool large_metadata = false;
    bool large_value = false;
};

std::shared_ptr<arrow::Array> make_child(bool large, const std::vector<std::string>& values) {
    std::shared_ptr<arrow::Array> array;
    if (large) {
        arrow::LargeBinaryBuilder builder;
        for (const auto& value : values) {
            EXPECT_TRUE(builder.Append(value).ok());
        }
        EXPECT_TRUE(builder.Finish(&array).ok());
    } else {
        arrow::BinaryBuilder builder;
        for (const auto& value : values) {
            EXPECT_TRUE(builder.Append(value).ok());
        }
        EXPECT_TRUE(builder.Finish(&array).ok());
    }
    return array;
}

// Builds struct<metadata, value> by hand. A nullopt row is a null struct row whose child slots hold
// junk bytes, which the reader must ignore.
std::shared_ptr<arrow::StructArray> make_binary_struct(const std::vector<std::optional<EncodedRow>>& rows,
                                                       StructShape shape = {}) {
    std::vector<std::string> metadatas;
    std::vector<std::string> values;
    int64_t null_count = 0;
    std::shared_ptr<arrow::Buffer> bitmap =
            arrow::AllocateEmptyBitmap(static_cast<int64_t>(rows.size())).ValueOrDie();
    for (size_t row = 0; row < rows.size(); ++row) {
        if (rows[row].has_value()) {
            arrow::bit_util::SetBit(bitmap->mutable_data(), static_cast<int64_t>(row));
            metadatas.push_back(rows[row]->first);
            values.push_back(rows[row]->second);
        } else {
            ++null_count;
            metadatas.emplace_back("\xff\xfejunk");
            values.emplace_back("\x7f\x7f\x7f");
        }
    }
    auto metadata = make_child(shape.large_metadata, metadatas);
    auto value = make_child(shape.large_value, values);
    arrow::ArrayVector children = shape.metadata_first ? arrow::ArrayVector {metadata, value}
                                                       : arrow::ArrayVector {value, metadata};
    std::vector<std::string> names = shape.metadata_first
                                             ? std::vector<std::string> {"metadata", "value"}
                                             : std::vector<std::string> {"value", "metadata"};
    return arrow::StructArray::Make(children, names, null_count == 0 ? nullptr : bitmap,
                                    null_count)
            .ValueOrDie();
}

ColumnNullable::MutablePtr make_nullable_variant_column() {
    MutableColumnPtr nested = ColumnVariantV2::create();
    MutableColumnPtr null_map = ColumnUInt8::create();
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

Status read_arrow(IColumn& column, const arrow::Array& array, int64_t start, int64_t end) {
    DataTypeVariantV2SerDe serde;
    return serde.read_column_from_arrow(column, &array, start, end, cctz::utc_time_zone());
}

void expect_same_rows(const ColumnVariantV2& expected, size_t expected_start,
                      const ColumnVariantV2& actual, size_t actual_start, size_t rows) {
    for (size_t row = 0; row < rows; ++row) {
        const VariantRef left = expected.get_value_ref(expected_start + row);
        const VariantRef right = actual.get_value_ref(actual_start + row);
        EXPECT_TRUE(canonical_equals(left, right)) << "row " << row;
        EXPECT_EQ(json_at(actual, actual_start + row), json_at(expected, expected_start + row))
                << "row " << row;
    }
}

// A minimal stand-in for the canonical extension type, which this Arrow build need not register.
class TestParquetVariantType : public arrow::ExtensionType {
public:
    explicit TestParquetVariantType(std::shared_ptr<arrow::DataType> storage, std::string name)
            : arrow::ExtensionType(std::move(storage)), _name(std::move(name)) {}
    std::string extension_name() const override { return _name; }
    bool ExtensionEquals(const arrow::ExtensionType& other) const override {
        return other.extension_name() == extension_name() &&
               other.storage_type()->Equals(*storage_type());
    }
    std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const override {
        return std::make_shared<arrow::ExtensionArray>(data);
    }
    arrow::Result<std::shared_ptr<arrow::DataType>> Deserialize(
            std::shared_ptr<arrow::DataType> storage, const std::string&) const override {
        return std::make_shared<TestParquetVariantType>(std::move(storage), _name);
    }
    std::string Serialize() const override { return ""; }

private:
    std::string _name;
};

} // namespace

// Rows emitted from the JSON path's shared dictionary come back through the compact
// single-dictionary form with byte-identical rows.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, JsonTextAndBinaryStructMatchWithSharedMetadata) {
    const auto& documents = tricky_documents();
    auto json_column = read_json_text(documents);
    ASSERT_EQ(json_column->size(), documents.size());
    ASSERT_EQ(json_column->read_view().metadata_count(), 1);

    for (bool value_first : {true, false}) {
        auto array = write_binary_struct(*json_column, value_first);
        auto binary_column = ColumnVariantV2::create();
        {
            const Status status = read_arrow(*binary_column, *array, 0, array->length());
            ASSERT_TRUE(status.ok()) << status;
        }
        ASSERT_EQ(binary_column->size(), documents.size());
        EXPECT_EQ(binary_column->read_view().metadata_count(), 1);
        expect_same_rows(*json_column, 0, *binary_column, 0, documents.size());
        for (size_t row = 0; row < documents.size(); ++row) {
            EXPECT_EQ(row_bytes(*binary_column, row), row_bytes(*json_column, row)) << row;
        }
    }
}

// Per-row dictionaries take the appender path; every row keeps its own bytes and reads back as the
// JSON text path's value for the same document.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, JsonTextAndBinaryStructMatchWithPerRowMetadata) {
    const auto& documents = tricky_documents();
    auto json_column = read_json_text(documents);
    auto per_row = encode_per_row(documents);
    ASSERT_GT(per_row->read_view().metadata_count(), 1);

    for (bool value_first : {true, false}) {
        auto array = write_binary_struct(*per_row, value_first);
        auto binary_column = ColumnVariantV2::create();
        {
            const Status status = read_arrow(*binary_column, *array, 0, array->length());
            ASSERT_TRUE(status.ok()) << status;
        }
        ASSERT_EQ(binary_column->size(), documents.size());
        EXPECT_EQ(binary_column->read_view().metadata_count(),
                  per_row->read_view().metadata_count());
        expect_same_rows(*json_column, 0, *binary_column, 0, documents.size());
        for (size_t row = 0; row < documents.size(); ++row) {
            EXPECT_EQ(row_bytes(*binary_column, row), row_bytes(*per_row, row)) << row;
        }
    }
}

// The JSON text path rejects an object with a repeated key unless
// variant_enable_duplicate_json_path_check is on (then the first occurrence wins), so a binary
// producer must never emit one: Variant validation rejects repeated keys too.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, DuplicateKeysAreRejectedOnBothPaths) {
    auto text = make_utf8({R"({"dup":1,"dup":2})"});
    auto column = ColumnVariantV2::create();
    EXPECT_FALSE(read_arrow(*column, *text, 0, 1).ok());
    EXPECT_EQ(column->size(), 0);

    // {"dup":1,"dup":2} encoded by hand against a one-key dictionary.
    const std::string metadata("\x11\x01\x00\x03dup", 7);
    const std::string value("\x02\x02\x00\x00\x00\x02\x04\x0c\x01\x0c\x02", 11);
    auto array = make_binary_struct({EncodedRow {metadata, value}});
    EXPECT_FALSE(read_arrow(*column, *array, 0, 1).ok());
    EXPECT_EQ(column->size(), 0);
}

// Pins the exact bytes JsonStringToVariantEncoder writes for one document per batch, so a binary
// producer can reproduce them. See the typing rules at the top of this file.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, JsonEncoderTypingRules) {
    auto hex = [](std::string_view bytes) {
        static constexpr char digits[] = "0123456789abcdef";
        std::string out;
        for (unsigned char byte : bytes) {
            out.push_back(digits[byte >> 4]);
            out.push_back(digits[byte & 0x0f]);
        }
        return out;
    };
    const std::string empty_metadata = "110000";
    // simdjson rejects an integer outside [INT64_MIN, UINT64_MAX] and a number that overflows a
    // double, so the whole document is kept as its raw text in a string.
    auto raw_text = [&](std::string_view text) {
        return hex(std::string(1, static_cast<char>((text.size() << 2) | 1))) + hex(text);
    };
    const std::vector<std::pair<std::string, std::string>> scalars = {
            {"0", "0c00"},
            {"-0", "0c00"},
            {"127", "0c7f"},
            {"-128", "0c80"},
            {"128", "108000"},
            {"-129", "107fff"},
            {"32768", "1400800000"},
            {"-2147483648", "1400000080"},
            {"2147483648", "180000008000000000"},
            {"-9223372036854775808", "180000000000000080"},
            {"9223372036854775808", "280000000000000000800000000000000000"},
            {"18446744073709551615", "2800ffffffffffffffff0000000000000000"},
            {"18446744073709551616", raw_text("18446744073709551616")},
            {"-9223372036854775809", raw_text("-9223372036854775809")},
            {R"({"a":[1,18446744073709551616]})", raw_text(R"({"a":[1,18446744073709551616]})")},
            {"123456789012345678901234567890", raw_text("123456789012345678901234567890")},
            {"1e400", raw_text("1e400")},
            {"123456789012345678901234567890.5", "1c3e376cff90eef845"},
            {"1.0", "1c000000000000f03f"},
            {"-0.0", "1c0000000000000080"},
            {"0.1", "1c9a9999999999b93f"},
            {"1e3", "1c0000000000408f40"},
            {"1.5E-2", "1cb81e85eb51b88e3f"},
            {"true", "04"},
            {"false", "08"},
            {"null", "00"},
            {R"("")", "01"},
            {R"("ab")", "096162"},
            {R"("\u00e9")", "09c3a9"},
            {"\"" + std::string(63, 'x') + "\"", "fd" + hex(std::string(63, 'x'))},
            {"\"" + std::string(64, 'x') + "\"", "4040000000" + hex(std::string(64, 'x'))},
            {"{}", "020000"},
            {"[]", "030000"},
            {"[1,\"a\"]", "03020002040c01056" "1"},
    };
    for (const auto& [document, expected] : scalars) {
        const EncodedRow row = encode_one(document);
        EXPECT_EQ(hex(row.first), empty_metadata) << document;
        EXPECT_EQ(hex(row.second), expected) << document;
    }

    // Keys are sorted by bytes in the dictionary (sorted_strings set) and object fields follow
    // the sorted ids, whatever the JSON order.
    const EncodedRow object = encode_one(R"({"b":1,"a":2})");
    EXPECT_EQ(hex(object.first), "1102000102" + hex("ab"));
    EXPECT_EQ(hex(object.second), "0202000100020" "40c020c01");
}

// Mixed batches append to one column, as the Arrow batches of one stream do.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, BinaryAndJsonBatchesAppendToOneColumn) {
    const auto& documents = tricky_documents();
    auto json_column = read_json_text(documents);
    auto per_row = encode_per_row(documents);
    auto shared_array = write_binary_struct(*json_column, false);
    auto per_row_array = write_binary_struct(*per_row, false);
    auto text_array = make_utf8(documents);

    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(read_arrow(*column, *per_row_array, 0, per_row_array->length()).ok());
    ASSERT_TRUE(read_arrow(*column, *shared_array, 0, shared_array->length()).ok());
    ASSERT_TRUE(read_arrow(*column, *text_array, 0, text_array->length()).ok());
    ASSERT_TRUE(read_arrow(*column, *shared_array, 0, shared_array->length()).ok());
    ASSERT_EQ(column->size(), 4 * documents.size());
    for (size_t batch = 0; batch < 4; ++batch) {
        expect_same_rows(*json_column, 0, *column, batch * documents.size(), documents.size());
    }
    column->sanity_check();
}

// A null struct row is SQL NULL in the nullable wrapper and Variant null below it; its child slots
// are never read, so junk there is harmless. Covered for both the shared and per-row paths.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, NullStructRowsIgnoreChildSlots) {
    const EncodedRow object = encode_one(R"({"a":1})");
    const EncodedRow other = encode_one(R"({"b":[true]})");
    for (bool shared : {true, false}) {
        auto array = make_binary_struct(
                {std::nullopt, object, std::nullopt, shared ? object : other, std::nullopt});
        DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
        auto outer = make_nullable_variant_column();
        ASSERT_TRUE(nullable_serde
                            .read_column_from_arrow(*outer, array.get(), 0, array->length(),
                                                    cctz::utc_time_zone())
                            .ok());
        ASSERT_EQ(outer->size(), 5);
        const auto& nested = assert_cast<const ColumnVariantV2&>(outer->get_nested_column());
        for (size_t row : {size_t {0}, size_t {2}, size_t {4}}) {
            EXPECT_TRUE(outer->is_null_at(row));
            EXPECT_EQ(json_at(nested, row), "null");
        }
        EXPECT_FALSE(outer->is_null_at(1));
        EXPECT_EQ(json_at(nested, 1), R"({"a":1})");
        EXPECT_EQ(json_at(nested, 3), shared ? R"({"a":1})" : R"({"b":[true]})");
        nested.sanity_check();
    }

    auto all_null = make_binary_struct({std::nullopt, std::nullopt});
    auto column = ColumnVariantV2::create();
    ASSERT_TRUE(read_arrow(*column, *all_null, 0, 2).ok());
    ASSERT_EQ(column->size(), 2);
    EXPECT_EQ(json_at(*column, 0), "null");
    EXPECT_EQ(json_at(*column, 1), "null");
}

// A sliced struct (offset != 0) and a [start, end) sub-range both address the logical rows.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, SlicedArrayAndRangeUseLogicalRows) {
    std::vector<std::string> documents;
    for (int i = 0; i < 10; ++i) {
        documents.push_back("{\"k" + std::to_string(i) + "\":" + std::to_string(i) + "}");
    }
    for (bool shared : {true, false}) {
        auto source = shared ? read_json_text(documents) : encode_per_row(documents);
        std::vector<std::optional<EncodedRow>> rows;
        for (size_t row = 0; row < documents.size(); ++row) {
            if (row == 5) {
                rows.emplace_back(std::nullopt);
            } else {
                rows.emplace_back(row_bytes(*source, row));
            }
        }
        auto array = make_binary_struct(rows);
        auto sliced = array->Slice(3, 6); // rows 3..8
        ASSERT_EQ(sliced->offset(), 3);
        auto column = ColumnVariantV2::create();
        const Status status = read_arrow(*column, *sliced, 1, 5); // rows 4..7
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(column->size(), 4);
        EXPECT_EQ(json_at(*column, 0), R"({"k4":4})");
        EXPECT_EQ(json_at(*column, 1), "null");
        EXPECT_EQ(json_at(*column, 2), R"({"k6":6})");
        EXPECT_EQ(json_at(*column, 3), R"({"k7":7})");
        EXPECT_EQ(column->read_view().metadata_count(), shared ? 1 : 3);

        auto empty = ColumnVariantV2::create();
        ASSERT_TRUE(read_arrow(*empty, *sliced, 2, 2).ok());
        EXPECT_EQ(empty->size(), 0);
    }
}

// Child order and binary/large_binary children are independent of each other.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, ChildOrderAndLargeBinaryChildren) {
    const auto& documents = tricky_documents();
    auto json_column = read_json_text(documents);
    auto per_row = encode_per_row(documents);
    for (const ColumnVariantV2* source : {json_column.get(), per_row.get()}) {
        std::vector<std::optional<EncodedRow>> rows;
        for (size_t row = 0; row < source->size(); ++row) {
            rows.emplace_back(row_bytes(*source, row));
        }
        for (bool metadata_first : {true, false}) {
            for (bool large_metadata : {true, false}) {
                for (bool large_value : {true, false}) {
                    auto array = make_binary_struct(rows, {.metadata_first = metadata_first,
                                                           .large_metadata = large_metadata,
                                                           .large_value = large_value});
                    auto column = ColumnVariantV2::create();
                    ASSERT_TRUE(read_arrow(*column, *array, 0, array->length()).ok());
                    ASSERT_EQ(column->size(), documents.size());
                    for (size_t row = 0; row < documents.size(); ++row) {
                        EXPECT_EQ(row_bytes(*column, row), row_bytes(*source, row));
                    }
                }
            }
        }
    }
}

// The canonical arrow.parquet.variant extension wraps the same storage; other extensions do not.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, ParquetVariantExtensionIsUnwrapped) {
    auto storage = make_binary_struct({encode_one(R"({"x":[1,2]})"), std::nullopt});
    auto variant_type =
            std::make_shared<TestParquetVariantType>(storage->type(), "arrow.parquet.variant");
    auto variant_array = arrow::ExtensionType::WrapArray(variant_type, storage);
    DataTypeNullableSerDe nullable_serde(std::make_shared<DataTypeVariantV2SerDe>());
    auto outer = make_nullable_variant_column();
    ASSERT_TRUE(nullable_serde
                        .read_column_from_arrow(*outer, variant_array.get(), 0, 2,
                                                cctz::utc_time_zone())
                        .ok());
    ASSERT_EQ(outer->size(), 2);
    EXPECT_EQ(json_at(outer->get_nested_column(), 0), R"({"x":[1,2]})");
    EXPECT_TRUE(outer->is_null_at(1));

    auto other_type = std::make_shared<TestParquetVariantType>(storage->type(), "example.other");
    auto other_array = arrow::ExtensionType::WrapArray(other_type, storage);
    auto column = ColumnVariantV2::create();
    EXPECT_EQ(read_arrow(*column, *other_array, 0, 2).code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), 0);
}

// Malformed rows fail the read without a crash, and a failing batch publishes nothing.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, MalformedRowsFailAtomically) {
    const EncodedRow good = encode_one(R"({"a":"b"})");
    const EncodedRow other = encode_one(R"({"c":1})");
    const std::string metadata = good.first;
    const std::vector<EncodedRow> bad_rows = {
            // Object header claiming more fields than the bytes hold.
            {metadata, std::string("\x02\x05\x00", 3)},
            // Primitive with an unknown primitive id.
            {metadata, std::string(1, static_cast<char>(0x7c))},
            // A valid value followed by trailing bytes.
            {metadata, good.second + "x"},
            // A field id outside the dictionary.
            {metadata, std::string("\x02\x01\x07\x00\x01\x00", 6)},
            // Metadata with an unsupported version.
            {std::string("\x02\x00\x00", 3), std::string(1, '\0')},
            // Truncated metadata.
            {std::string("\x01", 1), std::string(1, '\0')},
            // Empty value and empty metadata.
            {metadata, ""},
            {"", good.second},
    };
    for (bool shared : {true, false}) {
        for (const auto& bad : bad_rows) {
            auto column = ColumnVariantV2::create();
            ASSERT_TRUE(read_arrow(*column, *make_binary_struct({good}), 0, 1).ok());
            auto array = make_binary_struct({good, shared ? good : other, bad});
            const Status status = read_arrow(*column, *array, 0, array->length());
            EXPECT_FALSE(status.ok());
            EXPECT_EQ(column->size(), 1);
            column->sanity_check();
            EXPECT_EQ(json_at(*column, 0), R"({"a":"b"})");
        }
    }

    // A non-null struct row whose child is null.
    auto metadata_child = make_child(false, {metadata});
    arrow::BinaryBuilder value_builder;
    ASSERT_TRUE(value_builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> null_value;
    ASSERT_TRUE(value_builder.Finish(&null_value).ok());
    auto array = arrow::StructArray::Make({metadata_child, null_value}, Names {"metadata", "value"})
                         .ValueOrDie();
    auto column = ColumnVariantV2::create();
    EXPECT_EQ(read_arrow(*column, *array, 0, 1).code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(column->size(), 0);
}

// Anything but exactly two binary children named metadata and value is InvalidArgument.
TEST(DataTypeVariantV2SerdeArrowBinaryTest, WrongStructShapeIsInvalidArgument) {
    const EncodedRow good = encode_one("1");
    auto binary = [&](const std::string& value) { return make_child(false, {value}); };
    arrow::StringBuilder string_builder;
    ASSERT_TRUE(string_builder.Append(good.second).ok());
    std::shared_ptr<arrow::Array> string_child;
    ASSERT_TRUE(string_builder.Finish(&string_child).ok());

    std::vector<std::shared_ptr<arrow::StructArray>> arrays = {
            arrow::StructArray::Make({binary(good.first)}, Names {"metadata"}).ValueOrDie(),
            arrow::StructArray::Make({binary(good.first), binary(good.second), binary("")},
                                     Names {"metadata", "value", "typed_value"})
                    .ValueOrDie(),
            arrow::StructArray::Make({binary(good.first), binary(good.second)},
                                     Names {"meta", "value"})
                    .ValueOrDie(),
            arrow::StructArray::Make({binary(good.second), binary(good.second)},
                                     Names {"value", "value"})
                    .ValueOrDie(),
            arrow::StructArray::Make({binary(good.first), string_child}, Names {"metadata", "value"})
                    .ValueOrDie(),
    };
    for (const auto& array : arrays) {
        auto column = ColumnVariantV2::create();
        const Status status = read_arrow(*column, *array, 0, 1);
        EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT) << array->type()->ToString();
        EXPECT_EQ(column->size(), 0);
    }
}

} // namespace doris
