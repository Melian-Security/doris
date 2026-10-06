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

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

#include "core/string_ref.h"
#include "core/value/variant/variant_metadata.h"
#include "core/value/variant/variant_parquet_encoding.h"

namespace doris {

struct VariantRef {
    class ObjectView;
    class ArrayView;

    VariantMetadataRef metadata;
    StringRef value;

    VariantBasicType basic_type() const;
    VariantPrimitiveId primitive_id() const;
    size_t value_size() const;

    bool is_null() const;
    bool get_bool() const;
    int64_t get_int() const;
    float get_float() const;
    double get_double() const;
    VariantDecimal get_decimal() const;
    int32_t get_date() const;
    int64_t get_timestamp_micros() const;
    int64_t get_timestamp_ntz_micros() const;
    int64_t get_time_ntz_micros() const;
    int64_t get_timestamp_nanos() const;
    int64_t get_timestamp_ntz_nanos() const;
    StringRef get_binary() const;
    StringRef get_string() const;
    std::array<uint8_t, 16> get_uuid() const;

    uint32_t num_elements() const;
    ObjectView object_view() const;
    ArrayView array_view() const;
    bool object_find(StringRef key, VariantRef* out) const;
    bool object_find_by_id(uint32_t field_id, VariantRef* out) const;
    VariantRef object_value_at(uint32_t index, uint32_t* field_id_out) const;
    VariantRef array_at(uint32_t index) const;

private:
    struct ContainerLayout {
        uint32_t count;
        uint8_t offset_width;
        uint8_t id_width;
        size_t ids_offset;
        size_t offsets_offset;
        size_t values_offset;
        uint32_t values_size;
    };

    [[noreturn]] static void _throw_truncated_header();
    [[noreturn]] static void _throw_not_primitive(VariantBasicType type);
    [[noreturn]] static void _throw_unknown_primitive(uint8_t id);

    ContainerLayout _container_layout(VariantBasicType expected_type) const;
    uint32_t _object_field_id(const ContainerLayout& layout, uint32_t index,
                              const uint32_t* dictionary_size = nullptr) const;
    bool _object_find_by_id(const ContainerLayout& layout, uint32_t field_id,
                            VariantRef* out) const;
    VariantRef _container_value_at(const ContainerLayout& layout, uint32_t index,
                                   bool require_array_boundary,
                                   ContainerLayout* child_layout = nullptr,
                                   bool* child_has_layout = nullptr) const;
};

// Parses and validates an object's physical layout and metadata dictionary size once, then reuses
// them while iterating its children. The referenced metadata and value bytes must outlive the view.
class VariantRef::ObjectView {
public:
    uint32_t size() const { return _layout.count; }
    VariantRef value_at(uint32_t index, uint32_t* field_id_out = nullptr) const;
    // Same child as value_at(). When the child is an OBJECT, child_object also receives its view,
    // built from the header decode that already sized the child, so descending into a nested
    // object neither re-reads its header nor re-reads the shared metadata dictionary header.
    VariantRef value_at(uint32_t index, uint32_t* field_id_out,
                        std::optional<ObjectView>* child_object) const;

private:
    friend struct VariantRef;
    ObjectView(VariantRef value, ContainerLayout layout, uint32_t dictionary_size)
            : _value(value), _layout(layout), _dictionary_size(dictionary_size) {}

    VariantRef _value;
    ContainerLayout _layout;
    uint32_t _dictionary_size;
};

// Parses and validates an array's physical layout once, then reuses it for every element.
// value_at(i) returns exactly what array_at(i) returns. The referenced bytes must outlive the view.
class VariantRef::ArrayView {
public:
    uint32_t size() const { return _layout.count; }
    VariantRef value_at(uint32_t index) const {
        return _value._container_value_at(_layout, index, true);
    }

private:
    friend struct VariantRef;
    ArrayView(VariantRef value, ContainerLayout layout) : _value(value), _layout(layout) {}

    VariantRef _value;
    ContainerLayout _layout;
};

// The header accessors run once or more per encoded node on every shredding, conversion and
// comparison path, so they stay inline; only their error paths live out of line.
inline VariantBasicType VariantRef::basic_type() const {
    if (value.size < 1) [[unlikely]] {
        _throw_truncated_header();
    }
    return static_cast<VariantBasicType>(static_cast<uint8_t>(value.data[0]) &
                                         VARIANT_BASIC_TYPE_MASK);
}

inline VariantPrimitiveId VariantRef::primitive_id() const {
    const VariantBasicType type = basic_type();
    if (type != VariantBasicType::PRIMITIVE) [[unlikely]] {
        _throw_not_primitive(type);
    }
    const uint8_t id = static_cast<uint8_t>(value.data[0]) >> VARIANT_VALUE_HEADER_SHIFT;
    if (id > VARIANT_MAX_PRIMITIVE_ID) [[unlikely]] {
        _throw_unknown_primitive(id);
    }
    return static_cast<VariantPrimitiveId>(id);
}

inline bool VariantRef::is_null() const {
    return basic_type() == VariantBasicType::PRIMITIVE &&
           primitive_id() == VariantPrimitiveId::NULL_VALUE;
}

} // namespace doris
