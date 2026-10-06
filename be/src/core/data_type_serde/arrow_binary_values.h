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

#include <arrow/array/array_binary.h>
#include <arrow/type_fwd.h>

#include <cstddef>
#include <cstdint>

#include "common/config.h"
#include "common/status.h"
#include "core/data_type_serde/arrow_validation.h"
#include "core/string_ref.h"

namespace doris {

namespace arrow_binary_values_detail {

template <typename ArrowArray, typename OnValue, typename OnNull>
void visit_rows(const ArrowArray& array, int64_t start, int64_t end, OnValue& on_value,
                OnNull& on_null) {
    if (config::enable_arrow_input_validation) {
        check_arrow_array_range(array, start, end);
        check_arrow_binary_offsets_buffer(array);
    }
    const auto& buffer = array.value_data();
    const auto* data = buffer ? reinterpret_cast<const char*>(buffer->data()) : nullptr;
    const size_t buffer_size = buffer ? static_cast<size_t>(buffer->size()) : 0;
    for (int64_t row = start; row < end; ++row) {
        if (array.IsNull(row)) {
            on_null();
            continue;
        }
        const auto offset = array.value_offset(row);
        const auto length = array.value_length(row);
        if (config::enable_arrow_input_validation) {
            check_arrow_value_range(array, offset, length, buffer_size);
        }
        // A zero-length value may come with a null data buffer; the callee never reads it.
        on_value(StringRef(length == 0 ? "" : data + offset, static_cast<size_t>(length)));
    }
}

} // namespace arrow_binary_values_detail

// Visits rows [start, end) of a STRING, BINARY, LARGE_STRING or LARGE_BINARY Arrow array:
// on_value(StringRef) for each non-null row and on_null() for each null row, in row order.
// The StringRef borrows the array's data buffer and is valid only for the duration of the call.
// Any other Arrow type returns InvalidArgument without calling either callback; malformed input
// throws through the arrow_validation checks when config::enable_arrow_input_validation is set.
template <typename OnValue, typename OnNull>
Status for_each_arrow_binary_value(const arrow::Array& array, int64_t start, int64_t end,
                                   OnValue&& on_value, OnNull&& on_null) {
    switch (array.type_id()) {
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
        arrow_binary_values_detail::visit_rows(static_cast<const arrow::BinaryArray&>(array), start,
                                               end, on_value, on_null);
        return Status::OK();
    case arrow::Type::LARGE_STRING:
    case arrow::Type::LARGE_BINARY:
        arrow_binary_values_detail::visit_rows(static_cast<const arrow::LargeBinaryArray&>(array),
                                               start, end, on_value, on_null);
        return Status::OK();
    default:
        return Status::InvalidArgument("Unsupported arrow type {} for a JSON text column",
                                       array.type()->ToString());
    }
}

} // namespace doris
