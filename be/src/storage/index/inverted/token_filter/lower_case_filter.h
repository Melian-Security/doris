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

#include <unicode/ucasemap.h>

#include <optional>
#include <string_view>

#include "storage/index/inverted/token_filter/token_filter.h"

namespace doris::segment_v2::inverted_index {

/**
 * @brief A token filter that converts Unicode text to lowercase using ICU library.
 *
 * This filter handles full Unicode case conversion, not just ASCII characters.
 * It uses ICU's ucasemap functionality to properly handle case folding for all Unicode characters.
 */
class LowerCaseFilter : public DorisTokenFilter {
public:
    LowerCaseFilter(const TokenStreamPtr& in)
            : DorisTokenFilter(in), _ucsm(nullptr, &ucasemap_close) {}

    ~LowerCaseFilter() override = default;

    void initialize() {
        UErrorCode status = U_ZERO_ERROR;
        auto* ucsm = ucasemap_open("", 0, &status);
        if (U_FAILURE(status)) {
            throw Exception(ErrorCode::RUNTIME_ERROR,
                            "Failed to open UCaseMap. ICU Error: " + std::to_string(status) +
                                    " - " + u_errorName(status));
        }
        _ucsm.reset(ucsm);
    }

    Token* next(Token* t) override {
        if (_in->next(t) == nullptr) {
            return nullptr;
        }
        auto lower = to_lower(std::string_view(t->termBuffer<char>(), t->termLength<char>()));
        if (!lower.has_value()) {
            return nullptr;
        }
        set_text(t, *lower);
        return t;
    }

    void reset() override { DorisTokenFilter::reset(); }

private:
    // Returns the lower-cased term in a buffer the next call reuses, or nullopt when ICU fails.
    // In ASCII only 'A'-'Z' have a lower-case mapping, so a term without a byte >= 0x80 is
    // lower-cased bytewise to the same bytes ICU produces; any other term goes through ICU.
    std::optional<std::string_view> to_lower(std::string_view term) {
        if (!is_ascii(term)) {
            return to_lower_with_icu(term);
        }
        if (_lower_term.size() < term.size()) {
            _lower_term.resize(term.size());
        }
        for (size_t i = 0; i < term.size(); ++i) {
            const char c = term[i];
            _lower_term[i] = (c >= 'A' && c <= 'Z') ? static_cast<char>(c + ('a' - 'A')) : c;
        }
        return std::string_view(_lower_term.data(), term.size());
    }

    std::optional<std::string_view> to_lower_with_icu(std::string_view term) {
        size_t max_len = term.size() * 2;
        if (_lower_term.size() < max_len) {
            _lower_term.resize(max_len);
        }

        UErrorCode status = U_ZERO_ERROR;
        int32_t result_len = ucasemap_utf8ToLower(_ucsm.get(), _lower_term.data(), max_len,
                                                  term.data(), term.size(), &status);
        if (U_FAILURE(status)) {
            LOG(WARNING) << "Failed to convert to lowercase. "
                         << "Term: '" << term << "', "
                         << "ICU Error: " << status << " - " << u_errorName(status)
                         << ", Buffer size: " << max_len << std::endl;
            return std::nullopt;
        }
        return std::string_view(_lower_term.data(), result_len);
    }

    static bool is_ascii(std::string_view term) {
        for (const char c : term) {
            if (static_cast<unsigned char>(c) >= 0x80) {
                return false;
            }
        }
        return true;
    }

    std::unique_ptr<UCaseMap, decltype(&ucasemap_close)> _ucsm;
    std::string _lower_term;
};
using LowerCaseFilterPtr = std::shared_ptr<LowerCaseFilter>;

} // namespace doris::segment_v2::inverted_index
