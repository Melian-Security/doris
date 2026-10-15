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

#include <gtest/gtest.h>

#include <optional>
#include <random>
#include <string>
#include <vector>

#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/setting.h"
#include "storage/index/inverted/token_filter/lower_case_filter.h"
#include "storage/index/inverted/tokenizer/ngram/ngram_tokenizer.h"

namespace doris::segment_v2::inverted_index {

namespace {

std::string to_hex(std::string_view s) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    for (const char c : s) {
        out.push_back(kDigits[static_cast<unsigned char>(c) >> 4]);
        out.push_back(kDigits[static_cast<unsigned char>(c) & 0xF]);
    }
    return out;
}

std::vector<std::string> sample_terms() {
    std::vector<std::string> terms = {"",
                                      "a",
                                      "Hello",
                                      "HELLO WORLD",
                                      "already lower",
                                      "MiXeD123_Case-@[`{",
                                      std::string("A\0B", 3),
                                      std::string("\0", 1),
                                      std::string("\0\0", 2),
                                      "Grüße",
                                      "ÄÖÜ",
                                      "İstanbul",
                                      "ΣΊΣΥΦΟΣ",
                                      "日本語TEXT",
                                      "ǅ",
                                      "ẞ",
                                      "\xFF\xFE",
                                      "abc\xC3",
                                      "\xE2\x82"};
    // Every ASCII byte alone, including the neighbours of 'A' and 'Z': '@', '[', '`', '{'.
    std::string all_ascii;
    for (int c = 0; c < 0x80; ++c) {
        terms.emplace_back(1, static_cast<char>(c));
        all_ascii.push_back(static_cast<char>(c));
    }
    terms.push_back(all_ascii);
    std::mt19937 rng(42);
    for (int i = 0; i < 2000; ++i) {
        std::string ascii;
        std::string any;
        const int len = static_cast<int>(rng() % 48);
        for (int j = 0; j < len; ++j) {
            ascii.push_back(static_cast<char>(rng() % 0x80));
            any.push_back(static_cast<char>(rng() % 0x100));
        }
        terms.push_back(std::move(ascii));
        terms.push_back(std::move(any));
    }
    return terms;
}

std::optional<std::string> copy(std::optional<std::string_view> v) {
    return v.has_value() ? std::optional<std::string>(std::string(*v)) : std::nullopt;
}

} // namespace

// The ASCII fast path must produce exactly the bytes ICU produces.
TEST(AsciiLowerCaseTest, LowerCaseFilterMatchesIcu) {
    LowerCaseFilter filter(nullptr);
    filter.initialize();
    size_t ascii_terms = 0;
    for (const auto& term : sample_terms()) {
        auto expected = copy(filter.to_lower_with_icu(term));
        auto actual = copy(filter.to_lower(term));
        EXPECT_EQ(actual, expected) << "term " << to_hex(term);
        if (LowerCaseFilter::is_ascii(term)) {
            ASSERT_TRUE(expected.has_value()) << "ICU failed on " << to_hex(term);
            ++ascii_terms;
        }
    }
    EXPECT_GT(ascii_terms, 2000);
}

// The ASCII fast path of the n-gram tokenizer must encode a gram to the bytes ICU produces.
TEST(AsciiLowerCaseTest, NGramToCharsMatchesIcu) {
    std::vector<std::vector<UChar32>> grams = {{},
                                               {'a'},
                                               {0},
                                               {'A', 'b', 0, '{'},
                                               {0x7F, '@', '[', '`', '{'},
                                               {'a', 0xE9, 'b'},
                                               {0x65E5, 0x672C},
                                               {0x1F600, 'x'},
                                               {0x80},
                                               {0x10FFFF}};
    std::mt19937 rng(7);
    for (int i = 0; i < 2000; ++i) {
        std::vector<UChar32> ascii;
        std::vector<UChar32> mixed;
        const int len = static_cast<int>(rng() % 9);
        for (int j = 0; j < len; ++j) {
            ascii.push_back(static_cast<UChar32>(rng() % 0x80));
            auto c = static_cast<UChar32>(rng() % 0x110000);
            if (c >= 0xD800 && c <= 0xDFFF) {
                c = 'q';
            }
            mixed.push_back(c);
        }
        grams.push_back(std::move(ascii));
        grams.push_back(std::move(mixed));
    }

    NGramTokenizer tokenizer(4, 4);
    for (const auto& gram : grams) {
        // A leading code point checks that `start` is honoured.
        std::vector<UChar32> buffer = {'Z'};
        buffer.insert(buffer.end(), gram.begin(), gram.end());
        const auto size = static_cast<int32_t>(gram.size());
        tokenizer.to_chars_with_icu(buffer, 1, size);
        const std::string expected = tokenizer._utf8_buffer;
        tokenizer.to_chars(buffer, 1, size);
        EXPECT_EQ(tokenizer._utf8_buffer, expected) << "gram bytes " << to_hex(expected);
    }
}

namespace {

std::vector<std::string> analyze(const CustomAnalyzerPtr& analyzer, const std::string& line) {
    std::vector<std::string> terms;
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(line.data(), static_cast<int32_t>(line.size()), false);
    auto* stream = analyzer->reusableTokenStream(L"", reader);
    stream->reset();
    Token t;
    while (stream->next(&t)) {
        terms.emplace_back(t.termBuffer<char>(), t.termLength<char>());
    }
    return terms;
}

CustomAnalyzerPtr build_analyzer(const std::string& tokenizer, const Settings& settings,
                                 bool lowercase) {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config(tokenizer, settings);
    if (lowercase) {
        builder.add_token_filter_config("lowercase", {});
    }
    return CustomAnalyzer::build_custom_analyzer(builder.build());
}

} // namespace

// Analyzers built like ngram4_lower and alnum_lower give the terms of their tokenizer lower-cased
// by ICU.
TEST(AsciiLowerCaseTest, LowerCaseAnalyzersMatchIcu) {
    Settings ngram4;
    ngram4.set("min_gram", "4");
    ngram4.set("max_gram", "4");
    ngram4.set("token_chars", "");
    const std::vector<std::pair<std::string, Settings>> tokenizers = {{"ngram", ngram4},
                                                                      {"basic", {}}};
    const std::vector<std::string> lines = {
            "2026-10-14T08:12:31.512Z INFO [AuthService] User Alice.Smith@Example.COM logged in",
            R"({"EventType":"ProcessCreate","Image":"C:\\Windows\\System32\\cmd.exe"})",
            "GET /api/v1/Users?id=42&Sort=Desc HTTP/1.1 200 \"Mozilla/5.0 (Windows NT 10.0)\"",
            "Kernel: Grüße aus München, ÜNÏCÖDÉ PATH=/Usr/Lib",
            std::string("NUL\0Inside\0\0Value", 17),
            "@[`{AZaz}`]@",
            "",
    };
    LowerCaseFilter icu(nullptr);
    icu.initialize();
    for (const auto& [name, settings] : tokenizers) {
        auto with_lower = build_analyzer(name, settings, true);
        auto without_lower = build_analyzer(name, settings, false);
        for (const auto& line : lines) {
            std::vector<std::string> expected;
            for (const auto& term : analyze(without_lower, line)) {
                auto lower = icu.to_lower_with_icu(term);
                ASSERT_TRUE(lower.has_value());
                expected.emplace_back(*lower);
            }
            EXPECT_EQ(analyze(with_lower, line), expected) << name << ": " << to_hex(line);
        }
    }
}

} // namespace doris::segment_v2::inverted_index
