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

#include <benchmark/benchmark.h>

#include <string>
#include <vector>

#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/setting.h"
#include "storage/index/inverted/token_filter/lower_case_filter.h"
#include "storage/index/inverted/tokenizer/ngram/ngram_tokenizer.h"

namespace doris::inverted_index_lowercase_bench {

using segment_v2::inverted_index::CustomAnalyzer;
using segment_v2::inverted_index::CustomAnalyzerConfig;
using segment_v2::inverted_index::CustomAnalyzerPtr;
using segment_v2::inverted_index::Settings;

// Log-like lines: mostly ASCII with mixed case, one in twenty holding non-ASCII text.
inline const std::vector<std::string>& log_lines() {
    static const std::vector<std::string> lines = [] {
        std::vector<std::string> out;
        out.reserve(4000);
        for (int i = 0; i < 4000; ++i) {
            const std::string n = std::to_string(i);
            switch (i % 4) {
            case 0:
                out.push_back("2026-10-14T08:12:" + std::to_string(i % 60) +
                              ".512Z INFO [AuthService] User Alice.Smith" + n +
                              "@Example.COM logged in from 10.20.30." + std::to_string(i % 255) +
                              " session=AbC" + n + "XyZ result=SUCCESS");
                break;
            case 1:
                out.push_back(
                        R"({"EventType":"ProcessCreate","Image":"C:\\Windows\\System32\\cmd.exe",)"
                        R"("CommandLine":"cmd.exe /c whoami /ALL","User":"CORP\\Admin)" +
                        n + R"(","ProcessId":)" + n + "}");
                break;
            case 2:
                out.push_back("GET /api/v1/Users?id=" + n +
                              "&Sort=Desc HTTP/1.1 200 1234 \"Mozilla/5.0 (Windows NT 10.0; "
                              "Win64; x64) AppleWebKit/537.36\"");
                break;
            default:
                out.push_back(i % 20 == 3 ? "Kernel: Grüße aus München, Ünïcödé PATH=/Usr/Lib" + n
                                          : "Kernel: AUDIT type=SYSCALL msg=audit(" + n +
                                                    ") arch=C000003E syscall=59 SUCCESS=yes");
                break;
            }
        }
        return out;
    }();
    return lines;
}

inline CustomAnalyzerPtr ngram4_lower() {
    Settings ngram;
    ngram.set("min_gram", "4");
    ngram.set("max_gram", "4");
    ngram.set("token_chars", "");
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("ngram", ngram);
    builder.add_token_filter_config("lowercase", {});
    return CustomAnalyzer::build_custom_analyzer(builder.build());
}

inline CustomAnalyzerPtr alnum_lower() {
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("basic", {});
    builder.add_token_filter_config("lowercase", {});
    return CustomAnalyzer::build_custom_analyzer(builder.build());
}

inline size_t analyze_all(const CustomAnalyzerPtr& analyzer,
                          const std::vector<std::string>& lines) {
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    size_t bytes = 0;
    lucene::analysis::Token t;
    for (const auto& line : lines) {
        reader->init(line.data(), static_cast<int32_t>(line.size()), false);
        auto* stream = analyzer->reusableTokenStream(L"", reader);
        stream->reset();
        while (stream->next(&t)) {
            bytes += t.termLength<char>();
        }
    }
    return bytes;
}

static void BM_InvertedIndexAnalyzeNgram4Lower(benchmark::State& state) {
    auto analyzer = ngram4_lower();
    const auto& lines = log_lines();
    for (auto _ : state) {
        benchmark::DoNotOptimize(analyze_all(analyzer, lines));
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lines.size()));
}
BENCHMARK(BM_InvertedIndexAnalyzeNgram4Lower)->Unit(benchmark::kMillisecond);

static void BM_InvertedIndexAnalyzeAlnumLower(benchmark::State& state) {
    auto analyzer = alnum_lower();
    const auto& lines = log_lines();
    for (auto _ : state) {
        benchmark::DoNotOptimize(analyze_all(analyzer, lines));
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lines.size()));
}
BENCHMARK(BM_InvertedIndexAnalyzeAlnumLower)->Unit(benchmark::kMillisecond);

} // namespace doris::inverted_index_lowercase_bench

namespace doris::inverted_index_lowercase_bench {

// Terms of the log lines before lower-casing: their 4-grams and their basic-tokenizer tokens.
inline const std::vector<std::string>& unlowered_terms() {
    static const std::vector<std::string> terms = [] {
        std::vector<std::string> out;
        Settings ngram;
        ngram.set("min_gram", "4");
        ngram.set("max_gram", "4");
        ngram.set("token_chars", "");
        for (const auto& tokenizer : {std::string("ngram"), std::string("basic")}) {
            CustomAnalyzerConfig::Builder builder;
            builder.with_tokenizer_config(tokenizer, tokenizer == "ngram" ? ngram : Settings {});
            auto analyzer = CustomAnalyzer::build_custom_analyzer(builder.build());
            auto reader = std::make_shared<lucene::util::SStringReader<char>>();
            lucene::analysis::Token t;
            for (const auto& line : log_lines()) {
                reader->init(line.data(), static_cast<int32_t>(line.size()), false);
                auto* stream = analyzer->reusableTokenStream(L"", reader);
                stream->reset();
                while (stream->next(&t)) {
                    out.emplace_back(t.termBuffer<char>(), t.termLength<char>());
                }
            }
        }
        return out;
    }();
    return terms;
}

static void BM_LowerCaseFilterIcu(benchmark::State& state) {
    segment_v2::inverted_index::LowerCaseFilter filter(nullptr);
    filter.initialize();
    const auto& terms = unlowered_terms();
    for (auto _ : state) {
        for (const auto& term : terms) {
            benchmark::DoNotOptimize(filter.to_lower_with_icu(term));
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(terms.size()));
}
BENCHMARK(BM_LowerCaseFilterIcu)->Unit(benchmark::kMillisecond);

static void BM_LowerCaseFilterAsciiFastPath(benchmark::State& state) {
    segment_v2::inverted_index::LowerCaseFilter filter(nullptr);
    filter.initialize();
    const auto& terms = unlowered_terms();
    for (auto _ : state) {
        for (const auto& term : terms) {
            benchmark::DoNotOptimize(filter.to_lower(term));
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(terms.size()));
}
BENCHMARK(BM_LowerCaseFilterAsciiFastPath)->Unit(benchmark::kMillisecond);

inline std::vector<UChar32> ngram_code_points() {
    std::vector<UChar32> out;
    for (const auto& line : log_lines()) {
        for (int32_t i = 0; i < static_cast<int32_t>(line.size());) {
            UChar32 c = 0;
            U8_NEXT(line.data(), i, static_cast<int32_t>(line.size()), c);
            out.push_back(c < 0 ? 'x' : c);
        }
    }
    return out;
}

template <bool kIcu>
static void BM_NGramToChars(benchmark::State& state) {
    segment_v2::inverted_index::NGramTokenizer tokenizer(4, 4);
    const auto code_points = ngram_code_points();
    const auto grams = static_cast<int32_t>(code_points.size()) - 4;
    for (auto _ : state) {
        for (int32_t i = 0; i < grams; ++i) {
            if constexpr (kIcu) {
                tokenizer.to_chars_with_icu(code_points, i, 4);
            } else {
                tokenizer.to_chars(code_points, i, 4);
            }
            benchmark::DoNotOptimize(tokenizer._utf8_buffer.data());
        }
    }
    state.SetItemsProcessed(state.iterations() * grams);
}
BENCHMARK(BM_NGramToChars<true>)->Name("BM_NGramToCharsIcu")->Unit(benchmark::kMillisecond);
BENCHMARK(BM_NGramToChars<false>)
        ->Name("BM_NGramToCharsAsciiFastPath")
        ->Unit(benchmark::kMillisecond);

} // namespace doris::inverted_index_lowercase_bench
