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

// Measures DataTypeVariantV2::deserialize of an encoded VARIANT V2 column, the per-column step of
// Block::deserialize on tablet_writer_add_block. trusted_peer:0 runs the full recursive payload
// validation; trusted_peer:1 runs only the structural checks kept for internal load RPCs.

#pragma once

#include <benchmark/benchmark.h>

#include <string>
#include <vector>

#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/parse/variant_string_parse.h"

namespace doris {
namespace variant_v2_deserialize_bench {

constexpr int BE_EXEC_VERSION = 10;

inline std::string log_row(size_t row) {
    const std::string id = std::to_string(row);
    return R"({"time":"2026-10-06T12:00:00.)" + id + R"(Z","class_uid":3002,"severity_id":)" +
           std::to_string(row % 6) + R"(,"actor":{"user":{"name":"user-)" + id +
           R"(","uid":")" + id + R"(","groups":["admins","ops","dev"]}},)" +
           R"("src_endpoint":{"ip":"10.0.)" + std::to_string(row % 256) + R"(.1","port":)" +
           std::to_string(1024 + row % 50000) +
           R"(},"metadata":{"product":{"name":"bench","vendor_name":"doris"},)" +
           R"("labels":["a","b","c","d"]},"message":"login attempt number )" + id + R"("})";
}

inline std::vector<char> serialized_column(size_t rows) {
    JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    for (size_t row = 0; row < rows; ++row) {
        const std::string json = log_row(row);
        encoder.add_json({json.data(), json.size()});
    }
    VariantBatchBuilder batch = encoder.finish_batch();
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(batch);

    const DataTypeVariantV2 type;
    std::vector<char> bytes(type.get_uncompressed_serialized_bytes(*column, BE_EXEC_VERSION));
    char* end = type.serialize(*column, bytes.data(), BE_EXEC_VERSION);
    bytes.resize(end - bytes.data());
    return bytes;
}

} // namespace variant_v2_deserialize_bench

static void BM_VariantV2BlockDeserialize(benchmark::State& state) {
    const auto rows = static_cast<size_t>(state.range(0));
    const bool trusted_peer = state.range(1) != 0;
    const std::vector<char> bytes = variant_v2_deserialize_bench::serialized_column(rows);
    const DataTypeVariantV2 type;
    TrustedPeerVariantBlockScope scope(trusted_peer);
    for (auto _ : state) {
        MutableColumnPtr column = type.create_column();
        benchmark::DoNotOptimize(type.deserialize(bytes.data(), &column,
                                                  variant_v2_deserialize_bench::BE_EXEC_VERSION));
        benchmark::DoNotOptimize(column);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * bytes.size()));
}

BENCHMARK(BM_VariantV2BlockDeserialize)
        ->ArgNames({"rows", "trusted_peer"})
        ->ArgsProduct({{1024, 8192}, {0, 1}})
        ->Unit(benchmark::kMicrosecond);

} // namespace doris
