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

// CPU cost of handing one tablet sink batch to a tablet channel on the same BE.
//
// The batch is 4096 rows of a string column and a VARIANT V2 column, shaped like a log load.
//
//   LocalTabletWriter/BrpcRoundTrip  what the brpc path spends on the batch apart from the
//                                    network: Block::serialize with LZ4 into the add-block
//                                    request, the protobuf encode and parse brpc performs, and
//                                    Block::deserialize on the receiver (decompression and the
//                                    VARIANT V2 validation).
//   LocalTabletWriter/InProcess      the in-process handoff: the receiver resolves the sender's
//                                    block without copying it.
//
// With memtable_copy:1 both variants also copy every row into a fresh MutableBlock, which stands
// in for MemTable::insert, the receiver work that both paths share.
//
// Run: benchmark_test --benchmark_filter='LocalTabletWriter/'

#pragma once

#include <benchmark/benchmark.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/segment_v2.pb.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_serde.h"
#include "load/channel/tablets_channel.h"

namespace doris::local_tablet_writer_bench {

constexpr size_t kRows = 4096;

inline void throw_if_error(const Status& st) {
    if (!st.ok()) {
        throw std::runtime_error(st.to_string());
    }
}

inline Block make_log_block(size_t rows) {
    auto host_column = ColumnString::create();
    std::vector<std::string> jsons;
    jsons.reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        std::string host = "host-" + std::to_string(i % 97) + ".internal.example";
        host_column->insert_data(host.data(), host.size());
        jsons.push_back(R"({"ts":)" + std::to_string(1700000000000 + i) +
                        R"(,"src":"10.0.)" + std::to_string(i % 256) + R"(.7","msg":"request )" +
                        std::to_string(i) + R"( served","tags":["edge","waf"],"http":{"status":)" +
                        std::to_string(200 + i % 5) + R"(,"bytes":)" + std::to_string(i * 13) +
                        "}}");
    }
    std::vector<Slice> slices;
    slices.reserve(rows);
    for (const auto& json : jsons) {
        slices.emplace_back(json.data(), json.size());
    }
    auto variant_type = std::make_shared<DataTypeVariantV2>();
    auto variant_column = variant_type->create_column();
    uint64_t deserialized = 0;
    throw_if_error(variant_type->get_serde()->deserialize_column_from_json_vector(
            *variant_column, slices, &deserialized, DataTypeSerDe::FormatOptions {}));

    Block block;
    block.insert({std::move(host_column), std::make_shared<DataTypeString>(), "host"});
    block.insert({std::move(variant_column), variant_type, "payload"});
    return block;
}

inline void copy_into_memtable_stand_in(const Block& block) {
    auto dst = MutableBlock::create_unique(block.clone_empty());
    throw_if_error(dst->add_rows(&block, 0, block.rows()));
    benchmark::DoNotOptimize(dst->rows());
}

inline void set_counters(benchmark::State& state) {
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kRows));
    state.counters["rows/batch"] = static_cast<double>(kRows);
}

static void LocalTabletWriter_BrpcRoundTrip(benchmark::State& state) {
    const bool memtable_copy = state.range(0) != 0;
    const Block sender_block = make_log_block(kRows);
    const int be_exec_version = BeExecVersionManager::get_newest_version();
    size_t wire_bytes = 0;
    for (auto _ : state) {
        PTabletWriterAddBlockRequest request;
        // The required header fields of a real request; parsing rejects a request without them.
        request.mutable_id()->set_hi(1);
        request.mutable_id()->set_lo(1);
        request.set_index_id(1);
        request.set_sender_id(0);
        size_t uncompressed_bytes = 0;
        size_t compressed_bytes = 0;
        int64_t compress_time = 0;
        throw_if_error(sender_block.serialize(be_exec_version, request.mutable_block(),
                                              &uncompressed_bytes, &compressed_bytes,
                                              &compress_time, segment_v2::CompressionTypePB::LZ4));
        std::string wire;
        request.SerializeToString(&wire);
        wire_bytes = wire.size();

        PTabletWriterAddBlockRequest received;
        if (!received.ParseFromString(wire)) {
            throw std::runtime_error("failed to parse add block request");
        }
        Block deserialized;
        const Block* send_data = nullptr;
        throw_if_error(BaseTabletsChannel::resolve_send_block(received, nullptr, &deserialized,
                                                              &send_data));
        benchmark::DoNotOptimize(send_data->rows());
        if (memtable_copy) {
            copy_into_memtable_stand_in(*send_data);
        }
    }
    set_counters(state);
    state.counters["wire_bytes"] = static_cast<double>(wire_bytes);
}

static void LocalTabletWriter_InProcess(benchmark::State& state) {
    const bool memtable_copy = state.range(0) != 0;
    const Block sender_block = make_log_block(kRows);
    for (auto _ : state) {
        PTabletWriterAddBlockRequest request;
        Block unused;
        const Block* send_data = nullptr;
        throw_if_error(BaseTabletsChannel::resolve_send_block(request, &sender_block, &unused,
                                                              &send_data));
        benchmark::DoNotOptimize(send_data->rows());
        if (memtable_copy) {
            copy_into_memtable_stand_in(*send_data);
        }
    }
    set_counters(state);
}

BENCHMARK(LocalTabletWriter_BrpcRoundTrip)
        ->Name("LocalTabletWriter/BrpcRoundTrip")
        ->ArgName("memtable_copy")
        ->Arg(0)
        ->Arg(1)
        ->Unit(benchmark::kMicrosecond);
BENCHMARK(LocalTabletWriter_InProcess)
        ->Name("LocalTabletWriter/InProcess")
        ->ArgName("memtable_copy")
        ->Arg(0)
        ->Arg(1)
        ->Unit(benchmark::kMicrosecond);

} // namespace doris::local_tablet_writer_bench
