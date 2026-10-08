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

// CPU and size of writing one memtable flush's segment with each page codec a load may use.
//
// The block is a log table: DUPLICATE KEY(org_id, event_time) and 66 nullable value columns
// (integers, doubles, booleans, datetimes, low-cardinality strings that dictionary-encode and
// high-cardinality strings that do not). Each iteration writes the block with
// VerticalSegmentWriter, as SegmentFlusher does for a memtable, to a local file and finalizes it.
//
//   SegmentCompression/<codec>/<rows>   codec: 0 ZSTD level 3 (the table default),
//                                       1 ZSTD level 1, 2 LZ4, 3 LZ4F, 4 no compression
//
// Counters: rows/s and input bytes/s (items and bytes processed), segment_bytes (output size)
// and ratio (input block bytes / segment bytes).
//
// MemTableSortComparator and MergeCopyRowsColumnRef measure the per-call reference-count updates
// that the flush sort and the compaction merge copy used to pay, against the borrowed forms.
//
// Run: benchmark_test --benchmark_filter='SegmentCompression|MemTableSortComparator|MergeCopyRows'

#pragma once

#include <benchmark/benchmark.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <pdqsort.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/value/vdatetime_value.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "load/memtable/memtable.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/block_compression.h"

namespace doris::segment_compression_bench {

inline void throw_if_error(const Status& st) {
    if (!st.ok()) {
        throw std::runtime_error(st.to_string());
    }
}

enum class ValueKind { BIGINT, INT, DOUBLE, BOOLEAN, DATETIME, LOW_CARD_STRING, HIGH_CARD_STRING };

struct ColumnSpec {
    std::string name;
    ValueKind kind;
};

inline std::vector<ColumnSpec> value_columns() {
    std::vector<ColumnSpec> columns;
    auto add = [&](const std::string& prefix, ValueKind kind, int count) {
        for (int i = 0; i < count; ++i) {
            columns.push_back({prefix + std::to_string(i), kind});
        }
    };
    add("i64_", ValueKind::BIGINT, 12);
    add("i32_", ValueKind::INT, 12);
    add("f64_", ValueKind::DOUBLE, 6);
    add("b_", ValueKind::BOOLEAN, 4);
    add("ts_", ValueKind::DATETIME, 4);
    add("enum_", ValueKind::LOW_CARD_STRING, 18);
    add("text_", ValueKind::HIGH_CARD_STRING, 10);
    return columns;
}

inline void set_type(ColumnPB* column, ValueKind kind) {
    switch (kind) {
    case ValueKind::BIGINT:
        column->set_type("BIGINT");
        column->set_length(8);
        break;
    case ValueKind::INT:
        column->set_type("INT");
        column->set_length(4);
        break;
    case ValueKind::DOUBLE:
        column->set_type("DOUBLE");
        column->set_length(8);
        break;
    case ValueKind::BOOLEAN:
        column->set_type("BOOLEAN");
        column->set_length(1);
        break;
    case ValueKind::DATETIME:
        column->set_type("DATETIMEV2");
        column->set_length(8);
        column->set_frac(6);
        break;
    case ValueKind::LOW_CARD_STRING:
    case ValueKind::HIGH_CARD_STRING:
        column->set_type("VARCHAR");
        column->set_length(65533);
        break;
    }
}

inline TabletSchemaSPtr make_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(2);
    schema_pb.set_num_rows_per_row_block(1024);
    schema_pb.set_compression_type(segment_v2::ZSTD);
    int32_t unique_id = 0;
    auto add_column = [&](const std::string& name, ValueKind kind, bool is_key) {
        ColumnPB* column = schema_pb.add_column();
        column->set_unique_id(unique_id++);
        column->set_name(name);
        set_type(column, kind);
        column->set_is_key(is_key);
        column->set_is_nullable(!is_key);
        column->set_index_length(is_key ? 8 : 16);
        column->set_aggregation("NONE");
    };
    add_column("org_id", ValueKind::BIGINT, true);
    add_column("event_time", ValueKind::DATETIME, true);
    for (const auto& spec : value_columns()) {
        add_column(spec.name, spec.kind, false);
    }
    schema_pb.set_next_column_unique_id(unique_id);
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

inline uint64_t datetime_value(int64_t seconds, uint32_t micros) {
    DateV2Value<DateTimeV2ValueType> value;
    static_cast<void>(value.from_unixtime(seconds, "UTC"));
    value.set_microsecond(micros);
    return value.to_date_int_val();
}

// Rows of a few organizations ordered by event time, as a sorted memtable holds them.
inline Block make_block(const TabletSchemaSPtr& schema, size_t rows) {
    static const std::vector<std::string> kEnums = {
            "ALLOW",   "DENY",     "DROP",          "RESET",     "tcp",    "udp",
            "icmp",    "GET",      "POST",          "PUT",       "DELETE", "us-east",
            "eu-west", "ap-south", "authenticated", "anonymous", "admin",  "service"};
    std::mt19937_64 rng(42);
    Block block = schema->create_block();
    auto columns = std::move(block).mutate_columns();
    const auto specs = value_columns();
    const int64_t base_seconds = 1767225600;
    for (size_t row = 0; row < rows; ++row) {
        int64_t org = static_cast<int64_t>(row * 4 / rows);
        columns[0]->insert_data(reinterpret_cast<const char*>(&org), sizeof(org));
        uint64_t ts = datetime_value(base_seconds + static_cast<int64_t>(row % (rows / 4)) / 50,
                                     static_cast<uint32_t>(rng() % 1000000));
        columns[1]->insert_data(reinterpret_cast<const char*>(&ts), sizeof(ts));
        for (size_t c = 0; c < specs.size(); ++c) {
            auto& column = columns[c + 2];
            if (rng() % 10 == 0) {
                column->insert_default();
                continue;
            }
            switch (specs[c].kind) {
            case ValueKind::BIGINT: {
                int64_t v = c % 3 == 0 ? static_cast<int64_t>(rng() % 1000)
                                       : static_cast<int64_t>(rng() % 100000000000ULL);
                column->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
                break;
            }
            case ValueKind::INT: {
                int32_t v = c % 2 == 0 ? static_cast<int32_t>(200 + rng() % 7)
                                       : static_cast<int32_t>(rng() % 65536);
                column->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
                break;
            }
            case ValueKind::DOUBLE: {
                double v = static_cast<double>(rng() % 100000) / 100.0;
                column->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
                break;
            }
            case ValueKind::BOOLEAN: {
                uint8_t v = rng() % 4 == 0;
                column->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
                break;
            }
            case ValueKind::DATETIME: {
                uint64_t v = datetime_value(base_seconds + static_cast<int64_t>(rng() % 86400), 0);
                column->insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
                break;
            }
            case ValueKind::LOW_CARD_STRING: {
                const auto& v = kEnums[(rng() % (3 + c % kEnums.size())) % kEnums.size()];
                column->insert_data(v.data(), v.size());
                break;
            }
            case ValueKind::HIGH_CARD_STRING: {
                std::string v = "/api/v2/tenant/" + std::to_string(rng() % 5000) + "/objects/" +
                                std::to_string(rng()) + "?trace=" + std::to_string(rng() % 99991);
                column->insert_data(v.data(), v.size());
                break;
            }
            }
        }
    }
    block.set_columns(std::move(columns));
    return block;
}

struct Codec {
    segment_v2::CompressionTypePB type;
    int zstd_level;
    const char* label;
};

inline Codec codec_at(int64_t index) {
    static const Codec kCodecs[] = {{segment_v2::ZSTD, 3, "zstd3"},
                                    {segment_v2::ZSTD, 1, "zstd1"},
                                    {segment_v2::LZ4, 0, "lz4"},
                                    {segment_v2::LZ4F, 0, "lz4f"},
                                    {segment_v2::NO_COMPRESSION, 0, "none"}};
    return kCodecs[index];
}

inline uint64_t write_segment(const TabletSchemaSPtr& schema, const Block& block,
                              const Codec& codec, RowsetWriterContext* context,
                              const std::string& path) {
    io::FileWriterPtr file_writer;
    throw_if_error(io::global_local_filesystem()->create_file(path, &file_writer));
    segment_v2::VerticalSegmentWriterOptions options;
    options.rowset_ctx = context;
    options.write_type = DataWriteType::TYPE_DIRECT;
    options.compression_type = codec.type;
    ScopedZstdCompressionLevel level(codec.zstd_level);
    segment_v2::VerticalSegmentWriter writer(file_writer.get(), 0, schema, nullptr, nullptr,
                                             options, nullptr);
    throw_if_error(writer.init());
    throw_if_error(writer.batch_block(&block, 0, block.rows()));
    throw_if_error(writer.write_batch());
    uint64_t segment_size = 0;
    uint64_t index_size = 0;
    segment_v2::SegmentIndexFileCacheInfo cache_info;
    throw_if_error(writer.finalize(&segment_size, &index_size, &cache_info));
    throw_if_error(file_writer->close());
    return segment_size;
}

static void SegmentCompression(benchmark::State& state) {
    const Codec codec = codec_at(state.range(0));
    const auto rows = static_cast<size_t>(state.range(1));
    state.SetLabel(codec.label);
    const TabletSchemaSPtr schema = make_schema();
    const Block block = make_block(schema, rows);
    const std::string dir = "/tmp/doris_segment_compression_bench";
    throw_if_error(io::global_local_filesystem()->create_directory(dir));
    const std::string path = dir + "/" + codec.label + "_" + std::to_string(rows) + ".dat";
    RowsetWriterContext context;
    context.tablet_schema = schema;
    context.write_type = DataWriteType::TYPE_DIRECT;
    uint64_t segment_bytes = 0;
    for (auto _ : state) {
        segment_bytes = write_segment(schema, block, codec, &context, path);
        benchmark::DoNotOptimize(segment_bytes);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * block.bytes()));
    state.counters["segment_bytes"] = static_cast<double>(segment_bytes);
    state.counters["ratio"] = static_cast<double>(block.bytes()) /
                              static_cast<double>(std::max<uint64_t>(1, segment_bytes));
}

BENCHMARK(SegmentCompression)
        ->ArgsProduct({{0, 1, 2, 3, 4}, {65536, 262144}})
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();

// The memtable sorts shared_ptr<RowInBlock> per key column; the comparator either copies both
// pointers (two atomic increments and two decrements) or borrows them.
template <bool kByValue>
static void MemTableSortComparator(benchmark::State& state) {
    const auto rows = static_cast<size_t>(state.range(0));
    std::mt19937_64 rng(7 + state.thread_index());
    std::vector<int64_t> keys(rows);
    for (auto& key : keys) {
        key = static_cast<int64_t>(rng() % (rows / 8 + 1));
    }
    auto cmp = [&keys](RowInBlock* l, RowInBlock* r) {
        return keys[l->_row_pos] < keys[r->_row_pos] ? -1 : keys[l->_row_pos] > keys[r->_row_pos];
    };
    std::vector<std::shared_ptr<RowInBlock>> source;
    source.reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        source.push_back(std::make_shared<RowInBlock>(i));
    }
    std::shuffle(source.begin(), source.end(), rng);
    for (auto _ : state) {
        state.PauseTiming();
        auto rows_to_sort = source;
        state.ResumeTiming();
        if constexpr (kByValue) {
            pdqsort(rows_to_sort.begin(), rows_to_sort.end(),
                    [&cmp](auto lhs, auto rhs) -> bool { return cmp(lhs.get(), rhs.get()) < 0; });
        } else {
            pdqsort(rows_to_sort.begin(), rows_to_sort.end(),
                    [&cmp](const auto& lhs, const auto& rhs) -> bool {
                        return cmp(lhs.get(), rhs.get()) < 0;
                    });
        }
        benchmark::DoNotOptimize(rows_to_sort.data());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
}

BENCHMARK_TEMPLATE(MemTableSortComparator, true)
        ->Arg(1 << 18)
        ->Threads(1)
        ->Threads(32)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();
BENCHMARK_TEMPLATE(MemTableSortComparator, false)
        ->Arg(1 << 18)
        ->Threads(1)
        ->Threads(32)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();

// The vertical merge copies each same-source run into every output column; with interleaved
// sources the runs are a few rows long. The destination column is either taken through an
// owning MutablePtr (an atomic increment and decrement per run and column) or borrowed.
template <bool kOwning>
static void MergeCopyRowsColumnRef(benchmark::State& state) {
    const auto run_rows = static_cast<size_t>(state.range(0));
    constexpr size_t kColumns = 68;
    constexpr size_t kSourceRows = 4096;
    std::vector<ColumnPtr> sources;
    std::vector<ColumnPtr> destinations;
    for (size_t c = 0; c < kColumns; ++c) {
        auto source = ColumnInt64::create();
        for (size_t i = 0; i < kSourceRows; ++i) {
            source->insert_value(static_cast<int64_t>(i * 31 + c));
        }
        sources.emplace_back(std::move(source));
        destinations.emplace_back(ColumnInt64::create());
    }
    for (auto _ : state) {
        for (auto& destination : destinations) {
            destination->assert_mutable_ref().clear();
        }
        for (size_t start = 0; start + run_rows <= kSourceRows; start += run_rows) {
            for (size_t c = 0; c < kColumns; ++c) {
                if constexpr (kOwning) {
                    destinations[c]->assert_mutable()->insert_range_from(*sources[c], start,
                                                                         run_rows);
                } else {
                    destinations[c]->assert_mutable_ref().insert_range_from(*sources[c], start,
                                                                            run_rows);
                }
            }
        }
        benchmark::DoNotOptimize(destinations[0]->size());
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kSourceRows));
}

BENCHMARK_TEMPLATE(MergeCopyRowsColumnRef, true)
        ->Arg(1)
        ->Arg(4)
        ->Arg(64)
        ->Threads(1)
        ->Threads(32)
        ->UseRealTime();
BENCHMARK_TEMPLATE(MergeCopyRowsColumnRef, false)
        ->Arg(1)
        ->Arg(4)
        ->Arg(64)
        ->Threads(1)
        ->Threads(32)
        ->UseRealTime();

} // namespace doris::segment_compression_bench
