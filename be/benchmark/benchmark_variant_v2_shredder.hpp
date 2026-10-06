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

// ============================================================
// Benchmark: Variant V2 segment write path
//
// BM_VariantV2Shredder      VariantShredder append + finish, the memtable-flush shredding step.
// BM_VariantV2ColumnWriter  VariantV2ColumnWriter append through write_data: shredding plus the
//                           subcolumn page encoding and ZSTD compression of a real segment column.
//
// Input is synthetic OCSF-shaped JSON (nested objects, arrays of scalars and objects, JSON nulls,
// per-path type changes, ~150 common paths plus a row-unique long tail) with the production
// column settings: variant_max_subcolumns_count=2048, sparse shard count 1, sparse statistics
// size 10000, typed paths not forced to sparse.
//
// Arg 0 selects config::variant_v2_shredder_fast_path (0 = general path, 1 = fast path); arg 1 is
// rows per segment. Each benchmark is single threaded, so rows_per_core_s is rows per CPU second.
// Filters:
//   --benchmark_filter='BM_VariantV2Shredder|BM_VariantV2ColumnWriter'
// ============================================================

#pragma once

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/variant/v2/variant_column_writer.h"
#include "storage/segment/variant/v2/variant_shredder.h"
#include "storage/segment/variant/variant_writer_helpers.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace bench_variant_v2 {

constexpr size_t kBatchRows = 4096;
constexpr int32_t kMaxSubcolumns = 2048;

class ScopedFastPath {
public:
    explicit ScopedFastPath(bool enabled) : _old(config::variant_v2_shredder_fast_path) {
        config::variant_v2_shredder_fast_path = enabled;
    }
    ~ScopedFastPath() { config::variant_v2_shredder_fast_path = _old; }

private:
    bool _old;
};

inline std::string ip(std::mt19937_64& rng) {
    return "10." + std::to_string(rng() % 256) + "." + std::to_string(rng() % 256) + "." +
           std::to_string(rng() % 256);
}

inline std::string word(std::mt19937_64& rng, std::string_view prefix, size_t cardinality) {
    return std::string(prefix) + std::to_string(rng() % cardinality);
}

// One synthetic OCSF-like event. Field names follow the OCSF dictionary; values are random.
inline std::string ocsf_event(size_t row, std::mt19937_64& rng) {
    const auto q = [](const std::string& value) { return "\"" + value + "\""; };
    std::string j;
    j.reserve(2048);
    j += "{\"activity_id\":" + std::to_string(rng() % 6);
    j += ",\"category_uid\":" + std::to_string(rng() % 6 + 1);
    j += ",\"class_uid\":" + std::to_string(4000 + rng() % 7);
    j += ",\"type_uid\":" + std::to_string(400100 + rng() % 7);
    j += ",\"severity_id\":" + std::to_string(rng() % 6);
    j += ",\"status_id\":" + std::to_string(rng() % 3);
    j += ",\"time\":" + std::to_string(1760000000000 + row);
    j += ",\"duration\":" + std::to_string(rng() % 100000) + "." + std::to_string(rng() % 1000);
    j += ",\"message\":" + q("connection " + word(rng, "m", 100000) + " from " + ip(rng));
    j += ",\"metadata\":{\"version\":\"1.3.0\",\"uid\":" + q(word(rng, "uid-", 1u << 30)) +
         ",\"log_name\":" + q(word(rng, "log", 8)) + ",\"logged_time\":" +
         std::to_string(1760000000000 + row) + ",\"product\":{\"name\":" +
         q(word(rng, "product", 6)) + ",\"vendor_name\":" + q(word(rng, "vendor", 4)) +
         ",\"version\":" + q(word(rng, "v", 12)) + ",\"feature\":{\"name\":" +
         q(word(rng, "feature", 10)) + "}},\"labels\":[" + q(word(rng, "l", 5)) + "," +
         q(word(rng, "l", 5)) + "],\"profiles\":[\"host\",\"security_control\"]}";
    j += ",\"src_endpoint\":{\"ip\":" + q(ip(rng)) + ",\"port\":" + std::to_string(rng() % 65536) +
         ",\"hostname\":" + q(word(rng, "host-", 5000)) + ",\"mac\":" + q(word(rng, "mac", 5000)) +
         ",\"location\":{\"country\":" + q(word(rng, "C", 50)) + ",\"city\":" +
         q(word(rng, "city", 500)) + ",\"lat\":" + std::to_string(rng() % 90) + ".5,\"long\":" +
         std::to_string(rng() % 180) + ".25}}";
    j += ",\"dst_endpoint\":{\"ip\":" + q(ip(rng)) + ",\"port\":" + std::to_string(rng() % 65536) +
         ",\"svc_name\":" + q(word(rng, "svc", 40)) + "}";
    j += ",\"connection_info\":{\"protocol_name\":" + q(rng() % 2 ? "tcp" : "udp") +
         ",\"protocol_num\":" + std::to_string(rng() % 2 ? 6 : 17) + ",\"direction_id\":" +
         std::to_string(rng() % 3) + ",\"boundary_id\":" + std::to_string(rng() % 4) + "}";
    j += ",\"traffic\":{\"bytes_in\":" + std::to_string(rng() % 1000000) + ",\"bytes_out\":" +
         std::to_string(rng() % 1000000) + ",\"packets_in\":" + std::to_string(rng() % 1000) +
         ",\"packets_out\":" + std::to_string(rng() % 1000) + "}";
    if (rng() % 4 != 0) {
        j += ",\"actor\":{\"user\":{\"name\":" + q(word(rng, "user", 2000)) + ",\"uid\":" +
             q(word(rng, "S-1-5-21-", 1u << 20)) + ",\"domain\":" + q(word(rng, "dom", 5)) +
             ",\"type_id\":" + std::to_string(rng() % 3) + ",\"groups\":[{\"name\":" +
             q(word(rng, "g", 30)) + "},{\"name\":" + q(word(rng, "g", 30)) +
             "}]},\"process\":{\"pid\":" + std::to_string(rng() % 65536) + ",\"name\":" +
             q(word(rng, "proc", 300)) + ",\"cmd_line\":" +
             q("/usr/bin/" + word(rng, "proc", 300) + " --flag " + word(rng, "arg", 1000)) +
             ",\"file\":{\"path\":" + q("/usr/bin/" + word(rng, "proc", 300)) +
             ",\"hashes\":[{\"algorithm_id\":3,\"value\":" + q(word(rng, "sha", 1u << 30)) +
             "}]}},\"session\":{\"uid\":" + q(word(rng, "sess", 1u << 20)) +
             ",\"is_remote\":" + (rng() % 2 ? "true" : "false") + "}}";
    } else {
        j += ",\"actor\":null";
    }
    j += ",\"device\":{\"hostname\":" + q(word(rng, "dev", 3000)) + ",\"ip\":" + q(ip(rng)) +
         ",\"os\":{\"name\":" + q(word(rng, "os", 6)) + ",\"type_id\":" +
         std::to_string(rng() % 4) + ",\"version\":" + q(word(rng, "osv", 30)) + "}}";
    // Type changes: an integer that sometimes arrives as a string, a score that is an int or a
    // double, a flag that is a bool or an int.
    j += ",\"risk_score\":" +
         (rng() % 10 == 0 ? q(word(rng, "r", 10)) : std::to_string(rng() % 100));
    j += ",\"confidence\":" +
         (rng() % 2 ? std::to_string(rng() % 100) : std::to_string(rng() % 100) + ".5");
    j += ",\"is_alert\":" + std::string(rng() % 7 == 0 ? "1" : (rng() % 2 ? "true" : "false"));
    j += ",\"observables\":[{\"name\":\"src_endpoint.ip\",\"type_id\":2,\"value\":" + q(ip(rng)) +
         "},{\"name\":\"actor.user.name\",\"type_id\":4,\"value\":" + q(word(rng, "user", 2000)) +
         "}]";
    j += ",\"enrichments\":[{\"name\":\"geo\",\"value\":" + q(word(rng, "C", 50)) +
         ",\"data\":{\"asn\":" + std::to_string(rng() % 65536) + "}}]";
    j += ",\"category_names\":[" + q(word(rng, "cat", 8)) + "," + q(word(rng, "cat", 8)) + "]";
    j += ",\"ports\":[" + std::to_string(rng() % 65536) + "," + std::to_string(rng() % 65536) + "]";
    // Vendor passthrough: a few dozen frequent keys plus a sparse long tail that exceeds the
    // 2048-subcolumn budget over a segment.
    j += ",\"unmapped\":{";
    for (int field = 0; field < 12; ++field) {
        if (field != 0) {
            j += ",";
        }
        j += "\"" + word(rng, "vendor_field_", 60) + "_" + std::to_string(field) + "\":" +
             q(word(rng, "val", 1000));
    }
    j += ",\"tail_" + std::to_string(rng() % 20000) + "\":" + std::to_string(rng() % 1000);
    j += ",\"nullable\":null}";
    j += "}";
    return j;
}

// Built once per row count and shared by both benchmarks and both modes.
inline const ColumnVariantV2& corpus(size_t rows) {
    static std::map<size_t, ColumnVariantV2::MutablePtr> cache;
    auto& slot = cache[rows];
    if (!slot) {
        slot = ColumnVariantV2::create();
        DataTypeVariantV2SerDe serde;
        DataTypeSerDe::FormatOptions options;
        std::mt19937_64 rng(0x5eed0cf5);
        for (size_t row = 0; row < rows; ++row) {
            const std::string json = ocsf_event(row, rng);
            Slice slice(json.data(), json.size());
            const Status status = serde.deserialize_one_cell_from_json(*slot, slice, options);
            DORIS_CHECK(status.ok()) << status.to_string();
        }
    }
    return *slot;
}

inline std::vector<uint8_t> outer_nulls(size_t rows) {
    // The production columns are nullable; a small fraction of SQL NULL rows.
    std::vector<uint8_t> nulls(rows, 0);
    for (size_t row = 0; row < rows; row += 50) {
        nulls[row] = 1;
    }
    return nulls;
}

inline TabletSchemaSPtr make_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    ColumnPB* column = schema_pb.add_column();
    column->set_unique_id(1);
    column->set_name("event");
    column->set_type("VARIANT");
    column->set_is_key(false);
    column->set_is_nullable(true);
    column->set_variant_max_subcolumns_count(kMaxSubcolumns);
    column->set_variant_enable_typed_paths_to_sparse(false);
    column->set_variant_max_sparse_column_statistics_size(10000);
    column->set_variant_sparse_hash_shard_count(1);
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

inline void set_rate_counters(benchmark::State& state, size_t rows) {
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    // CPU-time rate of a single-threaded benchmark: rows per core-second.
    state.counters["rows_per_core_s"] = benchmark::Counter(
            static_cast<double>(rows), benchmark::Counter::kIsIterationInvariantRate);
}

} // namespace bench_variant_v2

static void BM_VariantV2Shredder(benchmark::State& state) {
    using namespace bench_variant_v2;
    const bool fast_path = state.range(0) != 0;
    const auto rows = static_cast<size_t>(state.range(1));
    const ColumnVariantV2& values = corpus(rows);
    const std::vector<uint8_t> nulls = outer_nulls(rows);
    const ScopedFastPath scoped(fast_path);
    const VariantShredderOptions options {
            .max_subcolumns_count = kMaxSubcolumns,
            .typed_paths_to_sparse = false,
            .sparse_bucket_count = 1,
            .max_sparse_column_statistics_size = 10000,
    };
    const auto view = values.read_view();
    for (auto _ : state) {
        VariantShredder shredder(options);
        for (size_t begin = 0; begin < rows; begin += kBatchRows) {
            const size_t count = std::min(kBatchRows, rows - begin);
            const Status status = shredder.append(
                    view, begin, count, std::span<const uint8_t>(nulls).subspan(begin, count));
            DORIS_CHECK(status.ok()) << status.to_string();
        }
        VariantShreddedColumns shredded;
        const Status status = shredder.finish(&shredded);
        DORIS_CHECK(status.ok()) << status.to_string();
        benchmark::DoNotOptimize(shredded.materialized.data());
        state.counters["materialized_paths"] = static_cast<double>(shredded.materialized.size());
    }
    set_rate_counters(state, rows);
}

static void BM_VariantV2ColumnWriter(benchmark::State& state) {
    using namespace bench_variant_v2;
    const bool fast_path = state.range(0) != 0;
    const auto rows = static_cast<size_t>(state.range(1));
    const ColumnVariantV2& values = corpus(rows);
    const std::vector<uint8_t> nulls = outer_nulls(rows);
    const ScopedFastPath scoped(fast_path);
    const TabletSchemaSPtr schema = make_schema();
    const TabletColumn& column = schema->column(0);
    const std::string path = "./variant_v2_column_writer_bench.dat";

    for (auto _ : state) {
        state.PauseTiming();
        static_cast<void>(io::global_local_filesystem()->delete_file(path));
        io::FileWriterPtr file_writer;
        Status status = io::global_local_filesystem()->create_file(path, &file_writer);
        DORIS_CHECK(status.ok()) << status.to_string();
        SegmentFooterPB footer;
        RowsetWriterContext rowset_ctx;
        rowset_ctx.write_type = DataWriteType::TYPE_DIRECT;
        rowset_ctx.tablet_schema = schema;
        ColumnWriterOptions opts;
        opts.meta = footer.add_columns();
        opts.compression_type = CompressionTypePB::ZSTD;
        opts.file_writer = file_writer.get();
        opts.footer = &footer;
        opts.rowset_ctx = &rowset_ctx;
        opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
        variant_writer_helpers::init_column_meta(opts.meta, 0, column, opts);
        state.ResumeTiming();

        VariantV2ColumnWriter writer(opts, &column);
        status = writer.init();
        DORIS_CHECK(status.ok()) << status.to_string();
        for (size_t begin = 0; begin < rows; begin += kBatchRows) {
            const size_t count = std::min(kBatchRows, rows - begin);
            status = writer.append(VariantColumnData {.column_data = &values, .row_pos = begin},
                                   count, std::span<const uint8_t>(nulls).subspan(begin, count));
            DORIS_CHECK(status.ok()) << status.to_string();
        }
        status = writer.finish();
        DORIS_CHECK(status.ok()) << status.to_string();
        status = writer.write_data();
        DORIS_CHECK(status.ok()) << status.to_string();
        status = writer.write_ordinal_index();
        DORIS_CHECK(status.ok()) << status.to_string();
        status = writer.write_zone_map();
        DORIS_CHECK(status.ok()) << status.to_string();

        state.PauseTiming();
        status = file_writer->close();
        DORIS_CHECK(status.ok()) << status.to_string();
        state.counters["segment_bytes"] = static_cast<double>(file_writer->bytes_appended());
        state.ResumeTiming();
    }
    static_cast<void>(io::global_local_filesystem()->delete_file(path));
    set_rate_counters(state, rows);
}

BENCHMARK(BM_VariantV2Shredder)
        ->ArgNames({"fast", "rows"})
        ->ArgsProduct({{0, 1}, {16384, 131072}})
        ->Unit(benchmark::kMillisecond);
BENCHMARK(BM_VariantV2ColumnWriter)
        ->ArgNames({"fast", "rows"})
        ->ArgsProduct({{0, 1}, {16384, 131072}})
        ->Unit(benchmark::kMillisecond);

} // namespace doris::segment_v2
