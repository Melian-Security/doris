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

// Cumulative vertical compaction of rowsets loaded through the Variant V2 writer.
//
// Each input rowset holds one load: a BIGINT sort key, a STRING column and a nullable VARIANT
// column of synthetic OCSF-like events (bench_variant_v2::ocsf_event). Keys interleave across
// rowsets the way concurrent loads covering the same time range do, so the key merge alternates
// sources on almost every row. The timed region is the production compaction body:
// get_extended_compaction_schema, Merger::vertical_merge_rowsets and RowsetWriter::build.
//
// rows_per_core_s is input rows per CPU second of the compacting thread. Settings are applied
// through config::set_config so one binary compares them; a setting that does not exist in the
// binary skips the case.
//
// V2COMP_ARROW_FILE=<Arrow IPC stream> replaces the synthetic events with a nullable Variant
// column read through the Variant V2 Arrow reader. The first column named by V2COMP_ARROW_COLUMN
// (default "event") is used; rows are repeated to fill each rowset.

#pragma once

#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <benchmark/benchmark.h>
#include <cctz/time_zone.h>

#include <cstdlib>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "benchmark_variant_v2_shredder.hpp"
#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "exec/common/variant_util.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/cache/page_cache.h"
#include "storage/data_dir.h"
#include "storage/merger.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/segment/segment_loader.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "util/cpu_info.h"

namespace doris {
namespace bench_variant_v2_compaction {

constexpr int32_t kMaxSubcolumns = 2048;

struct Env {
    StorageEngine* engine = nullptr;
    std::unique_ptr<DataDir> data_dir;
    std::string root;
};

inline Env& env() {
    // Never destroyed: the storage engine and caches are process-lifetime singletons.
    static Env* instance = [] {
        auto* e = new Env();
        CpuInfo::init();
        if (ExecEnv::GetInstance()->segment_loader() == nullptr) {
            ExecEnv::GetInstance()->set_segment_loader(new SegmentLoader(1000, 1000));
        }
        if (ExecEnv::GetInstance()->get_storage_page_cache() == nullptr) {
            ExecEnv::GetInstance()->set_storage_page_cache(
                    StoragePageCache::create_global_cache(1 << 30, 10, 0));
        }
        e->root = std::filesystem::absolute("./variant_v2_compaction_bench").string();
        static_cast<void>(io::global_local_filesystem()->delete_directory(e->root));
        DORIS_CHECK(io::global_local_filesystem()->create_directory(e->root).ok());
        auto engine = std::make_unique<StorageEngine>(EngineOptions {});
        e->engine = engine.get();
        e->data_dir = std::make_unique<DataDir>(*e->engine, e->root);
        static_cast<void>(e->data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        return e;
    }();
    return *instance;
}

inline TabletSchemaSPtr make_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    schema_pb.set_compression_type(segment_v2::CompressionTypePB::ZSTD);
    schema_pb.set_num_short_key_columns(1);
    ColumnPB* key = schema_pb.add_column();
    key->set_unique_id(0);
    key->set_name("ts");
    key->set_type("BIGINT");
    key->set_is_key(true);
    key->set_is_nullable(false);
    key->set_length(8);
    ColumnPB* org = schema_pb.add_column();
    org->set_unique_id(1);
    org->set_name("org");
    org->set_type("STRING");
    org->set_is_key(false);
    org->set_is_nullable(false);
    org->set_length(2147483643);
    org->set_aggregation("NONE");
    ColumnPB* event = schema_pb.add_column();
    event->set_unique_id(2);
    event->set_name("event");
    event->set_type("VARIANT");
    event->set_is_key(false);
    event->set_is_nullable(true);
    event->set_aggregation("NONE");
    event->set_variant_max_subcolumns_count(kMaxSubcolumns);
    event->set_variant_enable_typed_paths_to_sparse(false);
    event->set_variant_max_sparse_column_statistics_size(10000);
    event->set_variant_sparse_hash_shard_count(1);
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

// The shredder corpus gives every row one of 20000 rare `unmapped.tail_N` paths so a single
// segment overflows the subcolumn budget. Across the rowsets of one compaction that would
// materialize ~2000 near-empty subcolumns, while production events carry 100-300 paths; keep
// 64 tail paths so the compaction schema has the production shape.
inline void bound_tail_paths(std::string* json) {
    constexpr std::string_view marker = "\"tail_";
    const size_t begin = json->find(marker);
    if (begin == std::string::npos) {
        return;
    }
    const size_t digits = begin + marker.size();
    size_t end = digits;
    while (end < json->size() && (*json)[end] >= '0' && (*json)[end] <= '9') {
        ++end;
    }
    const auto id = std::stoul(json->substr(digits, end - digits));
    json->replace(digits, end - digits, std::to_string(id % 64));
}

// Nullable<ColumnVariantV2> event column for `rows` rows.
inline ColumnPtr synthetic_events(size_t rows, uint64_t seed) {
    auto values = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions options;
    std::mt19937_64 rng(seed);
    auto nulls = ColumnUInt8::create();
    for (size_t row = 0; row < rows; ++row) {
        std::string json = segment_v2::bench_variant_v2::ocsf_event(row, rng);
        bound_tail_paths(&json);
        Slice slice(json.data(), json.size());
        const Status status = serde.deserialize_one_cell_from_json(*values, slice, options);
        DORIS_CHECK(status.ok()) << status.to_string();
        nulls->insert_value(row % 50 == 0 ? 1 : 0);
    }
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

// Rows of one Variant column from an Arrow IPC stream, or nullptr when no file is configured.
inline ColumnPtr arrow_events() {
    static ColumnPtr cached = []() -> ColumnPtr {
        const char* path = std::getenv("V2COMP_ARROW_FILE");
        if (path == nullptr) {
            return nullptr;
        }
        const char* name = std::getenv("V2COMP_ARROW_COLUMN");
        const std::string column_name = name == nullptr ? "event" : name;
        auto file = arrow::io::ReadableFile::Open(path).ValueOrDie();
        auto reader = arrow::ipc::RecordBatchStreamReader::Open(file).ValueOrDie();
        const DataTypePtr type = make_nullable(std::make_shared<DataTypeVariantV2>(kMaxSubcolumns));
        auto column = type->create_column();
        cctz::time_zone tz = cctz::utc_time_zone();
        std::shared_ptr<arrow::RecordBatch> batch;
        while (reader->ReadNext(&batch).ok() && batch) {
            auto array = batch->GetColumnByName(column_name);
            DORIS_CHECK(array != nullptr) << "missing Arrow column " << column_name;
            const Status status = type->get_serde()->read_column_from_arrow(
                    *column, array.get(), 0, array->length(), tz);
            DORIS_CHECK(status.ok()) << status.to_string();
        }
        return column;
    }();
    return cached;
}

inline ColumnPtr events(size_t rows, uint64_t seed) {
    const ColumnPtr source = arrow_events();
    if (!source || source->empty()) {
        return synthetic_events(rows, seed);
    }
    auto column = source->clone_empty();
    const size_t offset = (seed * 7919) % source->size();
    for (size_t filled = 0; filled < rows;) {
        const size_t start = (offset + filled) % source->size();
        const size_t count = std::min(rows - filled, source->size() - start);
        column->insert_range_from(*source, start, count);
        filled += count;
    }
    return column;
}

struct Input {
    TabletSchemaSPtr schema;
    TabletSharedPtr tablet;
    std::vector<RowsetSharedPtr> rowsets;
    size_t rows = 0;
};

// `num_rowsets` loads of `rows_per_rowset` rows. With interleave, rowset r holds keys
// r, r + num_rowsets, ...; otherwise rowset r holds one contiguous key range.
inline const Input& input(int64_t num_rowsets, int64_t rows_per_rowset, bool interleave) {
    static std::map<std::tuple<int64_t, int64_t, bool>, std::unique_ptr<Input>> cache;
    auto& slot = cache[{num_rowsets, rows_per_rowset, interleave}];
    if (slot) {
        return *slot;
    }
    Env& e = env();
    slot = std::make_unique<Input>();
    slot->schema = make_schema();
    TabletMetaSharedPtr tablet_meta(new TabletMeta(slot->schema));
    static int64_t next_tablet_id = 880000;
    tablet_meta->_tablet_id = next_tablet_id++;
    slot->tablet = std::make_shared<Tablet>(*e.engine, tablet_meta, e.data_dir.get());
    DORIS_CHECK(slot->tablet->init().ok());
    static_cast<void>(io::global_local_filesystem()->delete_directory(slot->tablet->tablet_path()));
    DORIS_CHECK(io::global_local_filesystem()->create_directory(slot->tablet->tablet_path()).ok());

    const DataTypePtr key_type = std::make_shared<DataTypeInt64>();
    const DataTypePtr org_type = std::make_shared<DataTypeString>();
    const DataTypePtr event_type =
            make_nullable(std::make_shared<DataTypeVariantV2>(kMaxSubcolumns));
    for (int64_t r = 0; r < num_rowsets; ++r) {
        RowsetWriterContext ctx;
        RowsetId rowset_id;
        rowset_id.init(tablet_meta->_tablet_id * 1000 + r);
        ctx.rowset_id = rowset_id;
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = e.data_dir.get();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = slot->schema;
        ctx.tablet_path = slot->tablet->tablet_path();
        ctx.tablet_id = slot->tablet->tablet_id();
        ctx.tablet = slot->tablet;
        ctx.version = Version(r + 2, r + 2);
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = static_cast<uint32_t>(rows_per_rowset);
        ctx.write_type = DataWriteType::TYPE_DIRECT;
        auto writer = std::move(RowsetFactory::create_rowset_writer(*e.engine, ctx, false)).value();

        auto keys = ColumnInt64::create();
        auto orgs = ColumnString::create();
        for (int64_t row = 0; row < rows_per_rowset; ++row) {
            keys->insert_value(interleave ? row * num_rowsets + r : r * rows_per_rowset + row);
            const std::string org = "org-" + std::to_string(row % 17);
            orgs->insert_data(org.data(), org.size());
        }
        Block block;
        block.insert({std::move(keys), key_type, "ts"});
        block.insert({std::move(orgs), org_type, "org"});
        block.insert({events(rows_per_rowset, 0x5eed + r), event_type, "event"});
        Status st = writer->add_block(&block);
        DORIS_CHECK(st.ok()) << st.to_string();
        st = writer->flush();
        DORIS_CHECK(st.ok()) << st.to_string();
        RowsetSharedPtr rowset;
        st = writer->build(rowset);
        DORIS_CHECK(st.ok()) << st.to_string();
        slot->rowsets.push_back(std::move(rowset));
        slot->rows += rows_per_rowset;
    }
    return *slot;
}

// One compaction of `in.rowsets`; returns the output rowset.
inline RowsetSharedPtr compact(const Input& in, int64_t sequence) {
    Env& e = env();
    std::vector<RowsetReaderSharedPtr> readers;
    for (const auto& rowset : in.rowsets) {
        RowsetReaderSharedPtr reader;
        DORIS_CHECK(rowset->create_reader(&reader).ok());
        readers.push_back(std::move(reader));
    }
    auto schema = std::make_shared<TabletSchema>(*in.schema);
    Status st = variant_util::VariantCompactionUtil::get_extended_compaction_schema(in.rowsets,
                                                                                   schema);
    DORIS_CHECK(st.ok()) << st.to_string();

    RowsetWriterContext ctx;
    RowsetId rowset_id;
    rowset_id.init(in.tablet->tablet_id() * 1000 + 500 + sequence);
    ctx.rowset_id = rowset_id;
    ctx.rowset_type = BETA_ROWSET;
    ctx.data_dir = e.data_dir.get();
    ctx.rowset_state = VISIBLE;
    ctx.tablet_schema = schema;
    ctx.tablet_path = in.tablet->tablet_path();
    ctx.tablet_id = in.tablet->tablet_id();
    ctx.tablet = in.tablet;
    ctx.version = Version(in.rowsets.front()->start_version(), in.rowsets.back()->end_version());
    ctx.segments_overlap = NONOVERLAPPING;
    ctx.write_type = DataWriteType::TYPE_COMPACTION;
    ctx.compaction_type = ReaderType::READER_CUMULATIVE_COMPACTION;
    auto writer = std::move(RowsetFactory::create_rowset_writer(*e.engine, ctx, true)).value();

    Merger::Statistics stats;
    st = Merger::vertical_merge_rowsets(in.tablet, ReaderType::READER_CUMULATIVE_COMPACTION,
                                        *schema, readers, writer.get(),
                                        static_cast<uint32_t>(in.rows), in.rowsets.size(), &stats);
    DORIS_CHECK(st.ok()) << st.to_string();
    RowsetSharedPtr output;
    st = writer->build(output);
    DORIS_CHECK(st.ok()) << st.to_string();
    return output;
}

// Every case sets all knobs so no case inherits another's settings. A knob missing from the binary
// (an older build) fails set_config, and the case is skipped unless it uses the knob's default.
struct Knob {
    const char* name;
    const char* default_value;
};
constexpr Knob kKnobs[] = {
        {"enable_vertical_compaction_gather_copy", "true"},
        {"zstd_compression_level", "3"},
        {"vertical_compaction_num_columns_per_group", "5"},
};

inline std::map<std::string, std::string> settings_for(int64_t mode) {
    switch (mode) {
    case 0: // shipping defaults
        return {};
    case 1:
        return {{"zstd_compression_level", "1"}};
    case 2:
        return {{"zstd_compression_level", "-1"}};
    case 3:
        return {{"vertical_compaction_num_columns_per_group", "20"}};
    case 4:
        return {{"vertical_compaction_num_columns_per_group", "20"}, {"zstd_compression_level", "1"}};
    case 5:
        return {{"enable_vertical_compaction_gather_copy", "false"}};
    case 6:
        return {{"vertical_compaction_num_columns_per_group", "50"}};
    default:
        return {};
    }
}

// Returns false when a non-default setting of `mode` is unavailable in this binary.
inline bool apply_settings(int64_t mode) {
    const auto settings = settings_for(mode);
    for (const auto& knob : kKnobs) {
        const auto it = settings.find(knob.name);
        const bool requested = it != settings.end();
        const std::string value = requested ? it->second : knob.default_value;
        if (!config::set_config(knob.name, value).ok() && requested) {
            return false;
        }
    }
    return true;
}

} // namespace bench_variant_v2_compaction

static void BM_VariantV2Compaction(benchmark::State& state) {
    using namespace bench_variant_v2_compaction;
    const int64_t mode = state.range(0);
    const bool interleave = state.range(1) != 0;
    const int64_t num_rowsets = state.range(2);
    const int64_t rows_per_rowset = state.range(3);
    if (!apply_settings(mode)) {
        state.SkipWithError("setting not available in this binary");
        return;
    }
    const Input& in = input(num_rowsets, rows_per_rowset, interleave);
    int64_t sequence = 0;
    int64_t output_bytes = 0;
    for (auto _ : state) {
        RowsetSharedPtr output = compact(in, sequence++);
        state.PauseTiming();
        DORIS_CHECK_EQ(output->num_rows(), in.rows);
        output_bytes = output->data_disk_size();
        static_cast<void>(output->remove());
        state.ResumeTiming();
    }
    int64_t input_bytes = 0;
    for (const auto& rowset : in.rowsets) {
        input_bytes += rowset->data_disk_size();
    }
    state.counters["rows_per_core_s"] = benchmark::Counter(
            static_cast<double>(in.rows), benchmark::Counter::kIsIterationInvariantRate);
    state.counters["input_MB"] = static_cast<double>(input_bytes) / 1e6;
    state.counters["output_MB"] = static_cast<double>(output_bytes) / 1e6;
}

BENCHMARK(BM_VariantV2Compaction)
        ->ArgNames({"mode", "interleave", "rowsets", "rows"})
        ->ArgsProduct({{0, 1, 2, 3, 4, 5, 6}, {1}, {8}, {32768}})
        ->Args({0, 0, 8, 32768})
        ->Args({5, 0, 8, 32768})
        ->Unit(benchmark::kMillisecond)
        ->Iterations(2);

} // namespace doris
