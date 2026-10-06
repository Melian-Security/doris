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
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>

#ifdef USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cloud/cloud_delta_writer.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "io/fs/s3_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_profile.h"
#include "runtime/thread_context.h"
#include "storage/delete/calc_delete_bitmap_executor.h"
#include "storage/tablet/tablet_column_object_pool.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema_cache.h"
#include "storage/tablet_info.h"

// Per-load BE cost of the tablets a cloud-mode load never writes.
//
// A Stream Load with `load_to_single_tablet=true` into a RANDOM-bucket table writes one tablet of
// the partition, and CloudTabletsChannel::close() commits an empty rowset to each of the other
// tablets. One iteration models the close of such a load for the 31 untouched tablets of a
// 32-bucket partition: construct the delta writers as the channel's open does, then run
// commit_rowset, the delete bitmap hooks, set_txn_related_info and update_tablet_stats on each
// and destroy them. The touched tablet's own memtable/flush/commit cost is the same with and
// without the change and is left out.
//
// Args:
//   fix                 0: rowset writer built per untouched tablet (stock 4.1.4),
//                       1: config::skip_rowset_writer_for_empty_tablet.
//   meta_lock_hold_us   0: no contention. > 0: a background thread holds the exclusive meta lock
//                       (`get_header_lock()`) of one untouched tablet after another for this
//                       long, at a 50% duty cycle, as sync_rowsets/fill_version_holes do on
//                       tablets with tens of thousands of rowsets.
//
// Counters (per load):
//   rowset_writers      rowset writers built for untouched tablets
//   alloc_bytes         bytes allocated on the benchmark thread (jemalloc builds only)
//   ms_rpcs             meta-service RPCs: 0 in both modes here, since the tablets are cached and
//                       skip_writing_empty_rowset_metadata is on (its default). Stock 4.1.4 adds
//                       get_tablet_meta + sync_tablet_rowsets per untouched tablet on a tablet
//                       cache miss, and prepare_rowset + commit_rowset per untouched tablet when
//                       skip_writing_empty_rowset_metadata is off; this change keeps both.
//
// Run: benchmark_test --benchmark_filter=BM_CloudUntouchedTabletsClose
namespace doris::cloud_untouched_tablets_bench {

constexpr int kNumTablets = 32;
constexpr int kNumUntouched = kNumTablets - 1;
constexpr int kNumColumns = 48;
constexpr int64_t kDbId = 90000;
constexpr int64_t kTableId = 90001;
constexpr int64_t kPartitionId = 90002;
constexpr int64_t kIndexId = 90003;
constexpr int32_t kSchemaHash = 90004;
constexpr int64_t kFirstTabletId = 91000;

struct BenchEnv {
    std::unique_ptr<CloudStorageEngine> engine;
    std::vector<std::shared_ptr<CloudTablet>> tablets;
    std::shared_ptr<OlapTableSchemaParam> schema_param;
    std::string error;
};

// A DUPLICATE KEY log table shaped like the delta POC's: 3 key columns, then a mix of strings,
// integers and datetimes.
inline TTabletSchema make_tablet_schema() {
    TTabletSchema schema;
    schema.keys_type = TKeysType::DUP_KEYS;
    schema.short_key_column_count = 3;
    schema.schema_hash = kSchemaHash;
    schema.storage_type = TStorageType::COLUMN;
    for (int i = 0; i < kNumColumns; ++i) {
        TColumn column;
        column.column_name = "c" + std::to_string(i);
        column.__set_col_unique_id(i);
        column.__set_is_key(i < 3);
        column.__set_is_allow_null(i >= 3);
        column.__set_aggregation_type(TAggregationType::NONE);
        switch (i % 3) {
        case 0:
            column.column_type.type = TPrimitiveType::VARCHAR;
            column.column_type.__set_len(256);
            break;
        case 1:
            column.column_type.type = TPrimitiveType::BIGINT;
            break;
        default:
            column.column_type.type = TPrimitiveType::DATETIMEV2;
            break;
        }
        schema.columns.push_back(std::move(column));
    }
    return schema;
}

inline BenchEnv* make_env() {
    auto* env = new BenchEnv();
    auto* exec_env = ExecEnv::GetInstance();
    if (exec_env->get_cache_manager() == nullptr) {
        exec_env->set_cache_manager(CacheManager::create_global_instance());
    }
    if (exec_env->get_tablet_schema_cache() == nullptr) {
        exec_env->set_tablet_schema_cache(TabletSchemaCache::create_global_schema_cache(
                config::tablet_schema_cache_capacity));
    }
    if (exec_env->get_tablet_column_object_pool() == nullptr) {
        exec_env->set_tablet_column_object_pool(TabletColumnObjectPool::create_global_column_cache(
                config::tablet_schema_cache_capacity));
    }

    env->engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
    auto& engine = *env->engine;
    engine._calc_delete_bitmap_executor = std::make_unique<CalcDeleteBitmapExecutor>();
    engine._calc_delete_bitmap_executor->init("BenchCalcDeleteBitmap", 1);
    engine._txn_delete_bitmap_cache = std::make_unique<CloudTxnDeleteBitmapCache>(1 << 20);
    if (auto st = engine._txn_delete_bitmap_cache->init(); !st.ok()) {
        env->error = st.to_string();
        return env;
    }
    engine._committed_rs_mgr = std::make_unique<CloudCommittedRSMgr>();
    if (auto st = engine._committed_rs_mgr->init(); !st.ok()) {
        env->error = st.to_string();
        return env;
    }

    // The rowset writer of an empty rowset never touches the file system, it only needs one.
    S3Conf s3_conf;
    s3_conf.client_conf.ak = "fake_ak";
    s3_conf.client_conf.sk = "fake_sk";
    s3_conf.client_conf.endpoint = "fake_s3_endpoint";
    s3_conf.client_conf.region = "fake_s3_region";
    s3_conf.bucket = "fake_s3_bucket";
    s3_conf.prefix = "cloud_untouched_tablets_bench";
    auto fs = io::S3FileSystem::create(std::move(s3_conf), "cloud-untouched-tablets-bench-fs");
    if (!fs.has_value()) {
        env->error = fs.error().to_string();
        return env;
    }
    engine.set_latest_fs(fs.value());

    const auto tablet_schema = make_tablet_schema();
    for (int i = 0; i < kNumTablets; ++i) {
        int64_t tablet_id = kFirstTabletId + i;
        TabletMetaSharedPtr meta(new TabletMeta(
                kTableId, kPartitionId, tablet_id, tablet_id, kSchemaHash, 0, tablet_schema,
                kNumColumns, std::unordered_map<uint32_t, uint32_t> {}, UniqueId(tablet_id, 1),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::ZSTD, /*storage_policy_id=*/0,
                /*enable_unique_key_merge_on_write=*/false, /*binlog_config=*/std::nullopt,
                /*compaction_policy=*/"time_series"));
        auto tablet = std::make_shared<CloudTablet>(engine, std::move(meta));
        engine.tablet_mgr().put_tablet_in_cache_for_UT(tablet);
        env->tablets.push_back(std::move(tablet));
    }

    TOlapTableSchemaParam tschema;
    tschema.db_id = kDbId;
    tschema.table_id = kTableId;
    tschema.version = 0;
    tschema.indexes.resize(1);
    tschema.indexes[0].id = kIndexId;
    tschema.indexes[0].schema_hash = kSchemaHash;
    env->schema_param = std::make_shared<OlapTableSchemaParam>();
    if (auto st = env->schema_param->init(tschema); !st.ok()) {
        env->error = st.to_string();
    }
    return env;
}

// Intentionally leaked: the engine's caches and thread pools outlive every benchmark.
inline BenchEnv* bench_env() {
    static BenchEnv* env = make_env();
    return env;
}

inline uint64_t thread_allocated_bytes() {
#ifdef USE_JEMALLOC
    uint64_t allocated = 0;
    size_t size = sizeof(allocated);
    if (jemallctl("thread.allocated", &allocated, &size, nullptr, 0) == 0) {
        return allocated;
    }
#endif
    return 0;
}

// Holds the exclusive meta lock of the untouched tablets one after another, like the sync and
// fill-version-holes paths do while they rebuild a large tablet's rowset list.
class MetaLockHolder {
public:
    MetaLockHolder(const std::vector<std::shared_ptr<CloudTablet>>& tablets, int64_t hold_us)
            : _tablets(tablets), _hold_us(hold_us) {
        if (_hold_us > 0) {
            _thread = std::thread([this] { run(); });
        }
    }

    ~MetaLockHolder() {
        _stop.store(true);
        if (_thread.joinable()) {
            _thread.join();
        }
    }

private:
    void run() {
        size_t next = 1; // tablet 0 is the one the load writes
        while (!_stop.load()) {
            {
                std::unique_lock lock(_tablets[next]->get_header_lock());
                spin_for(_hold_us);
            }
            next = next + 1 < _tablets.size() ? next + 1 : 1;
            spin_for(_hold_us);
        }
    }

    static void spin_for(int64_t us) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(us);
        while (std::chrono::steady_clock::now() < deadline) {
        }
    }

    const std::vector<std::shared_ptr<CloudTablet>>& _tablets;
    const int64_t _hold_us;
    std::atomic<bool> _stop {false};
    std::thread _thread;
};

inline void BM_CloudUntouchedTabletsClose(benchmark::State& state) {
    auto* env = bench_env();
    if (!env->error.empty()) {
        state.SkipWithError(env->error.c_str());
        return;
    }
    const bool fix = state.range(0) != 0;
    const int64_t meta_lock_hold_us = state.range(1);

    const bool saved_skip_metadata = config::skip_writing_empty_rowset_metadata;
    const bool saved_skip_writer = config::skip_rowset_writer_for_empty_tablet;
    config::skip_writing_empty_rowset_metadata = true;
    config::skip_rowset_writer_for_empty_tablet = fix;

    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::LOAD,
                                                    "BM_CloudUntouchedTabletsClose");
    SCOPED_ATTACH_TASK(tracker);

    MetaLockHolder holder(env->tablets, meta_lock_hold_us);

    int64_t txn_id = 1;
    int64_t rowset_writers = 0;
    uint64_t alloc_bytes = 0;
    for (auto _ : state) {
        const uint64_t alloc_start = thread_allocated_bytes();
        RuntimeProfile profile("LoadChannel");
        std::vector<std::unique_ptr<CloudDeltaWriter>> writers;
        writers.reserve(kNumUntouched);
        for (int i = 1; i < kNumTablets; ++i) {
            WriteRequest req;
            req.tablet_id = env->tablets[i]->tablet_id();
            req.schema_hash = kSchemaHash;
            req.txn_id = txn_id;
            req.txn_expiration = 0;
            req.index_id = kIndexId;
            req.partition_id = kPartitionId;
            req.load_id.set_hi(txn_id);
            req.load_id.set_lo(i);
            req.table_schema_param = env->schema_param;
            writers.push_back(std::make_unique<CloudDeltaWriter>(*env->engine, req, &profile,
                                                                 UniqueId(txn_id, 0)));
        }
        for (auto& writer : writers) {
            auto st = writer->commit_rowset();
            if (st.ok()) {
                st = writer->submit_calc_delete_bitmap_task();
            }
            if (st.ok()) {
                st = writer->wait_calc_delete_bitmap();
            }
            if (st.ok()) {
                st = writer->set_txn_related_info();
            }
            if (!st.ok()) {
                state.SkipWithError(st.to_string().c_str());
                break;
            }
            writer->update_tablet_stats();
            rowset_writers += writer->committed_without_rowset_writer() ? 0 : 1;
        }
        writers.clear();
        alloc_bytes += thread_allocated_bytes() - alloc_start;
        ++txn_id;
        // Each load adds one rowset to the untouched tablets' approximate stats. Undo it so that
        // the rowset builder's version count check (stock path only) never trips over the
        // benchmark's own iteration count.
        for (int i = 1; i < kNumTablets; ++i) {
            env->tablets[i]->fetch_add_approximate_num_rowsets(-1);
            env->tablets[i]->fetch_add_approximate_cumu_num_rowsets(-1);
        }
    }

    const auto loads = static_cast<double>(state.iterations());
    state.counters["rowset_writers"] = static_cast<double>(rowset_writers) / loads;
    state.counters["alloc_bytes"] = static_cast<double>(alloc_bytes) / loads;
    state.counters["ms_rpcs"] = 0;
    state.counters["loads_per_s"] = benchmark::Counter(loads, benchmark::Counter::kIsRate);

    config::skip_writing_empty_rowset_metadata = saved_skip_metadata;
    config::skip_rowset_writer_for_empty_tablet = saved_skip_writer;
}

} // namespace doris::cloud_untouched_tablets_bench

BENCHMARK(doris::cloud_untouched_tablets_bench::BM_CloudUntouchedTabletsClose)
        ->ArgNames({"fix", "meta_lock_hold_us"})
        ->ArgsProduct({{0, 1}, {0, 200, 2000}})
        ->UseRealTime()
        ->Unit(benchmark::kMicrosecond);
