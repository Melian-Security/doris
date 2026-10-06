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

// Exclusive meta lock hold time of a cloud tablet's per-load paths as its rowset count grows.
//
// Setting: one partition of 32 RANDOM-bucket tablets loaded with load_to_single_tablet, so
// every load makes one version visible on all 32 tablets: one data rowset on the written tablet
// and an empty rowset on the other 31. A hot tablet accumulates ~16k rowsets when compaction
// falls behind. Every benchmark is parameterized by that rowset count N.
//
// Each benchmark has a `Legacy` variant, which runs the 4.1.4 code removed by the incremental
// version index (copied below, verbatim in behavior) on the same tablet under the same lock,
// and a `Fixed` variant, which runs the current code. Reported time is the time spent holding
// the tablet's exclusive `_meta_lock` (manual time), so Legacy grows with N and Fixed stays flat.
//
//   CloudCommit/*     the locked section of apply_visible_pending_rowsets for an empty rowset:
//                     pick the template rowset, create the empty rowset, add it.
//   CloudSync/*       the locked section of sync_tablet_rowsets after another tablet's load:
//                     fill_version_holes (one trailing hole) + reset_approximate_stats.
//   CloudCompaction/* the locked section of a cumulative compaction commit: replace 5 rowsets
//                     at the cumulative point by one output + reset_approximate_stats.
//   CloudContended/*  8 committer threads and 2 sync threads on one tablet; reports average
//                     wait and hold per locked section (contention), wall time as manual time.
//
// Run: benchmark_test --benchmark_filter='Cloud(Commit|Sync|Compaction|Contended)'

#pragma once

#include <benchmark/benchmark.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <ranges>
#include <thread>
#include <vector>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_cache.h"

namespace doris::cloud_commit_lock_bench {

using Clock = std::chrono::steady_clock;

inline double seconds_between(Clock::time_point from, Clock::time_point to) {
    return std::chrono::duration<double>(to - from).count();
}

inline CloudStorageEngine& engine() {
    // Never destroyed: stopping a storage engine during static destruction is not supported.
    static auto* instance = [] {
        if (ExecEnv::GetInstance()->get_cache_manager() == nullptr) {
            ExecEnv::GetInstance()->set_cache_manager(CacheManager::create_global_instance());
        }
        if (ExecEnv::GetInstance()->get_tablet_schema_cache() == nullptr) {
            ExecEnv::GetInstance()->set_tablet_schema_cache(
                    TabletSchemaCache::create_global_schema_cache(
                            config::tablet_schema_cache_capacity));
        }
        // The hole-filling path logs at INFO inside the lock; keep stderr out of the timing.
        FLAGS_minloglevel = 2;
        return new CloudStorageEngine(EngineOptions {});
    }();
    return *instance;
}

inline TabletSchemaSPtr bench_schema() {
    static TabletSchemaSPtr schema = [] {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        auto* col = schema_pb.add_column();
        col->set_unique_id(0);
        col->set_name("k1");
        col->set_type("BIGINT");
        col->set_is_key(true);
        col->set_is_nullable(false);
        auto s = std::make_shared<TabletSchema>();
        s->init_from_pb(schema_pb);
        return s;
    }();
    return schema;
}

inline RowsetSharedPtr make_rowset(Version version, int64_t tablet_id, bool empty) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_tablet_id(tablet_id);
    rs_meta->set_rowset_type(BETA_ROWSET);
    rs_meta->set_version(version);
    rs_meta->set_rowset_id(engine().next_rowset_id());
    rs_meta->set_num_segments(empty ? 0 : 1);
    rs_meta->set_num_rows(empty ? 0 : 1000);
    rs_meta->set_empty(empty);
    rs_meta->set_segments_overlap(NONOVERLAPPING);
    rs_meta->set_rowset_state(VISIBLE);
    rs_meta->set_tablet_schema(bench_schema());
    RowsetSharedPtr rowset;
    CHECK(RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset).ok());
    return rowset;
}

// A tablet with [0-1] and the singleton versions [2-2] .. [N-N], mostly empty rowsets like the
// untouched tablets of the incident, and its cumulative point at `cumulative_point`.
inline std::shared_ptr<CloudTablet> build_tablet(int64_t num_rowsets, int64_t cumulative_point) {
    static std::atomic<int64_t> next_tablet_id {900000};
    int64_t tablet_id = next_tablet_id++;
    auto meta = std::make_shared<TabletMeta>(1, 15673, tablet_id, 15674, 4, 5, TTabletSchema(), 6,
                                             std::unordered_map<uint32_t, uint32_t> {{7, 8}},
                                             UniqueId(9, 10), TTabletType::TABLET_TYPE_DISK,
                                             TCompressionType::LZ4F);
    auto tablet = std::make_shared<CloudTablet>(engine(), meta);
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(num_rowsets);
    rowsets.push_back(make_rowset({0, 1}, tablet_id, false));
    for (int64_t v = 2; v <= num_rowsets; ++v) {
        rowsets.push_back(make_rowset({v, v}, tablet_id, v % 32 != 0));
    }
    {
        std::unique_lock wlock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), false, wlock, false);
    }
    tablet->set_cumulative_layer_point(cumulative_point);
    return tablet;
}

// ---- 4.1.4 code paths removed by the version index, kept here as the baseline. ----

// apply_visible_pending_rowsets / try_make_committed_rs_visible_for_mow: the template rowset
// for an empty rowset is found by collecting and scanning every rowset meta.
inline RowsetMetaSharedPtr legacy_max_start_template(CloudTablet& tablet) {
    Versions existing_versions;
    for (const auto& [_, rs] : tablet.tablet_meta()->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }
    if (existing_versions.empty()) {
        return nullptr;
    }
    auto max_version = std::ranges::max(existing_versions, {}, &Version::first);
    return tablet.get_rowset_by_version(max_version)->rowset_meta();
}

// CloudMetaMgr::fill_version_holes: collect, sort and walk every version on each sync.
inline Status legacy_fill_version_holes(CloudTablet& tablet, int64_t max_version,
                                        std::unique_lock<BthreadSharedMutex>& wlock) {
    if (max_version <= 0) {
        return Status::OK();
    }
    Versions existing_versions;
    for (const auto& [_, rs] : tablet.tablet_meta()->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }
    if (existing_versions.empty()) {
        return Status::OK();
    }
    std::vector<RowsetSharedPtr> hole_rowsets;
    std::sort(existing_versions.begin(), existing_versions.end(),
              [](const Version& a, const Version& b) { return a.first < b.first; });
    int64_t last_version = -1;
    for (const Version& version : existing_versions) {
        if (version.first > last_version + 1) {
            auto prev_non_hole_rowset = tablet.get_rowset_by_version(version);
            for (int64_t ver = last_version + 1; ver < version.first; ++ver) {
                RowsetSharedPtr hole_rowset;
                RETURN_IF_ERROR(engine().meta_mgr().create_empty_rowset_for_hole(
                        &tablet, ver, prev_non_hole_rowset->rowset_meta(), &hole_rowset));
                hole_rowsets.push_back(hole_rowset);
            }
        }
        last_version = version.second;
    }
    for (; last_version + 1 <= max_version; ++last_version) {
        RowsetSharedPtr hole_rowset;
        auto prev_non_hole_rowset = tablet.get_rowset_by_version(existing_versions.back());
        RETURN_IF_ERROR(engine().meta_mgr().create_empty_rowset_for_hole(
                &tablet, last_version + 1, prev_non_hole_rowset->rowset_meta(), &hole_rowset));
        hole_rowsets.push_back(hole_rowset);
    }
    if (!hole_rowsets.empty()) {
        tablet.add_rowsets(std::move(hole_rowsets), false, wlock, false);
    }
    return Status::OK();
}

// CloudTablet::reset_approximate_stats: a full walk of the rowset map on every sync and
// compaction commit.
inline void legacy_reset_approximate_stats(CloudTablet& tablet) {
    int64_t cumu_num_deltas = 0;
    int64_t cumu_num_rowsets = 0;
    auto cp = tablet.cumulative_layer_point();
    for (auto& [v, r] : tablet._rs_version_map) {
        if (v.second < cp) {
            continue;
        }
        cumu_num_deltas += r->is_segments_overlapping() ? r->num_segments() : 1;
        ++cumu_num_rowsets;
    }
    tablet._approximate_cumu_num_rowsets.store(cumu_num_rowsets, std::memory_order_relaxed);
    tablet._approximate_cumu_num_deltas.store(cumu_num_deltas, std::memory_order_relaxed);
}

// ---- The locked sections, Legacy and Fixed. ----

enum class Variant { LEGACY, FIXED };

// apply_visible_pending_rowsets for one empty pending rowset, from lock to unlock.
inline void commit_empty_rowset_locked(CloudTablet& tablet, Variant variant,
                                       std::unique_lock<BthreadSharedMutex>& wlock) {
    int64_t version = tablet.max_version_unlocked() + 1;
    auto prev_rs_meta = variant == Variant::LEGACY
                                ? legacy_max_start_template(tablet)
                                : tablet.rowset_meta_with_max_start_version_unlocked();
    RowsetSharedPtr rowset;
    CHECK(engine().meta_mgr().create_empty_rowset_for_hole(&tablet, version, prev_rs_meta, &rowset)
                  .ok());
    tablet.add_rowsets({rowset}, false, wlock, true);
}

// sync_tablet_rowsets after another tablet's load raised the partition version by one.
inline void sync_one_version_locked(CloudTablet& tablet, Variant variant,
                                    std::unique_lock<BthreadSharedMutex>& wlock) {
    int64_t partition_max_version = tablet.max_version_unlocked() + 1;
    if (variant == Variant::LEGACY) {
        CHECK(legacy_fill_version_holes(tablet, partition_max_version, wlock).ok());
        legacy_reset_approximate_stats(tablet);
    } else {
        CHECK(engine().meta_mgr().fill_version_holes(&tablet, partition_max_version, wlock).ok());
        tablet.reset_approximate_stats(0, 0, 0, 0);
    }
}

// The end of CloudCumulativeCompaction::modify_rowsets for 5 inputs at the cumulative point.
inline void compaction_commit_locked(CloudTablet& tablet, Variant variant,
                                     std::unique_lock<BthreadSharedMutex>& wlock) {
    int64_t cp = tablet.cumulative_layer_point();
    std::vector<RowsetSharedPtr> inputs;
    for (int64_t v = cp; v < cp + 5; ++v) {
        inputs.push_back(tablet.get_rowset_by_version({v, v}));
    }
    auto output = make_rowset({cp, cp + 4}, tablet.tablet_id(), false);
    tablet.delete_rowsets(inputs, wlock);
    tablet.add_rowsets({output}, false, wlock);
    tablet.set_cumulative_layer_point(cp + 5);
    if (variant == Variant::LEGACY) {
        legacy_reset_approximate_stats(tablet);
    } else {
        tablet.reset_approximate_stats(0, 0, 0, 0);
    }
}

template <typename LockedSection>
inline double time_locked_section(CloudTablet& tablet, LockedSection&& section) {
    std::unique_lock wlock(tablet.get_header_lock());
    auto start = Clock::now();
    section(wlock);
    auto end = Clock::now();
    return seconds_between(start, end);
}

template <Variant variant>
static void BM_CloudCommit(benchmark::State& state) {
    auto tablet = build_tablet(state.range(0), 2);
    for (auto _ : state) {
        state.SetIterationTime(time_locked_section(*tablet, [&](auto& wlock) {
            commit_empty_rowset_locked(*tablet, variant, wlock);
        }));
    }
    state.counters["rowsets"] = static_cast<double>(tablet->rowset_map().size());
}

template <Variant variant>
static void BM_CloudSync(benchmark::State& state) {
    auto tablet = build_tablet(state.range(0), 2);
    for (auto _ : state) {
        state.SetIterationTime(time_locked_section(*tablet, [&](auto& wlock) {
            sync_one_version_locked(*tablet, variant, wlock);
        }));
    }
    state.counters["rowsets"] = static_cast<double>(tablet->rowset_map().size());
}

template <Variant variant>
static void BM_CloudCompaction(benchmark::State& state) {
    // The incident's tablet kept ~1/4 of its rowsets below the cumulative point.
    auto tablet = build_tablet(state.range(0), state.range(0) / 4);
    for (auto _ : state) {
        state.SetIterationTime(time_locked_section(*tablet, [&](auto& wlock) {
            compaction_commit_locked(*tablet, variant, wlock);
        }));
    }
    state.counters["rowsets"] = static_cast<double>(tablet->rowset_map().size());
}

// 8 committers (the make-visible tasks of concurrent loads) and 2 syncers (queries and loads
// syncing the tablet) on one tablet. Reports how long each locked section waited for the lock
// and how long it held it.
template <Variant variant>
static void BM_CloudContended(benchmark::State& state) {
    constexpr int kCommitters = 8;
    constexpr int kSyncers = 2;
    constexpr int kSectionsPerThread = 50;
    auto tablet = build_tablet(state.range(0), 2);
    double total_wait = 0;
    double total_hold = 0;
    int64_t total_sections = 0;
    for (auto _ : state) {
        std::atomic<int64_t> wait_ns {0};
        std::atomic<int64_t> hold_ns {0};
        auto worker = [&](bool is_committer) {
            for (int i = 0; i < kSectionsPerThread; ++i) {
                auto requested = Clock::now();
                std::unique_lock wlock(tablet->get_header_lock());
                auto acquired = Clock::now();
                if (is_committer) {
                    commit_empty_rowset_locked(*tablet, variant, wlock);
                } else {
                    sync_one_version_locked(*tablet, variant, wlock);
                }
                auto released = Clock::now();
                wlock.unlock();
                wait_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(acquired -
                                                                                requested)
                                   .count();
                hold_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(released -
                                                                                acquired)
                                   .count();
            }
        };
        auto start = Clock::now();
        std::vector<std::thread> threads;
        for (int i = 0; i < kCommitters + kSyncers; ++i) {
            threads.emplace_back(worker, i < kCommitters);
        }
        for (auto& t : threads) {
            t.join();
        }
        state.SetIterationTime(seconds_between(start, Clock::now()));
        total_wait += static_cast<double>(wait_ns.load()) / 1e3;
        total_hold += static_cast<double>(hold_ns.load()) / 1e3;
        total_sections += (kCommitters + kSyncers) * kSectionsPerThread;
    }
    state.counters["wait_us_per_section"] = total_wait / static_cast<double>(total_sections);
    state.counters["hold_us_per_section"] = total_hold / static_cast<double>(total_sections);
    state.counters["sections_per_s"] = benchmark::Counter(static_cast<double>(total_sections),
                                                          benchmark::Counter::kIsRate);
    state.counters["rowsets"] = static_cast<double>(tablet->rowset_map().size());
}

#define DORIS_CLOUD_COMMIT_LOCK_ARGS(bm, iterations)                                          \
    BENCHMARK_TEMPLATE(bm, Variant::LEGACY)                                                   \
            ->Name(#bm "/Legacy")                                                             \
            ->Arg(1000)                                                                       \
            ->Arg(4000)                                                                       \
            ->Arg(16000)                                                                      \
            ->Arg(20000)                                                                      \
            ->Iterations(iterations)                                                          \
            ->UseManualTime()                                                                 \
            ->Unit(benchmark::kMicrosecond);                                                  \
    BENCHMARK_TEMPLATE(bm, Variant::FIXED)                                                    \
            ->Name(#bm "/Fixed")                                                              \
            ->Arg(1000)                                                                       \
            ->Arg(4000)                                                                       \
            ->Arg(16000)                                                                      \
            ->Arg(20000)                                                                      \
            ->Iterations(iterations)                                                          \
            ->UseManualTime()                                                                 \
            ->Unit(benchmark::kMicrosecond)

DORIS_CLOUD_COMMIT_LOCK_ARGS(BM_CloudCommit, 2000);
DORIS_CLOUD_COMMIT_LOCK_ARGS(BM_CloudSync, 2000);
DORIS_CLOUD_COMMIT_LOCK_ARGS(BM_CloudCompaction, 100);
DORIS_CLOUD_COMMIT_LOCK_ARGS(BM_CloudContended, 3);

#undef DORIS_CLOUD_COMMIT_LOCK_ARGS

} // namespace doris::cloud_commit_lock_bench
