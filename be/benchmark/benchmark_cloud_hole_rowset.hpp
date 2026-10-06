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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <vector>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

// Cloud-mode version holes of an untouched tablet.
//
// Models one hot partition with 32 tablets (RANDOM bucketing, load_to_single_tablet=true) taking
// 60 commits/s, each commit writing one tablet. One tablet is simulated: each new partition
// version is a data rowset with probability 1/32 and an empty version otherwise, published one at
// a time through CloudTablet::apply_visible_pending_rowsets (the make-visible path every
// untouched tablet takes per load). No compaction runs, which is the backlog state of the
// production collapse.
//
// Arg 0: number of partition versions (commits). 3600 = 1 minute, 18000 = 5 minutes.
// Arg 1: 0 = base (one hole rowset per version), 1 = fix (hole rowset version ranges).
//
// Counters:
//   live_rowsets             live rowsets of the tablet after the run
//   live_rowsets_per_min     live rowset growth per minute at 60 commits/s
//   stale_rowsets            stale rowsets kept for older-version reads (swept after
//                            tablet_rowset_stale_sweep_time_sec, not modelled here)
//   publish_us_last          avg apply_visible_pending_rowsets latency over the last 500
//                            versions; the tablet's exclusive meta lock is held for all of it
//   fill_holes_us            CloudMetaMgr::fill_version_holes with nothing to fill, as every
//                            sync_tablet_rowsets does under the exclusive meta lock
//   score_us                 CloudTablet::reset_approximate_stats, which every sync runs to
//                            recompute the cumulative compaction score
//   empty_compaction_tasks   tasks to clear the empty rowsets when each absorbs at most
//                            5 of them (time_series_compaction_empty_rowsets_threshold default)
namespace doris::cloud_hole_rowset_bench {

class HoleRowsetBench {
public:
    explicit HoleRowsetBench(bool use_version_range) : _engine(EngineOptions {}) {
        _saved_enable = config::enable_hole_rowset_version_range;
        config::enable_hole_rowset_version_range = use_version_range;

        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        auto* col = schema_pb.add_column();
        col->set_unique_id(0);
        col->set_name("k1");
        col->set_type("INT");
        col->set_is_key(true);
        col->set_is_nullable(false);
        _schema = std::make_shared<TabletSchema>();
        _schema->init_from_pb(schema_pb);

        auto tablet_meta = std::make_shared<TabletMeta>(
                1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6,
                std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
        tablet_meta->set_tablet_state(TABLET_RUNNING);
        _tablet = std::make_shared<CloudTablet>(_engine, tablet_meta);
        std::unique_lock wlock(_tablet->get_header_lock());
        _tablet->add_rowsets({make_rowset(Version(0, 1))}, false, wlock, false);
    }

    ~HoleRowsetBench() { config::enable_hole_rowset_version_range = _saved_enable; }

    RowsetSharedPtr make_rowset(Version version) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_version(version);
        rs_meta->set_rowset_id(_engine.next_rowset_id());
        rs_meta->set_num_segments(1);
        rs_meta->set_num_rows(1000);
        rs_meta->set_tablet_schema(_schema);
        RowsetSharedPtr rowset;
        static_cast<void>(RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset));
        return rowset;
    }

    // Publishes the next partition version to this tablet, data or empty.
    void publish(bool has_data) {
        int64_t version = _tablet->max_version_unlocked() + 1;
        {
            std::lock_guard<std::mutex> lock(_tablet->_visible_pending_rs_lock);
            RowsetMetaSharedPtr meta =
                    has_data ? make_rowset(Version(version, version))->rowset_meta() : nullptr;
            _tablet->_visible_pending_rs_map.emplace(
                    version, CloudTablet::VisiblePendingRowset {meta, INT64_MAX, !has_data});
        }
        _tablet->apply_visible_pending_rowsets();
    }

    double fill_holes_us() {
        std::unique_lock wlock(_tablet->get_header_lock());
        auto start = std::chrono::steady_clock::now();
        static_cast<void>(_engine.meta_mgr().fill_version_holes(
                _tablet.get(), _tablet->max_version_unlocked(), wlock));
        return std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start)
                .count();
    }

    double score_us() {
        std::unique_lock wlock(_tablet->get_header_lock());
        auto start = std::chrono::steady_clock::now();
        _tablet->reset_approximate_stats(0, 0, 0, 0);
        return std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start)
                .count();
    }

    size_t live_rowsets() const { return _tablet->rowset_map().size(); }
    size_t stale_rowsets() const { return _tablet->_stale_rs_version_map.size(); }
    size_t live_empty_rowsets() const {
        size_t n = 0;
        for (const auto& [_, rs] : _tablet->rowset_map()) {
            n += rs->num_segments() == 0 ? 1 : 0;
        }
        return n;
    }

private:
    CloudStorageEngine _engine;
    TabletSchemaSPtr _schema;
    std::shared_ptr<CloudTablet> _tablet;
    bool _saved_enable = true;
};

static void BM_CloudHoleRowsetPublish(benchmark::State& state) {
    constexpr int kTabletsPerPartition = 32;
    constexpr int64_t kCommitsPerMinute = 60 * 60;
    constexpr int64_t kTailWindow = 500;
    const int64_t versions = state.range(0);
    const bool use_version_range = state.range(1) != 0;

    for (auto _ : state) {
        state.PauseTiming();
        HoleRowsetBench bench(use_version_range);
        std::mt19937_64 rng(42);
        std::uniform_int_distribution<int> pick_tablet(0, kTabletsPerPartition - 1);
        state.ResumeTiming();

        double tail_publish_us = 0;
        for (int64_t i = 0; i < versions; ++i) {
            bool has_data = pick_tablet(rng) == 0;
            auto start = std::chrono::steady_clock::now();
            bench.publish(has_data);
            if (i >= versions - kTailWindow) {
                tail_publish_us += std::chrono::duration<double, std::micro>(
                                           std::chrono::steady_clock::now() - start)
                                           .count();
            }
        }

        state.PauseTiming();
        double minutes = static_cast<double>(versions) / kCommitsPerMinute;
        state.counters["live_rowsets"] = static_cast<double>(bench.live_rowsets());
        state.counters["live_rowsets_per_min"] =
                static_cast<double>(bench.live_rowsets()) / minutes;
        state.counters["stale_rowsets"] = static_cast<double>(bench.stale_rowsets());
        state.counters["publish_us_last"] =
                tail_publish_us / static_cast<double>(std::min(versions, kTailWindow));
        state.counters["fill_holes_us"] = bench.fill_holes_us();
        state.counters["score_us"] = bench.score_us();
        state.counters["empty_compaction_tasks"] =
                static_cast<double>((bench.live_empty_rowsets() + 4) / 5);
        state.ResumeTiming();
    }
}

BENCHMARK(BM_CloudHoleRowsetPublish)
        ->ArgNames({"versions", "range"})
        ->Args({3600, 0})
        ->Args({3600, 1})
        ->Args({18000, 0})
        ->Args({18000, 1})
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond);

} // namespace doris::cloud_hole_rowset_bench
