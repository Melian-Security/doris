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

#include <algorithm>
#include <ctime>
#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "common/config.h"
#include "storage/compaction/cumulative_compaction_time_series_policy.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_meta.h"

// Time-series cumulative compaction over long runs of empty rowsets.
//
// Workload: one partition with 32 tablets and ~60 loads/s, each load writing one tablet
// (load_to_single_tablet) and leaving an empty rowset on the other 31. One tablet therefore
// receives a new version every ~16.7 ms, one in 32 of them with data.
//
// BM_TsEmptyRunDrain replays that arrival on one tablet for 10 virtual minutes and drives the
// real TimeSeriesCumulativeCompactionPolicy pick against it. Sizes and task durations use what
// the 4.1.4 cloud cluster logged: a data compaction window of ~1.09 GB covers ~900 versions
// (so ~35 MB per data rowset at one data rowset in 32) and merges at ~19.5 MB/s (~55 s), an
// empty-only compaction takes ~30 ms, the producer runs every 100 ms, and a pick that finds
// nothing backs the tablet off for 5 s. The tablet always gets a compaction slot, so the drain
// rates are an upper bound for compaction alone.
//
// BM_TsEmptyRunPick measures one condition-6 pick over a candidate list of N rowsets and how
// many compaction jobs clear a single long empty run, with and without the rowset count cap.

namespace doris::ts_empty_run_bench {

class Env {
public:
    static Env& instance() {
        static Env env;
        return env;
    }

    TabletMetaSharedPtr make_meta(int64_t empty_threshold) {
        auto meta = std::make_shared<TabletMeta>(
                1001, 2, 15673, 15674, 4, 5, TTabletSchema(), 6,
                std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
        meta->set_compaction_policy(std::string(CUMULATIVE_TIME_SERIES_POLICY));
        meta->set_time_series_compaction_goal_size_mbytes(1024);
        meta->set_time_series_compaction_file_count_threshold(2000);
        meta->set_time_series_compaction_time_threshold_seconds(3600);
        meta->set_time_series_compaction_empty_rowsets_threshold(empty_threshold);
        meta->set_time_series_compaction_level_threshold(1);
        return meta;
    }

    std::shared_ptr<CloudTablet> make_tablet(const TabletMetaSharedPtr& meta) {
        return std::make_shared<CloudTablet>(_engine, meta);
    }

    RowsetSharedPtr make_rowset(const TabletMetaSharedPtr& tablet_meta, int64_t start,
                                int64_t end, int64_t data_size, int64_t num_segments) {
        auto meta = std::make_shared<RowsetMeta>();
        meta->set_rowset_id(RowsetId {2, 0, 0, ++_next_rowset_id});
        meta->set_tablet_id(15673);
        meta->set_txn_id(_next_rowset_id);
        meta->set_rowset_type(BETA_ROWSET);
        meta->set_rowset_state(VISIBLE);
        meta->set_version({start, end});
        meta->set_num_rows(num_segments == 0 ? 0 : data_size / 560);
        meta->set_total_disk_size(data_size);
        meta->set_data_disk_size(data_size);
        meta->set_index_disk_size(0);
        meta->set_empty(num_segments == 0);
        meta->set_num_segments(num_segments);
        meta->set_segments_overlap(NONOVERLAPPING);
        // Real wall-clock creation time keeps the policy's time condition (3600 s) quiet; the
        // simulation clock is virtual.
        meta->set_creation_time(time(nullptr));
        meta->set_tablet_schema(tablet_meta->tablet_schema());
        RowsetSharedPtr rowset;
        static_cast<void>(
                RowsetFactory::create_rowset(tablet_meta->tablet_schema(), "", meta, &rowset));
        return rowset;
    }

private:
    Env() = default;
    CloudStorageEngine _engine {EngineOptions {}};
    int64_t _next_rowset_id = 0;
};

struct SimParams {
    int64_t empty_threshold = 5;
    int64_t parallel = 1;     // concurrent cumulative compactions on the tablet
    bool empty_first = false; // config::time_series_compaction_prefer_empty_rowsets
    int64_t sim_ms = 600 * 1000;
    int64_t versions_per_s = 60;
    int64_t tablets_per_partition = 32;
    int64_t data_rowset_bytes = 35'000'000;
    double merge_bytes_per_s = 19.5e6;
    int64_t empty_task_ms = 30;
    int64_t producer_interval_ms = 100;
    int64_t no_suitable_backoff_ms = 5000;
};

struct SimResult {
    int64_t versions = 0;
    int64_t rowsets = 0;      // all rowsets of the tablet at the end
    int64_t cumu_rowsets = 0; // rowsets at or above the cumulative point at the end
    int64_t max_rowsets = 0;
    int64_t cumu_rowsets_at_60s = 0;
    int64_t cumulative_point = 2;
    int64_t empty_tasks = 0;
    int64_t data_tasks = 0;
    int64_t empty_inputs = 0;
    int64_t data_inputs = 0;
    int64_t max_inputs = 0;
};

class Simulator {
public:
    explicit Simulator(const SimParams& params) : _p(params) {
        _meta = Env::instance().make_meta(_p.empty_threshold);
        _tablet = Env::instance().make_tablet(_meta);
        // Base rowset [0-1], data.
        _rowsets.push_back(Env::instance().make_rowset(_meta, 0, 1, 1024, 1));
    }

    SimResult run() {
        int64_t created = 0;
        for (int64_t now = 0; now <= _p.sim_ms; ++now) {
            const int64_t due = now * _p.versions_per_s / 1000;
            while (created < due) {
                add_version();
                ++created;
            }
            finish_tasks(now);
            if (now % _p.producer_interval_ms == 0) {
                schedule(now);
            }
            _r.max_rowsets = std::max<int64_t>(_r.max_rowsets, _rowsets.size());
            if (now == 60 * 1000) {
                _r.cumu_rowsets_at_60s = cumu_rowsets();
            }
        }
        _r.versions = created;
        _r.rowsets = _rowsets.size();
        _r.cumu_rowsets = cumu_rowsets();
        _r.cumulative_point = _cumulative_point;
        return _r;
    }

private:
    struct Task {
        int64_t end_ms;
        std::vector<RowsetSharedPtr> inputs;
    };

    void add_version() {
        const int64_t v = _next_version++;
        const bool has_data = v % _p.tablets_per_partition == 0;
        _rowsets.push_back(Env::instance().make_rowset(
                _meta, v, v, has_data ? _p.data_rowset_bytes : 0, has_data ? 1 : 0));
    }

    int64_t cumu_rowsets() const {
        return std::count_if(_rowsets.begin(), _rowsets.end(), [&](const RowsetSharedPtr& rs) {
            return rs->start_version() >= _cumulative_point;
        });
    }

    std::vector<RowsetSharedPtr> candidates() const {
        int64_t from = _cumulative_point;
        for (const auto& t : _running) {
            from = std::max(from, t.inputs.back()->end_version() + 1);
        }
        auto it = std::lower_bound(
                _rowsets.begin(), _rowsets.end(), from,
                [](const RowsetSharedPtr& rs, int64_t v) { return rs->start_version() < v; });
        return std::vector<RowsetSharedPtr>(it, _rowsets.end());
    }

    void schedule(int64_t now) {
        if (static_cast<int64_t>(_running.size()) >= _p.parallel || now < _backoff_until) {
            return;
        }
        auto cands = candidates();
        std::vector<RowsetSharedPtr> picked;
        Version last_delete_version {-1, -1};
        size_t score = 0;
        TimeSeriesCumulativeCompactionPolicy::pick_input_rowsets(
                _tablet.get(), 0, cands, config::cumulative_compaction_max_deltas,
                config::cumulative_compaction_min_deltas, &picked, &last_delete_version, &score,
                false);
        if (picked.size() < 2) {
            _backoff_until = now + _p.no_suitable_backoff_ms;
            return;
        }
        int64_t bytes = 0;
        for (const auto& rs : picked) {
            bytes += rs->total_disk_size();
        }
        const int64_t duration =
                bytes == 0 ? _p.empty_task_ms
                           : static_cast<int64_t>(static_cast<double>(bytes) /
                                                  _p.merge_bytes_per_s * 1000.0);
        if (bytes == 0) {
            ++_r.empty_tasks;
            _r.empty_inputs += picked.size();
        } else {
            ++_r.data_tasks;
            _r.data_inputs += picked.size();
        }
        _r.max_inputs = std::max<int64_t>(_r.max_inputs, picked.size());
        _running.push_back({now + duration, std::move(picked)});
    }

    void finish_tasks(int64_t now) {
        for (auto it = _running.begin(); it != _running.end();) {
            if (it->end_ms > now) {
                ++it;
                continue;
            }
            apply(it->inputs);
            it = _running.erase(it);
            if (_running.empty()) {
                // A finished compaction makes the tablet schedulable again.
                _backoff_until = 0;
            }
        }
    }

    void apply(const std::vector<RowsetSharedPtr>& inputs) {
        int64_t bytes = 0;
        for (const auto& rs : inputs) {
            bytes += rs->total_disk_size();
        }
        const int64_t start = inputs.front()->start_version();
        const int64_t end = inputs.back()->end_version();
        auto first = std::lower_bound(
                _rowsets.begin(), _rowsets.end(), start,
                [](const RowsetSharedPtr& rs, int64_t v) { return rs->start_version() < v; });
        auto last = first + static_cast<std::ptrdiff_t>(inputs.size());
        auto output = Env::instance().make_rowset(_meta, start, end, bytes, bytes == 0 ? 0 : 1);
        first = _rowsets.erase(first, last);
        _rowsets.insert(first, output);
        // Time-series policy: a non-empty output moves the cumulative point past it, an empty
        // output leaves it where it is.
        if (bytes != 0) {
            _cumulative_point = std::max(_cumulative_point, end + 1);
        }
    }

    SimParams _p;
    TabletMetaSharedPtr _meta;
    std::shared_ptr<CloudTablet> _tablet;
    std::vector<RowsetSharedPtr> _rowsets;
    std::vector<Task> _running;
    int64_t _next_version = 2;
    int64_t _cumulative_point = 2;
    int64_t _backoff_until = 0;
    SimResult _r;
};

// Args: {empty_rowsets_threshold, parallel cumu compactions per tablet, empty_first}
static void BM_TsEmptyRunDrain(benchmark::State& state) {
    SimParams params;
    params.empty_threshold = state.range(0);
    params.parallel = state.range(1);
    params.empty_first = state.range(2) != 0;
    SimResult r;
    const bool saved_prefer = config::time_series_compaction_prefer_empty_rowsets;
    config::time_series_compaction_prefer_empty_rowsets = params.empty_first;
    for (auto _ : state) {
        Simulator sim(params);
        r = sim.run();
        benchmark::DoNotOptimize(r);
    }
    config::time_series_compaction_prefer_empty_rowsets = saved_prefer;
    const double sim_s = static_cast<double>(params.sim_ms) / 1000.0;
    const double empty_versions = static_cast<double>(r.versions) *
                                  (1.0 - 1.0 / static_cast<double>(params.tablets_per_partition));
    state.counters["empty_rowsets_created_per_s"] = empty_versions / sim_s;
    state.counters["cumu_point_advance_per_s"] = static_cast<double>(r.cumulative_point) / sim_s;
    state.counters["rowsets_removed_per_s"] =
            static_cast<double>(r.empty_inputs - r.empty_tasks + r.data_inputs - r.data_tasks) /
            sim_s;
    state.counters["cumu_rowsets_growth_per_s"] =
            static_cast<double>(r.cumu_rowsets - r.cumu_rowsets_at_60s) / (sim_s - 60.0);
    state.counters["rowsets_end"] = static_cast<double>(r.rowsets);
    state.counters["cumu_rowsets_end"] = static_cast<double>(r.cumu_rowsets);
    state.counters["max_rowsets"] = static_cast<double>(r.max_rowsets);
    state.counters["empty_tasks"] = static_cast<double>(r.empty_tasks);
    state.counters["data_tasks"] = static_cast<double>(r.data_tasks);
    state.counters["avg_empty_inputs"] =
            r.empty_tasks == 0 ? 0.0
                               : static_cast<double>(r.empty_inputs) /
                                         static_cast<double>(r.empty_tasks);
    state.counters["avg_data_inputs"] =
            r.data_tasks == 0 ? 0.0
                              : static_cast<double>(r.data_inputs) /
                                        static_cast<double>(r.data_tasks);
    state.counters["max_inputs_per_job"] = static_cast<double>(r.max_inputs);
}

BENCHMARK(BM_TsEmptyRunDrain)
        ->ArgNames({"threshold", "parallel", "empty_first"})
        ->Args({5, 1, 0})
        ->Args({10000, 1, 0})
        ->Args({5, 1, 1})
        ->Args({5, 4, 0})
        ->Args({10000, 4, 0})
        ->Args({5, 4, 1})
        ->Iterations(1)
        ->Unit(benchmark::kMillisecond);

// Args: {candidate rowsets N, layout (0: one empty run of N-2 between two data rowsets,
//        1: one data rowset in every 32), cap (0: uncapped, else rowset count cap)}
static void BM_TsEmptyRunPick(benchmark::State& state) {
    const int64_t n = state.range(0);
    const bool interleaved = state.range(1) != 0;
    const int64_t cap = state.range(2) == 0 ? std::numeric_limits<int64_t>::max() : state.range(2);
    auto meta = Env::instance().make_meta(5);
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(n);
    for (int64_t v = 2; v < n + 2; ++v) {
        const bool has_data = interleaved ? v % 32 == 0 : (v == 2 || v == n + 1);
        rowsets.push_back(
                Env::instance().make_rowset(meta, v, v, has_data ? 1024 : 0, has_data ? 1 : 0));
    }
    std::vector<RowsetSharedPtr> picked;
    for (auto _ : state) {
        BaseTablet::calc_consecutive_empty_rowsets(&picked, rowsets, 5, cap);
        benchmark::DoNotOptimize(picked);
    }
    state.counters["inputs_per_job"] = static_cast<double>(picked.size());

    // Jobs needed to clear the first qualifying run: each job outputs one empty rowset that the
    // next job absorbs.
    int64_t jobs = 0;
    int64_t max_inputs = 0;
    while (true) {
        BaseTablet::calc_consecutive_empty_rowsets(&picked, rowsets, 5, cap);
        if (picked.empty() || (jobs > 0 && interleaved)) {
            break;
        }
        ++jobs;
        max_inputs = std::max<int64_t>(max_inputs, picked.size());
        const int64_t start = picked.front()->start_version();
        const int64_t end = picked.back()->end_version();
        auto first = std::find(rowsets.begin(), rowsets.end(), picked.front());
        first = rowsets.erase(first, first + static_cast<std::ptrdiff_t>(picked.size()));
        rowsets.insert(first, Env::instance().make_rowset(meta, start, end, 0, 0));
    }
    state.counters["jobs_to_clear_run"] = static_cast<double>(jobs);
    state.counters["max_inputs_per_job"] = static_cast<double>(max_inputs);
}

BENCHMARK(BM_TsEmptyRunPick)
        ->ArgNames({"n", "interleaved", "cap"})
        ->Args({1000, 0, 0})
        ->Args({4000, 0, 0})
        ->Args({16000, 0, 0})
        ->Args({20000, 0, 0})
        ->Args({40000, 0, 0})
        ->Args({40000, 0, 10000})
        ->Args({16000, 1, 0})
        ->Args({20000, 1, 0})
        ->Unit(benchmark::kMicrosecond);

} // namespace doris::ts_empty_run_bench
