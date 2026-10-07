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

// Time application threads spend blocked inside free() of column-sized buffers under heavy load.
//
// Setting: W worker threads build ColumnString columns (chars + offsets, both PODArrays on the
// Doris Allocator) of 64 KiB..64 MiB, write every byte, keep a window of 4 live columns and destroy
// the oldest, like memtable flush, scanners and compaction do. F extra threads page-fault fresh
// mmap regions and munmap them, standing in for the rest of the process faulting pages.
//
// The jemalloc options come from JEMALLOC_CONF, so one binary compares configurations. The second
// argument selects the purge regime Doris drives on top of them:
//   0  none: jemalloc decays on its own (dirty_decay_ms from JEMALLOC_CONF);
//   1  soft-limit: every arena's dirty_decay_ms is set to 0, which is what
//      Daemon::je_reset_dirty_decay_thread does once process memory exceeds the soft limit;
//   2  periodic purge: a thread runs arena.<all>.purge every 100 ms, as
//      MemoryReclamation::je_purge_dirty_pages does when threads wait for memory;
//   3  soft-limit with je_dirty_decay_ms_over_soft_limit=1000: every arena's dirty_decay_ms is
//      set to 1000 and a thread runs arena.<all>.decay every 500 ms, so purging stays off the
//      freeing threads.
//
// The 192-worker cases oversubscribe a 96-vCPU host, so their off-CPU figure includes run-queue
// waits. The 64-worker cases leave cores idle, as the production BEs have.
//
// Counters: ops/s (column build+destroy), GB/s written, free_wall_s and free_offcpu_s (wall time
// and wall minus thread CPU time spent destroying columns, summed over workers), the slowest
// single destroy, minor faults, voluntary context switches, process CPU utilisation and RSS.
//
// Run: JEMALLOC_CONF=... benchmark_test --benchmark_filter='JemallocFreeStall'

#pragma once

#include <benchmark/benchmark.h>
#include <sys/mman.h>
#include <sys/resource.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <ctime>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "core/column/column_string.h"
#include "runtime/memory/jemalloc_control.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/thread_context.h"

namespace doris {

namespace jemalloc_free_stall {

inline int64_t thread_cpu_ns() {
    timespec ts {};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return int64_t(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

inline int64_t wall_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
}

inline int64_t rss_bytes() {
    std::ifstream f("/proc/self/statm");
    int64_t size = 0;
    int64_t resident = 0;
    f >> size >> resident;
    return resident * sysconf(_SC_PAGESIZE);
}

#ifdef USE_JEMALLOC
inline void set_all_arenas_dirty_decay_ms(ssize_t ms) {
    // Uninitialised arena slots reject the write; new arenas take the arenas.* default.
    ssize_t value = ms;
    jemallctl("arenas.dirty_decay_ms", nullptr, nullptr, &value, sizeof(value));
    unsigned narenas = 0;
    size_t sz = sizeof(narenas);
    jemallctl("arenas.narenas", &narenas, &sz, nullptr, 0);
    for (unsigned i = 0; i < narenas; ++i) {
        std::string name = "arena." + std::to_string(i) + ".dirty_decay_ms";
        jemallctl(name.c_str(), nullptr, nullptr, &value, sizeof(value));
    }
}

inline ssize_t opt_dirty_decay_ms() {
    ssize_t v = 0;
    size_t sz = sizeof(v);
    jemallctl("opt.dirty_decay_ms", &v, &sz, nullptr, 0);
    return v;
}

inline void purge_all_arenas() {
    std::string name = "arena." + std::to_string(MALLCTL_ARENAS_ALL) + ".purge";
    jemallctl(name.c_str(), nullptr, nullptr, nullptr, 0);
}

inline void decay_all_arenas() {
    std::string name = "arena." + std::to_string(MALLCTL_ARENAS_ALL) + ".decay";
    jemallctl(name.c_str(), nullptr, nullptr, nullptr, 0);
}
#else
inline void decay_all_arenas() {}
inline void set_all_arenas_dirty_decay_ms(ssize_t) {}
inline ssize_t opt_dirty_decay_ms() {
    return 0;
}
inline void purge_all_arenas() {}
#endif

struct WorkerResult {
    int64_t ops = 0;
    int64_t bytes = 0;
    int64_t free_wall_ns = 0;
    int64_t free_cpu_ns = 0;
    int64_t free_max_ns = 0;
};

// Sizes are 64 KiB * 2^(10 * u^2): log-scaled, median ~360 KiB, maximum 64 MiB.
inline size_t pick_size(std::mt19937_64& rng) {
    double u = std::uniform_real_distribution<double>(0.0, 1.0)(rng);
    return size_t(65536.0 * std::exp2(10.0 * u * u));
}

inline void worker(int idx, const std::atomic<bool>& stop, WorkerResult* out,
                   const std::shared_ptr<MemTrackerLimiter>& tracker) {
    SCOPED_INIT_THREAD_CONTEXT();
    thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(tracker);
    std::mt19937_64 rng(0x9e3779b97f4a7c15ULL ^ uint64_t(idx));
    constexpr size_t kWindow = 4;
    std::vector<MutableColumnPtr> live;
    live.reserve(kWindow + 1);
    WorkerResult r;
    auto destroy_oldest = [&]() {
        int64_t w0 = wall_ns();
        int64_t c0 = thread_cpu_ns();
        live.erase(live.begin());
        int64_t dw = wall_ns() - w0;
        r.free_wall_ns += dw;
        r.free_cpu_ns += thread_cpu_ns() - c0;
        r.free_max_ns = std::max(r.free_max_ns, dw);
    };
    while (!stop.load(std::memory_order_relaxed)) {
        size_t bytes = pick_size(rng);
        size_t rows = std::max<size_t>(1, bytes / 64);
        auto col = ColumnString::create();
        auto& chars = col->get_chars();
        chars.resize(bytes);
        memset(chars.data(), int(r.ops & 0xff), bytes);
        auto& offsets = col->get_offsets();
        offsets.resize(rows);
        for (size_t i = 0; i < rows; ++i) {
            offsets[i] = static_cast<UInt32>((i + 1) * (bytes / rows));
        }
        live.emplace_back(std::move(col));
        if (live.size() > kWindow) {
            destroy_oldest();
        }
        r.ops++;
        r.bytes += int64_t(bytes + rows * sizeof(UInt32));
    }
    while (!live.empty()) {
        destroy_oldest();
    }
    *out = r;
}

inline void faulter(const std::atomic<bool>& stop) {
    constexpr size_t kRegion = 64UL << 20;
    long page = sysconf(_SC_PAGESIZE);
    while (!stop.load(std::memory_order_relaxed)) {
        void* p = mmap(nullptr, kRegion, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1,
                       0);
        if (p == MAP_FAILED) {
            continue;
        }
        for (size_t off = 0; off < kRegion; off += size_t(page)) {
            static_cast<volatile char*>(p)[off] = 1;
        }
        munmap(p, kRegion);
    }
}

} // namespace jemalloc_free_stall

// Args: {worker threads, purge mode (0 none, 1 soft-limit decay=0, 2 periodic purge,
//        3 soft-limit decay=1000 + off-thread decay),
//        fault threads, seconds}.
static void JemallocFreeStall(benchmark::State& state) {
    using namespace jemalloc_free_stall;
    const int workers = int(state.range(0));
    const int mode = int(state.range(1));
    const int faulters = int(state.range(2));
    const int seconds = int(state.range(3));
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL,
                                                    "JemallocFreeStall");
    const ssize_t configured_decay = opt_dirty_decay_ms();
    for (auto _ : state) {
        // Warm up so the per-CPU arenas exist before their decay is changed.
        {
            std::atomic<bool> stop {false};
            std::vector<WorkerResult> warm(workers);
            std::vector<std::thread> ts;
            for (int i = 0; i < workers; ++i) {
                ts.emplace_back(worker, i, std::cref(stop), &warm[i], std::cref(tracker));
            }
            std::this_thread::sleep_for(std::chrono::seconds(2));
            stop = true;
            for (auto& t : ts) {
                t.join();
            }
        }
        set_all_arenas_dirty_decay_ms(mode == 1 ? 0 : (mode == 3 ? 1000 : configured_decay));

        std::atomic<bool> stop {false};
        std::vector<WorkerResult> results(workers);
        std::vector<std::thread> ts;
        rusage ru0 {};
        getrusage(RUSAGE_SELF, &ru0);
        int64_t t0 = wall_ns();
        for (int i = 0; i < workers; ++i) {
            ts.emplace_back(worker, i, std::cref(stop), &results[i], std::cref(tracker));
        }
        for (int i = 0; i < faulters; ++i) {
            ts.emplace_back(faulter, std::cref(stop));
        }
        std::thread purger;
        if (mode == 2 || mode == 3) {
            purger = std::thread([&stop, mode]() {
                while (!stop.load(std::memory_order_relaxed)) {
                    if (mode == 2) {
                        purge_all_arenas();
                    } else {
                        decay_all_arenas();
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(mode == 2 ? 100 : 500));
                }
            });
        }
        int64_t rss_peak = 0;
        for (int s = 0; s < seconds * 10; ++s) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            rss_peak = std::max(rss_peak, rss_bytes());
        }
        stop = true;
        for (auto& t : ts) {
            t.join();
        }
        if (purger.joinable()) {
            purger.join();
        }
        double wall_s = double(wall_ns() - t0) / 1e9;
        rusage ru1 {};
        getrusage(RUSAGE_SELF, &ru1);
        set_all_arenas_dirty_decay_ms(configured_decay);

        WorkerResult total;
        for (auto& r : results) {
            total.ops += r.ops;
            total.bytes += r.bytes;
            total.free_wall_ns += r.free_wall_ns;
            total.free_cpu_ns += r.free_cpu_ns;
            total.free_max_ns = std::max(total.free_max_ns, r.free_max_ns);
        }
        auto tv_s = [](const timeval& tv) { return double(tv.tv_sec) + double(tv.tv_usec) / 1e6; };
        double cpu_s = tv_s(ru1.ru_utime) - tv_s(ru0.ru_utime) + tv_s(ru1.ru_stime) -
                       tv_s(ru0.ru_stime);
        double sys_s = tv_s(ru1.ru_stime) - tv_s(ru0.ru_stime);
        state.SetIterationTime(wall_s);
        state.counters["ops_per_s"] = double(total.ops) / wall_s;
        state.counters["GB_per_s"] = double(total.bytes) / wall_s / 1e9;
        state.counters["free_wall_s"] = double(total.free_wall_ns) / 1e9;
        state.counters["free_offcpu_s"] = double(total.free_wall_ns - total.free_cpu_ns) / 1e9;
        state.counters["free_max_ms"] = double(total.free_max_ns) / 1e6;
        state.counters["minflt"] = double(ru1.ru_minflt - ru0.ru_minflt);
        state.counters["nvcsw"] = double(ru1.ru_nvcsw - ru0.ru_nvcsw);
        state.counters["nivcsw"] = double(ru1.ru_nivcsw - ru0.ru_nivcsw);
        state.counters["cpu_util"] =
                cpu_s / wall_s / double(std::thread::hardware_concurrency());
        state.counters["sys_frac"] = cpu_s > 0 ? sys_s / cpu_s : 0;
        state.counters["rss_peak_GB"] = double(rss_peak) / 1e9;
    }
}

BENCHMARK(JemallocFreeStall)
        ->ArgNames({"workers", "mode", "faulters", "secs"})
        ->Args({192, 0, 32, 20})
        ->Args({192, 1, 32, 20})
        ->Args({192, 2, 32, 20})
        ->Args({192, 3, 32, 20})
        ->Args({64, 0, 16, 20})
        ->Args({64, 1, 16, 20})
        ->Args({64, 2, 16, 20})
        ->Args({64, 3, 16, 20})
        ->Iterations(1)
        ->UseManualTime()
        ->Unit(benchmark::kSecond);

} // namespace doris
