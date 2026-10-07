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

#include "load/memtable/memtable_memory_limiter.h"

#include <bvar/bvar.h>

#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "load/memtable/load_memory_backpressure.h"
#include "load/memtable/memtable.h"
#include "load/memtable/memtable_writer.h"
#include "util/mem_info.h"

namespace doris {
DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(memtable_memory_limiter_mem_consumption, MetricUnit::BYTES, "",
                                   memtable_memory_limiter_mem_consumption,
                                   Labels({{"type", "load"}}));

bvar::LatencyRecorder g_memtable_memory_limit_latency_ms("mm_limiter_limit_time_ms");
bvar::Adder<int> g_memtable_memory_limit_waiting_threads("mm_limiter_waiting_threads");
bvar::LatencyRecorder g_memtable_table_backpressure_latency_ms(
        "mm_limiter_table_backpressure_time_ms");
bvar::Adder<int> g_memtable_table_backpressure_waiting_threads(
        "mm_limiter_table_backpressure_waiting_threads");
bvar::Status<int64_t> g_memtable_table_backpressure_pending_count(
        "mm_limiter_table_backpressure_pending_count", 0);
bvar::Status<int64_t> g_memtable_active_memory("mm_limiter_mem_active", 0);
bvar::Status<int64_t> g_memtable_write_memory("mm_limiter_mem_write", 0);
bvar::Status<int64_t> g_memtable_flush_memory("mm_limiter_mem_flush", 0);
bvar::Status<int64_t> g_memtable_load_memory("mm_limiter_mem_load", 0);
bvar::Status<int64_t> g_load_hard_mem_limit("mm_limiter_limit_hard", 0);
bvar::Status<int64_t> g_load_soft_mem_limit("mm_limiter_limit_soft", 0);
bvar::Adder<uint64_t> g_flush_cuz_load_mem_exceed_hard_limit("flush_cuz_hard_limit");
bvar::Adder<uint64_t> g_flush_cuz_sys_mem_exceed_soft_limit("flush_cuz_soft_limit");
bvar::Adder<int> g_memtable_memory_limit_flush_memtable_count("mm_limiter_flush_memtable_count");
bvar::LatencyRecorder g_memtable_memory_limit_flush_size_bytes("mm_limiter_flush_size_bytes");

// Calculate the total memory limit of all load tasks on this BE
static int64_t calc_process_max_load_memory(int64_t process_mem_limit) {
    if (process_mem_limit == -1) {
        // no limit
        return -1;
    }
    int32_t max_load_memory_percent = config::load_process_max_memory_limit_percent;
    return process_mem_limit * max_load_memory_percent / 100;
}

MemTableMemoryLimiter::MemTableMemoryLimiter() {}

MemTableMemoryLimiter::~MemTableMemoryLimiter() {
    DEREGISTER_HOOK_METRIC(memtable_memory_limiter_mem_consumption);
}

Status MemTableMemoryLimiter::init(int64_t process_mem_limit) {
    _load_hard_mem_limit = calc_process_max_load_memory(process_mem_limit);
    _load_soft_mem_limit = _load_hard_mem_limit * config::load_process_soft_mem_limit_percent / 100;
    _load_safe_mem_permit =
            _load_hard_mem_limit * config::load_process_safe_mem_permit_percent / 100;
    g_load_hard_mem_limit.set_value(_load_hard_mem_limit);
    g_load_soft_mem_limit.set_value(_load_soft_mem_limit);
    _mem_tracker = std::make_unique<MemTracker>("AllMemTableMemory");
    REGISTER_HOOK_METRIC(memtable_memory_limiter_mem_consumption,
                         [this]() { return _mem_tracker->consumption(); });
    _log_timer.start();
    return Status::OK();
}

void MemTableMemoryLimiter::register_writer(std::weak_ptr<MemTableWriter> writer) {
    std::lock_guard<std::mutex> l(_lock);
    _writers.push_back(writer);
}

int64_t MemTableMemoryLimiter::_sys_avail_mem_less_than_warning_water_mark() {
    // reserve a small amount of memory so we do not trigger MinorGC
    return doris::MemInfo::sys_mem_available_warning_water_mark() -
           doris::GlobalMemoryArbitrator::sys_mem_available() +
           config::memtable_limiter_reserved_memory_bytes;
}

int64_t MemTableMemoryLimiter::_process_used_mem_more_than_soft_mem_limit() {
    // reserve a small amount of memory so we do not trigger MinorGC
    return GlobalMemoryArbitrator::process_memory_usage() - MemInfo::soft_mem_limit() +
           config::memtable_limiter_reserved_memory_bytes;
}

int64_t MemTableMemoryLimiter::_process_used_mem_more_than_backpressure_watermark(
        bool use_watermark) {
    int64_t watermark = LoadMemoryBackpressure::process_watermark(
            use_watermark && config::enable_load_memory_backpressure, MemInfo::mem_limit(),
            config::load_memory_backpressure_process_mem_percent);
    return LoadMemoryBackpressure::over_watermark(GlobalMemoryArbitrator::process_memory_usage(),
                                                  watermark);
}

bool MemTableMemoryLimiter::_soft_limit_reached() {
    return _mem_tracker->consumption() > _load_soft_mem_limit || _hard_limit_reached();
}

bool MemTableMemoryLimiter::_hard_limit_reached(bool use_watermark) {
    return _mem_tracker->consumption() > _load_hard_mem_limit ||
           _sys_avail_mem_less_than_warning_water_mark() > 0 ||
           _process_used_mem_more_than_soft_mem_limit() > 0 ||
           _process_used_mem_more_than_backpressure_watermark(use_watermark) > 0;
}

bool MemTableMemoryLimiter::_load_usage_low() {
    return _mem_tracker->consumption() <= _load_safe_mem_permit;
}

int64_t MemTableMemoryLimiter::_need_flush(bool use_watermark) {
    DBUG_EXECUTE_IF("MemTableMemoryLimiter._need_flush.random_flush", {
        if (rand() % 100 < (100 * dp->param("percent", 0.5))) {
            LOG(INFO) << "debug memtable need flush return 1";
            return 1;
        }
    });
    int64_t limit1 = _mem_tracker->consumption() - _load_soft_mem_limit;
    int64_t limit2 = _sys_avail_mem_less_than_warning_water_mark();
    int64_t limit3 = _process_used_mem_more_than_soft_mem_limit();
    int64_t limit4 = _process_used_mem_more_than_backpressure_watermark(use_watermark);
    int64_t need_flush = std::max({limit1, limit2, limit3, limit4});
    return need_flush - _queue_mem_usage - _flush_mem_usage;
}

void MemTableMemoryLimiter::_flush_pending_memtable_count(int64_t table_id,
                                                          const PUniqueId* load_id,
                                                          int64_t* table_pending,
                                                          int64_t* load_pending) {
    *table_pending = 0;
    *load_pending = 0;
    for (const auto& writer : _writers) {
        auto writer_sptr = writer.lock();
        if (writer_sptr == nullptr || writer_sptr->table_id() != table_id) {
            continue;
        }
        int64_t pending = writer_sptr->flush_pending_memtable_count();
        *table_pending += pending;
        if (load_id != nullptr && writer_sptr->load_id().hi() == load_id->hi() &&
            writer_sptr->load_id().lo() == load_id->lo()) {
            *load_pending += pending;
        }
    }
}

void MemTableMemoryLimiter::handle_table_memtable_backpressure(std::function<bool()> cancel_check,
                                                               int64_t table_id,
                                                               const PUniqueId* load_id) {
    if (!config::enable_table_memtable_flush_backpressure) {
        return;
    }
    const int64_t pending_count_limit = config::table_memtable_flush_pending_count_limit;
    const bool memory_enabled = config::enable_load_memory_backpressure;
    if (pending_count_limit <= 0 && !memory_enabled) {
        return;
    }
    DORIS_CHECK(table_id > 0);

    const int64_t mem_limit = MemInfo::mem_limit();
    const int64_t max_wait_ms = config::load_memory_backpressure_max_wait_ms;
    const int64_t pending_bytes_limit = LoadMemoryBackpressure::percent_of(
            mem_limit, config::load_memtable_pending_mem_limit_percent);
    const int64_t watermark = LoadMemoryBackpressure::process_watermark(
            memory_enabled, mem_limit, config::load_memory_backpressure_process_mem_percent);

    MonotonicStopWatch timer;
    timer.start();
    // Both the pending-count gate and the memory gate are re-evaluated on every wake-up. The
    // memory gate stops applying once its bounded wait is used up.
    //
    // With load_memory_backpressure_wait_on_own_load the memory gate only holds a write while its
    // own load has flushes pending. Otherwise, with many loads into one table, the table almost
    // always has a pending flush, so a write waits until process memory drops or the bounded
    // wait runs out, whatever its own load is doing.
    const PUniqueId* memory_scope_load_id =
            config::load_memory_backpressure_wait_on_own_load ? load_id : nullptr;
    int64_t pending_count = 0;
    int64_t load_pending_count = 0;
    auto refresh_pending = [&]() {
        _flush_pending_memtable_count(table_id, memory_scope_load_id, &pending_count,
                                      &load_pending_count);
    };
    auto should_wait = [&]() {
        bool memory_wait = LoadMemoryBackpressure::memory_wait_allowed(
                memory_enabled, timer.elapsed_time() / 1000 / 1000, max_wait_ms);
        int64_t memory_pending_count =
                memory_scope_load_id != nullptr ? load_pending_count : pending_count;
        return LoadMemoryBackpressure::table_write_should_wait(
                pending_count, pending_count_limit, memory_wait, memory_pending_count,
                _queue_mem_usage + _flush_mem_usage, pending_bytes_limit,
                GlobalMemoryArbitrator::process_memory_usage(), watermark);
    };

    std::unique_lock<std::mutex> l(_lock);
    refresh_pending();
    if (!should_wait()) {
        return;
    }

    g_memtable_table_backpressure_waiting_threads << 1;
    while (should_wait()) {
        g_memtable_table_backpressure_pending_count.set_value(pending_count);
        LOG_EVERY_T(INFO, 1) << "table memtable flush backpressure: table_id=" << table_id
                             << ", pending_memtables=" << pending_count
                             << ", load_pending_memtables=" << load_pending_count
                             << ", limit=" << pending_count_limit << ", pending_bytes="
                             << PrettyPrinter::print_bytes(_queue_mem_usage + _flush_mem_usage)
                             << ", pending_bytes_limit="
                             << PrettyPrinter::print_bytes(pending_bytes_limit)
                             << ", process_watermark=" << PrettyPrinter::print_bytes(watermark)
                             << ", memtable writers num: " << _writers.size() << ", "
                             << GlobalMemoryArbitrator::process_memory_used_str();
        if (cancel_check && cancel_check()) {
            LOG(INFO) << "cancelled when waiting for table memtable flush backpressure"
                      << ", table_id=" << table_id << ", pending_memtables=" << pending_count
                      << ", limit=" << pending_count_limit;
            g_memtable_table_backpressure_waiting_threads << -1;
            return;
        }
        static_cast<void>(_hard_limit_end_cond.wait_for(l, std::chrono::milliseconds(100)));
        refresh_pending();
    }
    g_memtable_table_backpressure_pending_count.set_value(pending_count);
    g_memtable_table_backpressure_waiting_threads << -1;
    timer.stop();
    int64_t time_ms = timer.elapsed_time() / 1000 / 1000;
    g_memtable_table_backpressure_latency_ms << time_ms;
    LOG(INFO) << "waited " << PrettyPrinter::print(timer.elapsed_time(), TUnit::TIME_NS)
              << " for table memtable flush backpressure"
              << ", table_id=" << table_id << ", pending_memtables=" << pending_count
              << ", limit=" << pending_count_limit << ", memtable writers num: " << _writers.size();
}

void MemTableMemoryLimiter::handle_memtable_flush(std::function<bool()> cancel_check,
                                                  WorkloadGroup*) {
    // Check the soft limit.
    DCHECK(_load_soft_mem_limit > 0);
    do {
        DBUG_EXECUTE_IF("MemTableMemoryLimiter.handle_memtable_flush.limit_reached", {
            LOG(INFO) << "debug memtable limit reached";
            break;
        });
        if (!_soft_limit_reached() || _load_usage_low()) {
            return;
        }
    } while (false);
    MonotonicStopWatch timer;
    timer.start();
    std::unique_lock<std::mutex> l(_lock);
    g_memtable_memory_limit_waiting_threads << 1;
    bool first = true;
    // The watermark is a bounded wait; once it is used up only the legacy limits keep waiting.
    bool use_watermark = true;
    do {
        use_watermark = LoadMemoryBackpressure::memory_wait_allowed(
                config::enable_load_memory_backpressure, timer.elapsed_time() / 1000 / 1000,
                config::load_memory_backpressure_max_wait_ms);
        if (!first) {
            auto st = _hard_limit_end_cond.wait_for(l, std::chrono::milliseconds(1000));
            if (st == std::cv_status::timeout) {
                LOG(INFO) << "timeout when waiting for memory hard limit end, try again";
            }
        }
        if (cancel_check && cancel_check()) {
            LOG(INFO) << "cancelled when waiting for memtable flush";
            g_memtable_memory_limit_waiting_threads << -1;
            return;
        }
        first = false;
        int64_t need_flush = _need_flush(use_watermark);
        if (need_flush > 0) {
            auto limit = _hard_limit_reached(use_watermark) ? Limit::HARD : Limit::SOFT;
            LOG(INFO) << "reached memtable memory " << (limit == Limit::HARD ? "hard" : "soft")
                      << ", " << GlobalMemoryArbitrator::process_memory_used_details_str() << ", "
                      << GlobalMemoryArbitrator::sys_mem_available_details_str()
                      << ", load mem: " << PrettyPrinter::print_bytes(_mem_tracker->consumption())
                      << ", memtable writers num: " << _writers.size()
                      << ", active: " << PrettyPrinter::print_bytes(_active_mem_usage)
                      << ", queue: " << PrettyPrinter::print_bytes(_queue_mem_usage)
                      << ", flush: " << PrettyPrinter::print_bytes(_flush_mem_usage)
                      << ", need flush: " << PrettyPrinter::print_bytes(need_flush);
            if (VLOG_DEBUG_IS_ON) {
                auto log_str = doris::ProcessProfile::instance()
                                       ->memory_profile()
                                       ->process_memory_detail_str();
                LOG_LONG_STRING(INFO, log_str);
            }
            if (limit == Limit::HARD) {
                g_flush_cuz_load_mem_exceed_hard_limit << 1;
            } else if (limit == Limit::SOFT) {
                g_flush_cuz_sys_mem_exceed_soft_limit << 1;
            } else {
                // will not reach here
            }
            _flush_active_memtables(need_flush);
        }
    } while (_hard_limit_reached(use_watermark) && !_load_usage_low());
    g_memtable_memory_limit_waiting_threads << -1;
    timer.stop();
    int64_t time_ms = timer.elapsed_time() / 1000 / 1000;
    g_memtable_memory_limit_latency_ms << time_ms;
    if (time_ms > 0) {
        LOG(INFO) << "waited " << PrettyPrinter::print(timer.elapsed_time(), TUnit::TIME_NS)
                  << " for memtable memory limit"
                  << ", " << GlobalMemoryArbitrator::process_memory_used_details_str() << ", "
                  << GlobalMemoryArbitrator::sys_mem_available_details_str()
                  << ", load mem: " << PrettyPrinter::print_bytes(_mem_tracker->consumption())
                  << ", memtable writers num: " << _writers.size()
                  << ", active: " << PrettyPrinter::print_bytes(_active_mem_usage)
                  << ", queue: " << PrettyPrinter::print_bytes(_queue_mem_usage)
                  << ", flush: " << PrettyPrinter::print_bytes(_flush_mem_usage);
    }
}

int64_t MemTableMemoryLimiter::_flush_active_memtables(int64_t need_flush) {
    if (need_flush <= 0) {
        return 0;
    }

    _refresh_mem_tracker();
    if (_active_writers.size() == 0) {
        return 0;
    }

    using WriterMem = std::pair<std::weak_ptr<MemTableWriter>, int64_t>;
    auto cmp = [](WriterMem left, WriterMem right) { return left.second < right.second; };
    std::priority_queue<WriterMem, std::vector<WriterMem>, decltype(cmp)> heap(cmp);

    for (auto writer : _active_writers) {
        auto w = writer.lock();
        if (w == nullptr) {
            continue;
        }
        heap.emplace(w, w->active_memtable_mem_consumption());
    }

    int64_t mem_flushed = 0;
    int64_t num_flushed = 0;

    while (mem_flushed < need_flush && !heap.empty()) {
        auto [writer, sort_mem] = heap.top();
        heap.pop();
        auto w = writer.lock();
        if (w == nullptr) {
            continue;
        }

        int64_t mem = w->active_memtable_mem_consumption();
        if (mem < sort_mem * 0.9) {
            // if the memtable writer just got flushed, don't flush it again
            continue;
        }
        Status st = w->flush_async();
        if (!st.ok()) {
            auto err_msg = fmt::format(
                    "tablet writer failed to reduce mem consumption by flushing memtable, "
                    "tablet_id={}, err={}",
                    w->tablet_id(), st.to_string());
            LOG(WARNING) << err_msg;
            static_cast<void>(w->cancel_with_status(st));
        }
        mem_flushed += mem;
        num_flushed += (mem > 0);
        g_memtable_memory_limit_flush_memtable_count << 1;
        g_memtable_memory_limit_flush_size_bytes << mem;
    }
    LOG(INFO) << "flushed " << num_flushed << " out of " << _active_writers.size()
              << " active writers, flushed size: " << PrettyPrinter::print_bytes(mem_flushed);
    return mem_flushed;
}

void MemTableMemoryLimiter::refresh_mem_tracker() {
    std::lock_guard<std::mutex> l(_lock);
    _refresh_mem_tracker();
    std::stringstream ss;
    Limit limit = Limit::NONE;
    if (_soft_limit_reached()) {
        limit = _hard_limit_reached() ? Limit::HARD : Limit::SOFT;
        ss << "reached " << (limit == Limit::HARD ? "hard" : "soft") << " limit";
    } else if (_last_limit == Limit::NONE) {
        return;
    } else {
        ss << "ended " << (_last_limit == Limit::HARD ? "hard" : "soft") << " limit";
    }

    if (_last_limit == limit && _log_timer.elapsed_time() < LOG_INTERVAL) {
        return;
    }

    _last_limit = limit;
    _log_timer.reset();
    LOG(INFO) << ss.str()
              << ", load mem: " << PrettyPrinter::print_bytes(_mem_tracker->consumption())
              << ", memtable writers num: " << _writers.size()
              << ", active: " << PrettyPrinter::print_bytes(_active_mem_usage)
              << ", queue: " << PrettyPrinter::print_bytes(_queue_mem_usage)
              << ", flush: " << PrettyPrinter::print_bytes(_flush_mem_usage);
    if (VLOG_DEBUG_IS_ON) {
        auto log_str =
                doris::ProcessProfile::instance()->memory_profile()->process_memory_detail_str();
        LOG_LONG_STRING(INFO, log_str);
    }
}

void MemTableMemoryLimiter::_refresh_mem_tracker() {
    _flush_mem_usage = 0;
    _queue_mem_usage = 0;
    _active_mem_usage = 0;
    _active_writers.clear();
    for (auto it = _writers.begin(); it != _writers.end();) {
        if (auto writer = it->lock()) {
            // The memtable is currently used by writer to insert blocks.
            auto active_usage = writer->active_memtable_mem_consumption();
            _active_mem_usage += active_usage;
            if (active_usage > 0) {
                _active_writers.push_back(writer);
            }

            auto flush_usage = writer->mem_consumption(MemType::FLUSH);
            _flush_mem_usage += flush_usage;

            auto write_usage = writer->mem_consumption(MemType::WRITE_FINISHED);
            _queue_mem_usage += write_usage;
            ++it;
        } else {
            *it = std::move(_writers.back());
            _writers.pop_back();
        }
    }
    _mem_usage = _active_mem_usage + _queue_mem_usage + _flush_mem_usage;
    g_memtable_active_memory.set_value(_active_mem_usage);
    g_memtable_write_memory.set_value(_queue_mem_usage);
    g_memtable_flush_memory.set_value(_flush_mem_usage);
    g_memtable_load_memory.set_value(_mem_usage);
    VLOG_DEBUG << "refreshed mem_tracker, num writers: " << _writers.size()
               << ", mem usage: " << _mem_usage;
    _mem_tracker->set_consumption(_mem_usage);
    if (!_hard_limit_reached()) {
        _hard_limit_end_cond.notify_all();
    }
}

} // namespace doris
