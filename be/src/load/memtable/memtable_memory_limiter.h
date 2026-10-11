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

#include <stdint.h>

#include <atomic>
#include <functional>

#include "common/status.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/workload_group/workload_group.h"
#include "util/countdown_latch.h"
#include "util/stopwatch.hpp"

namespace doris {
namespace io {
int64_t load_upload_buffer_bytes();
} // namespace io

// Holds new load writes while load S3 upload buffers (io::load_upload_buffer_bytes()) are at or
// above config::load_s3_upload_buffer_backpressure_bytes, until they fall to
// config::load_s3_upload_buffer_backpressure_resume_percent of it.
//
// It never blocks a buffer allocation. Callers wait only at the entry of a load write, holding no
// load upload buffer, and every counted buffer is filled, submitted and released by memtable
// flush, segment close, load stream and upload threads, none of which pass through this gate.
// Those threads may wait for an upload slot, which only already-submitted parts hold. So the
// counted bytes keep draining while writers wait here.
class S3UploadBufferBackpressure {
public:
    explicit S3UploadBufferBackpressure(
            std::function<int64_t()> bytes = [] { return io::load_upload_buffer_bytes(); })
            : _bytes(std::move(bytes)) {}

    // Returns at once while the gate is released. Otherwise waits until it is released,
    // `cancel_check` returns true, or config::load_s3_upload_buffer_backpressure_max_wait_ms runs
    // out. Must not be called while holding a load upload buffer.
    void wait(const std::function<bool()>& cancel_check);

    bool engaged() const { return _engaged.load(std::memory_order_relaxed); }
    int64_t waiters() const { return _waiters.load(std::memory_order_relaxed); }

private:
    // Re-evaluates the gate against the current bytes and configs.
    bool _refresh();

    std::function<int64_t()> _bytes;
    std::atomic<bool> _engaged {false};
    std::atomic<int64_t> _waiters {0};
};

class MemTableWriter;
class PUniqueId;
struct WriterMemItem {
    std::weak_ptr<MemTableWriter> writer;
    int64_t mem_size;
};
class MemTableMemoryLimiter {
public:
    MemTableMemoryLimiter();
    ~MemTableMemoryLimiter();

    Status init(int64_t process_mem_limit);

    // check if the total mem consumption exceeds limit.
    // If yes, it will flush memtable to try to reduce memory consumption.
    // Every write operation will call this API to check if need flush memtable OR hang
    // when memory is not available.
    // Then waits in the load S3 upload-buffer gate (see S3UploadBufferBackpressure).
    void handle_memtable_flush(std::function<bool()> cancel_check, WorkloadGroup* wg = nullptr);
    // `load_id`, when given, lets the memory gate wait only on the flushes of the writer's own load
    // (see config::load_memory_backpressure_wait_on_own_load).
    void handle_table_memtable_backpressure(std::function<bool()> cancel_check, int64_t table_id,
                                            const PUniqueId* load_id = nullptr);

    void register_writer(std::weak_ptr<MemTableWriter> writer);

    void refresh_mem_tracker();

    MemTracker* mem_tracker() { return _mem_tracker.get(); }

    int64_t mem_usage() const { return _mem_usage; }

    bool soft_limit_reached() { return _soft_limit_reached(); }

    S3UploadBufferBackpressure* s3_upload_buffer_backpressure() {
        return &_s3_upload_buffer_backpressure;
    }

private:
    void _handle_memtable_mem_limit(const std::function<bool()>& cancel_check);
    static inline int64_t _sys_avail_mem_less_than_warning_water_mark();
    static inline int64_t _process_used_mem_more_than_soft_mem_limit();
    // Bytes above the load back-pressure watermark; <= 0 when below it or when disabled.
    static int64_t _process_used_mem_more_than_backpressure_watermark(bool use_watermark);

    bool _soft_limit_reached();
    bool _hard_limit_reached(bool use_watermark = true);
    bool _load_usage_low();
    int64_t _need_flush(bool use_watermark);
    // Memtables queued for or running flush for `table_id`, and the part of them that belongs to
    // `load_id` (0 when `load_id` is null).
    void _flush_pending_memtable_count(int64_t table_id, const PUniqueId* load_id,
                                       int64_t* table_pending, int64_t* load_pending);
    int64_t _flush_active_memtables(int64_t need_flush);
    void _refresh_mem_tracker();
    std::mutex _lock;
    std::condition_variable _hard_limit_end_cond;
    int64_t _mem_usage = 0;
    int64_t _flush_mem_usage = 0;
    int64_t _queue_mem_usage = 0;
    int64_t _active_mem_usage = 0;

    // sum of all mem table memory.
    std::unique_ptr<MemTracker> _mem_tracker;
    int64_t _load_hard_mem_limit = -1;
    int64_t _load_soft_mem_limit = -1;
    int64_t _load_safe_mem_permit = -1;

    enum Limit { NONE, SOFT, HARD } _last_limit = Limit::NONE;
    MonotonicStopWatch _log_timer;
    static const int64_t LOG_INTERVAL = 1 * 1000 * 1000 * 1000; // 1s

    std::vector<std::weak_ptr<MemTableWriter>> _writers;
    std::vector<std::weak_ptr<MemTableWriter>> _active_writers;

    S3UploadBufferBackpressure _s3_upload_buffer_backpressure;
};
} // namespace doris
