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

#include <algorithm>
#include <cstdint>

namespace doris {

// Pure decision logic for memory-driven load back-pressure. Callers pass in every input, so the
// rules are unit-testable without a running BE.
struct LoadMemoryBackpressure {
    // Bytes of `mem_limit` that `percent` represents, or -1 when the input disables it.
    static int64_t percent_of(int64_t mem_limit, int64_t percent) {
        if (mem_limit <= 0 || percent <= 0) {
            return -1;
        }
        return mem_limit / 100 * percent + mem_limit % 100 * percent / 100;
    }

    // Process memory at or above which load writes flush and wait. -1 means no watermark.
    // A percent of 100 or more is rejected: the process GC already cancels at mem_limit.
    static int64_t process_watermark(bool enabled, int64_t mem_limit, int64_t percent) {
        if (!enabled || percent >= 100) {
            return -1;
        }
        return percent_of(mem_limit, percent);
    }

    // Bytes by which process memory exceeds the watermark; <= 0 when below or disabled.
    static int64_t over_watermark(int64_t process_used, int64_t watermark) {
        if (watermark <= 0) {
            return 0;
        }
        return process_used - watermark;
    }

    // Whether a write to a table must wait in the per-table back-pressure gate.
    //
    // The pending-count gate keeps its legacy meaning. When memory back-pressure is enabled and
    // the wait has not yet used up its bound, the table also waits while it has pending flushes
    // and either queued+flushing memtable bytes exceed their budget or process memory is above
    // the watermark. Requiring this table's own pending flushes keeps every wait one that a
    // finishing flush can end.
    static bool table_write_should_wait(int64_t pending_count, int64_t count_limit,
                                        bool memory_wait_allowed, int64_t pending_bytes,
                                        int64_t pending_bytes_limit, int64_t process_used,
                                        int64_t watermark) {
        return table_write_should_wait(pending_count, count_limit, memory_wait_allowed,
                                       pending_count, pending_bytes, pending_bytes_limit,
                                       process_used, watermark);
    }

    // Same as above, but the memory gate is conditioned on `memory_pending_count`, the pending
    // flushes the waiter can count on to end its wait, which may be narrower than the table's.
    static bool table_write_should_wait(int64_t pending_count, int64_t count_limit,
                                        bool memory_wait_allowed, int64_t memory_pending_count,
                                        int64_t pending_bytes, int64_t pending_bytes_limit,
                                        int64_t process_used, int64_t watermark) {
        if (count_limit > 0 && pending_count >= count_limit) {
            return true;
        }
        if (!memory_wait_allowed || memory_pending_count <= 0) {
            return false;
        }
        if (pending_bytes_limit > 0 && pending_bytes >= pending_bytes_limit) {
            return true;
        }
        return watermark > 0 && process_used >= watermark;
    }

    // Whether a memory-driven wait that started `waited_ms` ago may keep waiting.
    static bool memory_wait_allowed(bool enabled, int64_t waited_ms, int64_t max_wait_ms) {
        if (!enabled) {
            return false;
        }
        return max_wait_ms <= 0 || waited_ms < max_wait_ms;
    }

    // Memory the process GC should free by cancelling loads. With `extra_percent` < 0, or when
    // system available memory is below its low water mark, it is the full GC size. Otherwise it is
    // the overshoot above mem_limit plus `extra_percent` of mem_limit, capped at the full GC size,
    // so a small overshoot cancels a few loads instead of a tenth of the process limit.
    static int64_t process_gc_load_free_target(int64_t full_gc_size, int64_t process_used,
                                               int64_t mem_limit, int64_t extra_percent,
                                               bool sys_mem_low) {
        if (extra_percent < 0 || sys_mem_low || mem_limit <= 0) {
            return full_gc_size;
        }
        int64_t overshoot = std::max<int64_t>(0, process_used - mem_limit);
        int64_t extra = extra_percent == 0 ? 0 : percent_of(mem_limit, extra_percent);
        return std::min(full_gc_size, overshoot + extra);
    }
};

} // namespace doris
