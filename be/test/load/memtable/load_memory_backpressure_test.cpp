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

#include "load/memtable/load_memory_backpressure.h"

#include <gtest/gtest.h>

namespace doris {

namespace {
constexpr int64_t GB = 1024L * 1024 * 1024;
constexpr int64_t MEM_LIMIT = 318 * GB;
} // namespace

TEST(LoadMemoryBackpressureTest, PercentOf) {
    EXPECT_EQ(LoadMemoryBackpressure::percent_of(MEM_LIMIT, 0), -1);
    EXPECT_EQ(LoadMemoryBackpressure::percent_of(MEM_LIMIT, -5), -1);
    EXPECT_EQ(LoadMemoryBackpressure::percent_of(0, 50), -1);
    EXPECT_EQ(LoadMemoryBackpressure::percent_of(200, 50), 100);
    EXPECT_EQ(LoadMemoryBackpressure::percent_of(MEM_LIMIT, 100), MEM_LIMIT);
    // No overflow for large limits.
    int64_t big = int64_t(1) << 62;
    EXPECT_EQ(LoadMemoryBackpressure::percent_of(big, 50), big / 2);
}

TEST(LoadMemoryBackpressureTest, ProcessWatermark) {
    EXPECT_EQ(LoadMemoryBackpressure::process_watermark(false, MEM_LIMIT, 85), -1);
    EXPECT_EQ(LoadMemoryBackpressure::process_watermark(true, MEM_LIMIT, 0), -1);
    // At or above 100% the process GC already owns the decision.
    EXPECT_EQ(LoadMemoryBackpressure::process_watermark(true, MEM_LIMIT, 100), -1);
    EXPECT_EQ(LoadMemoryBackpressure::process_watermark(true, 1000, 85), 850);

    EXPECT_LE(LoadMemoryBackpressure::over_watermark(800, 850), 0);
    EXPECT_EQ(LoadMemoryBackpressure::over_watermark(900, 850), 50);
    EXPECT_EQ(LoadMemoryBackpressure::over_watermark(900, -1), 0);
}

TEST(LoadMemoryBackpressureTest, TableWriteLegacyCountGateUnchanged) {
    // Memory gate off: only the pending-count gate decides, whatever the memory state.
    EXPECT_FALSE(LoadMemoryBackpressure::table_write_should_wait(9, 10, false, 100 * GB, 1 * GB,
                                                                 MEM_LIMIT, 1));
    EXPECT_TRUE(LoadMemoryBackpressure::table_write_should_wait(10, 10, false, 0, 0, 0, -1));
    // A count limit <= 0 disables the count gate.
    EXPECT_FALSE(LoadMemoryBackpressure::table_write_should_wait(1000, 0, false, 0, 0, 0, -1));
    // The count gate still applies when the memory gate is on but its wait is used up.
    EXPECT_TRUE(LoadMemoryBackpressure::table_write_should_wait(100, 100, false, 0, 0, 0, -1));
}

TEST(LoadMemoryBackpressureTest, TableWriteMemoryGate) {
    const int64_t watermark = LoadMemoryBackpressure::process_watermark(true, MEM_LIMIT, 85);
    const int64_t pending_limit = LoadMemoryBackpressure::percent_of(MEM_LIMIT, 15);

    // Raised count limit, plenty of memory: no wait.
    EXPECT_FALSE(LoadMemoryBackpressure::table_write_should_wait(
            50, 100, true, 10 * GB, pending_limit, 200 * GB, watermark));
    // Pending memtable bytes above their budget: wait.
    EXPECT_TRUE(LoadMemoryBackpressure::table_write_should_wait(
            50, 100, true, pending_limit, pending_limit, 200 * GB, watermark));
    // Process above the watermark: wait.
    EXPECT_TRUE(LoadMemoryBackpressure::table_write_should_wait(1, 100, true, 0, pending_limit,
                                                                watermark, watermark));
    // Byte budget disabled, only the watermark applies.
    EXPECT_FALSE(LoadMemoryBackpressure::table_write_should_wait(1, 100, true, 1000 * GB, -1,
                                                                 watermark - 1, watermark));
    // A table with no pending flushes never waits on memory: no finishing flush could end it.
    EXPECT_FALSE(LoadMemoryBackpressure::table_write_should_wait(
            0, 100, true, 1000 * GB, pending_limit, MEM_LIMIT, watermark));
    // Memory gate allowed but process watermark disabled and byte budget disabled.
    EXPECT_FALSE(LoadMemoryBackpressure::table_write_should_wait(5, 100, true, 1000 * GB, -1,
                                                                 MEM_LIMIT, -1));
}

TEST(LoadMemoryBackpressureTest, MemoryWaitIsBounded) {
    EXPECT_FALSE(LoadMemoryBackpressure::memory_wait_allowed(false, 0, 60000));
    EXPECT_TRUE(LoadMemoryBackpressure::memory_wait_allowed(true, 0, 60000));
    EXPECT_TRUE(LoadMemoryBackpressure::memory_wait_allowed(true, 59999, 60000));
    EXPECT_FALSE(LoadMemoryBackpressure::memory_wait_allowed(true, 60000, 60000));
    // A non-positive bound means no bound.
    EXPECT_TRUE(LoadMemoryBackpressure::memory_wait_allowed(true, 1L << 40, 0));
}

TEST(LoadMemoryBackpressureTest, ProcessGcLoadFreeTarget) {
    const int64_t full_gc = MEM_LIMIT / 10;
    // Legacy: extra_percent < 0 keeps the full GC size.
    EXPECT_EQ(LoadMemoryBackpressure::process_gc_load_free_target(full_gc, MEM_LIMIT + GB,
                                                                  MEM_LIMIT, -1, false),
              full_gc);
    // Low system memory keeps the full GC size: the OS is the one running out.
    EXPECT_EQ(LoadMemoryBackpressure::process_gc_load_free_target(full_gc, MEM_LIMIT + GB,
                                                                  MEM_LIMIT, 1, true),
              full_gc);
    // Small overshoot frees the overshoot plus 1% of the limit.
    EXPECT_EQ(LoadMemoryBackpressure::process_gc_load_free_target(full_gc, MEM_LIMIT + GB,
                                                                  MEM_LIMIT, 1, false),
              GB + LoadMemoryBackpressure::percent_of(MEM_LIMIT, 1));
    // Zero extra frees exactly the overshoot.
    EXPECT_EQ(LoadMemoryBackpressure::process_gc_load_free_target(full_gc, MEM_LIMIT + GB,
                                                                  MEM_LIMIT, 0, false),
              GB);
    // Not over the limit (e.g. triggered just at the edge): only the extra.
    EXPECT_EQ(LoadMemoryBackpressure::process_gc_load_free_target(full_gc, MEM_LIMIT - GB,
                                                                  MEM_LIMIT, 0, false),
              0);
    // Large overshoot is capped at the full GC size.
    EXPECT_EQ(LoadMemoryBackpressure::process_gc_load_free_target(full_gc, MEM_LIMIT + 100 * GB,
                                                                  MEM_LIMIT, 1, false),
              full_gc);
}

} // namespace doris
