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

#include <fcntl.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/fs/local_file_reader.h"

namespace doris::io {

namespace {

std::shared_ptr<FileReader> make_reader(const std::string& name) {
    int fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
    EXPECT_GE(fd, 0);
    return std::make_shared<LocalFileReader>(name, 0, fd);
}

AccessKeyAndOffset key_of(int i) {
    return std::make_pair(BlockFileCache::hash("fd_cache_test_" + std::to_string(i % 7)),
                          static_cast<size_t>(i));
}

class FDCacheCapacityGuard {
public:
    explicit FDCacheCapacityGuard(int64_t capacity)
            : _saved(config::file_cache_max_file_reader_cache_size) {
        config::file_cache_max_file_reader_cache_size = capacity;
    }
    ~FDCacheCapacityGuard() { config::file_cache_max_file_reader_cache_size = _saved; }

private:
    int64_t _saved;
};

} // namespace

// Keys land in different shards; eviction must still drop the oldest inserted reader first.
TEST(FDCacheTest, EvictsOldestAcrossShards) {
    FDCacheCapacityGuard guard(3);
    FDCache cache;
    for (int i = 0; i < 5; ++i) {
        cache.insert_file_reader(key_of(i), make_reader("r" + std::to_string(i)));
    }
    EXPECT_EQ(cache.file_reader_cache_size(), 3);
    EXPECT_FALSE(cache.contains_file_reader(key_of(0)));
    EXPECT_FALSE(cache.contains_file_reader(key_of(1)));
    for (int i = 2; i < 5; ++i) {
        EXPECT_TRUE(cache.contains_file_reader(key_of(i))) << i;
        ASSERT_NE(cache.get_file_reader(key_of(i)), nullptr) << i;
    }
}

TEST(FDCacheTest, InsertKeepsExistingAndRemoveFreesSlot) {
    FDCacheCapacityGuard guard(2);
    FDCache cache;
    auto first = make_reader("first");
    cache.insert_file_reader(key_of(1), first);
    cache.insert_file_reader(key_of(1), make_reader("second"));
    EXPECT_EQ(cache.get_file_reader(key_of(1)), first);
    EXPECT_EQ(cache.file_reader_cache_size(), 1);

    cache.insert_file_reader(key_of(2), make_reader("two"));
    cache.remove_file_reader(key_of(1));
    EXPECT_EQ(cache.get_file_reader(key_of(1)), nullptr);
    EXPECT_EQ(cache.file_reader_cache_size(), 1);

    cache.insert_file_reader(key_of(3), make_reader("three"));
    EXPECT_TRUE(cache.contains_file_reader(key_of(2)));
    EXPECT_TRUE(cache.contains_file_reader(key_of(3)));
    EXPECT_EQ(cache.file_reader_cache_size(), 2);
}

TEST(FDCacheTest, ShrinkingCapacityEvictsDownToIt) {
    FDCacheCapacityGuard guard(4);
    FDCache cache;
    for (int i = 0; i < 4; ++i) {
        cache.insert_file_reader(key_of(i), make_reader("r" + std::to_string(i)));
    }
    config::file_cache_max_file_reader_cache_size = 2;
    cache.insert_file_reader(key_of(4), make_reader("r4"));
    EXPECT_EQ(cache.file_reader_cache_size(), 2);
    EXPECT_TRUE(cache.contains_file_reader(key_of(3)));
    EXPECT_TRUE(cache.contains_file_reader(key_of(4)));
}

TEST(FDCacheTest, ConcurrentReadsInsertsAndRemoves) {
    constexpr int kKeys = 512;
    FDCacheCapacityGuard guard(kKeys / 2);
    FDCache cache;
    std::atomic<bool> stop {false};
    std::atomic<int64_t> hits {0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&, t] {
            int i = t;
            while (!stop.load(std::memory_order_relaxed)) {
                if (cache.get_file_reader(key_of(i % kKeys)) != nullptr) {
                    hits.fetch_add(1, std::memory_order_relaxed);
                }
                i += 7;
            }
        });
    }
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t] {
            for (int round = 0; round < 20000; ++round) {
                int i = (round * 13 + t) % kKeys;
                if (round % 3 == 0) {
                    cache.remove_file_reader(key_of(i));
                } else {
                    cache.insert_file_reader(key_of(i), make_reader("c" + std::to_string(i)));
                }
            }
        });
    }
    for (size_t t = 8; t < threads.size(); ++t) {
        threads[t].join();
    }
    stop = true;
    for (int t = 0; t < 8; ++t) {
        threads[t].join();
    }
    EXPECT_LE(cache.file_reader_cache_size(), kKeys / 2);
    EXPECT_GT(hits.load(), 0);
    int present = 0;
    for (int i = 0; i < kKeys; ++i) {
        present += cache.contains_file_reader(key_of(i)) ? 1 : 0;
    }
    EXPECT_EQ(present, static_cast<int>(cache.file_reader_cache_size()));
}

} // namespace doris::io
