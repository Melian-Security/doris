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

#include "cloud/cloud_rowset_version_index.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <random>
#include <set>
#include <tuple>
#include <vector>

#include "storage/olap_common.h"

namespace doris {

namespace {

using Hole = CloudRowsetVersionIndex::Hole;

// The sorted walk that fill_version_holes ran before the index existed: sort every version by
// start version, report a hole wherever the next start is past the previous end + 1, then a
// trailing hole up to `max_version`.
std::vector<Hole> legacy_holes(std::vector<Version> versions, int64_t max_version) {
    std::vector<Hole> holes;
    if (versions.empty()) {
        return holes;
    }
    std::sort(versions.begin(), versions.end(), [](const Version& a, const Version& b) {
        return a.first != b.first ? a.first < b.first : a.second < b.second;
    });
    int64_t last_version = -1;
    for (const auto& v : versions) {
        if (v.first > last_version + 1) {
            holes.push_back({last_version + 1, v.first - 1, v});
        }
        last_version = v.second;
    }
    if (last_version + 1 <= max_version) {
        holes.push_back({last_version + 1, max_version, versions.back()});
    }
    return holes;
}

void expect_same_holes(const std::vector<Hole>& actual, const std::vector<Hole>& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        EXPECT_EQ(actual[i].first, expected[i].first) << "hole " << i;
        EXPECT_EQ(actual[i].last, expected[i].last) << "hole " << i;
        EXPECT_EQ(actual[i].anchor, expected[i].anchor) << "hole " << i;
    }
}

std::vector<Version> as_vector(const std::set<std::pair<int64_t, int64_t>>& versions) {
    std::vector<Version> result;
    for (auto [first, second] : versions) {
        result.emplace_back(first, second);
    }
    return result;
}

} // namespace

TEST(CloudRowsetVersionIndexTest, EmptyIndexHasNoHolesAndNoLast) {
    CloudRowsetVersionIndex index;
    EXPECT_TRUE(index.empty());
    EXPECT_FALSE(index.last().has_value());
    EXPECT_TRUE(index.holes(10).empty());
    EXPECT_TRUE(index.versions_contained_in({0, 100}).empty());
}

TEST(CloudRowsetVersionIndexTest, ContiguousVersionsOnlyHaveTrailingHole) {
    CloudRowsetVersionIndex index;
    index.insert({0, 1});
    for (int64_t v = 2; v <= 100; ++v) {
        index.insert({v, v});
    }
    EXPECT_EQ(index.num_inner_holes(), 0);
    EXPECT_TRUE(index.holes(100).empty());
    auto holes = index.holes(103);
    ASSERT_EQ(holes.size(), 1);
    EXPECT_EQ(holes[0].first, 101);
    EXPECT_EQ(holes[0].last, 103);
    EXPECT_EQ(holes[0].anchor, Version(100, 100));
    EXPECT_EQ(index.last(), Version(100, 100));
}

TEST(CloudRowsetVersionIndexTest, InnerHolesOpenAndCloseOnInsertAndErase) {
    CloudRowsetVersionIndex index;
    index.insert({0, 1});
    index.insert({2, 2});
    index.insert({5, 5});
    index.insert({8, 9});
    // Holes: [3-4] before [5-5], [6-7] before [8-9].
    expect_same_holes(index.holes(9), {{3, 4, {5, 5}}, {6, 7, {8, 9}}});

    index.insert({3, 4});
    expect_same_holes(index.holes(9), {{6, 7, {8, 9}}});

    // Erasing a version in the middle reopens a hole before its successor.
    index.erase({3, 4});
    expect_same_holes(index.holes(9), {{3, 4, {5, 5}}, {6, 7, {8, 9}}});

    // A leading hole when version 0 is missing.
    index.erase({0, 1});
    expect_same_holes(index.holes(9), {{0, 1, {2, 2}}, {3, 4, {5, 5}}, {6, 7, {8, 9}}});
}

TEST(CloudRowsetVersionIndexTest, CompactionReplacementKeepsContinuity) {
    CloudRowsetVersionIndex index;
    index.insert({0, 1});
    for (int64_t v = 2; v <= 20; ++v) {
        index.insert({v, v});
    }
    // A cumulative compaction replaces [5-12] by one output rowset, inputs first.
    auto inputs = index.versions_contained_in({5, 12});
    ASSERT_EQ(inputs.size(), 8);
    for (const auto& v : inputs) {
        index.erase(v);
    }
    // In between, the inputs are gone and [5-12] is a hole.
    expect_same_holes(index.holes(20), {{5, 12, {13, 13}}});
    index.insert({5, 12});
    EXPECT_TRUE(index.holes(20).empty());
    // [0-1], [2-2]..[4-4], [5-12], [13-13]..[20-20]
    EXPECT_EQ(index.size(), 1 + 3 + 1 + 8);
}

TEST(CloudRowsetVersionIndexTest, DuplicateInsertAndMissingEraseAreNoops) {
    CloudRowsetVersionIndex index;
    index.insert({0, 1});
    index.insert({3, 3});
    index.insert({3, 3});
    index.erase({7, 7});
    EXPECT_EQ(index.size(), 2);
    expect_same_holes(index.holes(3), {{2, 2, {3, 3}}});
}

TEST(CloudRowsetVersionIndexTest, VersionsContainedInMatchesFullScan) {
    CloudRowsetVersionIndex index;
    std::vector<Version> versions = {{0, 1}, {2, 5}, {6, 6}, {7, 10}, {11, 11}, {12, 20}};
    for (const auto& v : versions) {
        index.insert(v);
    }
    for (int64_t first = 0; first <= 21; ++first) {
        for (int64_t second = first; second <= 21; ++second) {
            Version range(first, second);
            std::vector<Version> expected;
            for (const auto& v : versions) {
                if (range.contains(v)) {
                    expected.push_back(v);
                }
            }
            EXPECT_EQ(index.versions_contained_in(range), expected) << range;
        }
    }
}

TEST(CloudRowsetVersionIndexTest, ForEachStartingBeforeVisitsPrefix) {
    CloudRowsetVersionIndex index;
    for (const auto& v : std::vector<Version> {{0, 1}, {2, 5}, {6, 6}, {7, 10}, {11, 11}}) {
        index.insert(v);
    }
    std::vector<Version> visited;
    index.for_each_starting_before(7, [&](const Version& v) { visited.push_back(v); });
    EXPECT_EQ(visited, (std::vector<Version> {{0, 1}, {2, 5}, {6, 6}}));
}

// Drive the index through random loads, compactions, schema-change style range deletions and
// hole fills, and check every answer against the legacy sorted walk over the same versions.
TEST(CloudRowsetVersionIndexTest, RandomOperationsMatchLegacySortedWalk) {
    std::mt19937_64 rng(20261006);
    for (int round = 0; round < 20; ++round) {
        CloudRowsetVersionIndex index;
        std::set<std::pair<int64_t, int64_t>> truth;
        auto insert = [&](Version v) {
            index.insert(v);
            truth.emplace(v.first, v.second);
        };
        auto erase = [&](Version v) {
            index.erase(v);
            truth.erase({v.first, v.second});
        };
        insert({0, 1});
        int64_t next_version = 2;
        for (int op = 0; op < 2000; ++op) {
            switch (rng() % 6) {
            case 0:
            case 1:
            case 2: {
                // A load commits the next version; sometimes a version is skipped (a hole).
                if (rng() % 10 == 0) {
                    ++next_version;
                }
                insert({next_version, next_version});
                ++next_version;
                break;
            }
            case 3: {
                // A compaction replaces every version contained in a random range.
                int64_t first = 2 + static_cast<int64_t>(rng() % next_version);
                int64_t second = first + static_cast<int64_t>(rng() % 30);
                auto inputs = index.versions_contained_in({first, second});
                if (inputs.size() < 2) {
                    break;
                }
                for (const auto& v : inputs) {
                    erase(v);
                }
                insert({inputs.front().first, inputs.back().second});
                break;
            }
            case 4: {
                // A random version disappears (schema change style delete).
                if (truth.size() <= 1) {
                    break;
                }
                auto it = truth.begin();
                std::advance(it, static_cast<int64_t>(rng() % truth.size()));
                erase({it->first, it->second});
                break;
            }
            case 5: {
                // Hole filling inserts single versions into every hole.
                for (const auto& hole : index.holes(next_version - 1)) {
                    for (int64_t v = hole.first; v <= hole.last; ++v) {
                        insert({v, v});
                    }
                }
                ASSERT_TRUE(index.holes(next_version - 1).empty());
                break;
            }
            }

            auto versions = as_vector(truth);
            ASSERT_EQ(index.size(), versions.size());
            int64_t max_version = next_version + static_cast<int64_t>(rng() % 3) - 1;
            expect_same_holes(index.holes(max_version), legacy_holes(versions, max_version));
            if (versions.empty()) {
                ASSERT_FALSE(index.last().has_value());
            } else {
                ASSERT_EQ(index.last(), versions.back());
            }
        }
    }
}

} // namespace doris
