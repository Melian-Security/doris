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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <ctime>
#include <memory>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "common/config.h"
#include "storage/compaction/cumulative_compaction_time_series_policy.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {

// Covers how a time-series cumulative compaction picks a run of consecutive empty rowsets
// (condition 6 of TimeSeriesCumulativeCompactionPolicy::pick_input_rowsets).
class CloudTimeSeriesEmptyRunCompactionTest : public testing::Test {
public:
    void SetUp() override {
        _saved_max_rowset_count = config::compaction_max_rowset_count;
        _saved_prefer_empty = config::time_series_compaction_prefer_empty_rowsets;
        config::time_series_compaction_prefer_empty_rowsets = false;
        _tablet_meta = std::make_shared<TabletMeta>(
                1001, 2, 15673, 15674, 4, 5, TTabletSchema(), 6,
                std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
        _tablet_meta->set_compaction_policy(std::string(CUMULATIVE_TIME_SERIES_POLICY));
        _tablet_meta->set_time_series_compaction_goal_size_mbytes(1024);
        _tablet_meta->set_time_series_compaction_file_count_threshold(2000);
        _tablet_meta->set_time_series_compaction_time_threshold_seconds(3600);
        _tablet_meta->set_time_series_compaction_empty_rowsets_threshold(5);
        _tablet = std::make_shared<CloudTablet>(_engine, _tablet_meta);
        _next_version = 2;
        _next_rowset_id = 1;
    }

    void TearDown() override {
        config::compaction_max_rowset_count = _saved_max_rowset_count;
        config::time_series_compaction_prefer_empty_rowsets = _saved_prefer_empty;
    }

    RowsetSharedPtr make_rowset(int64_t start, int64_t end, bool empty,
                                bool delete_predicate = false, int64_t data_size = 1024) {
        auto meta = std::make_shared<RowsetMeta>();
        meta->set_rowset_id(RowsetId {2, 0, 0, _next_rowset_id++});
        meta->set_tablet_id(15673);
        meta->set_txn_id(_next_rowset_id);
        meta->set_tablet_schema_hash(567997577);
        meta->set_rowset_type(BETA_ROWSET);
        meta->set_rowset_state(VISIBLE);
        meta->set_version({start, end});
        meta->set_num_rows(empty ? 0 : 1000);
        meta->set_total_disk_size(empty ? 0 : data_size);
        meta->set_data_disk_size(empty ? 0 : data_size);
        meta->set_index_disk_size(0);
        meta->set_empty(empty);
        meta->set_num_segments(empty ? 0 : 1);
        meta->set_segments_overlap(NONOVERLAPPING);
        // Recent creation time so the time-based condition of the policy never fires.
        meta->set_creation_time(time(nullptr));
        if (delete_predicate) {
            DeletePredicatePB del;
            del.add_sub_predicates("a = 1");
            del.set_version(static_cast<int32_t>(start));
            meta->set_delete_predicate(std::move(del));
        }
        meta->set_tablet_schema(_tablet_meta->tablet_schema());
        RowsetSharedPtr rowset;
        EXPECT_TRUE(
                RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", meta, &rowset)
                        .ok());
        return rowset;
    }

    void append_data(std::vector<RowsetSharedPtr>* rowsets, int64_t data_size = 1024) {
        rowsets->push_back(make_rowset(_next_version, _next_version, false, false, data_size));
        ++_next_version;
    }

    void append_empty(std::vector<RowsetSharedPtr>* rowsets, int count) {
        for (int i = 0; i < count; ++i) {
            rowsets->push_back(make_rowset(_next_version, _next_version, true));
            ++_next_version;
        }
    }

    // Replaces `inputs`, a contiguous slice of `rowsets`, by the single empty rowset that an
    // empty-run compaction outputs.
    void apply_empty_compaction(std::vector<RowsetSharedPtr>* rowsets,
                                const std::vector<RowsetSharedPtr>& inputs) {
        ASSERT_GE(inputs.size(), 2);
        auto first = std::find(rowsets->begin(), rowsets->end(), inputs.front());
        ASSERT_NE(first, rowsets->end());
        auto last = first + static_cast<std::ptrdiff_t>(inputs.size());
        ASSERT_EQ(*(last - 1), inputs.back());
        auto output =
                make_rowset(inputs.front()->start_version(), inputs.back()->end_version(), true);
        first = rowsets->erase(first, last);
        rowsets->insert(first, output);
    }

    std::vector<RowsetSharedPtr> pick(const std::vector<RowsetSharedPtr>& candidates) {
        std::vector<RowsetSharedPtr> input_rowsets;
        Version last_delete_version {-1, -1};
        size_t compaction_score = 0;
        TimeSeriesCumulativeCompactionPolicy::pick_input_rowsets(
                _tablet.get(), time(nullptr) * 1000, candidates, 1000, 5, &input_rowsets,
                &last_delete_version, &compaction_score, false);
        return input_rowsets;
    }

protected:
    CloudStorageEngine _engine {EngineOptions {}};
    TabletMetaSharedPtr _tablet_meta;
    std::shared_ptr<CloudTablet> _tablet;
    int64_t _next_version = 2;
    int64_t _next_rowset_id = 1;
    int32_t _saved_max_rowset_count = 0;
    bool _saved_prefer_empty = false;
};

// The table property is a minimum run length, not a cap: the whole run is picked.
TEST_F(CloudTimeSeriesEmptyRunCompactionTest, threshold_is_minimum_run_taken_whole) {
    std::vector<RowsetSharedPtr> rowsets;
    append_data(&rowsets);        // 2
    append_empty(&rowsets, 300);  // 3..302
    append_data(&rowsets);        // 303
    append_empty(&rowsets, 3);    // 304..306
    append_data(&rowsets);        // 307

    std::vector<RowsetSharedPtr> out;
    BaseTablet::calc_consecutive_empty_rowsets(&out, rowsets, 5);
    ASSERT_EQ(300, out.size());
    EXPECT_EQ(3, out.front()->start_version());
    EXPECT_EQ(302, out.back()->end_version());

    // A threshold above every run length disables empty-run compaction entirely, which is
    // what setting the property to 10000 does for runs shorter than 10000.
    BaseTablet::calc_consecutive_empty_rowsets(&out, rowsets, 10000);
    EXPECT_TRUE(out.empty());

    // The first qualifying run wins even if a later one is longer.
    BaseTablet::calc_consecutive_empty_rowsets(&out, rowsets, 3);
    ASSERT_EQ(300, out.size());
}

TEST_F(CloudTimeSeriesEmptyRunCompactionTest, tail_run_is_not_picked) {
    std::vector<RowsetSharedPtr> rowsets;
    append_data(&rowsets);
    append_empty(&rowsets, 50);

    std::vector<RowsetSharedPtr> out;
    BaseTablet::calc_consecutive_empty_rowsets(&out, rowsets, 5);
    EXPECT_TRUE(out.empty());

    // Once a later rowset exists the run is closed and is picked.
    append_data(&rowsets);
    BaseTablet::calc_consecutive_empty_rowsets(&out, rowsets, 5);
    EXPECT_EQ(50, out.size());
}

TEST_F(CloudTimeSeriesEmptyRunCompactionTest, delete_predicate_and_version_gap_split_runs) {
    std::vector<RowsetSharedPtr> rowsets;
    append_data(&rowsets);       // 2
    append_empty(&rowsets, 4);   // 3..6
    rowsets.push_back(make_rowset(_next_version, _next_version, true, true)); // 7, delete
    ++_next_version;
    append_empty(&rowsets, 4);   // 8..11
    _next_version += 2;          // hole at 12..13
    append_empty(&rowsets, 6);   // 14..19
    append_data(&rowsets);       // 20

    std::vector<RowsetSharedPtr> out;
    BaseTablet::calc_consecutive_empty_rowsets(&out, rowsets, 5);
    ASSERT_EQ(6, out.size());
    EXPECT_EQ(14, out.front()->start_version());
    EXPECT_EQ(19, out.back()->end_version());
}

// A run longer than the cap is taken in batches; each batch output is an empty rowset that the
// next batch absorbs, so a run of N needs ceil((N - 1) / (cap - 1)) compactions.
TEST_F(CloudTimeSeriesEmptyRunCompactionTest, cap_takes_long_run_in_batches) {
    std::vector<RowsetSharedPtr> rowsets;
    append_data(&rowsets);       // 2
    append_empty(&rowsets, 25);  // 3..27
    append_data(&rowsets);       // 28

    int tasks = 0;
    std::vector<RowsetSharedPtr> out;
    while (true) {
        BaseTablet::calc_consecutive_empty_rowsets(&out, rowsets, 5, 10);
        if (out.empty()) {
            break;
        }
        EXPECT_LE(out.size(), 10);
        apply_empty_compaction(&rowsets, out);
        ++tasks;
        ASSERT_LT(tasks, 100);
    }
    EXPECT_EQ(3, tasks);
    ASSERT_EQ(3, rowsets.size());
    EXPECT_EQ(3, rowsets[1]->start_version());
    EXPECT_EQ(27, rowsets[1]->end_version());
    EXPECT_EQ(0, rowsets[1]->num_segments());

    // A tail run that is exactly as long as the cap is still open and is not picked.
    std::vector<RowsetSharedPtr> tail;
    append_data(&tail);
    append_empty(&tail, 10);
    BaseTablet::calc_consecutive_empty_rowsets(&out, tail, 5, 10);
    EXPECT_TRUE(out.empty());
}

// Without a cap the helper behaves as before: the full run in one pick.
TEST_F(CloudTimeSeriesEmptyRunCompactionTest, uncapped_long_run_single_pick) {
    std::vector<RowsetSharedPtr> rowsets;
    append_data(&rowsets);
    append_empty(&rowsets, 5000);
    append_data(&rowsets);

    std::vector<RowsetSharedPtr> out;
    BaseTablet::calc_consecutive_empty_rowsets(&out, rowsets, 5);
    EXPECT_EQ(5000, out.size());
}

// Through the policy: condition 6 picks the whole interior run when no data condition fires.
TEST_F(CloudTimeSeriesEmptyRunCompactionTest, policy_picks_whole_interior_run) {
    config::compaction_max_rowset_count = 10000;
    std::vector<RowsetSharedPtr> rowsets;
    append_data(&rowsets);       // 2
    append_empty(&rowsets, 31);  // 3..33
    append_data(&rowsets);       // 34
    append_empty(&rowsets, 31);  // 35..65
    append_data(&rowsets);       // 66
    append_empty(&rowsets, 7);   // 67..73, still growing

    auto picked = pick(rowsets);
    ASSERT_EQ(31, picked.size());
    EXPECT_EQ(3, picked.front()->start_version());
    EXPECT_EQ(33, picked.back()->end_version());
}

// Through the policy: once the data in the window reaches the goal size, condition 1 wins
// and the empty rowsets ride along inside the data compaction instead of being merged alone.
TEST_F(CloudTimeSeriesEmptyRunCompactionTest, policy_data_window_absorbs_empty_rowsets) {
    config::compaction_max_rowset_count = 10000;
    _tablet_meta->set_time_series_compaction_goal_size_mbytes(1);
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 0; i < 40; ++i) {
        append_data(&rowsets, 64 * 1024);
        append_empty(&rowsets, 31);
    }
    auto picked = pick(rowsets);
    // 1 MiB of 64 KiB data rowsets: 16 data rowsets, with the 31 empty rowsets between each.
    ASSERT_EQ(16 + 15 * 31, picked.size());
    int64_t empty = 0;
    for (const auto& rs : picked) {
        empty += rs->num_segments() == 0;
    }
    EXPECT_EQ(15 * 31, empty);
}

// Through the policy: the rowset count cap of conditions 1 and 2 bounds any single pick.
TEST_F(CloudTimeSeriesEmptyRunCompactionTest, policy_pick_bounded_by_max_rowset_count) {
    config::compaction_max_rowset_count = 64;
    std::vector<RowsetSharedPtr> rowsets;
    append_data(&rowsets);
    append_empty(&rowsets, 500);
    append_data(&rowsets);

    auto picked = pick(rowsets);
    EXPECT_EQ(64, picked.size());
}

// With time_series_compaction_prefer_empty_rowsets, an interior empty run is merged before the
// data window that would otherwise take the tablet, and the data window then has fewer inputs.
TEST_F(CloudTimeSeriesEmptyRunCompactionTest, prefer_empty_rowsets_runs_before_data_window) {
    config::compaction_max_rowset_count = 10000;
    _tablet_meta->set_time_series_compaction_goal_size_mbytes(1);
    std::vector<RowsetSharedPtr> rowsets;
    for (int i = 0; i < 40; ++i) {
        append_data(&rowsets, 64 * 1024);
        append_empty(&rowsets, 31);
    }

    config::time_series_compaction_prefer_empty_rowsets = true;
    int empty_merges = 0;
    while (true) {
        auto picked = pick(rowsets);
        ASSERT_FALSE(picked.empty());
        if (picked.front()->num_segments() != 0) {
            // Data window: every interior run is already one merged empty rowset.
            ASSERT_EQ(16 + 15, picked.size());
            break;
        }
        ASSERT_EQ(31, picked.size());
        apply_empty_compaction(&rowsets, picked);
        ++empty_merges;
        ASSERT_LE(empty_merges, 40);
    }
    // The run after the last data rowset is the open tail and is left alone.
    EXPECT_EQ(39, empty_merges);
}

// The preferred pick never crosses or follows a delete version.
TEST_F(CloudTimeSeriesEmptyRunCompactionTest, prefer_empty_rowsets_stops_at_delete_version) {
    config::compaction_max_rowset_count = 10000;
    config::time_series_compaction_prefer_empty_rowsets = true;
    std::vector<RowsetSharedPtr> rowsets;
    append_data(&rowsets);       // 2
    append_empty(&rowsets, 3);   // 3..5
    rowsets.push_back(make_rowset(_next_version, _next_version, false, true)); // 6, delete
    ++_next_version;
    append_empty(&rowsets, 20);  // 7..26
    append_data(&rowsets);       // 27

    auto picked = pick(rowsets);
    // Without the preferred pick the policy meets the delete version: it returns the rowsets
    // before it (data 2 and the empty run 3..5) and records the delete version.
    ASSERT_EQ(4, picked.size());
    EXPECT_EQ(2, picked.front()->start_version());
    EXPECT_EQ(5, picked.back()->end_version());
}

} // namespace doris
