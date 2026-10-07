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
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/metrics/doris_metrics.h"
#include "cpp/sync_point.h"
#include "runtime/exec_env.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_meta.h"

namespace doris {

// After TRUNCATE or DROP PARTITION the meta service answers TABLET_NOT_FOUND for the old
// tablets, while the BE still holds them with the score of their compaction backlog. These tests
// cover that compaction marks such a tablet dropped, stops picking it, and refuses to start on it.
class CloudDroppedTabletCompactionTest : public testing::Test {
protected:
    CloudDroppedTabletCompactionTest() : _engine(EngineOptions {}) {}

    void SetUp() override {
        _cluster_info = std::make_shared<CloudClusterInfo>();
        _cluster_info->_is_in_standby = false;
        ExecEnv::GetInstance()->_cluster_info = _cluster_info.get();
        auto* sp = SyncPoint::get_instance();
        sp->clear_all_call_backs();
        sp->enable_processing();
    }

    void TearDown() override {
        auto* sp = SyncPoint::get_instance();
        sp->clear_all_call_backs();
        sp->disable_processing();
    }

    CloudTabletSPtr make_tablet(int64_t tablet_id) {
        auto tablet_meta = std::make_shared<TabletMeta>(
                1, 2, tablet_id, 15674, 4, 5, TTabletSchema(), 6,
                std::unordered_map<uint32_t, uint32_t> {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F);
        tablet_meta->set_tablet_state(TABLET_RUNNING);
        auto tablet = std::make_shared<CloudTablet>(_engine, tablet_meta);
        tablet->tablet_meta()->tablet_schema()->set_disable_auto_compaction(false);
        return tablet;
    }

    static RowsetSharedPtr make_rowset(int64_t start, int64_t end) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->_rowset_meta_pb.set_start_version(start);
        rs_meta->_rowset_meta_pb.set_end_version(end);
        rs_meta->set_num_segments(1);
        rs_meta->set_segments_overlap(NONOVERLAPPING);
        rs_meta->set_total_disk_size(1024);
        RowsetSharedPtr rowset;
        EXPECT_TRUE(RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset).ok());
        return rowset;
    }

    // Makes the next meta-service call at `point` fail with `code`, as a dropped tablet does.
    template <typename Response>
    static void fail_meta_service_call(const std::string& point, cloud::MetaServiceCode code) {
        SyncPoint::get_instance()->set_call_back(point, [code](auto&& outcome) {
            auto* pairs = try_any_cast_ret<Status>(outcome);
            pairs->first = Status::InternalError<false>("injected meta service error");
            pairs->second = true;
            auto* resp = try_any_cast<Response*>(outcome[1]);
            resp->mutable_status()->set_code(code);
        });
    }

    CloudStorageEngine _engine;
    std::shared_ptr<CloudClusterInfo> _cluster_info;
};

TEST_F(CloudDroppedTabletCompactionTest, topn_skips_dropped_tablet) {
    CloudTabletMgr mgr(_engine);
    auto tablet = make_tablet(20001);
    tablet->_approximate_cumu_num_deltas = 5000;
    mgr.put_tablet_for_UT(tablet);
    auto filter_out = [](CloudTablet*) { return false; };

    std::vector<std::shared_ptr<CloudTablet>> picked;
    int64_t max_score = 0;
    ASSERT_TRUE(mgr.get_topn_tablets_to_compact(1, CompactionType::CUMULATIVE_COMPACTION,
                                                filter_out, &picked, &max_score)
                        .ok());
    ASSERT_EQ(picked.size(), 1);
    picked.clear();

    tablet->mark_dropped();
    ASSERT_TRUE(mgr.get_topn_tablets_to_compact(1, CompactionType::CUMULATIVE_COMPACTION,
                                                filter_out, &picked, &max_score)
                        .ok());
    EXPECT_TRUE(picked.empty());
    // The dropped tablet's backlog no longer counts toward the reported max score either.
    EXPECT_EQ(max_score, 0);
}

TEST_F(CloudDroppedTabletCompactionTest, max_score_metric_falls_once_backlog_is_dropped) {
    auto tablet = make_tablet(20007);
    tablet->_approximate_cumu_num_deltas = 984;
    _engine.tablet_mgr().put_tablet_for_UT(tablet);
    auto* gauge = DorisMetrics::instance()->tablet_cumulative_max_compaction_score;

    _engine._generate_cloud_compaction_tasks(CompactionType::CUMULATIVE_COMPACTION, true);
    EXPECT_GT(gauge->value(), 0);

    tablet->mark_dropped();
    _engine._generate_cloud_compaction_tasks(CompactionType::CUMULATIVE_COMPACTION, true);
    EXPECT_EQ(gauge->value(), 0);
}

TEST_F(CloudDroppedTabletCompactionTest, lease_tablet_not_found_marks_cumu_tablet_dropped) {
    auto tablet = make_tablet(20002);
    CloudCumulativeCompaction compaction(_engine, tablet);

    SyncPoint::get_instance()->set_call_back("CloudMetaMgr::lease_tablet_job", [](auto&& outcome) {
        auto* pairs = try_any_cast_ret<Status>(outcome);
        pairs->first = Status::OK();
        pairs->second = true;
    });
    compaction.do_lease();
    EXPECT_FALSE(tablet->is_dropped());

    fail_meta_service_call<cloud::FinishTabletJobResponse>("CloudMetaMgr::lease_tablet_job",
                                                           cloud::TABLET_NOT_FOUND);
    compaction.do_lease();
    EXPECT_TRUE(tablet->is_dropped());
}

TEST_F(CloudDroppedTabletCompactionTest, lease_other_error_keeps_base_tablet) {
    auto tablet = make_tablet(20003);
    CloudBaseCompaction compaction(_engine, tablet);

    fail_meta_service_call<cloud::FinishTabletJobResponse>("CloudMetaMgr::lease_tablet_job",
                                                           cloud::KV_TXN_CONFLICT);
    compaction.do_lease();
    EXPECT_FALSE(tablet->is_dropped());

    fail_meta_service_call<cloud::FinishTabletJobResponse>("CloudMetaMgr::lease_tablet_job",
                                                           cloud::TABLET_NOT_FOUND);
    compaction.do_lease();
    EXPECT_TRUE(tablet->is_dropped());
}

TEST_F(CloudDroppedTabletCompactionTest, prepare_job_tablet_not_found_marks_tablet_dropped) {
    auto tablet = make_tablet(20004);
    CloudCumulativeCompaction compaction(_engine, tablet);
    compaction._input_rowsets = {make_rowset(2, 2), make_rowset(3, 3)};

    fail_meta_service_call<cloud::StartTabletJobResponse>("CloudMetaMgr::prepare_tablet_job",
                                                          cloud::TABLET_NOT_FOUND);
    EXPECT_FALSE(compaction.request_global_lock().ok());
    EXPECT_TRUE(tablet->is_dropped());
}

TEST_F(CloudDroppedTabletCompactionTest, prepare_compact_rejects_dropped_tablet) {
    auto tablet = make_tablet(20005);
    tablet->mark_dropped();

    CloudCumulativeCompaction cumu(_engine, tablet);
    auto st = cumu.prepare_compact();
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("dropped"), std::string::npos) << st;

    CloudBaseCompaction base(_engine, tablet);
    st = base.prepare_compact();
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("dropped"), std::string::npos) << st;
}

TEST_F(CloudDroppedTabletCompactionTest, execute_compact_stops_when_lease_finds_tablet_dropped) {
    auto tablet = make_tablet(20006);
    CloudBaseCompaction compaction(_engine, tablet);

    fail_meta_service_call<cloud::FinishTabletJobResponse>("CloudMetaMgr::lease_tablet_job",
                                                           cloud::TABLET_NOT_FOUND);
    auto st = compaction.execute_compact();
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("dropped"), std::string::npos) << st;
    EXPECT_TRUE(tablet->is_dropped());
}

} // namespace doris
