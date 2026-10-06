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

#include "cloud/cloud_delta_writer.h"

#include <gen_cpp/Descriptors_types.h>
#include <gtest/gtest.h>

#include <memory>

#include "cloud/cloud_committed_rs_mgr.h"
#include "cloud/cloud_rowset_builder.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/cloud_txn_delete_bitmap_cache.h"
#include "cloud/config.h"
#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/fs/s3_file_system.h"
#include "runtime/runtime_profile.h"
#include "storage/delete/calc_delete_bitmap_executor.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet_info.h"
#include "util/debug_points.h"
#include "util/uid_util.h"

namespace doris {

namespace {
constexpr int64_t kDbId = 10000;
constexpr int64_t kTableId = 10001;
constexpr int64_t kPartitionId = 10002;
constexpr int64_t kIndexId = 10003;
constexpr int32_t kSchemaHash = 10004;
constexpr int64_t kTxnId = 30001;
} // namespace

// A load with `load_to_single_tablet=true` writes one tablet of the partition, and the tablets
// channel commits an empty rowset to every other tablet of that partition. These tests drive
// CloudDeltaWriter through the sequence CloudTabletsChannel::close() runs for such an untouched
// (never initialized) writer.
class CloudDeltaWriterTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_skip_metadata = config::skip_writing_empty_rowset_metadata;
        _saved_skip_writer = config::skip_rowset_writer_for_empty_tablet;
        _saved_make_visible = config::enable_cloud_make_rs_visible_on_be;
        config::skip_writing_empty_rowset_metadata = true;
        config::skip_rowset_writer_for_empty_tablet = true;
        config::enable_cloud_make_rs_visible_on_be = true;

        _engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
        _engine->_txn_delete_bitmap_cache = std::make_unique<CloudTxnDeleteBitmapCache>(1 << 20);
        ASSERT_TRUE(_engine->_txn_delete_bitmap_cache->init().ok());
        _engine->_committed_rs_mgr = std::make_unique<CloudCommittedRSMgr>();
        ASSERT_TRUE(_engine->_committed_rs_mgr->init().ok());
        _profile = std::make_unique<RuntimeProfile>("CloudDeltaWriterTest");
    }

    void TearDown() override {
        auto* sp = SyncPoint::get_instance();
        sp->disable_processing();
        sp->clear_all_call_backs();
        _engine.reset();
        config::skip_writing_empty_rowset_metadata = _saved_skip_metadata;
        config::skip_rowset_writer_for_empty_tablet = _saved_skip_writer;
        config::enable_cloud_make_rs_visible_on_be = _saved_make_visible;
    }

    std::shared_ptr<CloudTablet> make_tablet(int64_t tablet_id, bool mow) {
        TTabletSchema tschema;
        tschema.keys_type = mow ? TKeysType::UNIQUE_KEYS : TKeysType::DUP_KEYS;
        TabletMetaSharedPtr meta(new TabletMeta(
                kTableId, kPartitionId, tablet_id, tablet_id, kSchemaHash, 0, tschema, 6, {{7, 8}},
                UniqueId(tablet_id, 1), TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F,
                /*storage_policy_id=*/0, /*enable_unique_key_merge_on_write=*/mow));
        return std::make_shared<CloudTablet>(*_engine, std::move(meta));
    }

    std::shared_ptr<CloudTablet> cache_tablet(int64_t tablet_id, bool mow = false) {
        auto tablet = make_tablet(tablet_id, mow);
        _engine->tablet_mgr().put_tablet_in_cache_for_UT(tablet);
        return tablet;
    }

    std::unique_ptr<CloudDeltaWriter> make_writer(int64_t tablet_id) {
        WriteRequest req;
        req.tablet_id = tablet_id;
        req.schema_hash = kSchemaHash;
        req.txn_id = kTxnId;
        req.txn_expiration = 0;
        req.index_id = kIndexId;
        req.partition_id = kPartitionId;
        req.load_id.set_hi(1);
        req.load_id.set_lo(2);
        req.table_schema_param = _schema_param;
        return std::make_unique<CloudDeltaWriter>(*_engine, req, _profile.get(), UniqueId(1, 2));
    }

    // What CloudRowsetBuilder::init() needs to build the rowset writer of an empty rowset without
    // a meta service: a load schema, a storage resource and the delete bitmap executor.
    void enable_rowset_builder() {
        _engine->_calc_delete_bitmap_executor = std::make_unique<CalcDeleteBitmapExecutor>();
        _engine->_calc_delete_bitmap_executor->init("CloudDeltaWriterTest", 1);

        S3Conf s3_conf;
        s3_conf.client_conf.ak = "fake_ak";
        s3_conf.client_conf.sk = "fake_sk";
        s3_conf.client_conf.endpoint = "fake_s3_endpoint";
        s3_conf.client_conf.region = "fake_s3_region";
        s3_conf.bucket = "fake_s3_bucket";
        s3_conf.prefix = "cloud_delta_writer_test";
        auto fs = io::S3FileSystem::create(std::move(s3_conf), "cloud-delta-writer-ut-fs");
        ASSERT_TRUE(fs.has_value()) << fs.error();
        _engine->set_latest_fs(fs.value());

        TOlapTableSchemaParam tschema;
        tschema.db_id = kDbId;
        tschema.table_id = kTableId;
        tschema.version = 0;
        tschema.indexes.resize(1);
        tschema.indexes[0].id = kIndexId;
        tschema.indexes[0].schema_hash = kSchemaHash;
        _schema_param = std::make_shared<OlapTableSchemaParam>();
        ASSERT_TRUE(_schema_param->init(tschema).ok());
    }

    // The part of CloudTabletsChannel::close() that runs for each writer of a closed partition
    static Status close_untouched_writer(CloudDeltaWriter* writer) {
        RETURN_IF_ERROR(writer->commit_rowset());
        RETURN_IF_ERROR(writer->submit_calc_delete_bitmap_task());
        RETURN_IF_ERROR(writer->wait_calc_delete_bitmap());
        RETURN_IF_ERROR(writer->set_txn_related_info());
        EXPECT_EQ(writer->total_received_rows(), 0);
        EXPECT_EQ(writer->num_rows_filtered(), 0);
        writer->update_tablet_stats();
        return Status::OK();
    }

    static bool rowset_builder_initialized(CloudDeltaWriter* writer) {
        return writer->_rowset_builder->_is_init;
    }

    bool has_committed_rs_marker(int64_t tablet_id) {
        auto res = _engine->committed_rs_mgr().get_committed_rowset(kTxnId, tablet_id);
        return res.has_value() && res.value().first == nullptr;
    }

    std::unique_ptr<CloudStorageEngine> _engine;
    std::unique_ptr<RuntimeProfile> _profile;
    std::shared_ptr<OlapTableSchemaParam> _schema_param;
    bool _saved_skip_metadata = true;
    bool _saved_skip_writer = true;
    bool _saved_make_visible = false;
};

TEST_F(CloudDeltaWriterTest, UntouchedTabletCommitsWithoutRowsetWriter) {
    auto tablet = cache_tablet(20001);
    {
        auto writer = make_writer(20001);
        ASSERT_FALSE(writer->is_init());
        ASSERT_TRUE(close_untouched_writer(writer.get()).ok());

        EXPECT_TRUE(writer->committed_without_rowset_writer());
        EXPECT_FALSE(rowset_builder_initialized(writer.get()));
        EXPECT_EQ(writer->_rowset_builder->rowset_writer(), nullptr);
        EXPECT_EQ(writer->_rowset_builder->tablet(), nullptr);
    }
    // The make-visible path still finds this txn's empty rowset for the tablet
    EXPECT_TRUE(has_committed_rs_marker(20001));
    EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 1);
    EXPECT_EQ(tablet->fetch_add_approximate_cumu_num_rowsets(0), 1);
    EXPECT_EQ(tablet->fetch_add_approximate_num_segments(0), 0);
    EXPECT_EQ(tablet->fetch_add_approximate_num_rows(0), 0);
    EXPECT_EQ(tablet->write_count.load(), 1);
    EXPECT_GT(tablet->last_load_time_ms, 0);
}

TEST_F(CloudDeltaWriterTest, UntouchedTabletWithoutMakeVisibleLeavesNoMarker) {
    config::enable_cloud_make_rs_visible_on_be = false;
    auto tablet = cache_tablet(20002);
    auto writer = make_writer(20002);
    ASSERT_TRUE(close_untouched_writer(writer.get()).ok());
    EXPECT_TRUE(writer->committed_without_rowset_writer());
    EXPECT_FALSE(has_committed_rs_marker(20002));
    EXPECT_EQ(tablet->fetch_add_approximate_num_rowsets(0), 1);
}

TEST_F(CloudDeltaWriterTest, MowUntouchedTabletLeavesEmptyRowsetMarker) {
    auto tablet = cache_tablet(20003, /*mow=*/true);
    auto writer = make_writer(20003);
    ASSERT_TRUE(close_untouched_writer(writer.get()).ok());
    EXPECT_TRUE(writer->committed_without_rowset_writer());
    EXPECT_FALSE(rowset_builder_initialized(writer.get()));
    // CloudTabletCalcDeleteBitmapTask skips the tablet on this marker
    EXPECT_TRUE(_engine->txn_delete_bitmap_cache().is_empty_rowset(kTxnId, 20003));
    EXPECT_FALSE(has_committed_rs_marker(20003));
}

TEST_F(CloudDeltaWriterTest, UncachedTabletFallsBackToRowsetBuilder) {
    auto* sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();
    int get_tablet_meta_calls = 0;
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&](auto&& args) {
        ++get_tablet_meta_calls;
        auto* ret = try_any_cast_ret<Status>(args);
        ret->first = Status::InternalError("injected get_tablet_meta failure");
        ret->second = true;
    });

    auto writer = make_writer(20004);
    // The rowset builder loads the tablet from the meta service, which fails here
    EXPECT_FALSE(writer->commit_rowset().ok());
    EXPECT_EQ(get_tablet_meta_calls, 1);
    EXPECT_FALSE(writer->committed_without_rowset_writer());
}

// Whether commit_rowset() of an untouched writer for a cached tablet goes through
// CloudRowsetBuilder::init(); the injected version-count check failure only happens there.
static bool commit_uses_rowset_builder(CloudDeltaWriter* writer) {
    bool saved = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add("RowsetBuilder.check_tablet_version_count.too_many_version");
    auto st = writer->commit_rowset();
    DebugPoints::instance()->clear();
    config::enable_debug_points = saved;
    return st.is<ErrorCode::TOO_MANY_VERSION>();
}

TEST_F(CloudDeltaWriterTest, InjectedVersionCountFailureOnlyReachesRowsetBuilder) {
    cache_tablet(20005);
    auto writer = make_writer(20005);
    EXPECT_FALSE(commit_uses_rowset_builder(writer.get()));
    EXPECT_TRUE(writer->committed_without_rowset_writer());
}

TEST_F(CloudDeltaWriterTest, WritingEmptyRowsetMetadataKeepsRowsetBuilder) {
    // The meta service then needs an empty rowset KV per tablet, built by the rowset writer
    config::skip_writing_empty_rowset_metadata = false;
    cache_tablet(20008);
    auto writer = make_writer(20008);
    EXPECT_TRUE(commit_uses_rowset_builder(writer.get()));
    EXPECT_FALSE(writer->committed_without_rowset_writer());
}

TEST_F(CloudDeltaWriterTest, DisabledByConfigKeepsRowsetBuilder) {
    config::skip_rowset_writer_for_empty_tablet = false;
    cache_tablet(20009);
    auto writer = make_writer(20009);
    EXPECT_TRUE(commit_uses_rowset_builder(writer.get()));
    EXPECT_FALSE(writer->committed_without_rowset_writer());
}

TEST_F(CloudDeltaWriterTest, SameBookkeepingAsRowsetBuilder) {
    enable_rowset_builder();
    auto base_tablet = cache_tablet(20006);
    auto fix_tablet = cache_tablet(20007);
    {
        config::skip_rowset_writer_for_empty_tablet = false;
        auto base_writer = make_writer(20006);
        ASSERT_TRUE(close_untouched_writer(base_writer.get()).ok());
        EXPECT_FALSE(base_writer->committed_without_rowset_writer());
        EXPECT_TRUE(rowset_builder_initialized(base_writer.get()));
        EXPECT_NE(base_writer->_rowset_builder->rowset_writer(), nullptr);

        config::skip_rowset_writer_for_empty_tablet = true;
        auto fix_writer = make_writer(20007);
        ASSERT_TRUE(close_untouched_writer(fix_writer.get()).ok());
        EXPECT_TRUE(fix_writer->committed_without_rowset_writer());
        EXPECT_FALSE(rowset_builder_initialized(fix_writer.get()));
    }
    EXPECT_TRUE(has_committed_rs_marker(20006));
    EXPECT_TRUE(has_committed_rs_marker(20007));
    for (auto* t : {base_tablet.get(), fix_tablet.get()}) {
        EXPECT_EQ(t->fetch_add_approximate_num_rowsets(0), 1) << t->tablet_id();
        EXPECT_EQ(t->fetch_add_approximate_cumu_num_rowsets(0), 1) << t->tablet_id();
        EXPECT_EQ(t->fetch_add_approximate_num_segments(0), 0) << t->tablet_id();
        EXPECT_EQ(t->fetch_add_approximate_cumu_num_deltas(0), 0) << t->tablet_id();
        EXPECT_EQ(t->fetch_add_approximate_data_size(0), 0) << t->tablet_id();
        EXPECT_EQ(t->write_count.load(), 1) << t->tablet_id();
        EXPECT_GT(t->last_load_time_ms, 0) << t->tablet_id();
    }
}

} // namespace doris
