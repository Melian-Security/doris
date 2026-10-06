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

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <vector>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet_meta.h"

namespace doris {

using VersionSet = std::set<std::pair<int64_t, int64_t>>;

// Version holes of a cloud tablet (versions whose loads wrote other tablets) are filled with
// empty hole rowsets. These tests cover filling a run of holes with one rowset covering the run.
class CloudHoleRowsetRangeTest : public testing::Test {
public:
    CloudHoleRowsetRangeTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        _saved_enable = config::enable_hole_rowset_version_range;
        _saved_max_versions = config::hole_rowset_max_versions;
        config::enable_hole_rowset_version_range = true;
        config::hole_rowset_max_versions = 128;
        _tablet = make_tablet(false);
    }

    void TearDown() override {
        config::enable_hole_rowset_version_range = _saved_enable;
        config::hole_rowset_max_versions = _saved_max_versions;
    }

    std::shared_ptr<CloudTablet> make_tablet(bool merge_on_write) {
        auto tablet_meta = std::make_shared<TabletMeta>(
                1, 2, 15673, 15674, 4, 5, TTabletSchema(), 6, {{7, 8}}, UniqueId(9, 10),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, merge_on_write);
        tablet_meta->set_tablet_state(TABLET_RUNNING);
        return std::make_shared<CloudTablet>(_engine, tablet_meta);
    }

    RowsetSharedPtr create_rowset(Version version, int num_segments = 1) {
        auto rs_meta = std::make_shared<RowsetMeta>();
        rs_meta->set_rowset_type(BETA_ROWSET);
        rs_meta->set_version(version);
        rs_meta->set_rowset_id(_engine.next_rowset_id());
        rs_meta->set_num_segments(num_segments);
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        auto* col = schema_pb.add_column();
        col->set_unique_id(0);
        col->set_name("k1");
        col->set_type("INT");
        col->set_is_key(true);
        col->set_is_nullable(false);
        rs_meta->set_tablet_schema(schema_pb);
        RowsetSharedPtr rowset;
        EXPECT_TRUE(RowsetFactory::create_rowset(nullptr, "", rs_meta, &rowset).ok());
        return rowset;
    }

    void add_rowsets(CloudTablet* tablet, std::vector<RowsetSharedPtr> rowsets, bool overlap) {
        std::unique_lock wlock(tablet->get_header_lock());
        tablet->add_rowsets(std::move(rowsets), overlap, wlock, false);
    }

    void add_empty_pending(CloudTablet* tablet, int64_t version) {
        std::lock_guard<std::mutex> lock(tablet->_visible_pending_rs_lock);
        tablet->_visible_pending_rs_map.emplace(
                version, CloudTablet::VisiblePendingRowset {nullptr, INT64_MAX, true});
    }

    void add_real_pending(CloudTablet* tablet, int64_t version) {
        auto rs = create_rowset(Version(version, version));
        std::lock_guard<std::mutex> lock(tablet->_visible_pending_rs_lock);
        tablet->_visible_pending_rs_map.emplace(
                version, CloudTablet::VisiblePendingRowset {rs->rowset_meta(), INT64_MAX, false});
    }

    Status fill_version_holes(CloudTablet* tablet, int64_t max_version) {
        std::unique_lock wlock(tablet->get_header_lock());
        return _engine.meta_mgr().fill_version_holes(tablet, max_version, wlock);
    }

    static VersionSet live_versions(const CloudTablet& tablet) {
        VersionSet versions;
        for (const auto& [v, _] : tablet.rowset_map()) {
            versions.emplace(v.first, v.second);
        }
        return versions;
    }

    // Every version in [1, max_version] must be readable through the version graph, and the
    // rowsets on the captured path must cover [0, v] exactly.
    static void expect_all_versions_capturable(const CloudTablet& tablet) {
        for (int64_t v = 1; v <= tablet.max_version_unlocked(); ++v) {
            auto path = tablet.capture_consistent_versions_unlocked(Version(0, v), {});
            ASSERT_TRUE(path.has_value()) << "version " << v << ": " << path.error();
            int64_t next = 0;
            for (const auto& version : path.value()) {
                ASSERT_EQ(version.first, next) << "version " << v;
                ASSERT_TRUE(tablet.get_rowset_by_version(version, true) != nullptr)
                        << "version " << v << " path rowset " << version.to_string();
                next = version.second + 1;
            }
            ASSERT_EQ(next, v + 1);
        }
    }

protected:
    CloudStorageEngine _engine;
    std::shared_ptr<CloudTablet> _tablet;
    bool _saved_enable = true;
    int64_t _saved_max_versions = 128;
};

TEST_F(CloudHoleRowsetRangeTest, ApplyPendingCoalescesEmptyRun) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    for (int64_t v = 2; v <= 5; ++v) {
        add_empty_pending(_tablet.get(), v);
    }
    _tablet->apply_visible_pending_rowsets();

    EXPECT_EQ(_tablet->max_version_unlocked(), 5);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 5}}));
    EXPECT_TRUE(_tablet->get_rowset_by_version({2, 5})->is_hole_rowset());
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, ApplyPendingExtendsTrailingHole) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    // One publish per version, the production pattern of a tablet that most loads do not write.
    for (int64_t v = 2; v <= 40; ++v) {
        add_empty_pending(_tablet.get(), v);
        _tablet->apply_visible_pending_rowsets();
        ASSERT_EQ(_tablet->max_version_unlocked(), v);
        ASSERT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, v}}));
    }
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, ApplyPendingMixesRealAndEmptyVersions) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    add_empty_pending(_tablet.get(), 2);
    add_empty_pending(_tablet.get(), 3);
    add_real_pending(_tablet.get(), 4);
    add_empty_pending(_tablet.get(), 5);
    add_empty_pending(_tablet.get(), 6);
    _tablet->apply_visible_pending_rowsets();

    EXPECT_EQ(_tablet->max_version_unlocked(), 6);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 3}, {4, 4}, {5, 6}}));
    EXPECT_FALSE(_tablet->get_rowset_by_version({4, 4})->is_hole_rowset());
    expect_all_versions_capturable(*_tablet);

    // A real version ends the trailing hole; the next empty version starts a new one.
    add_real_pending(_tablet.get(), 7);
    add_empty_pending(_tablet.get(), 8);
    _tablet->apply_visible_pending_rowsets();
    add_empty_pending(_tablet.get(), 9);
    _tablet->apply_visible_pending_rowsets();
    EXPECT_EQ(live_versions(*_tablet),
              (VersionSet {{0, 1}, {2, 3}, {4, 4}, {5, 6}, {7, 7}, {8, 9}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, HoleHeldByCompactionIsNotExtended) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    add_empty_pending(_tablet.get(), 2);
    _tablet->apply_visible_pending_rowsets();
    // A compaction holding the trailing hole rowset as an input pins it.
    auto held = _tablet->get_rowset_by_version({2, 2});
    ASSERT_NE(held, nullptr);
    add_empty_pending(_tablet.get(), 3);
    _tablet->apply_visible_pending_rowsets();
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 2}, {3, 3}}));
    held.reset();
    add_empty_pending(_tablet.get(), 4);
    _tablet->apply_visible_pending_rowsets();
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 2}, {3, 4}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, HoleRangeIsCappedByMaxVersions) {
    config::hole_rowset_max_versions = 4;
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 11).ok());
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 5}, {6, 9}, {10, 11}}));
    for (int64_t v = 12; v <= 14; ++v) {
        add_empty_pending(_tablet.get(), v);
        _tablet->apply_visible_pending_rowsets();
    }
    EXPECT_EQ(live_versions(*_tablet),
              (VersionSet {{0, 1}, {2, 5}, {6, 9}, {10, 13}, {14, 14}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, FillVersionHolesCoalescesEachRun) {
    add_rowsets(_tablet.get(),
                {create_rowset(Version(0, 1)), create_rowset(Version(5, 5)),
                 create_rowset(Version(9, 12))},
                false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 20).ok());
    EXPECT_EQ(live_versions(*_tablet),
              (VersionSet {{0, 1}, {2, 4}, {5, 5}, {6, 8}, {9, 12}, {13, 20}}));
    expect_all_versions_capturable(*_tablet);

    // A later sync with a higher partition version extends the trailing hole rowset.
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 25).ok());
    EXPECT_EQ(live_versions(*_tablet),
              (VersionSet {{0, 1}, {2, 4}, {5, 5}, {6, 8}, {9, 12}, {13, 25}}));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, DisabledKeepsOneHolePerVersion) {
    config::enable_hole_rowset_version_range = false;
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 4).ok());
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 2}, {3, 3}, {4, 4}}));
    add_empty_pending(_tablet.get(), 5);
    _tablet->apply_visible_pending_rowsets();
    EXPECT_EQ(live_versions(*_tablet),
              (VersionSet {{0, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}));
}

TEST_F(CloudHoleRowsetRangeTest, MergeOnWriteKeepsOneHolePerVersion) {
    auto mow_tablet = make_tablet(true);
    add_rowsets(mow_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(mow_tablet.get(), 4).ok());
    EXPECT_EQ(live_versions(*mow_tablet), (VersionSet {{0, 1}, {2, 2}, {3, 3}, {4, 4}}));
}

TEST_F(CloudHoleRowsetRangeTest, NotRunningTabletKeepsOneHolePerVersion) {
    _tablet->tablet_meta()->set_tablet_state(TABLET_NOTREADY);
    _tablet->set_alter_version(2);
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 5).ok());
    // Versions <= alter_version are skipped for a schema change tablet.
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {3, 3}, {4, 4}, {5, 5}}));
}

// A rowset from meta-service follows the version boundaries of the BE that produced it, so it can
// cover only part of a local hole range.
TEST_F(CloudHoleRowsetRangeTest, MetaServiceRowsetSplitsHoleRange) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 10).ok());
    ASSERT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 10}}));

    add_rowsets(_tablet.get(), {create_rowset(Version(5, 7), 0)}, true);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 4}, {5, 7}, {8, 10}}));
    EXPECT_FALSE(_tablet->get_rowset_by_version({5, 7})->is_hole_rowset());
    expect_all_versions_capturable(*_tablet);

    // Starting at the hole's first version and ending past it.
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 15).ok());
    ASSERT_EQ(live_versions(*_tablet),
              (VersionSet {{0, 1}, {2, 4}, {5, 7}, {8, 15}}));
    add_rowsets(_tablet.get(), {create_rowset(Version(2, 9), 0)}, true);
    EXPECT_EQ(live_versions(*_tablet), (VersionSet {{0, 1}, {2, 9}, {10, 15}}));
    expect_all_versions_capturable(*_tablet);

    // The version graph rebuilt from tablet meta still reaches every version.
    _tablet->_timestamped_version_tracker.construct_versioned_tracker(
            _tablet->tablet_meta()->all_rs_metas(), _tablet->tablet_meta()->all_stale_rs_metas());
    expect_all_versions_capturable(*_tablet);
}

// A live rowset whose version is also held by a stale hole rowset is dropped rather than put on
// the stale path a second time.
TEST_F(CloudHoleRowsetRangeTest, DeleteRowsetWithStaleHoleVersionIsNotStaledTwice) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 6).ok()); // live [2-6], stale [2-2]..[2-5]
    ASSERT_NE(_tablet->get_stale_rowset_by_version({2, 4}), nullptr);

    // White-box: a live rowset with the version of a stale hole rowset.
    auto live = create_rowset(Version(2, 4), 0);
    {
        std::unique_lock wlock(_tablet->get_header_lock());
        std::vector<RowsetSharedPtr> to_add {live};
        _tablet->_add_rowsets_directly(to_add, false);
    }
    auto stale_before = _tablet->_stale_rs_version_map.size();
    auto paths_before = _tablet->_timestamped_version_tracker._stale_version_path_map.size();
    {
        std::unique_lock wlock(_tablet->get_header_lock());
        _tablet->delete_rowsets({live}, wlock);
    }
    EXPECT_EQ(_tablet->_stale_rs_version_map.size(), stale_before);
    EXPECT_EQ(_tablet->_timestamped_version_tracker._stale_version_path_map.size(), paths_before);
    EXPECT_TRUE(_tablet->get_stale_rowset_by_version({2, 4})->is_hole_rowset());
    EXPECT_FALSE(_tablet->rowset_map().contains(Version(2, 4)));
    EXPECT_TRUE(_tablet->_unused_rowsets.contains(live->rowset_id()));
    expect_all_versions_capturable(*_tablet);
}

TEST_F(CloudHoleRowsetRangeTest, SplitRemovesCrossingStaleHoleRowsets) {
    add_rowsets(_tablet.get(), {create_rowset(Version(0, 1))}, false);
    ASSERT_TRUE(fill_version_holes(_tablet.get(), 10).ok()); // live [2-10], stale [2-2]..[2-9]
    add_rowsets(_tablet.get(), {create_rowset(Version(5, 7), 0)}, true);

    // No stale hole rowset may cross a live rowset boundary, or the greedy capture can stop
    // inside a live rowset.
    for (const auto& [stale_version, _] : _tablet->_stale_rs_version_map) {
        bool nested = false;
        for (const auto& [live_version, __] : _tablet->rowset_map()) {
            nested |= live_version.contains(stale_version);
        }
        EXPECT_TRUE(nested) << stale_version.to_string();
    }
    // The stale path holds exactly the stale rowsets.
    size_t path_versions = 0;
    for (const auto& [_, path] : _tablet->_timestamped_version_tracker._stale_version_path_map) {
        for (const auto& v : path->timestamped_versions()) {
            EXPECT_TRUE(_tablet->_stale_rs_version_map.contains(v->version()))
                    << v->version().to_string();
            ++path_versions;
        }
    }
    EXPECT_EQ(path_versions, _tablet->_stale_rs_version_map.size());
    EXPECT_EQ(_tablet->tablet_meta()->all_stale_rs_metas().size(),
              _tablet->_stale_rs_version_map.size());
}

} // namespace doris
