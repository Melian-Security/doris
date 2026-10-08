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

#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>
#include <sys/resource.h>

#include <set>
#include <string>

#include "common/exception.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/s3_file_system.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/segment/segment.h"
#include "storage/storage_policy.h"
#include "util/hash_util.hpp"

namespace doris {

TEST(StorageResourceTest, RemotePath) {
    S3Conf s3_conf {.bucket = "bucket",
                    .prefix = "prefix",
                    .client_conf = {
                            .endpoint = "endpoint",
                            .region = "region",
                            .ak = "ak",
                            .sk = "sk",
                            .token = "",
                            .bucket = "",
                            .role_arn = "",
                            .external_id = "",
                    }};
    auto res = io::S3FileSystem::create(std::move(s3_conf), io::FileSystem::TMP_FS_ID);
    ASSERT_TRUE(res.has_value()) << res.error();

    StorageResource storage_resource(res.value()); // path v0
    EXPECT_EQ(storage_resource.remote_tablet_path(10005), "data/10005");

    constexpr std::string_view rowset_id_str = "0200000000001cc2224124562e7dfd4834d031b13c0210be";
    EXPECT_EQ(storage_resource.remote_segment_path(10005, rowset_id_str, 5),
              "data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat");
    RowsetMeta rs_meta;
    rs_meta.set_tablet_id(10005);
    RowsetId rowset_id;
    rowset_id.init(rowset_id_str);
    rs_meta.set_rowset_id(rowset_id);
    EXPECT_EQ(storage_resource.remote_segment_path(rs_meta, 5),
              "data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat");

    EXPECT_EQ(storage_resource.cooldown_tablet_meta_path(10005, 10006, 13),
              "data/10005/10006.13.meta");

    cloud::StorageVaultPB storage_vault_pb;
    storage_resource = StorageResource(res.value(), storage_vault_pb.path_format()); // path v0
    EXPECT_EQ(storage_resource.remote_tablet_path(10005), "data/10005");
    EXPECT_EQ(storage_resource.remote_segment_path(10005, rowset_id_str, 5),
              "data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat");
    EXPECT_EQ(storage_resource.remote_segment_path(rs_meta, 5),
              "data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat");

    auto* path_format = storage_vault_pb.mutable_path_format();
    path_format->set_path_version(1);
    path_format->set_shard_num(1000);
    storage_resource = StorageResource(res.value(), storage_vault_pb.path_format()); // path v1
    EXPECT_EQ(storage_resource.remote_tablet_path(10005), "data/611/10005");
    EXPECT_EQ(storage_resource.remote_segment_path(10005, rowset_id_str, 5),
              "data/611/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be/5.dat");
    EXPECT_EQ(storage_resource.remote_segment_path(rs_meta, 5),
              "data/611/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be/5.dat");
    EXPECT_EQ(storage_resource.cooldown_tablet_meta_path(10005, 10006, 13),
              "data/611/10005/10006.13.meta");

    path_format->set_path_version(2);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_DEATH(({
                     struct rlimit core_limit {};
                     setrlimit(RLIMIT_CORE, &core_limit);
                     StorageResource(res.value(), storage_vault_pb.path_format());
                 }),
                 "unknown");
}

TEST(StorageResourceTest, ParseTabletIdFromPath) {
    // Test Version 0 format: data/{tablet_id}/{rowset_id}_{seg_id}.dat
    // see function StorageResource::remote_segment_path
    // fmt::format("{}/{}/{}_{}.dat", DATA_PREFIX, tablet_id, rowset_id, seg_id);
    EXPECT_EQ(
            StorageResource::parse_tablet_id_from_path(
                    "prefix_xxx/data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat"),
            10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("//data/12345/rowset_001_0.dat"), 12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/999999/rowset_abc_10.dat"), 999999);

    // Test Version 0 format with .idx files (v1 format)
    // see function StorageResource::remote_idx_v1_path
    // fmt::format("{}/{}/{}_{}_{}{}.idx", DATA_PREFIX, rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id, index_id, suffix);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "//data/10005/0200000000001cc2224124562e7_6_6666_suffix.idx"),
              10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "bucket_xxx/data/12345/rowsetid_1_666_suffix.idx"),
              12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/999999/rowsetid_10_8888_suffix.idx"),
              999999);

    // Test Version 0 format with .idx files (v2 format)
    // see function StorageResource::remote_idx_v2_path
    // fmt::format("{}/{}/{}_{}.idx", DATA_PREFIX, rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "s3://prefix_bucket/data/10005/0200000000001cc2224124562e7_5.idx"),
              10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("/data/12345/rowset001_0.idx"), 12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/999999/rowsetabc_10.idx"), 999999);

    // Test Version 1 format: data/{shard}/{tablet_id}/{rowset_id}/{seg_id}.dat
    // see function StorageResource::remote_segment_path
    // fmt::format("{}/{}/{}/{}/{}.dat", DATA_PREFIX, shard_fn(rowset.tablet_id()), rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "prefix_xxxx/data/611/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be/"
                      "5.dat"),
              10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/0/12345/rowset_001/0.dat"), 12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("s3:///data/999/999999/rowset_abc/10.dat"),
              999999);

    // Test Version 1 format with .idx files (v1 format)
    // see function StorageResource::remote_idx_v1_path
    // fmt::format("{}/{}/{}/{}/{}_{}{}.idx", DATA_PREFIX, shard_fn(rowset.tablet_id()), rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id, index_id, suffix);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "s3:///data/611/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be/"
                      "5_6666_suffix.idx"),
              10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "prefix_bucket/data/0/12345/rowsetid/1_666_suffix.idx"),
              12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "data/999/999999/rowsetid/10_8888_suffix.idx"),
              999999);

    // Test Version 1 format with .idx files (v2 format)
    // see function StorageResource::remote_idx_v2_path
    // fmt::format("{}/{}/{}/{}/{}.idx", DATA_PREFIX, shard_fn(rowset.tablet_id()), rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "s3://prefix_bucket/data/611/10005/"
                      "0200000000001cc2224124562e7dfd4834d031b13c0210be/5.idx"),
              10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("/data/0/12345/rowset001/0.idx"), 12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/999/999999/rowsetabc/10.idx"),
              999999);

    // Test edge cases
    // fmt::format("{}/{}/{}_{}.dat", DATA_PREFIX, tablet_id, rowset_id, seg_id);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("prefix_bucket/data/0/rowset001_0.dat"),
              0);
    // fmt::format("{}/{}/{}/{}/{}.dat", DATA_PREFIX, shard_fn(rowset.tablet_id()), rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("/data/0/0/rowset001/0.dat"), 0);

    // Test invalid cases
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(""), std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("invalid_path"), std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/"), std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("/data/abc/rowset_001_0.dat"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "s3://prefix_bucket/data/0/abc/rowset_001/0.dat"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/10005/rowset_001_0.txt"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/10005/rowset_001_0"), std::nullopt);

    // Test paths with different slash counts (should return nullopt)
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/10005/rowset_001/extra/0.dat"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("/data/10005/rowset_001/extra/0.idx"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "prefix_bucket/data/10005/rowset_001/extra/0.dat"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/10005.dat"), std::nullopt);

    // Test paths without data prefix
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("10005/rowset_001_0.dat"), std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("0/12345/rowset_001/0.dat"), std::nullopt);

    // Test paths with leading slash after data prefix
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data//10005/rowset_001_0.dat"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data//0/12345/rowset_001/0.dat"),
              std::nullopt);
}

namespace {

io::RemoteFileSystemSPtr make_test_s3_fs() {
    S3Conf s3_conf {.bucket = "bucket",
                    .prefix = "prefix",
                    .client_conf = {
                            .endpoint = "endpoint",
                            .region = "region",
                            .ak = "ak",
                            .sk = "sk",
                            .token = "",
                            .bucket = "",
                            .role_arn = "",
                            .external_id = "",
                    }};
    auto res = io::S3FileSystem::create(std::move(s3_conf), io::FileSystem::TMP_FS_ID);
    EXPECT_TRUE(res.has_value()) << res.error();
    return res.value();
}

cloud::StorageVaultPB_PathFormat path_v1(int64_t shard_num) {
    cloud::StorageVaultPB_PathFormat path_format;
    path_format.set_path_version(1);
    path_format.set_shard_num(shard_num);
    return path_format;
}

} // namespace

// The recycler recomputes these keys (cloud/src/recycler/util.h) and its RecyclerTest pins the
// same shard values; both sides must agree or objects of dropped tablets are never deleted.
TEST(StorageResourceTest, PathV1Shards) {
    auto fs = make_test_s3_fs();
    auto shard_of = [](int64_t tablet_id, int64_t shard_num) {
        return HashUtil::murmur_hash64A(&tablet_id, sizeof(tablet_id), HashUtil::MURMUR_SEED) %
               shard_num;
    };
    EXPECT_EQ(shard_of(10003, 16), 13);
    EXPECT_EQ(shard_of(10004, 16), 9);
    EXPECT_EQ(shard_of(10003, 1024), 173);
    EXPECT_EQ(shard_of(1, 1024), 194);
    EXPECT_EQ(shard_of(123456789, 1024), 933);
    EXPECT_EQ(shard_of(-1, 1024), 21);

    StorageResource resource16(fs, path_v1(16));
    EXPECT_EQ(resource16.remote_tablet_path(10003), "data/13/10003");
    EXPECT_EQ(resource16.remote_tablet_path(10004), "data/9/10004");
    StorageResource resource1024(fs, path_v1(1024));
    EXPECT_EQ(resource1024.remote_tablet_path(1), "data/194/1");
    EXPECT_EQ(resource1024.remote_tablet_path(123456789), "data/933/123456789");

    constexpr std::string_view rowset_id_str = "0200000000001cc2224124562e7dfd4834d031b13c0210be";
    RowsetMeta rs_meta;
    rs_meta.set_tablet_id(10003);
    RowsetId rowset_id;
    rowset_id.init(rowset_id_str);
    rs_meta.set_rowset_id(rowset_id);
    EXPECT_EQ(resource16.remote_segment_path(rs_meta, 2), fmt::format("data/13/10003/{}/2.dat", rowset_id_str));
    EXPECT_EQ(resource16.remote_idx_v2_path(rs_meta, 2), fmt::format("data/13/10003/{}/2.idx", rowset_id_str));
    EXPECT_EQ(resource16.remote_idx_v1_path(rs_meta, 2, 7, "sfx"),
              fmt::format("data/13/10003/{}/2_7@sfx.idx", rowset_id_str));
    EXPECT_EQ(resource16.remote_delete_bitmap_path(10003, rowset_id_str),
              fmt::format("data/13/10003/{}_delete_bitmap.db", rowset_id_str));
}

// Status::FatalError aborts in debug builds and throws in release builds.
#ifndef NDEBUG
#define EXPECT_FATAL(statement, message) EXPECT_DEATH(statement, message)
#else
#define EXPECT_FATAL(statement, message) EXPECT_THROW(statement, Exception)
#endif

TEST(StorageResourceTest, PathV1RequiresPositiveShardNum) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    auto fs = make_test_s3_fs();
    EXPECT_FATAL(StorageResource(fs, path_v1(0)), "invalid shard_num");
    EXPECT_FATAL(StorageResource(fs, path_v1(-4)), "invalid shard_num");
    cloud::StorageVaultPB_PathFormat unknown;
    unknown.set_path_version(2);
    unknown.set_shard_num(16);
    EXPECT_FATAL(StorageResource(fs, unknown), "unknown path version");
}

#undef EXPECT_FATAL

TEST(StorageResourceTest, FileCacheNameOfPathV1) {
    auto fs = make_test_s3_fs();
    StorageResource v0(fs);
    StorageResource v1(fs, path_v1(16));
    const std::string rs_a = "0200000000001cc2224124562e7dfd4834d031b13c0210be";
    const std::string rs_b = "0200000000001cc3224124562e7dfd4834d031b13c0210be";

    // Version 0: the file name, byte-identical to before.
    for (const auto& path : {v0.remote_segment_path(10003, rs_a, 2),
                             "s3://bucket/prefix/" + v0.remote_segment_path(10003, rs_a, 2),
                             "/" + v0.remote_segment_path(10003, rs_a, 2)}) {
        EXPECT_EQ(io::remote_file_cache_name(path), rs_a + "_2.dat") << path;
        EXPECT_EQ(io::remote_file_cache_name(path), io::Path(path).filename().native()) << path;
    }
    // A root prefix shaped like a version-1 directory does not change a version-0 name.
    EXPECT_EQ(io::remote_file_cache_name("data/1/2/" + v0.remote_segment_path(10003, rs_a, 2)),
              rs_a + "_2.dat");
    EXPECT_EQ(io::remote_file_cache_name("data/10003/" + rs_a + "_delete_bitmap.db"),
              rs_a + "_delete_bitmap.db");
    EXPECT_EQ(io::remote_file_cache_name("data/packed_file/3/0f2a.bin"), "0f2a.bin");
    EXPECT_EQ(io::remote_file_cache_name("0.dat"), "0.dat");

    // Version 1: every segment is named `<seg>.dat`, so the rowset id is part of the name, and the
    // name equals the version-0 file name of the same segment.
    std::set<std::string> names;
    for (int64_t tablet_id : {10003, 10004}) {
        for (const auto& rs : {rs_a, rs_b}) {
            for (int64_t seg : {0, 1}) {
                const auto path = v1.remote_segment_path(tablet_id, rs, seg);
                const auto name = io::remote_file_cache_name(path);
                EXPECT_EQ(name, fmt::format("{}_{}.dat", rs, seg)) << path;
                EXPECT_EQ(name, io::remote_file_cache_name(
                                        "s3://bucket/prefix/" + path)) << path;
                EXPECT_EQ(io::BlockFileCache::hash(name),
                          segment_v2::Segment::file_cache_key(rs, static_cast<uint32_t>(seg)));
                names.insert(name);
            }
        }
    }
    // Rowset ids are unique across tablets, so (rowset, segment) identifies a segment.
    EXPECT_EQ(names.size(), 4);

    RowsetMeta rs_meta;
    rs_meta.set_tablet_id(10003);
    RowsetId rowset_id;
    rowset_id.init(rs_a);
    rs_meta.set_rowset_id(rowset_id);
    EXPECT_EQ(io::remote_file_cache_name(v1.remote_idx_v2_path(rs_meta, 2)),
              segment_v2::InvertedIndexDescriptor::get_index_file_name_v2(rs_a, 2));
    EXPECT_EQ(io::remote_file_cache_name(v1.remote_idx_v1_path(rs_meta, 2, 7, "sfx")),
              segment_v2::InvertedIndexDescriptor::get_index_file_name_v1(rs_a, 2, 7, "sfx"));
    EXPECT_EQ(io::remote_file_cache_name(v1.remote_idx_v1_path(rs_meta, 2, 7, "sfx")),
              io::remote_file_cache_name(v0.remote_idx_v1_path(rs_meta, 2, 7, "sfx")));
    EXPECT_EQ(io::remote_file_cache_name(v1.remote_delete_bitmap_path(10003, rs_a)),
              rs_a + "_delete_bitmap.db");
}


} // namespace doris
