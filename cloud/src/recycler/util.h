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

#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>

#include <string>

#include "common/defer.h"

namespace doris::cloud {

// The time unit is the same with BE: us
#define SCOPED_BVAR_LATENCY(bvar_item) \
    StopWatch sw;                      \
    DORIS_CLOUD_DEFER {                \
        bvar_item << sw.elapsed_us();  \
    };

class TxnKv;

/**
 * Get all instances, include DELETED instance
 * @return 0 for success, otherwise error
 */
int get_all_instances(TxnKv* txn_kv, std::vector<InstanceInfoPB>& res);

/**
 *
 * @return 0 for success
 */
int prepare_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 int64_t interval_ms);

void finish_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 bool success, int64_t ctime_ms);

/**
 *
 * @return 0 for success, 1 if job should be aborted, negative for other errors
 */
int lease_instance_recycle_job(TxnKv* txn_kv, std::string_view key, const std::string& instance_id,
                               const std::string& ip_port);

// Object key layout of a storage vault. Version 0 (also the layout of every legacy obj_info and of
// vaults without a path format): `data/<tablet_id>/<rowset_id>_<seg>.dat`. Version 1:
// `data/<shard>/<tablet_id>/<rowset_id>/<seg>.dat`. Every key below must stay byte-identical to the
// BE's `StorageResource` (be/src/storage/storage_policy.cpp), otherwise objects written by BE are
// never recycled.
using VaultPathFormat = StorageVaultPB::PathFormat;

// Whether keys of this layout can be computed. A vault with any other layout must not be touched:
// deleting keys of a guessed layout would miss its objects or hit unrelated ones.
inline bool is_supported_path_format(const VaultPathFormat& path_format) {
    return path_format.path_version() == 0 ||
           (path_format.path_version() == 1 && path_format.shard_num() > 0);
}

// `murmur_hash64A(tablet_id) % shard_num`, the shard directory of a tablet in a version-1 vault.
int64_t vault_shard_of_tablet(int64_t tablet_id, int64_t shard_num);

// `data/<tablet_id>` (version 0) or `data/<shard>/<tablet_id>` (version 1), without trailing '/'.
inline std::string tablet_dir(const VaultPathFormat& path_format, int64_t tablet_id) {
    DCHECK(is_supported_path_format(path_format)) << path_format.ShortDebugString();
    if (path_format.path_version() == 1) {
        return fmt::format("data/{}/{}", vault_shard_of_tablet(tablet_id, path_format.shard_num()),
                           tablet_id);
    }
    return fmt::format("data/{}", tablet_id);
}

// Version 0 joins the rowset id and the per-rowset file name with '_', version 1 with '/'.
inline std::string rowset_file_path(const VaultPathFormat& path_format, int64_t tablet_id,
                                    const std::string& rowset_id, std::string_view file_name) {
    return fmt::format("{}/{}{}{}", tablet_dir(path_format, tablet_id), rowset_id,
                       path_format.path_version() == 1 ? '/' : '_', file_name);
}

inline std::string segment_path(const VaultPathFormat& path_format, int64_t tablet_id,
                                const std::string& rowset_id, int64_t segment_id) {
    return rowset_file_path(path_format, tablet_id, rowset_id, fmt::format("{}.dat", segment_id));
}

inline std::string segment_path(int64_t tablet_id, const std::string& rowset_id,
                                int64_t segment_id) {
    return segment_path(VaultPathFormat {}, tablet_id, rowset_id, segment_id);
}

// The delete bitmap file sits directly in the tablet directory in both versions.
inline std::string delete_bitmap_path(const VaultPathFormat& path_format, int64_t tablet_id,
                                      const std::string& rowset_id) {
    return fmt::format("{}/{}_delete_bitmap.db", tablet_dir(path_format, tablet_id), rowset_id);
}

inline std::string delete_bitmap_path(int64_t tablet_id, const std::string& rowset_id) {
    return delete_bitmap_path(VaultPathFormat {}, tablet_id, rowset_id);
}

inline std::string inverted_index_path_v2(const VaultPathFormat& path_format, int64_t tablet_id,
                                          const std::string& rowset_id, int64_t segment_id) {
    return rowset_file_path(path_format, tablet_id, rowset_id, fmt::format("{}.idx", segment_id));
}

inline std::string inverted_index_path_v2(int64_t tablet_id, const std::string& rowset_id,
                                          int64_t segment_id) {
    return inverted_index_path_v2(VaultPathFormat {}, tablet_id, rowset_id, segment_id);
}

inline std::string inverted_index_path_v1(const VaultPathFormat& path_format, int64_t tablet_id,
                                          const std::string& rowset_id, int64_t segment_id,
                                          int64_t index_id, std::string_view index_path_suffix) {
    std::string suffix =
            index_path_suffix.empty() ? "" : std::string {"@"} + index_path_suffix.data();
    return rowset_file_path(path_format, tablet_id, rowset_id,
                            fmt::format("{}_{}{}.idx", segment_id, index_id, suffix));
}

inline std::string inverted_index_path_v1(int64_t tablet_id, const std::string& rowset_id,
                                          int64_t segment_id, int64_t index_id,
                                          std::string_view index_path_suffix) {
    return inverted_index_path_v1(VaultPathFormat {}, tablet_id, rowset_id, segment_id, index_id,
                                  index_path_suffix);
}

// Prefix of every object of a rowset, including its delete bitmap file. Version 1 has no '_'
// after the rowset id because both `<rowset_id>/` and `<rowset_id>_delete_bitmap.db` must match;
// rowset ids (v2) have a fixed length, so no other rowset id extends this one.
inline std::string rowset_path_prefix(const VaultPathFormat& path_format, int64_t tablet_id,
                                      const std::string& rowset_id) {
    return fmt::format("{}/{}{}", tablet_dir(path_format, tablet_id), rowset_id,
                       path_format.path_version() == 1 ? "" : "_");
}

inline std::string rowset_path_prefix(int64_t tablet_id, const std::string& rowset_id) {
    return rowset_path_prefix(VaultPathFormat {}, tablet_id, rowset_id);
}

inline std::string tablet_path_prefix(const VaultPathFormat& path_format, int64_t tablet_id) {
    return tablet_dir(path_format, tablet_id) + '/';
}

inline std::string tablet_path_prefix(int64_t tablet_id) {
    return tablet_path_prefix(VaultPathFormat {}, tablet_id);
}

// Rewrites a version-1 object path to the version-0 path of the same object
// (`data/<shard>/<tablet_id>/<rowset_id>/<file>` -> `data/<tablet_id>/<rowset_id>_<file>`,
// `data/<shard>/<tablet_id>/<file>` -> `data/<tablet_id>/<file>`), so path parsers written for
// version 0 apply to both. Any other path is returned unchanged.
std::string to_path_v0_layout(const VaultPathFormat& path_format, const std::string& path);

int get_tablet_idx(TxnKv* txn_kv, const std::string& instance_id, int64_t tablet_id,
                   TabletIndexPB& tablet_idx);

int get_tablet_meta(TxnKv* txn_kv, const std::string& instance_id, int64_t tablet_id,
                    TabletMetaCloudPB& tablet_meta);
} // namespace doris::cloud
