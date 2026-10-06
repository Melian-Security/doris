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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <set>
#include <vector>

#include "storage/olap_common.h"

namespace doris {

// Ordered index over the versions of a cloud tablet's visible rowset metas.
//
// The load visible path, the sync path and version-hole filling all run under
// the tablet's exclusive meta lock and need three answers from the visible
// version set: the version with the greatest start, the versions contained in
// a range, and the version holes up to a target version. Collecting and
// sorting every rowset version to get them costs O(N log N) per call, which
// dominates the lock hold time once a tablet carries tens of thousands of
// rowsets. This index keeps the versions ordered and the hole positions current
// on every insert and erase, so each query costs O(log N) plus the size of its
// answer.
//
// A hole is defined exactly as the sorted walk it replaces defines it: with the
// versions ordered by start version, a hole sits right before version `v` when
// `v.first` is greater than the previous version's end version + 1, or, for the
// first version, when `v.first` is greater than 0. A trailing hole runs from the
// last version's end version + 1 to the requested max version.
//
// Not thread-safe. The owning tablet guards it with its meta lock.
class CloudRowsetVersionIndex {
public:
    struct Hole {
        // First and last (inclusive) missing version.
        int64_t first;
        int64_t last;
        // The version right after an inner hole, or the last version for the
        // trailing hole. Hole filling copies schema and storage settings from
        // the rowset at this version.
        Version anchor;
    };

    void insert(const Version& version);
    void erase(const Version& version);
    void clear();

    template <typename VersionRange>
    void rebuild(const VersionRange& versions) {
        clear();
        for (const auto& version : versions) {
            insert(version);
        }
    }

    bool empty() const { return _versions.empty(); }
    size_t size() const { return _versions.size(); }
    bool contains(const Version& version) const { return _versions.contains(version); }
    // Number of inner holes, i.e. holes before the last version.
    size_t num_inner_holes() const { return _versions_after_hole.size(); }

    // The version with the greatest start version (greatest end version on a tie).
    std::optional<Version> last() const;

    // Calls `fn(version)` for every indexed version whose start version is below
    // `version`, in ascending order.
    template <typename Fn>
    void for_each_starting_before(int64_t version, Fn&& fn) const {
        for (auto it = _versions.begin(); it != _versions.end() && it->first < version; ++it) {
            fn(*it);
        }
    }

    // All indexed versions `v` with `range.contains(v)`, in ascending order.
    std::vector<Version> versions_contained_in(const Version& range) const;

    // All indexed versions sharing at least one version with `range`, in ascending order.
    // Assumes indexed versions do not overlap each other, as visible rowset versions do not.
    std::vector<Version> versions_overlapping(const Version& range) const;

    // Inner holes in ascending order, then the trailing hole up to `max_version`
    // if there is one. Empty when the index is empty.
    std::vector<Hole> holes(int64_t max_version) const;

private:
    struct VersionLess {
        bool operator()(const Version& lhs, const Version& rhs) const {
            return lhs.first != rhs.first ? lhs.first < rhs.first : lhs.second < rhs.second;
        }
    };
    using VersionSet = std::set<Version, VersionLess>;

    // Recomputes whether a hole sits right before `it`, which must point into `_versions`.
    void _refresh_hole_before(VersionSet::const_iterator it);

    VersionSet _versions;
    // Versions in `_versions` with a hole right before them.
    VersionSet _versions_after_hole;
};

} // namespace doris
