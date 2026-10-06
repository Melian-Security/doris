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

#include <iterator>
#include <limits>

namespace doris {
#include "common/compile_check_begin.h"

void CloudRowsetVersionIndex::insert(const Version& version) {
    auto [it, inserted] = _versions.insert(version);
    if (!inserted) {
        return;
    }
    _refresh_hole_before(it);
    if (auto next = std::next(it); next != _versions.end()) {
        _refresh_hole_before(next);
    }
}

void CloudRowsetVersionIndex::erase(const Version& version) {
    auto it = _versions.find(version);
    if (it == _versions.end()) {
        return;
    }
    _versions_after_hole.erase(version);
    auto next = _versions.erase(it);
    if (next != _versions.end()) {
        _refresh_hole_before(next);
    }
}

void CloudRowsetVersionIndex::clear() {
    _versions.clear();
    _versions_after_hole.clear();
}

std::optional<Version> CloudRowsetVersionIndex::last() const {
    if (_versions.empty()) {
        return std::nullopt;
    }
    return *_versions.rbegin();
}

std::vector<Version> CloudRowsetVersionIndex::versions_contained_in(const Version& range) const {
    std::vector<Version> result;
    // A version starting before `range.first` cannot be contained in `range`.
    for (auto it = _versions.lower_bound(Version(range.first, std::numeric_limits<int64_t>::min()));
         it != _versions.end() && it->first <= range.second; ++it) {
        if (range.contains(*it)) {
            result.push_back(*it);
        }
    }
    return result;
}

std::vector<Version> CloudRowsetVersionIndex::versions_overlapping(const Version& range) const {
    std::vector<Version> result;
    auto it = _versions.lower_bound(Version(range.first, std::numeric_limits<int64_t>::min()));
    // Only the version right before the first one starting in `range` can start before `range`
    // and still reach into it.
    if (it != _versions.begin() && std::prev(it)->second >= range.first) {
        it = std::prev(it);
    }
    for (; it != _versions.end() && it->first <= range.second; ++it) {
        result.push_back(*it);
    }
    return result;
}

std::vector<CloudRowsetVersionIndex::Hole> CloudRowsetVersionIndex::holes(
        int64_t max_version) const {
    std::vector<Hole> result;
    if (_versions.empty()) {
        return result;
    }
    result.reserve(_versions_after_hole.size() + 1);
    for (const auto& version : _versions_after_hole) {
        auto it = _versions.find(version);
        int64_t prev_end = it == _versions.begin() ? -1 : std::prev(it)->second;
        result.push_back({prev_end + 1, version.first - 1, version});
    }
    const auto& last_version = *_versions.rbegin();
    if (last_version.second + 1 <= max_version) {
        result.push_back({last_version.second + 1, max_version, last_version});
    }
    return result;
}

void CloudRowsetVersionIndex::_refresh_hole_before(VersionSet::const_iterator it) {
    bool has_hole = it == _versions.begin() ? it->first > 0
                                            : it->first > std::prev(it)->second + 1;
    if (has_hole) {
        _versions_after_hole.insert(*it);
    } else {
        _versions_after_hole.erase(*it);
    }
}

#include "common/compile_check_end.h"
} // namespace doris
