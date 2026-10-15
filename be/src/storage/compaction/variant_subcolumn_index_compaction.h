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

#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

// The sub-columns one segment stores for one VARIANT column.
struct VariantSegmentPaths {
    // relative path -> storage types of its materialized (typed or extracted) sub-column
    std::unordered_map<std::string, std::vector<DataTypePtr>> materialized;
    // relative paths kept in the sparse column
    std::unordered_set<std::string> sparse;
};

// A string sub-column of the compaction output schema and the inverted indexes the compaction
// writer would build for it.
struct VariantSubcolumnIndexCandidate {
    int32_t variant_uid = -1;
    std::string path; // relative to the VARIANT column
    FieldType type = FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    std::vector<const TabletIndex*> indexes;
};

struct VariantSubcolumnIndexSelection {
    std::vector<VariantSubcolumnIndexToCompact> merge;
    // candidate indexes left to the writer to rebuild
    size_t rebuild = 0;
};

// True when every segment stores `path` as a materialized string sub-column and none keeps it in
// a sparse column. An empty segment list is not enough to prove it.
bool path_materialized_as_string_in_all(const std::vector<VariantSegmentPaths>& segments,
                                        const std::string& path);

// Picks the sub-column indexes compaction can merge from the input segments: the path is a
// materialized string sub-column in every input segment, every input rowset has the index with
// the same properties, and `inputs_have_index_files` finds its file in every input segment.
// Anything else is rebuilt by the writer.
VariantSubcolumnIndexSelection select_variant_subcolumn_indexes(
        const std::vector<VariantSubcolumnIndexCandidate>& candidates,
        const std::vector<TabletSchemaSPtr>& input_schemas,
        const std::map<int32_t, std::vector<VariantSegmentPaths>>& input_segments,
        const std::set<int32_t>& skipped_variant_uids,
        const std::function<bool(int32_t variant_uid, const TabletIndex& index)>&
                inputs_have_index_files);

// Appends, for every segment of `rowset`, what it stores for each VARIANT column in
// `variant_uids`; a segment without a reader for the column contributes an empty entry.
Status collect_variant_segment_paths(const RowsetSharedPtr& rowset,
                                     const std::set<int32_t>& variant_uids,
                                     std::map<int32_t, std::vector<VariantSegmentPaths>>* out);

} // namespace doris
