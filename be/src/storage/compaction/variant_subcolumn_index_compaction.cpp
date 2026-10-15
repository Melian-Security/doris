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

#include "storage/compaction/variant_subcolumn_index_compaction.h"

#include <algorithm>

#include "core/data_type/data_type_nullable.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/variant/variant_statistics.h"

namespace doris {

namespace {

bool is_string_storage_type(const DataTypePtr& type) {
    return type != nullptr && is_string_type(remove_nullable(type)->get_primitive_type());
}

const TabletIndex* find_index(const TabletSchema& schema, int64_t index_id) {
    for (const auto* index : schema.inverted_indexes()) {
        if (index->index_id() == index_id) {
            return index;
        }
    }
    return nullptr;
}

} // namespace

bool path_materialized_as_string_in_all(const std::vector<VariantSegmentPaths>& segments,
                                        const std::string& path) {
    if (segments.empty()) {
        return false;
    }
    return std::all_of(segments.begin(), segments.end(), [&](const VariantSegmentPaths& segment) {
        if (segment.sparse.contains(path)) {
            return false;
        }
        auto it = segment.materialized.find(path);
        return it != segment.materialized.end() && !it->second.empty() &&
               std::all_of(it->second.begin(), it->second.end(), is_string_storage_type);
    });
}

VariantSubcolumnIndexSelection select_variant_subcolumn_indexes(
        const std::vector<VariantSubcolumnIndexCandidate>& candidates,
        const std::vector<TabletSchemaSPtr>& input_schemas,
        const std::map<int32_t, std::vector<VariantSegmentPaths>>& input_segments,
        const std::set<int32_t>& skipped_variant_uids,
        const std::function<bool(int32_t variant_uid, const TabletIndex& index)>&
                inputs_have_index_files) {
    VariantSubcolumnIndexSelection selection;
    for (const auto& candidate : candidates) {
        if (!field_is_slice_type(candidate.type)) {
            continue;
        }
        bool path_ok = !skipped_variant_uids.contains(candidate.variant_uid);
        if (path_ok) {
            auto it = input_segments.find(candidate.variant_uid);
            path_ok = it != input_segments.end() &&
                      path_materialized_as_string_in_all(it->second, candidate.path);
        }
        for (const auto* index : candidate.indexes) {
            bool ok = path_ok && !input_schemas.empty() &&
                      std::all_of(input_schemas.begin(), input_schemas.end(),
                                  [&](const TabletSchemaSPtr& schema) {
                                      const auto* input_index =
                                              find_index(*schema, index->index_id());
                                      return input_index != nullptr &&
                                             input_index->properties() == index->properties();
                                  }) &&
                      inputs_have_index_files(candidate.variant_uid, *index);
            if (ok) {
                selection.merge.push_back({.variant_uid = candidate.variant_uid,
                                           .path = candidate.path,
                                           .index = std::make_shared<TabletIndex>(*index)});
            } else {
                ++selection.rebuild;
            }
        }
    }
    return selection;
}

Status collect_variant_segment_paths(const RowsetSharedPtr& rowset,
                                     const std::set<int32_t>& variant_uids,
                                     std::map<int32_t, std::vector<VariantSegmentPaths>>* out) {
    SegmentCacheHandle segment_cache;
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(rowset), &segment_cache));
    for (const auto& segment : segment_cache.get_segments()) {
        for (const int32_t uid : variant_uids) {
            VariantSegmentPaths paths;
            std::shared_ptr<segment_v2::ColumnReader> column_reader;
            OlapReaderStatistics stats;
            RETURN_IF_ERROR(segment->get_column_reader(uid, &column_reader, &stats));
            if (column_reader != nullptr &&
                column_reader->get_meta_type() == FieldType::OLAP_FIELD_TYPE_VARIANT) {
                auto* variant_reader =
                        assert_cast<segment_v2::VariantColumnReader*>(column_reader.get());
                RETURN_IF_ERROR(variant_reader->load_external_meta_once());
                std::unordered_map<PathInData, DataTypes, PathInData::Hash> types;
                variant_reader->get_subcolumns_types(&types);
                for (auto& [path, path_types] : types) {
                    paths.materialized[path.get_path()] = std::move(path_types);
                }
                if (const auto* source_stats = variant_reader->get_stats();
                    source_stats != nullptr) {
                    for (const auto& [path, _] : source_stats->sparse_column_non_null_size) {
                        paths.sparse.insert(path);
                    }
                }
            }
            (*out)[uid].push_back(std::move(paths));
        }
    }
    return Status::OK();
}

} // namespace doris
