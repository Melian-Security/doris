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

#include "storage/rowset_builder.h"

namespace doris {

class CloudTablet;
class CloudStorageEngine;

class CloudRowsetBuilder final : public BaseRowsetBuilder {
public:
    CloudRowsetBuilder(CloudStorageEngine& engine, const WriteRequest& req,
                       RuntimeProfile* profile);

    ~CloudRowsetBuilder() override;

    Status init() override;

    void update_tablet_stats();

    const RowsetMetaSharedPtr& rowset_meta();

    Status set_txn_related_info();

    void set_skip_writing_rowset_metadata(bool skip) { _skip_writing_rowset_metadata = skip; }

    // Adds a load's rowset to the tablet's approximate stats. `rowset` is nullptr for an empty
    // rowset committed without a rowset writer, which counts as one rowset with no segments.
    static void add_rowset_to_tablet_stats(CloudTablet* tablet, const Rowset* rowset);

    // Records on this BE that the txn committed an empty rowset to `tablet` without writing its
    // metadata to the meta service, so that the delete bitmap calculation (MoW) and the make
    // visible path can tell the version apart from a missing rowset.
    static void mark_empty_rowset(CloudStorageEngine& engine, const BaseTablet& tablet,
                                  int64_t txn_id, int64_t txn_expiration);

private:
    // Convert `_tablet` from `BaseTablet` to `CloudTablet`
    CloudTablet* cloud_tablet();

    Status check_tablet_version_count();

    CloudStorageEngine& _engine;

    // whether to skip writing rowset metadata to meta service.
    // This is used for empty rowset when config::skip_writing_empty_rowset_metadata is true.
    bool _skip_writing_rowset_metadata = false;
};

} // namespace doris
