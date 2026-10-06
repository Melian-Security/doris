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

#include <bthread/mutex.h>

#include "load/delta_writer/delta_writer.h"
#include "runtime/workload_management/resource_context.h"

namespace doris {

class CloudStorageEngine;
class CloudRowsetBuilder;
class CloudTablet;

class CloudDeltaWriter final : public BaseDeltaWriter {
public:
    CloudDeltaWriter(CloudStorageEngine& engine, const WriteRequest& req, RuntimeProfile* profile,
                     const UniqueId& load_id);
    ~CloudDeltaWriter() override;

    Status write(const Block* block, const DorisVector<uint32_t>& row_idxs,
                 bool* memtable_flushed = nullptr) override;

    Status close() override;

    Status flush_memtable_async() override;

    Status cancel_with_status(const Status& st) override;

    Status build_rowset() override;

    Status submit_calc_delete_bitmap_task() override;

    Status wait_calc_delete_bitmap() override;

    void update_tablet_stats();

    const RowsetMetaSharedPtr& rowset_meta();

    bool is_init() const { return _is_init; }

    static Status batch_init(std::vector<CloudDeltaWriter*> writers);

    Status commit_rowset();

    Status set_txn_related_info();
    std::shared_ptr<ResourceContext> resource_context() { return _resource_ctx; }

    // Whether this writer committed the empty rowset of a tablet that received no rows without
    // building a rowset writer (see `config::skip_rowset_writer_for_empty_tablet`).
    bool committed_without_rowset_writer() const { return _empty_rowset_tablet != nullptr; }

private:
    // Convert `_rowset_builder` from `BaseRowsetBuilder` to `CloudRowsetBuilder`
    CloudRowsetBuilder* rowset_builder();

    // Handle commit for empty rowset (when no data is written)
    Status _commit_empty_rowset();

    bthread::Mutex _mtx;
    CloudStorageEngine& _engine;
    std::shared_ptr<ResourceContext> _resource_ctx;
    // Set iff the empty rowset was committed without a rowset writer; `_rowset_builder` is then
    // never initialized, and this is the tablet the load's bookkeeping applies to.
    std::shared_ptr<CloudTablet> _empty_rowset_tablet;
};

} // namespace doris
