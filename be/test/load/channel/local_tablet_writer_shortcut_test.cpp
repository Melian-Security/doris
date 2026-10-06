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

#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_serde.h"
#include "load/channel/load_channel_mgr.h"
#include "load/channel/tablets_channel.h"
#include "util/defer_op.h"

namespace doris {
namespace {

// A block shaped like a log load batch: a string column and a VARIANT V2 column.
Block make_log_block(size_t rows) {
    auto host_column = ColumnString::create();
    std::vector<std::string> jsons;
    jsons.reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        std::string host = "host-" + std::to_string(i % 97) + ".internal.example";
        host_column->insert_data(host.data(), host.size());
        jsons.push_back(R"({"ts":)" + std::to_string(1700000000000 + i) +
                        R"(,"src":"10.0.)" + std::to_string(i % 256) + R"(.7","msg":"request )" +
                        std::to_string(i) + R"( served","tags":["edge","waf"],"http":{"status":)" +
                        std::to_string(200 + i % 5) + R"(,"bytes":)" + std::to_string(i * 13) +
                        "}}");
    }
    std::vector<Slice> slices;
    slices.reserve(rows);
    for (const auto& json : jsons) {
        slices.emplace_back(json.data(), json.size());
    }
    auto variant_type = std::make_shared<DataTypeVariantV2>();
    auto variant_column = variant_type->create_column();
    uint64_t deserialized = 0;
    auto st = variant_type->get_serde()->deserialize_column_from_json_vector(
            *variant_column, slices, &deserialized, DataTypeSerDe::FormatOptions {});
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_EQ(deserialized, rows);

    Block block;
    block.insert({std::move(host_column), std::make_shared<DataTypeString>(), "host"});
    block.insert({std::move(variant_column), variant_type, "payload"});
    return block;
}

TEST(LocalTabletWriterShortcutTest, ResolveSendBlockUsesTheLocalBlockAsIs) {
    Block local = make_log_block(16);
    PTabletWriterAddBlockRequest request;
    Block deserialized;
    const Block* send_data = nullptr;

    auto st = BaseTabletsChannel::resolve_send_block(request, &local, &deserialized, &send_data);

    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(send_data, &local);
    EXPECT_EQ(deserialized.columns(), 0);
}

TEST(LocalTabletWriterShortcutTest, ResolveSendBlockRejectsARequestWithBothSources) {
    Block local = make_log_block(16);
    PTabletWriterAddBlockRequest request;
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compress_time = 0;
    ASSERT_TRUE(local.serialize(BeExecVersionManager::get_newest_version(),
                                request.mutable_block(), &uncompressed_bytes, &compressed_bytes,
                                &compress_time, segment_v2::CompressionTypePB::LZ4)
                        .ok());
    Block deserialized;
    const Block* send_data = nullptr;

    auto st = BaseTabletsChannel::resolve_send_block(request, &local, &deserialized, &send_data);

    EXPECT_FALSE(st.ok());
    EXPECT_EQ(send_data, nullptr);
}

// The receiver must see the same rows whether they arrive in-process or through the PBlock round
// trip that the brpc path takes.
TEST(LocalTabletWriterShortcutTest, LocalBlockMatchesTheSerializedRoundTrip) {
    constexpr size_t kRows = 4096;
    Block local = make_log_block(kRows);
    PTabletWriterAddBlockRequest remote_request;
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compress_time = 0;
    ASSERT_TRUE(local.serialize(BeExecVersionManager::get_newest_version(),
                                remote_request.mutable_block(), &uncompressed_bytes,
                                &compressed_bytes, &compress_time,
                                segment_v2::CompressionTypePB::LZ4)
                        .ok());

    Block deserialized;
    const Block* remote_data = nullptr;
    ASSERT_TRUE(BaseTabletsChannel::resolve_send_block(remote_request, nullptr, &deserialized,
                                                       &remote_data)
                        .ok());
    PTabletWriterAddBlockRequest local_request;
    Block unused;
    const Block* local_data = nullptr;
    ASSERT_TRUE(
            BaseTabletsChannel::resolve_send_block(local_request, &local, &unused, &local_data)
                    .ok());

    ASSERT_EQ(remote_data, &deserialized);
    ASSERT_EQ(local_data->rows(), kRows);
    ASSERT_EQ(local_data->columns(), remote_data->columns());
    for (size_t i = 0; i < local_data->columns(); ++i) {
        EXPECT_TRUE(local_data->get_by_position(i).type->equals(
                *remote_data->get_by_position(i).type))
                << local_data->get_by_position(i).name;
        EXPECT_EQ(local_data->get_by_position(i).name, remote_data->get_by_position(i).name);
    }
    // A deserialized Variant type renders its properties in its name, so dump both blocks under
    // the local types and compare the values only.
    Block remote_as_local;
    for (size_t i = 0; i < remote_data->columns(); ++i) {
        remote_as_local.insert({remote_data->get_by_position(i).column,
                                local_data->get_by_position(i).type,
                                remote_data->get_by_position(i).name});
    }
    EXPECT_EQ(local_data->dump_data(0, kRows), remote_as_local.dump_data(0, kRows));
}

PTabletWriterAddBlockRequest make_add_block_request() {
    PTabletWriterAddBlockRequest request;
    request.mutable_id()->set_hi(20261006);
    request.mutable_id()->set_lo(4);
    request.set_index_id(10);
    request.set_sender_id(0);
    request.set_packet_seq(0);
    request.set_eos(false);
    for (int i = 0; i < 16; ++i) {
        request.add_tablet_ids(1000);
        request.add_partition_ids(100);
    }
    return request;
}

TEST(LocalTabletWriterShortcutTest, AddBatchLocalReportsAnUnknownLoadLikeTheRpc) {
    LoadChannelMgr mgr;
    ASSERT_TRUE(mgr.init(-1).ok());
    Defer stop {[&]() { mgr.stop(); }};
    auto request = make_add_block_request();
    Block block = make_log_block(16);
    PTabletWriterAddBlockResult response;

    mgr.add_batch_local(request, block, &response);

    auto st = Status::create(response.status());
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("unknown load_id"), std::string::npos) << st;
    EXPECT_TRUE(response.has_execution_time_us());
    EXPECT_EQ(response.wait_execution_time_us(), 0);
}

TEST(LocalTabletWriterShortcutTest, AddBatchLocalReportsACancelledLoadWithItsReason) {
    LoadChannelMgr mgr;
    ASSERT_TRUE(mgr.init(-1).ok());
    Defer stop {[&]() { mgr.stop(); }};
    auto request = make_add_block_request();
    PTabletWriterCancelRequest cancel;
    *cancel.mutable_id() = request.id();
    cancel.set_index_id(request.index_id());
    cancel.set_sender_id(request.sender_id());
    cancel.set_cancel_reason("cancelled by the test");
    ASSERT_TRUE(mgr.cancel(cancel).ok());
    Block block = make_log_block(16);
    PTabletWriterAddBlockResult response;

    mgr.add_batch_local(request, block, &response);

    auto st = Status::create(response.status());
    EXPECT_TRUE(st.is<ErrorCode::CANCELLED>()) << st;
    EXPECT_NE(st.to_string().find("cancelled by the test"), std::string::npos) << st;
}

} // namespace
} // namespace doris
