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

#include "io/fs/s3_file_writer.h"

#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <any>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>

#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/packed_file_system.h"
#include "io/fs/packed_file_writer.h"
#include "io/fs/s3_file_bufferpool.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_obj_storage_client.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "storage/index/index_file_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "util/defer_op.h"
#include "util/slice.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/uuid_generator.h"

using namespace doris::io;

namespace doris {

static std::shared_ptr<io::S3FileSystem> s3_fs {nullptr};

// This MockS3Client is only responsible for handling normal situations,
// while error injection is left to other macros to resolve
class MockS3Client {
public:
    MockS3Client() = default;
    ~MockS3Client() = default;

    Aws::S3::Model::CreateMultipartUploadOutcome create_multi_part_upload(
            const Aws::S3::Model::CreateMultipartUploadRequest request) {
        auto uuid = UUIDGenerator::instance()->next_uuid();
        std::stringstream ss;
        ss << uuid;
        upload_id = ss.str();
        bucket = request.GetBucket();
        key = request.GetKey();
        auto result = Aws::S3::Model::CreateMultipartUploadResult();
        result.SetUploadId(upload_id);
        auto outcome = Aws::S3::Model::CreateMultipartUploadOutcome(std::move(result));
        return outcome;
    }

    Aws::S3::Model::AbortMultipartUploadOutcome abort_multi_part_upload(
            const Aws::S3::Model::AbortMultipartUploadRequest& request) {
        if (request.GetKey() != key || request.GetBucket() != bucket ||
            upload_id != request.GetUploadId()) {
            return Aws::S3::Model::AbortMultipartUploadOutcome(
                    Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::NO_SUCH_UPLOAD,
                                                             false));
        }
        uploaded_parts.clear();
        return Aws::S3::Model::AbortMultipartUploadOutcome(
                Aws::S3::Model::AbortMultipartUploadResult());
    }

    Aws::S3::Model::UploadPartOutcome upload_part(const Aws::S3::Model::UploadPartRequest& request,
                                                  std::string_view buf) {
        if (request.GetKey() != key || request.GetBucket() != bucket ||
            upload_id != request.GetUploadId()) {
            return Aws::S3::Model::UploadPartOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(
                    Aws::S3::S3Errors::NO_SUCH_UPLOAD, false));
        }
        if (request.ContentMD5HasBeenSet()) {
            const auto& origin_md5 = request.GetContentMD5();
            auto content = request.GetBody();
            Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*content));
            const auto& md5 = Aws::Utils::HashingUtils::Base64Encode(part_md5);
            if (origin_md5 != md5) {
                return Aws::S3::Model::UploadPartOutcome(Aws::Client::AWSError<Aws::S3::S3Errors>(
                        Aws::S3::S3Errors::INVALID_OBJECT_STATE, "wrong md5", "md5 not match",
                        false));
            }
        }
        {
            Slice slice {buf.data(), buf.size()};
            std::string str;
            str.resize(slice.get_size());
            std::memcpy(str.data(), slice.get_data(), slice.get_size());
            std::unique_lock lck {latch};
            uploaded_parts.insert({request.GetPartNumber(), std::move(str)});
            file_size += request.GetContentLength();
        }
        LOG_INFO("upload part size is {}", request.GetContentLength());
        return Aws::S3::Model::UploadPartOutcome(Aws::S3::Model::UploadPartResult());
    }

    Aws::S3::Model::CompleteMultipartUploadOutcome complete_multi_part_upload(
            const Aws::S3::Model::CompleteMultipartUploadRequest& request) {
        if (request.GetKey() != key || request.GetBucket() != bucket ||
            upload_id != request.GetUploadId()) {
            return Aws::S3::Model::CompleteMultipartUploadOutcome(
                    Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::NO_SUCH_UPLOAD,
                                                             false));
        }
        const auto& multi_part_upload = request.GetMultipartUpload();
        if (multi_part_upload.GetParts().size() != uploaded_parts.size()) {
            return Aws::S3::Model::CompleteMultipartUploadOutcome(
                    Aws::Client::AWSError<Aws::S3::S3Errors>(
                            Aws::S3::S3Errors::INVALID_OBJECT_STATE, "part num not match",
                            "part num not match", false));
        }
        for (size_t i = 0; i < multi_part_upload.GetParts().size(); i++) {
            if (i + 1 != multi_part_upload.GetParts().at(i).GetPartNumber()) {
                return Aws::S3::Model::CompleteMultipartUploadOutcome(
                        Aws::Client::AWSError<Aws::S3::S3Errors>(
                                Aws::S3::S3Errors::INVALID_OBJECT_STATE, "part num not coutinous",
                                "part num not coutinous", false));
            }
        }
        exists = true;
        return Aws::S3::Model::CompleteMultipartUploadOutcome(
                Aws::S3::Model::CompleteMultipartUploadResult());
    }

    Aws::S3::Model::PutObjectOutcome put_object(const Aws::S3::Model::PutObjectRequest& request,
                                                std::string_view& buf) {
        exists = true;
        file_size = request.GetContentLength();
        key = request.GetKey();
        bucket = request.GetBucket();
        Slice s {buf.data(), buf.size()};
        std::string str;
        str.resize(s.get_size());
        std::memcpy(str.data(), s.get_data(), s.get_size());
        uploaded_parts.insert({1, std::move(str)});
        return Aws::S3::Model::PutObjectOutcome(Aws::S3::Model::PutObjectResult());
    }

    Aws::S3::Model::HeadObjectOutcome head_object(
            const Aws::S3::Model::HeadObjectRequest& request) {
        if (request.GetKey() != key || request.GetBucket() != bucket || !exists) {
            auto error = Aws::Client::AWSError<Aws::S3::S3Errors>(
                    Aws::S3::S3Errors::RESOURCE_NOT_FOUND, false);
            error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
            return Aws::S3::Model::HeadObjectOutcome(error);
        }
        auto result = Aws::S3::Model::HeadObjectResult();
        result.SetContentLength(file_size);
        return Aws::S3::Model::HeadObjectOutcome(result);
    }

    [[nodiscard]] const std::map<int64_t, std::string>& contents() const { return uploaded_parts; }

private:
    std::mutex latch;
    std::string upload_id;
    size_t file_size {0};
    std::map<int64_t, std::string> uploaded_parts;
    std::string key;
    std::string bucket;
    bool exists {false};
};

static std::shared_ptr<MockS3Client> mock_client = nullptr;

struct MockCallback {
    std::string point_name;
    std::function<void(std::vector<std::any>&&)> callback;
};

static auto test_mock_callbacks = std::array {
        MockCallback {"s3_file_writer::create_multi_part_upload",
                      [](auto&& outcome) {
                          const auto& req =
                                  try_any_cast<const Aws::S3::Model::CreateMultipartUploadRequest&>(
                                          outcome.at(0));
                          auto pair =
                                  try_any_cast_ret<Aws::S3::Model::CreateMultipartUploadOutcome>(
                                          outcome);
                          pair->second = true;
                          pair->first = mock_client->create_multi_part_upload(req);
                      }},
        MockCallback {"s3_file_writer::abort_multi_part",
                      [](auto&& outcome) {
                          const auto& req =
                                  try_any_cast<const Aws::S3::Model::AbortMultipartUploadRequest&>(
                                          outcome.at(0));
                          auto pair = try_any_cast_ret<Aws::S3::Model::AbortMultipartUploadOutcome>(
                                  outcome);
                          pair->second = true;
                          pair->first = mock_client->abort_multi_part_upload(req);
                      }},
        MockCallback {"s3_file_writer::upload_part",
                      [](auto&& outcome) {
                          const auto& req = try_any_cast<const Aws::S3::Model::UploadPartRequest&>(
                                  outcome.at(0));
                          const auto& buf = try_any_cast<std::string_view*>(outcome.at(1));
                          auto pair = try_any_cast_ret<Aws::S3::Model::UploadPartOutcome>(outcome);
                          pair->second = true;
                          pair->first = mock_client->upload_part(req, *buf);
                      }},
        MockCallback {
                "s3_file_writer::complete_multi_part",
                [](auto&& outcome) {
                    const auto& req =
                            try_any_cast<const Aws::S3::Model::CompleteMultipartUploadRequest&>(
                                    outcome.at(0));
                    auto pair = try_any_cast_ret<Aws::S3::Model::CompleteMultipartUploadOutcome>(
                            outcome);
                    pair->second = true;
                    pair->first = mock_client->complete_multi_part_upload(req);
                }},
        MockCallback {"s3_file_writer::put_object",
                      [](auto&& outcome) {
                          const auto& req = try_any_cast<const Aws::S3::Model::PutObjectRequest&>(
                                  outcome.at(0));
                          const auto& buf = try_any_cast<std::string_view*>(outcome.at(1));
                          auto pair = try_any_cast_ret<Aws::S3::Model::PutObjectOutcome>(outcome);
                          pair->second = true;
                          pair->first = mock_client->put_object(req, *buf);
                      }},
        MockCallback {"s3_file_system::head_object",
                      [](auto&& outcome) {
                          const auto& req = try_any_cast<const Aws::S3::Model::HeadObjectRequest&>(
                                  outcome.at(0));
                          auto pair = try_any_cast_ret<Aws::S3::Model::HeadObjectOutcome>(outcome);
                          pair->second = true;
                          pair->first = mock_client->head_object(req);
                      }},
        MockCallback {"s3_client_factory::create", [](auto&& outcome) {
                          auto pair = try_any_cast_ret<std::shared_ptr<io::S3ObjStorageClient>>(
                                  outcome);
                          pair->second = true;
                      }}};

class S3FileWriterTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        auto sp = SyncPoint::get_instance();
        sp->enable_processing();
        config::file_cache_enter_disk_resource_limit_mode_percent = 99;
        std::for_each(test_mock_callbacks.begin(), test_mock_callbacks.end(),
                      [sp](const MockCallback& mockcallback) {
                          sp->set_call_back(mockcallback.point_name, mockcallback.callback);
                      });
        std::string cur_path = std::filesystem::current_path();
        S3Conf s3_conf;
        s3_conf.client_conf.ak = "fake_ak";
        s3_conf.client_conf.sk = "fake_sk";
        s3_conf.client_conf.endpoint = "fake_s3_endpoint";
        s3_conf.client_conf.region = "fake_s3_region";
        s3_conf.bucket = "fake_s3_bucket";
        s3_conf.prefix = "s3_file_writer_test";
        LOG_INFO("s3 conf is {}", s3_conf.to_string());
        auto res = io::S3FileSystem::create(std::move(s3_conf), io::FileSystem::TMP_FS_ID);
        ASSERT_TRUE(res.has_value()) << res.error();
        s3_fs = res.value();

        std::unique_ptr<ThreadPool> _pool;
        std::ignore = ThreadPoolBuilder("s3_upload_file_thread_pool")
                              .set_min_threads(5)
                              .set_max_threads(10)
                              .build(&_pool);
        ExecEnv::GetInstance()->_s3_file_upload_thread_pool = std::move(_pool);
    }

    static void TearDownTestSuite() {
        auto sp = SyncPoint::get_instance();
        std::for_each(test_mock_callbacks.begin(), test_mock_callbacks.end(),
                      [sp](const MockCallback& mockcallback) {
                          sp->clear_call_back(mockcallback.point_name);
                      });
        sp->disable_processing();
        ExecEnv::GetInstance()->_s3_file_upload_thread_pool.reset();
    }
};

TEST_F(S3FileWriterTest, DisableFileCacheWriteFromS3FileWriter) {
    bool upload_called = false;
    bool cache_allocator_called = false;
    bool completion_called = false;
    bool original_enable_file_cache = config::enable_file_cache;
    bool original_enable_file_cache_write = config::enable_file_cache_write_from_s3_file_writer;
    Defer restore_config {[&]() {
        config::enable_file_cache = original_enable_file_cache;
        config::enable_file_cache_write_from_s3_file_writer = original_enable_file_cache_write;
    }};
    config::enable_file_cache = true;
    config::enable_file_cache_write_from_s3_file_writer = false;

    OperationState state(
            [&completion_called](Status status) {
                EXPECT_TRUE(status.ok()) << status;
                completion_called = true;
                return false;
            },
            [] { return false; });
    UploadFileBuffer buffer([&upload_called](UploadFileBuffer&) { upload_called = true; },
                            std::move(state), 0,
                            [&cache_allocator_called]() -> FileBlocksHolderPtr {
                                cache_allocator_called = true;
                                return nullptr;
                            });

    std::string data = "test data";
    ASSERT_TRUE(buffer.append_data(Slice(data)).ok());
    buffer.on_upload();

    EXPECT_TRUE(upload_called);
    EXPECT_TRUE(completion_called);
    EXPECT_FALSE(cache_allocator_called);
}

TEST_F(S3FileWriterTest, multi_part_io_error) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    auto sp = SyncPoint::get_instance();
    int largerThan5MB = 0;
    sp->set_call_back("S3FileWriter::_upload_one_part", [&largerThan5MB](auto&& outcome) {
        // Deliberately make one upload one part task fail to test if s3 file writer could
        // handle io error
        if (largerThan5MB > 0) {
            LOG(INFO) << "set upload one part to error";
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            auto ptr = try_any_cast<
                    Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error>*>(
                    outcome.back());
            *ptr = Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error>(
                    Aws::Client::AWSError<Aws::S3::S3Errors>());
        }
        largerThan5MB++;
    });
    Defer defer {[&]() { sp->clear_call_back("S3FileWriter::_upload_one_part"); }};
    auto client = s3_fs->client_holder();
    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/storage/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    doris::Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    // The second part would fail uploading itself to s3
    // so the result of close should be not ok
    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
    bool exists = false;
    st = s3_fs->exists("multi_part_io_error", &exists);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(exists);
}

TEST_F(S3FileWriterTest, offset_test) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();
    std::map<int, std::shared_ptr<io::FileBuffer>> bufs;

    auto sp = SyncPoint::get_instance();
    // The buffer wouldn't be submitted to the threadpool after it reaches 5MB, it would immediately
    // return when it finishes the appending data logic
    sp->set_call_back("s3_file_writer::appenv_1", [&bufs](auto&& outcome) {
        std::shared_ptr<io::FileBuffer> buf =
                *try_any_cast<std::shared_ptr<io::FileBuffer>*>(outcome.at(0));
        int part_num = try_any_cast<int>(outcome.at(1));
        bufs.emplace(part_num, buf);
    });
    sp->set_call_back("UploadFileBuffer::append_data", [](auto&& outcome) {
        auto pair = try_any_cast_ret<Status>(outcome);
        io::UploadFileBuffer& buf = *try_any_cast<io::UploadFileBuffer*>(outcome.at(0));
        auto size = try_any_cast<size_t>(outcome.at(1));
        buf._size += size;
        pair->second = true;
    });
    sp->set_call_back("UploadFileBuffer::submit", [](auto&& outcome) {
        auto buf = try_any_cast<io::FileBuffer*>(outcome.at(0));
        auto* upload_buf = dynamic_cast<io::UploadFileBuffer*>(buf);
        upload_buf->set_status(Status::OK());
        auto pair = try_any_cast_ret<Status>(outcome);
        pair->second = true;
    });
    Defer defer {[&]() {
        sp->clear_call_back("s3_file_writer::appenv_1");
        sp->clear_call_back("UploadFileBuffer::append_data");
        sp->clear_call_back("UploadFileBuffer::submit");
    }};

    {
        constexpr int buf_size = 8192; // 8 * 1024
        char buf[buf_size];
        doris::Slice slice(buf, buf_size);
        bufs.clear();
        io::FileWriterPtr s3_file_writer;
        auto st = s3_fs->create_file("file1", &s3_file_writer, &state);
        ASSERT_TRUE(st.ok()) << st;
        size_t offset = 0;
        constexpr size_t slice_num = 10;
        std::array<Slice, slice_num> slices;
        slices.fill(slice);
        int cur_part_num = dynamic_cast<io::S3FileWriter*>(s3_file_writer.get())->_cur_part_num;
        for (auto s : slices) {
            st = s3_file_writer->append(s);
            ASSERT_TRUE(st.ok()) << st;
            cur_part_num = dynamic_cast<io::S3FileWriter*>(s3_file_writer.get())->_cur_part_num;
            const auto& buffer = bufs.at(cur_part_num);
            offset += s.get_size();
            ASSERT_EQ(buffer->get_file_offset(), 0);
            ASSERT_EQ(s3_file_writer->bytes_appended(), offset);
        }
    }

    {
        constexpr int buf_size = 8888;
        char buf[buf_size];
        doris::Slice slice(buf, buf_size);
        bufs.clear();
        io::FileWriterPtr s3_file_writer;
        auto st = s3_fs->create_file("file2", &s3_file_writer, &state);
        ASSERT_TRUE(st.ok()) << st;
        size_t offset = 0;
        constexpr size_t slice_num = 1024;
        std::array<Slice, slice_num> slices;
        slices.fill(slice);
        int cur_part_num = dynamic_cast<io::S3FileWriter*>(s3_file_writer.get())->_cur_part_num;
        for (auto s : slices) {
            st = s3_file_writer->append(s);
            ASSERT_TRUE(st.ok()) << st;
            auto ptr = dynamic_cast<io::S3FileWriter*>(s3_file_writer.get());
            cur_part_num = ptr->_cur_part_num;
            const auto& buffer = bufs.at(cur_part_num);
            offset += s.get_size();
            ASSERT_EQ(buffer->get_file_offset(), (cur_part_num - 1) * config::s3_write_buffer_size);
            ASSERT_EQ(s3_file_writer->bytes_appended(), offset);
        }
        st = s3_file_writer->close();
    }
}

TEST_F(S3FileWriterTest, put_object_io_error) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("S3FileWriter::_put_object", [](auto&& outcome) {
        // Deliberately make put object task fail to test if s3 file writer could
        // handle io error
        LOG(INFO) << "set put object to error";
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        io::S3FileWriter* writer = try_any_cast<io::S3FileWriter*>(outcome.at(0));
        auto* buf = try_any_cast<io::UploadFileBuffer*>(outcome.at(1));
        writer->_st = Status::IOError(
                "failed to put object (bucket={}, key={}, upload_id={}, exception=inject "
                "error): "
                "inject error",
                writer->_obj_storage_path_opts.bucket, writer->_obj_storage_path_opts.path.native(),
                writer->upload_id());
        buf->set_status(writer->_st);
        bool* pred = try_any_cast<bool*>(outcome.back());
        *pred = true;
    });
    Defer defer {[&]() { sp->clear_call_back("S3FileWriter::_put_object"); }};
    auto client = s3_fs->client_holder();
    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/storage/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("put_object_io_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    // Only upload 4MB to trigger put object operation
    auto file_size = 4 * 1024 * 1024;
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    // The object might be timeout but still succeed in loading
    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
}

TEST_F(S3FileWriterTest, appendv_random_quit) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    io::FileReaderSPtr local_file_reader;

    ASSERT_EQ(Status::OK(), fs->open_file("./be/test/storage/test_data/all_types_100000.txt",
                                          &local_file_reader));

    constexpr int buf_size = 8192;
    size_t quit_time = rand() % local_file_reader->size();
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("s3_file_writer::appenv", [&quit_time](auto&& st) {
        if (quit_time == 0) {
            auto pair = try_any_cast_ret<Status>(st);
            pair->second = true;
            pair->first = Status::InternalError("error");
            return;
        }
        quit_time--;
    });
    Defer defer {[&]() { sp->clear_call_back("s3_file_writer::appenv"); }};

    io::FileWriterPtr s3_file_writer;
    auto st = s3_fs->create_file("appendv_random_quit", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        auto st = s3_file_writer->append(Slice(buf, bytes_read));
        if (quit_time == 0) {
            ASSERT_FALSE(st.ok()) << st;
        } else {
            ASSERT_TRUE(st.ok()) << st;
        }
        offset += bytes_read;
    }
    bool exists = false;
    st = s3_fs->exists("appendv_random_quit", &exists);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(exists);
}

TEST_F(S3FileWriterTest, multi_part_open_error) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/storage/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 5 * 1024 * 1024;
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("s3_file_writer::_open", [](auto&& outcome) {
        auto open_outcome =
                try_any_cast<Aws::S3::Model::CreateMultipartUploadOutcome*>(outcome.back());
        *open_outcome =
                Aws::Utils::Outcome<Aws::S3::Model::CreateMultipartUploadResult, Aws::S3::S3Error>(
                        Aws::Client::AWSError<Aws::S3::S3Errors>());
    });
    Defer defer {[&]() { sp->clear_call_back("s3_file_writer::_open"); }};

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("multi_part_open_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    auto buf = std::make_unique<char[]>(buf_size);
    Slice slice(buf.get(), buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    st = local_file_reader->read_at(offset, slice, &bytes_read);
    ASSERT_TRUE(st.ok()) << st;
    // Directly write 5MB would cause one create multi part upload request
    // and it would be rejectd one error
    st = s3_file_writer->append(Slice(buf.get(), bytes_read));
    ASSERT_FALSE(st.ok()) << st;
    bool exists = false;
    st = s3_fs->exists("multi_part_open_error", &exists);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(exists);
}

// TEST_F(S3FileWriterTest, write_into_cache_io_error) {
//     mock_client = std::make_shared<MockS3Client>();
//     std::filesystem::path caches_dir =
//             std::filesystem::current_path() / "s3_file_writer_cache_test";
//     std::string cache_base_path = caches_dir / "cache1" / "";
//     Defer fs_clear {[&]() {
//         if (std::filesystem::exists(cache_base_path)) {
//             std::error_code ec;
//             std::filesystem::remove_all(cache_base_path, ec);
//         }
//     }};
//     io::FileCacheSettings settings;
//     settings.query_queue_size = 10 * 1024 * 1024;
//     settings.query_queue_elements = 100;
//     settings.total_size = 10 * 1024 * 1024;
//     settings.max_file_block_size = 1 * 1024 * 1024;
//     settings.max_query_cache_size = 30;
//     io::FileCacheFactory::instance()._caches.clear();
//     io::FileCacheFactory::instance()._path_to_cache.clear();
//     io::FileCacheFactory::instance()._total_cache_size = 0;
//     auto cache = std::make_unique<io::BlockFileCache>(cache_base_path, settings);
//     ASSERT_TRUE(cache->initialize());
//     while (true) {
//         if (cache->get_async_open_success()) {
//             break;
//         };
//         std::this_thread::sleep_for(std::chrono::milliseconds(1));
//     }
//     io::FileCacheFactory::instance()._caches.emplace_back(std::move(cache));
//     doris::io::FileWriterOptions state;
//     auto fs = io::global_local_filesystem();

//     io::FileReaderSPtr local_file_reader;

//     auto st = fs->open_file("./be/test/storage/test_data/all_types_100000.txt", &local_file_reader);
//     ASSERT_TRUE(st.ok()) << st;

//     constexpr int buf_size = 8192;
//     auto sp = SyncPoint::get_instance();
//     config::enable_file_cache = true;
//     // Make append to cache return one error to test if it would exit
//     sp->set_call_back("file_block::append", [](auto&& values) {
//         LOG(INFO) << "file segment append";
//         auto pairs = try_any_cast_ret<Status>(values);
//         pairs->second = true;
//         pairs->first = Status::IOError("failed to append to cache file segments");
//     });
//     sp->set_call_back("S3FileWriter::_complete:3", [](auto&& values) {
//         LOG(INFO) << "don't send s3 complete request";
//         auto pairs = try_any_cast_ret<Status>(values);
//         pairs->second = true;
//     });
//     sp->set_call_back("UploadFileBuffer::upload_to_local_file_cache", [](auto&& values) {
//         LOG(INFO) << "Check if upload failed due to injected error";
//         bool ret = *try_any_cast<bool*>(values.back());
//         ASSERT_FALSE(ret);
//     });
//     Defer defer {[&]() {
//         sp->clear_call_back("file_block::append");
//         sp->clear_call_back("S3FileWriter::_complete:3");
//         sp->clear_call_back("UploadFileBuffer::upload_to_local_file_cache");
//         config::enable_file_cache = false;
//     }};

//     io::FileWriterPtr s3_file_writer;
//     st = s3_fs->create_file("write_into_cache_io_error", &s3_file_writer, &state);
//     ASSERT_TRUE(st.ok()) << st;

//     char buf[buf_size];
//     Slice slice(buf, buf_size);
//     size_t offset = 0;
//     size_t bytes_read = 0;
//     auto file_size = local_file_reader->size();
//     LOG(INFO) << "file size is " << file_size;
//     while (offset < file_size) {
//         st = local_file_reader->read_at(offset, slice, &bytes_read);
//         ASSERT_TRUE(st.ok()) << st;
//         st = s3_file_writer->append(Slice(buf, bytes_read));
//         ASSERT_TRUE(st.ok()) << st;
//         offset += bytes_read;
//     }
//     st = s3_file_writer->finalize();
//     ASSERT_TRUE(st.ok()) << st;
//     st = s3_file_writer->close();
//     ASSERT_TRUE(st.ok()) << st;
// }

// TEST_F(S3FileWriterTest, DISABLED_read_from_cache_io_error) {
//     std::filesystem::path caches_dir =
//             std::filesystem::current_path() / "s3_file_writer_cache_test";
//     std::string cache_base_path = caches_dir / "cache2" / "";
//     Defer fs_clear {[&]() {
//         if (std::filesystem::exists(cache_base_path)) {
//             std::filesystem::remove_all(cache_base_path);
//         }
//     }};
//     io::FileCacheSettings settings;
//     settings.query_queue_size = 10 * 1024 * 1024;
//     settings.query_queue_elements = 100;
//     settings.total_size = 10 * 1024 * 1024;
//     settings.max_file_block_size = 1 * 1024 * 1024;
//     settings.max_query_cache_size = 30;
//     io::FileCacheFactory::instance()._caches.clear();
//     io::FileCacheFactory::instance()._path_to_cache.clear();
//     io::FileCacheFactory::instance()._total_cache_size = 0;
//     auto cache = std::make_unique<io::BlockFileCache>(cache_base_path, settings);
//     ASSERT_TRUE(cache->initialize());
//     while (true) {
//         if (cache->get_async_open_success()) {
//             break;
//         };
//         std::this_thread::sleep_for(std::chrono::milliseconds(1));
//     }
//     io::FileCacheFactory::instance()._caches.emplace_back(std::move(cache));
//     doris::io::FileWriterOptions state;
//     auto fs = io::global_local_filesystem();

//     io::FileReaderSPtr local_file_reader;

//     auto st = fs->open_file("./be/test/storage/test_data/all_types_100000.txt", &local_file_reader);
//     ASSERT_TRUE(st.ok()) << st;

//     constexpr int buf_size = 8192;
//     std::atomic_int empty_slice_times = 2;
//     auto sp = SyncPoint::get_instance();
//     config::enable_file_cache = true;
//     // Make the s3 file buffer pool return empty slice for the first two part
//     // to let the first two part be written into file cache first
//     sp->set_call_back("s3_file_bufferpool::allocate", [&empty_slice_times](auto&& values) {
//         LOG(INFO) << "file buffer pool allocate";
//         empty_slice_times--;
//         if (empty_slice_times >= 0) {
//             auto pairs = try_any_cast_ret<Slice>(values);
//             pairs->second = true;
//             LOG(INFO) << "return empty slice";
//         }
//     });
//     // Make append to cache return one error to test if it would exit
//     sp->set_call_back("file_block::read_at", [](auto&& values) {
//         LOG(INFO) << "file segment read at";
//         auto pairs = try_any_cast_ret<Status>(values);
//         pairs->second = true;
//         pairs->first = Status::IOError("failed to read from local cache file segments");
//     });
//     // Let read from cache some time for the next buffer to get one empty slice
//     sp->set_call_back("upload_file_buffer::read_from_cache", [](auto&& /*values*/) {
//         std::this_thread::sleep_for(std::chrono::milliseconds(500));
//     });
//     Defer defer {[&]() {
//         sp->clear_call_back("s3_file_bufferpool::allocate");
//         sp->clear_call_back("file_block::read_at");
//         sp->clear_call_back("upload_file_buffer::read_from_cache");
//         config::enable_file_cache = false;
//     }};

//     io::FileWriterPtr s3_file_writer;
//     st = s3_fs->create_file("read_from_cache_local_io_error", &s3_file_writer, &state);
//     ASSERT_TRUE(st.ok()) << st;

//     char buf[buf_size];
//     Slice slice(buf, buf_size);
//     size_t offset = 0;
//     size_t bytes_read = 0;
//     auto file_size = local_file_reader->size();
//     LOG(INFO) << "file size is " << file_size;
//     while (offset < file_size) {
//         st = local_file_reader->read_at(offset, slice, &bytes_read);
//         ASSERT_TRUE(st.ok()) << st;
//         auto st = s3_file_writer->append(Slice(buf, bytes_read));
//         ASSERT_TRUE(st.ok()) << st;
//         offset += bytes_read;
//     }
//     st = s3_file_writer->finalize();
//     ASSERT_TRUE(st.ok()) << st;
//     st = s3_file_writer->close();
//     ASSERT_FALSE(!st.ok()) << st;
//     bool exists = false;
//     st = s3_fs->exists("read_from_cache_local_io_error", &exists);
//     ASSERT_TRUE(st.ok()) << st;
//     ASSERT_FALSE(exists);
// }

TEST_F(S3FileWriterTest, normal) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    io::FileReaderSPtr local_file_reader;

    ASSERT_TRUE(
            fs->open_file("./be/test/storage/test_data/all_types_100000.txt", &local_file_reader)
                    .ok());

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    auto st = s3_fs->create_file("normal", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    LOG_INFO("the file size is {}", file_size);
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    st = s3_file_writer->close();
    ASSERT_TRUE(st.ok()) << st;
    int64_t s3_file_size = 0;
    st = s3_fs->file_size("normal", &s3_file_size);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(s3_file_size, file_size);
    const auto& contents = mock_client->contents();
    std::stringstream ss;
    for (size_t i = 1; i <= contents.size(); i++) {
        ss << contents.at(i);
    }
    std::string content = ss.str();
    std::unique_ptr<char[]> content_buf = std::make_unique<char[]>(file_size);
    Slice s(content_buf.get(), file_size);
    bytes_read = 0;
    st = local_file_reader->read_at(0, s, &bytes_read);
    ASSERT_EQ(0, std::memcmp(content.data(), s.get_data(), file_size));
}

TEST_F(S3FileWriterTest, smallFile) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/storage/test_data/all_types_1000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("small", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    st = s3_file_writer->close();
    ASSERT_TRUE(st.ok()) << st;
    int64_t s3_file_size = 0;
    st = s3_fs->file_size("small", &s3_file_size);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(s3_file_size, file_size);
    const auto& contents = mock_client->contents();
    std::stringstream ss;
    for (size_t i = 1; i <= contents.size(); i++) {
        ss << contents.at(i);
    }
    std::string content = ss.str();
    std::unique_ptr<char[]> content_buf = std::make_unique<char[]>(file_size);
    Slice s(content_buf.get(), file_size);
    bytes_read = 0;
    st = local_file_reader->read_at(0, s, &bytes_read);
    ASSERT_EQ(0, std::memcmp(content.data(), s.get_data(), file_size));
}

TEST_F(S3FileWriterTest, close_error) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/storage/test_data/all_types_1000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("s3_file_writer::close", [](auto&& values) {
        auto pairs = try_any_cast_ret<Status>(values);
        pairs->second = true;
        pairs->first = Status::InternalError("failed to close s3 file writer");
        LOG(INFO) << "return error when closing s3 file writer";
    });
    sp->set_call_back("S3FileWriter::_put_object", [](auto&& values) {
        // Deliberately make put object task fail to test if s3 file writer could
        // handle io error
        LOG(INFO) << "set put object to error";
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        io::S3FileWriter* writer = try_any_cast<io::S3FileWriter*>(values.at(0));
        io::UploadFileBuffer* buf = try_any_cast<io::UploadFileBuffer*>(values.at(1));
        writer->_st = Status::IOError(
                "failed to put object (bucket={}, key={}, upload_id={}, exception=inject "
                "error): "
                "inject error",
                writer->_obj_storage_path_opts.bucket, writer->_obj_storage_path_opts.path.native(),
                writer->upload_id());
        buf->set_status(writer->_st);
        bool* pred = try_any_cast<bool*>(values.back());
        *pred = true;
    });
    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("close_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;
    Defer defer {[&]() {
        sp->clear_call_back("s3_file_writer::close");
        sp->clear_call_back("S3FileWriter::_put_object");
    }};

    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
    bool exists = false;
    st = s3_fs->exists("close_error", &exists);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(exists);
}

TEST_F(S3FileWriterTest, multi_part_complete_error_2) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("S3FileWriter::_complete:2", [](auto&& outcome) {
        // Deliberately make one upload one part task fail to test if s3 file writer could
        // handle io error
        auto* parts = try_any_cast<std::vector<io::ObjectCompleteMultiPart>*>(outcome.back());
        size_t size = parts->size();
        parts->back().part_num = (size + 2);
    });
    Defer defer {[&]() { sp->clear_call_back("S3FileWriter::_complete:2"); }};
    auto client = s3_fs->client_holder();
    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/storage/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    // The second part would fail uploading itself to s3
    // so the result of close should be not ok
    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
}

TEST_F(S3FileWriterTest, multi_part_complete_error_1) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("S3FileWriter::_complete:1", [](auto&& outcome) {
        // Deliberately make one upload one part task fail to test if s3 file writer could
        // handle io error
        const auto& points = try_any_cast<
                const std::pair<std::atomic_bool*, std::vector<io::ObjectCompleteMultiPart>*>&>(
                outcome.back());
        (*points.first) = false;
        points.second->pop_back();
    });
    Defer defer {[&]() { sp->clear_call_back("S3FileWriter::_complete:1"); }};
    auto client = s3_fs->client_holder();
    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/storage/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    // The second part would fail uploading itself to s3
    // so the result of close should be not ok
    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
}

TEST_F(S3FileWriterTest, multi_part_complete_error_3) {
    mock_client = std::make_shared<MockS3Client>();
    doris::io::FileWriterOptions state;
    auto fs = io::global_local_filesystem();

    auto sp = SyncPoint::get_instance();
    sp->set_call_back("S3FileWriter::_complete:3", [](auto&& outcome) {
        auto pair = try_any_cast_ret<io::ObjectStorageResponse>(outcome);
        pair->second = true;
        pair->first = io::ObjectStorageResponse {
                .status = convert_to_obj_response(Status::IOError<false>("inject error"))};
    });
    Defer defer {[&]() { sp->clear_call_back("S3FileWriter::_complete:3"); }};
    auto client = s3_fs->client_holder();
    io::FileReaderSPtr local_file_reader;

    auto st = fs->open_file("./be/test/storage/test_data/all_types_100000.txt", &local_file_reader);
    ASSERT_TRUE(st.ok()) << st;

    constexpr int buf_size = 8192;

    io::FileWriterPtr s3_file_writer;
    st = s3_fs->create_file("multi_part_io_error", &s3_file_writer, &state);
    ASSERT_TRUE(st.ok()) << st;

    char buf[buf_size];
    Slice slice(buf, buf_size);
    size_t offset = 0;
    size_t bytes_read = 0;
    auto file_size = local_file_reader->size();
    while (offset < file_size) {
        st = local_file_reader->read_at(offset, slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        st = s3_file_writer->append(Slice(buf, bytes_read));
        ASSERT_TRUE(st.ok()) << st;
        offset += bytes_read;
    }
    ASSERT_EQ(s3_file_writer->bytes_appended(), file_size);
    st = s3_file_writer->close(true);
    ASSERT_TRUE(st.ok()) << st;
    // The second part would fail uploading itself to s3
    // so the result of close should be not ok
    st = s3_file_writer->close();
    ASSERT_FALSE(st.ok()) << st;
}

namespace io {
/**
 * This class is for boundary test
 */
class SimpleMockObjStorageClient : public io::ObjStorageClient {
public:
    SimpleMockObjStorageClient() = default;
    ~SimpleMockObjStorageClient() override = default;

    ObjectStorageResponse default_response {ObjectStorageResponse::OK()};
    ObjectStorageUploadResponse default_upload_response {.resp = ObjectStorageResponse::OK(),
                                                         .upload_id = "mock-upload-id",
                                                         .etag = "mock-etag"};
    ObjectStorageHeadResponse default_head_response {.resp = ObjectStorageResponse::OK(),
                                                     .file_size = 1024};
    std::string default_presigned_url = "https://mock-presigned-url.com";

    ObjectStorageUploadResponse create_multipart_upload(
            const ObjectStoragePathOptions& opts) override {
        std::lock_guard lock(_mutex);
        create_multipart_count++;
        create_multipart_params.push_back(opts);
        last_opts = opts;
        return default_upload_response;
    }

    ObjectStorageResponse put_object(const ObjectStoragePathOptions& opts,
                                     std::string_view stream) override {
        std::lock_guard lock(_mutex);
        put_object_count++;
        put_object_params.emplace_back(opts, std::string(stream));
        last_opts = opts;
        last_stream = std::string(stream);
        objects.emplace(opts.path.native(), std::string(stream));
        uploaded_bytes += stream.size();
        return default_response;
    }

    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions& opts,
                                            std::string_view stream, int part_num) override {
        std::lock_guard lock(_mutex);
        upload_part_count++;
        // upload_part_params.push_back({opts, std::string(stream), part_num});
        last_opts = opts;
        last_stream = std::string(stream);
        last_part_num = part_num;
        parts[_part_key(opts.path.native(), part_num)] = std::string(stream);
        uploaded_bytes += stream.size();
        return default_upload_response;
    }

    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions& opts,
            const std::vector<ObjectCompleteMultiPart>& completed_parts) override {
        std::lock_guard lock(_mutex);
        complete_multipart_count++;
        complete_multipart_params.push_back({opts, completed_parts});
        last_opts = opts;
        last_completed_parts = completed_parts;
        std::string final_obj;
        final_obj.reserve(uploaded_bytes);
        for (const auto& part : completed_parts) {
            final_obj.append(parts.at(_part_key(opts.path.native(), part.part_num)));
        }
        complete[opts.path.native()] = final_obj;
        objects[opts.path.native()] = final_obj;
        return default_response;
    }

    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions& opts) override {
        std::lock_guard lock(_mutex);
        return {.resp = ObjectStorageResponse::OK(),
                .file_size = static_cast<int64_t>(objects[opts.path.native()].size())};
    }

    ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                     size_t offset, size_t bytes_read,
                                     size_t* size_return) override {
        std::lock_guard lock(_mutex);
        last_opts = opts;
        last_offset = offset;
        last_bytes_read = bytes_read;
        if (size_return) {
            *size_return = bytes_read; // return default value
        }
        return default_response;
    }

    ObjectStorageResponse list_objects(const ObjectStoragePathOptions& opts,
                                       std::vector<FileInfo>* files) override {
        std::lock_guard lock(_mutex);
        last_opts = opts;
        if (files) {
            *files = default_file_list;
        }
        return default_response;
    }

    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                         std::vector<std::string> objs) override {
        std::lock_guard lock(_mutex);
        last_opts = opts;
        last_deleted_objects = std::move(objs);
        return default_response;
    }

    ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) override {
        std::lock_guard lock(_mutex);
        last_opts = opts;
        return default_response;
    }

    ObjectStorageResponse delete_objects_recursively(
            const ObjectStoragePathOptions& opts) override {
        std::lock_guard lock(_mutex);
        last_opts = opts;
        return default_response;
    }

    std::string generate_presigned_url(const ObjectStoragePathOptions& opts,
                                       int64_t expiration_secs, const S3ClientConf& conf) override {
        std::lock_guard lock(_mutex);
        last_opts = opts;
        last_expiration_secs = expiration_secs;
        return default_presigned_url;
    }

    // Variables to store the last call
    ObjectStoragePathOptions last_opts;
    std::string last_stream;
    int last_part_num = 0;
    std::vector<ObjectCompleteMultiPart> last_completed_parts;
    size_t last_offset = 0;
    size_t last_bytes_read = 0;
    std::vector<std::string> last_deleted_objects;
    int64_t last_expiration_secs = 0;
    std::vector<FileInfo> default_file_list;

    // Add counters for each function
    int create_multipart_count = 0;
    int put_object_count = 0;
    int upload_part_count = 0;
    int complete_multipart_count = 0;

    // Structures to store input parameters for each call
    struct UploadPartParams {
        ObjectStoragePathOptions opts;
        std::string stream;
        int part_num;
    };

    struct CompleteMultipartParams {
        ObjectStoragePathOptions opts;
        std::vector<ObjectCompleteMultiPart> parts;
    };

    // Vectors to store parameters from each call
    std::vector<ObjectStoragePathOptions> create_multipart_params;
    std::vector<std::pair<ObjectStoragePathOptions, std::string>> put_object_params;
    // std::vector<UploadPartParams> upload_part_params;
    std::vector<CompleteMultipartParams> complete_multipart_params;
    std::map<std::string, std::string> objects;
    std::map<std::string, std::string> complete;
    std::map<std::string, std::string> parts;
    int64_t uploaded_bytes = 0;

    void reset() {
        std::lock_guard lock(_mutex);
        last_opts = ObjectStoragePathOptions {};
        last_stream.clear();
        last_part_num = 0;
        last_completed_parts.clear();
        last_offset = 0;
        last_bytes_read = 0;
        last_deleted_objects.clear();
        last_expiration_secs = 0;

        create_multipart_count = 0;
        put_object_count = 0;
        upload_part_count = 0;
        complete_multipart_count = 0;

        create_multipart_params.clear();
        put_object_params.clear();
        // upload_part_params.clear();
        complete_multipart_params.clear();
        objects.clear();
        complete.clear();
        parts.clear();
        uploaded_bytes = 0;
    }

private:
    static std::string _part_key(const std::string& path, int part_num) {
        std::stringstream ss;
        ss << path << "_" << std::setfill('0') << std::setw(3) << part_num;
        return ss.str();
    }

    std::mutex _mutex;
};

} // namespace io

/**
 * Create a mock S3 client and a S3FileWriter.
 * @return A tuple containing the mock S3 client and the S3FileWriter.
 */
std::tuple<std::shared_ptr<SimpleMockObjStorageClient>, std::shared_ptr<S3FileWriter>>
create_s3_client(const std::string& path) {
    doris::io::FileWriterOptions opts;
    io::FileWriterPtr file_writer;
    auto st = s3_fs->create_file(path, &file_writer, &opts);
    EXPECT_TRUE(st.ok()) << st;
    std::shared_ptr<S3FileWriter> s3_file_writer(static_cast<S3FileWriter*>(file_writer.release()));
    auto holder = std::make_shared<ObjClientHolder>(S3ClientConf {});
    auto mock_client = std::make_shared<SimpleMockObjStorageClient>();
    holder->_client = mock_client;
    s3_file_writer->_obj_client = holder;
    return {mock_client, s3_file_writer};
}

/**
 * Generate test data for S3FileWriter boundary tests.
 * Returns a vector of sizes that we'll use to generate data on demand.
 * This way we don't need to hold all the data in memory at once.
 */
std::vector<size_t> generate_test_sizes(size_t num_samples = 20) {
    std::vector<size_t> sizes;
    const size_t MB = 1024 * 1024;
    const size_t MAX_SIZE = 256 * MB;

    // Add boundary cases
    sizes.push_back(0);      // Empty file
    sizes.push_back(1);      // Single byte
    sizes.push_back(MB - 1); // Just under 1MB
    sizes.push_back(MB);     // Exactly 1MB
    sizes.push_back(MB + 1); // Just over 1MB

    for (size_t i = 1; i <= 10; i++) { // Add buffer boundary cases
        sizes.push_back(i * config::s3_write_buffer_size - 1);
        sizes.push_back(i * config::s3_write_buffer_size);
        sizes.push_back(i * config::s3_write_buffer_size + 1);
    }

    // Add MB boundary cases up to 10MB
    for (size_t i = 2; i <= 20; i++) {
        sizes.push_back(i * MB - 1);
        sizes.push_back(i * MB);
        sizes.push_back(i * MB + 1);
    }

    // Add some larger MB boundaries
    for (size_t mb : {1, 2, 4, 8, 16, 32, 64, 128, 256}) {
        sizes.push_back(mb * MB - 1);
        sizes.push_back(mb * MB);
        sizes.push_back(mb * MB + 1);
    }

    // Add some random sizes
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> small_dist(
            2, config::s3_write_buffer_size); // Random sizes under s3_write_buffer_size
    std::uniform_int_distribution<size_t> large_dist(config::s3_write_buffer_size,
                                                     MAX_SIZE); // Random sizes up to 256MB
    for (int i = 0; i < 5; i++) {
        sizes.push_back(small_dist(gen));
    }
    for (int i = 0; i < 5; i++) { // sparse test
        sizes.push_back(large_dist(gen));
    }

    // Sort and remove duplicates
    std::sort(sizes.begin(), sizes.end());
    sizes.erase(std::unique(sizes.begin(), sizes.end()), sizes.end());
    std::shuffle(sizes.begin(), sizes.end(), gen);
    sizes.resize(std::min(sizes.size(), num_samples));
    return sizes;
}

/**
 * Generate a string of specified size efficiently.
 * The string will start and end with the magic character,
 * and have random content in between.
 */
std::string generate_test_string(char magic_char, size_t size) {
    if (size == 0) return "";
    std::string result;
    result.reserve(size);
    result.resize(size);
    result.front() = magic_char;
    result.back() = magic_char;
    return result;
}

// the internal implementation of s3_file_writer and s3_fs
std::string get_s3_path(std::string_view path) {
    return std::string("s3://") + s3_fs->bucket() + "/" + s3_fs->prefix() + "/" + std::string(path);
};

// put object
// create_multi_parts_upload + upload_part + complete_parts
TEST_F(S3FileWriterTest, write_buffer_boundary) {
    // diable file cache to avoid write to cache
    bool enable_file_cache = config::enable_file_cache;
    config::enable_file_cache = false;
    Defer defer {[&]() { config::enable_file_cache = enable_file_cache; }};

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->clear_all_call_backs();

    // s3_file_writer is the interface to write to s3
    // mock_client is a SimpleMockObjStorageClient for testing, it holds the data in memory
    // we check the data in mock_client to make sure s3_file_writer is working as expected
    auto test = [](char magic_char, size_t data_size, const std::string& filename) {
        std::string content = generate_test_string(magic_char, data_size);
        auto [mock_client, s3_file_writer] = create_s3_client(filename);
        std::string expected_path = get_s3_path(filename);
        std::stringstream ss;
        ss << "filename: " << filename << ", data_size: " << data_size
           << ", magic_char: " << magic_char;
        std::string msg = ss.str();
        EXPECT_EQ(s3_file_writer->append(content), Status::OK()) << msg;
        EXPECT_EQ(s3_file_writer->close(), Status::OK()) << msg;
        if (content.size() < config::s3_write_buffer_size) {
            EXPECT_EQ(mock_client->put_object_count, 1) << msg;
            EXPECT_EQ(mock_client->create_multipart_count, 0) << msg;
            EXPECT_EQ(mock_client->complete_multipart_count, 0) << msg;
            EXPECT_EQ(mock_client->upload_part_count, 0) << msg;
        } else { // >= s3_write_buffer_size, use multipart
            int expected_num_parts = (content.size() / config::s3_write_buffer_size) +
                                     !!(content.size() % config::s3_write_buffer_size);
            EXPECT_EQ(mock_client->put_object_count, 0) << msg;
            EXPECT_EQ(mock_client->create_multipart_count, 1) << msg;
            EXPECT_EQ(mock_client->complete_multipart_count, 1) << msg;
            EXPECT_EQ(mock_client->upload_part_count, expected_num_parts) << msg;
        }
        EXPECT_EQ(mock_client->last_opts.path.native(), expected_path) << msg;
        EXPECT_EQ(mock_client->objects[expected_path].size(), content.size()) << msg;
        // EXPECT_EQ(mock_client->last_stream, content); // Will print too many if compare all content if failed
        if (content.size() > 0 && mock_client->objects[expected_path].size() > 0) {
            EXPECT_EQ(mock_client->objects[expected_path].front(), content.front()) << msg;
            EXPECT_EQ(mock_client->objects[expected_path].back(), content.back()) << msg;
        }
    };
    // fpath is a function to generate a file path for debug to locate line number if some tests failed
    auto fpath = [](const char* file, int line, std::string suffix) {
        std::stringstream ss;
        ss << file << ":" << line << "_" << suffix;
        std::string ret = ss.str();
        // return ret.substr(ret.rfind('/') + 1); // keep file name only
        return ret.substr(ret[0] == '/'); // remove the first '/'
    };

    // test all sizes in generate_test_sizes()
    for (auto& i : generate_test_sizes(20)) { // reduce number of cases if it spends too much time
        test(char('a' + (i % 26)), i, fpath(__FILE__, __LINE__, std::to_string(i) + ".dat"));
    }

    // clang-format off
    // some verbose tests
    test('a', 0, fpath(__FILE__, __LINE__, "0.dat"));
    test('b', 1, fpath(__FILE__, __LINE__, "1.dat"));
    test('c', 2, fpath(__FILE__, __LINE__, "2.dat"));
    test('d', 1024L, fpath(__FILE__, __LINE__, "1024.dat"));
    test('e', 4 * 1024L, fpath(__FILE__, __LINE__, "512K.dat"));
    test('f', 64 * 1024L, fpath(__FILE__, __LINE__, "1M.dat"));
    test('g', 512 * 1024L, fpath(__FILE__, __LINE__, "2M.dat"));
    test('h', 1 * 1024L * 1024L, fpath(__FILE__, __LINE__, "1M.dat"));
    test('i', 2 * 1024L * 1024L, fpath(__FILE__, __LINE__, "2M.dat"));
    test('j', 4 * 1024L * 1024L, fpath(__FILE__, __LINE__, "4M.dat"));
    test('k', 8 * 1024L * 1024L, fpath(__FILE__, __LINE__, "8M.dat"));
    test('l', 16 * 1024L * 1024L, fpath(__FILE__, __LINE__, "16M.dat"));
    test('m', 32 * 1024L * 1024L, fpath(__FILE__, __LINE__, "32M.dat"));
    test('n', 64 * 1024L * 1024L, fpath(__FILE__, __LINE__, "64M.dat"));
    test('o', 128 * 1024L * 1024L, fpath(__FILE__, __LINE__, "128M.dat"));
    test('p', 256 * 1024L * 1024L, fpath(__FILE__, __LINE__, "256M.dat"));
    // test('q', 512 * 1024L * 1024L, fpath(__FILE__, __LINE__, "512M.dat"));
    test('r', config::s3_write_buffer_size - 1, fpath(__FILE__, __LINE__, ".dat"));
    test('s', config::s3_write_buffer_size, fpath(__FILE__, __LINE__, ".dat"));
    test('t', config::s3_write_buffer_size + 1, fpath(__FILE__, __LINE__, ".dat"));
    test('u', 2 * config::s3_write_buffer_size - 1, fpath(__FILE__, __LINE__, ".dat"));
    test('v', 2 * config::s3_write_buffer_size, fpath(__FILE__, __LINE__, ".dat"));
    test('w', 2 * config::s3_write_buffer_size + 1, fpath(__FILE__, __LINE__, ".dat"));
    test('x', 3 * config::s3_write_buffer_size - 1, fpath(__FILE__, __LINE__, ".dat"));
    test('y', 3 * config::s3_write_buffer_size, fpath(__FILE__, __LINE__, ".dat"));
    test('z', 3 * config::s3_write_buffer_size + 1, fpath(__FILE__, __LINE__, ".dat"));
    // test with large buffer size
    config::s3_write_buffer_size = 8 * 1024L * 1024L;
    test('0', config::s3_write_buffer_size - 1, fpath(__FILE__, __LINE__, ".dat"));
    test('1', config::s3_write_buffer_size, fpath(__FILE__, __LINE__, ".dat"));
    test('2', config::s3_write_buffer_size + 1, fpath(__FILE__, __LINE__, ".dat"));
    test('0', 2 * config::s3_write_buffer_size - 1, fpath(__FILE__, __LINE__, ".dat"));
    test('1', 2 * config::s3_write_buffer_size, fpath(__FILE__, __LINE__, ".dat"));
    test('2', 2 * config::s3_write_buffer_size + 1, fpath(__FILE__, __LINE__, ".dat"));
    // test with small buffer size
    config::s3_write_buffer_size = 4 * 1024L * 1024L;
    test('0', config::s3_write_buffer_size - 1, fpath(__FILE__, __LINE__, ".dat"));
    test('1', config::s3_write_buffer_size, fpath(__FILE__, __LINE__, ".dat"));
    test('2', config::s3_write_buffer_size + 1, fpath(__FILE__, __LINE__, ".dat"));
    test('0', 2 * config::s3_write_buffer_size - 1, fpath(__FILE__, __LINE__, ".dat"));
    test('1', 2 * config::s3_write_buffer_size, fpath(__FILE__, __LINE__, ".dat"));
    test('2', 2 * config::s3_write_buffer_size + 1, fpath(__FILE__, __LINE__, ".dat"));
    // clang-format on
}

// A live change of s3_write_buffer_size must not affect a writer that is already open: its parts,
// its buffers and its expected part count all keep the size it was created with.
TEST_F(S3FileWriterTest, buffer_size_change_mid_write) {
    bool enable_file_cache = config::enable_file_cache;
    config::enable_file_cache = false;
    auto origin_buffer_size = config::s3_write_buffer_size;
    Defer defer {[&]() {
        config::enable_file_cache = enable_file_cache;
        config::s3_write_buffer_size = origin_buffer_size;
    }};
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->clear_all_call_backs();

    constexpr size_t MB = 1024 * 1024;
    auto run = [](size_t open_size, size_t first, size_t changed_size, size_t second,
                  int expected_parts, const std::string& filename) {
        config::s3_write_buffer_size = open_size;
        auto [mock_client, s3_file_writer] = create_s3_client(filename);
        std::string head = generate_test_string('h', first);
        std::string tail = generate_test_string('t', second);
        ASSERT_EQ(s3_file_writer->append(head), Status::OK());
        config::s3_write_buffer_size = changed_size;
        ASSERT_EQ(s3_file_writer->append(tail), Status::OK());
        ASSERT_EQ(s3_file_writer->close(), Status::OK());
        std::string expected_path = get_s3_path(filename);
        EXPECT_EQ(mock_client->upload_part_count, expected_parts) << filename;
        EXPECT_EQ(mock_client->complete_multipart_count, 1) << filename;
        ASSERT_EQ(mock_client->objects[expected_path].size(), first + second) << filename;
        EXPECT_EQ(mock_client->objects[expected_path].front(), 'h') << filename;
        EXPECT_EQ(mock_client->objects[expected_path].back(), 't') << filename;
    };
    // Grown while a part is half full: 5 + 5 + 3 MB, not an overrun of the 5 MB buffer.
    run(5 * MB, 7 * MB, 8 * MB, 6 * MB, 3, "buffer_size_grows_mid_write.dat");
    // Shrunk below the bytes already buffered: 8 + 2 MB, not a size underflow.
    run(8 * MB, 6 * MB, 5 * MB, 4 * MB, 2, "buffer_size_shrinks_mid_write.dat");
}

TEST_F(S3FileWriterTest, test_empty_file) {
    std::vector<StorePath> paths;
    paths.emplace_back(std::string("tmp_dir"), 1024000000);
    auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
    EXPECT_TRUE(tmp_file_dirs->init().ok());
    ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
    doris::io::FileWriterOptions opts;
    io::FileWriterPtr file_writer;
    auto st = s3_fs->create_file("test_empty_file.idx", &file_writer, &opts);
    EXPECT_TRUE(st.ok()) << st;
    auto holder = std::make_shared<ObjClientHolder>(S3ClientConf {});
    auto mock_client = std::make_shared<SimpleMockObjStorageClient>();
    holder->_client = mock_client;
    dynamic_cast<io::S3FileWriter*>(file_writer.get())->_obj_client = holder;
    auto fs = io::global_local_filesystem();
    std::string index_path = "/tmp/empty_index_file_test";
    std::string rowset_id = "1234567890";
    int64_t seg_id = 1234567890;
    auto index_file_writer = std::make_unique<segment_v2::IndexFileWriter>(
            fs, index_path, rowset_id, seg_id, InvertedIndexStorageFormatPB::V2,
            std::move(file_writer), false);
    EXPECT_TRUE(index_file_writer->begin_close().ok());
    EXPECT_TRUE(index_file_writer->finish_close().ok());
}

namespace {

std::string current_pool_thread_name() {
    auto* t = Thread::current_thread();
    return t == nullptr ? std::string("<none>") : t->name();
}

// Records which pool thread served each upload / complete call, and optionally blocks
// calls for keys containing `gate_marker` until release() or injects per-part latency.
class PoolRecordingMockClient : public io::SimpleMockObjStorageClient {
public:
    io::ObjectStorageResponse put_object(const io::ObjectStoragePathOptions& opts,
                                         std::string_view stream) override {
        _on_upload(opts);
        return SimpleMockObjStorageClient::put_object(opts, stream);
    }

    io::ObjectStorageUploadResponse upload_part(const io::ObjectStoragePathOptions& opts,
                                                std::string_view stream, int part_num) override {
        _on_upload(opts);
        return SimpleMockObjStorageClient::upload_part(opts, stream, part_num);
    }

    io::ObjectStorageResponse complete_multipart_upload(
            const io::ObjectStoragePathOptions& opts,
            const std::vector<io::ObjectCompleteMultiPart>& completed_parts) override {
        {
            std::lock_guard lock(_record_mutex);
            complete_threads.push_back(current_pool_thread_name());
        }
        return SimpleMockObjStorageClient::complete_multipart_upload(opts, completed_parts);
    }

    void release() {
        {
            std::lock_guard lock(_record_mutex);
            _released = true;
        }
        _cv.notify_all();
    }

    std::vector<std::string> upload_thread_names() {
        std::lock_guard lock(_record_mutex);
        return upload_threads;
    }
    std::vector<std::string> complete_thread_names() {
        std::lock_guard lock(_record_mutex);
        return complete_threads;
    }

    std::string gate_marker;
    std::chrono::milliseconds upload_latency {0};
    std::atomic<int> concurrent_uploads {0};
    std::atomic<int> peak_concurrent_uploads {0};
    std::atomic<int> blocked_uploads {0};

private:
    void _on_upload(const io::ObjectStoragePathOptions& opts) {
        {
            std::lock_guard lock(_record_mutex);
            upload_threads.push_back(current_pool_thread_name());
        }
        int now = ++concurrent_uploads;
        int peak = peak_concurrent_uploads.load();
        while (now > peak && !peak_concurrent_uploads.compare_exchange_weak(peak, now)) {
        }
        if (upload_latency.count() > 0) {
            std::this_thread::sleep_for(upload_latency);
        }
        if (!gate_marker.empty() && opts.path.native().find(gate_marker) != std::string::npos) {
            ++blocked_uploads;
            std::unique_lock lock(_record_mutex);
            _cv.wait(lock, [this] { return _released; });
            --blocked_uploads;
        }
        --concurrent_uploads;
    }

    std::mutex _record_mutex;
    std::condition_variable _cv;
    bool _released = false;
    std::vector<std::string> upload_threads;
    std::vector<std::string> complete_threads;
};

std::unique_ptr<S3FileWriter> make_writer(const std::string& path, bool background,
                                          const std::shared_ptr<io::ObjStorageClient>& client) {
    io::FileWriterOptions opts;
    opts.background_write = background;
    io::FileWriterPtr file_writer;
    auto st = s3_fs->create_file(path, &file_writer, &opts);
    EXPECT_TRUE(st.ok()) << st;
    std::unique_ptr<S3FileWriter> writer(static_cast<S3FileWriter*>(file_writer.release()));
    auto holder = std::make_shared<ObjClientHolder>(S3ClientConf {});
    holder->_client = client;
    writer->_obj_client = holder;
    return writer;
}

bool all_start_with(const std::vector<std::string>& names, const std::string& prefix) {
    return !names.empty() && std::all_of(names.begin(), names.end(),
                                         [&](const auto& n) { return n.rfind(prefix, 0) == 0; });
}

std::unique_ptr<ThreadPool> build_pool(const std::string& name, int threads) {
    std::unique_ptr<ThreadPool> pool;
    EXPECT_TRUE(ThreadPoolBuilder(name)
                        .set_min_threads(threads)
                        .set_max_threads(threads)
                        .build(&pool)
                        .ok());
    return pool;
}

} // namespace

class S3FileWriterPoolTest : public S3FileWriterTest {
protected:
    void SetUp() override {
        _saved_enable_file_cache = config::enable_file_cache;
        _saved_enable_separate = config::enable_separate_compaction_s3_upload_pool;
        _saved_max_inflight = config::compaction_s3_upload_max_inflight_parts;
        config::enable_file_cache = false;
        config::enable_separate_compaction_s3_upload_pool = true;
        config::compaction_s3_upload_max_inflight_parts = 0;
        auto sp = SyncPoint::get_instance();
        sp->enable_processing();
        sp->clear_all_call_backs();
    }
    void TearDown() override {
        ExecEnv::GetInstance()->set_compaction_s3_file_upload_thread_pool(nullptr);
        ExecEnv::GetInstance()->set_compaction_non_block_close_thread_pool(nullptr);
        config::enable_file_cache = _saved_enable_file_cache;
        config::enable_separate_compaction_s3_upload_pool = _saved_enable_separate;
        config::compaction_s3_upload_max_inflight_parts = _saved_max_inflight;
    }
    void install_compaction_pools(int upload_threads, int close_threads) {
        ExecEnv::GetInstance()->set_compaction_s3_file_upload_thread_pool(
                build_pool("TestCompactionUpload", upload_threads));
        ExecEnv::GetInstance()->set_compaction_non_block_close_thread_pool(
                build_pool("TestCompactionClose", close_threads));
    }

    bool _saved_enable_file_cache = false;
    bool _saved_enable_separate = true;
    int64_t _saved_max_inflight = 0;
};

TEST_F(S3FileWriterPoolTest, compaction_and_load_writers_use_separate_pools) {
    install_compaction_pools(4, 2);
    std::string part(config::s3_write_buffer_size, 'x');

    auto run = [&](const std::string& path, bool background) {
        auto client = std::make_shared<PoolRecordingMockClient>();
        auto writer = make_writer(path, background, client);
        for (int i = 0; i < 2; ++i) {
            EXPECT_TRUE(writer->append(part).ok());
        }
        EXPECT_TRUE(writer->append(Slice("tail", 4)).ok());
        EXPECT_TRUE(writer->close(true).ok());
        EXPECT_TRUE(writer->close().ok());
        EXPECT_EQ(client->upload_part_count, 3);
        return std::make_tuple(writer->uses_background_pools(), client->upload_thread_names(),
                               client->complete_thread_names());
    };

    auto [load_bg, load_uploads, load_completes] = run("load_seg.dat", false);
    EXPECT_FALSE(load_bg);
    EXPECT_TRUE(all_start_with(load_uploads, "s3_upload_file_thread_pool"));
    EXPECT_TRUE(all_start_with(load_completes, "NonBlockCloseThreadPool"));

    auto [comp_bg, comp_uploads, comp_completes] = run("compaction_seg.dat", true);
    EXPECT_TRUE(comp_bg);
    EXPECT_TRUE(all_start_with(comp_uploads, "TestCompactionUpload"));
    EXPECT_TRUE(all_start_with(comp_completes, "TestCompactionClose"));

    // Disabled: background writers fall back to the shared pools.
    config::enable_separate_compaction_s3_upload_pool = false;
    auto [off_bg, off_uploads, off_completes] = run("compaction_seg_off.dat", true);
    EXPECT_FALSE(off_bg);
    EXPECT_TRUE(all_start_with(off_uploads, "s3_upload_file_thread_pool"));
    EXPECT_TRUE(all_start_with(off_completes, "NonBlockCloseThreadPool"));
}

TEST_F(S3FileWriterPoolTest, compaction_falls_back_when_pools_absent) {
    auto client = std::make_shared<PoolRecordingMockClient>();
    auto writer = make_writer("compaction_no_pool.dat", true, client);
    std::string part(config::s3_write_buffer_size, 'y');
    EXPECT_TRUE(writer->append(part).ok());
    EXPECT_TRUE(writer->append(part).ok());
    EXPECT_TRUE(writer->close(true).ok());
    EXPECT_TRUE(writer->close().ok());
    EXPECT_TRUE(all_start_with(client->upload_thread_names(), "s3_upload_file_thread_pool"));
    EXPECT_EQ(io::UploadBufferInflightLimiter::compaction()->inflight(), 0);
}

TEST_F(S3FileWriterPoolTest, inflight_limiter_blocks_at_limit) {
    std::atomic<int64_t> limit {2};
    io::UploadBufferInflightLimiter limiter([&] { return limit.load(); });
    auto p1 = limiter.acquire();
    auto p2 = limiter.acquire();
    EXPECT_EQ(limiter.inflight(), 2);

    std::atomic<bool> acquired {false};
    std::thread t([&] {
        auto p3 = limiter.acquire();
        acquired = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_FALSE(acquired.load());
    EXPECT_EQ(limiter.waiters(), 1);
    p1.reset();
    t.join();
    EXPECT_TRUE(acquired.load());
    EXPECT_EQ(limiter.inflight(), 1);

    // Raising the limit at runtime wakes a blocked waiter without any release.
    limit = 1;
    std::atomic<bool> acquired2 {false};
    std::thread t2([&] {
        auto p = limiter.acquire();
        acquired2 = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_FALSE(acquired2.load());
    limit = 0; // unbounded
    t2.join();
    EXPECT_TRUE(acquired2.load());
    p2.reset();
    EXPECT_EQ(limiter.inflight(), 0);
}

TEST_F(S3FileWriterPoolTest, inflight_cap_bounds_concurrent_compaction_parts) {
    install_compaction_pools(8, 4);
    config::compaction_s3_upload_max_inflight_parts = 2;
    auto client = std::make_shared<PoolRecordingMockClient>();
    client->upload_latency = std::chrono::milliseconds(50);
    std::string part(config::s3_write_buffer_size, 'z');

    constexpr int kWriters = 4;
    constexpr int kParts = 4;
    std::atomic<int64_t> peak_inflight {0};
    std::atomic<bool> stop_sampling {false};
    std::thread sampler([&] {
        while (!stop_sampling) {
            auto v = io::UploadBufferInflightLimiter::compaction()->inflight();
            auto p = peak_inflight.load();
            while (v > p && !peak_inflight.compare_exchange_weak(p, v)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    std::vector<std::thread> writers;
    std::atomic<int> ok {0};
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            auto writer = make_writer(fmt::format("compaction_cap_{}.dat", w), true, client);
            bool good = true;
            for (int i = 0; i < kParts; ++i) {
                good &= writer->append(part).ok();
            }
            good &= writer->close(true).ok();
            good &= writer->close().ok();
            ok += good;
        });
    }
    for (auto& t : writers) {
        t.join();
    }
    stop_sampling = true;
    sampler.join();

    EXPECT_EQ(ok.load(), kWriters);
    EXPECT_EQ(client->upload_part_count, kWriters * kParts);
    EXPECT_LE(client->peak_concurrent_uploads.load(), 2);
    EXPECT_GE(client->peak_concurrent_uploads.load(), 1);
    EXPECT_LE(peak_inflight.load(), 2);
    EXPECT_EQ(io::UploadBufferInflightLimiter::compaction()->inflight(), 0);
}

// Compaction writers whose uploads hang must not delay a load writer's close: with the
// separate pools a load close finishes while every compaction close is still pending.
// With the feature off the same burst fills the shared upload pool and the load close
// stalls behind it.
TEST_F(S3FileWriterPoolTest, saturated_compaction_pool_does_not_delay_load_close) {
    constexpr int kCompactionWriters = 12; // > the shared test upload pool's 10 threads
    std::string part(config::s3_write_buffer_size, 'c');

    auto run = [&](bool separate) -> std::pair<bool, bool> {
        config::enable_separate_compaction_s3_upload_pool = separate;
        install_compaction_pools(2, 2);
        auto client = std::make_shared<PoolRecordingMockClient>();
        client->gate_marker = "compaction_";
        Defer release {[&] { client->release(); }};

        std::vector<std::unique_ptr<S3FileWriter>> compaction_writers;
        for (int i = 0; i < kCompactionWriters; ++i) {
            auto writer =
                    make_writer(fmt::format("compaction_{}_{}.dat", separate, i), true, client);
            EXPECT_TRUE(writer->append(Slice("small", 5)).ok());
            EXPECT_TRUE(writer->close(true).ok());
            compaction_writers.push_back(std::move(writer));
        }
        // Wait until the burst occupies its upload threads.
        int expected_blocked = separate ? 2 : 10;
        for (int i = 0; i < 500 && client->blocked_uploads.load() < expected_blocked; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(client->blocked_uploads.load(), expected_blocked);

        auto load_writer = make_writer(fmt::format("load_{}.dat", separate), false, client);
        EXPECT_TRUE(load_writer->append(part).ok());
        EXPECT_TRUE(load_writer->append(part).ok());
        EXPECT_TRUE(load_writer->close(true).ok());
        auto load_close = std::async(std::launch::async, [&] { return load_writer->close(); });
        bool load_finished_quickly =
                load_close.wait_for(std::chrono::seconds(3)) == std::future_status::ready;
        bool compaction_pending = std::none_of(compaction_writers.begin(), compaction_writers.end(),
                                               [](auto& w) { return w->try_finish_close().ok(); });

        client->release();
        EXPECT_TRUE(load_close.get().ok());
        for (auto& w : compaction_writers) {
            auto st = w->close();
            EXPECT_TRUE(st.ok() || w->state() == io::FileWriter::State::CLOSED) << st;
        }
        compaction_writers.clear();
        return {load_finished_quickly, compaction_pending};
    };

    auto [separate_fast, separate_pending] = run(true);
    EXPECT_TRUE(separate_fast);
    EXPECT_TRUE(separate_pending);

    auto [shared_fast, shared_pending] = run(false);
    EXPECT_FALSE(shared_fast);
    EXPECT_TRUE(shared_pending);
}

// Every file a compaction writes - segment, inverted index, and the small segment-0 files
// that load would route through a shared packed file - must reach S3 on the compaction pools.
TEST_F(S3FileWriterPoolTest, compaction_segment_index_and_packed_files_use_compaction_pool) {
    install_compaction_pools(4, 2);
    auto packed_fs = std::make_shared<io::PackedFileSystem>(s3_fs);
    std::string part(config::s3_write_buffer_size, 'p');

    auto options_for = [](DataWriteType type, FileType file_type) {
        RowsetWriterContext ctx;
        ctx.write_type = type;
        return ctx.get_file_writer_options(file_type);
    };
    for (auto type : {DataWriteType::TYPE_COMPACTION, DataWriteType::TYPE_SCHEMA_CHANGE}) {
        EXPECT_TRUE(options_for(type, FileType::SEGMENT_FILE).background_write);
        EXPECT_TRUE(options_for(type, FileType::INVERTED_INDEX_FILE).background_write);
    }
    EXPECT_FALSE(options_for(DataWriteType::TYPE_DIRECT, FileType::SEGMENT_FILE).background_write);
    EXPECT_FALSE(options_for(DataWriteType::TYPE_DIRECT, FileType::INVERTED_INDEX_FILE)
                         .background_write);

    struct Case {
        std::string path;
        FileType file_type;
        size_t bytes;
    };
    std::vector<Case> cases {
            {"compaction_rs_0.dat", FileType::SEGMENT_FILE, 2 * part.size() + 7}, // packed-eligible
            {"compaction_rs_0.idx", FileType::INVERTED_INDEX_FILE, 1024}, // small, packed-eligible
            {"compaction_rs_3.dat", FileType::SEGMENT_FILE, 2 * part.size()},
            {"compaction_rs_3.idx", FileType::INVERTED_INDEX_FILE, 1024},
    };
    for (const auto& c : cases) {
        auto opts = options_for(DataWriteType::TYPE_COMPACTION, c.file_type);
        io::FileWriterPtr file_writer;
        ASSERT_TRUE(packed_fs->create_file(c.path, &file_writer, &opts).ok());
        auto* s3_writer = dynamic_cast<S3FileWriter*>(file_writer.get());
        ASSERT_NE(s3_writer, nullptr) << c.path << " was wrapped in a shared packed file";
        EXPECT_TRUE(s3_writer->uses_background_pools()) << c.path;

        auto client = std::make_shared<PoolRecordingMockClient>();
        auto holder = std::make_shared<ObjClientHolder>(S3ClientConf {});
        holder->_client = client;
        s3_writer->_obj_client = holder;
        for (size_t left = c.bytes; left > 0;) {
            size_t n = std::min(left, part.size());
            ASSERT_TRUE(file_writer->append(Slice(part.data(), n)).ok());
            left -= n;
        }
        ASSERT_TRUE(file_writer->close(true).ok());
        ASSERT_TRUE(file_writer->close().ok());
        EXPECT_TRUE(all_start_with(client->upload_thread_names(), "TestCompactionUpload"))
                << c.path;
        auto completes = client->complete_thread_names();
        if (!completes.empty()) {
            EXPECT_TRUE(all_start_with(completes, "TestCompactionClose")) << c.path;
        }
    }

    // The same segment-0 files written by a load still join the packed file.
    for (auto file_type : {FileType::SEGMENT_FILE, FileType::INVERTED_INDEX_FILE}) {
        auto opts = options_for(DataWriteType::TYPE_DIRECT, file_type);
        io::FileWriterPtr file_writer;
        auto path = file_type == FileType::SEGMENT_FILE ? "load_rs_0.dat" : "load_rs_0.idx";
        ASSERT_TRUE(packed_fs->create_file(path, &file_writer, &opts).ok());
        EXPECT_NE(dynamic_cast<io::PackedFileWriter*>(file_writer.get()), nullptr) << path;
    }
}

} // namespace doris
