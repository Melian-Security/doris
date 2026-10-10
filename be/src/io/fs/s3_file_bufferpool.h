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

#include <crc32c/crc32c.h>

#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <utility>

#include "common/status.h"
#include "io/cache/file_block.h"
#include "util/slice.h"
#include "util/threadpool.h"

namespace doris {
namespace io {
enum class BufferType : uint32_t { DOWNLOAD, UPLOAD };

// Bounds how many upload buffers of one class (compaction or load) are submitted but not yet
// released. A permit is held by the buffer and released when the buffer is destroyed, i.e.
// once its memory is freed. Acquire only when the caller holds no other unsubmitted permit
// it depends on: permit holders are submitted buffers, which finish without the caller.
class UploadBufferInflightLimiter {
public:
    class Permit {
    public:
        Permit() = default;
        explicit Permit(UploadBufferInflightLimiter* limiter) : _limiter(limiter) {}
        Permit(Permit&& other) noexcept : _limiter(std::exchange(other._limiter, nullptr)) {}
        Permit& operator=(Permit&& other) noexcept {
            if (this != &other) {
                reset();
                _limiter = std::exchange(other._limiter, nullptr);
            }
            return *this;
        }
        Permit(const Permit&) = delete;
        Permit& operator=(const Permit&) = delete;
        ~Permit() { reset(); }
        void reset() {
            if (_limiter != nullptr) {
                std::exchange(_limiter, nullptr)->_release();
            }
        }
        bool valid() const { return _limiter != nullptr; }

    private:
        UploadBufferInflightLimiter* _limiter = nullptr;
    };

    // The bvars a limiter reports to, defined in s3_file_bufferpool.cpp.
    struct Metrics;

    // `limit` is re-read on every acquire so a mutable config takes effect at runtime;
    // a value <= 0 means unbounded. `metrics` may be null, and must outlive the limiter.
    explicit UploadBufferInflightLimiter(std::function<int64_t()> limit,
                                         const Metrics* metrics = nullptr)
            : _limit(std::move(limit)), _metrics(metrics) {}

    // The process-wide limiter for compaction / schema change uploads, bounded by
    // config::compaction_s3_upload_max_inflight_parts.
    static UploadBufferInflightLimiter* compaction();
    // The process-wide limiter for load (non background) uploads, bounded by
    // config::load_s3_upload_max_inflight_parts.
    static UploadBufferInflightLimiter* load();

    // Blocks until a slot is free.
    Permit acquire();
    int64_t inflight() const;
    int64_t waiters() const;

private:
    void _release();

    std::function<int64_t()> _limit;
    const Metrics* _metrics;
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    int64_t _inflight = 0;
    int64_t _waiters = 0;
};
using FileBlocksHolderPtr = std::unique_ptr<FileBlocksHolder>;
struct OperationState {
    OperationState(std::function<bool(Status)> sync_after_complete_task,
                   std::function<bool()> is_cancelled)
            : _sync_after_complete_task(std::move(sync_after_complete_task)),
              _is_cancelled(std::move(is_cancelled)) {}
    /**
    * set the val of this operation state which indicates it failed or succeeded
    *
    * @param S the execution result
    */
    void set_status(Status s = Status::OK()) {
        // make sure we wouldn't sync twice
        if (_value_set) [[unlikely]] {
            return;
        }
        if (nullptr != _sync_after_complete_task) {
            _fail_after_sync = _sync_after_complete_task(std::move(s));
        }
        _value_set = true;
    }

    /**
    * detect whether the execution task is done
    *
    * @return is the execution task is done
    */
    [[nodiscard]] bool is_cancelled() const {
        DCHECK(nullptr != _is_cancelled);
        // If _fail_after_sync is true then it means the sync task already returns
        // that the task failed and if the outside file writer might already be
        // destructed
        return _fail_after_sync ? true : _is_cancelled();
    }

    std::function<bool(Status)> _sync_after_complete_task;
    std::function<bool()> _is_cancelled;
    bool _value_set = false;
    bool _fail_after_sync = false;
};

struct FileBuffer {
    // capacity 0 means config::s3_write_buffer_size at construction.
    FileBuffer(BufferType type, std::function<FileBlocksHolderPtr()> alloc_holder, size_t offset,
               OperationState state, size_t capacity = 0);
    virtual ~FileBuffer();
    /**
    * submit the correspoding task to async executor
    */
    static Status submit(std::shared_ptr<FileBuffer> buf);
    /**
    * append data to the inner memory buffer
    *
    * @param S the content to be appended
    */
    virtual Status append_data(const Slice& s) = 0;
    virtual void execute_async() = 0;
    /**
    * set the val of it's operation state
    *
    * @param S the execution result
    */
    void set_status(Status s) { _state.set_status(s); }
    /**
    * get the start offset of this file buffer
    *
    * @return start offset of this file buffer
    */
    size_t get_file_offset() const { return _offset; }
    /**
    * get the size of the buffered data
    *
    * @return the size of the buffered data
    */
    size_t get_size() const { return _size; }
    size_t get_capacaticy() const { return _capacity; }
    Slice get_slice() const;
    /**
    * detect whether the execution task is done
    *
    * @return is the execution task is done
    */
    bool is_cancelled() const { return _state.is_cancelled(); }

    std::string_view get_string_view_data() const;

    // Released in ~FileBuffer after the part memory is freed.
    void set_inflight_permit(UploadBufferInflightLimiter::Permit permit) {
        _inflight_permit = std::move(permit);
    }

    // Declared first so it is destroyed last, after ~FileBuffer frees _inner_data.
    UploadBufferInflightLimiter::Permit _inflight_permit;
    BufferType _type;
    std::function<FileBlocksHolderPtr()> _alloc_holder;
    size_t _offset;
    size_t _size;
    OperationState _state;
    struct PartData;
    std::unique_ptr<PartData> _inner_data;
    size_t _capacity;
};

struct DownloadFileBuffer final : public FileBuffer {
    DownloadFileBuffer(std::function<Status(Slice&)> download,
                       std::function<void(FileBlocksHolderPtr, Slice)> write_to_cache,
                       std::function<void(Slice, size_t)> write_to_use_buffer, OperationState state,
                       size_t offset, std::function<FileBlocksHolderPtr()> alloc_holder,
                       size_t capacity = 0)
            : FileBuffer(BufferType::DOWNLOAD, alloc_holder, offset, state, capacity),
              _download(std::move(download)),
              _write_to_local_file_cache(std::move(write_to_cache)),
              _write_to_use_buffer(std::move(write_to_use_buffer)) {}
    ~DownloadFileBuffer() override = default;
    /**
    * do the download work, it would write the content into local memory buffer
    */
    void on_download();
    void execute_async() override { on_download(); }
    Status append_data(const Slice& s) override { return Status::OK(); }

    std::function<Status(Slice&)> _download;
    std::function<void(FileBlocksHolderPtr, Slice)> _write_to_local_file_cache;
    std::function<void(Slice, size_t)> _write_to_use_buffer;
};

struct UploadFileBuffer final : public FileBuffer {
    UploadFileBuffer(std::function<void(UploadFileBuffer&)> upload_cb, OperationState state,
                     size_t offset, std::function<FileBlocksHolderPtr()> alloc_holder,
                     size_t capacity = 0)
            : FileBuffer(BufferType::UPLOAD, alloc_holder, offset, state, capacity),
              _upload_to_remote(std::move(upload_cb)) {}
    ~UploadFileBuffer() override = default;
    Status append_data(const Slice& s) override;
    /**
    * read the content from local file cache
    * because previously lack of  memory buffer
    */
    void read_from_cache();
    /**
    * write the content inside memory buffer into 
    * local file cache
    */
    void upload_to_local_file_cache(bool);

    // True once if on_upload left the file cache copy for the caller to submit to
    // S3FileCacheWriterThreadPool, which needs a reference that outlives the upload task.
    bool take_pending_file_cache_write() { return std::exchange(_file_cache_write_pending, false); }

    void execute_async() override { on_upload(); }
    /**
    * do the upload work
    * 1. read from cache if the data is written to cache first
    * 2. upload content of buffer to S3
    * 3. upload content to file cache if necessary
    * 4. call the finish callback caller specified
    * 5. reclaim self
    */
    void on_upload();
    /**
    *
    * @return the stream representing the inner memory buffer
    */
    std::shared_ptr<std::iostream> get_stream() const { return _stream_ptr; }

    /**
    * Currently only used for small file to set callback
    */
    void set_upload_to_remote(std::function<void(UploadFileBuffer&)> cb) {
        _upload_to_remote = std::move(cb);
    }

    // nullptr means ExecEnv's shared S3FileUploadThreadPool.
    void set_upload_thread_pool(ThreadPool* pool) { _upload_thread_pool = pool; }
    ThreadPool* upload_thread_pool() const { return _upload_thread_pool; }
    // CRC32C of the appended bytes; on_upload() verifies it against the buffer before
    // the upload callback runs, so the callback may send it as the body checksum.
    uint32_t crc32c() const { return _crc_value; }

private:
    ThreadPool* _upload_thread_pool = nullptr;
    std::function<void(UploadFileBuffer&)> _upload_to_remote = nullptr;
    std::shared_ptr<std::iostream> _stream_ptr; // point to _buffer.get_data()

    bool _is_cache_allocated {false};
    bool _file_cache_write_pending {false};
    FileBlocksHolderPtr _holder;
    decltype(_holder->file_blocks.begin()) _cur_file_block;
    size_t _append_offset {0};
    uint32_t _crc_value = 0;
};

struct FileBufferBuilder {
    FileBufferBuilder() = default;
    ~FileBufferBuilder() = default;
    /**
    * build one file buffer using previously set properties
    * @return the file buffer's base shared pointer
    */
    Status build(std::shared_ptr<FileBuffer>* buf);
    /**
    * set the file buffer type
    *
    * @param type enum class for buffer type
    */
    FileBufferBuilder& set_type(BufferType type);
    /**
    * set the download callback which would download the content on cloud into file buffer
    *
    * @param cb 
    */
    FileBufferBuilder& set_download_callback(std::function<Status(Slice&)> cb) {
        _download = std::move(cb);
        return *this;
    }
    /**
    * set the upload callback which would upload the content inside buffer into remote storage
    *
    * @param cb 
    */
    FileBufferBuilder& set_upload_callback(std::function<void(UploadFileBuffer& buf)> cb);
    /**
    * set the pool the upload task runs on; nullptr means the shared S3 upload pool
    */
    FileBufferBuilder& set_upload_thread_pool(ThreadPool* pool) {
        _upload_thread_pool = pool;
        return *this;
    }
    /**
    * set the callback which would do task sync for the caller
    *
    * @param cb 
    */
    FileBufferBuilder& set_sync_after_complete_task(std::function<bool(Status)> cb);
    /**
    * set the callback which detect whether the task is done
    *
    * @param cb 
    */
    FileBufferBuilder& set_is_cancelled(std::function<bool()> cb) {
        _is_cancelled = std::move(cb);
        return *this;
    }
    /**
    * set the callback which allocate file cache block holder
    * **Notice**: Because the load file cache workload coule be done
    * asynchronously so you must make sure all the dependencies of this
    * cb could last until this cb is invoked
    * @param cb 
    */
    FileBufferBuilder& set_allocate_file_blocks_holder(std::function<FileBlocksHolderPtr()> cb);
    /**
    * set the file offset of the file buffer
    *
    * @param cb 
    */
    FileBufferBuilder& set_file_offset(size_t offset) {
        _offset = offset;
        return *this;
    }
    // Capacity of the buffer's memory; 0 means config::s3_write_buffer_size at build time.
    FileBufferBuilder& set_buffer_size(size_t size) {
        _buffer_size = size;
        return *this;
    }
    /**
    * set the callback which write the content into local file cache
    *
    * @param cb 
    */
    FileBufferBuilder& set_write_to_local_file_cache(
            std::function<void(FileBlocksHolderPtr, Slice)> cb) {
        _write_to_local_file_cache = std::move(cb);
        return *this;
    }
    /**
    * set the callback which would write the downloaded content into user's buffer
    *
    * @param cb 
    */
    FileBufferBuilder& set_write_to_use_buffer(std::function<void(Slice, size_t)> cb) {
        _write_to_use_buffer = std::move(cb);
        return *this;
    }

    BufferType _type;
    std::function<void(UploadFileBuffer& buf)> _upload_cb = nullptr;
    std::function<bool(Status)> _sync_after_complete_task = nullptr;
    std::function<FileBlocksHolderPtr()> _alloc_holder_cb = nullptr;
    std::function<bool()> _is_cancelled = nullptr;
    std::function<void(FileBlocksHolderPtr, Slice)> _write_to_local_file_cache;
    std::function<Status(Slice&)> _download;
    std::function<void(Slice, size_t)> _write_to_use_buffer;
    size_t _offset;
    ThreadPool* _upload_thread_pool = nullptr;
    size_t _buffer_size = 0;
};
} // namespace io
} // namespace doris
