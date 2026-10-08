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

#include <event2/util.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/status.h"

struct evbuffer;
struct event_base;
struct evhttp_request;

namespace doris {

class HttpRequest;
namespace io {
class StreamLoadPipe;
} // namespace io

// Pauses and resumes reading the body of one HTTP request on the event loop thread that owns its
// connection. Each event loop thread serves many connections, so a body consumer that falls behind
// must not block that thread: it pauses the read instead, the socket buffer fills and TCP flow
// control holds back the sender, and the other connections keep being served.
//
// create(), pause() and body_complete() run on the event loop thread. resume() may run on any
// thread; the read is re-enabled on the event loop thread, and only while the request is alive,
// paused, and its body is still being read (libevent aborts on a read in any other state).
class HttpBodyFlowControl : public std::enable_shared_from_this<HttpBodyFlowControl> {
public:
    // Returns nullptr when the request has no connection.
    static std::shared_ptr<HttpBodyFlowControl> create(HttpRequest* req);

    HttpBodyFlowControl(event_base* base, evhttp_request* ev_req, std::weak_ptr<void> alive)
            : _base(base), _ev_req(ev_req), _request_alive(std::move(alive)) {}

    // Resumes the read whenever `pipe` drains after an append found it full. Call before the first
    // move_body_to_pipe().
    void attach_pipe(io::StreamLoadPipe* pipe);

    // Event loop thread, from the request's chunk callback: moves every byte of `evbuf` into
    // `pipe` without waiting, and pauses the read when the pipe is full. libevent drops whatever
    // a chunk callback leaves in the request's input buffer, so nothing may stay behind.
    Status move_body_to_pipe(evbuffer* evbuf, io::StreamLoadPipe* pipe, int64_t* moved_bytes);

    void pause();
    void resume();
    // Event loop thread: re-enables the read before returning to the loop, unlike resume().
    void resume_on_loop();
    void body_complete() { _body_complete = true; }

    bool paused() const { return _paused; }
    int64_t pause_count() const { return _pause_count.load(std::memory_order_relaxed); }

private:
    static constexpr size_t kMaxChunkBytes = 128 * 1024;

    static void _resume_on_loop(evutil_socket_t fd, short events, void* arg);
    void _set_read_enabled(bool enabled);

    event_base* _base;
    evhttp_request* _ev_req;
    std::weak_ptr<void> _request_alive;
    // Event loop thread only.
    bool _paused = false;
    bool _body_complete = false;
    std::atomic<int64_t> _pause_count {0};
};

} // namespace doris
