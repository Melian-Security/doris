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

#include "service/http/http_body_flow_control.h"

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/http.h>

#include <algorithm>

#include "common/logging.h"
#include "io/fs/stream_load_pipe.h"
#include "service/http/http_request.h"
#include "util/byte_buffer.h"

namespace doris {

namespace {

struct ResumeTask {
    std::weak_ptr<HttpBodyFlowControl> flow_control;
};

} // namespace

std::shared_ptr<HttpBodyFlowControl> HttpBodyFlowControl::create(HttpRequest* req) {
    if (req == nullptr || req->get_evhttp_request() == nullptr) {
        return nullptr;
    }
    evhttp_connection* conn = evhttp_request_get_connection(req->get_evhttp_request());
    event_base* base = conn == nullptr ? nullptr : evhttp_connection_get_base(conn);
    if (base == nullptr) {
        return nullptr;
    }
    return std::make_shared<HttpBodyFlowControl>(base, req->get_evhttp_request(),
                                                 req->lifetime_token());
}

void HttpBodyFlowControl::attach_pipe(io::StreamLoadPipe* pipe) {
    pipe->set_drain_callback([flow_control = weak_from_this()]() {
        if (auto self = flow_control.lock()) {
            self->resume();
        }
    });
}

Status HttpBodyFlowControl::move_body_to_pipe(evbuffer* evbuf, io::StreamLoadPipe* pipe,
                                              int64_t* moved_bytes) {
    *moved_bytes = 0;
    bool pipe_full = false;
    Status st = Status::OK();
    while (evbuffer_get_length(evbuf) > 0) {
        ByteBufferPtr bb;
        st = ByteBuffer::allocate(std::min<size_t>(evbuffer_get_length(evbuf), kMaxChunkBytes),
                                  &bb);
        if (!st.ok()) {
            break;
        }
        auto removed = evbuffer_remove(evbuf, bb->ptr, bb->capacity);
        if (removed <= 0) {
            break;
        }
        bb->pos = removed;
        bb->flip();
        bool full = false;
        st = pipe->append_without_wait(bb, &full);
        if (!st.ok()) {
            break;
        }
        pipe_full = pipe_full || full;
        *moved_bytes += removed;
    }
    if (pipe_full) {
        pause();
    }
    return st;
}

void HttpBodyFlowControl::_set_read_enabled(bool enabled) {
    evhttp_connection* conn = evhttp_request_get_connection(_ev_req);
    bufferevent* bev = conn == nullptr ? nullptr : evhttp_connection_get_bufferevent(conn);
    if (bev == nullptr) {
        return;
    }
    if (enabled) {
        bufferevent_enable(bev, EV_READ);
    } else {
        bufferevent_disable(bev, EV_READ);
    }
}

void HttpBodyFlowControl::pause() {
    if (_paused || _body_complete || _request_alive.expired()) {
        return;
    }
    _set_read_enabled(false);
    _paused = true;
    _pause_count.fetch_add(1, std::memory_order_relaxed);
}

void HttpBodyFlowControl::resume() {
    // The event bases are created after evthread_use_pthreads(), so event_base_once() may be
    // called from any thread and wakes the loop.
    auto* task = new ResumeTask {weak_from_this()};
    static const timeval kImmediately {0, 0};
    if (event_base_once(_base, -1, EV_TIMEOUT, _resume_on_loop, task, &kImmediately) != 0) {
        LOG(WARNING) << "failed to schedule resuming an http body read on its event loop";
        delete task;
    }
}

void HttpBodyFlowControl::_resume_on_loop(evutil_socket_t /*fd*/, short /*events*/, void* arg) {
    std::unique_ptr<ResumeTask> task(static_cast<ResumeTask*>(arg));
    if (auto self = task->flow_control.lock()) {
        self->resume_on_loop();
    }
}

void HttpBodyFlowControl::resume_on_loop() {
    // The request is freed on this thread, so it stays alive for the rest of this call.
    if (_request_alive.expired() || !_paused) {
        return;
    }
    _paused = false;
    if (!_body_complete) {
        _set_read_enabled(true);
    }
}

} // namespace doris
