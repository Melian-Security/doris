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
#include <event2/http.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "io/fs/stream_load_pipe.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_client.h"
#include "service/http/http_handler.h"
#include "service/http/http_request.h"
#include "util/byte_buffer.h"
#include "util/defer_op.h"

namespace doris {

namespace {

ByteBufferPtr make_buffer(size_t size, char fill = 'x') {
    ByteBufferPtr buf;
    EXPECT_TRUE(ByteBuffer::allocate(size, &buf).ok());
    std::string data(size, fill);
    buf->put_bytes(data.data(), data.size());
    buf->flip();
    return buf;
}

// Reads `bytes` bytes from the pipe.
Status read_bytes(io::StreamLoadPipe* pipe, size_t bytes, std::string* out) {
    out->resize(bytes);
    size_t read = 0;
    return pipe->read_at(0, Slice(out->data(), bytes), &read);
}

struct LoadState {
    std::shared_ptr<io::StreamLoadPipe> pipe;
    std::shared_ptr<HttpBodyFlowControl> flow_control;
};

// Moves a request body into a pipe on the event loop like StreamLoadAction, either through
// HttpBodyFlowControl or with the blocking append. A separate consumer drains each pipe.
class PipeLoadHandler : public HttpHandler {
public:
    PipeLoadHandler(size_t pipe_bytes, bool flow_control)
            : _pipe_bytes(pipe_bytes), _flow_control(flow_control) {}

    bool request_will_be_read_progressively() override { return true; }

    int on_header(HttpRequest* req) override {
        auto state = std::make_shared<LoadState>();
        state->pipe = std::make_shared<io::StreamLoadPipe>(_pipe_bytes);
        if (_flow_control) {
            state->flow_control = HttpBodyFlowControl::create(req);
            EXPECT_NE(state->flow_control, nullptr);
            state->flow_control->attach_pipe(state->pipe.get());
        }
        req->set_handler_ctx(state);
        {
            std::lock_guard l(_mutex);
            _loads.push_back(state);
        }
        _cond.notify_all();
        return 0;
    }

    void on_chunk_data(HttpRequest* req) override {
        auto state = std::static_pointer_cast<LoadState>(req->handler_ctx());
        auto* evbuf = evhttp_request_get_input_buffer(req->get_evhttp_request());
        if (state->flow_control != nullptr) {
            int64_t moved = 0;
            EXPECT_TRUE(
                    state->flow_control->move_body_to_pipe(evbuf, state->pipe.get(), &moved).ok());
        } else {
            while (evbuffer_get_length(evbuf) > 0) {
                ByteBufferPtr bb;
                EXPECT_TRUE(ByteBuffer::allocate(128 * 1024, &bb).ok());
                auto removed = evbuffer_remove(evbuf, bb->ptr, bb->capacity);
                bb->pos = removed;
                bb->flip();
                EXPECT_TRUE(state->pipe->append(bb).ok());
            }
        }
        size_t buffered = state->pipe->current_capacity();
        size_t prev = max_buffered.load();
        while (buffered > prev && !max_buffered.compare_exchange_weak(prev, buffered)) {
        }
    }

    void handle(HttpRequest* req) override {
        auto state = std::static_pointer_cast<LoadState>(req->handler_ctx());
        if (state->flow_control != nullptr) {
            state->flow_control->body_complete();
        }
        EXPECT_TRUE(state->pipe->finish().ok());
        HttpChannel::send_reply(req, "loaded");
    }

    std::shared_ptr<LoadState> wait_for_load(size_t index) {
        std::unique_lock l(_mutex);
        _cond.wait_for(l, std::chrono::seconds(10), [&]() { return _loads.size() > index; });
        return _loads.size() > index ? _loads[index] : nullptr;
    }

    std::atomic<size_t> max_buffered {0};

private:
    size_t _pipe_bytes;
    bool _flow_control;
    std::mutex _mutex;
    std::condition_variable _cond;
    std::vector<std::shared_ptr<LoadState>> _loads;
};

class FastHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override { HttpChannel::send_reply(req, "fast"); }
};

std::string make_body(size_t size) {
    std::string body(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        body[i] = static_cast<char>('a' + (i * 7 + i / 4096) % 26);
    }
    return body;
}

// Serves one slow load and one fast request on a single event loop thread. The consumer of the
// load does not read until the fast request returned (or timed out). Returns whether the fast
// request was served while the load's pipe was full.
bool fast_request_served_while_pipe_full(bool flow_control) {
    constexpr size_t kPipeBytes = 256 * 1024;
    const std::string body = make_body(16 * 1024 * 1024);

    PipeLoadHandler load(kPipeBytes, flow_control);
    FastHandler fast;
    EvHttpServer server(0, 1);
    server.register_handler(POST, "/load", &load);
    server.register_handler(GET, "/fast", &fast);
    server.start();
    Defer stop_server {[&]() { server.stop(); }};
    const std::string base_url = "http://127.0.0.1:" + std::to_string(server.get_real_port());

    std::string load_response;
    Status load_status;
    std::thread load_client([&]() {
        HttpClient client;
        load_status = client.init(base_url + "/load");
        if (load_status.ok()) {
            client.set_timeout_ms(60000);
            load_status = client.execute_post_request(body, &load_response);
        }
    });

    auto state = load.wait_for_load(0);
    EXPECT_NE(state, nullptr);
    std::string received;
    std::promise<void> release;
    std::thread consumer([&, release_future = release.get_future()]() {
        release_future.wait();
        if (state == nullptr) {
            return;
        }
        std::string chunk;
        while (true) {
            size_t read = 0;
            chunk.resize(64 * 1024);
            if (!state->pipe->read_at(0, Slice(chunk.data(), chunk.size()), &read).ok() ||
                read == 0) {
                break;
            }
            received.append(chunk.data(), read);
        }
    });
    // Wait until the receiver filled the pipe; the blocking append then waits inside the event
    // loop thread for the consumer, while flow control has paused the read.
    for (int i = 0;
         i < 1000 && state != nullptr && state->pipe->current_capacity() <= kPipeBytes / 2; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_GT(state->pipe->current_capacity(), kPipeBytes / 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    if (flow_control) {
        EXPECT_GT(state->flow_control->pause_count(), 0);
    }

    HttpClient client;
    EXPECT_TRUE(client.init(base_url + "/fast").ok());
    client.set_method(GET);
    client.set_timeout_ms(2000);
    std::string fast_response;
    bool served = client.execute(&fast_response).ok() && fast_response == "fast";

    release.set_value();
    consumer.join();
    load_client.join();
    EXPECT_TRUE(load_status.ok()) << load_status;
    EXPECT_EQ(load_response, "loaded");
    EXPECT_EQ(received.size(), body.size());
    EXPECT_TRUE(received == body);
    if (flow_control) {
        EXPECT_GT(state->flow_control->pause_count(), 0);
        // Reads pause as soon as the pipe is full, so it overshoots by at most one read.
        EXPECT_LT(load.max_buffered.load(), kPipeBytes + 1024 * 1024);
    }
    return served;
}

} // namespace

TEST(StreamLoadPipeFlowControlTest, AppendWithoutWaitReportsFullAndSignalsAtHalf) {
    io::StreamLoadPipe pipe(1024);
    std::atomic<int> drained {0};
    pipe.set_drain_callback([&]() { drained++; });

    bool full = false;
    ASSERT_TRUE(pipe.append_without_wait(make_buffer(600), &full).ok());
    EXPECT_FALSE(full);
    ASSERT_TRUE(pipe.append_without_wait(make_buffer(600), &full).ok());
    EXPECT_TRUE(full);
    // Never waits, even far above the capacity.
    ASSERT_TRUE(pipe.append_without_wait(make_buffer(600), &full).ok());
    EXPECT_TRUE(full);
    EXPECT_EQ(pipe.current_capacity(), 1800);

    std::string out;
    // 1200 buffered: still above half.
    ASSERT_TRUE(read_bytes(&pipe, 600, &out).ok());
    EXPECT_EQ(drained.load(), 0);
    // 600 buffered: above half (512).
    ASSERT_TRUE(read_bytes(&pipe, 600, &out).ok());
    EXPECT_EQ(drained.load(), 0);
    ASSERT_TRUE(read_bytes(&pipe, 600, &out).ok());
    EXPECT_EQ(drained.load(), 1);

    // Signalled once per full append.
    ASSERT_TRUE(pipe.append_without_wait(make_buffer(100), &full).ok());
    EXPECT_FALSE(full);
    ASSERT_TRUE(read_bytes(&pipe, 100, &out).ok());
    EXPECT_EQ(drained.load(), 1);
}

TEST(StreamLoadPipeFlowControlTest, CancelSignalsAFullPipe) {
    io::StreamLoadPipe pipe(1024);
    std::atomic<int> drained {0};
    pipe.set_drain_callback([&]() { drained++; });
    bool full = false;
    ASSERT_TRUE(pipe.append_without_wait(make_buffer(2048), &full).ok());
    EXPECT_TRUE(full);
    pipe.cancel("test");
    EXPECT_EQ(drained.load(), 1);
    EXPECT_FALSE(pipe.append_without_wait(make_buffer(10), &full).ok());
}

TEST(HttpBodyFlowControlTest, FullPipeDoesNotStallOtherRequestsOnTheSameLoop) {
    EXPECT_TRUE(fast_request_served_while_pipe_full(true));
}

TEST(HttpBodyFlowControlTest, BlockingAppendStallsOtherRequestsOnTheSameLoop) {
    EXPECT_FALSE(fast_request_served_while_pipe_full(false));
}

} // namespace doris
