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

#include <event2/http.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_client.h"
#include "service/http/http_handler.h"
#include "service/http/http_request.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris {

namespace {

// Blocks off the event loop until released, then replies "slow".
class SlowHandler : public HttpHandler {
public:
    explicit SlowHandler(ThreadPool* pool) : _pool(pool) {}

    void handle(HttpRequest* req) override {
        bool scheduled = run_off_event_loop(
                req, _pool,
                [this]() {
                    started.set_value();
                    release.wait();
                },
                [req]() { HttpChannel::send_reply(req, "slow"); });
        ASSERT_TRUE(scheduled);
    }

    std::promise<void> started;
    std::shared_future<void> release;

private:
    ThreadPool* _pool;
};

class FastHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override { HttpChannel::send_reply(req, "fast"); }
};

} // namespace

TEST(OffEventLoopTest, BlockedHandlerDoesNotStallOtherRequestsOnTheSameLoop) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("OffLoopTest").set_max_threads(2).build(&pool).ok());

    std::promise<void> release;
    SlowHandler slow(pool.get());
    slow.release = release.get_future().share();
    auto started = slow.started.get_future();
    FastHandler fast;

    // One worker: both requests are served by the same event loop thread.
    EvHttpServer server(0, 1);
    server.register_handler(GET, "/slow", &slow);
    server.register_handler(GET, "/fast", &fast);
    server.start();
    Defer stop_server {[&]() { server.stop(); }};
    const std::string base_url = "http://127.0.0.1:" + std::to_string(server.get_real_port());

    std::atomic<bool> released {false};
    auto do_release = [&]() {
        if (!released.exchange(true)) {
            release.set_value();
        }
    };
    Defer release_on_exit {[&]() { do_release(); }};

    std::string slow_response;
    Status slow_status;
    std::thread slow_client([&]() {
        HttpClient client;
        slow_status = client.init(base_url + "/slow");
        if (slow_status.ok()) {
            client.set_method(GET);
            client.set_timeout_ms(30000);
            slow_status = client.execute(&slow_response);
        }
    });
    Defer join_slow {[&]() {
        do_release();
        if (slow_client.joinable()) {
            slow_client.join();
        }
    }};

    ASSERT_EQ(started.wait_for(std::chrono::seconds(10)), std::future_status::ready);

    // The slow request is still waiting for its work; the event loop must still serve others.
    HttpClient client;
    ASSERT_TRUE(client.init(base_url + "/fast").ok());
    client.set_method(GET);
    client.set_timeout_ms(5000);
    std::string fast_response;
    auto fast_status = client.execute(&fast_response);
    EXPECT_TRUE(fast_status.ok()) << fast_status;
    EXPECT_EQ(fast_response, "fast");

    do_release();
    slow_client.join();
    EXPECT_TRUE(slow_status.ok()) << slow_status;
    EXPECT_EQ(slow_response, "slow");
}

TEST(OffEventLoopTest, RequestWithoutConnectionIsNotScheduled) {
    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("OffLoopTest").set_max_threads(1).build(&pool).ok());
    auto* ev_req = evhttp_request_new(nullptr, nullptr);
    {
        HttpRequest req(ev_req);
        bool ran = false;
        EXPECT_FALSE(run_off_event_loop(&req, pool.get(), [&]() { ran = true; }, []() {}));
        pool->wait();
        EXPECT_FALSE(ran);
        EXPECT_FALSE(run_off_event_loop(&req, nullptr, []() {}, []() {}));
    }
    evhttp_request_free(ev_req);
}

} // namespace doris
