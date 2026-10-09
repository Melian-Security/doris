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

#include <arpa/inet.h>
#include <gen_cpp/FrontendService_types.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_executor.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "service/http/action/stream_load.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_client.h"
#include "service/http/http_handler.h"
#include "service/http/http_request.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris {

extern TLoadTxnBeginResult k_stream_load_begin_result;
extern TLoadTxnCommitResult k_stream_load_commit_result;
extern TLoadTxnRollbackResult k_stream_load_rollback_result;
extern TStreamLoadPutResult k_stream_load_put_result;

namespace {

const std::string kBlockHeaderWork = "StreamLoadAction._begin_txn_and_plan.block";

class FastHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override { HttpChannel::send_reply(req, "fast"); }
};

bool wait_until(const std::function<bool()>& cond, int timeout_ms = 10000) {
    for (int waited = 0; waited < timeout_ms; waited += 10) {
        if (cond()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return cond();
}

// A raw client so the test controls when each part of the request reaches the server.
class RawClient {
public:
    explicit RawClient(int port) {
        _fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        _connected = connect(_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    }
    ~RawClient() { close(); }

    bool connected() const { return _connected; }

    bool send_all(const std::string& data) {
        size_t sent = 0;
        while (sent < data.size()) {
            auto n = ::send(_fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
            if (n <= 0) {
                return false;
            }
            sent += n;
        }
        return true;
    }

    // Returns false when nothing arrives within timeout_ms.
    bool readable(int timeout_ms) {
        pollfd pfd {_fd, POLLIN, 0};
        return poll(&pfd, 1, timeout_ms) > 0;
    }

    // Reads one response with a Content-Length body; empty on timeout or close.
    std::string read_response(int timeout_ms = 10000) {
        std::string data;
        size_t header_end = std::string::npos;
        size_t total = std::string::npos;
        while (total == std::string::npos || data.size() < total) {
            if (!readable(timeout_ms)) {
                return "";
            }
            char buf[4096];
            auto n = ::recv(_fd, buf, sizeof(buf), 0);
            if (n <= 0) {
                return "";
            }
            data.append(buf, n);
            if (header_end == std::string::npos &&
                (header_end = data.find("\r\n\r\n")) != std::string::npos) {
                auto pos = data.find("Content-Length: ");
                size_t length = pos == std::string::npos
                                        ? 0
                                        : std::stoul(data.substr(pos + strlen("Content-Length: ")));
                total = header_end + 4 + length;
            }
        }
        return data;
    }

    // True when the server closed the connection within timeout_ms.
    bool closed_by_peer(int timeout_ms = 5000) {
        if (!readable(timeout_ms)) {
            return false;
        }
        char buf[64];
        return ::recv(_fd, buf, sizeof(buf), 0) == 0;
    }

    void close() {
        if (_fd >= 0) {
            ::close(_fd);
            _fd = -1;
        }
    }

private:
    int _fd = -1;
    bool _connected = false;
};

std::string load_headers(const std::string& label, size_t body_bytes) {
    return "PUT /api/db1/tbl1/_stream_load HTTP/1.1\r\n"
           "Host: 127.0.0.1\r\n"
           "Authorization: Basic cm9vdDo=\r\n"
           "label: " +
           label + "\r\n" + "Content-Length: " + std::to_string(body_bytes) + "\r\n\r\n";
}

std::string make_body(size_t size) {
    std::string body;
    while (body.size() < size) {
        body += std::to_string(body.size()) + ",abc\n";
    }
    body.resize(size);
    return body;
}

bool fast_request_served(int port, int timeout_ms) {
    HttpClient client;
    if (!client.init("http://127.0.0.1:" + std::to_string(port) + "/fast").ok()) {
        return false;
    }
    client.set_method(GET);
    client.set_timeout_ms(timeout_ms);
    std::string response;
    return client.execute(&response).ok() && response == "fast";
}

int64_t begin_count() {
    return DorisMetrics::instance()->stream_load_txn_begin_request_total->value();
}

int64_t rollback_count() {
    return DorisMetrics::instance()->stream_load_txn_rollback_request_total->value();
}

} // namespace

class StreamLoadHeaderOffLoopTest : public testing::Test {
public:
    void SetUp() override {
        _saved_enable_debug_points = config::enable_debug_points;
        _saved_off_loop = config::enable_stream_load_header_off_event_loop;
        config::enable_debug_points = true;
        config::enable_stream_load_header_off_event_loop = true;

        auto* env = ExecEnv::GetInstance();
        _saved_cluster_info = env->cluster_info();
        _cluster_info.master_fe_addr.hostname = "127.0.0.1";
        _cluster_info.master_fe_addr.port = 9020;
        _cluster_info.backend_id = 10001;
        env->set_cluster_info(&_cluster_info);
        env->set_new_load_stream_mgr(NewLoadStreamMgr::create_unique());
        env->set_stream_load_executor(StreamLoadExecutor::create_unique(env));

        k_stream_load_begin_result = TLoadTxnBeginResult();
        k_stream_load_begin_result.__set_txnId(1);
        k_stream_load_commit_result = TLoadTxnCommitResult();
        k_stream_load_rollback_result = TLoadTxnRollbackResult();
        k_stream_load_put_result = TStreamLoadPutResult();
        k_stream_load_put_result.__isset.pipeline_params = true;

        _action = std::make_unique<StreamLoadAction>(env);
        _processing_base = _action->streaming_load_current_processing->value();
        _server = std::make_unique<EvHttpServer>(0, 1);
        _server->register_handler(PUT, "/api/{db}/{table}/_stream_load", _action.get());
        _server->register_handler(GET, "/fast", &_fast);
        _server->start();
        _port = _server->get_real_port();
    }

    void TearDown() override {
        DebugPoints::instance()->clear();
        _server->stop();
        _server.reset();
        _action.reset();
        auto* env = ExecEnv::GetInstance();
        env->clear_stream_load_executor();
        env->clear_new_load_stream_mgr();
        env->set_cluster_info(_saved_cluster_info);
        k_stream_load_put_result = TStreamLoadPutResult();
        k_stream_load_begin_result = TLoadTxnBeginResult();
        config::enable_debug_points = _saved_enable_debug_points;
        config::enable_stream_load_header_off_event_loop = _saved_off_loop;
    }

protected:
    // Loads in flight on this test's action.
    int64_t processing() const {
        return _action->streaming_load_current_processing->value() - _processing_base;
    }

    int64_t _processing_base = 0;

    int _port = 0;
    std::unique_ptr<StreamLoadAction> _action;
    std::unique_ptr<EvHttpServer> _server;
    FastHandler _fast;

private:
    bool _saved_enable_debug_points = false;
    bool _saved_off_loop = true;
    ClusterInfo* _saved_cluster_info = nullptr;
    ClusterInfo _cluster_info;
};

// Begins a load whose header work blocks after begin txn, sends part of its body, and checks
// whether another request on the same (single) event loop is served meanwhile.
TEST_F(StreamLoadHeaderOffLoopTest, BlockedHeaderWorkDoesNotStallOtherRequestsOnTheSameLoop) {
    const std::string body = make_body(64 * 1024);
    DebugPoints::instance()->add(kBlockHeaderWork);
    const int64_t begins = begin_count();

    RawClient load(_port);
    ASSERT_TRUE(load.connected());
    ASSERT_TRUE(load.send_all(load_headers("blocked_header", body.size()) + body.substr(0, 4096)));
    ASSERT_TRUE(wait_until([&]() { return begin_count() > begins; }));

    EXPECT_TRUE(fast_request_served(_port, 2000));

    // The rest of the body waits in the socket until the load is planned.
    ASSERT_TRUE(load.send_all(body.substr(4096)));
    EXPECT_FALSE(load.readable(200));

    DebugPoints::instance()->remove(kBlockHeaderWork);
    auto response = load.read_response();
    EXPECT_NE(response.find("\"Status\": \"Success\""), std::string::npos) << response;
    EXPECT_NE(response.find("\"LoadBytes\": 65536"), std::string::npos) << response;
    EXPECT_NE(response.find("\"HeaderWaitTimeMs\""), std::string::npos) << response;

    // The connection is kept alive and serves the next load.
    const std::string next = make_body(100);
    ASSERT_TRUE(load.send_all(load_headers("next_load", next.size()) + next));
    response = load.read_response();
    EXPECT_NE(response.find("\"Status\": \"Success\""), std::string::npos) << response;
    EXPECT_TRUE(wait_until([&]() { return processing() == 0; }));
}

TEST_F(StreamLoadHeaderOffLoopTest, BlockedHeaderWorkOnTheLoopStallsOtherRequests) {
    config::enable_stream_load_header_off_event_loop = false;
    DebugPoints::instance()->add(kBlockHeaderWork);
    const int64_t begins = begin_count();
    const std::string body = make_body(1024);

    RawClient load(_port);
    ASSERT_TRUE(load.connected());
    ASSERT_TRUE(load.send_all(load_headers("inline_header", body.size()) + body));
    ASSERT_TRUE(wait_until([&]() { return begin_count() > begins; }));

    EXPECT_FALSE(fast_request_served(_port, 1000));

    DebugPoints::instance()->remove(kBlockHeaderWork);
    auto response = load.read_response();
    EXPECT_NE(response.find("\"Status\": \"Success\""), std::string::npos) << response;
    EXPECT_EQ(response.find("HeaderWaitTimeMs"), std::string::npos) << response;
}

// The whole body arrives with the headers, so libevent finishes the request before the load is
// planned; the reply still waits for the plan and carries every body byte.
TEST_F(StreamLoadHeaderOffLoopTest, BodyCompletedBeforeThePlanIsLoaded) {
    const std::string body = make_body(1000);
    DebugPoints::instance()->add(kBlockHeaderWork);
    const int64_t begins = begin_count();

    RawClient load(_port);
    ASSERT_TRUE(load.connected());
    ASSERT_TRUE(load.send_all(load_headers("whole_body", body.size()) + body));
    ASSERT_TRUE(wait_until([&]() { return begin_count() > begins; }));
    EXPECT_FALSE(load.readable(200));

    DebugPoints::instance()->remove(kBlockHeaderWork);
    auto response = load.read_response();
    EXPECT_NE(response.find("\"Status\": \"Success\""), std::string::npos) << response;
    EXPECT_NE(response.find("\"LoadBytes\": 1000"), std::string::npos) << response;
}

TEST_F(StreamLoadHeaderOffLoopTest, PlanFailureRepliesAndRollsBack) {
    k_stream_load_put_result.status.__set_status_code(TStatusCode::INTERNAL_ERROR);
    k_stream_load_put_result.status.__set_error_msgs({"injected plan failure"});
    const std::string body = make_body(64 * 1024);
    const int64_t rollbacks = rollback_count();

    RawClient load(_port);
    ASSERT_TRUE(load.connected());
    ASSERT_TRUE(load.send_all(load_headers("plan_failure", body.size()) + body.substr(0, 1024)));
    auto response = load.read_response();
    EXPECT_NE(response.find("\"Status\": \"Fail\""), std::string::npos) << response;
    EXPECT_NE(response.find("injected plan failure"), std::string::npos) << response;
    EXPECT_EQ(rollback_count(), rollbacks + 1);
    // The unread rest of the body is not parsed as another request.
    EXPECT_TRUE(load.closed_by_peer());
    EXPECT_TRUE(wait_until([&]() { return processing() == 0; }));
    EXPECT_TRUE(fast_request_served(_port, 2000));
}

TEST_F(StreamLoadHeaderOffLoopTest, BeginTxnFailureReplies) {
    k_stream_load_begin_result.status.__set_status_code(TStatusCode::LABEL_ALREADY_EXISTS);
    k_stream_load_begin_result.status.__set_error_msgs({"injected label exists"});
    const std::string body = make_body(1000);
    const int64_t rollbacks = rollback_count();

    RawClient load(_port);
    ASSERT_TRUE(load.connected());
    ASSERT_TRUE(load.send_all(load_headers("begin_failure", body.size()) + body));
    auto response = load.read_response();
    EXPECT_NE(response.find("\"Status\": \"Label Already Exists\""), std::string::npos) << response;
    EXPECT_EQ(rollback_count(), rollbacks);
    EXPECT_TRUE(wait_until([&]() { return processing() == 0; }));
}

// The client goes away while the header work waits on FE. The connection is read again once the
// load is planned; the close then frees the request, which cancels the load and rolls back.
TEST_F(StreamLoadHeaderOffLoopTest, DisconnectDuringHeaderWorkCancelsTheLoad) {
    const std::string body = make_body(64 * 1024);
    DebugPoints::instance()->add(kBlockHeaderWork);
    const int64_t begins = begin_count();
    const int64_t rollbacks = rollback_count();

    {
        RawClient load(_port);
        ASSERT_TRUE(load.connected());
        ASSERT_TRUE(load.send_all(load_headers("disconnect", body.size()) + body.substr(0, 1024)));
        ASSERT_TRUE(wait_until([&]() { return begin_count() > begins; }));
        EXPECT_EQ(processing(), 1);
    }
    DebugPoints::instance()->remove(kBlockHeaderWork);

    EXPECT_TRUE(wait_until([&]() { return rollback_count() > rollbacks; }));
    EXPECT_TRUE(wait_until([&]() { return processing() == 0; }));
    EXPECT_TRUE(fast_request_served(_port, 2000));
}

// The client goes away mid-body after the header work finished: libevent frees the request, which
// cancels the load and rolls back.
TEST_F(StreamLoadHeaderOffLoopTest, DisconnectMidBodyFreesTheRequest) {
    const std::string body = make_body(64 * 1024);
    const int64_t rollbacks = rollback_count();
    {
        RawClient load(_port);
        ASSERT_TRUE(load.connected());
        ASSERT_TRUE(load.send_all(load_headers("mid_body", body.size()) + body.substr(0, 1024)));
        ASSERT_TRUE(wait_until([&]() { return processing() == 1; }));
        EXPECT_FALSE(load.readable(200));
    }
    EXPECT_TRUE(wait_until([&]() { return processing() == 0; }));
    EXPECT_TRUE(wait_until([&]() { return rollback_count() > rollbacks; }));
}

} // namespace doris
