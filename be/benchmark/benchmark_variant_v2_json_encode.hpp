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

// Measures DataTypeVariantV2SerDe::read_column_from_arrow on a utf8 Arrow array of OCSF-shaped
// JSON events (about 150 leaf paths each), the JSON -> VARIANT V2 encoding step of an Arrow Stream
// Load. The benchmark is single-threaded, so items_per_second is rows per core per second.
// fast_path toggles config::variant_v2_json_encoder_fast_path (0 = general path, 1 = fast path).
// varied:0 repeats one document shape; varied:1 adds optional members, variable-length arrays of
// objects, a second key order and row-unique keys, the shape mix of a real OCSF feed.

#pragma once

#include <arrow/array/array_binary.h>
#include <arrow/array/builder_binary.h>
#include <benchmark/benchmark.h>
#include <cctz/time_zone.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

#include "common/config.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"

namespace doris {
namespace variant_v2_json_encode_bench {

inline std::string endpoint(size_t row, const char* host) {
    const std::string octet = std::to_string(row % 250 + 1);
    return R"({"hostname":")" + std::string(host) + std::to_string(row % 40) +
           R"(.corp.example.com","ip":"10.)" + std::to_string(row % 16) + ".1." + octet +
           R"(","port":)" + std::to_string(1024 + row % 60000) +
           R"(,"location":{"city":"Tel Aviv","country":"IL","lat":32.0853,"lon":34.7818})" +
           R"(,"interface_name":"eth0","mac":"00:1a:2b:3c:4d:)" + std::to_string(10 + row % 80) +
           R"(","svc_name":"https"})";
}

inline std::string ocsf_event(size_t row, bool varied) {
    const std::string r = std::to_string(row);
    std::string json = "{";
    if (varied && row % 5 == 3) {
        json += R"("time":)" + std::to_string(1759752000000 + row) + R"(,"class_uid":3002)";
    } else {
        json += R"("class_uid":3002,"time":)" + std::to_string(1759752000000 + row);
    }
    json += R"(,"activity_id":1,"activity_name":"Logon","category_uid":3,)"
            R"("category_name":"Identity & Access Management","class_name":"Authentication",)"
            R"("type_uid":300201,"type_name":"Authentication: Logon","severity_id":)" +
            std::to_string(row % 6) + R"(,"severity":"Informational","status_id":)" +
            std::to_string(1 + row % 2) + R"(,"status":"Success","status_code":"0x0",)"
            R"("status_detail":"The user logged on successfully","is_mfa":)" +
            std::string(row % 3 == 0 ? "true" : "false") +
            R"(,"auth_protocol_id":2,"auth_protocol":"Kerberos","logon_type_id":3,)"
            R"("logon_type":"Network","message":"An account was successfully logged on. Event )" +
            r + R"(","count":1,"duration":)" + std::to_string(row % 1000) +
            R"(,"timezone_offset":0,"raw_data_size":)" + std::to_string(1200 + row % 300);
    json += R"(,"metadata":{"product":{"name":"Windows Security","vendor_name":"Microsoft",)"
            R"("version":"10.0.20348","feature":{"name":"Security Auditing","uid":"4624"}},)"
            R"("uid":"evt-)" +
            r + R"(","version":"1.3.0","log_name":"Security","log_provider":"Microsoft-Windows-)"
                R"(Security-Auditing","logged_time":)" +
            std::to_string(1759752000500 + row) + R"(,"original_time":"2026-10-06T12:00:00Z",)"
                                                   R"("event_code":"4624","profiles":["host","user")" +
            std::string(varied && row % 4 == 0 ? R"(,"cloud")" : "") +
            R"(],"labels":["prod","eu1"],"tenant_uid":"t-)" + std::to_string(row % 8) + R"("})";
    if (varied && row % 7 == 2) {
        json += R"(,"enrichments":[{"name":"ip","value":"10.0.0.1","type":"location",)"
                R"("data":{"asn":64512,"org":"corp"}}])";
    }
    json += R"(,"actor":{"user":{"name":"user)" + std::to_string(row % 500) +
            R"(","uid":"S-1-5-21-1004336348-1177238915-682003330-)" + std::to_string(row % 500) +
            R"(","domain":"CORP","email_addr":"user)" + std::to_string(row % 500) +
            R"(@example.com","type_id":1,"type":"User","groups":[)";
    const size_t groups = varied ? row % 4 : 2;
    for (size_t group = 0; group < groups; ++group) {
        json += (group == 0 ? "" : ",");
        json += R"({"name":"group)" + std::to_string(group) + R"(","uid":"g-)" +
                std::to_string(group) + R"("})";
    }
    json += R"(]},"session":{"uid":"0x)" + std::to_string(100000 + row) +
            R"(","created_time":)" + std::to_string(1759751990000 + row) +
            R"(,"is_remote":true,"issuer":"kdc01"},"process":{"pid":)" +
            std::to_string(4000 + row % 4000) +
            R"(,"name":"lsass.exe","file":{"path":"C:\\Windows\\System32\\lsass.exe",)"
            R"("name":"lsass.exe","type_id":1}}})";
    if (varied && row % 3 == 1) {
        json += R"(,"proxy_endpoint":)" + endpoint(row, "proxy");
    }
    json += R"(,"src_endpoint":)" + endpoint(row, "ws") + R"(,"dst_endpoint":)" +
            endpoint(row + 7, "dc");
    json += R"(,"device":{"hostname":"dc)" + std::to_string(row % 4) +
            R"(.corp.example.com","ip":"10.0.0.)" + std::to_string(row % 4 + 1) +
            R"(","type_id":2,"type":"Server","os":{"name":"Windows Server 2022","type_id":100,)"
            R"("version":"10.0.20348"},"owner":{"name":"it-ops","uid":"o-1"},"uid":"dev-)" +
            std::to_string(row % 4) + R"("})";
    json += R"(,"user":{"name":"svc)" + std::to_string(row % 50) + R"(","uid":"u-)" +
            std::to_string(row % 50) +
            R"(","domain":"CORP","type_id":1,"type":"User","account":{"name":"acct)" +
            std::to_string(row % 50) + R"(","type_id":2,"uid":"a-)" + std::to_string(row % 50) +
            R"("},"org":{"name":"corp","ou_name":"eng","uid":"org-1"}})";
    json += R"(,"service":{"name":"krbtgt","uid":"s-1","version":"1"},)"
            R"("policy":{"name":"default","uid":"pol-1","is_applied":true},)"
            R"("cloud":{"provider":"AWS","region":"eu-west-1","zone":"eu-west-1a",)"
            R"("account":{"uid":"123456789012","name":"prod"}},)"
            R"("api":{"operation":"AssumeRole","version":"2011-06-15",)"
            R"("service":{"name":"sts.amazonaws.com"},"request":{"uid":"req-)" +
            r + R"("},"response":{"code":200,"message":"OK"}})";
    json += R"J(,"http_request":{"http_method":"POST","user_agent":"Mozilla/5.0 (Windows NT 10.0; )J"
            R"J(Win64; x64)","version":"1.1","url":{"scheme":"https","hostname":"login.example.com",)J"
            R"J("port":443,"path":"/oauth2/token","query_string":"client_id=)J" +
            std::to_string(row % 50) + R"("},"http_headers":[)";
    const size_t headers = varied ? 1 + row % 3 : 2;
    for (size_t header = 0; header < headers; ++header) {
        json += (header == 0 ? "" : ",");
        json += R"({"name":"x-h)" + std::to_string(header) + R"(","value":"v)" + r + R"("})";
    }
    json += "]}";
    json += R"(,"observables":[)";
    const size_t observables = varied ? row % 6 : 3;
    for (size_t index = 0; index < observables; ++index) {
        json += (index == 0 ? "" : ",");
        json += R"({"name":"src_endpoint.ip","type_id":2,"type":"IP Address","value":"10.1.)" +
                std::to_string(index) + "." + std::to_string(row % 250) + R"(")";
        if (varied && index == 1) {
            json += R"(,"reputation":{"score_id":3,"score":"Suspicious","provider":"intel"})";
        }
        json += "}";
    }
    json += "]";
    json += R"(,"unmapped":{"LogonGuid":"{00000000-0000-0000-0000-000000000000}",)"
            R"("KeyLength":"0","TransmittedServices":"-","LmPackageName":"-",)"
            R"("ImpersonationLevel":"%%1833","RestrictedAdminMode":"-","VirtualAccount":"%%1843",)"
            R"("ElevatedToken":"%%1842","TargetLinkedLogonId":"0x0")";
    if (varied && row % 11 == 0) {
        json += R"(,"row_)" + r + R"(":")" + r + R"(")";
    }
    json += "}}";
    return json;
}

inline void throw_if_not_ok(const arrow::Status& status) {
    if (!status.ok()) {
        throw std::runtime_error(status.ToString());
    }
}

struct ArrowEvents {
    std::shared_ptr<arrow::Array> array;
    int64_t json_bytes = 0;
};

inline ArrowEvents arrow_events(size_t rows, bool varied) {
    arrow::StringBuilder builder;
    ArrowEvents events;
    for (size_t row = 0; row < rows; ++row) {
        const std::string json = ocsf_event(row, varied);
        events.json_bytes += static_cast<int64_t>(json.size());
        throw_if_not_ok(builder.Append(json));
    }
    throw_if_not_ok(builder.Finish(&events.array));
    return events;
}

class ScopedJsonEncoderFastPath {
public:
    explicit ScopedJsonEncoderFastPath(bool enabled)
            : _old_value(config::variant_v2_json_encoder_fast_path) {
        config::variant_v2_json_encoder_fast_path = enabled;
    }
    ~ScopedJsonEncoderFastPath() { config::variant_v2_json_encoder_fast_path = _old_value; }

    ScopedJsonEncoderFastPath(const ScopedJsonEncoderFastPath&) = delete;
    ScopedJsonEncoderFastPath& operator=(const ScopedJsonEncoderFastPath&) = delete;

private:
    bool _old_value;
};

} // namespace variant_v2_json_encode_bench

static void BM_VariantV2JsonEncodeFromArrow(benchmark::State& state) {
    const auto rows = static_cast<size_t>(state.range(0));
    const bool fast_path = state.range(1) != 0;
    const bool varied = state.range(2) != 0;
    const variant_v2_json_encode_bench::ArrowEvents events =
            variant_v2_json_encode_bench::arrow_events(rows, varied);
    const DataTypeVariantV2 type;
    const DataTypeSerDeSPtr serde = type.get_serde();
    const cctz::time_zone utc = cctz::utc_time_zone();
    variant_v2_json_encode_bench::ScopedJsonEncoderFastPath scope(fast_path);
    for (auto _ : state) {
        MutableColumnPtr column = type.create_column();
        const Status status = serde->read_column_from_arrow(*column, events.array.get(), 0,
                                                            static_cast<int64_t>(rows), utc);
        if (!status.ok()) {
            state.SkipWithError(status.to_string().c_str());
            break;
        }
        benchmark::DoNotOptimize(column);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * events.json_bytes);
    state.counters["json_bytes_per_row"] =
            static_cast<double>(events.json_bytes) / static_cast<double>(rows);
}

BENCHMARK(BM_VariantV2JsonEncodeFromArrow)
        ->ArgNames({"rows", "fast_path", "varied"})
        ->ArgsProduct({{4096}, {0, 1}, {0, 1}})
        ->Unit(benchmark::kMillisecond);

} // namespace doris
