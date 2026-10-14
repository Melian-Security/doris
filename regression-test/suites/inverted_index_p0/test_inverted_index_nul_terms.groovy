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

// Values holding a NUL byte are left out of an untokenized inverted index. Every predicate that
// an index can serve must return the same rows with and without the index.
suite("test_inverted_index_nul_terms", "p0") {
    def tableName = "test_inverted_index_nul_terms"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `id` int NULL,
            `s` string NULL,
            `t` string NULL,
            `arr` array<string> NULL,
            `v` variant NULL,
            INDEX s_idx (`s`) USING INVERTED COMMENT '',
            INDEX t_idx (`t`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
            INDEX arr_idx (`arr`) USING INVERTED COMMENT '',
            INDEX v_idx (`v`) USING INVERTED COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // rows: 1 "ok", 2 "ab\0cd", 3 "a\0\0b", 4 "trail\0", 5 "", 6 NULL, 7 "trail", 8 "ab"
    streamLoad {
        table "${tableName}"
        set 'read_json_by_line', 'true'
        set 'format', 'json'
        file 'test_inverted_index_nul_terms.json'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(8, json.NumberLoadedRows)
        }
    }
    sql "sync"

    // The NUL bytes reach storage unchanged.
    def hexes = sql "SELECT id, hex(s) FROM ${tableName} ORDER BY id"
    assertEquals([[1, "6F6B"], [2, "6162006364"], [3, "61000062"], [4, "747261696C00"], [5, ""],
                  [6, null], [7, "747261696C"], [8, "6162"]], hexes)

    sql "set enable_common_expr_pushdown = true"
    sql "set enable_match_without_inverted_index = true"

    def ids = { String predicate, boolean useIndex ->
        sql "set enable_inverted_index_query = ${useIndex}"
        return sql("SELECT id FROM ${tableName} WHERE ${predicate} ORDER BY id").collect { it[0] }
    }
    // Same rows with and without the index, and the expected rows.
    def check = { String predicate, List expected ->
        def withIndex = ids(predicate, true)
        def withoutIndex = ids(predicate, false)
        assertEquals(withoutIndex, withIndex, "index changes the result of: ${predicate}")
        assertEquals(expected, withIndex, "unexpected rows for: ${predicate}")
    }

    // Equality on the untokenized index.
    check("s = 'ok'", [1])
    check("s = 'ab\\0cd'", [2])
    check("s = 'a\\0\\0b'", [3])
    check("s = 'trail\\0'", [4])
    check("s = 'trail'", [7])
    check("s = 'ab'", [8])
    check("s = ''", [5])
    check("s = unhex('6162006364')", [2])

    // IN and NOT IN, NOT EQUAL: rows left out of the index are not NULL.
    check("s IN ('ok', 'ab\\0cd')", [1, 2])
    check("s IN ('ok', 'trail')", [1, 7])
    check("s NOT IN ('ok', 'zz')", [2, 3, 4, 5, 7, 8])
    check("s NOT IN ('ok', 'ab\\0cd')", [3, 4, 5, 7, 8])
    check("s != 'ok'", [2, 3, 4, 5, 7, 8])
    check("s != 'ab\\0cd'", [1, 3, 4, 5, 7, 8])
    check("s = 'ok' OR s = 'ab\\0cd'", [1, 2])
    check("NOT (s = 'trail')", [1, 2, 3, 4, 5, 8])

    // Array membership.
    check("array_contains(arr, 'ok')", [1, 3])
    check("array_contains(arr, 'ab')", [8])
    check("array_contains(arr, 'trail')", [7])
    check("array_contains(arr, 'ab\\0cd')", [2, 8])
    check("array_contains(arr, 'trail\\0')", [4])
    check("NOT array_contains(arr, 'ok')", [2, 4, 5, 7, 8])

    // VARIANT path equality.
    check("cast(v['k'] as string) = 'ok'", [1])
    check("cast(v['k'] as string) = 'ab\\0cd'", [2])
    check("cast(v['k'] as string) = 'trail'", [7])
    check("cast(v['k'] as string) != 'ok'", [2, 3, 4, 5, 7, 8])

    // Patterns: LIKE is never served by the index; match_phrase_prefix and match_regexp on an
    // untokenized index are evaluated on the data.
    check("s LIKE 'ab%'", [2, 8])
    check("s LIKE 'tr%'", [4, 7])
    check("s MATCH_PHRASE_PREFIX 'ab'", [2, 8])
    check("s MATCH_PHRASE_PREFIX 'tr'", [4, 7])
    check("s MATCH_REGEXP '^tr'", [4, 7])
    check("s MATCH_ANY 'ab\\0cd'", [2])
    check("s MATCH_ANY 'trail'", [7])

    // Range predicates on an untokenized string index are not served by the index.
    check("s > 'trail'", [4])
    check("s >= 'ab'", [1, 2, 4, 7, 8])

    // Tokenized index: a NUL byte separates tokens.
    check("t MATCH_ANY 'hello'", [1, 3])
    check("t MATCH_ANY 'world'", [3])
    check("t MATCH_PHRASE 'hello world'", [3])
    check("t MATCH_ANY 'trail'", [4, 7])
    check("t MATCH_ANY 'ab\\0cd'", [2, 8])

    sql "set enable_inverted_index_query = true"
}
