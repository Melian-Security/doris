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

import org.apache.doris.regression.action.ProfileAction

// Values holding a NUL byte cannot be stored as untokenized inverted index terms, so they are left
// out of the index (a tokenized index analyzes a NUL as a space). Every predicate the index can
// answer exactly must return the same rows with and without the index, before and after full
// compaction and after BUILD INDEX. Index-served prefix and regexp queries only see indexed
// terms, the same limitation as ignore_above.
suite("test_inverted_index_nul_terms", "p0") {
    def tableName = "test_inverted_index_nul_terms"
    def buildTableName = "test_inverted_index_nul_terms_build"

    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS nul_terms_ngram3_tokenizer
        PROPERTIES ("type" = "ngram", "min_gram" = "3", "max_gram" = "3");
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS nul_terms_ngram3
        PROPERTIES ("tokenizer" = "nul_terms_ngram3_tokenizer");
    """

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `id` int NULL,
            `s` string NULL,
            `t` string NULL,
            `g` string NULL,
            `arr` array<string> NULL,
            `v` variant<MATCH_NAME 'k' : string, PROPERTIES("variant_max_subcolumns_count" = "0")> NULL,
            INDEX s_idx (`s`) USING INVERTED,
            INDEX t_idx (`t`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
            INDEX g_idx (`g`) USING INVERTED PROPERTIES("analyzer" = "nul_terms_ngram3", "support_phrase" = "true"),
            INDEX arr_idx (`arr`) USING INVERTED,
            INDEX v_idx (`v`) USING INVERTED PROPERTIES("field_pattern" = "k")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        );
    """

    def load = { String targetTable, String dataFile, int rows ->
        streamLoad {
            table targetTable
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            // Array elements arrive hex-encoded, so their NUL bytes do not depend on how the JSON
            // reader unescapes strings nested in an array.
            set 'columns', "id, s, t, g, v, arr_hex, arr = if(arr_hex = '', cast([] as array<string>), " +
                    "array_map(x -> unhex(x), split_by_string(arr_hex, ',')))"
            file dataFile
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(rows, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
    }
    // Two loads, so the table has two rowsets for full compaction to merge.
    // rows 1 "ok", 2 "ab\0cd", 3 "a\0\0b", 4 "trail\0", 5 "", 6 NULL, 7 "trail", 8 "ab",
    //      12 "zz" | 9 "\0lead", 10 "\0\0", 11 "ok"
    // "zz" keeps "trail\0" from being a segment's maximum: a string zone-map bound drops trailing
    // NUL bytes, which prunes the segment for `= 'trail\0'` with or without an inverted index.
    load(tableName, 'test_inverted_index_nul_terms.json', 9)
    load(tableName, 'test_inverted_index_nul_terms_2.json', 3)
    sql "sync"

    // A string literal '\0' is a NUL byte.
    assertEquals([["6162006364"]], sql("SELECT hex('ab\\0cd')"))

    // The NUL bytes reach storage unchanged.
    def expectedHex = [[1, "6F6B"], [2, "6162006364"], [3, "61000062"], [4, "747261696C00"],
                       [5, ""], [6, null], [7, "747261696C"], [8, "6162"], [9, "006C656164"],
                       [10, "0000"], [11, "6F6B"], [12, "7A7A"]]
    assertEquals(expectedHex, sql("SELECT id, hex(s) FROM ${tableName} ORDER BY id"))
    assertEquals(expectedHex, sql("SELECT id, hex(cast(v['k'] as string)) FROM ${tableName} ORDER BY id"))
    qt_hex "SELECT id, hex(s), hex(t), hex(g), array_map(x -> hex(x), arr), hex(cast(v['k'] as string)) FROM ${tableName} ORDER BY id"
    // Rows whose s holds a NUL byte, from the stored bytes.
    def nulIds = expectedHex.findAll { row ->
        def h = row[1]
        h != null && (0..<(h.length() / 2)).any { h.substring(it * 2, it * 2 + 2) == "00" }
    }.collect { it[0] }
    assertEquals([2, 3, 4, 9, 10], nulIds)

    sql "set enable_common_expr_pushdown = true"
    sql "set enable_match_without_inverted_index = true"

    def ids = { String table, String predicate, boolean useIndex ->
        sql "set enable_inverted_index_query = ${useIndex}"
        return sql("SELECT id FROM ${table} WHERE ${predicate} ORDER BY id").collect { it[0] }
    }
    // The index must not change the result; the rows are recorded in the .out file.
    def same = { String table, String predicate ->
        def withIndex = ids(table, predicate, true)
        def withoutIndex = ids(table, predicate, false)
        assertEquals(withoutIndex, withIndex, "index changes the result of: ${predicate}")
        quickTest("same", "SELECT id FROM ${table} WHERE ${predicate} ORDER BY id")
        return withIndex
    }
    // Index-served prefix and regexp queries miss exactly the NUL-holding rows the data matches.
    def limitation = { String table, String predicate ->
        def withIndex = ids(table, predicate, true)
        def withoutIndex = ids(table, predicate, false)
        assertTrue(withoutIndex.any { nulIds.contains(it) }, "no NUL row matches: ${predicate}")
        assertEquals(withoutIndex.findAll { !nulIds.contains(it) }, withIndex,
                "index-served rows for: ${predicate}")
        sql "set enable_inverted_index_query = true"
        quickTest("limitation_index", "SELECT id FROM ${table} WHERE ${predicate} ORDER BY id")
        sql "set enable_inverted_index_query = false"
        quickTest("limitation_data", "SELECT id FROM ${table} WHERE ${predicate} ORDER BY id")
        sql "set enable_inverted_index_query = true"
    }

    def scalarChecks = { String table ->
        assertEquals([1, 11], same(table, "s = 'ok'"))
        assertEquals([2], same(table, "s = 'ab\\0cd'"))
        assertEquals([3], same(table, "s = 'a\\0\\0b'"))
        assertEquals([4], same(table, "s = 'trail\\0'"))
        assertEquals([9], same(table, "s = '\\0lead'"))
        assertEquals([10], same(table, "s = '\\0\\0'"))
        assertEquals([7], same(table, "s = 'trail'"))
        assertEquals([8], same(table, "s = 'ab'"))
        assertEquals([5], same(table, "s = ''"))
        assertEquals([2], same(table, "s = unhex('6162006364')"))
        assertEquals([2, 3, 4, 5, 7, 8, 9, 10, 12], same(table, "s != 'ok'"))
        assertEquals([1, 3, 4, 5, 7, 8, 9, 10, 11, 12], same(table, "s != 'ab\\0cd'"))
        assertEquals([1, 2, 11], same(table, "s IN ('ok', 'ab\\0cd')"))
        assertEquals([1, 7, 11], same(table, "s IN ('ok', 'trail')"))
        assertEquals([2, 3, 4, 5, 7, 8, 9, 10], same(table, "s NOT IN ('ok', 'zz')"))
        assertEquals([3, 4, 5, 7, 8, 9, 10, 12], same(table, "s NOT IN ('ok', 'ab\\0cd')"))
        assertEquals([6], same(table, "s IS NULL"))
        assertEquals([1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12], same(table, "s IS NOT NULL"))
        assertEquals([1, 2, 11], same(table, "s = 'ok' OR s = 'ab\\0cd'"))
        assertEquals([1, 2, 3, 4, 5, 8, 9, 10, 11, 12], same(table, "NOT (s = 'trail')"))
        // String ranges are not served by an untokenized index.
        assertEquals([4, 12], same(table, "s > 'trail'"))
        assertEquals([1, 2, 4, 7, 8, 11, 12], same(table, "s >= 'ab'"))
        assertEquals([2, 3, 5, 8, 9, 10], same(table, "s < 'b'"))
        assertEquals([2, 3, 8], same(table, "s BETWEEN 'a' AND 'b'"))
        // LIKE is not served by the index.
        assertEquals([2, 8], same(table, "s LIKE 'ab%'"))
        assertEquals([4, 7], same(table, "s LIKE 'tr%'"))
        assertEquals([2], same(table, "s MATCH_ANY 'ab\\0cd'"))
        assertEquals([7], same(table, "s MATCH_ANY 'trail'"))
        limitation(table, "s MATCH_PHRASE_PREFIX 'ab'")
        limitation(table, "s MATCH_PHRASE_PREFIX 'tr'")
        limitation(table, "s MATCH_REGEXP '^tr'")

        assertEquals([1, 3, 9, 11], same(table, "array_contains(arr, 'ok')"))
        assertEquals([8], same(table, "array_contains(arr, 'ab')"))
        assertEquals([7], same(table, "array_contains(arr, 'trail')"))
        assertEquals([2, 8], same(table, "array_contains(arr, 'ab\\0cd')"))
        assertEquals([4], same(table, "array_contains(arr, 'trail\\0')"))
        assertEquals([9], same(table, "array_contains(arr, '\\0lead')"))
        assertEquals([10], same(table, "array_contains(arr, '\\0\\0')"))
        assertEquals([2, 4, 5, 7, 8, 10, 12], same(table, "NOT array_contains(arr, 'ok')"))
        assertEquals([6], same(table, "arr IS NULL"))
    }

    def allChecks = { String table ->
        scalarChecks(table)

        // VARIANT path predicates.
        assertEquals([1, 11], same(table, "cast(v['k'] as string) = 'ok'"))
        assertEquals([2], same(table, "cast(v['k'] as string) = 'ab\\0cd'"))
        assertEquals([10], same(table, "cast(v['k'] as string) = '\\0\\0'"))
        assertEquals([7], same(table, "cast(v['k'] as string) = 'trail'"))
        assertEquals([2, 3, 4, 5, 7, 8, 9, 10, 12], same(table, "cast(v['k'] as string) != 'ok'"))
        assertEquals([1, 2, 11], same(table, "cast(v['k'] as string) IN ('ok', 'ab\\0cd')"))

        // Tokenized indexes analyze a NUL byte as a space.
        assertEquals([1, 3], same(table, "t MATCH_ANY 'hello'"))
        assertEquals([3], same(table, "t MATCH_ANY 'world'"))
        assertEquals([3], same(table, "t MATCH_PHRASE 'hello world'"))
        assertEquals([3], same(table, "t MATCH_PHRASE 'hello\\0world'"))
        assertEquals([4, 7], same(table, "t MATCH_ANY 'trail'"))
        assertEquals([2, 8], same(table, "t MATCH_ANY 'ab\\0cd'"))
        assertEquals([9], same(table, "t MATCH_ANY 'lead'"))
        same(table, "g MATCH_ANY 'ab\\0cd'")
        same(table, "g MATCH_ANY 'lo\\0wo'")
        same(table, "g MATCH_PHRASE 'hello\\0world'")
        assertEquals([4, 7], same(table, "g MATCH_PHRASE 'trail'"))
    }

    allChecks(tableName)

    // A NUL-free lookup is answered by the index.
    sql "set enable_inverted_index_query = true"
    sql "set enable_profile = true"
    sql "set profile_level = 2"
    def profileSql = "SELECT /* nul_terms_ok_index */ id FROM ${tableName} WHERE s = 'ok' ORDER BY id"
    assertEquals([[1], [11]], sql(profileSql))
    sleep(2000)
    def profileAction = new ProfileAction(context)
    def profileId = profileAction.getProfileList().find { it["Sql Statement"].contains("nul_terms_ok_index") }?.getAt("Profile ID")
    assertNotNull(profileId)
    def profileDetail = profileAction.getProfile(profileId).toString()
    def filtered = (profileDetail =~ /RowsInvertedIndexFiltered: (\d+)/).collect { it[1] as int }
    log.info("RowsInvertedIndexFiltered: ${filtered}")
    assertTrue(filtered.sum() == 10, "s = 'ok' should filter 10 rows by the index: ${filtered}")
    sql "set enable_profile = false"

    // The same results after full compaction rewrites the index.
    trigger_and_wait_compaction(tableName, "full")
    assertEquals(expectedHex, sql("SELECT id, hex(s) FROM ${tableName} ORDER BY id"))
    allChecks(tableName)

    // The same results for an index built over existing data.
    sql "DROP TABLE IF EXISTS ${buildTableName}"
    sql """
        CREATE TABLE ${buildTableName} (
            `id` int NULL,
            `s` string NULL,
            `t` string NULL,
            `g` string NULL,
            `arr` array<string> NULL,
            `v` variant NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        );
    """
    load(buildTableName, 'test_inverted_index_nul_terms.json', 9)
    load(buildTableName, 'test_inverted_index_nul_terms_2.json', 3)
    sql "sync"
    sql "ALTER TABLE ${buildTableName} ADD INDEX s_idx (`s`) USING INVERTED"
    wait_for_last_col_change_finish(buildTableName, 60000)
    sql "ALTER TABLE ${buildTableName} ADD INDEX arr_idx (`arr`) USING INVERTED"
    wait_for_last_col_change_finish(buildTableName, 60000)
    build_index_on_table("s_idx", buildTableName)
    wait_for_last_build_index_finish(buildTableName, 60000)
    build_index_on_table("arr_idx", buildTableName)
    wait_for_last_build_index_finish(buildTableName, 60000)
    assertEquals(expectedHex, sql("SELECT id, hex(s) FROM ${buildTableName} ORDER BY id"))
    scalarChecks(buildTableName)

    sql "set enable_inverted_index_query = true"
}
