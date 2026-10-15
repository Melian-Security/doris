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

// Full compaction merges the inverted indexes of VARIANT typed sub-columns (field_pattern
// indexes) instead of rebuilding them when enable_variant_subcolumn_index_compaction is on. The
// merged and the rebuilt indexes must answer every query alike, and alike with the index off.
suite("test_variant_subcolumn_index_compaction", "nonConcurrent") {
    def backends = sql_return_maparray("show backends")
    assertTrue(backends.size() > 0)
    def http = { String method, String url ->
        def conn = new URL(url).openConnection()
        conn.setRequestMethod(method)
        def auth = Base64.getEncoder().encodeToString("${context.config.feHttpUser}:${context.config.feHttpPassword ?: ''}".getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${auth}")
        if (method == "POST") {
            conn.setDoOutput(true)
            conn.getOutputStream().close()
        }
        return conn.getInputStream().getText()
    }
    def setMerge = { boolean enabled ->
        for (def be : backends) {
            def out = http("POST", "http://${be.Host}:${be.HttpPort}/api/update_config?enable_variant_subcolumn_index_compaction=${enabled}")
            log.info("update_config: ${out}".toString())
        }
    }
    def bvar = { String name ->
        long total = 0
        for (def be : backends) {
            def out = http("GET", "http://${be.Host}:${be.BrpcPort}/vars/${name}?console=1").trim()
            total += Long.parseLong(out.split(":")[1].trim())
        }
        return total
    }

    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS vsic_ngram4_tokenizer
        PROPERTIES ("type" = "ngram", "min_gram" = "4", "max_gram" = "4");
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS vsic_ngram4_lower
        PROPERTIES ("tokenizer" = "vsic_ngram4_tokenizer", "token_filter" = "lowercase");
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS vsic_alnum_lower
        PROPERTIES ("tokenizer" = "basic", "token_filter" = "lowercase");
    """
    // backends learn new analyzers asynchronously
    sleep(10000)

    def createTable = { String name ->
        sql "DROP TABLE IF EXISTS ${name}"
        sql """
            CREATE TABLE ${name} (
                `id` int NOT NULL,
                `v` variant<
                    'msg' : string,
                    'proc' : string,
                    'host' : string,
                    'port' : bigint,
                    properties("variant_max_subcolumns_count" = "2048",
                               "variant_enable_typed_paths_to_sparse" = "false")
                > NULL,
                INDEX idx_msg_ngram (`v`) USING INVERTED PROPERTIES("field_pattern" = "msg", "analyzer" = "vsic_ngram4_lower", "support_phrase" = "true"),
                INDEX idx_msg_words (`v`) USING INVERTED PROPERTIES("field_pattern" = "msg", "analyzer" = "vsic_alnum_lower", "support_phrase" = "true"),
                INDEX idx_proc_eq (`v`) USING INVERTED PROPERTIES("field_pattern" = "proc"),
                INDEX idx_host_eq (`v`) USING INVERTED PROPERTIES("field_pattern" = "host"),
                INDEX idx_port_eq (`v`) USING INVERTED PROPERTIES("field_pattern" = "port")
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V3"
            );
        """
    }

    def procs = ["cmd.exe", "powershell.exe", "svchost.exe", "explorer.exe"]
    def hosts = ["web-01", "DB-02", "cache-03"]
    // Four loads; every load carries every string path in some row, rows without a path and a
    // NULL variant included.
    def load = { String name ->
        for (int batch = 0; batch < 4; batch++) {
            def rows = []
            for (int i = 0; i < 25; i++) {
                int id = batch * 100 + i
                if (i == 7) {
                    rows << "(${id}, NULL)"
                    continue
                }
                def fields = []
                if (i % 5 != 4) {
                    fields << "\"msg\":\"User Alice${i % 3} ran ${procs[i % 4]} error code ${i % 6} on ${hosts[i % 3]}\""
                }
                fields << "\"proc\":\"${procs[(i + batch) % 4]}\""
                if (i % 6 != 5) {
                    fields << "\"host\":\"${hosts[(i + batch) % 3]}\""
                }
                fields << "\"port\":${443 + i % 2}"
                rows << "(${id}, '{${fields.join(',')}}')"
            }
            sql "INSERT INTO ${name} VALUES ${rows.join(',')}"
        }
        // a deleted row is remapped away by the compaction
        sql "DELETE FROM ${name} WHERE id = 103"
    }

    def predicates = [
            "v['msg'] MATCH_ANY 'alice1'",
            "v['msg'] MATCH_PHRASE 'error code 3'",
            "v['msg'] MATCH_ALL 'ran cmd'",
            "v['msg'] MATCH_PHRASE_PREFIX 'error co'",
            "v['msg'] MATCH_ANY 'werS' USING ANALYZER vsic_ngram4_lower",
            "v['msg'] MATCH_PHRASE 'shell.exe' USING ANALYZER vsic_ngram4_lower",
            "v['msg'] MATCH_ANY 'alice2' USING ANALYZER vsic_alnum_lower",
            "cast(v['proc'] as string) = 'cmd.exe'",
            "cast(v['proc'] as string) IN ('svchost.exe', 'explorer.exe')",
            "cast(v['proc'] as string) != 'cmd.exe'",
            "cast(v['host'] as string) = 'DB-02'",
            "cast(v['host'] as string) IS NULL",
            "cast(v['msg'] as string) IS NULL",
            "cast(v['port'] as bigint) = 444",
    ]
    def ids = { String table, String predicate, boolean useIndex ->
        sql "set enable_inverted_index_query = ${useIndex}"
        return sql("SELECT id FROM ${table} WHERE ${predicate} ORDER BY id").collect { it[0] }
    }
    def results = { String table ->
        def out = [:]
        for (def predicate : predicates) {
            def withIndex = ids(table, predicate, true)
            def withoutIndex = ids(table, predicate, false)
            assertEquals(withoutIndex, withIndex, "${table}: index changes the result of: ${predicate}")
            out[predicate] = withIndex
        }
        sql "set enable_inverted_index_query = true"
        return out
    }

    sql "set enable_common_expr_pushdown = true"
    sql "set enable_match_without_inverted_index = true"
    try {
        // Rebuilt through the writer.
        setMerge(false)
        createTable("vsic_rebuilt")
        load("vsic_rebuilt")
        def before = results("vsic_rebuilt")
        long mergedBefore = bvar("variant_subcolumn_index_compaction_merged")
        trigger_and_wait_compaction("vsic_rebuilt", "full")
        assertEquals(mergedBefore, bvar("variant_subcolumn_index_compaction_merged"))
        def rebuilt = results("vsic_rebuilt")
        assertEquals(before, rebuilt)

        // Merged by index compaction.
        setMerge(true)
        createTable("vsic_merged")
        load("vsic_merged")
        mergedBefore = bvar("variant_subcolumn_index_compaction_merged")
        trigger_and_wait_compaction("vsic_merged", "full")
        long merged = bvar("variant_subcolumn_index_compaction_merged") - mergedBefore
        log.info("variant sub-column indexes merged: ${merged}".toString())
        // msg (two indexes), proc and host; port is a numeric sub-column and stays rebuilt
        assertEquals(4L, merged)
        def mergedResults = results("vsic_merged")
        assertEquals(rebuilt, mergedResults)
        assertTrue(mergedResults.values().count { !it.isEmpty() } >= 10)
        for (def predicate : predicates) {
            quickTest("rows", "SELECT id FROM vsic_merged WHERE ${predicate} ORDER BY id")
        }
    } finally {
        setMerge(true)
    }
}
