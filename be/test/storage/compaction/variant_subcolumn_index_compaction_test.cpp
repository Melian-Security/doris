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

#include "storage/compaction/variant_subcolumn_index_compaction.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_compaction.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/inverted_index_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris {

using namespace segment_v2;

namespace {

TabletIndex make_index(int64_t index_id, int32_t col_uid,
                       std::map<std::string, std::string> properties) {
    TabletIndexPB pb;
    pb.set_index_id(index_id);
    pb.set_index_name("idx_" + std::to_string(index_id));
    pb.set_index_type(IndexType::INVERTED);
    pb.add_col_unique_id(col_uid);
    for (const auto& [k, v] : properties) {
        (*pb.mutable_properties())[k] = v;
    }
    TabletIndex index;
    index.init_from_pb(pb);
    return index;
}

TabletSchemaSPtr schema_with(std::vector<TabletIndex> indexes) {
    auto schema = std::make_shared<TabletSchema>();
    for (auto& index : indexes) {
        schema->append_index(std::move(index));
    }
    return schema;
}

DataTypePtr nullable_string() {
    return make_nullable(std::make_shared<DataTypeString>());
}

VariantSegmentPaths segment(std::map<std::string, DataTypePtr> materialized,
                            std::vector<std::string> sparse = {}) {
    VariantSegmentPaths paths;
    for (auto& [path, type] : materialized) {
        paths.materialized[path].push_back(type);
    }
    paths.sparse.insert(sparse.begin(), sparse.end());
    return paths;
}

} // namespace

TEST(VariantSubcolumnIndexCompactionTest, PathMaterializedAsStringInAll) {
    const auto str = nullable_string();
    const auto i64 = make_nullable(std::make_shared<DataTypeInt64>());
    EXPECT_TRUE(path_materialized_as_string_in_all({segment({{"a", str}}), segment({{"a", str}})},
                                                   "a"));
    // missing in one segment
    EXPECT_FALSE(path_materialized_as_string_in_all({segment({{"a", str}}), segment({{"b", str}})},
                                                    "a"));
    // another type in one segment
    EXPECT_FALSE(path_materialized_as_string_in_all({segment({{"a", str}}), segment({{"a", i64}})},
                                                    "a"));
    // kept in the sparse column of one segment
    EXPECT_FALSE(path_materialized_as_string_in_all(
            {segment({{"a", str}}), segment({{"b", str}}, {"a"})}, "a"));
    // no segment proves nothing
    EXPECT_FALSE(path_materialized_as_string_in_all({}, "a"));
}

TEST(VariantSubcolumnIndexCompactionTest, SelectsOnlyIndexesEveryInputCanMerge) {
    const int32_t uid = 7;
    const std::map<std::string, std::string> ngram = {
            {"field_pattern", "a"}, {"analyzer", "ngram4_lower"}, {"support_phrase", "true"}};
    const std::map<std::string, std::string> keyword = {{"field_pattern", "a"}};
    TabletIndex ngram_index = make_index(10, uid, ngram);
    TabletIndex keyword_index = make_index(11, uid, keyword);
    ngram_index.set_escaped_escaped_index_suffix_path("v.a");
    keyword_index.set_escaped_escaped_index_suffix_path("v.a");

    const auto str = nullable_string();
    std::map<int32_t, std::vector<VariantSegmentPaths>> segments = {
            {uid, {segment({{"a", str}}), segment({{"a", str}})}}};
    auto inputs = std::vector<TabletSchemaSPtr> {
            schema_with({make_index(10, uid, ngram), make_index(11, uid, keyword)}),
            schema_with({make_index(10, uid, ngram), make_index(11, uid, keyword)})};
    const VariantSubcolumnIndexCandidate candidate {.variant_uid = uid,
                                                    .path = "a",
                                                    .type = FieldType::OLAP_FIELD_TYPE_STRING,
                                                    .indexes = {&ngram_index, &keyword_index}};
    auto all_files = [](int32_t, const TabletIndex&) { return true; };

    auto selection = select_variant_subcolumn_indexes({candidate}, inputs, segments, {}, all_files);
    ASSERT_EQ(selection.merge.size(), 2);
    EXPECT_EQ(selection.rebuild, 0);
    EXPECT_EQ(selection.merge[0].index->index_id(), 10);
    EXPECT_EQ(selection.merge[0].index->get_index_suffix(), ngram_index.get_index_suffix());
    EXPECT_FALSE(selection.merge[0].index->get_index_suffix().empty());
    EXPECT_EQ(selection.merge[0].path, "a");

    // The path is missing from one input segment: both indexes are rebuilt.
    auto missing = segments;
    missing[uid][1] = segment({{"b", str}});
    selection = select_variant_subcolumn_indexes({candidate}, inputs, missing, {}, all_files);
    EXPECT_TRUE(selection.merge.empty());
    EXPECT_EQ(selection.rebuild, 2);

    // The path is another type in one input segment.
    auto mistyped = segments;
    mistyped[uid][0] = segment({{"a", make_nullable(std::make_shared<DataTypeInt64>())}});
    selection = select_variant_subcolumn_indexes({candidate}, inputs, mistyped, {}, all_files);
    EXPECT_TRUE(selection.merge.empty());

    // The path is sparse in one input segment.
    auto sparse = segments;
    sparse[uid][1] = segment({}, {"a"});
    selection = select_variant_subcolumn_indexes({candidate}, inputs, sparse, {}, all_files);
    EXPECT_TRUE(selection.merge.empty());

    // One input rowset built the ngram index with other properties: only the keyword index merges.
    auto changed = inputs;
    auto other_ngram = ngram;
    other_ngram["support_phrase"] = "false";
    changed[1] = schema_with({make_index(10, uid, other_ngram), make_index(11, uid, keyword)});
    selection = select_variant_subcolumn_indexes({candidate}, changed, segments, {}, all_files);
    ASSERT_EQ(selection.merge.size(), 1);
    EXPECT_EQ(selection.merge[0].index->index_id(), 11);
    EXPECT_EQ(selection.rebuild, 1);

    // One input rowset has no such index at all.
    changed[1] = schema_with({make_index(11, uid, keyword)});
    selection = select_variant_subcolumn_indexes({candidate}, changed, segments, {}, all_files);
    ASSERT_EQ(selection.merge.size(), 1);
    EXPECT_EQ(selection.merge[0].index->index_id(), 11);

    // An index file is missing or corrupt in some input segment.
    selection = select_variant_subcolumn_indexes(
            {candidate}, inputs, segments, {},
            [](int32_t, const TabletIndex& index) { return index.index_id() != 10; });
    ASSERT_EQ(selection.merge.size(), 1);
    EXPECT_EQ(selection.merge[0].index->index_id(), 11);

    // An earlier index compaction of this column failed.
    selection = select_variant_subcolumn_indexes({candidate}, inputs, segments, {uid}, all_files);
    EXPECT_TRUE(selection.merge.empty());
    EXPECT_EQ(selection.rebuild, 2);

    // Non-string sub-columns are not candidates.
    auto numeric = candidate;
    numeric.type = FieldType::OLAP_FIELD_TYPE_BIGINT;
    selection = select_variant_subcolumn_indexes({numeric}, inputs, segments, {}, all_files);
    EXPECT_TRUE(selection.merge.empty());
    EXPECT_EQ(selection.rebuild, 0);
}

// A per-path index merged by compaction answers queries exactly as one rebuilt from the
// remapped rows, including NULL rows, NUL bytes and rows deleted by the compaction.
class VariantSubcolumnIndexMergeTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/variant_subcolumn_index_merge_test";
    static constexpr int32_t kVariantUid = 3;

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(kTestDir, 1024);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        _searcher_cache.reset(InvertedIndexSearcherCache::create_global_instance(1 << 30, 1));
        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(1 << 30, 1));
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_searcher_cache.get());
        ExecEnv::GetInstance()->_inverted_index_query_cache = _query_cache.get();

        TIndexPolicy tokenizer;
        tokenizer.id = 1;
        tokenizer.name = "merge_test_ngram4";
        tokenizer.type = TIndexPolicyType::TOKENIZER;
        tokenizer.properties["type"] = "ngram";
        tokenizer.properties["min_gram"] = "4";
        tokenizer.properties["max_gram"] = "4";
        TIndexPolicy filter;
        filter.id = 2;
        filter.name = "merge_test_lowercase";
        filter.type = TIndexPolicyType::TOKEN_FILTER;
        filter.properties["type"] = "lowercase";
        TIndexPolicy analyzer;
        analyzer.id = 3;
        analyzer.name = "merge_test_ngram4_lower";
        analyzer.type = TIndexPolicyType::ANALYZER;
        analyzer.properties["tokenizer"] = "merge_test_ngram4";
        analyzer.properties["token_filter"] = "merge_test_lowercase";
        _policy_mgr.apply_policy_changes({tokenizer, filter, analyzer}, {});
        _previous_policy_mgr = ExecEnv::GetInstance()->_index_policy_mgr;
        ExecEnv::GetInstance()->_index_policy_mgr = &_policy_mgr;

        _column.set_name("v.a");
        _column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        _column.set_is_nullable(true);
        _column.set_parent_unique_id(kVariantUid);
        _column.set_path_info(PathInData("v.a", true));
    }

    void TearDown() override {
        ExecEnv::GetInstance()->_index_policy_mgr = _previous_policy_mgr;
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    TabletIndex index_with(int64_t index_id, std::map<std::string, std::string> properties) {
        properties["field_pattern"] = "a";
        TabletIndex index = make_index(index_id, kVariantUid, properties);
        index.set_escaped_escaped_index_suffix_path("v.a");
        return index;
    }

    std::string prefix(const std::string& name) {
        return std::string {InvertedIndexDescriptor::get_index_file_path_prefix(
                fmt::format("{}/{}_0.dat", kTestDir, name))};
    }

    // Writes rows (nullopt is SQL NULL) for every index into one segment index file.
    void write_segment(const std::string& name, const std::vector<TabletIndex*>& indexes,
                       const std::vector<std::optional<std::string>>& rows) {
        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix(name)),
                                    &file_writer, &opts)
                            .ok());
        IndexFileWriter index_file_writer(fs, prefix(name), name, 0,
                                          InvertedIndexStorageFormatPB::V2, std::move(file_writer));
        for (auto* index : indexes) {
            std::unique_ptr<IndexColumnWriter> writer;
            ASSERT_TRUE(
                    IndexColumnWriter::create(&_column, &writer, &index_file_writer, index).ok());
            for (const auto& row : rows) {
                if (row.has_value()) {
                    Slice value(*row);
                    ASSERT_TRUE(writer->add_values("v.a", &value, 1).ok());
                } else {
                    ASSERT_TRUE(writer->add_nulls(1).ok());
                }
            }
            ASSERT_TRUE(writer->finish().ok());
        }
        ASSERT_TRUE(index_file_writer.begin_close().ok());
        ASSERT_TRUE(index_file_writer.finish_close().ok());
    }

    // Merges the source segments' index into one destination segment with `trans_vec`.
    void merge(const std::vector<std::string>& sources, const std::string& dest,
               const std::vector<TabletIndex*>& indexes,
               const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& trans_vec,
               uint32_t dest_rows) {
        auto fs = io::global_local_filesystem();
        std::vector<std::unique_ptr<IndexFileReader>> readers;
        for (const auto& source : sources) {
            readers.push_back(std::make_unique<IndexFileReader>(fs, prefix(source),
                                                                InvertedIndexStorageFormatPB::V2));
            ASSERT_TRUE(readers.back()->init().ok());
        }
        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        ASSERT_TRUE(fs->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix(dest)),
                                    &file_writer, &opts)
                            .ok());
        IndexFileWriter index_file_writer(fs, prefix(dest), dest, 0,
                                          InvertedIndexStorageFormatPB::V2, std::move(file_writer));
        for (auto* index : indexes) {
            std::vector<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>> src_dirs;
            for (auto& reader : readers) {
                auto dir = reader->open(index);
                ASSERT_TRUE(dir.has_value()) << dir.error();
                src_dirs.push_back(std::move(dir.value()));
            }
            auto dest_dir = index_file_writer.open(index);
            ASSERT_TRUE(dest_dir.has_value()) << dest_dir.error();
            std::vector<lucene::store::Directory*> dest_dirs = {dest_dir.value().get()};
            auto st = compact_column(index->index_id(), src_dirs, dest_dirs,
                                     kTestDir + "/tmp_" + std::to_string(index->index_id()),
                                     trans_vec, {dest_rows});
            ASSERT_TRUE(st.ok()) << st;
        }
        ASSERT_TRUE(index_file_writer.begin_close().ok());
        ASSERT_TRUE(index_file_writer.finish_close().ok());
    }

    roaring::Roaring query(const std::string& segment, TabletIndex* index, bool fulltext,
                           const std::string& value, InvertedIndexQueryType type) {
        auto file_reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), prefix(segment), InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(file_reader->init().ok());
        InvertedIndexReaderPtr reader;
        if (fulltext) {
            reader = FullTextIndexReader::create_shared(index, file_reader);
        } else {
            reader = StringTypeInvertedIndexReader::create_shared(index, file_reader);
        }
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        query_options.enable_inverted_index_query_cache = false;
        runtime_state.set_query_options(query_options);
        io::IOContext io_ctx;
        auto context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
        auto bitmap = std::make_shared<roaring::Roaring>();
        Field qp = Field::create_field<TYPE_STRING>(value);
        auto st = reader->query(context, std::to_string(kVariantUid) + ".v.a", qp, type, bitmap);
        EXPECT_TRUE(st.ok()) << st;
        return *bitmap;
    }

    roaring::Roaring nulls(const std::string& segment, TabletIndex* index) {
        auto file_reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), prefix(segment), InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(file_reader->init().ok());
        auto reader = StringTypeInvertedIndexReader::create_shared(index, file_reader);
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        io::IOContext io_ctx;
        auto context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
        InvertedIndexQueryCacheHandle handle;
        EXPECT_TRUE(reader->read_null_bitmap(context, &handle, nullptr).ok());
        auto bitmap = handle.get_bitmap();
        return bitmap ? *bitmap : roaring::Roaring();
    }

    TabletColumn _column;
    IndexPolicyMgr _policy_mgr;
    IndexPolicyMgr* _previous_policy_mgr = nullptr;
    std::unique_ptr<InvertedIndexSearcherCache> _searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
};

TEST_F(VariantSubcolumnIndexMergeTest, MergedIndexMatchesRebuiltIndex) {
    TabletIndex ngram =
            index_with(20, {{"analyzer", "merge_test_ngram4_lower"}, {"support_phrase", "true"}});
    TabletIndex words = index_with(21, {{"parser", "unicode"}, {"support_phrase", "true"}});
    TabletIndex keyword = index_with(22, {});
    std::vector<TabletIndex*> indexes = {&ngram, &words, &keyword};

    using Row = std::optional<std::string>;
    const std::string nul_value("cmd\0exe /c", 10);
    // Segment A row 1 is deleted by the compaction; NULL rows stay NULL.
    const std::vector<Row> a = {"C:\\Windows\\System32\\cmd.exe", "deleted Powershell.EXE",
                                std::nullopt, "svchost.exe -k netsvcs"};
    const std::vector<Row> b = {nul_value, "Hello World cmd.exe", std::nullopt, "hello"};
    write_segment("src_a", indexes, a);
    write_segment("src_b", indexes, b);

    constexpr uint32_t kDeleted = UINT32_MAX;
    // A: 0->0, 1 deleted, 2->1, 3->2; B: 0->3, 1->4, 2->5, 3->6
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec = {
            {{0, 0}, {kDeleted, kDeleted}, {0, 1}, {0, 2}}, {{0, 3}, {0, 4}, {0, 5}, {0, 6}}};
    merge({"src_a", "src_b"}, "merged", indexes, trans_vec, 7);
    write_segment("rebuilt", indexes, {a[0], a[2], a[3], b[0], b[1], b[2], b[3]});

    struct Probe {
        TabletIndex* index;
        bool fulltext;
        std::string value;
        InvertedIndexQueryType type;
    };
    const std::vector<Probe> probes = {
            {&ngram, true, "cmd.", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {&ngram, true, "cmd.exe", InvertedIndexQueryType::MATCH_PHRASE_QUERY},
            {&ngram, true, "System32", InvertedIndexQueryType::MATCH_PHRASE_QUERY},
            {&ngram, true, "powershell", InvertedIndexQueryType::MATCH_PHRASE_QUERY},
            {&ngram, true, nul_value, InvertedIndexQueryType::MATCH_PHRASE_QUERY},
            {&words, true, "hello", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {&words, true, "hello world", InvertedIndexQueryType::MATCH_PHRASE_QUERY},
            {&words, true, "netsvcs", InvertedIndexQueryType::MATCH_ALL_QUERY},
            {&keyword, false, "hello", InvertedIndexQueryType::EQUAL_QUERY},
            {&keyword, false, "svchost.exe -k netsvcs", InvertedIndexQueryType::EQUAL_QUERY},
            {&keyword, false, "deleted Powershell.EXE", InvertedIndexQueryType::EQUAL_QUERY},
    };
    size_t non_empty = 0;
    for (const auto& probe : probes) {
        auto merged = query("merged", probe.index, probe.fulltext, probe.value, probe.type);
        auto rebuilt = query("rebuilt", probe.index, probe.fulltext, probe.value, probe.type);
        EXPECT_EQ(merged, rebuilt)
                << "index " << probe.index->index_id() << " value " << probe.value << ": merged "
                << merged.toString() << " rebuilt " << rebuilt.toString();
        non_empty += merged.isEmpty() ? 0 : 1;
    }
    EXPECT_GE(non_empty, 7);
    // The deleted row is gone from the merged index.
    EXPECT_TRUE(query("merged", &keyword, false, "deleted Powershell.EXE",
                      InvertedIndexQueryType::EQUAL_QUERY)
                        .isEmpty());
    for (auto* index : indexes) {
        EXPECT_EQ(nulls("merged", index), roaring::Roaring::bitmapOf(2, 1, 5))
                << "index " << index->index_id();
        EXPECT_EQ(nulls("merged", index), nulls("rebuilt", index));
    }
}

} // namespace doris
