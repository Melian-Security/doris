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

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <exception>
#include <memory>
#include <string>
#include <string_view>

namespace lucene {
namespace store {
class Directory;
} // namespace store

namespace index {
class Term;
class TermDocs;
class TermPositions;
class IndexReader;
} // namespace index
} // namespace lucene

class CLuceneError;

namespace doris::segment_v2 {

struct DirectoryDeleter {
    void operator()(lucene::store::Directory* p) const;
};

struct TermDeleter {
    void operator()(lucene::index::Term* p) const;
};
using TermPtr = std::unique_ptr<lucene::index::Term, TermDeleter>;

template <typename... Args>
TermPtr make_term_ptr(Args&&... args);

struct CLuceneDeleter {
    void operator()(lucene::index::TermDocs* p) const;
};
using TermDocsPtr = std::unique_ptr<lucene::index::TermDocs, CLuceneDeleter>;
using TermPositionsPtr = std::unique_ptr<lucene::index::TermPositions, CLuceneDeleter>;

template <typename... Args>
TermDocsPtr make_term_doc_ptr(lucene::index::IndexReader* reader, Args&&... args);

template <typename... Args>
TermPositionsPtr make_term_positions_ptr(lucene::index::IndexReader* reader, Args&&... args);

// CLucene keeps term text NUL-terminated, so an untokenized term holding a NUL byte has no
// faithful representation: a NUL followed by other bytes makes the writer throw, and trailing
// NULs are cut off, which would index "a\0" under the term "a". The writer leaves such a value
// out of an untokenized index, and the reader must not answer a lookup for one from the index.
inline bool untokenized_term_has_nul(std::string_view term) {
    return !term.empty() && std::memchr(term.data(), '\0', term.size()) != nullptr;
}

// A tokenizer can keep a NUL byte inside a token, which CLucene cannot store or look up, so a
// tokenized value and a query against a tokenized index are both analyzed with every NUL byte
// replaced by a space. Returns `value` itself when it holds no NUL, otherwise a view of `buf`.
inline std::string_view nul_as_space_for_analyzer(std::string_view value, std::string* buf) {
    const size_t nul_at = value.find('\0');
    if (nul_at == std::string_view::npos) {
        return value;
    }
    buf->assign(value);
    std::replace(buf->begin() + static_cast<std::ptrdiff_t>(nul_at), buf->end(), '\0', ' ');
    return *buf;
}

struct ErrorContext {
    std::string err_msg;
    std::exception_ptr eptr;
};

template <typename T>
concept HasClose = requires(T t) {
    { t->close() };
};

template <typename PtrType>
    requires HasClose<PtrType>
void finally_close(PtrType& resource, ErrorContext& error_context);

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-macros"
#endif

#define FINALLY_CLOSE(resource)                                                     \
    {                                                                               \
        static_assert(sizeof(error_context) > 0,                                    \
                      "error_context must be defined before using FINALLY macro!"); \
        finally_close(resource, error_context);                                     \
    }

// Return ERROR after finally
#define FINALLY(finally_block)                                                                    \
    {                                                                                             \
        static_assert(sizeof(error_context) > 0,                                                  \
                      "error_context must be defined before using FINALLY macro!");               \
        finally_block;                                                                            \
        if (error_context.eptr) {                                                                 \
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(error_context.err_msg); \
        }                                                                                         \
    }

// Re-throw the exception after finally
#define FINALLY_EXCEPTION(finally_block)                                            \
    {                                                                               \
        static_assert(sizeof(error_context) > 0,                                    \
                      "error_context must be defined before using FINALLY macro!"); \
        finally_block;                                                              \
        if (error_context.eptr) {                                                   \
            std::rethrow_exception(error_context.eptr);                             \
        }                                                                           \
    }

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

} // namespace doris::segment_v2

#include "storage/index/inverted/inverted_index_common_impl.h"