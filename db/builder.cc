// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "table/pmem.h"
#include "db/dbformat.h"
#include <chrono>

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);

  Pmem_TableBuilder* pmem_table_builder;
  pmem_table_builder = new Pmem_TableBuilder(meta->number);
  pmem_table_builder->init();

  if (iter->Valid()) {
    meta->smallest.DecodeFrom(iter->key());
    auto start = std::chrono::high_resolution_clock::now();
    Slice key;
    Slice smallkey;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      if(pmem_table_builder->get_block_offset()==0){
        smallkey = iter->key();
      }
      pmem_table_builder->Add(key,iter->value());
      if(pmem_table_builder->get_block_offset() >= 4*1024){
        pmem_table_builder -> Flush(meta,smallkey,key);
      }
    }
    pmem_table_builder->Finish();
    meta->block_indexes = std::vector<BlockOffset>(std::make_move_iterator(meta->block_indexes.begin()), \
                                                  std::make_move_iterator(meta->block_indexes.end()));;
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }
    // Finish and check for builder errors
    if (s.ok()) {
      meta->file_size = pmem_table_builder->get_file_size();
      assert(meta->file_size > 0);
    }
    delete pmem_table_builder;
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }
  return s;
}

}  // namespace leveldb
