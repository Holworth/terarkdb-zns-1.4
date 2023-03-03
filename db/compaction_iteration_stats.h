//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <algorithm>
#include <cstdint>
#include <unordered_map>

#include "rocksdb/slice.h"
#include "util/murmurhash.h"

using terarkdb::murmur_hash;
using terarkdb::Slice;

struct CompactionIterationStats {
  // Compaction statistics

  // Doesn't include records skipped because of
  // CompactionFilter::Decision::kRemoveAndSkipUntil.
  int64_t num_record_drop_user = 0;

  int64_t num_record_drop_hidden = 0;
  int64_t num_record_drop_obsolete = 0;
  int64_t num_record_drop_range_del = 0;
  int64_t num_range_del_drop_obsolete = 0;
  // Deletions obsoleted before bottom level due to file gap optimization.
  int64_t num_optimized_del_drop_obsolete = 0;
  uint64_t total_filter_time = 0;

  // Input statistics
  // TODO(noetzli): The stats are incomplete. They are lacking everything
  // consumed by MergeHelper.
  uint64_t num_input_records = 0;
  uint64_t num_input_deletion_records = 0;
  uint64_t num_input_corrupt_records = 0;
  uint64_t total_input_raw_key_bytes = 0;
  uint64_t total_input_raw_value_bytes = 0;

  // Single-Delete diagnostics for exceptional situations
  uint64_t num_single_del_fallthru = 0;
  uint64_t num_single_del_mismatch = 0;

  // (kqh): Related structures for recording compaction stats
  // The number of occurrence of each key in a compaction process
  // struct SliceEqualComparator {
  //   bool operator()(const Slice& a, const Slice& b) {
  //     return a.compare(b) == 0;
  //   }
  // };
  // std::unordered_map<Slice, uint64_t, murmur_hash, SliceEqualComparator>
  //     occurrence_table;

  // void AddOccurrence(const Slice& user_key, int num) {
  //   occurrence_table[user_key] += 1;
  // }

  // Record obsolete information gained from this compaction process.
  //
  // It maintains a map from file number to the number of contained keys being
  // deprecated. E.g. <<1,2> 100> means 1.sst has deprecated 100 records in 
  // 2.sst. If the src node of an edge <a, b> (i.e. The a) is -1, it means it's
  // a not-separated value deprecated a record in b.sst.
  //
  // TODO: Only add deprecated record number is not efficient, may add the
  // record size of deprecated record.
  struct pair_hash {
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2>& pair) const {
      return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
    }
  };
  std::unordered_map<std::pair<uint64_t, uint64_t>, uint64_t, pair_hash>
      deprecated_count;

  uint64_t SumDeprecatedRecordCount() const {
    uint64_t ret = 0;
    std::for_each(deprecated_count.begin(), deprecated_count.end(),
                  [&](const auto& a) { ret += a.second; });
    return ret;
  }
};
