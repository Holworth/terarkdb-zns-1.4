//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <algorithm>
#include <deque>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "db/compaction_iteration_stats.h"
#include "db/compaction_iterator.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "fs/fs_zenfs.h"
#include "fs/log.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/terark_namespace.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/table_builder.h"
#include "util/c_style_callback.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/murmurhash.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"
#include "utilities/persistent_cache/block_cache_tier_file.h"

namespace TERARKDB_NAMESPACE {

class TableFactory;

struct BuilderSeparateHelper : public SeparateHelper {
  std::vector<FileMetaData>* output = nullptr;
  std::vector<TableProperties>* prop = nullptr;
  std::string fname;
  TableProperties tp;
  std::unique_ptr<WritableFileWriter> file_writer;
  std::unique_ptr<TableBuilder> builder;
  FileMetaData* current_output = nullptr;
  TableProperties* current_prop = nullptr;
  std::unique_ptr<ValueExtractor> value_meta_extractor;
  Status (*trans_to_separate_callback)(void* args, const Slice& key,
                                       LazyBuffer& value) = nullptr;
  void* trans_to_separate_callback_args = nullptr;

  Status TransToSeparate(const Slice& internal_key, LazyBuffer& value,
                         const Slice& meta, bool is_merge,
                         bool is_index) override {
    return SeparateHelper::TransToSeparate(
        internal_key, value, value.file_number(), meta, is_merge, is_index,
        value_meta_extractor.get());
  }

  Status TransToSeparate(const Slice& internal_key,
                         LazyBuffer& value) override {
    if (trans_to_separate_callback == nullptr) {
      return Status::NotSupported();
    }
    return trans_to_separate_callback(trans_to_separate_callback_args,
                                      internal_key, value);
  }

  LazyBuffer TransToCombined(const Slice& /*user_key*/, uint64_t /*sequence*/,
                             const LazyBuffer& /*value*/) const override {
    assert(false);
    return LazyBuffer();
  }
};

TableBuilder* NewTableBuilder(
    const ImmutableCFOptions& ioptions, const MutableCFOptions& moptions,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    WritableFileWriter* file, const CompressionType compression_type,
    const CompressionOptions& compression_opts, int level,
    double compaction_load, const std::string* compression_dict,
    bool skip_filters, uint64_t creation_time, uint64_t oldest_key_time,
    SstPurpose sst_purpose, Env* env) {
  assert((column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         column_family_name.empty());
  return ioptions.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, internal_comparator,
                          int_tbl_prop_collector_factories, compression_type,
                          compression_opts, compression_dict, skip_filters,
                          column_family_name, level, compaction_load,
                          creation_time, oldest_key_time, sst_purpose, env),
      column_family_id, file);
}

Status BuildTable(
    const std::string& dbname, VersionSet* versions_, Env* env,
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const EnvOptions& env_options,
    TableCache* table_cache,
    InternalIterator* (*get_input_iter_callback)(void*, Arena&),
    void* get_input_iter_arg,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>> (
        *get_range_del_iters_callback)(void*),
    void* get_range_del_iters_arg, std::vector<FileMetaData>* meta_vec,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories_for_blob,
    uint32_t column_family_id, const std::string& column_family_name,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, const CompressionType compression,
    const CompressionOptions& compression_opts, bool paranoid_file_checks,
    InternalStats* internal_stats, TableFileCreationReason reason,
    EventLogger* event_logger, int job_id, const Env::IOPriority io_priority,
    std::vector<TableProperties>* table_properties_vec, int level,
    double compaction_load, const uint64_t creation_time,
    const uint64_t oldest_key_time, Env::WriteLifeTimeHint write_hint) {
  assert((column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         column_family_name.empty());
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  Status s;
  assert(meta_vec->size() == 1);
  if (table_properties_vec != nullptr) {
    table_properties_vec->emplace_back();
  }
  auto sst_meta = [meta_vec] { return &meta_vec->front(); };
  Arena arena;
  ScopedArenaIterator iter(get_input_iter_callback(get_input_iter_arg, arena));
  iter->SeekToFirst();
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&internal_comparator, snapshots));
  for (auto& range_del_iter :
       get_range_del_iters_callback(get_range_del_iters_arg)) {
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }

  std::string fname =
      TableFileName(ioptions.cf_paths, sst_meta()->fd.GetNumber(),
                    sst_meta()->fd.GetPathId());
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      ioptions.listeners, dbname, column_family_name, fname, job_id, reason);
#endif  // !ROCKSDB_LITE
  TableProperties tp;

  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    TableBuilder* builder;
    std::unique_ptr<WritableFileWriter> file_writer;
    {
      std::unique_ptr<WritableFile> file = std::make_unique<WritableFile>();
#ifndef NDEBUG
      bool use_direct_writes = env_options.use_direct_writes;
      TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
      file->SetFileLevel(level);
      s = NewWritableFile(env, fname, &file, env_options);
      if (!s.ok()) {
        EventHelpers::LogAndNotifyTableFileCreationFinished(
            event_logger, ioptions.listeners, dbname, column_family_name, fname,
            job_id, sst_meta()->fd, tp, reason, s);
        return s;
      }
      file->SetIOPriority(io_priority);
      file->SetWriteLifeTimeHint(write_hint);

      file_writer.reset(new WritableFileWriter(std::move(file), fname,
                                               env_options, ioptions.statistics,
                                               ioptions.listeners));
      builder = NewTableBuilder(
          ioptions, mutable_cf_options, internal_comparator,
          int_tbl_prop_collector_factories, column_family_id,
          column_family_name, file_writer.get(), compression, compression_opts,
          level, compaction_load, nullptr /* compression_dict */,
          false /* skip_filters */, creation_time, oldest_key_time, kEssenceSst,
          env);
    }

    MergeHelper merge(env, internal_comparator.user_comparator(),
                      ioptions.merge_operator, nullptr, ioptions.info_log,
                      true /* internal key corruption is not ok */,
                      snapshots.empty() ? 0 : snapshots.back(),
                      snapshot_checker);

    BuilderSeparateHelper separate_helper;
    if (ioptions.value_meta_extractor_factory != nullptr) {
      ValueExtractorContext context = {column_family_id};
      separate_helper.value_meta_extractor =
          ioptions.value_meta_extractor_factory->CreateValueExtractor(context);
    }

    auto finish_output_blob_sst = [&] {
      Status status;
      TableBuilder* blob_builder = separate_helper.builder.get();
      FileMetaData* blob_meta = separate_helper.current_output;
      blob_meta->prop.num_entries = blob_builder->NumEntries();
      blob_meta->prop.num_deletions = 0;
      blob_meta->prop.purpose = kEssenceSst;
      blob_meta->prop.flags |= TablePropertyCache::kNoRangeDeletions;
      status = blob_builder->Finish(&blob_meta->prop, nullptr);
      TableProperties& tp = *separate_helper.current_prop;
      if (status.ok()) {
        blob_meta->fd.file_size = blob_builder->FileSize();
        tp = blob_builder->GetTableProperties();
        blob_meta->prop.raw_key_size = tp.raw_key_size;
        blob_meta->prop.raw_value_size = tp.raw_value_size;
        StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
        status = separate_helper.file_writer->Sync(ioptions.use_fsync);
      }
      if (status.ok()) {
        status = separate_helper.file_writer->Close();
      }
      // (ZNS): Update the table properties to file system. Invoke this
      // function after Close() is called to ensure this file has a determined
      // belong zone
      if (status.ok()) {
        auto tp = blob_builder->GetTableProperties();
        auto fname = separate_helper.file_writer->file_name();
        env->UpdateTableProperties(fname, &tp);
      }
      separate_helper.file_writer.reset();
      EventHelpers::LogAndNotifyTableFileCreationFinished(
          event_logger, ioptions.listeners, dbname, column_family_name,
          separate_helper.fname, job_id, blob_meta->fd, tp,
          TableFileCreationReason::kFlush, status);

      separate_helper.builder.reset();
      return s;
    };

    size_t target_blob_file_size = MaxBlobSize(
        mutable_cf_options, ioptions.num_levels, ioptions.compaction_style);

    auto trans_to_separate = [&](const Slice& key, LazyBuffer& value) {
      assert(value.file_number() == uint64_t(-1));
      Status status;
      TableBuilder* blob_builder = separate_helper.builder.get();
      FileMetaData* blob_meta = separate_helper.current_output;
      if (blob_builder != nullptr &&
          blob_builder->FileSize() > target_blob_file_size) {
        status = finish_output_blob_sst();
        blob_builder = nullptr;
      }
      if (status.ok() && blob_builder == nullptr) {
        std::unique_ptr<WritableFile> blob_file;
#ifndef NDEBUG
        bool use_direct_writes = env_options.use_direct_writes;
        TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
        separate_helper.output->emplace_back();
        blob_meta = separate_helper.current_output =
            &separate_helper.output->back();
        if (separate_helper.prop == nullptr) {
          separate_helper.current_prop = &separate_helper.tp;
        } else {
          separate_helper.prop->emplace_back();
          separate_helper.current_prop = &separate_helper.prop->back();
        }
        blob_meta->fd = FileDescriptor(versions_->NewFileNumber(),
                                       sst_meta()->fd.GetPathId(), 0);
        separate_helper.fname =
            TableFileName(ioptions.cf_paths, blob_meta->fd.GetNumber(),
                          blob_meta->fd.GetPathId());
        status = NewWritableFile(env, separate_helper.fname, &blob_file,
                                 env_options);
        if (!status.ok()) {
          EventHelpers::LogAndNotifyTableFileCreationFinished(
              event_logger, ioptions.listeners, dbname, column_family_name,
              fname, job_id, blob_meta->fd, TableProperties(), reason, status);
          return status;
        }
        blob_file->SetIOPriority(io_priority);
        // (kqh): Should set the lifetime hint of blob file to be extreme
        blob_file->SetWriteLifeTimeHint(Env::WriteLifeTimeHint::WLTH_EXTREME);

        separate_helper.file_writer.reset(new WritableFileWriter(
            std::move(blob_file), separate_helper.fname, env_options,
            ioptions.statistics, ioptions.listeners));
        ZnsLog(kCyan, "Set separate_helper.file_writer: %s",
               separate_helper.file_writer->file_name().c_str());
        separate_helper.builder.reset(NewTableBuilder(
            ioptions, mutable_cf_options, internal_comparator,
            int_tbl_prop_collector_factories_for_blob, column_family_id,
            column_family_name, separate_helper.file_writer.get(), compression,
            compression_opts, -1 /* level */, 0 /* compaction_load */, nullptr,
            true, 0, 0, kEssenceSst, env));
        blob_builder = separate_helper.builder.get();
      }
      if (status.ok()) {
        status = blob_builder->Add(key, value);
      }
      if (status.ok()) {
        blob_meta->UpdateBoundaries(key, GetInternalKeySeqno(key));
        status = SeparateHelper::TransToSeparate(
            key, value, blob_meta->fd.GetNumber(), Slice(),
            GetInternalKeyType(key) == kTypeMerge, false,
            separate_helper.value_meta_extractor.get());
      }
      return status;
    };

    separate_helper.output = meta_vec;
    separate_helper.prop = table_properties_vec;
    BlobConfig blob_config = mutable_cf_options.get_blob_config();
    if (ioptions.table_factory->IsBuilderNeedSecondPass()) {
      blob_config.blob_size = size_t(-1);
    } else {
      separate_helper.trans_to_separate_callback =
          c_style_callback(trans_to_separate);
      separate_helper.trans_to_separate_callback_args = &trans_to_separate;
    }

    CompactionIterator c_iter(
        iter.get(), &separate_helper, nullptr,
        internal_comparator.user_comparator(), &merge, kMaxSequenceNumber,
        &snapshots, earliest_write_conflict_snapshot, snapshot_checker, env,
        ShouldReportDetailedTime(env, ioptions.statistics),
        true /* internal key corruption is not ok */, range_del_agg.get(),
        nullptr, blob_config);

    struct SecondPassIterStorage {
      std::unique_ptr<CompactionRangeDelAggregator> range_del_agg;
      ScopedArenaIterator iter;
      std::aligned_storage<sizeof(MergeHelper), alignof(MergeHelper)>::type
          merge;

      ~SecondPassIterStorage() {
        if (iter.get() != nullptr) {
          range_del_agg.reset();
          iter.set(nullptr);
          auto merge_ptr = reinterpret_cast<MergeHelper*>(&merge);
          merge_ptr->~MergeHelper();
        }
      }
    } second_pass_iter_storage;

    auto make_compaction_iterator = [&] {
      second_pass_iter_storage.range_del_agg.reset(
          new CompactionRangeDelAggregator(&internal_comparator, snapshots));
      for (auto& range_del_iter :
           get_range_del_iters_callback(get_range_del_iters_arg)) {
        second_pass_iter_storage.range_del_agg->AddTombstones(
            std::move(range_del_iter));
      }
      second_pass_iter_storage.iter = ScopedArenaIterator(
          get_input_iter_callback(get_input_iter_arg, arena));
      auto merge_ptr = new (&second_pass_iter_storage.merge) MergeHelper(
          env, internal_comparator.user_comparator(), ioptions.merge_operator,
          nullptr, ioptions.info_log,
          true /* internal key corruption is not ok */,
          snapshots.empty() ? 0 : snapshots.back(), snapshot_checker);
      return new CompactionIterator(
          second_pass_iter_storage.iter.get(), &separate_helper, nullptr,
          internal_comparator.user_comparator(), merge_ptr, kMaxSequenceNumber,
          &snapshots, earliest_write_conflict_snapshot, snapshot_checker, env,
          false /* report_detailed_time */,
          true /* internal key corruption is not ok */, range_del_agg.get());
    };
    std::unique_ptr<InternalIterator> second_pass_iter(NewCompactionIterator(
        c_style_callback(make_compaction_iterator), &make_compaction_iterator));

    if (ioptions.merge_operator == nullptr ||
        ioptions.merge_operator->IsStableMerge()) {
      builder->SetSecondPassIterator(second_pass_iter.get());
    }
    c_iter.SeekToFirst();
    for (; s.ok() && c_iter.Valid(); c_iter.Next()) {
      s = builder->Add(c_iter.key(), c_iter.value());
      sst_meta()->UpdateBoundaries(c_iter.key(), c_iter.ikey().sequence);

      // TODO(noetzli): Update stats after flush, too.
      if (io_priority == Env::IO_HIGH &&
          IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        ThreadStatusUtil::SetThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
      }
    }

    auto range_del_it = range_del_agg->NewIterator();
    for (range_del_it->SeekToFirst(); s.ok() && range_del_it->Valid();
         range_del_it->Next()) {
      auto tombstone = range_del_it->Tombstone();
      auto kv = tombstone.Serialize();
      s = builder->AddTombstone(kv.first.Encode(), LazyBuffer(kv.second));
      sst_meta()->UpdateBoundariesForRange(kv.first,
                                           tombstone.SerializeEndKey(),
                                           tombstone.seq_, internal_comparator);
    }

    // Finish and check for builder errors
    tp = builder->GetTableProperties();
    bool empty = builder->NumEntries() == 0 && tp.num_range_deletions == 0;
    if (s.ok()) {
      s = c_iter.status();
    }
    if (separate_helper.builder) {
      if (!s.ok() || empty) {
        separate_helper.builder->Abandon();
      } else {
        s = finish_output_blob_sst();
      }
    }
    if (!s.ok() || empty) {
      builder->Abandon();
    } else {
      for (size_t i = 1; i < meta_vec->size(); ++i) {
        auto& blob = (*meta_vec)[i];
        assert(sst_meta()->prop.dependence.empty() ||
               blob.fd.GetNumber() >
                   sst_meta()->prop.dependence.back().file_number);
        sst_meta()->prop.dependence.emplace_back(
            Dependence{blob.fd.GetNumber(), blob.prop.num_entries});
      }
      auto shrinked_snapshots = sst_meta()->ShrinkSnapshot(snapshots);
      s = builder->Finish(&sst_meta()->prop, &shrinked_snapshots);

      ProcessFileMetaData("FlushOutput", sst_meta(), &tp, &ioptions,
                          &mutable_cf_options);
    }

    if (s.ok() && !empty) {
      uint64_t file_size = builder->FileSize();
      sst_meta()->fd.file_size = file_size;
      sst_meta()->marked_for_compaction =
          builder->NeedCompact() ? FileMetaData::kMarkedFromTableBuilder : 0;
      sst_meta()->prop.num_entries = builder->NumEntries();
      assert(sst_meta()->fd.GetFileSize() > 0);
      // refresh now that builder is finished
      tp = builder->GetTableProperties();
      if (table_properties_vec != nullptr) {
        assert(table_properties_vec->size() >= 1);
        table_properties_vec->front() = tp;
      }
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok() && !empty) {
      StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
      s = file_writer->Sync(ioptions.use_fsync);
    }
    if (s.ok() && !empty) {
      s = file_writer->Close();
    }

    if (s.ok() && !empty) {
      // this sst has no depend ...
      DependenceMap empty_dependence_map;
      assert(!sst_meta()->prop.is_map_sst());
      // Verify that the table is usable
      // We set for_compaction to false and don't OptimizeForCompactionTableRead
      // here because this is a special case after we finish the table building
      // No matter whether use_direct_io_for_flush_and_compaction is true,
      // we will regrad this verification as user reads since the goal is
      // to cache it here for further user reads
      ReadOptions ro;
      ro.fill_cache = false;
      for (auto& meta : *meta_vec) {
        std::unique_ptr<InternalIterator> it(table_cache->NewIterator(
            ro, env_options, meta, empty_dependence_map,
            nullptr /* range_del_agg */,
            mutable_cf_options.prefix_extractor.get(), nullptr,
            (internal_stats == nullptr) ? nullptr
                                        : internal_stats->GetFileReadHist(0),
            false /* for_compaction */, nullptr /* arena */,
            false /* skip_filter */, level));
        s = it->status();
        if (s.ok() && paranoid_file_checks) {
          for (it->SeekToFirst(); it->Valid(); it->Next()) {
          }
          s = it->status();
        }
        if (!s.ok()) {
          break;
        }
      }
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (!s.ok() || sst_meta()->fd.GetFileSize() == 0) {
    env->DeleteFile(fname);
  }

  // Output to event logger and fire events.
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger, ioptions.listeners, dbname, column_family_name, fname,
      job_id, sst_meta()->fd, tp, reason, s);

  return s;
}

// ZNS: A separate helper for constructing partitioned table: including Hot
// table, Warm Table and Hash Partitioned Table. The noticeable difference is
// that we have to maintain individual TableBuilder (WritableFileWriter) for
// each table type.
struct PartitionTableBuilderSeparateHelper : public SeparateHelper {
  // Each Writer is simply a calpsulation (more specifically, collection)
  // of FileWriter, TableBuilder and correspdoning meta information of a table.
  // Since we may maintain multiple Table within one SeparateHelper, such
  // calpsulation would be more convenient
  struct Writer {
    std::string fname;
    std::unique_ptr<WritableFileWriter> file_writer = nullptr;
    std::unique_ptr<TableBuilder> table_builder = nullptr;
    TableProperties tp;
    FileMetaData* meta = nullptr;
    TableProperties* curr_props = nullptr;
  };
  Writer hot_writer;
  Writer warm_writer;
  // Writer for each partition, 32 is the maximum possible value of partition
  Writer partition_writer[32];

  std::vector<FileMetaData>* output_meta = nullptr;
  std::vector<TableProperties>* prop = nullptr;
  std::unique_ptr<ValueExtractor> value_meta_extractor;

  // Oracle for hotness detection
  std::shared_ptr<Oracle> oracle;

  // Configurable parameters controlling the SeparateHelper
  //
  // For simplicity we use hard-coding here to specify the partiton number.
  // A more general implementation would require this field to be initialized
  // from configuration structs.
  uint32_t partition_num = 4;

  // If enabled, the keys identified as hot or warm would be written into
  // individual blobs.
  bool enable_hot_separation = true;

  // Get the type of a specific key
  KeyType CalculateKeyType(const Slice& key) {
    // TODO: Query the hotness set to determine its hotness
    ParsedInternalKey ikey;
    if (enable_hot_separation) {
      ParseInternalKey(key, &ikey);
      auto tmp = std::string(ikey.user_key.data(), ikey.user_key.size());
      auto t = oracle->ProbeKeyType(tmp, 0);
      if (t.IsHot() || t.IsWarm()) {
        return t;
      }
    }

    // Dealing with hash partition case
    murmur_hash hasher;
    return KeyType::Partition(hasher(key) % partition_num);
  }

  std::vector<Writer*> GetAllWriters() {
    std::vector<Writer*> ret;
    ret.emplace_back(&hot_writer);
    ret.emplace_back(&warm_writer);
    for (uint32_t i = 0; i < partition_num; ++i) {
      ret.emplace_back(&partition_writer[i]);
    }
    return ret;
  }

  Status (*trans_to_separate_callback)(void* args, const Slice& key,
                                       LazyBuffer& value) = nullptr;
  void* trans_to_separate_callback_args = nullptr;

  Status TransToSeparate(const Slice& internal_key, LazyBuffer& value,
                         const Slice& meta, bool is_merge,
                         bool is_index) override {
    return SeparateHelper::TransToSeparate(
        internal_key, value, value.file_number(), meta, is_merge, is_index,
        value_meta_extractor.get());
  }

  Status TransToSeparate(const Slice& internal_key,
                         LazyBuffer& value) override {
    if (trans_to_separate_callback == nullptr) {
      return Status::NotSupported();
    }
    return trans_to_separate_callback(trans_to_separate_callback_args,
                                      internal_key, value);
  }

  LazyBuffer TransToCombined(const Slice& /*user_key*/, uint64_t /*sequence*/,
                             const LazyBuffer& /*value*/) const override {
    assert(false);
    return LazyBuffer();
  }
};

Status BuildPartitionTable(
    const std::string& dbname, VersionSet* versions_, Env* env,
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const EnvOptions& env_options,
    TableCache* table_cache,
    InternalIterator* (*get_input_iter_callback)(void*, Arena&),
    void* get_input_iter_arg,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>> (
        *get_range_del_iters_callback)(void*),
    void* get_range_del_iters_arg, std::vector<FileMetaData>* meta_vec,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories_for_blob,
    uint32_t column_family_id, const std::string& column_family_name,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, const CompressionType compression,
    const CompressionOptions& compression_opts, bool paranoid_file_checks,
    InternalStats* internal_stats, TableFileCreationReason reason,
    EventLogger* event_logger, int job_id, const Env::IOPriority io_priority,
    std::vector<TableProperties>* table_properties_vec, int level,
    double compaction_load, const uint64_t creation_time,
    const uint64_t oldest_key_time, Env::WriteLifeTimeHint write_hint) {
  // std::cout << "[BuildPartitionTable]: " << std::this_thread::get_id()
  //           << std::endl;

  StopWatch sw(env, ioptions.statistics, ZNS_BUILD_PARTITION_TABLE);
  // ZnsLog(kCyan, "BuildPartitionTable::Start");
  // Defer d([]() { ZnsLog(kCyan, "BuildPartitionTable::End"); });

  assert((column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         column_family_name.empty());
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  Status s;
  assert(meta_vec->size() == 1);
  if (table_properties_vec != nullptr) {
    table_properties_vec->emplace_back();
  }

  // (ZNS): sst_meta is the first element of the meta_vec, what is this meta_vec
  // for? it seems that the sst_meta is used to construct the key sst of this
  // flush job, see how fname is constructed below
  auto sst_meta = [meta_vec] { return &meta_vec->front(); };
  Arena arena;
  ScopedArenaIterator iter(get_input_iter_callback(get_input_iter_arg, arena));
  iter->SeekToFirst();
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&internal_comparator, snapshots));
  for (auto& range_del_iter :
       get_range_del_iters_callback(get_range_del_iters_arg)) {
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }

  std::string fname =
      TableFileName(ioptions.cf_paths, sst_meta()->fd.GetNumber(),
                    sst_meta()->fd.GetPathId());
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      ioptions.listeners, dbname, column_family_name, fname, job_id, reason);
#endif  // !ROCKSDB_LITE
  TableProperties tp;
  struct {
    // The time spent on probe key type
    uint64_t probe_key_type_total_time = 0;
    uint64_t sep_hot_count = 0;
    uint64_t sep_warm_count = 0;
    uint64_t total_output_count = 0;
  } counter;

  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    // Create the table builder for KeySST
    TableBuilder* builder;
    std::unique_ptr<WritableFileWriter> file_writer;
    {
      std::unique_ptr<WritableFile> file = std::make_unique<WritableFile>();
#ifndef NDEBUG
      bool use_direct_writes = env_options.use_direct_writes;
      TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
      file->SetFileLevel(level);
      s = NewWritableFile(env, fname, &file, env_options);
      if (!s.ok()) {
        EventHelpers::LogAndNotifyTableFileCreationFinished(
            event_logger, ioptions.listeners, dbname, column_family_name, fname,
            job_id, sst_meta()->fd, tp, reason, s);
        return s;
      }
      file->SetIOPriority(io_priority);
      file->SetWriteLifeTimeHint(write_hint);

      file_writer.reset(new WritableFileWriter(std::move(file), fname,
                                               env_options, ioptions.statistics,
                                               ioptions.listeners));
      builder = NewTableBuilder(
          ioptions, mutable_cf_options, internal_comparator,
          int_tbl_prop_collector_factories, column_family_id,
          column_family_name, file_writer.get(), compression, compression_opts,
          level, compaction_load, nullptr /* compression_dict */,
          false /* skip_filters */, creation_time, oldest_key_time, kEssenceSst,
          env);
    }

    MergeHelper merge(env, internal_comparator.user_comparator(),
                      ioptions.merge_operator, nullptr, ioptions.info_log,
                      true /* internal key corruption is not ok */,
                      snapshots.empty() ? 0 : snapshots.back(),
                      snapshot_checker);

    // psh is short for "partition_separate_helper"
    PartitionTableBuilderSeparateHelper psh;
    if (ioptions.value_meta_extractor_factory != nullptr) {
      ValueExtractorContext context = {column_family_id};
      psh.value_meta_extractor =
          ioptions.value_meta_extractor_factory->CreateValueExtractor(context);
    }

    // Finish the output blob sst for a specific writer
    auto finish_output_blob_sst =
        [&](PartitionTableBuilderSeparateHelper::Writer* writer) {
          auto ftype =
              writer->file_writer->writable_file()->GetPlacementFileType();
          Status status;
          TableBuilder* blob_builder = writer->table_builder.get();
          FileMetaData* blob_meta = writer->meta;
          blob_meta->prop.num_entries = blob_builder->NumEntries();
          blob_meta->prop.num_deletions = 0;
          blob_meta->prop.purpose = kEssenceSst;
          blob_meta->prop.flags |= TablePropertyCache::kNoRangeDeletions;
          status = blob_builder->Finish(&blob_meta->prop, nullptr);
          TableProperties& tp = *writer->curr_props;
          if (status.ok()) {
            blob_meta->fd.file_size = blob_builder->FileSize();
            tp = blob_builder->GetTableProperties();
            blob_meta->prop.raw_key_size = tp.raw_key_size;
            blob_meta->prop.raw_value_size = tp.raw_value_size;
            StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
            status = writer->file_writer->Sync(ioptions.use_fsync);
          }
          if (status.ok()) {
            status = writer->file_writer->Close();
          }

          ZnsLog(kCyan, "Finish Blob: %s, Type: %s",
                 writer->file_writer->file_name().c_str(),
                 ftype.ToString().c_str());

          // (ZNS): Update the table properties to file system. Invoke this
          // function after Close() is called to ensure this file has a
          // determined belong zone
          if (status.ok()) {
            auto tp = blob_builder->GetTableProperties();
            auto fname = writer->file_writer->file_name();
            env->UpdateTableProperties(fname, &tp);
          }
          writer->file_writer.reset();
          EventHelpers::LogAndNotifyTableFileCreationFinished(
              event_logger, ioptions.listeners, dbname, column_family_name,
              writer->fname, job_id, blob_meta->fd, tp,
              TableFileCreationReason::kFlush, status);

          writer->table_builder.reset();
          return s;
        };

    size_t target_blob_file_size = MaxBlobSize(
        mutable_cf_options, ioptions.num_levels, ioptions.compaction_style);

    // The separate function invoked by PartitionTableSeparateHelper
    auto trans_to_separate_partition = [&](const Slice& key,
                                           LazyBuffer& value) {
      assert(value.file_number() == uint64_t(-1));
      Status status;
      PartitionTableBuilderSeparateHelper::Writer* writer = nullptr;

      auto start = env->NowMicros();
      auto key_type = psh.CalculateKeyType(key);
      counter.probe_key_type_total_time += (env->NowMicros() - start);

      counter.total_output_count++;

      // Pick a writer
      if (key_type.IsHot()) {
        writer = &psh.hot_writer;
        counter.sep_hot_count++;
      } else if (key_type.IsWarm()) {
        writer = &psh.warm_writer;
        counter.sep_warm_count++;
      } else if (key_type.IsPartition()) {
        writer = &psh.partition_writer[key_type.PartitionId()];
      } else {
        // Can not be a Cold type or unknown type in flush job
        assert(false);
      }
      assert(writer != nullptr);

      TableBuilder* blob_builder = writer->table_builder.get();
      FileMetaData* blob_meta = writer->meta;

      if (blob_builder != nullptr &&
          blob_builder->FileSize() > target_blob_file_size) {
        // TODO: Dealing with FinishOutputBlobSST function
        status = finish_output_blob_sst(writer);
        blob_builder = nullptr;
      }
      // Create a new blob file and an corresponding builder
      if (status.ok() && blob_builder == nullptr) {
        std::unique_ptr<WritableFile> blob_file =
            std::make_unique<WritableFile>();

#ifndef NDEBUG
        bool use_direct_writes = env_options.use_direct_writes;
        TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG

        // Add a new FileMeta structure for the newly created file
        psh.output_meta->emplace_back();
        blob_meta = writer->meta = &(psh.output_meta->back());
        // ZnsLog(kCyan, "[Writer %d Set Meta: %p]", key_type.PartitionId(),
        //        blob_meta);

        // Add a new property structure for the newly created file
        if (psh.prop == nullptr) {
          writer->curr_props = &writer->tp;
        } else {
          psh.prop->emplace_back();
          writer->curr_props = &psh.prop->back();
        }

        // Create file for this writer
        blob_meta->fd = FileDescriptor(versions_->NewFileNumber(),
                                       sst_meta()->fd.GetPathId(), 0);
        writer->fname =
            TableFileName(ioptions.cf_paths, blob_meta->fd.GetNumber(),
                          blob_meta->fd.GetPathId());
        blob_file->SetPlacementFileType(PlacementFileType(key_type));
        status = NewWritableFile(env, writer->fname, &blob_file, env_options);
        if (!status.ok()) {
          EventHelpers::LogAndNotifyTableFileCreationFinished(
              event_logger, ioptions.listeners, dbname, column_family_name,
              writer->fname, job_id, blob_meta->fd, TableProperties(), reason,
              status);
          return status;
        }

        // Set necessary information
        blob_file->SetIOPriority(io_priority);
        blob_file->SetWriteLifeTimeHint(Env::WriteLifeTimeHint::WLTH_EXTREME);

        writer->file_writer.reset(new WritableFileWriter(
            std::move(blob_file), writer->fname, env_options,
            ioptions.statistics, ioptions.listeners));
        writer->table_builder.reset(NewTableBuilder(
            ioptions, mutable_cf_options, internal_comparator,
            int_tbl_prop_collector_factories_for_blob, column_family_id,
            column_family_name, writer->file_writer.get(), compression,
            compression_opts, -1 /* level */, 0 /* compaction_load */, nullptr,
            true, 0, 0, kEssenceSst, env));
        blob_builder = writer->table_builder.get();
        ZnsLog(kCyan, "Create New Blob: %s, Type: %s",
               writer->file_writer->file_name().c_str(),
               key_type.ToString().c_str());
      }
      if (status.ok()) {
        status = blob_builder->Add(key, value);
        // ZnsLog(kCyan, "Writer%d Add Key: %lu", key_type.PartitionId(),
        //        *(uint64_t*)key.data());
      }
      if (status.ok()) {
        blob_meta->UpdateBoundaries(key, GetInternalKeySeqno(key));
        status = SeparateHelper::TransToSeparate(
            key, value, blob_meta->fd.GetNumber(), Slice(),
            GetInternalKeyType(key) == kTypeMerge, false,
            psh.value_meta_extractor.get());
      }
      return status;
    };

    // NOTE: We must first reserve enough space for meta_vec and
    // table_properties_vec. Otherwise the expansions of these vectors will
    // invalidate the pointer cached in Writer (and SeparateHelper)
    if (meta_vec != nullptr) {
      meta_vec->reserve(64);
    }
    if (table_properties_vec != nullptr) {
      table_properties_vec->reserve(64);
    }

    psh.output_meta = meta_vec;
    psh.prop = table_properties_vec;
    psh.oracle = env->GetOracle();

    // Set some related parameters for this SeparateHelper
    psh.partition_num = env_options.partition_num;

    BlobConfig blob_config = mutable_cf_options.get_blob_config();
    if (ioptions.table_factory->IsBuilderNeedSecondPass()) {
      blob_config.blob_size = size_t(-1);
    } else {
      psh.trans_to_separate_callback =
          c_style_callback(trans_to_separate_partition);
      psh.trans_to_separate_callback_args = &trans_to_separate_partition;
    }

    CompactionIterator c_iter(
        iter.get(), &psh, nullptr, internal_comparator.user_comparator(),
        &merge, kMaxSequenceNumber, &snapshots,
        earliest_write_conflict_snapshot, snapshot_checker, env,
        ShouldReportDetailedTime(env, ioptions.statistics),
        true /* internal key corruption is not ok */, range_del_agg.get(),
        nullptr, blob_config);

    c_iter.SeekToFirst();
    for (; s.ok() && c_iter.Valid(); c_iter.Next()) {
      s = builder->Add(c_iter.key(), c_iter.value());
      sst_meta()->UpdateBoundaries(c_iter.key(), c_iter.ikey().sequence);

      // TODO(noetzli): Update stats after flush, too.
      if (io_priority == Env::IO_HIGH &&
          IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        ThreadStatusUtil::SetThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
      }
    }

    auto range_del_it = range_del_agg->NewIterator();
    for (range_del_it->SeekToFirst(); s.ok() && range_del_it->Valid();
         range_del_it->Next()) {
      auto tombstone = range_del_it->Tombstone();
      auto kv = tombstone.Serialize();
      s = builder->AddTombstone(kv.first.Encode(), LazyBuffer(kv.second));
      sst_meta()->UpdateBoundariesForRange(kv.first,
                                           tombstone.SerializeEndKey(),
                                           tombstone.seq_, internal_comparator);
    }

    // Finish and check for builder errors
    tp = builder->GetTableProperties();
    bool empty = builder->NumEntries() == 0 && tp.num_range_deletions == 0;
    if (s.ok()) {
      s = c_iter.status();
    }

    // For each output writer that remains: Abandon it or finish it
    auto all_writers = psh.GetAllWriters();
    for (const auto& writer : all_writers) {
      if (writer->table_builder) {
        if (!s.ok() || empty) {
          writer->table_builder->Abandon();
        } else {
          s = finish_output_blob_sst(writer);
        }
      }
    }

    // (ZNS): TODO: The inheritance map may need a re-implementation
    if (!s.ok() || empty) {
      builder->Abandon();
    } else {
      for (size_t i = 1; i < meta_vec->size(); ++i) {
        auto& blob = (*meta_vec)[i];
        assert(sst_meta()->prop.dependence.empty() ||
               blob.fd.GetNumber() >
                   sst_meta()->prop.dependence.back().file_number);
        sst_meta()->prop.dependence.emplace_back(
            Dependence{blob.fd.GetNumber(), blob.prop.num_entries});
      }
      auto shrinked_snapshots = sst_meta()->ShrinkSnapshot(snapshots);
      s = builder->Finish(&sst_meta()->prop, &shrinked_snapshots);

      ProcessFileMetaData("FlushOutput", sst_meta(), &tp, &ioptions,
                          &mutable_cf_options);
    }

    if (s.ok() && !empty) {
      uint64_t file_size = builder->FileSize();
      sst_meta()->fd.file_size = file_size;
      sst_meta()->marked_for_compaction =
          builder->NeedCompact() ? FileMetaData::kMarkedFromTableBuilder : 0;
      sst_meta()->prop.num_entries = builder->NumEntries();
      assert(sst_meta()->fd.GetFileSize() > 0);
      // refresh now that builder is finished
      tp = builder->GetTableProperties();
      if (table_properties_vec != nullptr) {
        assert(table_properties_vec->size() >= 1);
        table_properties_vec->front() = tp;
      }
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok() && !empty) {
      StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
      s = file_writer->Sync(ioptions.use_fsync);
    }
    if (s.ok() && !empty) {
      s = file_writer->Close();
    }

    if (s.ok() && !empty) {
      // this sst has no depend ...
      DependenceMap empty_dependence_map;
      assert(!sst_meta()->prop.is_map_sst());
      // Verify that the table is usable
      // We set for_compaction to false and don't OptimizeForCompactionTableRead
      // here because this is a special case after we finish the table building
      // No matter whether use_direct_io_for_flush_and_compaction is true,
      // we will regrad this verification as user reads since the goal is
      // to cache it here for further user reads
      ReadOptions ro;
      ro.fill_cache = false;
      // Is this necessary?
      for (auto& meta : *meta_vec) {
        std::unique_ptr<InternalIterator> it(table_cache->NewIterator(
            ro, env_options, meta, empty_dependence_map,
            nullptr /* range_del_agg */,
            mutable_cf_options.prefix_extractor.get(), nullptr,
            (internal_stats == nullptr) ? nullptr
                                        : internal_stats->GetFileReadHist(0),
            false /* for_compaction */, nullptr /* arena */,
            false /* skip_filter */, level));
        s = it->status();
        if (s.ok() && paranoid_file_checks) {
          for (it->SeekToFirst(); it->Valid(); it->Next()) {
          }
          s = it->status();
        }
        if (!s.ok()) {
          break;
        }
      }
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (!s.ok() || sst_meta()->fd.GetFileSize() == 0) {
    env->DeleteFile(fname);
  }

  ioptions.statistics->measureTime(ZNS_BUILD_PARTITION_TABLE_PROBE,
                                   counter.probe_key_type_total_time);
  // env->GetOracle()->ReportProbeStats();
  ZnsLog(kCyan,
         "[BuildPartitionTable][Output: %lu][Separate Hot: %lu "
         "(%.2lf)][Separate Warm: %lu (%.2lf)]",
         counter.total_output_count, counter.sep_hot_count,
         (double)counter.sep_hot_count / counter.total_output_count,
         counter.sep_warm_count,
         (double)counter.sep_warm_count / counter.total_output_count);

  ROCKS_LOG_INFO(ioptions.info_log,
                 "[BuildPartitionTable][Output: %lu][Separate Hot: %lu "
                 "(%.2lf)][Separate Warm: %lu (%.2lf)]",
                 counter.total_output_count, counter.sep_hot_count,
                 (double)counter.sep_hot_count / counter.total_output_count,
                 counter.sep_warm_count,
                 (double)counter.sep_warm_count / counter.total_output_count);

  // Output to event logger and fire events.
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger, ioptions.listeners, dbname, column_family_name, fname,
      job_id, sst_meta()->fd, tp, reason, s);

  return s;
}

}  // namespace TERARKDB_NAMESPACE
