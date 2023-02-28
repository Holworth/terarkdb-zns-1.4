//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/compaction_job.h"
#include "db/dbformat.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/external_sst_file_ingestion_job.h"
#include "db/flush_job.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/log_writer.h"
#include "db/logs_with_prep_tracker.h"
#include "db/pre_release_callback.h"
#include "db/range_del_aggregator.h"
#include "db/read_callback.h"
#include "db/snapshot_checker.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "db/wal_manager.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "memtable_list.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/metrics_reporter.h"
#include "rocksdb/status.h"
#include "rocksdb/terark_namespace.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/scoped_arena_iterator.h"
#include "util/autovector.h"
#include "util/chash_set.h"
#include "util/event_logger.h"
#include "util/hash.h"
#include "util/repeatable_thread.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"
#include "util/trace_replay.h"
#include "utilities/console/server.h"
#include "utilities/trace/zbd_stat.h"

namespace TERARKDB_NAMESPACE {

class Arena;
class ArenaWrappedDBIter;
class InMemoryStatsHistoryIterator;
class MemTable;
class PersistentStatsHistoryIterator;
class PeriodicWorkScheduler;
#ifndef NDEBUG
class PeriodicWorkTestScheduler;
#endif  // !NDEBUG

class TableCache;
class Version;
class VersionEdit;
class VersionSet;
class WriteCallback;
struct JobContext;
struct ExternalSstFileInfo;
struct MemTableInfo;

const uint64_t kMicrosInSecond = 1000 * 1000;

class DBImpl : public DB {
 public:
  DBImpl(const DBOptions& options, const std::string& dbname,
         const bool seq_per_batch = false, const bool batch_per_txn = true);
  virtual ~DBImpl();

  using DB::Resume;
  virtual Status Resume() override;

  // Implementations of the DB interface
  using DB::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value) override;
  using DB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override;
  using DB::Delete;
  virtual Status Delete(const WriteOptions& options,
                        ColumnFamilyHandle* column_family,
                        const Slice& key) override;
  using DB::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key) override;
  using DB::Write;
  virtual Status Write(const WriteOptions& options,
                       WriteBatch* updates) override;

  using DB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     LazyBuffer* value) override;

  // Function that Get and KeyMayExist call with no_io true or false
  // Note: 'value_found' from KeyMayExist propagates here
  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* column_family,
                 const Slice& key, LazyBuffer* value,
                 bool* value_found = nullptr, ReadCallback* callback = nullptr);

  using DB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  virtual Status CreateColumnFamily(const ColumnFamilyOptions& cf_options,
                                    const std::string& column_family,
                                    ColumnFamilyHandle** handle) override;
  virtual Status CreateColumnFamilies(
      const ColumnFamilyOptions& cf_options,
      const std::vector<std::string>& column_family_names,
      std::vector<ColumnFamilyHandle*>* handles) override;
  virtual Status CreateColumnFamilies(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles) override;
  virtual Status DropColumnFamily(ColumnFamilyHandle* column_family) override;
  virtual Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  // Returns false if key doesn't exist in the database and true if it may.
  // If value_found is not passed in as null, then return the value if found in
  // memory. On return, if value was found, then value_found will be set to true
  // , otherwise false.
  using DB::KeyMayExist;
  virtual bool KeyMayExist(const ReadOptions& options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           std::string* value,
                           bool* value_found = nullptr) override;

  using DB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;
  virtual Status NewIterators(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators) override;
  ArenaWrappedDBIter* NewIteratorImpl(const ReadOptions& options,
                                      ColumnFamilyData* cfd,
                                      SequenceNumber snapshot,
                                      ReadCallback* read_callback,
                                      bool allow_refresh = true);

  virtual const Snapshot* GetSnapshot() override;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) override;
  using DB::GetProperty;
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                           const Slice& property, std::string* value) override;
  using DB::GetMapProperty;
  virtual bool GetMapProperty(
      ColumnFamilyHandle* column_family, const Slice& property,
      std::map<std::string, std::string>* value) override;
  using DB::GetIntProperty;
  virtual bool GetIntProperty(ColumnFamilyHandle* column_family,
                              const Slice& property, uint64_t* value) override;
  using DB::GetAggregatedIntProperty;
  virtual bool GetAggregatedIntProperty(const Slice& property,
                                        uint64_t* aggregated_value) override;
  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(
      ColumnFamilyHandle* column_family, const Range* range, int n,
      uint64_t* sizes, uint8_t include_flags = INCLUDE_FILES) override;
  using DB::GetApproximateMemTableStats;
  virtual void GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                           const Range& range,
                                           uint64_t* const count,
                                           uint64_t* const size) override;
  using DB::CompactRange;
  virtual Status CompactRange(const CompactRangeOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice* begin, const Slice* end) override;

  using DB::CompactFiles;
  virtual Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr) override;

  virtual Status PauseBackgroundWork() override;
  virtual Status ContinueBackgroundWork() override;

  virtual Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override;

  using DB::SetOptions;
  Status SetOptions(
      ColumnFamilyHandle* column_family,
      const std::unordered_map<std::string, std::string>& options_map) override;

  virtual Status SetDBOptions(
      const std::unordered_map<std::string, std::string>& options_map) override;

  using DB::NumberLevels;
  virtual int NumberLevels(ColumnFamilyHandle* column_family) override;
  using DB::MaxMemCompactionLevel;
  virtual int MaxMemCompactionLevel(ColumnFamilyHandle* column_family) override;
  using DB::Level0StopWriteTrigger;
  virtual int Level0StopWriteTrigger(
      ColumnFamilyHandle* column_family) override;
  virtual const std::string& GetName() const override;
  virtual Env* GetEnv() const override;
  using DB::GetOptions;
  virtual Options GetOptions(ColumnFamilyHandle* column_family) const override;
  using DB::GetDBOptions;
  virtual DBOptions GetDBOptions() const override;
  using DB::Flush;
  virtual Status Flush(const FlushOptions& options,
                       ColumnFamilyHandle* column_family) override;
  virtual Status Flush(
      const FlushOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;
  virtual Status FlushWAL(bool sync) override;
  bool TEST_WALBufferIsEmpty(bool lock = true);
  virtual Status SyncWAL() override;
  virtual Status LockWAL() override;
  virtual Status UnlockWAL() override;

  virtual SequenceNumber GetLatestSequenceNumber() const override;
  // REQUIRES: joined the main write queue if two_write_queues is disabled, and
  // the second write queue otherwise.
  virtual void SetLastPublishedSequence(SequenceNumber seq);
  // Returns LastSequence in last_seq_same_as_publish_seq_
  // mode and LastAllocatedSequence otherwise. This is useful when visiblility
  // depends also on data written to the WAL but not to the memtable.
  SequenceNumber TEST_GetLastVisibleSequence() const;

  virtual bool SetPreserveDeletesSequenceNumber(SequenceNumber seqnum) override;

  virtual Status GetDbIdentity(std::string& identity) const override;

  virtual Status GetDbSessionId(std::string& session_id) const override;
  ColumnFamilyHandle* DefaultColumnFamily() const override;

  ColumnFamilyHandle* PersistentStatsColumnFamily() const;

  virtual Status Close() override;

  Status GetStatsHistory(
      uint64_t start_time, uint64_t end_time,
      std::unique_ptr<StatsHistoryIterator>* stats_iterator) override;
#ifndef ROCKSDB_LITE
  using DB::ResetStats;
  virtual Status ResetStats() override;
  virtual Status DisableFileDeletions() override;
  virtual Status EnableFileDeletions(bool force) override;
  virtual int IsFileDeletionsEnabled() const;
  // All the returned filenames start with "/"
  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true) override;
  virtual Status GetSortedWalFiles(VectorLogPtr& files) override;

  virtual Status GetUpdatesSince(
      SequenceNumber seq_number, std::unique_ptr<TransactionLogIterator>* iter,
      const TransactionLogIterator::ReadOptions& read_options =
          TransactionLogIterator::ReadOptions()) override;
  virtual void SetGuardSeqno(SequenceNumber guard_seqno) override;
  virtual Status DeleteFile(std::string name) override;
  Status DeleteFilesInRanges(ColumnFamilyHandle* column_family,
                             const RangePtr* ranges, size_t n);

  virtual void GetLiveFilesMetaData(
      std::vector<LiveFileMetaData>* metadata) override;

  // Obtains the meta data of the specified column family of the DB.
  // Status::NotFound() will be returned if the current DB does not have
  // any column family match the specified name.
  // TODO(yhchiang): output parameter is placed in the end in this codebase.
  virtual void GetColumnFamilyMetaData(ColumnFamilyHandle* column_family,
                                       ColumnFamilyMetaData* metadata) override;

  Status SuggestCompactRange(ColumnFamilyHandle* column_family,
                             const Slice* begin, const Slice* end) override;

  Status SuggestCompactColumnFamily(ColumnFamilyHandle* column_family) override;

  Status PromoteL0(ColumnFamilyHandle* column_family,
                   int target_level) override;

  // Similar to Write() but will call the callback once on the single write
  // thread to determine whether it is safe to perform the write.
  virtual Status WriteWithCallback(const WriteOptions& write_options,
                                   WriteBatch* my_batch,
                                   WriteCallback* callback);

  // Returns the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into the current
  // memtables. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  //
  // If the earliest sequence number could not be determined,
  // kMaxSequenceNumber will be returned.
  //
  // If include_history=true, will also search Memtables in MemTableList
  // History.
  SequenceNumber GetEarliestMemTableSequenceNumber(SuperVersion* sv,
                                                   bool include_history);

  // For a given key, check to see if there are any records for this key
  // in the memtables, including memtable history.  If cache_only is false,
  // SST files will also be checked.
  //
  // If a key is found, *found_record_for_key will be set to true and
  // *seq will be set to the stored sequence number for the latest
  // operation on this key or kMaxSequenceNumber if unknown.
  // If no key is found, *found_record_for_key will be set to false.
  //
  // Note: If cache_only=false, it is possible for *seq to be set to 0 if
  // the sequence number has been cleared from the record.  If the caller is
  // holding an active db snapshot, we know the missing sequence must be less
  // than the snapshot's sequence number (sequence numbers are only cleared
  // when there are no earlier active snapshots).
  //
  // If NotFound is returned and found_record_for_key is set to false, then no
  // record for this key was found.  If the caller is holding an active db
  // snapshot, we know that no key could have existing after this snapshot
  // (since we do not compact keys that have an earlier snapshot).
  //
  // Returns OK or NotFound on success,
  // other status on unexpected error.
  // TODO(andrewkr): this API need to be aware of range deletion operations
  Status GetLatestSequenceForKey(SuperVersion* sv, const Slice& key,
                                 bool cache_only, SequenceNumber* seq,
                                 bool* found_record_for_key);

  using DB::IngestExternalFile;
  virtual Status IngestExternalFile(
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& external_files,
      const IngestExternalFileOptions& ingestion_options) override;

  virtual Status VerifyChecksum() override;

  using DB::StartTrace;
  virtual Status StartTrace(
      const TraceOptions& options,
      std::unique_ptr<TraceWriter>&& trace_writer) override;

  using DB::EndTrace;
  virtual Status EndTrace() override;
  Status TraceIteratorSeek(const uint32_t& cf_id, const Slice& key);
  Status TraceIteratorSeekForPrev(const uint32_t& cf_id, const Slice& key);
#endif  // ROCKSDB_LITE

  // Similar to GetSnapshot(), but also lets the db know that this snapshot
  // will be used for transaction write-conflict checking.  The DB can then
  // make sure not to compact any keys that would prevent a write-conflict from
  // being detected.
  const Snapshot* GetSnapshotForWriteConflictBoundary();

  // checks if all live files exist on file system and that their file sizes
  // match to our in-memory records
  virtual Status CheckConsistency(bool read_only);

  Status RunManualCompaction(ColumnFamilyData* cfd,
                             SeparationType separation_type, int input_level,
                             int output_level, uint32_t output_path_id,
                             uint32_t max_subcompactions, const Slice* begin,
                             const Slice* end,
                             const chash_set<uint64_t>* files_being_compact,
                             bool exclusive,
                             bool disallow_trivial_move = false);

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  InternalIterator* NewInternalIterator(
      Arena* arena, RangeDelAggregator* range_del_agg, SequenceNumber sequence,
      ColumnFamilyHandle* column_family = nullptr,
      SeparateHelper** separate_helper = nullptr);

  LogsWithPrepTracker* logs_with_prep_tracker() {
    return &logs_with_prep_tracker_;
  }

#ifndef NDEBUG
  // Extra methods (for testing) that are not in the public DB interface
  // Implemented in db_impl_debug.cc

  // Compact any files in the named level that overlap [*begin, *end]
  Status TEST_CompactRange(
      int level, const Slice* begin, const Slice* end,
      ColumnFamilyHandle* column_family = nullptr,
      SeparationType separation_type = kCompactionTransToSeparate,
      bool disallow_trivial_move = false);

  void TEST_SwitchWAL();

  bool TEST_UnableToReleaseOldestLog() { return unable_to_release_oldest_log_; }

  bool TEST_IsLogGettingFlushed() {
    return alive_log_files_.begin()->getting_flushed;
  }

  Status TEST_SwitchMemtable(ColumnFamilyData* cfd = nullptr);

  // Force current memtable contents to be flushed.
  Status TEST_FlushMemTable(bool wait = true, bool allow_write_stall = false,
                            ColumnFamilyHandle* cfh = nullptr);

  // Wait for memtable compaction
  Status TEST_WaitForFlushMemTable(ColumnFamilyHandle* column_family = nullptr);

  // Wait for any compaction
  // We add a bool parameter to wait for unscheduledCompactions_ == 0, but this
  // is only for the special test of CancelledCompactions
  Status TEST_WaitForCompact(bool waitUnscheduled = false);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes(
      ColumnFamilyHandle* column_family = nullptr);

  // Return the current manifest file no.
  uint64_t TEST_Current_Manifest_FileNo();

  // Returns the number that'll be assigned to the next file that's created.
  uint64_t TEST_Current_Next_FileNo();

  // get total level0 file size. Only for testing.
  uint64_t TEST_GetLevel0TotalSize();

  void TEST_GetFilesMetaData(ColumnFamilyHandle* column_family,
                             std::vector<std::vector<FileMetaData>>* metadata);

  void TEST_LockMutex();

  void TEST_UnlockMutex();

  // REQUIRES: mutex locked
  void* TEST_BeginWrite();

  // REQUIRES: mutex locked
  // pass the pointer that you got from TEST_BeginWrite()
  void TEST_EndWrite(void* w);

  uint64_t TEST_MaxTotalInMemoryState() const {
    return max_total_in_memory_state_;
  }

  size_t TEST_LogsToFreeSize();

  uint64_t TEST_LogfileNumber();

  uint64_t TEST_total_log_size() const { return total_log_size_; }

  // Returns column family name to ImmutableCFOptions map.
  Status TEST_GetAllImmutableCFOptions(
      std::unordered_map<std::string, const ImmutableCFOptions*>* iopts_map);

  // Return the lastest MutableCFOptions of a column family
  Status TEST_GetLatestMutableCFOptions(ColumnFamilyHandle* column_family,
                                        MutableCFOptions* mutable_cf_options);

  Cache* TEST_table_cache() { return table_cache_.get(); }

  WriteController& TEST_write_controler() { return write_controller_; }

  uint64_t TEST_FindMinLogContainingOutstandingPrep();
  uint64_t TEST_FindMinPrepLogReferencedByMemTable();
  size_t TEST_PreparedSectionCompletedSize();
  size_t TEST_LogsWithPrepSize();

  int TEST_BGCompactionsAllowed() const;
  int TEST_BGGarbageCollectionAllowed() const;
  int TEST_BGFlushesAllowed() const;
  size_t TEST_GetWalPreallocateBlockSize(uint64_t write_buffer_size) const;
  void TEST_WaitForStatsDumpRun(std::function<void()> callback) const;
  bool TEST_IsPersistentStatsEnabled() const;
  size_t TEST_EstiamteStatsHistorySize() const;

#endif  // NDEBUG

  struct BGJobLimits {
    int max_flushes;
    int max_compactions;
    int max_garbage_collections;
  };
  // Returns maximum background flushes and compactions allowed to be scheduled
  BGJobLimits GetBGJobLimits() const;
  // Need a static version that can be called during SanitizeOptions().
  static BGJobLimits GetBGJobLimits(int max_background_flushes,
                                    int max_background_compactions,
                                    int max_background_garbage_collections,
                                    int max_background_jobs,
                                    bool parallelize_compactions);
  int GetSubCompactionSlots(uint32_t max_subcompactions);

  // move logs pending closing from job_context to the DB queue and
  // schedule a purge
  // add superversion to the DB queue and schedule a purge
  void ScheduleBgFree(JobContext* job_context, SuperVersion* sv);

  uint64_t MinLogNumberToKeep();

  // Returns the lower bound file number for SSTs that won't be deleted, even if
  // they're obsolete. This lower bound is used internally to prevent newly
  // created flush/compaction output files from being deleted before they're
  // installed. This technique avoids the need for tracking the exact numbers of
  // files pending creation, although it prevents more files than necessary from
  // being deleted.
  uint64_t MinObsoleteSstNumberToKeep();

  // Returns the list of live files in 'live' and the list
  // of all files in the filesystem in 'candidate_files'.
  // If force == false and the last call was less than
  // db_options_.delete_obsolete_files_period_micros microseconds ago,
  // it will not fill up the job_context
  void FindObsoleteFiles(JobContext* job_context, bool force,
                         bool no_full_scan = false);

  // Diffs the files listed in filenames and those that do not
  // belong to live files are possibly removed. Also, removes all the
  // files in sst_delete_files and log_delete_files.
  // It is not necessary to hold the mutex when invoking this method.
  // If FindObsoleteFiles() was run, we need to also run
  // PurgeObsoleteFiles(), even if disable_delete_obsolete_files_ is true
  void PurgeObsoleteFiles(JobContext& background_contet,
                          bool schedule_only = false);

  void SchedulePurge();

  const SnapshotList& snapshots() const { return snapshots_; }

  const ImmutableDBOptions& immutable_db_options() const {
    return immutable_db_options_;
  }
  const EnvOptions& env_options() const { return env_options_; }

  void CancelAllBackgroundWork(bool wait);

  // Find Super version and reference it. Based on options, it might return
  // the thread local cached one.
  // Call ReturnAndCleanupSuperVersion() when it is no longer needed.
  SuperVersion* GetAndRefSuperVersion(ColumnFamilyData* cfd);

  // Similar to the previous function but looks up based on a column family id.
  // nullptr will be returned if this column family no longer exists.
  // REQUIRED: this function should only be called on the write thread or if the
  // mutex is held.
  SuperVersion* GetAndRefSuperVersion(uint32_t column_family_id);

  // Un-reference the super version and clean it up if it is the last reference.
  void CleanupSuperVersion(SuperVersion* sv);

  // Un-reference the super version and return it to thread local cache if
  // needed. If it is the last reference of the super version. Clean it up
  // after un-referencing it.
  void ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd, SuperVersion* sv);

  // Similar to the previous function but looks up based on a column family id.
  // nullptr will be returned if this column family no longer exists.
  // REQUIRED: this function should only be called on the write thread.
  void ReturnAndCleanupSuperVersion(uint32_t colun_family_id, SuperVersion* sv);

  // REQUIRED: this function should only be called on the write thread or if the
  // mutex is held.  Return value only valid until next call to this function or
  // mutex is released.
  ColumnFamilyHandle* GetColumnFamilyHandle(uint32_t column_family_id);

  // Same as above, should called without mutex held and not on write thread.
  std::unique_ptr<ColumnFamilyHandle> GetColumnFamilyHandleUnlocked(
      uint32_t column_family_id);

  // Returns the number of currently running flushes.
  // REQUIREMENT: mutex_ must be held when calling this function.
  int num_running_flushes() {
    mutex_.AssertHeld();
    return num_running_flushes_;
  }

  // Returns the number of currently running compactions.
  // REQUIREMENT: mutex_ must be held when calling this function.
  int num_running_compactions() {
    mutex_.AssertHeld();
    return num_running_compactions_;
  }

  const WriteController& write_controller() { return write_controller_; }

  InternalIterator* NewInternalIterator(
      const ReadOptions&, ColumnFamilyData* cfd, SuperVersion* super_version,
      Arena* arena, RangeDelAggregator* range_del_agg, SequenceNumber sequence,
      SeparateHelper** separate_helper = nullptr);

  // hollow transactions shell used for recovery.
  // these will then be passed to TransactionDB so that
  // locks can be reacquired before writing can resume.
  struct RecoveredTransaction {
    std::string name_;
    bool unprepared_;

    struct BatchInfo {
      uint64_t log_number_;
      // TODO(lth): For unprepared, the memory usage here can be big for
      // unprepared transactions. This is only useful for rollbacks, and we
      // can in theory just keep keyset for that.
      WriteBatch* batch_;
      // Number of sub-batches. A new sub-batch is created if txn attempts to
      // insert a duplicate key,seq to memtable. This is currently used in
      // WritePreparedTxn/WriteUnpreparedTxn.
      size_t batch_cnt_;
    };

    // This maps the seq of the first key in the batch to BatchInfo, which
    // contains WriteBatch and other information relevant to the batch.
    //
    // For WriteUnprepared, batches_ can have size greater than 1, but for
    // other write policies, it must be of size 1.
    std::map<SequenceNumber, BatchInfo> batches_;

    explicit RecoveredTransaction(const uint64_t log, const std::string& name,
                                  WriteBatch* batch, SequenceNumber seq,
                                  size_t batch_cnt, bool unprepared)
        : name_(name), unprepared_(unprepared) {
      batches_[seq] = {log, batch, batch_cnt};
    }

    ~RecoveredTransaction() {
      for (auto& it : batches_) {
        delete it.second.batch_;
      }
    }

    void AddBatch(SequenceNumber seq, uint64_t log_number, WriteBatch* batch,
                  size_t batch_cnt, bool unprepared) {
      assert(batches_.count(seq) == 0);
      batches_[seq] = {log_number, batch, batch_cnt};
      // Prior state must be unprepared, since the prepare batch must be the
      // last batch.
      assert(unprepared_);
      unprepared_ = unprepared;
    }
  };

  bool allow_2pc() const { return immutable_db_options_.allow_2pc; }

  const std::string& bytedance_tags() const { return bytedance_tags_; }

  using QPSReporter = CountReporterHandle&;
  QPSReporter seek_qps_reporter() { return seek_qps_reporter_; }
  QPSReporter seekforprev_qps_reporter() { return seekforprev_qps_reporter_; }
  QPSReporter next_qps_reporter() { return next_qps_reporter_; }
  QPSReporter prev_qps_reporter() { return prev_qps_reporter_; }

  using LatencyReporter = HistReporterHandle&;
  LatencyReporter seek_latency_reporter() { return seek_latency_reporter_; }
  LatencyReporter seekforprev_latency_reporter() {
    return seekforprev_latency_reporter_;
  }
  LatencyReporter next_latency_reporter() { return next_latency_reporter_; }
  LatencyReporter prev_latency_reporter() { return prev_latency_reporter_; }

  using ThroughputReporter = CountReporterHandle&;
  using DistributionReporter = HistReporterHandle&;

  std::unordered_map<std::string, RecoveredTransaction*>
  recovered_transactions() {
    return recovered_transactions_;
  }

  RecoveredTransaction* GetRecoveredTransaction(const std::string& name) {
    auto it = recovered_transactions_.find(name);
    if (it == recovered_transactions_.end()) {
      return nullptr;
    } else {
      return it->second;
    }
  }

  void InsertRecoveredTransaction(const uint64_t log, const std::string& name,
                                  WriteBatch* batch, SequenceNumber seq,
                                  size_t batch_cnt, bool unprepared_batch) {
    // For WriteUnpreparedTxn, InsertRecoveredTransaction is called multiple
    // times for every unprepared batch encountered during recovery.
    //
    // If the transaction is prepared, then the last call to
    // InsertRecoveredTransaction will have unprepared_batch = false.
    auto rtxn = recovered_transactions_.find(name);
    if (rtxn == recovered_transactions_.end()) {
      recovered_transactions_[name] = new RecoveredTransaction(
          log, name, batch, seq, batch_cnt, unprepared_batch);
    } else {
      rtxn->second->AddBatch(seq, log, batch, batch_cnt, unprepared_batch);
    }
    logs_with_prep_tracker_.MarkLogAsContainingPrepSection(log);
  }

  void DeleteRecoveredTransaction(const std::string& name) {
    auto it = recovered_transactions_.find(name);
    assert(it != recovered_transactions_.end());
    auto* trx = it->second;
    recovered_transactions_.erase(it);
    for (const auto& info : trx->batches_) {
      logs_with_prep_tracker_.MarkLogAsHavingPrepSectionFlushed(
          info.second.log_number_);
    }
    delete trx;
  }

  void DeleteAllRecoveredTransactions() {
    for (auto it = recovered_transactions_.begin();
         it != recovered_transactions_.end(); it++) {
      delete it->second;
    }
    recovered_transactions_.clear();
  }

  void AddToLogsToFreeQueue(log::Writer* log_writer) {
    logs_to_free_queue_.push_back(log_writer);
  }

  void AddToSuperVersionToFreeQueue(SuperVersion* sv) {
    superversion_to_free_queue_.push_back(sv);
  }

  void SetSnapshotChecker(SnapshotChecker* snapshot_checker);

  // Not thread-safe.
  void SetRecoverableStatePreReleaseCallback(PreReleaseCallback* callback);

  InstrumentedMutex* mutex() { return &mutex_; }

  Status NewDB();

  // This is to be used only by internal rocksdb classes.
  static Status Open(const DBOptions& db_options, const std::string& name,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
                     const bool seq_per_batch, const bool batch_per_txn);

  static Status CreateAndNewDirectory(Env* env, const std::string& dirname,
                                      std::unique_ptr<Directory>* directory);
  // find stats map from stats_history_ with smallest timestamp in
  // the range of [start_time, end_time)
  bool FindStatsByTime(uint64_t start_time, uint64_t end_time,
                       uint64_t* new_time,
                       std::map<std::string, uint64_t>* stats_map);

  // Print information of all tombstones of all iterators to the std::string
  // This is only used by ldb. The output might be capped. Tombstones
  // printed out are not guaranteed to be in any order.
  Status TablesRangeTombstoneSummary(ColumnFamilyHandle* column_family, int,
                                     std::string* out_str);

#ifndef NDEBUG

  Status TEST_FlushMemTable(ColumnFamilyData* cfd,
                            const FlushOptions& flush_opts);

  // Flush (multiple) ColumnFamilyData without using ColumnFamilyHandle. This
  // is because in certain cases, we can flush column families, wait for the
  // flush to complete, but delete the column family handle before the wait
  // finishes. For example in CompactRange.
  Status TEST_AtomicFlushMemTables(const autovector<ColumnFamilyData*>& cfds,
                                   const FlushOptions& flush_opts);
  size_t TEST_EstimateInMemoryStatsHistorySize() const;

  VersionSet* TEST_GetVersionSet() const { return versions_.get(); }

#ifndef ROCKSDB_LITE
  PeriodicWorkTestScheduler* TEST_GetPeriodicWorkScheduler() const;
#endif  // !ROCKSDB_LITE

#endif  // NDEBUG

  bool IsShuttingDown() {
    return shutting_down_.load(std::memory_order_acquire);
  }

  // warm up index cache when open the db
  void WarmUpDBIndexCache();

  // persist stats to column family "_persistent_stats"
  void PersistStats();

  // dump rocksdb.stats to LOG
  void DumpStats();

  // flush LOG out of application buffer
  void FlushInfoLog();

  void ScheduleTtlGC();

#ifdef WITH_ZENFS
  struct ZenFSStatisticsStatus {
    uint64_t used = 0;
    uint64_t free = 0;
    uint64_t garbage = 0;
    uint64_t total = 0;
    uint64_t empty_zone_cnt = 0;
    uint64_t reclaim = 0;
    uint64_t occupy = 0;
  };

  // Get Current ZenFS Statistics
  ZenFSStatisticsStatus GetZenFSStatistics(const BDZenFSStat& zenfs_stat);

  //====================================================================
  // Implementation of naive ZenFS Garbage collection
  //====================================================================
  // schedule Metrics Reporter background.
  void ScheduleMetricsReporter();
  // schedule GC by polling ZNS zone status
  void ScheduleZNSGC();
  // (kqh): Report ZNS status
  void ScheduleZNSStatusReporter();
  // (kqh): Pick zones for migration during ZNS GC
  void PickMigrationZone(const std::vector<BDZoneStat>& zone_stat,
                         std::set<uint64_t>* picked_zones);
  // (kqh): Do Compaction work for a zone to reclaim the free space
  void MaybeDoZoneCompaction();
  int force_gc_count_ = 0;
  int schedule_gc_count_ = 0;
  int regular_gc_count_ = 0;
  int compact_zone_count_ = 0;
#endif

 protected:
  Env* const env_;
  const std::string dbname_;
  std::string db_id_;
  // db_session_id_ is an identifier that gets reset
  // every time the DB is opened
  std::string db_session_id_;
  std::unique_ptr<VersionSet> versions_;
  // Flag to check whether we allocated and own the info log file
  bool own_info_log_;
  const DBOptions initial_db_options_;
  const ImmutableDBOptions immutable_db_options_;
  MutableDBOptions mutable_db_options_;
  Statistics* stats_;
  std::unordered_map<std::string, RecoveredTransaction*>
      recovered_transactions_;
  std::unique_ptr<Tracer> tracer_;
  InstrumentedMutex trace_mutex_;

  // Except in DB::Open(), WriteOptionsFile can only be called when:
  // Persist options to options file.
  // If need_mutex_lock = false, the method will lock DB mutex.
  // If need_enter_write_thread = false, the method will enter write thread.
  Status WriteOptionsFile(bool need_mutex_lock, bool need_enter_write_thread);

  // The following two functions can only be called when:
  // 1. WriteThread::Writer::EnterUnbatched() is used.
  // 2. db_mutex is NOT held
  Status RenameTempFileToOptionsFile(const std::string& file_name);
  Status DeleteObsoleteOptionsFiles();

  void NotifyOnFlushBegin(ColumnFamilyData* cfd,
                          const MutableCFOptions& mutable_cf_options,
                          int job_id);

  void NotifyOnFlushCompleted(ColumnFamilyData* cfd,
                              const std::vector<FileMetaData>& file_meta,
                              const MutableCFOptions& mutable_cf_options,
                              int job_id,
                              const std::vector<TableProperties>& prop);

  void NotifyOnCompactionBegin(ColumnFamilyData* cfd, Compaction* c,
                               const Status& st,
                               const CompactionJobStats& job_stats, int job_id);

  void NotifyOnCompactionCompleted(ColumnFamilyData* cfd, Compaction* c,
                                   const Status& st,
                                   const CompactionJobStats& job_stats,
                                   int job_id);
  void NotifyOnMemTableSealed(ColumnFamilyData* cfd,
                              const MemTableInfo& mem_table_info);

#ifndef ROCKSDB_LITE
  void NotifyOnExternalFileIngested(
      ColumnFamilyData* cfd, const ExternalSstFileIngestionJob& ingestion_job);
#endif  // !ROCKSDB_LITE

  void NewThreadStatusCfInfo(ColumnFamilyData* cfd) const;

  void EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const;

  void EraseThreadStatusDbInfo() const;

  // If disable_memtable is set the application logic must guarantee that the
  // batch will still be skipped from memtable during the recovery. An excption
  // to this is seq_per_batch_ mode, in which since each batch already takes one
  // seq, it is ok for the batch to write to memtable during recovery as long as
  // it only takes one sequence number: i.e., no duplicate keys.
  // In WriteCommitted it is guarnateed since disable_memtable is used for
  // prepare batch which will be written to memtable later during the commit,
  // and in WritePrepared it is guaranteed since it will be used only for WAL
  // markers which will never be written to memtable. If the commit marker is
  // accompanied with CommitTimeWriteBatch that is not written to memtable as
  // long as it has no duplicate keys, it does not violate the one-seq-per-batch
  // policy.
  // batch_cnt is expected to be non-zero in seq_per_batch mode and
  // indicates the number of sub-patches. A sub-patch is a subset of the write
  // batch that does not have duplicate keys.
  Status WriteImpl(const WriteOptions& options, WriteBatch* updates,
                   WriteCallback* callback = nullptr,
                   uint64_t* log_used = nullptr, uint64_t log_ref = 0,
                   bool disable_memtable = false, uint64_t* seq_used = nullptr,
                   size_t batch_cnt = 0,
                   PreReleaseCallback* pre_release_callback = nullptr);

  Status PipelinedWriteImpl(const WriteOptions& options, WriteBatch* updates,
                            WriteCallback* callback = nullptr,
                            uint64_t* log_used = nullptr, uint64_t log_ref = 0,
                            bool disable_memtable = false,
                            uint64_t* seq_used = nullptr);

  // batch_cnt is expected to be non-zero in seq_per_batch mode and indicates
  // the number of sub-patches. A sub-patch is a subset of the write batch that
  // does not have duplicate keys.
  Status WriteImplWALOnly(const WriteOptions& options, WriteBatch* updates,
                          WriteCallback* callback = nullptr,
                          uint64_t* log_used = nullptr, uint64_t log_ref = 0,
                          uint64_t* seq_used = nullptr, size_t batch_cnt = 0,
                          PreReleaseCallback* pre_release_callback = nullptr);

  // write cached_recoverable_state_ to memtable if it is not empty
  // The writer must be the leader in write_thread_ and holding mutex_
  Status WriteRecoverableState();

  // Actual implementation of Close()
  Status CloseImpl();
  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  // recovered_seq is set to less than kMaxSequenceNumber if the log's tail is
  // skipped.
  virtual Status Recover(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      bool read_only = false, bool error_if_log_file_exist = false,
      bool error_if_data_exists_in_logs = false);

  virtual bool OwnTablesAndLogs() const { return true; }

  // REQUIRES: db mutex held when calling this function, but the db mutex can
  // be released and re-acquired. Db mutex will be held when the function
  // returns.
  // After best-efforts recovery, there may be SST files in db/cf paths that are
  // not referenced in the MANIFEST. We delete these SST files. In the
  // meantime, we find out the largest file number present in the paths, and
  // bump up the version set's next_file_number_ to be 1 + largest_file_number.
  Status FinishBestEffortsRecovery();

  // SetDbSessionId() should be called in the constuctor DBImpl()
  // to ensure that db_session_id_ gets updated every time the DB is opened
  void SetDbSessionId();

 private:
  friend class DB;
  friend class ErrorHandler;
  friend class InternalStats;
  friend class PessimisticTransaction;
  friend class TransactionBaseImpl;
  friend class WriteCommittedTxn;
  friend class WritePreparedTxn;
  friend class WritePreparedTxnDB;
  friend class WriteBatchWithIndex;
  friend class WriteUnpreparedTxnDB;
  friend class WriteUnpreparedTxn;

#ifndef ROCKSDB_LITE
  friend class ForwardIterator;
#endif
  friend struct SuperVersion;
  friend class CompactedDBImpl;
  friend class DBTest_ConcurrentFlushWAL_Test;
  friend class DBTest_MixedSlowdownOptionsStop_Test;
  friend class DBCompactionTest_CompactBottomLevelFilesWithDeletions_Test;
  friend class DBCompactionTest_CompactionDuringShutdown_Test;
  friend class StatsHistoryTest_PersistentStatsCreateColumnFamilies_Test;
#ifndef NDEBUG
  friend class DBTest2_ReadCallbackTest_Test;
  friend class WriteCallbackTest_WriteWithCallbackTest_Test;
  friend class XFTransactionWriteHandler;
  friend class WriteUnpreparedTransactionTest_RecoveryTest_Test;
#endif
  struct CompactionState;

  struct WriteContext {
    SuperVersionContext superversion_context;
    autovector<MemTable*> memtables_to_free;
    LogBuffer info_buffer;
    LogBuffer warn_buffer;

    explicit WriteContext(Logger* logger, bool create_superversion = false)
        : superversion_context(create_superversion),
          info_buffer(InfoLogLevel::INFO_LEVEL, logger),
          warn_buffer(InfoLogLevel::WARN_LEVEL, logger) {}

    void FlushBufferToLog() {
      info_buffer.FlushBufferToLog();
      warn_buffer.FlushBufferToLog();
    }

    ~WriteContext() {
      superversion_context.Clean();
      for (auto& m : memtables_to_free) {
        delete m;
      }
      FlushBufferToLog();
    }
  };

  struct PrepickedCompaction;
  struct PurgeFileInfo;

  // Initialize the built-in column family for persistent stats. Depending on
  // whether on-disk persistent stats have been enabled before, it may either
  // create a new column family and column family handle or just a column family
  // handle.
  // Required: DB mutex held
  Status InitPersistStatsColumnFamily();

  // Persistent Stats column family has two format version key which are used
  // for compatibility check. Write format version if it's created for the
  // first time, read format version and check compatibility if recovering
  // from disk. This function requires DB mutex held at entrance but may
  // release and re-acquire DB mutex in the process.
  // Required: DB mutex held
  Status PersistentStatsProcessFormatVersion();

  Status ResumeImpl();

  void MaybeIgnoreError(Status* s) const;

  const Status CreateArchivalDirectory();

  autovector<Status> CreateColumnFamilyImpl(
      autovector<const ColumnFamilyOptions*> cf_options,
      autovector<const std::string*> cf_name,
      autovector<ColumnFamilyHandle**> handle);

  Status DropColumnFamilyImpl(ColumnFamilyHandle* column_family);

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();
  // Delete obsolete files and log status and information of file deletion
  void DeleteObsoleteFileImpl(int job_id, const std::string& fname,
                              const std::string& path_to_sync, FileType type,
                              uint64_t number);

  // Background process needs to call
  //     auto x = CaptureCurrentFileNumberInPendingOutputs()
  //     auto file_num = versions_->NewFileNumber();
  //     <do something>
  //     ReleaseFileNumberFromPendingOutputs(x)
  // This will protect any file with number `file_num` or greater from being
  // deleted while <do something> is running.
  // -----------
  // This function will capture current file number and append it to
  // pending_outputs_. This will prevent any background process to delete any
  // file created after this point.
  std::list<uint64_t>::iterator CaptureCurrentFileNumberInPendingOutputs();
  // This function should be called with the result of
  // CaptureCurrentFileNumberInPendingOutputs(). It then marks that any file
  // created between the calls CaptureCurrentFileNumberInPendingOutputs() and
  // ReleaseFileNumberFromPendingOutputs() can now be deleted (if it's not live
  // and blocked by any other pending_outputs_ calls)
  void ReleaseFileNumberFromPendingOutputs(std::list<uint64_t>::iterator v);

  Status SyncClosedLogs(JobContext* job_context);

  // Flush the in-memory write buffer to storage.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful. Then
  // installs a new super version for the column family.
  Status FlushMemTableToOutputFile(ColumnFamilyData* cfd,
                                   const MutableCFOptions& mutable_cf_options,
                                   bool* madeProgress, JobContext* job_context,
                                   SuperVersionContext* superversion_context,
                                   LogBuffer* log_buffer,
                                   VersionEdit::ApplyCallback apply_callback);

  // Argument required by background flush thread.
  struct BGFlushArg {
    BGFlushArg()
        : cfd_(nullptr), max_memtable_id_(0), superversion_context_(nullptr) {}
    BGFlushArg(ColumnFamilyData* cfd, uint64_t max_memtable_id,
               SuperVersionContext* superversion_context)
        : cfd_(cfd),
          max_memtable_id_(max_memtable_id),
          superversion_context_(superversion_context) {}

    // Column family to flush.
    ColumnFamilyData* cfd_;
    // Maximum ID of memtable to flush. In this column family, memtables with
    // IDs smaller than this value must be flushed before this flush completes.
    uint64_t max_memtable_id_;
    // Pointer to a SuperVersionContext object. After flush completes, RocksDB
    // installs a new superversion for the column family. This operation
    // requires a SuperVersionContext object (currently embedded in JobContext).
    SuperVersionContext* superversion_context_;
  };

  // Flush the memtables of (multiple) column families to multiple files on
  // persistent storage.
  Status FlushMemTablesToOutputFiles(
      const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
      JobContext* job_context, LogBuffer* log_buffer);

  Status AtomicFlushMemTablesToOutputFiles(
      const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
      JobContext* job_context, LogBuffer* log_buffer);

  // REQUIRES: log_numbers are sorted in ascending order
  Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers,
                         SequenceNumber* next_sequence, bool read_only);

  // The following two methods are used to flush a memtable to
  // storage. The first one is used at database RecoveryTime (when the
  // database is opened) and is heavyweight because it holds the mutex
  // for the entire period. The second method WriteLevel0Table supports
  // concurrent flush memtables to storage.
  Status WriteLevel0TableForRecovery(int job_id, ColumnFamilyData* cfd,
                                     MemTable* mem, VersionEdit* edit);

  // Restore alive_log_files_ and total_log_size_ after recovery.
  // It needs to run only when there's no flush during recovery
  // (e.g. avoid_flush_during_recovery=true). May also trigger flush
  // in case total_log_size > max_total_wal_size.
  Status RestoreAliveLogFiles(const std::vector<uint64_t>& log_numbers,
                              const std::vector<SequenceNumber>& log_seqs);

  // num_bytes: for slowdown case, delay time is calculated based on
  //            `num_bytes` going through.
  Status DelayWrite(uint64_t num_bytes, const WriteOptions& write_options);

  Status ThrottleLowPriWritesIfNeeded(const WriteOptions& write_options,
                                      WriteBatch* my_batch);

  Status ScheduleFlushes(WriteContext* context);

  Status NewLogWriter(std::unique_ptr<log::Writer>* new_log,
                      uint64_t recycle_log_number, const DBOptions& db_options,
                      Env::WriteLifeTimeHint write_hint);

  void FillLogWriterPool();

  Status SwitchMemtable(ColumnFamilyData* cfd, WriteContext* context);

  // Force current memtable contents to be flushed.
  Status FlushMemTable(const autovector<ColumnFamilyData*>& column_family_datas,
                       const FlushOptions& options, FlushReason flush_reason,
                       bool writes_stopped = false);

  // Wait until flushing this column family won't stall writes
  Status WaitUntilFlushWouldNotStallWrites(ColumnFamilyData* cfd,
                                           bool* flush_needed);

  // Wait for memtable flushed.
  // If flush_memtable_id is non-null, wait until the memtable with the ID
  // gets flush. Otherwise, wait until the column family don't have any
  // memtable pending flush.
  // resuming_from_bg_err indicates whether the caller is attempting to resume
  // from background error.
  Status WaitForFlushMemTable(ColumnFamilyData* cfd,
                              const uint64_t* flush_memtable_id = nullptr,
                              bool resuming_from_bg_err = false) {
    return WaitForFlushMemTables({cfd}, {flush_memtable_id},
                                 resuming_from_bg_err);
  }
  // Wait for memtables to be flushed for multiple column families.
  Status WaitForFlushMemTables(
      const autovector<ColumnFamilyData*>& cfds,
      const autovector<const uint64_t*>& flush_memtable_ids,
      bool resuming_from_bg_err);

  // REQUIRES: mutex locked
  Status SwitchWAL(WriteContext* write_context);

  // REQUIRES: mutex locked
  Status HandleWriteBufferFull(WriteContext* write_context);

  // REQUIRES: mutex locked
  Status HandleMaxWalSize(WriteContext* write_context);

  // REQUIRES: mutex locked
  Status PreprocessWrite(const WriteOptions& write_options, bool* need_log_sync,
                         WriteContext* write_context);

  WriteBatch* MergeBatch(const WriteThread::WriteGroup& write_group,
                         WriteBatch* tmp_batch, size_t* write_with_wal,
                         WriteBatch** to_be_cached_state);

  Status WriteToWAL(const WriteBatch& merged_batch, log::Writer* log_writer,
                    uint64_t* log_used, uint64_t* log_size);

  Status WriteToWAL(const WriteThread::WriteGroup& write_group,
                    log::Writer* log_writer, uint64_t* log_used,
                    bool need_log_sync, bool need_log_dir_sync,
                    SequenceNumber sequence);

  Status ConcurrentWriteToWAL(const WriteThread::WriteGroup& write_group,
                              uint64_t* log_used, SequenceNumber* last_sequence,
                              size_t seq_inc);

  // Used by WriteImpl to update bg_error_ if paranoid check is enabled.
  void WriteStatusCheck(const Status& status);

  // Used by WriteImpl to update bg_error_ in case of memtable insert error.
  void MemTableInsertStatusCheck(const Status& memtable_insert_status);

#ifndef ROCKSDB_LITE

  Status CompactFilesImpl(const CompactionOptions& compact_options,
                          ColumnFamilyData* cfd, Version* version,
                          const std::vector<std::string>& input_file_names,
                          std::vector<std::string>* const output_file_names,
                          const int output_level, int output_path_id,
                          JobContext* job_context, LogBuffer* log_buffer);

  // Wait for current IngestExternalFile() calls to finish.
  // REQUIRES: mutex_ held
  void WaitForIngestFile();

#else
  // IngestExternalFile is not supported in ROCKSDB_LITE so this function
  // will be no-op
  void WaitForIngestFile() {}
#endif  // ROCKSDB_LITE

  ColumnFamilyData* GetColumnFamilyDataByName(const std::string& cf_name);

  void MaybeScheduleFlushOrCompaction();

  // A flush request specifies the column families to flush as well as the
  // largest memtable id to persist for each column family. Once all the
  // memtables whose IDs are smaller than or equal to this per-column-family
  // specified value, this flush request is considered to have completed its
  // work of flushing this column family. After completing the work for all
  // column families in this request, this flush is considered complete.
  typedef autovector<std::pair<ColumnFamilyData*, uint64_t>> FlushRequest;
  typedef autovector<FlushRequest> FlushRequestVec;

  // process atomic flush group
  void ProcessAtomicFlushGroup(autovector<ColumnFamilyData*>* cfds,
                               FlushRequestVec* req);

  // REQUIRES: mutex locked and in write thread.
  void PrepareFlushReqVec(FlushRequestVec& req, bool force_flush);

  void SchedulePendingFlush(const FlushRequestVec& req,
                            FlushReason flush_reason);
  void SchedulePendingCompaction(ColumnFamilyData* cfd);
  void SchedulePendingGarbageCollection(ColumnFamilyData* cfd);
  void SchedulePendingPurge(const std::string& fname,
                            const std::string& dir_to_sync, FileType type,
                            uint64_t number, int job_id);
  static void BGWorkCompaction(void* arg);
  static void BGWorkGarbageCollection(void* arg);
  // Runs a pre-chosen universal compaction involving bottom level in a
  // separate, bottom-pri thread pool.
  static void BGWorkBottomCompaction(void* arg);
  static void BGWorkFlush(void* db);
  static void BGWorkPurge(void* arg);
  static void UnscheduleCallback(void* arg);
  void BackgroundCallCompaction(PrepickedCompaction* prepicked_compaction,
                                Env::Priority bg_thread_pri);
  void BackgroundCallGarbageCollection();
  void BackgroundCallFlush();
  void BackgroundCallPurge();
  Status BackgroundCompaction(bool* madeProgress, JobContext* job_context,
                              LogBuffer* log_buffer,
                              PrepickedCompaction* prepicked_compaction);
  Status BackgroundGarbageCollection(bool* madeProgress,
                                     JobContext* job_context,
                                     LogBuffer* log_buffer);
  Status BackgroundFlush(bool* madeProgress, JobContext* job_context,
                         LogBuffer* log_buffer, FlushReason* reason);

  bool EnoughRoomForCompaction(ColumnFamilyData* cfd,
                               const std::vector<CompactionInputFiles>& inputs,
                               bool* sfm_bookkeeping, LogBuffer* log_buffer);

  // Schedule background tasks
  void StartPeriodicWorkScheduler();

  void PrintStatistics();

  size_t EstimateInMemoryStatsHistorySize() const;

  // Return the minimum empty level that could hold the total data in the
  // input level. Return the input level, if such level could not be found.
  int FindMinimumEmptyLevelFitting(ColumnFamilyData* cfd,
                                   const MutableCFOptions& mutable_cf_options,
                                   int level);

  // Move the files in the input level to the target level.
  // If target_level < 0, automatically calculate the minimum level that could
  // hold the data set.
  Status ReFitLevel(ColumnFamilyData* cfd, int level, int target_level = -1);

  // helper functions for adding and removing from flush & compaction queues
  void AddToCompactionQueue(ColumnFamilyData* cfd);
  ColumnFamilyData* PopFirstFromCompactionQueue();

  void AddToGarbageCollectionQueue(ColumnFamilyData* cfd);
  ColumnFamilyData* PopFirstFromGarbageCollectionQueue();

  FlushRequest PopFirstFromFlushQueue();

  // helper function to call after some of the logs_ were synced
  void MarkLogsSynced(uint64_t up_to, bool synced_dir, const Status& status);

  SnapshotImpl* GetSnapshotImpl(bool is_write_conflict_boundary);

  uint64_t GetMaxWalSize() const;
  uint64_t GetMaxTotalWalSize() const;

  Directory* GetDataDir(ColumnFamilyData* cfd, size_t path_id) const;

  Status CloseHelper();

  void WaitForBackgroundWork();

  // table_cache_ provides its own synchronization
  std::shared_ptr<Cache> table_cache_;

  // Lock over the persistent DB state.  Non-nullptr iff successfully acquired.
  FileLock* db_lock_;

  // In addition to mutex_, log_write_mutex_ protected writes to stats_history_
  InstrumentedMutex stats_history_mutex_;
  // In addition to mutex_, log_write_mutex_ protected writes to logs_ and
  // logfile_number_. With two_write_queues it also protects alive_log_files_,
  // and log_empty_. Refer to the definition of each variable below for more
  // details.
  InstrumentedMutex log_write_mutex_;
  // State below is protected by mutex_
  // With two_write_queues enabled, some of the variables that accessed during
  // WriteToWAL need different synchronization: log_empty_, alive_log_files_,
  // logs_, logfile_number_. Refer to the definition of each variable below for
  // more description.
  mutable InstrumentedMutex mutex_;

  std::atomic<bool> shutting_down_;
  // This condition variable is signaled on these conditions:
  // * whenever bg_compaction_scheduled_ goes down to 0
  // * if AnyManualCompaction, whenever a compaction finishes, even if it hasn't
  // made any progress
  // * whenever a compaction made any progress
  // * whenever bg_flush_scheduled_ or bg_purge_scheduled_ value decreases
  // (i.e. whenever a flush is done, even if it didn't make any progress)
  // * whenever there is an error in background purge, flush or compaction
  // * whenever num_running_ingest_file_ goes to 0.
  // * whenever pending_purge_obsolete_files_ goes to 0.
  // * whenever disable_delete_obsolete_files_ goes to 0.
  // * whenever SetOptions successfully updates options.
  // * whenever a column family is dropped.
  InstrumentedCondVar bg_cv_;
  // Writes are protected by locking both mutex_ and log_write_mutex_, and reads
  // must be under either mutex_ or log_write_mutex_. Since after ::Open,
  // logfile_number_ is currently updated only in write_thread_, it can be read
  // from the same write_thread_ without any locks.
  uint64_t logfile_number_;
  std::deque<uint64_t>
      log_recycle_files_;  // a list of log files that we can recycle
  std::deque<std::unique_ptr<log::Writer>> log_writer_pool_;
  autovector<std::pair<ColumnFamilyData*, MemTableInfo>> memtable_info_queue_;
  enum LogWriterPoolFlags : uint8_t {
    kLogWriterPoolIdle = 0,
    kLogWriterPoolWorking = 1,
    kLogWriterPoolWaiting = 2,
    kLogWriterPoolError = 3,
  };
  uint8_t log_writer_pool_state_;
  bool memtable_info_queue_lock_;
  bool log_dir_synced_;
  // Without two_write_queues, read and writes to log_empty_ are protected by
  // mutex_. Since it is currently updated/read only in write_thread_, it can be
  // accessed from the same write_thread_ without any locks. With
  // two_write_queues writes, where it can be updated in different threads,
  // read and writes are protected by log_write_mutex_ instead. This is to avoid
  // expesnive mutex_ lock during WAL write, which update log_empty_.
  bool log_empty_;
  ColumnFamilyHandleImpl* default_cf_handle_;
  InternalStats* default_cf_internal_stats_;
  std::unique_ptr<ColumnFamilyMemTablesImpl> column_family_memtables_;
  struct LogFileNumberSize {
    LogFileNumberSize(uint64_t _number, SequenceNumber _seq)
        : number(_number), seq(_seq) {}
    void AddSize(uint64_t new_size) { size += new_size; }
    uint64_t number;
    SequenceNumber seq;
    uint64_t size = 0;
    bool getting_flushed = false;
  };
  struct LogWriterNumber {
    // pass ownership of _writer
    LogWriterNumber(uint64_t _number, log::Writer* _writer)
        : number(_number), writer(_writer) {}

    log::Writer* ReleaseWriter() {
      auto* w = writer;
      writer = nullptr;
      return w;
    }
    Status ClearWriter() {
      Status s = writer->WriteBuffer();
      delete writer;
      writer = nullptr;
      return s;
    }

    uint64_t number;
    // Visual Studio doesn't support deque's member to be noncopyable because
    // of a std::unique_ptr as a member.
    log::Writer* writer;  // own
    // true for some prefix of logs_
    bool getting_synced = false;
  };

  ColumnFamilyHandleImpl* persist_stats_cf_handle_;
  bool persistent_stats_cfd_exists_ = true;
  // Without two_write_queues, read and writes to alive_log_files_ are
  // protected by mutex_. However since back() is never popped, and push_back()
  // is done only from write_thread_, the same thread can access the item
  // reffered by back() without mutex_. With two_write_queues_, writes
  // are protected by locking both mutex_ and log_write_mutex_, and reads must
  // be under either mutex_ or log_write_mutex_.
  std::deque<LogFileNumberSize> alive_log_files_;
  // Log files that aren't fully synced, and the current log file.
  // Synchronization:
  //  - push_back() is done from write_thread_ with locked mutex_ and
  //  log_write_mutex_
  //  - pop_front() is done from any thread with locked mutex_ and
  //  log_write_mutex_
  //  - reads are done with either locked mutex_ or log_write_mutex_
  //  - back() and items with getting_synced=true are not popped,
  //  - The same thread that sets getting_synced=true will reset it.
  //  - it follows that the object referred by back() can be safely read from
  //  the write_thread_ without using mutex
  //  - it follows that the items with getting_synced=true can be safely read
  //  from the same thread that has set getting_synced=true
  std::deque<LogWriterNumber> logs_;
  // Signaled when getting_synced becomes false for some of the logs_.
  InstrumentedCondVar log_sync_cv_;
  // This is the app-level state that is written to the WAL but will be used
  // only during recovery. Using this feature enables not writing the state to
  // memtable on normal writes and hence improving the throughput. Each new
  // write of the state will replace the previous state entirely even if the
  // keys in the two consecuitive states do not overlap.
  // It is protected by log_write_mutex_ when two_write_queues_ is enabled.
  // Otherwise only the heaad of write_thread_ can access it.
  WriteBatch cached_recoverable_state_;
  std::atomic<bool> cached_recoverable_state_empty_ = {true};
  std::atomic<uint64_t> total_log_size_;
  // only used for dynamically adjusting max_total_wal_size. it is a sum of
  // [write_buffer_size * max_write_buffer_number] over all column families
  uint64_t max_total_in_memory_state_;
  // If true, we have only one (default) column family. We use this to optimize
  // some code-paths
  bool single_column_family_mode_;
  // If this is non-empty, we need to delete these log files in background
  // threads. Protected by db mutex.
  autovector<log::Writer*> logs_to_free_;

  bool is_snapshot_supported_;

  std::map<uint64_t, std::map<std::string, uint64_t>> stats_history_;

  std::map<std::string, uint64_t> stats_slice_;

  bool stats_slice_initialized_ = false;

  // Class to maintain directories for all database paths other than main one.
  class Directories {
   public:
    Status SetDirectories(Env* env, const std::string& dbname,
                          const std::string& wal_dir,
                          const std::vector<DbPath>& data_paths);

    Directory* GetDataDir(size_t path_id) const;

    Directory* GetWalDir() {
      if (wal_dir_) {
        return wal_dir_.get();
      }
      return db_dir_.get();
    }

    Directory* GetDbDir() { return db_dir_.get(); }

   private:
    std::unique_ptr<Directory> db_dir_;
    std::vector<std::unique_ptr<Directory>> data_dirs_;
    std::unique_ptr<Directory> wal_dir_;
  };

  Directories directories_;

  WriteBufferManager* write_buffer_manager_;

  WriteThread write_thread_;
  WriteBatch tmp_batch_;
  // The write thread when the writers have no memtable write. This will be used
  // in 2PC to batch the prepares separately from the serial commit.
  WriteThread nonmem_write_thread_;

  WriteController write_controller_;

  std::unique_ptr<RateLimiter> low_pri_write_rate_limiter_;

  // Size of the last batch group. In slowdown mode, next write needs to
  // sleep if it uses up the quota.
  // Note: This is to protect memtable and compaction. If the batch only writes
  // to the WAL its size need not to be included in this.
  uint64_t last_batch_group_size_;

  FlushScheduler flush_scheduler_;

  SnapshotList snapshots_;

  // For each background job, pending_outputs_ keeps the current file number at
  // the time that background job started.
  // FindObsoleteFiles()/PurgeObsoleteFiles() never deletes any file that has
  // number bigger than any of the file number in pending_outputs_. Since file
  // numbers grow monotonically, this also means that pending_outputs_ is always
  // sorted. After a background job is done executing, its file number is
  // deleted from pending_outputs_, which allows PurgeObsoleteFiles() to clean
  // it up.
  // State is protected with db mutex.
  std::list<uint64_t> pending_outputs_;

  // PurgeFileInfo is a structure to hold information of files to be deleted in
  // purge_queue_
  struct PurgeFileInfo {
    std::string fname;
    std::string dir_to_sync;
    FileType type;
    uint64_t number;
    int job_id;
    PurgeFileInfo(std::string fn, std::string d, FileType t, uint64_t num,
                  int jid)
        : fname(fn), dir_to_sync(d), type(t), number(num), job_id(jid) {}
  };

  // flush_queue_ and compaction_queue_ hold column families that we need to
  // flush and compact, respectively.
  // A column family is inserted into flush_queue_ when it satisfies condition
  // cfd->imm()->IsFlushPending()
  // A column family is inserted into compaction_queue_ when it satisfied
  // condition cfd->NeedsCompaction()
  // Column families in this list are all Ref()-erenced
  // TODO(icanadi) Provide some kind of ReferencedColumnFamily class that will
  // do RAII on ColumnFamilyData
  // Column families are in this queue when they need to be flushed or
  // compacted. Consumers of these queues are flush and compaction threads. When
  // column family is put on this queue, we increase unscheduled_flushes_ and
  // unscheduled_compactions_. When these variables are bigger than zero, that
  // means we need to schedule background threads for compaction and thread.
  // Once the background threads are scheduled, we decrease unscheduled_flushes_
  // and unscheduled_compactions_. That way we keep track of number of
  // compaction and flush threads we need to schedule. This scheduling is done
  // in MaybeScheduleFlushOrCompaction()
  // invariant(column family present in flush_queue_ <==>
  // ColumnFamilyData::pending_flush_ == true)
  std::deque<FlushRequest> flush_queue_;
  // invariant(column family present in compaction_queue_ <==>
  // ColumnFamilyData::pending_compaction_ == true)
  std::deque<ColumnFamilyData*> compaction_queue_;
  std::deque<ColumnFamilyData*> garbage_collection_queue_;

  // A queue to store filenames of the files to be purged
  std::deque<PurgeFileInfo> purge_queue_;

  // A pointer to the file numbers that have been assigned to certain
  // JobContext. Current implementation tracks SST, WAL & MANIFEST files.
  std::unordered_set<const std::vector<uint64_t>*> files_grabbed_for_purge_;

  // when doing the full scan, we need to know which elements removed from
  // `purge_queue_` and `files_grabbed_for_purge_`
  std::list<std::vector<uint64_t>*> candidate_file_listener_;

  // A queue to store superversions to delete
  std::deque<SuperVersion*> superversion_to_free_queue_;

  // A queue to store log writers to close
  std::deque<log::Writer*> logs_to_free_queue_;

  int unscheduled_flushes_;
  int unscheduled_compactions_;
  int unscheduled_garbage_collections_;

  // count how many background compactions are running or have been scheduled in
  // the BOTTOM pool
  int bg_bottom_compaction_scheduled_;

  // count how many background compactions are running or have been scheduled
  int bg_compaction_scheduled_;

  // count how many background garbage collections are running or have been
  // scheduled
  int bg_garbage_collection_scheduled_;

  // stores the number of compactions are currently running
  int num_running_compactions_;

  // stores the number of garbage collections are currently running
  int num_running_garbage_collections_;

  // number of background memtable flush jobs, submitted to the HIGH pool
  int bg_flush_scheduled_;

  // stores the number of flushes are currently running
  int num_running_flushes_;

  // number of background obsolete file purge jobs, submitted to the HIGH pool
  int bg_purge_scheduled_;

  // Information for a manual compaction
  struct ManualCompactionState {
    ColumnFamilyData* cfd;
    int input_level;
    int output_level;
    uint32_t output_path_id;
    Status status;
    bool done;
    bool in_progress;            // compaction request being processed?
    bool incomplete;             // only part of requested range compacted
    bool exclusive;              // current behavior of only one manual
    bool disallow_trivial_move;  // Force actual compaction to run
    const InternalKey* begin;    // nullptr means beginning of key range
    const InternalKey* end;      // nullptr means end of key range
    InternalKey* manual_end;     // how far we are compacting
    InternalKey tmp_storage;     // Used to keep track of compaction progress
    InternalKey tmp_storage1;    // Used to keep track of compaction progress
  };
  struct PrepickedCompaction {
    // background compaction takes ownership of `compaction`.
    Compaction* compaction;
    // caller retains ownership of `manual_compaction_state` as it is reused
    // across background compactions.
    ManualCompactionState* manual_compaction_state;  // nullptr if non-manual
  };
  std::deque<ManualCompactionState*> manual_compaction_dequeue_;

  struct CompactionArg {
    // caller retains ownership of `db`.
    DBImpl* db;
    // background compaction takes ownership of `prepicked_compaction`.
    PrepickedCompaction* prepicked_compaction;
  };

  // shall we disable deletion of obsolete files
  // if 0 the deletion is enabled.
  // if non-zero, files will not be getting deleted
  // This enables two different threads to call
  // EnableFileDeletions() and DisableFileDeletions()
  // without any synchronization
  int disable_delete_obsolete_files_;

  // Number of times FindObsoleteFiles has found deletable files and the
  // corresponding call to PurgeObsoleteFiles has not yet finished.
  int pending_purge_obsolete_files_;

  // last time when DeleteObsoleteFiles with full scan was executed. Originaly
  // initialized with startup time.
  uint64_t delete_obsolete_files_last_run_;

  // last time stats were dumped to LOG
  std::atomic<uint64_t> last_stats_dump_time_microsec_;

  // Each flush or compaction gets its own job id. this counter makes sure
  // they're unique
  std::atomic<int> next_job_id_;

  // A flag indicating whether the current rocksdb database has any
  // data that is not yet persisted into either WAL or SST file.
  // Used when disableWAL is true.
  std::atomic<bool> has_unpersisted_data_;

  // full scan running lock
  bool delete_obsolete_files_lock_;

  // if an attempt was made to flush all column families that
  // the oldest log depends on but uncommited data in the oldest
  // log prevents the log from being released.
  // We must attempt to free the dependent memtables again
  // at a later time after the transaction in the oldest
  // log is fully commited.
  bool unable_to_release_oldest_log_;

  static const int KEEP_LOG_FILE_NUM = 1000;
  // MSVC version 1800 still does not have constexpr for ::max()
  static const uint64_t kNoTimeOut = port::kMaxUint64;

  std::string db_absolute_path_;

  // The options to access storage files
  const EnvOptions env_options_;

  // Additonal options for compaction and flush
  EnvOptions env_options_for_compaction_;

  // Number of running IngestExternalFile() calls.
  // REQUIRES: mutex held
  int num_running_ingest_file_;

#ifndef ROCKSDB_LITE
  WalManager wal_manager_;
#endif  // ROCKSDB_LITE

  // Unified interface for logging events
  EventLogger event_logger_;

  // A value of > 0 temporarily disables scheduling of background work
  int bg_work_paused_;

  // A value of > 0 temporarily disables scheduling of background compaction
  int bg_compaction_paused_;

  // Guard against multiple concurrent refitting
  bool refitting_level_;

  // Indicate DB was opened successfully
  bool opened_successfully_;

  LogsWithPrepTracker logs_with_prep_tracker_;

  // Callback for compaction to check if a key is visible to a snapshot.
  // REQUIRES: mutex held
  std::unique_ptr<SnapshotChecker> snapshot_checker_;

  // Callback for when the cached_recoverable_state_ is written to memtable
  // Only to be set during initialization
  std::unique_ptr<PreReleaseCallback> recoverable_state_pre_release_callback_;

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  // Background threads call this function, which is just a wrapper around
  // the InstallSuperVersion() function. Background threads carry
  // sv_context which can have new_superversion already
  // allocated.
  // All ColumnFamily state changes go through this function. Here we analyze
  // the new state and we schedule background work if we detect that the new
  // state needs flush or compaction.
  void InstallSuperVersionAndScheduleWork(
      ColumnFamilyData* cfd, SuperVersionContext* sv_context,
      const MutableCFOptions& mutable_cf_options,
      FlushReason flush_reason = FlushReason::kOthers);

#ifndef ROCKSDB_LITE
  using DB::NewPropertiesOfAllTablesIterator;
  virtual TablePropertiesCollectionIterator* NewPropertiesOfAllTablesIterator(
      ColumnFamilyHandle* column_family) override;

  using DB::GetPropertiesOfAllTables;
  virtual Status GetPropertiesOfAllTables(
      ColumnFamilyHandle* column_family,
      TablePropertiesCollection* props) override;
  virtual Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
      TablePropertiesCollection* props, bool include_blob = true) override;

#endif  // ROCKSDB_LITE

  bool GetIntPropertyInternal(ColumnFamilyData* cfd,
                              const DBPropertyInfo& property_info,
                              bool is_locked, uint64_t* value);
  bool GetPropertyHandleOptionsStatistics(std::string* value);

  bool HasPendingManualCompaction();
  bool HasExclusiveManualCompaction();
  void AddManualCompaction(ManualCompactionState* m);
  void RemoveManualCompaction(ManualCompactionState* m);
  bool ShouldntRunManualCompaction(ManualCompactionState* m);
  bool HaveManualCompaction(ColumnFamilyData* cfd);
  bool MCOverlap(ManualCompactionState* m, ManualCompactionState* m1);

  size_t GetWalPreallocateBlockSize(uint64_t write_buffer_size) const;
  Env::WriteLifeTimeHint CalculateWALWriteHint() { return Env::WLTH_SHORT; }
#ifndef ROCKSDB_LITE
  // Scheduler to run DumpStats(), PersistStats(), and FlushInfoLog().
  // Currently, it always use a global instance from
  // PeriodicWorkScheduler::Default(). Only in unittest, it can be overrided by
  // PeriodicWorkTestScheduler.
  PeriodicWorkScheduler* periodic_work_scheduler_;
#endif

  // When set, we use a separate queue for writes that dont write to memtable.
  // In 2PC these are the writes at Prepare phase.
  const bool two_write_queues_;
  const bool manual_wal_flush_;
  // Increase the sequence number after writing each batch, whether memtable is
  // disabled for that or not. Otherwise the sequence number is increased after
  // writing each key into memtable. This implies that when disable_memtable is
  // set, the seq is not increased at all.
  //
  // Default: false
  const bool seq_per_batch_;
  // This determines during recovery whether we expect one writebatch per
  // recovered transaction, or potentially multiple writebatches per
  // transaction. For WriteUnprepared, this is set to false, since multiple
  // batches can exist per transaction.
  //
  // Default: true
  const bool batch_per_txn_;
  // LastSequence also indicates last published sequence visibile to the
  // readers. Otherwise LastPublishedSequence should be used.
  const bool last_seq_same_as_publish_seq_;
  // It indicates that a customized gc algorithm must be used for
  // flush/compaction and if it is not provided vis SnapshotChecker, we should
  // disable gc to be safe.
  const bool use_custom_gc_;
  // Flag to indicate that the DB instance shutdown has been initiated. This
  // different from shutting_down_ atomic in that it is set at the beginning
  // of shutdown sequence, specifically in order to prevent any background
  // error recovery from going on in parallel. The latter, shutting_down_,
  // is set a little later during the shutdown after scheduling memtable
  // flushes
  std::atomic<bool> shutdown_initiated_;
  // Flag to indicate whether sst_file_manager object was allocated in
  // DB::Open() or passed to us
  bool own_sfm_;

  // Clients must periodically call SetPreserveDeletesSequenceNumber()
  // to advance this seqnum. Default value is 0 which means ALL deletes are
  // preserved. Note that this has no effect if DBOptions.preserve_deletes
  // is set to false.
  std::atomic<SequenceNumber> preserve_deletes_seqnum_;
  const bool preserve_deletes_;

  // Flag to check whether Close() has been called on this DB
  bool closed_;

  ErrorHandler error_handler_;

  // Conditional variable to coordinate installation of atomic flush results.
  // With atomic flush, each bg thread installs the result of flushing multiple
  // column families, and different threads can flush different column
  // families. It's difficult to rely on one thread to perform batch
  // installation for all threads. This is different from the non-atomic flush
  // case.
  // atomic_flush_install_cv_ makes sure that threads install atomic flush
  // results sequentially. Flush results of memtables with lower IDs get
  // installed to MANIFEST first.
  InstrumentedCondVar atomic_flush_install_cv_;

  std::string bytedance_tags_;
  std::shared_ptr<MetricsReporterFactory> metrics_reporter_factory_;
  cheapis::ServerRunner console_runner_;

  QPSReporter write_qps_reporter_;
  QPSReporter read_qps_reporter_;
  QPSReporter newiterator_qps_reporter_;
  QPSReporter seek_qps_reporter_;
  QPSReporter next_qps_reporter_;
  QPSReporter seekforprev_qps_reporter_;
  QPSReporter prev_qps_reporter_;

  LatencyReporter write_latency_reporter_;
  LatencyReporter read_latency_reporter_;
  LatencyReporter newiterator_latency_reporter_;
  LatencyReporter seek_latency_reporter_;
  LatencyReporter next_latency_reporter_;
  LatencyReporter seekforprev_latency_reporter_;
  LatencyReporter prev_latency_reporter_;

  LatencyReporter zenfs_get_snapshot_latency_reporter_;
  // Report ZenFS GC latency
  LatencyReporter zenfs_gc_latency_reporter_;

  ThroughputReporter write_throughput_reporter_;
  DistributionReporter write_batch_size_reporter_;
};

extern Options SanitizeOptions(const std::string& db, const Options& src);

extern DBOptions SanitizeOptions(const std::string& db, const DBOptions& src);

extern CompressionType GetCompressionFlush(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options);

// Return the earliest log file to keep after the memtable flush is
// finalized.
// `cfd_to_flush` is the column family whose memtable (specified in
// `memtables_to_flush`) will be flushed and thus will not depend on any WAL
// file.
// The function is only applicable to 2pc mode.
extern uint64_t PrecomputeMinLogNumberToKeep(
    VersionSet* vset, const ColumnFamilyData& cfd_to_flush,
    autovector<VersionEdit*> edit_list,
    const autovector<MemTable*>& memtables_to_flush,
    LogsWithPrepTracker* prep_tracker);

// `cfd_to_flush` is the column family whose memtable will be flushed and thus
// will not depend on any WAL file. nullptr means no memtable is being flushed.
// The function is only applicable to 2pc mode.
extern uint64_t FindMinPrepLogReferencedByMemTable(
    VersionSet* vset, const ColumnFamilyData* cfd_to_flush,
    const autovector<MemTable*>& memtables_to_flush);

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

}  // namespace TERARKDB_NAMESPACE
