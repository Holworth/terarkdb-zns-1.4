//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "monitoring/statistics.h"
#include <boost/fiber/all.hpp>
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/thread_status.h"
#include "util/stop_watch.h"

namespace rocksdb {
class InstrumentedCondVar;

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutex {
 public:
  explicit InstrumentedMutex(bool /*adaptive*/ = false)
      : mutex_(), stats_(nullptr), env_(nullptr),
        stats_code_(0) {}

  InstrumentedMutex(
      Statistics* stats, Env* env,
      int stats_code, bool /*adaptive*/ = false)
      : mutex_(), stats_(stats), env_(env),
        stats_code_(stats_code) {}

  void Lock();

  void Unlock() {
    mutex_.unlock();
  }

  void AssertHeld();

 private:
  void LockInternal();
  friend class InstrumentedCondVar;
  boost::fibers::mutex mutex_;
  Statistics* stats_;
  Env* env_;
  int stats_code_;
};

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutexLock {
 public:
  explicit InstrumentedMutexLock(InstrumentedMutex* mutex) : mutex_(mutex) {
    mutex_->Lock();
  }

  ~InstrumentedMutexLock() {
    mutex_->Unlock();
  }

 private:
  InstrumentedMutex* const mutex_;
  InstrumentedMutexLock(const InstrumentedMutexLock&) = delete;
  void operator=(const InstrumentedMutexLock&) = delete;
};

class InstrumentedCondVar {
 public:
  explicit InstrumentedCondVar(InstrumentedMutex* instrumented_mutex)
      : cond_(),
        mutex_(&instrumented_mutex->mutex_),
        stats_(instrumented_mutex->stats_),
        env_(instrumented_mutex->env_),
        stats_code_(instrumented_mutex->stats_code_) {}

  void Wait();

  bool TimedWait(uint64_t abs_time_us);

  void Signal() {
    cond_.notify_one();
  }

  void SignalAll() {
    cond_.notify_all();
  }

 private:
  void WaitInternal();
  bool TimedWaitInternal(uint64_t abs_time_us);
  boost::fibers::condition_variable cond_;
  boost::fibers::mutex* mutex_;
  Statistics* stats_;
  Env* env_;
  int stats_code_;
};

}  // namespace rocksdb
