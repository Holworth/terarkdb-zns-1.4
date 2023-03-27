#pragma once
#include <folly/concurrency/ConcurrentHashMap.h>

#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/port_posix.h"
#include "rocksdb/comparator.h"
#include "rocksdb/status.h"
#include "rocksdb/terark_namespace.h"
#include "util/arena.h"
#include "util/autovector.h"

namespace TERARKDB_NAMESPACE {

//
// FileMap is a class tracking the GC history and provides the functionalities
// of quering the latest corresponding blob file given user key.
//
// Concurrency Control: This class is supposed to be Thread-Safe but not
// implmented yet
//
// The FileMap is implemented in a MVCC manner, a version number is passed unpon
// creation to identify which version this file belongs to. A query operation
// needs a version number to only search files created before this version (has
// smaller version number)
//
class FileMap {
 public:
  // Each MapNode coresponds to a blob file and derived blob files.
  // We say a.sst is derived from b.sst, if b.sst contains at least
  // one valid entry from a.sst
  struct MapNode {
    uint64_t version_num;
    FileMetaData* file_meta;
    FileDescriptor fd;         // FileDescriptor of this file
    InternalKey smallest_key;  // smallest internal key
    InternalKey largest_key;   // largest internal key
    std::vector<std::shared_ptr<MapNode>> children;  // derived blobs

    MapNode(FileMetaData* _file_meta, uint64_t _version_num)
        : version_num(_version_num),
          file_meta(_file_meta),
          fd(_file_meta->fd),
          smallest_key(_file_meta->smallest),
          largest_key(_file_meta->largest),
          children() {
      // We reserve the space for children to prevent concurrent error
      // We hope 16 is enough for our case
      children.reserve(32);
    }

    bool IsLeaf() const { return children.size() == 0; }
    bool Accessible(uint64_t version) const { return version_num <= version; }
    bool Contain(const Slice& u_key, const Comparator* cmp) const {
      return cmp->Compare(smallest_key.user_key(), u_key) <= 0 &&
             cmp->Compare(u_key, largest_key.user_key()) <= 0;
    }
  };

  // Monitor for internal metrics
  struct Monitor {
    uint64_t query_num = 0;    // The number of invokations of Query
    uint64_t hop_num = 0;      // The total hop number during Query
    uint64_t max_hop_num = 0;  // The max number of hop ever executed
  };

 public:
  FileMap() = default;

  // Copy is not allowed for FileMap, using pointer or reference to access it
  FileMap(const FileMap&) = delete;
  FileMap& operator=(const FileMap&) = delete;

  // Add a node with given file meta and the version it belongs to. This file
  // has no derivation yet. it is used to add a file initially flushed from
  // memtable. Subsequent GC works will expand the initial node by adding
  // derived blobs.
  // Set the default parameter to be kMaxUint64 so that this file can not be
  // accessed by any version.
  Status AddNode(FileMetaData* fmeta, uint64_t version_num = port::kMaxUint64);

  // Add a derivation relation in this FileMap.
  // p is the garbage collected file and c is the resultant file.
  // Set the default parameter to be kMaxUint64 so that the child file can not
  // be accessed by any version.
  Status AddDerivedNode(FileMetaData* p, FileMetaData* c,
                        uint64_t version_num = port::kMaxUint64);

  // Do the same thing as above. We find passing the file number of parent file
  // is more convenient and useful.
  Status AddDerivedNode(uint64_t p_filenum, FileMetaData* c,
                        uint64_t version_num = port::kMaxUint64);

  // Given an original file number and the quried user key, find the latest
  // file number that may contain the specified user key. Return OK and set
  // ret if succeed.
  Status QueryFileNumber(uint64_t fn, const Slice& ukey,
                         const Comparator* user_cmp, uint64_t version_num,
                         uint64_t* ret);

  // Query the latest file number given the specific key within a specific
  // version. Returns the FileMetaData directly. Return NOK if any error
  // happens
  Status QueryFileMeta(uint64_t fn, const Slice& ukey,
                       const Comparator* user_cmp, uint64_t version_num,
                       FileMetaData** filemeta);

  // The mapping/tracking chain gets longer as the garbage collection work
  // continues to add new relations, which aggravates the inefficiency of
  // query operation. A explicit invokation of Shrink() removes useless
  // relations as much as possible. However, the implementation of Shrink()
  // is undecided
  void Shrink(Comparator* u_cmp);

  // Return the number of nodes in current file map
  size_t size() const { return nodes_.size(); }

 private:
  // Indexing node in a map makes the query swiftly
  folly::ConcurrentHashMap<uint64_t, std::shared_ptr<MapNode>> nodes_;
  Monitor monitor_;   // Monitoring information
  bool auto_shrink_;  // enable auto shrink or not

  // TODO: Some concurrency control stuff
};
}  // namespace TERARKDB_NAMESPACE