#include "filemap.h"

#include <cassert>

#include "db/dbformat.h"
#include "fs/log.h"
#include "rocksdb/comparator.h"
#include "rocksdb/status.h"

namespace TERARKDB_NAMESPACE {

Status FileMap::AddNode(FileMetaData *fmeta, uint64_t version_num) {
  auto file_number = fmeta->fd.GetNumber();
  if (nodes_.find(file_number) != nodes_.end()) {
    return Status::Corruption("Node to add already exists");
  }
  // ZnsLog(kCyan, "FileMap::AddNode %lu.sst", fmeta->fd.GetNumber());
  nodes_.emplace(file_number, std::make_shared<MapNode>(fmeta, version_num));
  return Status::OK();
}

Status FileMap::AddDerivedNode(FileMetaData *p, FileMetaData *c,
                               uint64_t version_num) {
  return AddDerivedNode(p->fd.GetNumber(), c, version_num);
}

Status FileMap::AddDerivedNode(uint64_t p_filenum, FileMetaData *c,
                               uint64_t version_num) {
  auto c_filenum = c->fd.GetNumber();
  if (nodes_.find(p_filenum) == nodes_.end()) {
    return Status::Corruption("Request parent node does not exist");
  }
  auto p_node = nodes_[p_filenum];
  // The parantal version number must be less than child's version number
  assert(p_node->version_num < version_num);

  std::shared_ptr<MapNode> add_child = nullptr;
  if (nodes_.find(c_filenum) != nodes_.end()) {
    add_child = nodes_[c_filenum];
    assert(add_child->version_num == version_num);
  } else {
    add_child = std::make_shared<MapNode>(c, version_num);
    nodes_.emplace(c_filenum, add_child);
  }
  assert(add_child != nullptr);
  nodes_[p_filenum]->children.emplace_back(add_child);

  // TODO: May sort the children nodes based on their key ranges

  // ZnsLog(kCyan, "FileMap::AddDerivedNode %llu.sst -> %llu.sst", p_filenum,
  //        c_filenum);
  return Status::OK();
}

Status FileMap::QueryFileNumber(uint64_t fn, const Slice &ukey,
                                const Comparator *u_cmp, uint64_t version_num,
                                uint64_t *ret) {
  if (nodes_.find(fn) == nodes_.end()) {
    return Status::Corruption("Queried file number does not exist");
  }
  auto node = nodes_[fn];
  uint64_t hop_num = 0;
  while (!node->IsLeaf() && node->Accessible(version_num)) {
    auto iter = node->children.begin();
    bool changed = false;
    // Seeking to the child that contains the requested user key
    for (; iter != node->children.end(); ++iter) {
      if ((*iter)->Accessible(version_num) && (*iter)->Contain(ukey, u_cmp)) {
        node = *iter;
        changed = true;
        hop_num++;
        break;
      }
    }
    // No further step
    if (!changed) {
      break;
    }
  }

  if (!node->Contain(ukey, u_cmp) || !node->Accessible(version_num)) {
    return Status::Corruption("Resultant file does not contain seeking key");
  }

  if (ret) {
    *ret = node->fd.GetNumber();
  }
  monitor_.hop_num += hop_num;
  monitor_.max_hop_num = std::max(monitor_.max_hop_num, hop_num);
  return Status::OK();
}

Status FileMap::QueryFileMeta(uint64_t fn, const Slice &ukey,
                              const Comparator *u_cmp, uint64_t version_num,
                              FileMetaData **filemeta) {
  if (nodes_.find(fn) == nodes_.end()) {
    return Status::Corruption("Queried file number does not exist");
  }
  auto node = nodes_[fn];
  uint64_t hop_num = 0;
  // node->version_num <= version_num means this node can be accessed in the
  // requested version.
  while (!node->IsLeaf() && node->Accessible(version_num)) {
    auto iter = node->children.begin();
    bool changed = false;
    // Seeking to the child that contains the requested user key
    for (; iter != node->children.end(); ++iter) {
      if ((*iter)->Accessible(version_num) && (*iter)->Contain(ukey, u_cmp)) {
        node = *iter;
        hop_num++;
        changed = true;
        break;
      }
    }
    if (!changed) {
      break;
    }
  }

  if (!node->Contain(ukey, u_cmp) || !node->Accessible(version_num)) {
    return Status::Corruption("Resultant file does not contain seeking key");
  }

  if (filemeta) {
    *filemeta = node->file_meta;
  }
  monitor_.hop_num += hop_num;
  monitor_.max_hop_num = std::max(monitor_.max_hop_num, hop_num);
  return Status::OK();
}

void FileMap::Shrink(Comparator *u_cmp) {}

}  // namespace TERARKDB_NAMESPACE