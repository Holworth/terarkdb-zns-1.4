#include "filemap.h"

#include <algorithm>
#include <cstdint>
#include <memory>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "gtest/gtest.h"
#include "port/port_posix.h"
#include "port/stack_trace.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {
class FileMapTest : public ::testing::Test {
 public:
  Slice Int64ToUserKey(int64_t key) {
    auto ch = new char[sizeof(key)];
    *(int64_t*)ch = key;
    return Slice((const char*)ch, sizeof(key));
  }

  class MockComparator : public Comparator {
    int Compare(const Slice& a, const Slice& b) const override {
      auto num_a = *(int64_t*)a.data(), num_b = *(int64_t*)b.data();
      return num_a - num_b;
    }

    const char* Name() const override { return "MockComparator"; }

    void FindShortestSeparator(std::string* start,
                               const Slice& limit) const override {
      return;
    }
    void FindShortSuccessor(std::string* key) const override {}
  };

  InternalKey ToInternalKey(int64_t key) {
    static uint64_t seq_gen = 0;
    return InternalKey(Int64ToUserKey(key), seq_gen++, kTypeValue);
  }

  std::shared_ptr<FileMetaData> ConstructFile(uint64_t min_key,
                                              uint64_t max_key, uint64_t fn) {
    auto ret = std::make_shared<FileMetaData>();
    ret->smallest = ToInternalKey(min_key);
    ret->largest = ToInternalKey(max_key);
    ret->fd = FileDescriptor(fn, 0, 0);
    return ret;
  }
};

TEST_F(FileMapTest, DISABLED_TestQueryWithoutGC) {
  auto test_map = std::make_shared<FileMap>();
  MockComparator cmp;

  // (min_key, max_key, fn)
  test_map->AddNode(ConstructFile(1, 10, 1).get());
  test_map->AddNode(ConstructFile(5, 20, 2).get());
  test_map->AddNode(ConstructFile(9, 15, 3).get());
  test_map->AddNode(ConstructFile(40, 50, 4).get());
  test_map->AddNode(ConstructFile(30, 70, 5).get());

  uint64_t fn = -1;
  // Query key exceeding the range will return failure
  ASSERT_NOK(test_map->QueryFileNumber(1, Int64ToUserKey(11), &cmp,
                                       port::kMaxUint64, &fn));
  ASSERT_NOK(test_map->QueryFileNumber(2, Int64ToUserKey(25), &cmp,
                                       port::kMaxUint64, &fn));
  ASSERT_NOK(test_map->QueryFileNumber(3, Int64ToUserKey(20), &cmp,
                                       port::kMaxUint64, &fn));
  ASSERT_NOK(test_map->QueryFileNumber(4, Int64ToUserKey(2), &cmp,
                                       port::kMaxUint64, &fn));
  ASSERT_NOK(test_map->QueryFileNumber(5, Int64ToUserKey(20), &cmp,
                                       port::kMaxUint64, &fn));

  // Query not existed key will return failure
  ASSERT_NOK(test_map->QueryFileNumber(10, Int64ToUserKey(11), &cmp,
                                       port::kMaxUint64, &fn));
  ASSERT_NOK(test_map->QueryFileNumber(21, Int64ToUserKey(25), &cmp,
                                       port::kMaxUint64, &fn));

  // Query key and according file number contains the key returns ok
  ASSERT_OK(test_map->QueryFileNumber(1, Int64ToUserKey(5), &cmp,
                                      port::kMaxUint64, &fn));
  ASSERT_EQ(fn, 1);
  ASSERT_OK(test_map->QueryFileNumber(2, Int64ToUserKey(9), &cmp,
                                      port::kMaxUint64, &fn));
  ASSERT_EQ(fn, 2);
  ASSERT_OK(test_map->QueryFileNumber(5, Int64ToUserKey(49), &cmp,
                                      port::kMaxUint64, &fn));
  ASSERT_EQ(fn, 5);
}

TEST_F(FileMapTest, TestQueryWithMVCC) {
  auto test_map = std::make_shared<FileMap>();
  MockComparator cmp;
  uint64_t fn = -1;

  // Say we have the following GC execution flow:
  //
  //           |----------------------------------------|
  // Version1: |    1.sst        2.sst         3.sst    |
  //           |   [1, 10]      [5, 20]       [3, 15]   |
  //           |      |---------------------------------|-------------|
  // Version2: |      |   4.sst         5.sst           |    6.sst    |
  //           |      |   [2, 8]       [9, 18]          |   [15, 30]  |
  //           |------|---------------------------------|             |
  // Version3:        |        7.sst          8.sst                   |
  //                  |       [2, 19]        [20, 30]                 |
  //                  |-----------------------------------------------|
  //
  //
  // (min_key, max_key, file_number)
  auto f1 = ConstructFile(1, 10, 1);
  auto f2 = ConstructFile(5, 20, 2);
  auto f3 = ConstructFile(3, 15, 3);
  auto f4 = ConstructFile(2, 8, 4);
  auto f5 = ConstructFile(9, 18, 5);
  auto f6 = ConstructFile(15, 30, 6);
  auto f7 = ConstructFile(2, 19, 7);
  auto f8 = ConstructFile(20, 30, 8);

  // Version number for different snapshot
  uint64_t v1 = 1, v2 = 2, v3 = 3;

  // Add the base node
  ASSERT_OK(test_map->AddNode(f1.get(), v1));
  ASSERT_OK(test_map->AddNode(f2.get(), v1));
  ASSERT_OK(test_map->AddNode(f3.get(), v1));

  // Add the resultant node for the first GC
  ASSERT_OK(test_map->AddDerivedNode(f1.get(), f4.get(), v2));
  ASSERT_OK(test_map->AddDerivedNode(f1.get(), f5.get(), v2));

  ASSERT_OK(test_map->AddDerivedNode(f2.get(), f4.get(), v2));
  ASSERT_OK(test_map->AddDerivedNode(f2.get(), f5.get(), v2));

  ASSERT_OK(test_map->AddDerivedNode(f3.get(), f4.get(), v2));
  ASSERT_OK(test_map->AddDerivedNode(f3.get(), f5.get(), v2));
  
  ASSERT_OK(test_map->AddNode(f6.get(), v2));

  // Add the resultant node for the second GC
  ASSERT_OK(test_map->AddDerivedNode(f4.get(), f7.get(), v3));
  ASSERT_OK(test_map->AddDerivedNode(f4.get(), f8.get(), v3));

  ASSERT_OK(test_map->AddDerivedNode(f5.get(), f7.get(), v3));
  ASSERT_OK(test_map->AddDerivedNode(f5.get(), f8.get(), v3));

  ASSERT_OK(test_map->AddDerivedNode(f6.get(), f7.get(), v3));
  ASSERT_OK(test_map->AddDerivedNode(f6.get(), f8.get(), v3));

  // Check for different snapshot:
  // Version1: 
  ASSERT_OK(test_map->QueryFileNumber(1, Int64ToUserKey(7), &cmp, v1, &fn));
  ASSERT_EQ(fn, 1);
  ASSERT_OK(test_map->QueryFileNumber(2, Int64ToUserKey(15), &cmp, v1, &fn));
  ASSERT_EQ(fn, 2);

  // Version2:
  ASSERT_OK(test_map->QueryFileNumber(1, Int64ToUserKey(7), &cmp, v2, &fn));
  ASSERT_EQ(fn, 4);
  ASSERT_OK(test_map->QueryFileNumber(2, Int64ToUserKey(15), &cmp, v2, &fn));
  ASSERT_EQ(fn, 5);

  // Version3:
  ASSERT_OK(test_map->QueryFileNumber(1, Int64ToUserKey(7), &cmp, v3, &fn));
  ASSERT_EQ(fn, 7);
  ASSERT_OK(test_map->QueryFileNumber(2, Int64ToUserKey(15), &cmp, v3, &fn));
  ASSERT_EQ(fn, 7);
  ASSERT_OK(test_map->QueryFileNumber(6, Int64ToUserKey(25), &cmp, v3, &fn));
  ASSERT_EQ(fn, 8);
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  TERARKDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}