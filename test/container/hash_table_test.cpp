//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

template <typename KeyType>
class ZeroHashFunction : public HashFunction<KeyType> {
  uint64_t GetHash(KeyType key /* unused */) override { return 0; }
};

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

template <typename KeyType>
KeyType GetKey(int i) {
  KeyType key;
  key.SetFromInteger(i);
  return key;
}

template <>
int GetKey<int>(int i) {
  return i;
}

template <typename ValueType>
ValueType GetValue(int i) {
  return static_cast<ValueType>(i);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void InsertTestCall(KeyType k /* unused */, ValueType v /* unused */, KeyComparator comparator) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(3, disk_manager);
  ExtendibleHashTable<KeyType, ValueType, KeyComparator> ht("blah", bpm, comparator, HashFunction<KeyType>());

  for (int i = 0; i < 10; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 10; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 1; i < 10; i++) {
    auto key = GetKey<KeyType>(i);
    auto value1 = GetValue<ValueType>(i);
    auto value2 = GetValue<ValueType>(2 * i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value2));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(2, res.size()) << "Failed to insert/get multiple values " << i << std::endl;
    if (res[0] == value1) {
      EXPECT_EQ(value2, res[1]);
    } else {
      EXPECT_EQ(value2, res[0]);
      EXPECT_EQ(value1, res[1]);
    }
  }

  ht.VerifyIntegrity();

  auto key20 = GetKey<KeyType>(20);
  std::vector<ValueType> res;
  EXPECT_FALSE(ht.GetValue(nullptr, key20, &res));
  EXPECT_EQ(0, res.size());

  for (int i = 20; i < 40; i++) {
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key20, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key20, &res));
    EXPECT_EQ(i - 19, res.size()) << "Failed to insert " << i << std::endl;
  }

  for (int i = 40; i < 50; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    std::vector<ValueType> res1;
    EXPECT_FALSE(ht.GetValue(nullptr, key, &res1)) << "Found non-existent value: " << i << std::endl;
    EXPECT_TRUE(ht.Insert(nullptr, key, value)) << "Failed to insert value: " << i << std::endl;
    std::vector<ValueType> res2;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res2)) << "Failed to find value: " << i << std::endl;
    EXPECT_EQ(1, res2.size()) << "Invalid result size for: " << i << std::endl;
    EXPECT_EQ(value, res2[0]);
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void RemoveTestCall(KeyType k /* unused */, ValueType v /* unused */, KeyComparator comparator) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(3, disk_manager);
  ExtendibleHashTable<KeyType, ValueType, KeyComparator> ht("blah", bpm, comparator, HashFunction<KeyType>());

  for (int i = 1; i < 10; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Insert(nullptr, key, value);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  for (int i = 1; i < 10; i++) {
    auto key = GetKey<KeyType>(i);
    auto value1 = GetValue<ValueType>(i);
    auto value2 = GetValue<ValueType>(2 * i);
    ht.Insert(nullptr, key, value1);
    ht.Insert(nullptr, key, value2);
    ht.Remove(nullptr, key, value1);
    std::vector<ValueType> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(1, res.size());
    EXPECT_EQ(value2, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 20; i < 50; i += 2) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Insert(nullptr, key, value);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(0, res.size()) << "Failed to remove " << i << std::endl;
  }

  ht.VerifyIntegrity();

  for (int i = 20; i < 50; i += 2) {
    auto key = GetKey<KeyType>(i);
    auto value1 = GetValue<ValueType>(i);
    auto value2 = GetValue<ValueType>(2 * i);
    ht.Insert(nullptr, key, value1);
    ht.Insert(nullptr, key, value2);
    ht.Remove(nullptr, key, value2);
    ht.Remove(nullptr, key, value1);
    std::vector<ValueType> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(0, res.size()) << "Failed to remove " << i << std::endl;
  }

  ht.VerifyIntegrity();

  for (int i = 20; i < 50; i += 2) {
    auto key = GetKey<KeyType>(i);
    auto value2 = GetValue<ValueType>(2 * i);
    ht.Insert(nullptr, key, value2);
  }

  ht.VerifyIntegrity();

  for (int i = 20; i < 50; i += 2) {
    auto key = GetKey<KeyType>(i);
    auto value2 = GetValue<ValueType>(2 * i);
    ht.Remove(nullptr, key, value2);
    std::vector<ValueType> res;
    ht.GetValue(nullptr, key, &res);
    EXPECT_EQ(0, res.size()) << "Failed to remove" << i << std::endl;
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void SplitGrowTestCall(KeyType k /* unused */, ValueType v /* unused */, KeyComparator comparator) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);
  ExtendibleHashTable<KeyType, ValueType, KeyComparator> ht("blah", bpm, comparator, HashFunction<KeyType>());

  for (int i = 0; i < 500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void GrowShrinkTestCall(KeyType k /* unused */, ValueType v /* unused */, KeyComparator comparator) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(15, disk_manager);
  ExtendibleHashTable<KeyType, ValueType, KeyComparator> ht("blah", bpm, comparator, HashFunction<KeyType>());

  for (int i = 0; i < 1000; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    EXPECT_FALSE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
  }

  ht.VerifyIntegrity();

  for (int i = 1000; i < 1500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 500; i < 1000; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    EXPECT_FALSE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    EXPECT_TRUE(ht.Insert(nullptr, key, value));
    std::vector<ValueType> res;
    EXPECT_TRUE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(value, res[0]);
  }

  ht.VerifyIntegrity();

  for (int i = 1000; i < 1500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    EXPECT_FALSE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
  }

  ht.VerifyIntegrity();

  for (int i = 0; i < 500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Remove(nullptr, key, value);
    std::vector<ValueType> res;
    EXPECT_FALSE(ht.GetValue(nullptr, key, &res));
    EXPECT_EQ(0, res.size()) << "Found non-existent key " << i << std::endl;
  }

  ht.VerifyIntegrity();

  //  remove everything and make sure global depth < max_global_depth
  for (int i = 0; i < 1500; i++) {
    auto key = GetKey<KeyType>(i);
    auto value = GetValue<ValueType>(i);
    ht.Remove(nullptr, key, value);
  }

  assert(ht.GetGlobalDepth() <= 1);
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void GenericTestCall(void (*func)(KeyType, ValueType, KeyComparator)) {
  Schema schema(std::vector<Column>({Column("A", TypeId::BIGINT)}));
  KeyComparator comparator(&schema);
  auto key = GetKey<KeyType>(0);
  auto value = GetValue<ValueType>(0);
  func(key, value, comparator);
}

TEST(HashTableTest, InsertTest) {
  InsertTestCall(1, 1, IntComparator());

  GenericTestCall<GenericKey<8>, RID, GenericComparator<8>>(InsertTestCall);
  GenericTestCall<GenericKey<16>, RID, GenericComparator<16>>(InsertTestCall);
  GenericTestCall<GenericKey<32>, RID, GenericComparator<32>>(InsertTestCall);
  GenericTestCall<GenericKey<64>, RID, GenericComparator<64>>(InsertTestCall);
}

TEST(HashTableTest, RemoveTest) {
  RemoveTestCall(1, 1, IntComparator());

  GenericTestCall<GenericKey<8>, RID, GenericComparator<8>>(RemoveTestCall);
  GenericTestCall<GenericKey<16>, RID, GenericComparator<16>>(RemoveTestCall);
  GenericTestCall<GenericKey<32>, RID, GenericComparator<32>>(RemoveTestCall);
  GenericTestCall<GenericKey<64>, RID, GenericComparator<64>>(RemoveTestCall);
}

TEST(HashTableTest, SplitGrowTest) {
  SplitGrowTestCall(1, 1, IntComparator());

  GenericTestCall<GenericKey<8>, RID, GenericComparator<8>>(SplitGrowTestCall);
  GenericTestCall<GenericKey<16>, RID, GenericComparator<16>>(SplitGrowTestCall);
  GenericTestCall<GenericKey<32>, RID, GenericComparator<32>>(SplitGrowTestCall);
  GenericTestCall<GenericKey<64>, RID, GenericComparator<64>>(SplitGrowTestCall);
}

TEST(HashTableTest, GrowShrinkTest) {
  GrowShrinkTestCall(1, 1, IntComparator());

  GenericTestCall<GenericKey<8>, RID, GenericComparator<8>>(GrowShrinkTestCall);
  GenericTestCall<GenericKey<16>, RID, GenericComparator<16>>(GrowShrinkTestCall);
  GenericTestCall<GenericKey<32>, RID, GenericComparator<32>>(GrowShrinkTestCall);
  GenericTestCall<GenericKey<64>, RID, GenericComparator<64>>(GrowShrinkTestCall);
}

// TEST(HashTableTest, IntegratedConcurrencyTest) {
//  const int num_threads = 5;
//  const int num_runs = 50;
//
//  for (int run = 0; run < num_runs; run++) {
//    auto *disk_manager = new DiskManager("test.db");
//    auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
//
//    ExtendibleHashTable<int, int, IntComparator> *ht =
//        new ExtendibleHashTable<int, int, IntComparator>("blah", bpm, IntComparator(), HashFunction<int>());
//    std::vector<std::thread> threads(num_threads);
//
//    for (int tid = 0; tid < num_threads; tid++) {
//      threads[tid] = std::thread([&ht, tid]() {
//        ht->Insert(nullptr, tid, tid);
//        std::vector<int> res;
//        ht->GetValue(nullptr, tid, &res);
//        EXPECT_EQ(1, res.size()) << "Failed to insert " << tid << std::endl;
//        EXPECT_EQ(tid, res[0]);
//      });
//    }
//
//    for (int i = 0; i < num_threads; i++) {
//      threads[i].join();
//    }
//
//    threads.clear();
//
//    threads.resize(num_threads);
//    for (int tid = 0; tid < num_threads; tid++) {
//      threads[tid] = std::thread([&ht, tid]() {
//        ht->Remove(nullptr, tid, tid);
//        std::vector<int> res;
//        ht->GetValue(nullptr, tid, &res);
//        EXPECT_EQ(0, res.size());
//      });
//    }
//
//    for (int i = 0; i < num_threads; i++) {
//      threads[i].join();
//    }
//
//    threads.clear();
//
//    threads.resize(num_threads);
//    for (int tid = 0; tid < num_threads; tid++) {
//      threads[tid] = std::thread([&ht, tid]() {
//        // LOG_DEBUG("thread %d\n",tid);
//        ht->Insert(nullptr, 1, tid);
//        std::vector<int> res;
//        ht->GetValue(nullptr, 1, &res);
//        bool found = false;
//        for (auto r : res) {
//          if (r == tid) {
//            found = true;
//          }
//        }
//        EXPECT_EQ(true, found);
//      });
//    }
//
//    for (int i = 0; i < num_threads; i++) {
//      threads[i].join();
//    }
//
//    std::vector<int> res;
//    ht->GetValue(nullptr, 1, &res);
//
//    EXPECT_EQ(num_threads, res.size());
//
//    delete ht;
//    disk_manager->ShutDown();
//    remove("test.db");
//    delete disk_manager;
//    delete bpm;
//  }
//}
//
// TEST(HashTableTest, GrowShrinkConcurrencyTest) {
//  const int num_threads = 5;
//  const int num_runs = 50;
//
//  for (int run = 0; run < num_runs; run++) {
//    auto *disk_manager = new DiskManager("test.db");
//    auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
//    ExtendibleHashTable<int, int, IntComparator> *ht =
//        new ExtendibleHashTable<int, int, IntComparator>("blah", bpm, IntComparator(), HashFunction<int>());
//    std::vector<std::thread> threads(num_threads);
//
//    for (int tid = 0; tid < num_threads; tid++) {
//      threads[tid] = std::thread([&ht, tid]() {
//        for (int i = num_threads * tid; i < num_threads * (tid + 1); i++) {
//          ht->Insert(nullptr, i, i);
//          std::vector<int> res;
//          ht->GetValue(nullptr, i, &res);
//          EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
//          EXPECT_EQ(i, res[0]);
//        }
//      });
//    }
//
//    for (int i = 0; i < num_threads; i++) {
//      threads[i].join();
//    }
//
//    threads.clear();
//
//    threads.resize(num_threads);
//    for (int tid = 0; tid < num_threads; tid++) {
//      threads[tid] = std::thread([&ht, tid]() {
//        for (int i = num_threads * tid; i < num_threads * (tid + 1); i++) {
//          std::vector<int> res;
//          ht->GetValue(nullptr, i, &res);
//          EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
//          EXPECT_EQ(i, res[0]);
//        }
//      });
//    }
//
//    for (int i = 0; i < num_threads; i++) {
//      threads[i].join();
//    }
//
//    threads.clear();
//
//    threads.resize(num_threads);
//    for (int tid = 0; tid < num_threads; tid++) {
//      threads[tid] = std::thread([&ht, tid]() {
//        for (int i = num_threads * tid; i < num_threads * (tid + 1); i++) {
//          ht->Insert(nullptr, i, i);
//          std::vector<int> res;
//          ht->GetValue(nullptr, i, &res);
//          EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
//        }
//        for (int i = num_threads * tid; i < num_threads * (tid + 1); i++) {
//          ht->Remove(nullptr, i, i);
//          std::vector<int> res;
//          ht->GetValue(nullptr, i, &res);
//          EXPECT_EQ(0, res.size()) << "Failed to insert " << tid << std::endl;
//        }
//      });
//    }
//
//    for (int i = 0; i < num_threads; i++) {
//      threads[i].join();
//    }
//
//    threads.clear();
//    delete ht;
//    disk_manager->ShutDown();
//    remove("test.db");
//    delete disk_manager;
//    delete bpm;
//  }
//}
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, uint64_t txn_id_start, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.emplace_back(std::thread(args..., txn_id_start + thread_itr, thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys, uint64_t tid,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    int value = key;
    hash_table->Insert(nullptr, key, value);
  }
  EXPECT_NE(keys[0], keys[1]);
}

// helper function to seperate insert
void InsertHelperSplit(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys,
                       int total_threads, uint64_t tid, __attribute__((unused)) uint64_t thread_itr) {
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int value = key;
      hash_table->Insert(nullptr, key, value);
    }
  }
}

// helper function to delete
void DeleteHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &remove_keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : remove_keys) {
    int value = key;
    hash_table->Remove(nullptr, key, value);
  }
}

// helper function to seperate delete
void DeleteHelperSplit(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &remove_keys,
                       int total_threads, uint64_t tid, __attribute__((unused)) uint64_t thread_itr) {
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int value = key;
      hash_table->Remove(nullptr, key, value);
    }
  }
}

void LookupHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys, uint64_t tid,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    int value = key;
    std::vector<int> result;
    bool res = hash_table->GetValue(nullptr, key, &result);
    EXPECT_EQ(res, true);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], value);
  }
}

const size_t NUM_ITERS = 100;

void InsertTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(25, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // keys to Insert
    std::vector<int> keys;
    int scale_factor = 100;
    for (int key = 1; key < scale_factor; key++) {
      keys.emplace_back(key);
    }

    LaunchParallelTest(2, 0, InsertHelper, &hash_table, keys);

    std::vector<int> result;
    for (auto key : keys) {
      result.clear();
      hash_table.GetValue(nullptr, key, &result);
      EXPECT_EQ(result.size(), 1);

      int value = key;
      EXPECT_EQ(result[0], value);
    }

    hash_table.VerifyIntegrity();

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void InsertTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(25, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // keys to Insert
    std::vector<int> keys;
    int scale_factor = 1000;
    for (int key = 1; key < scale_factor; key++) {
      keys.emplace_back(key);
    }

    LaunchParallelTest(2, 0, InsertHelperSplit, &hash_table, keys, 2);

    std::vector<int> result;
    for (auto key : keys) {
      result.clear();
      hash_table.GetValue(nullptr, key, &result);
      EXPECT_EQ(result.size(), 1);

      int value = key;
      EXPECT_EQ(result[0], value);
    }

    hash_table.VerifyIntegrity();

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}
void DeleteTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(25, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // sequential insert
    std::vector<int> keys = {1, 2, 3, 4, 5};
    InsertHelper(&hash_table, keys, 1);

    std::vector<int> remove_keys = {1, 5, 3, 4};
    LaunchParallelTest(2, 1, DeleteHelper, &hash_table, remove_keys);

    int size = 0;
    std::vector<int> result;
    for (auto key : keys) {
      result.clear();
      int value = key;
      hash_table.GetValue(nullptr, key, &result);
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }
    EXPECT_EQ(size, keys.size() - remove_keys.size());

    hash_table.VerifyIntegrity();

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void DeleteTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(25, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // sequential insert
    std::vector<int> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    InsertHelper(&hash_table, keys, 1);

    std::vector<int> remove_keys = {1, 4, 3, 2, 5, 6};
    LaunchParallelTest(2, 1, DeleteHelperSplit, &hash_table, remove_keys, 2);

    int size = 0;
    std::vector<int> result;
    for (auto key : keys) {
      result.clear();
      int value = key;
      hash_table.GetValue(nullptr, key, &result);
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }
    EXPECT_EQ(size, keys.size() - remove_keys.size());

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(21, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // first, populate index
    std::vector<int> for_insert;
    std::vector<int> for_delete;
    size_t sieve = 2;  // divide evenly
    size_t total_keys = 1000;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        for_insert.emplace_back(i);
      } else {
        for_delete.emplace_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper(&hash_table, for_delete, 1);

    auto insert_task = [&](int tid) { InsertHelper(&hash_table, for_insert, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&hash_table, for_delete, tid); };
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 10;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    int size = 0;
    std::vector<int> result;
    for (auto key : for_insert) {
      result.clear();
      int value = key;
      hash_table.GetValue(nullptr, key, &result);
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }

    EXPECT_EQ(size, for_insert.size());

    hash_table.VerifyIntegrity();

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    auto *disk_manager = new DiskManager("test.db");
    auto *bpm = new BufferPoolManagerInstance(13, disk_manager);
    ExtendibleHashTable<int, int, IntComparator> hash_table("foo_pk", bpm, IntComparator(), HashFunction<int>());

    // Create header_page
    page_id_t page_id;
    bpm->NewPage(&page_id, nullptr);

    // Add perserved_keys
    std::vector<int> perserved_keys;
    std::vector<int> dynamic_keys;
    size_t total_keys = 300;
    size_t sieve = 5;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        perserved_keys.emplace_back(i);
      } else {
        dynamic_keys.emplace_back(i);
      }
    }
    InsertHelper(&hash_table, perserved_keys, 1);
    size_t size;

    auto insert_task = [&](int tid) { InsertHelper(&hash_table, dynamic_keys, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&hash_table, dynamic_keys, tid); };
    auto lookup_task = [&](int tid) { LookupHelper(&hash_table, perserved_keys, tid); };

    std::vector<std::thread> threads;
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    tasks.emplace_back(lookup_task);

    size_t num_threads = 6;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    // Check all reserved keys exist
    size = 0;
    std::vector<int> result;
    for (auto key : perserved_keys) {
      result.clear();
      int value = key;
      hash_table.GetValue(nullptr, key, &result);
      if (std::find(result.begin(), result.end(), value) != result.end()) {
        size++;
      }
    }

    EXPECT_EQ(size, perserved_keys.size());

    hash_table.VerifyIntegrity();

    // Cleanup
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    disk_manager->ShutDown();
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}
// Macro for time out mechanism
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

TEST(HashTableConcurrentTest, InsertTest1) {
  TEST_TIMEOUT_BEGIN
  InsertTest1Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

TEST(HashTableConcurrentTest, InsertTest2) {
  TEST_TIMEOUT_BEGIN
  InsertTest2Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}
TEST(HashTableConcurrentTest, DeleteTest1) {
  TEST_TIMEOUT_BEGIN
  DeleteTest1Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

/*
 * Score: 10
 * Description: Split the concurrent delete task to multiple threads
 * without overlap.
 */
TEST(HashTableConcurrentTest, DeleteTest2) {
  TEST_TIMEOUT_BEGIN
  DeleteTest2Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 60)
}

/*
 * Score: 10
 * Description: Insert a set of keys. Concurrently insert and delete
 * a different set of keys.
 * At the same time, concurrently get the previously inserted keys.
 * Check all the keys get are the same set of keys as previously
 * inserted.
 */
TEST(HashTableConcurrentTest2, MixTest2) {
  TEST_TIMEOUT_BEGIN
  MixTest2Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 120)
}

TEST(HashTableConcurrentTest2, MixTest1) {
  TEST_TIMEOUT_BEGIN
  MixTest1Call();
  TEST_TIMEOUT_FAIL_END(3 * 1000 * 120)
}

}  // namespace bustub
