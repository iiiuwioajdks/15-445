//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  auto mask = dir_page->GetGlobalDepthMask();
  return mask & Hash(key);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  auto idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *dir_page;
  dir_latch_.lock();
  if (directory_page_id_ == INVALID_PAGE_ID) {
    // new directory page
    Page *page = buffer_pool_manager_->NewPage(&directory_page_id_);
    dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
    dir_page->SetPageId(directory_page_id_);
    // new bucket page
    page_id_t new_bucket_id = INVALID_PAGE_ID;
    page = nullptr;
    page = buffer_pool_manager_->NewPage(&new_bucket_id);
    dir_page->SetBucketPageId(0, new_bucket_id);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    buffer_pool_manager_->UnpinPage(new_bucket_id, true);
  }
  dir_latch_.unlock();
  assert(directory_page_id_ != -1);
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page != nullptr);
  dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  assert(dir_page != nullptr);
  return dir_page;
}

// todo : need to change
template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);
  return page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::GetHashBucketPage(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // todo
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_idx = KeyToPageId(key, dir_page);
  auto page = FetchBucketPage(bucket_page_idx);
  auto bucket_page = GetHashBucketPage(page);
  page->RLatch();
  bool ret = bucket_page->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(bucket_page_idx, false);
  page->RUnlatch();
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  //    std::cout<<key<<std::endl;
  // locate
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_idx = KeyToPageId(key, dir_page);
  auto page = FetchBucketPage(bucket_page_idx);
  auto bucket_page = GetHashBucketPage(page);
  page->WLatch();
  if (!bucket_page->IsFull()) {
    auto success = bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(bucket_page_idx, true);
    page->WUnlatch();
    table_latch_.RUnlock();
    return success;
  }
  // todo : otherwise, bucket full, need to split
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(bucket_page_idx, false);
  page->WUnlatch();
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // todo : i need to split the bucket
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto global_depth = dir_page->GetGlobalDepth();
  uint32_t split_key_bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t split_local_depth = dir_page->GetLocalDepth(split_key_bucket_idx);
  //  std::cout << "start split:" << key << " " << KeyToDirectoryIndex(key, dir_page) << std::endl;
  assert(split_local_depth <= global_depth);

  // start split
  auto split_bucket_page_id = KeyToPageId(key, dir_page);
  auto page = FetchBucketPage(split_bucket_page_id);
  page->WLatch();
  auto split_key_bucket_page = GetHashBucketPage(page);
  if (!split_key_bucket_page->IsFull()) {
    split_key_bucket_page->Insert(key, value, comparator_);
    page->WUnlatch();
    table_latch_.WUnlock();
    return true;
  }
  // todo : latch
  if (split_local_depth >= 9) {
    page->WUnlatch();
    table_latch_.WUnlock();
    return false;
  }
  page_id_t image_bucket_id = INVALID_PAGE_ID;
  auto image_bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&image_bucket_id)->GetData());
  //  std::cout<<split_local_depth<<":"<<global_depth<<std::endl;
  //  std::cout<<split_bucket_page_id<<":"<<image_bucket_id<<std::endl;
  if (split_local_depth < global_depth) {
    // todo : set
    // first: incr
    dir_page->IncrLocalDepth(split_key_bucket_idx);
    //    dir_page->SetBucketPageId(split_key_bucket_idx, image_bucket_id);
    uint32_t mask = (1 << dir_page->GetLocalDepth(split_key_bucket_idx)) - 1;
    for (uint32_t i = 0; i < uint32_t(1 << global_depth); ++i) {
      //      std::cout << "debug:" << dir_page->GetBucketPageId(i) << ":" << split_bucket_page_id << std::endl;
      if (i == split_key_bucket_idx) {
        continue;
      }
      if (uint32_t(dir_page->GetBucketPageId(i)) == split_bucket_page_id) {
        dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_key_bucket_idx));
        //        std::cout<<"my flag::"<<i<<" "<<mask<<" "<<(mask&i)<<" "<<split_key_bucket_idx<<std::endl;
        if ((mask & i) != split_key_bucket_idx) {
          dir_page->SetBucketPageId(i, image_bucket_id);
        }
      }
    }
  } else if (split_local_depth == global_depth) {
    // first: grow
    auto mask = dir_page->GetGlobalDepthMask();
    //    std::cout<<mask<<"  "<<dir_page->GetGlobalDepth()<<std::endl;
    global_depth++;
    dir_page->IncrGlobalDepth();
    // second: rehash directory->new bucket
    //    std::cout << "flag:" << uint32_t(1 << (global_depth - 1)) << ":" << uint32_t(1 << global_depth) << std::endl;
    if (mask == uint32_t(0)) {
      dir_page->IncrLocalDepth(split_key_bucket_idx);
      dir_page->SetBucketPageId(1, image_bucket_id);
      dir_page->SetLocalDepth(1, ++split_local_depth);
    } else {
      for (uint32_t i = uint32_t(1 << (global_depth - 1)); i < uint32_t(1 << global_depth); ++i) {
        auto map_bucket_idx = mask & i;
        //        std::cout << mask << "  " << map_bucket_idx << "  " << split_key_bucket_idx << std::endl;
        if (map_bucket_idx == split_key_bucket_idx) {
          dir_page->IncrLocalDepth(split_key_bucket_idx);
          dir_page->SetBucketPageId(i, image_bucket_id);
          dir_page->SetLocalDepth(i, ++split_local_depth);
        } else {
          auto map_page_id = dir_page->GetBucketPageId(map_bucket_idx);
          dir_page->SetBucketPageId(i, map_page_id);
          dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(map_bucket_idx));
        }
      }
    }
  }
  // rehash data
  std::list<MappingType> list;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (split_key_bucket_page->IsReadable(i)) {
      auto key = split_key_bucket_page->KeyAt(i);
      auto value = split_key_bucket_page->ValueAt(i);
      list.push_back(MappingType(key, value));
    }
  }
  //  std::cout<<list.size()<<std::endl;
  split_key_bucket_page->Reset();
  //  std::cout<<dir_page->GetGlobalDepth()<<":ajja:"<<dir_page->GetGlobalDepthMask()<<std::endl;
  while (!list.empty()) {
    MappingType pair = list.front();
    list.pop_front();
    auto key = pair.first;
    auto hash = KeyToDirectoryIndex(key, dir_page);
    //    std::cout<<key<<" "<<hash<<"  "<<split_key_bucket_idx<<std::endl;
    if (hash == split_key_bucket_idx) {
      assert(split_key_bucket_page->Insert(key, pair.second, comparator_) == true);
      //      std::cout<<"insert:"<<key<<std::endl;
    } else {
      image_bucket_page->Insert(key, pair.second, comparator_);
    }
  }
  //  std::cout<<dir_page->GetGlobalDepth()<<std::endl;
  //  dir_page->PrintDirectory();
  buffer_pool_manager_->UnpinPage(split_bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(image_bucket_id, true);
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  page->WUnlatch();
  table_latch_.WUnlock();
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  //  auto bucket_idx = KeyToDirectoryIndex(key ,dir_page);
  auto bucket_id = KeyToPageId(key, dir_page);
  auto page = FetchBucketPage(bucket_id);
  auto bucket_page = GetHashBucketPage(page);
  page->WLatch();
  // remove
  bool ret = bucket_page->Remove(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(bucket_id, true);
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  page->WUnlatch();
  table_latch_.RUnlock();
  // if empty : merge
  if (bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  //  std::cout << "start merge: " << bucket_idx << std::endl;
  page_id_t bucket_id = KeyToPageId(key, dir_page);
  auto page = FetchBucketPage(bucket_id);
  page->RLatch();
  auto bucket_page = GetHashBucketPage(page);
  //  There are three conditions under which we skip the merge:
  // 1. The bucket is no longer empty.
  if (!bucket_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(bucket_id, false);
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    page->RUnlatch();
    table_latch_.WUnlock();
    return;
  }
  // 2. The bucket has local depth 0.
  auto local_depth = dir_page->GetLocalDepth(bucket_idx);
  if (local_depth == 0) {
    buffer_pool_manager_->UnpinPage(bucket_id, false);
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    page->RUnlatch();
    table_latch_.WUnlock();
    return;
  }
  // 3. The bucket's local depth doesn't match its split image's local depth.
  // find image index
  uint32_t highest_bit = bucket_idx >> (local_depth - 1);
  uint32_t image_idx;
  if (highest_bit == 0) {
    image_idx = (1 << (local_depth - 1)) + bucket_idx;
  } else {
    image_idx = ~(1 << (local_depth - 1)) & bucket_idx;
  }
  auto image_depth = dir_page->GetLocalDepth(image_idx);
  if (local_depth != image_depth) {
    buffer_pool_manager_->UnpinPage(bucket_id, false);
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    page->RUnlatch();
    table_latch_.WUnlock();
    return;
  }

  // remove page
  buffer_pool_manager_->UnpinPage(bucket_id, false);
  buffer_pool_manager_->DeletePage(bucket_id);

  // merge
  page_id_t image_id = dir_page->GetBucketPageId(image_idx);
  if (image_id == bucket_id) {
    //    dir_page->PrintDirectory();
    //    dir_page->DecrLocalDepth(bucket_idx);
  } else {
    dir_page->SetBucketPageId(bucket_idx, image_id);
    dir_page->DecrLocalDepth(bucket_idx);
    dir_page->DecrLocalDepth(image_idx);
    for (uint32_t i = 0; i < dir_page->Size(); ++i) {
      if (dir_page->GetBucketPageId(i) == bucket_id || dir_page->GetBucketPageId(i) == image_id) {
        dir_page->SetBucketPageId(i, image_id);
        dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(image_idx));
      }
    }
  }

  // shrink
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  page->RUnlatch();
  table_latch_.WUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Debug() {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->PrintDirectory();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
