//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  page_num_ = num_pages;
//  std::pair<bool, std::list<frame_id_t>::iterator> null_data(false, list_.end());
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mu_);
  if (list_.empty()) {
    return false;
  }
  *frame_id = list_.back();
  list_.pop_back();
  hash_[*frame_id].first = false;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mu_);
  if(hash_[frame_id].first) {
    list_.erase(hash_[frame_id].second);
    hash_[frame_id].first = false;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mu_);
  if(!hash_[frame_id].first) {
    list_.push_front(frame_id);
    hash_[frame_id].first = true;
    hash_[frame_id].second = list_.begin();
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(mu_);
  return list_.size();
}

}  // namespace bustub
