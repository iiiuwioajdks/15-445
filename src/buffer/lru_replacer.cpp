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

LRUReplacer::LRUReplacer(size_t num_pages) { page_num_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  mu_.lock();
  if (list_.empty()) {
    mu_.unlock();
    return false;
  }
  *frame_id = list_.back();
  list_.pop_back();
  mu_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  mu_.lock();
  list_.remove(frame_id);
//  list_.erase(std::find(list_.begin(), list_.end(), frame_id));
  mu_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  mu_.lock();
  std::list<frame_id_t>::iterator it = std::find(list_.begin(), list_.end(), frame_id);
  if (it != list_.end()) {
    // find
    mu_.unlock();
    return;
  }
  mu_.unlock();
  while (Size() >= page_num_) {
    list_.back();
    list_.pop_back();
  }
  mu_.lock();
  list_.push_front(frame_id);
  mu_.unlock();
}

size_t LRUReplacer::Size() {
  mu_.lock();
  size_t size = list_.size();
  mu_.unlock();
  return size;
}

}  // namespace bustub
