//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <exception>
#include <iterator>
#include <limits>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);
  size_t tmp_min;
  int index_min = -1;
  size_t i = 0;
  bool flag = false;
  // 1、在visited_cnt>=k_的情况下，淘汰timestamp最早的那个
  // 2、有visited_cnt<k的情况下，优先淘汰
  // 3、有多个+inf时，优先淘汰timestamp最早的那个
  if (!level1_.empty()) {
    for (const auto &elem : level1_) {
      if (!flag && node_store_[elem].is_evictable_) {
        tmp_min = elem;
        index_min = i;
        flag = true;
      }

      if (node_store_[elem].history_.back() < node_store_[tmp_min].history_.back() && node_store_[elem].is_evictable_) {
        tmp_min = elem;
        index_min = i;
      }
      ++i;
    }
    if (index_min != -1) {
      auto it1 = level1_.begin();
      std::advance(it1, index_min);
      level1_.erase(it1);
      curr_size_--;
      node_store_.erase(tmp_min);
      std::cout << "被逐出的Frame: " << tmp_min << std::endl;
      return tmp_min;
    }
  }
  if (!level2_.empty()) {
    i = 0;  // 重置i
    for (const auto &elem : level2_) {
      if (!flag && node_store_[elem].is_evictable_) {
        tmp_min = elem;
        index_min = i;
        flag = true;
        std::cout << "tmp_min" << tmp_min << "的最远访问时间是: "
                  << " " << node_store_[tmp_min].history_.back() << std::endl;
      }
      std::cout << "当前elem" << elem << "的最远访问时间是: "
                << " " << node_store_[elem].history_.back() << std::endl;
      if (node_store_[elem].history_.back() < node_store_[tmp_min].history_.back() && node_store_[elem].is_evictable_) {
        tmp_min = elem;
        index_min = i;
      }
      ++i;
    }
    if (index_min != -1) {
      auto it2 = level2_.begin();
      std::advance(it2, index_min);
      level2_.erase(it2);
      curr_size_--;
      node_store_.erase(tmp_min);
      std::cout << "被逐出的Frame: " << tmp_min << std::endl;
      return tmp_min;
    }
  }
  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    level1_.push_back(frame_id);
    node_store_[frame_id].fid_ = frame_id;
    node_store_[frame_id].level_ = 1;
  }

  if (node_store_[frame_id].history_.size() == k_) {  // 如果这个frame的history大小达到临界值了
    node_store_[frame_id].history_.pop_back();
  }

  current_timestamp_++;
  node_store_[frame_id].history_.push_front(current_timestamp_);

  node_store_[frame_id].visited_cnt_++;

  if (node_store_[frame_id].visited_cnt_ == k_) {  // 访问次数第一次达到阈值放入level2
    for (auto it = level1_.begin(); it != level1_.end(); it++) {
      if (*it == frame_id) {
        level1_.erase(it);
        break;
      }
    }
    level2_.push_back(frame_id);
    node_store_[frame_id].level_ = 2;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  if (!node_store_[frame_id].is_evictable_ && set_evictable) {
    curr_size_++;
  }
  if (node_store_[frame_id].is_evictable_ && !set_evictable) {
    curr_size_--;
  }
  node_store_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  if (!node_store_[frame_id].is_evictable_) {
    throw std::exception();
  }

  if (node_store_[frame_id].level_ == 1) {
    for (auto it = level1_.begin(); it != level1_.end(); it++) {
      if (*it == frame_id) {
        level1_.erase(it);
        break;
      }
    }
  } else {
    for (auto it = level2_.begin(); it != level2_.end(); it++) {
      if (*it == frame_id) {
        level2_.erase(it);
        break;
      }
    }
  }
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
