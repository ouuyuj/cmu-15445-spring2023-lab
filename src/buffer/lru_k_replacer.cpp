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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // curr_timestamp_ =
  //   std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> l(latch_);

  frame_id_t fid;
  bool evicted = false;

  if (!hist_list_.empty()) {
    auto it = std::find_if(hist_list_.begin(), hist_list_.end(),
                           [this](const auto &x) { return node_store_[x].is_evictable_; });

    if (it != hist_list_.end()) {
      fid = *it;
      *frame_id = fid;
      hist_list_.erase(it);
      node_store_.erase(fid);
      curr_size_--;
      evicted = true;
    }
  }
  if (!evicted && !cache_list_.empty()) {
    auto it = std::find_if(cache_list_.begin(), cache_list_.end(),
                           [this](const auto &x) { return node_store_[x].is_evictable_; });

    if (it != cache_list_.end()) {
      fid = *it;
      *frame_id = fid;
      cache_list_.erase(it);
      node_store_.erase(fid);
      curr_size_--;
      evicted = true;
    }
  }

  return evicted;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> l(latch_);
  BUSTUB_ASSERT((((size_t)(frame_id)) < replacer_size_ && frame_id >= 0), "frame id out of replacer size");

  if (node_store_.count(frame_id) == 0U) {  // 首次访问
    auto lruk_node = LRUKNode(1);
    node_store_.insert({frame_id, lruk_node});

    hist_list_.push_back(frame_id);
  } else {
    size_t new_access_cnt = ++node_store_[frame_id].access_cnt_;
    frame_id_t fid;
    if (new_access_cnt == k_) {
      auto it =
          std::find_if(hist_list_.begin(), hist_list_.end(), [&frame_id](const auto &x) { return x == frame_id; });
      fid = *it;

      hist_list_.erase(it);
      cache_list_.push_back(fid);
    } else if (new_access_cnt > k_) {
      auto it =
          std::find_if(cache_list_.begin(), cache_list_.end(), [&frame_id](const auto &x) { return x == frame_id; });
      fid = *it;

      cache_list_.erase(it);
      cache_list_.push_back(fid);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> l(latch_);
  BUSTUB_ASSERT((((size_t)(frame_id)) < replacer_size_ && frame_id >= 0), "frame id out of replacer size");

  if (node_store_.count(frame_id) == 0U) {
    return;
  }

  bool pre_evictable = node_store_[frame_id].is_evictable_;
  if (pre_evictable && !set_evictable) {
    curr_size_--;
  } else if (!pre_evictable && set_evictable) {
    curr_size_++;
  }
  node_store_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> l(latch_);
  BUSTUB_ASSERT((((size_t)(frame_id)) < replacer_size_ && frame_id >= 0), "frame id out of replacer size");

  if (node_store_.count(frame_id) == 0U) {
    return;
  }

  BUSTUB_ASSERT((node_store_[frame_id].is_evictable_), "remove frame falled");

  if (node_store_[frame_id].access_cnt_ >= k_) {
    cache_list_.remove_if([&frame_id](const auto &x) { return frame_id == x; });
  } else {
    hist_list_.remove_if([&frame_id](const auto &x) { return frame_id == x; });
  }

  if (node_store_.erase(frame_id) > 0) {
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
