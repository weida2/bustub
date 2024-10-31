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
#include <sstream>
#include "common/exception.h"
#include "common/logger.h"
// #define WZC_DEBUG
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t evict_id = -1;
  size_t max_dist = 0;
  size_t min_timestamp = std::numeric_limits<size_t>::max();
  if (curr_size_ == 0) {
    return false;
  }
  for (const auto &pair : lruknode_hash_) {
    const auto &cur_node = pair.second;
    if (cur_node.IsEvictable()) {
      size_t cur_dist = cur_node.GetBackDist(current_timestamp_);
      size_t cur_min_ts = cur_node.history_.back();
      if (cur_dist == std::numeric_limits<size_t>::max()) {
        if (evict_id == -1 || cur_min_ts < min_timestamp) {
          max_dist = cur_dist;
          min_timestamp = cur_min_ts;
          evict_id = cur_node.GetFrameId();
        }
      } else if (cur_dist > max_dist) {
        max_dist = cur_dist;
        evict_id = cur_node.GetFrameId();
      }
    }
  }
#ifdef WZC_DEBUG
  auto log = std::stringstream();
  log << "evict frame_id: " << evict_id << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
#endif
  if (evict_id != -1) {
    lruknode_hash_.erase(evict_id);
    curr_size_--;
    *frame_id = evict_id;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < replacer_size_, "Invalid frame id.");
  std::lock_guard<std::mutex> lock(latch_);
  if (lruknode_hash_.count(frame_id) == 0) {
    lruknode_hash_.emplace(frame_id, LRUKNode(k_, frame_id));
  }
  lruknode_hash_[frame_id].RecordAccess(current_timestamp_);
  current_timestamp_++;
#ifdef WZC_DEBUG
  auto log = std::stringstream();
  LOG_DEBUG("[LRUKReplacer] replacer_size: %d, cur_size: %ld, k_: %ld", replacer_size_, curr_size_, k_);
  log << "RecordAccess frame_id: " << frame_id;
  LOG_DEBUG("%s, LKNode_history_size: %ld\n", log.str().c_str(), lruknode_hash_[frame_id].history_.size());
#endif
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < replacer_size_, "Invalid frame id.");
  std::lock_guard<std::mutex> lock(latch_);
  if (lruknode_hash_.count(frame_id) == 0) {
    return;
  }
  if (lruknode_hash_[frame_id].IsEvictable() != set_evictable) {
    lruknode_hash_[frame_id].SetEvictable(set_evictable);
    curr_size_ += set_evictable ? 1 : -1;
  }
#ifdef WZC_DEBUG
  LOG_DEBUG("frame_id : %d, evictable: %s, cur_size_: %ld", frame_id,
            lruknode_hash_[frame_id].IsEvictable() ? "true" : "false", curr_size_);
#endif
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < replacer_size_, "Invalid frame id.");
  std::lock_guard<std::mutex> lock(latch_);
#ifdef WZC_DEBUG
  auto log = std::stringstream();
  log << "remove frame_id: " << frame_id << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
#endif
  if (lruknode_hash_.count(frame_id) != 0) {
    if (!lruknode_hash_[frame_id].IsEvictable()) {
      throw ExecutionException("Cannot remove a non-evictable frame.");
    }
    lruknode_hash_.erase(frame_id);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
