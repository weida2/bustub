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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::lock_guard<std::mutex> lock(latch_);
    
    frame_id_t evict_id = -1;
    size_t max_dist = 0;
    size_t min_timestamp = std::numeric_limits<size_t>::max(); 

    for (const auto &pair: LRUKNode_hash) {
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

    if (evict_id != -1) {
        LRUKNode_hash.erase(evict_id);
        curr_size_--;
        *frame_id = evict_id;
        return true;
    }


    return false; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    BUSTUB_ASSERT(frame_id >= 0 && frame_id < replacer_size_, "Invalid frame id.");
    
    std::lock_guard<std::mutex> lock(latch_);

    if (!LRUKNode_hash.count(frame_id)) {
        LRUKNode_hash.emplace(frame_id, LRUKNode(k_, frame_id));
    }
    LRUKNode_hash[frame_id].RecordAccess(current_timestamp_);
    current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT(frame_id >= 0 && frame_id < replacer_size_, "Invalid frame id.");
    std::lock_guard<std::mutex> lock(latch_);

    if (!LRUKNode_hash.count(frame_id)) {
        return ;
    }
    
    if (LRUKNode_hash[frame_id].IsEvictable() != set_evictable) {
        LRUKNode_hash[frame_id].SetEvictable(set_evictable);
        curr_size_ += set_evictable ? 1 : -1;
    }
    
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    BUSTUB_ASSERT(frame_id >= 0 && frame_id < replacer_size_, "Invalid frame id.");
    
    std::lock_guard<std::mutex> lock(latch_); 

    if (LRUKNode_hash.count(frame_id)) {
        if (!LRUKNode_hash[frame_id].IsEvictable()) {
            throw std::runtime_error("Cannot remove a non-evictable frame.");
        }
        LRUKNode_hash.erase(frame_id);
        curr_size_--;
    }

}

auto LRUKReplacer::Size() -> size_t { 
    std::lock_guard<std::mutex> lock(latch_);
    return curr_size_;
}

}  // namespace bustub
