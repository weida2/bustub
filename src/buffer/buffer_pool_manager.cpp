//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { 
  delete[] pages_; 
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  // 检查是否有空的帧frame (是否有位置存放)
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    *page_id = AllocatePage();
    
    

    pages_[frame_id].WLatch();
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].ResetMemory();
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;

    replacer_->RecordAccess(frame_id, AccessType::Init);
    replacer_->SetEvictable(frame_id, false);  // may be nouse
    page_table_[*page_id] = frame_id;

    pages_[frame_id].WUnlatch();

    return &pages_[frame_id];
  }

  frame_id_t frame_id;
  if (replacer_->Size() && replacer_->Evict(&frame_id)) {      
    pages_[frame_id].RLatch();
    if (pages_[frame_id].IsDirty()) {
      FlushPage(pages_[frame_id].GetPageId());
      
    }
    pages_[frame_id].RUnlatch();

    *page_id = AllocatePage();
    
    pages_[frame_id].WLatch();

    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].ResetMemory();
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;     

    replacer_->RecordAccess(frame_id, AccessType::Init);
    replacer_->SetEvictable(frame_id, false);  // may be nouse
    page_table_[*page_id] = frame_id;

    pages_[frame_id].WUnlatch();
    
    return &pages_[frame_id];
  }

  return nullptr; // no free frames(frames都被占用了并且不可驱逐)
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;

  auto it = page_table_.find(page_id);

  //该page记录被写过
  if (it != page_table_.end()) {
    frame_id = page_table_[page_id];
    bool is_same = false;
    pages_[frame_id].RLatch();
    // 确定该page现在是在mem中还是disk中
    is_same = pages_[frame_id].GetPageId() == page_id ? true : false;
    pages_[frame_id].RUnlatch();
    
    if (is_same) {
      pages_[frame_id].WLatch();

      pages_[frame_id].pin_count_++;
      replacer_->RecordAccess(frame_id, AccessType::Get);
      replacer_->SetEvictable(frame_id, false);  // may be nouse

      pages_[frame_id].WUnlatch();
      
      return &pages_[frame_id];
    } else {
        if (replacer_->Size() && replacer_->Evict(&frame_id)) {            
          pages_[frame_id].RLatch();
          if (pages_[frame_id].IsDirty()) {
            FlushPage(pages_[frame_id].GetPageId());
          }
          pages_[frame_id].RUnlatch();
          
          pages_[frame_id].WLatch();

          pages_[frame_id].page_id_ = page_id;
          
          disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
          pages_[frame_id].pin_count_ = 1;
          pages_[frame_id].is_dirty_ = false;     

          replacer_->RecordAccess(frame_id, AccessType::Init);
          replacer_->SetEvictable(frame_id, false);  // may be nouse
          page_table_[page_id] = frame_id;

          pages_[frame_id].WUnlatch();
          
          return &pages_[frame_id];
        }        
    }
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  
  frame_id_t frame_id;
  if (!page_table_.count(page_id) || (pages_[page_table_[page_id]].GetPageId() != page_id)) {
    return false;
  }
  
  frame_id = page_table_[page_id];
  pages_[frame_id].RLatch();
  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }
  pages_[frame_id].RUnlatch();

  pages_[frame_id].WLatch(); 

  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }

  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }  
  pages_[frame_id].WUnlatch();

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  // std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  
  if (!page_table_.count(page_id) || (pages_[page_table_[page_id]].GetPageId() != page_id)) {
    return false;
  }

  frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());

  pages_[frame_id].is_dirty_ = false;

  return true; 
}

void BufferPoolManager::FlushAllPages() {
  // std::lock_guard<std::mutex> lock(latch_);
  
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].IsDirty()){
      FlushPage(pages_[i].GetPageId());
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_.count(page_id) || (pages_[page_table_[page_id]].GetPageId() != page_id)) {
    return false;
  }
  
  frame_id = page_table_[page_id];
  pages_[frame_id].RLatch();
  if (pages_[frame_id].GetPinCount() > 0) {
    pages_[frame_id].RUnlatch();
    return false;
  }

  pages_[frame_id].WLatch();

  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false; 

  free_list_.emplace_back(frame_id);    
  replacer_->Remove(frame_id);
  page_table_.erase(page_id);

  //disk_manager_->DeallocatePage(page_id);

  pages_[frame_id].WUnlatch();


  return false; 
}



auto BufferPoolManager::AllocatePage() -> page_id_t { 
  return next_page_id_++; 
}



auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { 
  return {this, nullptr}; 
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { 
  return {this, nullptr}; 
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
  return {this, nullptr}; 
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { 
  return {this, nullptr}; 
}

}  // namespace bustub
