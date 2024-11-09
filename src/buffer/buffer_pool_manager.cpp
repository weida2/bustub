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

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // 检查是否有空的帧frame (是否有位置存放)
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    *page_id = AllocatePage();
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].ResetMemory();
    replacer_->RecordAccess(frame_id, AccessType::Init);
    replacer_->SetEvictable(frame_id, false);  // may be nouse
    page_table_[*page_id] = frame_id;
    return &pages_[frame_id];
  }

  if (replacer_->Evict(&frame_id)) {
    page_id_t evict_page_id = pages_[frame_id].page_id_;
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evict_page_id, pages_[frame_id].data_);
    }
    *page_id = AllocatePage();
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].ResetMemory();
    page_table_.erase(evict_page_id);
    page_table_[*page_id] = frame_id;
    replacer_->RecordAccess(frame_id, AccessType::Init);
    replacer_->SetEvictable(frame_id, false);  // may be nouse

    return &pages_[frame_id];
  }

  return nullptr;  // no free frames(frames都被占用了并且不可驱逐)
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  auto it = page_table_.find(page_id);
  // 该page记录在frame中
  if (it != page_table_.end()) {
    frame_id = page_table_.at(page_id);
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id, AccessType::Get);
    replacer_->SetEvictable(frame_id, false);  // may be nouse

    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    page_table_.insert(std::make_pair(page_id, frame_id));
    replacer_->RecordAccess(frame_id, AccessType::Init);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  if (replacer_->Evict(&frame_id)) {
    page_id_t evict_page_id = pages_[frame_id].page_id_;
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evict_page_id, pages_[frame_id].data_);
    }
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    page_table_.erase(evict_page_id);
    page_table_.insert(std::make_pair(page_id, frame_id));
    replacer_->RecordAccess(frame_id, AccessType::Init);
    replacer_->SetEvictable(frame_id, false);  // may be nouse

    return &pages_[frame_id];
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id = page_table_.at(page_id);
  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id = page_table_.at(page_id);
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].IsDirty()) {
      FlushPage(pages_[i].GetPageId());
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id = page_table_.at(page_id);
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();

  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_.erase(page_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *fetch_page = FetchPage(page_id);
  return BasicPageGuard{this, fetch_page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *fetch_page = FetchPage(page_id);
  fetch_page->RLatch();
#ifdef WZC_
  auto log = std::stringstream();
  log << "--[thread " << std::this_thread::get_id() << "] | 获取page: " << fetch_page->GetPageId() << " 读 锁"
      << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
#endif
  return ReadPageGuard{this, fetch_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *fetch_page = FetchPage(page_id);
  fetch_page->WLatch();
#ifdef WZC_
  auto log = std::stringstream();
  log << "--[thread " << std::this_thread::get_id() << "] | 获取page: " << fetch_page->GetPageId() << " 写 锁"
      << std::endl;
  LOG_DEBUG("%s", log.str().c_str());
#endif
  return WritePageGuard{this, fetch_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *fetch_page = NewPage(page_id);
  return BasicPageGuard{this, fetch_page};
}

}  // namespace bustub
