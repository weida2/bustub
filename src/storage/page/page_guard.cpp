#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept 
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
        that.bpm_ = nullptr;
        that.page_ = nullptr;
        that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
    if (bpm_ && page_) {
        bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
        bpm_ = nullptr;
        is_dirty_ = false;
    }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
    if (this != &that) {
        Drop();
        bpm_ = that.bpm_;
        page_ = that.page_;
        is_dirty_ = that.is_dirty_;

        that.bpm_ = nullptr;
        that.page_ = nullptr;
        that.is_dirty_ = false;
    } 
    return *this; 
}

BasicPageGuard::~BasicPageGuard(){
    Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {
    if (page) {
        page->RLatch(); // 获取读锁
    }
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept 
    : guard_(std::move(that.guard_)) {
    // 使移动后的对象失效
    that.guard_ = BasicPageGuard();
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
    if (this != &that) {
        Drop(); // 如果存在则解锁当前页面
        guard_ = std::move(that.guard_);
        that.guard_ = BasicPageGuard(); // 使移动后的对象失效
    }
    return *this;
}

void ReadPageGuard::Drop() {
    if (guard_.GetData()) {
        guard_.page_->RUnlatch(); // 释放读锁
    }
    guard_.Drop(); // 解锁页面
}

ReadPageGuard::~ReadPageGuard() {
    Drop(); // 确保页面在销毁时被解锁和解锁
}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {
    if (page) {
        page->WLatch(); // 获取写锁
    }
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept 
    : guard_(std::move(that.guard_)) {
    // 使移动后的对象失效
    that.guard_ = BasicPageGuard();
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
    if (this != &that) {
        Drop(); // 如果存在则解锁当前页面
        guard_ = std::move(that.guard_);
        that.guard_ = BasicPageGuard(); // 使移动后的对象失效
    }
    return *this;
}

void WritePageGuard::Drop() {
    if (guard_.GetData()) {
        guard_.page_->WUnlatch(); // 释放写锁
    }
    guard_.Drop(); // 解锁页面
}

WritePageGuard::~WritePageGuard() {
    Drop();
}  // NOLINT

}  // namespace bustub
