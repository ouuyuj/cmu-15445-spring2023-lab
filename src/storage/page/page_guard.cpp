#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept : BasicPageGuard(that.bpm_, that.page_) {
  this->is_dirty_ = that.is_dirty_;
  // 将对象置于有效但未指定状态，确保不会发生两次析构释放掉指针所指向的内存

  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
  }

  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    if (page_ != nullptr && bpm_ != nullptr) {
      bpm_->UnpinPage(PageId(), is_dirty_);
    }

    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;

    that.page_ = nullptr;
    that.bpm_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() {  // NOLINT
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
    page_ = nullptr;
    bpm_ = nullptr;
    is_dirty_ = false;
  }
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
      guard_.page_->RUnlatch();
      guard_.Drop();
    }
    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() {  // NOLINT
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
      guard_.page_->WUnlatch();
      guard_.Drop();
    }
    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() {  // NOLINT
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

}  // namespace bustub
