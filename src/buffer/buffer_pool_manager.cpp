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
  pages_ = new Page[pool_size_];                                      //
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);  //

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t fid;
  page_id_t pid;
  page_id_t pre_page_id;

  std::lock_guard<std::mutex> l(latch_);

  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&fid)) {
    pre_page_id = pages_[fid].page_id_;
    page_table_.erase(pre_page_id);

  } else {
    return nullptr;
  }
  pid = AllocatePage();

  if (pages_[fid].IsDirty()) {
    disk_manager_->WritePage(pre_page_id, pages_[fid].data_);
    pages_[fid].is_dirty_ = false;
  }

  pages_[fid].ResetMemory();
  disk_manager_->ReadPage(pid, pages_[fid].data_);
  page_table_.insert({pid, fid});  //
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  *page_id = pid;

  pages_[fid].pin_count_ = 1;
  pages_[fid].page_id_ = pid;
  return &pages_[fid];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t fid;
  page_id_t pre_page_id;
  std::lock_guard<std::mutex> l(latch_);

  if (page_table_.count(page_id) != 0U) {
    fid = page_table_[page_id];
    pages_[fid].pin_count_++;
  } else {
    if (!free_list_.empty()) {
      fid = free_list_.front();
      free_list_.pop_front();
    } else if (replacer_->Evict(&fid)) {
      pre_page_id = pages_[fid].page_id_;
      page_table_.erase(pre_page_id);

      if (pages_[fid].IsDirty()) {
        disk_manager_->WritePage(pre_page_id, pages_[fid].data_);
        pages_[fid].is_dirty_ = false;
      }
      pages_[fid].ResetMemory();

    } else {
      return nullptr;
    }

    page_table_.insert({page_id, fid});
    disk_manager_->ReadPage(page_id, pages_[fid].data_);

    pages_[fid].pin_count_ = 1;
    pages_[fid].page_id_ = page_id;
  }

  replacer_->SetEvictable(fid, false);
  replacer_->RecordAccess(fid);  //

  return &pages_[fid];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> l(latch_);
  frame_id_t fid;

  if (page_table_.count(page_id) != 0U) {
    fid = page_table_[page_id];
  } else {
    return false;
  }

  if (pages_[fid].pin_count_ > 0) {
    if ((--pages_[fid].pin_count_) == 0) {
      replacer_->SetEvictable(fid, true);
    }
    if (is_dirty) {  // 注意这里的细节
      pages_[fid].is_dirty_ = is_dirty;
    }
    return true;
  }

  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> l(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.count(page_id) != 0U) {
    disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);
    pages_[page_table_[page_id]].is_dirty_ = false;
    return true;
  }

  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> l(latch_);

  for (auto &x : page_table_) {
    FlushPage(x.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> l(latch_);

  if (page_table_.count(page_id) == 0U) {
    return true;
  }

  frame_id_t fid = page_table_[page_id];

  if (pages_[fid].GetPinCount() == 0) {
    page_table_.erase(page_id);
    free_list_.push_back(fid);
    pages_[fid].ResetMemory();
    pages_[fid].page_id_ = INVALID_PAGE_ID;
    pages_[fid].is_dirty_ = false;
    DeallocatePage(page_id);
    replacer_->Remove(fid);
    return true;
  }

  return false;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // std::scoped_lock<std::mutex> l(latch_);

  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // std::scoped_lock<std::mutex> l(latch_);
  auto *page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  page->RLatch();
  // latch_.unlock();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // std::scoped_lock<std::mutex> l(latch_);
  auto *page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  page->WLatch();  // 注意需要先判定page是否为nullptr

  // latch_.unlock();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  // std::scoped_lock<std::mutex> l(latch_);

  return {this, NewPage(page_id)};
}

}  // namespace bustub
