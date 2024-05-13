//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {


const int UpgradeGraph::can_upgraded_graph_[5][5] = {
  {0, 1, 0, 0, 1}, // 第一行
  {0, 0, 0, 0, 0}, // 第二行
  {1, 1, 0, 1, 1}, // 第三行
  {0, 1, 0, 0, 1}, // 第四行
  {0, 1, 0, 0, 0}  // 第五行
};

const int CompatibleLockGraph::compatible_lock_graph[5][5] = {
  {1, 0, 1, 0, 0},
  {0, 0, 0, 0, 0},
  {1, 0, 1, 1, 1},
  {0, 0, 1, 1, 0},
  {0, 0, 1, 0, 0}
};

void LockManager::MapLockModeToTxnLockSetFunc(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      (txn->GetSharedTableLockSet())->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      (txn->GetExclusiveTableLockSet())->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      (txn->GetIntentionSharedTableLockSet())->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      (txn->GetIntentionExclusiveTableLockSet())->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      (txn->GetSharedIntentionExclusiveTableLockSet())->insert(oid);
      break;
  }
   
}

void LockManager::MapLockModeToTxnLockRemoveFunc(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      (txn->GetSharedTableLockSet())->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      (txn->GetExclusiveTableLockSet())->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      (txn->GetIntentionSharedTableLockSet())->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      (txn->GetIntentionExclusiveTableLockSet())->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      (txn->GetSharedIntentionExclusiveTableLockSet())->erase(oid);
      break;
  }
}


auto LockManager::UpgradeLockTable(Transaction *txn, LockMode cur_mode, LockMode new_mode, const table_oid_t &oid) -> bool {
  if (CanLockUpgrade(cur_mode, new_mode)) {
    MapLockModeToTxnLockRemoveFunc(txn, cur_mode, oid);
    return true;
  }
  return false;
}

inline auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  return UpgradeGraph::can_upgraded_graph_[static_cast<uint>(curr_lock_mode)][static_cast<uint>(requested_lock_mode)] == 1;
}

inline auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  return CompatibleLockGraph::compatible_lock_graph[static_cast<uint>(l1)][static_cast<uint>(l2)] == 1;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode,
                                 std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    // printf("%d abort\n", txn->GetTransactionId());
    for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
         iter++) {
      auto lr = *iter;
      if (lr->txn_id_ == txn->GetTransactionId()) {
        lock_request_queue->request_queue_.erase(iter);
        break;
      }
    }
    // printf("ck\n");

    return true;
  }

  for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
       iter++) {
    auto lr = *iter;
    if (lr->granted_ && !AreLocksCompatible(lock_mode, lr->lock_mode_)) {
      return false;
    }
  }

  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
           iter++) {
        auto lr = *iter;
        if (!lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
          (*iter)->granted_ = true;
          break;
        }
      }
      return true;
    }
    return false;
  }

  bool first_ungranted = true;
  for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
       iter++) {
    auto lr = *iter;
    if (!lr->granted_) {
      if (first_ungranted) {
        if (lr->txn_id_ == txn->GetTransactionId()) {
          (*iter)->granted_ = true;
          return true;
        }
        lr->granted_ = false;
      }

      if (lr->txn_id_ == txn->GetTransactionId()) {
        (*iter)->granted_ = true;
        return true;
      }

      if (!AreLocksCompatible(lock_mode, lr->lock_mode_)) {
        return false;
      }
    }
  }

  return true;
}
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  auto state = txn->GetState();
  auto level = txn->GetIsolationLevel();

  if (state == TransactionState::COMMITTED || state == TransactionState::ABORTED) {
    return false;
  }

  if (level == IsolationLevel::REPEATABLE_READ && state == TransactionState::SHRINKING) {
    return false;
  }

  if (level == IsolationLevel::READ_COMMITTED) {
    if (state == TransactionState::SHRINKING) {
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode == LockMode::SHARED) {
        return false;
      }
    }
  }

  if (level == IsolationLevel::READ_UNCOMMITTED) { 
    if (state == TransactionState::GROWING) {
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
        return false;
      }
    }
    if (state == TransactionState::SHRINKING) {
      return false;
    }
  }

  table_lock_map_latch_.lock();

  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  } 

  auto it = table_lock_map_.find(oid);

  std::unique_lock<std::mutex> l(it->second->latch_);
  table_lock_map_latch_.unlock();

  // try upgrade lock mode
  bool upgraded = false;

  for (auto lrq_it = it->second->request_queue_.begin(); lrq_it != it->second->request_queue_.end(); ++lrq_it) {
    auto lr = *lrq_it;
    if (lr->txn_id_ == txn->GetTransactionId()) {
      if (lr->lock_mode_ == lock_mode) { // same lock mode
        // it->second->cv_.notify_all();
        return true;
      } else {
        if (it->second->upgrading_ != INVALID_TXN_ID) { // upgrade comflict
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        } else {
          it->second->upgrading_ = txn->GetTransactionId();
          it->second->request_queue_.erase(lrq_it);

          if (UpgradeLockTable(txn, lr->lock_mode_, lock_mode, oid)) { // true if compatible, else false
            it->second->request_queue_.emplace_back(std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
            upgraded = true;
          } else {
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
          }
          
          break;
        }
      }
    }
  }

  if (!upgraded)
    it->second->request_queue_.emplace_back(std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));

  while (!CanTxnTakeLock(txn, lock_mode, it->second)) {
    it->second->cv_.wait(l);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    it->second->cv_.notify_all();
    return false;
  }

  MapLockModeToTxnLockSetFunc(txn, lock_mode, oid);
  // it->second->cv_.notify_all();

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
  auto level = txn->GetIsolationLevel();
  
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    return false;
  }

  auto lrq = table_lock_map_[oid];
  std::unique_lock<std::mutex> l(lrq->latch_);
  table_lock_map_latch_.unlock();

  for (auto lr_it = lrq->request_queue_.begin(); lr_it != lrq->request_queue_.end(); ++lr_it) {
    auto lr = *lr_it;
    if (lr->txn_id_ == txn->GetTransactionId() && lr->granted_) {
      auto cur_lock_mode = lr->lock_mode_;
      if (level == IsolationLevel::REPEATABLE_READ) {
        if (cur_lock_mode == LockMode::SHARED || cur_lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } else if (level == IsolationLevel::READ_COMMITTED) {
        if (cur_lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        } 
      } else if (level == IsolationLevel::READ_UNCOMMITTED) {
        if (cur_lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        } 
        if (cur_lock_mode == LockMode::SHARED) {
          return false;
        }
      }

      MapLockModeToTxnLockRemoveFunc(txn, cur_lock_mode, oid);
      lrq->request_queue_.erase(lr_it);
      lrq->cv_.notify_all();
      return true;
    }
  }

  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);

  return false;  
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
