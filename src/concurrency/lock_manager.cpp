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

const int CompatibleLockGraph::compatible_lock_graph_[5][5] = {
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
  return CompatibleLockGraph::compatible_lock_graph_[static_cast<uint>(l1)][static_cast<uint>(l2)] == 1;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode,
                                 std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    
    for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
         iter++) {
      auto lr = *iter;
      if (lr->txn_id_ == txn->GetTransactionId()) {
        lock_request_queue->request_queue_.erase(iter);
        break;
      }
    }

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

auto LockManager::CheckAllRowsUnlockInLM(Transaction *txn, const table_oid_t &oid) -> bool {
  row_lock_map_latch_.lock();
  
  for (auto [k, v] : row_lock_map_) {
    row_lock_map_latch_.unlock();

    // std::lock_guard<std::mutex> ql(v->latch_);
    v->latch_.lock();
    for (auto lr : v->request_queue_) {
      if (lr->oid_ == oid && lr->txn_id_ == txn->GetTransactionId() && lr->granted_) {
        v->latch_.unlock();
        row_lock_map_latch_.unlock();
        return false;
      }
    }
    v->latch_.unlock();

    row_lock_map_latch_.lock();
  }
  row_lock_map_latch_.unlock();

  return true;
}

auto LockManager::CheckAllRowsUnlockInTxn(Transaction *txn, const table_oid_t &oid) -> bool {
  auto srls = txn->GetSharedRowLockSet();
  auto xrls = txn->GetExclusiveRowLockSet();
  return false;
}

inline auto LockManager::CheckIsolationLevel(TransactionState state, IsolationLevel level, LockMode lock_mode) -> bool {
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
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  auto state = txn->GetState();
  auto level = txn->GetIsolationLevel();

  if (!CheckIsolationLevel(state, level, lock_mode)) {
    return false;
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
  if (!CheckAllRowsUnlockInLM(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

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

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool {
  table_lock_map_latch_.lock();
  auto lrq = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  std::lock_guard<std::mutex> l(lrq->latch_);
  // if (row_lock_mode == LockMode::SHARED) {
  //   for (auto lr : lrq->request_queue_) {
  //     if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId() && (lr->lock_mode_ == LockMode::INTENTION_SHARED || 
  //         lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
  //       return true;
  //     }
  //   }
  // }

  if (row_lock_mode == LockMode::SHARED) {
    for (auto lr : lrq->request_queue_) {
      if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
        return true;
      }
    }
  }

  if (row_lock_mode == LockMode::EXCLUSIVE) {
    for (auto lr : lrq->request_queue_) {
      if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId() && (lr->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || 
          lr->lock_mode_ == LockMode::EXCLUSIVE || lr->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        return true;
      }
    }
  }
  
  return false;
}

void LockManager::MapLockModeToTxnRowLockSetFunc(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
  }
}

void LockManager::MapLockModeToTxnRowLockRemoveFunc(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
  }
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  auto state = txn->GetState();
  auto level = txn->GetIsolationLevel();

  if (state == TransactionState::COMMITTED || state == TransactionState::ABORTED) {
    return false;
  }

  // if (state == TransactionState::SHRINKING) {
  //   if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
  //     txn->SetState(TransactionState::ABORTED);
  //     throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  //   }
  // }

  if (state == TransactionState::SHRINKING) {
    if (level == IsolationLevel::REPEATABLE_READ || level == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    } else {
      if (lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }

  // try {
  if (!CheckAppropriateLockOnTable(txn, oid, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  // } catch (TransactionAbortException &e) {
  //   if (e.GetAbortReason() == AbortReason::UPGRADE_CONFLICT || e.GetAbortReason() == AbortReason::INCOMPATIBLE_UPGRADE) {

  //   }
  // }

  // if (lock_mode == LockMode::SHARED && !CheckAppropriateLockOnTable(txn, oid, LockMode::INTENTION_SHARED) &&
  //     !CheckAppropriateLockOnTable(txn, oid, LockMode::SHARED) &&
  //     !CheckAppropriateLockOnTable(txn, oid, LockMode::SHARED_INTENTION_EXCLUSIVE)) {
  //   txn->SetState(TransactionState::ABORTED);
  //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  // } else if (lock_mode == LockMode::EXCLUSIVE && !CheckAppropriateLockOnTable(txn, oid, LockMode::INTENTION_EXCLUSIVE) &&
  //           !CheckAppropriateLockOnTable(txn, oid, LockMode::EXCLUSIVE) &&
  //           !CheckAppropriateLockOnTable(txn, oid, LockMode::SHARED_INTENTION_EXCLUSIVE)) {
  //   txn->SetState(TransactionState::ABORTED);
  //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  // }
  

  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  // 检查此锁请求是否为一次锁升级(S->X)
  bool upgrade = false;
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->txn_id_ == txn->GetTransactionId()) {
      if (lr->lock_mode_ == lock_mode) {
        // 重复的锁
        return true;
      }
      if (lrq->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!CanLockUpgrade(lr->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      lrq->upgrading_ = txn->GetTransactionId();
      lrq->request_queue_.erase(iter);
      MapLockModeToTxnLockRemoveFunc(txn, lr->lock_mode_, oid);
      lrq->request_queue_.push_back(std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
      upgrade = true;
      break;
    }
  }

  if (!upgrade) {
    lrq->request_queue_.push_back(std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
  }

  while (!CanTxnTakeLock(txn, lock_mode, lrq)) {
    lrq->cv_.wait(lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    lrq->cv_.notify_all();
    return false;
  }

  MapLockModeToTxnRowLockSetFunc(txn, lock_mode, oid, rid);

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
    row_lock_map_latch_.lock();
  auto lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      if (!force) {
        auto iso_level = txn->GetIsolationLevel();
        if (iso_level == IsolationLevel::REPEATABLE_READ) {
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
        } else if (iso_level == IsolationLevel::READ_COMMITTED) {
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
        } else if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }

      MapLockModeToTxnRowLockRemoveFunc(txn, lr->lock_mode_, oid, rid);

      lrq->request_queue_.erase(iter);

      lrq->cv_.notify_all();

      return true;
    }
  }

  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);

  return false;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (it != waits_for_[t1].end()) {
    waits_for_[t1].push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (it != waits_for_[t1].end()) {
    waits_for_[t1].erase(it);
  }
}

auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  on_path.insert(source_txn);
  // const auto v = waits_for_.find(source_txn);
  // if (v == waits_for_.end()) {
  //   return false;
  // }
  if (*abort_txn_id < source_txn) {
    *abort_txn_id = source_txn;
  }
  
  for (const auto &v : waits_for_[source_txn]) {
    if (visited.count(v) != 1 && on_path.count(v) != 1) {
      if (FindCycle(v, path, visited, on_path, abort_txn_id)) {
        return true;
      }
      on_path.erase(v);
    } else if (visited.count(v) == 1 && on_path.count(v) == 1) {
      return true;
    }
  }

  visited.insert(source_txn);

  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { 

  std::vector<txn_id_t> path;
  std::unordered_set<txn_id_t> on_path;
  std::unordered_set<txn_id_t> visited;

  for (const auto &p : waits_for_) {
    if (visited.count(p.first) == 0 && on_path.count(p.first) == 0 && FindCycle(p.first, path, on_path, visited, txn_id)) {
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[k, v] : waits_for_) {
    for (const auto &x : v) {
      edges.emplace_back(k, x);
    }
  }
  return edges;
}

void LockManager::BuildGraph() {
  table_lock_map_latch_.lock();
  for (auto &[k, v] : table_lock_map_) {
    v->latch_.lock();
    for (auto iter1 = v->request_queue_.begin(); iter1 != v->request_queue_.end(); ++iter1) {
      for (auto iter2 = v->request_queue_.begin(); iter2 != v->request_queue_.end(); ++iter2) {
        auto lr1 = *iter1;
        auto lr2 = *iter2;
        if (txn_manager_->GetTransaction(lr1->txn_id_)->GetState() != TransactionState::ABORTED &&
            txn_manager_->GetTransaction(lr2->txn_id_)->GetState() != TransactionState::ABORTED && !lr1->granted_ &&
            lr2->granted_ && !AreLocksCompatible(lr1->lock_mode_, lr2->lock_mode_)) {
          AddEdge(lr1->txn_id_, lr2->txn_id_);
        }
      }
    }
    v->latch_.unlock();
  }
  table_lock_map_latch_.unlock();

  row_lock_map_latch_.lock();
  for (auto &[k, v] : row_lock_map_) {
    v->latch_.lock();
    for (auto iter1 = v->request_queue_.begin(); iter1 != v->request_queue_.end(); iter1++) {
      for (auto iter2 = v->request_queue_.begin(); iter2 != v->request_queue_.end(); iter2++) {
        auto lr1 = *iter1;
        auto lr2 = *iter2;
        if (!lr1->granted_ && lr2->granted_ && !AreLocksCompatible(lr1->lock_mode_, lr2->lock_mode_)) {
          AddEdge(lr1->txn_id_, lr2->txn_id_);
        }
      }
    }
    v->latch_.unlock();
  }
  row_lock_map_latch_.unlock();

  for (auto &[k, v] : waits_for_) {
    std::sort(v.begin(), v.end());
  }
}

void LockManager::RemoveAllAboutAbortTxn(txn_id_t abort_id) {
  table_lock_map_latch_.lock();
  for (auto &[k, v] : table_lock_map_) {
    v->latch_.lock();
    for (auto it = v->request_queue_.begin(); it != v->request_queue_.end(); ) {
      auto lr = *it;
      if (lr->txn_id_ == abort_id) {
        v->request_queue_.erase(it++);
        if (lr->granted_) {
          MapLockModeToTxnLockRemoveFunc(txn_manager_->GetTransaction(abort_id), lr->lock_mode_, lr->oid_);
          v->cv_.notify_all();
        }
      } else {
        ++it;
      }
    }
    v->latch_.unlock();
  }
  table_lock_map_latch_.unlock();

  row_lock_map_latch_.lock();
  for (auto &[k, v] : row_lock_map_) {
    v->latch_.lock();

    for (auto it = v->request_queue_.begin(); it != v->request_queue_.end();) {
      auto lr = *it;
      if (lr->txn_id_ == abort_id) {
        v->request_queue_.erase(it++);
        if (lr->granted_) {
          MapLockModeToTxnRowLockRemoveFunc(txn_manager_->GetTransaction(abort_id), lr->lock_mode_, lr->oid_, lr->rid_);
          v->cv_.notify_all();
        }
      } else {
        ++it;
      }
    }
    v->latch_.unlock();
  }
  row_lock_map_latch_.unlock();
  // 删除出度边
  waits_for_.erase(abort_id);
  // 删除入度边
  for (auto iter = waits_for_.begin(); iter != waits_for_.end();) {
    RemoveEdge((*iter).first, abort_id);
    
    if ((*iter).second.empty()) {
      waits_for_.erase(iter++);
    } else {
      ++iter;
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock

    }
  }
}

}  // namespace bustub
