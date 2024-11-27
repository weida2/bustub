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
#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 1.根据隔离级别判断判断获取锁的类型是否合理
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        ThrowAbort(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING &&
          !(lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED)) {
        ThrowAbort(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      // required 锁的类型判断
      if (!(lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE)) {
        ThrowAbort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      // allowed_state 获取锁的时机判断
      if (txn->GetState() != TransactionState::GROWING) {
        ThrowAbort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      break;
    default:
      break;
  }
  // 2.LOCK Note
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> ti_lock_request_queue = table_lock_map_[oid];  // 当前表的锁请求队列
  table_lock_map_latch_.unlock();

  std::unique_lock lock(ti_lock_request_queue->latch_);  // cv_wait的锁

  for (auto it = ti_lock_request_queue->request_queue_.begin(); it != ti_lock_request_queue->request_queue_.end();
       it++) {
    std::shared_ptr<LockRequest> cur_lock_request = *it;
    // 2.1 Txn LOCK UPGRADE:
    // 事务更新在这个表上的锁等级
    if (cur_lock_request->txn_id_ == txn->GetTransactionId()) {
      // 同样的表加锁模式
      if (cur_lock_request->lock_mode_ == lock_mode) {
        return true;
      }
      if (ti_lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        ThrowAbort(txn, AbortReason::UPGRADE_CONFLICT);  // 已经有其他事务在等待更新该表的锁级别
                                                         // 同一时间只能有一个更新
      }
      // 1.check the precondition of update
      LockMode old_lock_mode = cur_lock_request->lock_mode_;
      if (!((old_lock_mode == LockMode::INTENTION_SHARED) ||
            (old_lock_mode == LockMode::SHARED &&
             (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) ||
            (old_lock_mode == LockMode::INTENTION_EXCLUSIVE &&
             (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) ||
            (old_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode == LockMode::EXCLUSIVE))) {
        ThrowAbort(txn, AbortReason::INCOMPATIBLE_UPGRADE);  // 不合适的更新
      }

      // 2.Drop the current lock, reserve the upgrade position
      ti_lock_request_queue->request_queue_.erase(it);
      DeleteTxnLockTable(txn, old_lock_mode, oid);

      auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      ti_lock_request_queue->request_queue_.emplace_back(new_lock_request);
      ti_lock_request_queue->upgrading_ = txn->GetTransactionId();

      // 3.wait to get the new lock granted
      // 如果没有被允许获取锁会阻塞事务
      while (!GrantAllowed(txn, ti_lock_request_queue, lock_mode)) {
        ti_lock_request_queue->cv_.wait(lock);  // 等待其他事务线程更新该表的锁队列的cv变量
                                                // 就是其他事务线程拥有该表的锁，等待它释放
        if (txn->GetState() == TransactionState::ABORTED) {
          ti_lock_request_queue->request_queue_.remove(new_lock_request);
          ti_lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock.unlock();
          ti_lock_request_queue->cv_.notify_all();  // 通知其他等待该cv变量的事务线程
          return false;
        }
      }
      new_lock_request->granted_ = true;  // 这个事务获得该表的锁
      ti_lock_request_queue->upgrading_ = INVALID_TXN_ID;
      InsertTxnLockTable(txn, lock_mode, oid);
      return true;
    }
  }

  // 2.2 这个事务第一次在这个表上获取锁请求
  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  ti_lock_request_queue->request_queue_.emplace_back(new_lock_request);  // 这里存储的share_ptr实例
                                                                         // new_lock_re指向相同的 LockRequest 对象。
  while (!GrantAllowed(txn, ti_lock_request_queue, lock_mode)) {
    ti_lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      ti_lock_request_queue->request_queue_.remove(new_lock_request);
      lock.unlock();
      ti_lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  // 在外面修改, 所有指向同一个 LockRequest 对象的 shared_ptr 都会看到这个状态的变化。
  new_lock_request->granted_ = true;
  // 3.[BOOK KEEPING]
  InsertTxnLockTable(txn, lock_mode, oid);  // 修改事务的表锁信息

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    ThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  std::shared_ptr<LockRequestQueue> ti_lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock lock(ti_lock_request_queue->latch_);
  // 1.找到这个事务在这个表加的锁
  auto it = ti_lock_request_queue->request_queue_.begin();
  for (; it != ti_lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
      break;
    }
  }
  if (it == ti_lock_request_queue->request_queue_.end()) {
    ThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  // 2.释放表锁前确保没拥有任何行锁
  auto s_row_set = txn->GetSharedRowLockSet();
  auto x_row_set = txn->GetExclusiveRowLockSet();
  if (!((s_row_set->find(oid) == s_row_set->end() || s_row_set->at(oid).empty()) &&
        (x_row_set->find(oid) == x_row_set->end() || x_row_set->at(oid).empty()))) {
    ThrowAbort(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  // 3.更新事务状态，根据隔离级别
  switch ((*it)->lock_mode_) {
    case LockMode::SHARED: {
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        txn->SetState(TransactionState::SHRINKING);
      }
    } break;
    case LockMode::EXCLUSIVE: {
      txn->SetState(TransactionState::SHRINKING);
    } break;
    default:
      break;
  }
  // 4.[Book KEEPING]
  DeleteTxnLockTable(txn, (*it)->lock_mode_, oid);
  ti_lock_request_queue->request_queue_.erase(it);
  lock.unlock();
  ti_lock_request_queue->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 1.判断锁类型是否合理
  if (!(lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE)) {
    ThrowAbort(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // 2.加锁前判断是否加了合适的表锁
  if (lock_mode == LockMode::SHARED) {
    if (!(txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid) ||
          txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableSharedLocked(oid) ||
          txn->IsTableIntentionSharedLocked(oid))) {
      ThrowAbort(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!(txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid) ||
          txn->IsTableIntentionExclusiveLocked(oid))) {
      ThrowAbort(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  // 3.根据隔离级别判断判断获取锁的类型是否合理
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        ThrowAbort(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING &&
          !(lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED)) {
        ThrowAbort(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      // required 锁的类型判断
      if (!(lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE)) {
        ThrowAbort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      // allowed_state 获取锁的时机判断
      if (txn->GetState() != TransactionState::GROWING) {
        ThrowAbort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      break;
    default:
      break;
  }

  // 4.LOCK Note
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> ri_lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();

  std::unique_lock lock(ri_lock_request_queue->latch_);

  for (auto it = ri_lock_request_queue->request_queue_.begin(); it != ri_lock_request_queue->request_queue_.end();
       it++) {
    std::shared_ptr<LockRequest> cur_lock_request = *it;
    // 4.1 Txn LOCK UPGRADE:
    // 事务更新在这个行上的锁等级
    if (cur_lock_request->txn_id_ == txn->GetTransactionId()) {
      if (cur_lock_request->lock_mode_ == lock_mode) {
        return true;
      }
      if (ri_lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        ThrowAbort(txn, AbortReason::UPGRADE_CONFLICT);  // 已经有其他事务在等待更新该行的锁级别
                                                         // 同一时间只能有一个更新
      }
      // 1.check the precondition of update
      LockMode old_lock_mode = cur_lock_request->lock_mode_;
      if (!((old_lock_mode == LockMode::INTENTION_SHARED) ||
            (old_lock_mode == LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED &&
             lock_mode != LockMode::INTENTION_EXCLUSIVE) ||
            (old_lock_mode == LockMode::INTENTION_EXCLUSIVE &&
             (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE)) ||
            (old_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode == LockMode::EXCLUSIVE))) {
        ThrowAbort(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }

      // 2.Drop the current lock, reserve the upgrade position
      ri_lock_request_queue->request_queue_.erase(it);
      DeleteTxnLockRow(txn, old_lock_mode, oid, rid);

      auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      ri_lock_request_queue->request_queue_.emplace_back(new_lock_request);
      ri_lock_request_queue->upgrading_ = txn->GetTransactionId();

      // 3.wait to get the new lock granted
      // 如果没有被允许获取锁会阻塞事务
      while (!GrantAllowed(txn, ri_lock_request_queue, lock_mode)) {
        ri_lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          ri_lock_request_queue->request_queue_.remove(new_lock_request);
          ri_lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock.unlock();
          ri_lock_request_queue->cv_.notify_all();  // 通知其他等待该cv变量的事务线程
          return false;
        }
      }
      new_lock_request->granted_ = true;  // 这个事务获得该行的锁
      ri_lock_request_queue->upgrading_ = INVALID_TXN_ID;
      InsertTxnLockRow(txn, lock_mode, oid, rid);
      return true;
    }
  }

  // 2.2 这个事务第一次在这个表上获取锁请求
  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  ri_lock_request_queue->request_queue_.emplace_back(new_lock_request);
  while (!GrantAllowed(txn, ri_lock_request_queue, lock_mode)) {
    ri_lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      ri_lock_request_queue->request_queue_.remove(new_lock_request);
      lock.unlock();
      ri_lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  new_lock_request->granted_ = true;
  // 3.[BOOK KEEPING]
  InsertTxnLockRow(txn, lock_mode, oid, rid);  // 修改事务的行锁信息

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    ThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  std::shared_ptr<LockRequestQueue> ri_lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock lock(ri_lock_request_queue->latch_);
  // 1.找到这个事务在这个表加的锁
  auto it = ri_lock_request_queue->request_queue_.begin();
  for (; it != ri_lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
      break;
    }
  }
  if (it == ri_lock_request_queue->request_queue_.end()) {
    ThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 2.更新事务状态，根据隔离级别
  if (!force) {
    switch ((*it)->lock_mode_) {
      case LockMode::SHARED: {
        if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } break;
      case LockMode::EXCLUSIVE: {
        txn->SetState(TransactionState::SHRINKING);
      } break;
      default:
        break;
    }
  }

  // 3.[Book KEEPING]
  DeleteTxnLockRow(txn, (*it)->lock_mode_, oid, rid);
  ri_lock_request_queue->request_queue_.erase(it);
  lock.unlock();
  ri_lock_request_queue->cv_.notify_all();

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::ThrowAbort(Transaction *txn, AbortReason abort_reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
}

// 删除事务的五种表锁
void LockManager::DeleteTxnLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  txn->LockTxn();
  if (lock_mode == LockMode::INTENTION_SHARED) {  // IS
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED) {  // S
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {  // IX
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {  // SIX
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  } else {  // X
    txn->GetExclusiveTableLockSet()->erase(oid);
  }
  txn->UnlockTxn();
}

// 添加事务的五种表锁
void LockManager::InsertTxnLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  txn->LockTxn();
  if (lock_mode == LockMode::INTENTION_SHARED) {  // IS
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED) {  // S
    txn->GetSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {  // IX
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {  // SIX
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  } else {  // X
    txn->GetExclusiveTableLockSet()->insert(oid);
  }
  txn->UnlockTxn();
}

// 删除事务的两种行锁
void LockManager::DeleteTxnLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {  // S
    txn->GetSharedRowLockSet()->at(oid).erase(rid);
  } else {  // X
    txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
  }
  txn->UnlockTxn();
}

// 添加事务的两种行锁
void LockManager::InsertTxnLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {  // S
    if (txn->GetSharedRowLockSet()->find(oid) == txn->GetSharedRowLockSet()->end()) {
      txn->GetSharedRowLockSet()->emplace(oid, std::unordered_set<RID>());
    }
    txn->GetSharedRowLockSet()->at(oid).insert(rid);
  } else {  // X
    if (txn->GetExclusiveRowLockSet()->find(oid) == txn->GetExclusiveRowLockSet()->end()) {
      txn->GetExclusiveRowLockSet()->emplace(oid, std::unordered_set<RID>());
    }
    txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
  }
  txn->UnlockTxn();
}

// 判断锁是否兼容
// l1 already_grant lock_request [T1 holds]
// l2 want_grant lock_request [T2 Wants]
auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::INTENTION_SHARED: {
      if (l2 == LockMode::EXCLUSIVE) {
        return false;
      }
    } break;
    case LockMode::INTENTION_EXCLUSIVE: {
      if (l2 != LockMode::INTENTION_SHARED && l2 != LockMode::INTENTION_EXCLUSIVE) {
        return false;
      }
    } break;
    case LockMode::SHARED: {
      if (l2 != LockMode::SHARED && l2 != LockMode::INTENTION_SHARED) {
        return false;
      }
    } break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      if (l2 != LockMode::INTENTION_SHARED) {
        return false;
      }
    } break;
    default:
      return false;  // EXCLUSIVE
  }
  return true;
}

auto LockManager::GrantAllowed(Transaction *txn, const std::shared_ptr<LockRequestQueue> &lock_request_queue,
                               LockMode lock_mode) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return false;
  }
  for (auto const &grant_lock_request : lock_request_queue->request_queue_) {
    if (grant_lock_request->granted_) {
      // 判断当前事务想要获得的锁和先前的事务已经获得的锁是否兼容
      /* 锁兼容矩阵
       *     | IS | IX | S | SIX | X
       *  IS | ✔  | ✔ | ✔ |  ✔ | x
       *  IX | ✔  | ✔ | x |  x  | x
       *   S | ✔  | x  | ✔|  x  | x
       * SIX | ✔  | x  | x |  x  | x
       *   X | x   | x  | x |  x  | x
       *
       */
      if (!AreLocksCompatible(grant_lock_request->lock_mode_, lock_mode)) {
        return false;
      }
      continue;
    }
  }
  // 如果是更新，只允许当前事务在在做更新
  // 且更新的话能更优先获取锁
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    return true;
  }
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    return false;
  }

  // 如果是第一次插入, 如果它在前面，就给它获取，不然就正常排队 FIFO方式分配锁
  for (auto const &yet_grant_lock_request : lock_request_queue->request_queue_) {
    if (yet_grant_lock_request->txn_id_ == txn->GetTransactionId()) {
      return true;
    }
    // 在它前面有其他没获得锁的事务在等待且不兼容
    if (!yet_grant_lock_request->granted_ && !AreLocksCompatible(yet_grant_lock_request->lock_mode_, lock_mode)) {
      return false;
    }
  }
  // 不会到这
  return false;
}

/*
 * task_2
 *
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_[t1] = std::vector<txn_id_t>();
  }
  auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (it == waits_for_[t1].end()) {
    waits_for_[t1].push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) != waits_for_.end()) {
    auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
    if (it != waits_for_[t1].end()) {
      waits_for_[t1].erase(it);
    }
  }
}

auto LockManager::Dfs(std::vector<txn_id_t> visited, std::vector<txn_id_t> rely_tids, txn_id_t *abort_txn_id) -> bool {
  // if (rely_tids.empty()) return false;
  std::sort(rely_tids.begin(), rely_tids.end());
  for (const auto &tid : rely_tids) {
    // 即有环
    if (std::find(visited.begin(), visited.end(), tid) != visited.end()) {
      *abort_txn_id = tid;
      for (auto yougest_tid : visited) {
        *abort_txn_id = std::max(*abort_txn_id, yougest_tid);  // 最年轻的tid,即后来到来的
      }
      return true;
    }
    visited.push_back(tid);
    const auto &this_rely_tids = waits_for_[tid];
    if (Dfs(visited, this_rely_tids, abort_txn_id)) {
      return true;
    }
    visited.pop_back();
  }
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  waits_for_latch_.lock();
  std::vector<txn_id_t> visited{};  // 标记数组
  std::vector<txn_id_t> rely_tids{};
  for (const auto &pair : waits_for_) {
    txn_id_t st = pair.first;
    if (!pair.second.empty()) {
      rely_tids.push_back(st);
    }
  }
  if (Dfs(visited, rely_tids, txn_id)) {
    waits_for_latch_.unlock();
    return true;
  }
  waits_for_latch_.unlock();
  return false;
}

// 死锁检测
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      waits_for_latch_.lock();
      waits_for_.clear();
      waits_for_latch_.unlock();

      // 1.处理表锁请求队列
      table_lock_map_latch_.lock();
      for (const auto &table : table_lock_map_) {
        auto ti_lock_request_queue = table.second;
        std::vector<txn_id_t> st_s{};
        std::vector<txn_id_t> ed_s{};
        ti_lock_request_queue->latch_.lock();
        for (const auto &lock_request_i : ti_lock_request_queue->request_queue_) {
          auto txn = txn_manager_->GetTransaction(lock_request_i->txn_id_);
          if (txn->GetState() != TransactionState::ABORTED) {
            if (!lock_request_i->granted_) {
              st_s.push_back(lock_request_i->txn_id_);
            } else {
              ed_s.push_back(lock_request_i->txn_id_);
            }
          }
        }
        ti_lock_request_queue->latch_.unlock();
        for (auto st : st_s) {
          for (auto ed : ed_s) {
            waits_for_latch_.lock();
            AddEdge(st, ed);
            waits_for_latch_.unlock();
          }
        }
      }
      table_lock_map_latch_.unlock();
      // 2.处理行锁请求队列
      row_lock_map_latch_.lock();
      for (const auto &row : row_lock_map_) {
        auto ri_lock_request_queue = row.second;
        std::vector<txn_id_t> st_s{};
        std::vector<txn_id_t> ed_s{};
        ri_lock_request_queue->latch_.lock();
        for (const auto &lock_request_i : ri_lock_request_queue->request_queue_) {
          auto txn = txn_manager_->GetTransaction(lock_request_i->txn_id_);
          if (txn->GetState() != TransactionState::ABORTED) {
            if (!lock_request_i->granted_) {
              st_s.push_back(lock_request_i->txn_id_);
            } else {
              ed_s.push_back(lock_request_i->txn_id_);
            }
          }
        }
        ri_lock_request_queue->latch_.unlock();
        for (auto st : st_s) {
          for (auto ed : ed_s) {
            // waits_for_latch_.lock();
            AddEdge(st, ed);
            // waits_for_latch_.unlock();
          }
        }
      }
      row_lock_map_latch_.unlock();
      // 3.死锁检查
      txn_id_t abort_txn_id{0};
      while (HasCycle(&abort_txn_id)) {
        waits_for_latch_.lock();
        auto txn = txn_manager_->GetTransaction(abort_txn_id);
        txn_manager_->Abort(txn);
        waits_for_.erase(abort_txn_id);
        for (auto [t1, _] : waits_for_) {
          RemoveEdge(t1, abort_txn_id);
        }
        waits_for_latch_.unlock();
      }
    }
  }
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &pair : waits_for_) {
    txn_id_t st = pair.first;
    for (auto ed : pair.second) {
      edges.emplace_back(std::make_pair(st, ed));
    }
  }
  return edges;
}

}  // namespace bustub
