//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tbl_info_(exec_ctx->GetCatalog()->GetTable(plan_->table_oid_)),
      tbl_it_(std::make_unique<TableIterator>(tbl_info_->table_->MakeIterator())) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
#ifdef PRO_4_TXN
  LockManager::LockMode lock_mode = LockManager::LockMode::INTENTION_SHARED;
  switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
    case IsolationLevel::READ_COMMITTED: {
      // 禁止事务降锁
      // 给提交前给表上锁
      if (!(exec_ctx_->GetTransaction()->IsTableExclusiveLocked(tbl_info_->oid_) ||
            exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(tbl_info_->oid_) ||
            exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(tbl_info_->oid_) ||
            exec_ctx_->GetTransaction()->IsTableSharedLocked(tbl_info_->oid_))) {
        exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_);
      }
    } break;
    case IsolationLevel::READ_UNCOMMITTED:
      break;
  }
#endif
  tbl_it_ = std::make_unique<TableIterator>(tbl_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
#ifdef PRO_4_TXN
  while (!tbl_it_->IsEnd()) {
    *rid = tbl_it_->GetRID();
    LockManager::LockMode lock_mode = LockManager::LockMode::SHARED;
    switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
      case IsolationLevel::REPEATABLE_READ:
      case IsolationLevel::READ_COMMITTED: {
        // 给行上锁
        if (!exec_ctx_->GetTransaction()->IsRowExclusiveLocked(tbl_info_->oid_, *rid)) {
          exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_, *rid);
        }
      } break;
      case IsolationLevel::READ_UNCOMMITTED:
        break;
    }
    /* 2.
     * 针对delete的处理
     * 在此加锁，delete算子就不用加锁了
     */
    if (exec_ctx_->IsDelete()) {
      try {
        exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                               tbl_info_->oid_);
        exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                             tbl_info_->oid_, *rid);
      } catch (TransactionAbortException &e) {
        throw ExecutionException(e.GetInfo());
      }
    }
    // 3.check tuple
    auto [meta, new_tuple] = tbl_it_->GetTuple();
    ++(*tbl_it_);
    // 3.1 没有被删除的行的处理
    if (!meta.is_deleted_) {
      if (plan_->filter_predicate_ != nullptr) {  // filter下推到seq_scan的处理
        auto value = plan_->filter_predicate_->Evaluate(&new_tuple, GetOutputSchema());
        if (value.IsNull() || !value.GetAs<bool>()) {
          // 不符合条件给行强制解锁
          // 这样就能提前给不需要的行解锁，filter下推seq的优势
          try {
            exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), tbl_info_->oid_, *rid, true);
          } catch (TransactionAbortException &e) {
            throw ExecutionException(e.GetInfo());
          }
          continue;
        }
      }
      if (!exec_ctx_->IsDelete()) {
        switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
          case IsolationLevel::REPEATABLE_READ:
            break;
          // 提交的话给行解锁
          case IsolationLevel::READ_COMMITTED:
            exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), tbl_info_->oid_, *rid, false);
            break;
          case IsolationLevel::READ_UNCOMMITTED:
            break;
        }
      }
      *tuple = new_tuple;
      return true;
    }
    // 3.2 已经被删除的行的处理
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), tbl_info_->oid_, *rid, true);
    }
  }
  return false;

#else
  while (!tbl_it_->IsEnd()) {
    *rid = tbl_it_->GetRID();
    auto [meta, new_tuple] = tbl_it_->GetTuple();
    ++(*tbl_it_);
    if (!meta.is_deleted_) {
      if (plan_->filter_predicate_ != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(&new_tuple, GetOutputSchema());
        if (value.IsNull() || !value.GetAs<bool>()) {
          continue;
        }
      }
      *tuple = new_tuple;
      return true;
    }
  }
  return false;
#endif
}

}  // namespace bustub
