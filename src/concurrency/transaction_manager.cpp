//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  // 事务回滚
  // 1.表回滚
  while (!txn->GetWriteSet()->empty()) {
    auto record = txn->GetWriteSet()->back();
    switch (record.wtype_) {
      case WType::INSERT: {
        TupleMeta meta = record.table_heap_->GetTupleMeta(record.rid_);
        meta.is_deleted_ = true;  // 这样插入的数据就无效了
        record.table_heap_->UpdateTupleMeta(meta, record.rid_);
      } break;
      case WType::DELETE: {
        TupleMeta meta = record.table_heap_->GetTupleMeta(record.rid_);
        meta.is_deleted_ = false;  // 删除的数据重新有效
        record.table_heap_->UpdateTupleMeta(meta, record.rid_);
      } break;     
      default: {  // 更新回滚
        // TODO 原地更新
      } break;
    }
    txn->GetWriteSet()->pop_back();
  }
  // 2.索引回滚
  while (!txn->GetIndexWriteSet()->empty()) {
    auto record = txn->GetIndexWriteSet()->back();
    auto index_info = record.catalog_->GetIndex(record.index_oid_);
    auto table_info = record.catalog_->GetTable(record.table_oid_);
    auto key = record.tuple_.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    switch (record.wtype_) {
      case WType::INSERT: {
        // 插入的话就再删除索引
        index_info->index_->DeleteEntry(key, record.rid_, txn);
      } break;
      case WType::DELETE: {
        // 删除的话就再插入索引
        index_info->index_->InsertEntry(key, record.rid_, txn);
      } break;     
      default: {  // 更新回滚
        // TODO 原地更新
      } break;
    }
    txn->GetIndexWriteSet()->pop_back();
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
