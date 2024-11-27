//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "common/config.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      tbl_info_(exec_ctx->GetCatalog()->GetTable(plan_->table_oid_)),
      tbl_indexes_(exec_ctx->GetCatalog()->GetTableIndexes(tbl_info_->name_)) {}

void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
#ifdef PRO_4_TXN
  LockManager::LockMode lock_mode = LockManager::LockMode::INTENTION_EXCLUSIVE;
  try {
    exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, tbl_info_->oid_);
  } catch (TransactionAbortException &e) {
    throw ExecutionException(e.GetInfo());
  }
#endif
  child_executor_->Init();
  done_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
  int32_t insert_rows{0};
  while (child_executor_->Next(tuple, rid)) {
    // 在insert_tuple里给row上锁
    auto new_tuple_rid = tbl_info_->table_->InsertTuple(meta, *tuple, exec_ctx_->GetLockManager(),
                                                        exec_ctx_->GetTransaction(), tbl_info_->oid_);
    if (!new_tuple_rid.has_value()) {
      continue;
    }
    *rid = new_tuple_rid.value();
    // prj4.记录操作
    auto tbl_write_record = TableWriteRecord(tbl_info_->oid_, *rid, tbl_info_->table_.get());
    tbl_write_record.wtype_ = WType::INSERT;
    // 将记录插入表的写记录队列
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(tbl_write_record);
    // 更新该表对应的索引信息 (索引对应的结构为b+树)
    for (auto &index : tbl_indexes_) {
      auto key = tuple->KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, new_tuple_rid.value(), exec_ctx_->GetTransaction());
      // 将记录插入表的索引的写记录队列
      // 索引操作的记录就记录它的 key-value (index_key, rid)
      auto index_write_record = IndexWriteRecord(*rid, tbl_info_->oid_, WType::INSERT, *tuple, index->index_oid_, exec_ctx_->GetCatalog());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(index_write_record);
    }
    ++insert_rows;
  }
  *tuple = Tuple({Value(INTEGER, insert_rows)}, &GetOutputSchema());
  done_ = true;
  return true;
}
}  // namespace bustub
