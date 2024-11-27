//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      tbl_info_(exec_ctx->GetCatalog()->GetTable(plan_->table_oid_)),
      tbl_indexes_(exec_ctx->GetCatalog()->GetTableIndexes(tbl_info_->name_)) {}

// 删除算子执行器不需要获取锁，因为已经在seq扫描算子执行器获取了对应的表锁和行锁
// 这里只需记录操作
void DeleteExecutor::Init() {
  // throw NotImplementedException("DeleteExecutor is not implemented");
  child_executor_->Init();
  done_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int32_t delete_rows{0};
  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
  while (child_executor_->Next(tuple, rid)) {
    meta.is_deleted_ = true;
    tbl_info_->table_->UpdateTupleMeta(meta, *rid);
    auto tbl_write_record = TableWriteRecord(tbl_info_->oid_, *rid, tbl_info_->table_.get());
    tbl_write_record.wtype_ = WType::DELETE;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(tbl_write_record);
    for (auto &index : tbl_indexes_) {
      auto key = tuple->KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
      auto index_write_record = IndexWriteRecord(*rid, tbl_info_->oid_, WType::DELETE, *tuple, index->index_oid_, exec_ctx_->GetCatalog());
      exec_ctx_->GetTransaction()->AppendIndexWriteRecord(index_write_record);
    }
    ++delete_rows;
  }

  *tuple = Tuple({Value(INTEGER, delete_rows)}, &GetOutputSchema());
  done_ = true;
  return true;
}

}  // namespace bustub
