//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      tbl_info_(exec_ctx->GetCatalog()->GetTable(plan_->table_oid_)),
      tbl_indexes_(exec_ctx->GetCatalog()->GetTableIndexes(tbl_info_->name_)) {}
    
void UpdateExecutor::Init() {
  // throw NotImplementedException("UpdateExecutor is not implemented");
  child_executor_->Init();
  done_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  uint32_t update_rows{0};
  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
  int64_t first_rid;
  bool first_st{false};
  while (child_executor_->Next(tuple, rid)) {  // 这里选出的就是符合条件的要更改的row?
    // 用于判断第一个次被删除的tuple重新插入后再次遍历到的情况
    if (first_st && first_rid == rid->Get()) {  // 这里rid.get = [高32bits=page_id|低32bits=slot_num]
      break;
    }
    // delete
    meta.is_deleted_ = true;
    tbl_info_->table_->UpdateTupleMeta(meta, *rid);  // 更新该tuple元数据,说明其无效了,不是原地更新
    // 更新索引信息
    for (auto &index : tbl_indexes_) {
      auto key = tuple->KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    // 更新要准备插入的rows tuple
    std::vector<Value> values{};
    // 这里的筛选符合条件的rows?还是说符合条件的已经在tuple里了
    // 然后做更新是expr里对应的列的值做更新
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    Tuple update_tuple = Tuple(values, &child_executor_->GetOutputSchema());
    // insert
    meta.is_deleted_ = false;
    auto insert_tuple_id = tbl_info_->table_->InsertTuple(meta, update_tuple);
    if (!insert_tuple_id.has_value()) {
      continue;
    }
    for (auto index : tbl_indexes_) {
      auto key = update_tuple.KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, insert_tuple_id.value(), exec_ctx_->GetTransaction());
    }
    ++update_rows;
    if (!first_st) {
      first_rid = insert_tuple_id->Get();
      first_st = true;
    }
  }
  done_ = true;
  *tuple = Tuple({Value(INTEGER, update_rows)}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
