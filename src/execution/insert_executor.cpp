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
    auto new_tuple_rid = tbl_info_->table_->InsertTuple(meta, *tuple);
    if (!new_tuple_rid.has_value()) {
      continue;
    }
    *rid = new_tuple_rid.value();
    // 更新索引信息
    for (auto &index : tbl_indexes_) {
      auto key = tuple->KeyFromTuple(tbl_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, new_tuple_rid.value(), exec_ctx_->GetTransaction());
    }
    ++insert_rows;
  }
  *tuple = Tuple({Value(INTEGER, insert_rows)}, &GetOutputSchema());
  done_ = true;
  return true;
}
}  // namespace bustub
