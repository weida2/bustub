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
  tbl_it_ = std::make_unique<TableIterator>(tbl_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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
}

}  // namespace bustub
