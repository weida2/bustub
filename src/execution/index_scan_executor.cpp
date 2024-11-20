//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan_->index_oid_)),
      tbl_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      it_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())->GetBeginIterator()) {}

void IndexScanExecutor::Init() {
  // throw NotImplementedException("IndexScanExecutor is not implemented");
  it_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!it_.IsEnd()) {
    *rid = (*it_).second;
    auto [meta, new_tuple] = tbl_info_->table_->GetTuple(*rid);
    ++it_;
    if (!meta.is_deleted_) {
      *tuple = new_tuple;
      return true;  
    }
  }
  return false;
}

}  // namespace bustub
