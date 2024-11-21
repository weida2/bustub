//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    // 在初始化的为groupBy列建立聚合
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
  copy_with_empty = false;
}

// 之后就遍历聚合后的表
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 处理没有group和空表的情况
  if (aht_.Begin() == aht_.End() && !copy_with_empty && plan_->group_bys_.empty()) {
    *tuple = Tuple(aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema());  // return null
    copy_with_empty = true;
    return true;
  }
  while (aht_iterator_ != aht_.End()) {
    std::vector<Value> values{};
    for (auto &group_key : aht_iterator_.Key().group_bys_) {
      values.push_back(group_key);
    }
    for (auto &aggregate_value : aht_iterator_.Val().aggregates_) {
      values.push_back(aggregate_value);
    }
    *tuple = Tuple(values, &GetOutputSchema());
    // 哈希表的迭代器, 进入聚合表的下一行
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
