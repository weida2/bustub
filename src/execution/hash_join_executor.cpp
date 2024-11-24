//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

// 和聚合一样,在初始化的时候构建哈希表
void HashJoinExecutor::Init() {
  // throw NotImplementedException("HashJoinExecutor is not implemented");
  left_executor_->Init();
  right_executor_->Init();
  jht_.clear();
  tuples_.clear();
  Tuple tuple{};
  RID rid{};
  // 1.构建哈希右表
  // HashJoin { type=Inner, left_key=[#0.0, #0.1], right_key=[#0.0, #0.2] }
  // SeqScan { table=test_1 }
  // SeqScan { table=test_2 }
  while (right_executor_->Next(&tuple, &rid)) {
    JoinHashKey r_keys{};
    for (auto &exp : plan_->right_key_expressions_) {
      r_keys.join_keys_.emplace_back(exp->Evaluate(&tuple, right_executor_->GetOutputSchema()));
    }
    JoinHashValue r_values{};
    uint32_t r_col_size = right_executor_->GetOutputSchema().GetColumns().size();
    r_values.join_values_.reserve(r_col_size);
    for (uint32_t i = 0; i < r_col_size; i++) {
      r_values.join_values_.emplace_back(tuple.GetValue(&right_executor_->GetOutputSchema(), i));
    }
    jht_.insert({r_keys, r_values});
  }
  // 2.左表Probe
  while (left_executor_->Next(&tuple, &rid)) {
    JoinHashKey l_keys{};
    for (auto &exp : plan_->left_key_expressions_) {
      l_keys.join_keys_.emplace_back(exp->Evaluate(&tuple, left_executor_->GetOutputSchema()));
    }
    if (jht_.count(l_keys) != 0) {
      auto range = jht_.equal_range(l_keys);  // 因为key值不一定唯一,有多个key需要都遍历加入其值
      for (auto it = range.first; it != range.second; ++it) {
        std::vector<Value> values{};
        uint32_t l_col_size = left_executor_->GetOutputSchema().GetColumns().size();
        values.reserve(GetOutputSchema().GetColumns().size());
        for (uint32_t i = 0; i < l_col_size; i++) {
          values.emplace_back(tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (auto const &value : it->second.join_values_) {
          values.emplace_back(value);
        }
        tuples_.emplace_back(Tuple{values, &GetOutputSchema()});
      }
    } else if (jht_.count(l_keys) == 0 && plan_->join_type_ == JoinType::LEFT) {
      std::vector<Value> values{};
      uint32_t l_col_size = left_executor_->GetOutputSchema().GetColumns().size();
      uint32_t r_col_size = right_executor_->GetOutputSchema().GetColumns().size();
      values.reserve(GetOutputSchema().GetColumns().size());
      for (uint32_t i = 0; i < l_col_size; i++) {
        values.emplace_back(tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < r_col_size; i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      tuples_.emplace_back(Tuple{values, &GetOutputSchema()});
    }
  }
  tuples_it_ = tuples_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (tuples_it_ != tuples_.end()) {
    *rid = tuples_it_->GetRid();
    *tuple = *tuples_it_;
    ++tuples_it_;
    return true;
  }
  return false;
}

}  // namespace bustub
