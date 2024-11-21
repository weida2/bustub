//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), 
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      predicate_(plan_->Predicate()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented");
  left_executor_->Init();
  right_executor_->Init();
  match_ = false;
  outer_finish_ = true;
}

// 找到一条记录tuple返回
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (outer_finish_) {
      // 实现simple nlj, 即每一条tuple,遍历一遍右边的表
      if (!left_executor_->Next(tuple, rid)) {
        return false;
      }
      outer_finish_ = false;
      left_tuple_ = *tuple;
      right_executor_->Init();
    }
    // 开始进行右表匹配
    // NestedLoopJoin { type=Inner, predicate=((#0.0=#1.0)and(#0.1=#1.2)) } 
    // SeqScan { table=test_1 }                                           
    // SeqScan { table=test_2 }
    while (right_executor_->Next(tuple, rid)) {
      Tuple right_tuple = *tuple;
      auto value = predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                            right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values{};
        values.reserve(GetOutputSchema().GetColumns().size());
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumns().size(); i++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumns().size(); i++) {
          values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        match_ = true;
        return true;
      }
    }
    if (!match_ && plan_->join_type_ == JoinType::LEFT) {
      std::vector<Value> values{};
      values.reserve(GetOutputSchema().GetColumns().size());
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumns().size(); i++) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumns().size(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      match_ = false;
      outer_finish_ = true;
      return true;        
    }
    // 到这说明这一次outer的tuple和inner的所有tuple匹配完了
    match_ = false;
    outer_finish_ = true;
  }
  // return false;
}

}  // namespace bustub
