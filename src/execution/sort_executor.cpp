#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

// 先排序好
void SortExecutor::Init() {
  // throw NotImplementedException("SortExecutor is not implemented");
  child_executor_->Init();
  tuples_.clear();
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  // 排序自定义比较器
  std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &left_tuple, const Tuple &right_tuple) {
    for (auto &[type, exp] : this->plan_->GetOrderBy()) {
      auto left_value = exp->Evaluate(&left_tuple, this->child_executor_->GetOutputSchema());
      auto right_value = exp->Evaluate(&right_tuple, this->child_executor_->GetOutputSchema());
      if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {  // 如果小于就不换
        return type != OrderByType::DESC;                                 // 升序
      }
      if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {  // 如果大于就不换
        return type == OrderByType::DESC;                                    // 降序
      }
    }
    return true;
  });
  tuples_it_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (tuples_it_ != tuples_.end()) {
    *rid = tuples_it_->GetRid();
    *tuple = *tuples_it_;
    ++tuples_it_;
    return true;
  }
  return false;
}

}  // namespace bustub
