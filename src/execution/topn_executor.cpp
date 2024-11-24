#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

// 初始化就排序好
void TopNExecutor::Init() {
  // throw NotImplementedException("TopNExecutor is not implemented");
  child_executor_->Init();
  tuples_.clear();
  // 定义比较器
  auto compare = [this](const Tuple tpl1, const Tuple tpl2) -> bool {
    for (auto &[type, expr] : this->plan_->GetOrderBy()) {
      auto left_value = expr->Evaluate(&tpl1, this->child_executor_->GetOutputSchema());
      auto right_value = expr->Evaluate(&tpl2, this->child_executor_->GetOutputSchema());
      if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {
        return type != OrderByType::DESC;
      }
      if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {
        return type == OrderByType::DESC;
      }
    }
    return true;
  };

  // 优先队列(堆)存数据
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(compare)> pq(compare);
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    pq.emplace(tuple);
    if (pq.size() > plan_->n_) {
      pq.pop();
    }
  }
  while (!pq.empty()) {
    tuples_.push_back(pq.top());
    pq.pop();
  }
  tuples_it_ = tuples_.rbegin();  // 反向遍历
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (tuples_it_ != tuples_.rend()) {
    *rid = tuples_it_->GetRid();
    *tuple = *tuples_it_;
    ++tuples_it_;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  // throw NotImplementedException("TopNExecutor is not implemented");
  return std::distance(tuples_it_, tuples_.rend());
};

}  // namespace bustub
