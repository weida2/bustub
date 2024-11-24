#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  // 将sort和limit算子转换为topn算子
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto opt_plan = plan->CloneWithChildren(std::move(children));
  // 如果该结点为 limit 且子结点为sort
  if (opt_plan->GetType() == PlanType::Limit && opt_plan->GetChildren()[0]->GetType() == PlanType::Sort) {
    auto limit_plan_node = dynamic_cast<LimitPlanNode *>(opt_plan.get());
    auto sort_plan_node = dynamic_cast<const SortPlanNode *>(opt_plan->GetChildren()[0].get());
    return std::make_shared<TopNPlanNode>(opt_plan->output_schema_, sort_plan_node->GetChildren()[0],
                                          sort_plan_node->order_bys_, limit_plan_node->limit_);
  }
  return opt_plan;
}

}  // namespace bustub
