#include <algorithm>

#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  output_tuples_.clear();

  while (child_executor_->Next(&tuple, &rid)) {
    output_tuples_.emplace_back(tuple);
  }

  std::sort(output_tuples_.begin(), output_tuples_.end(), [this](const auto &x, const auto &y) {
    CmpBool s;
    Value l;
    Value r;
    for (size_t i = 0; i < plan_->GetOrderBy().size(); i++) {
      l = plan_->GetOrderBy().at(i).second->Evaluate(&x, GetOutputSchema());
      r = plan_->GetOrderBy().at(i).second->Evaluate(&y, GetOutputSchema());

      if (l.CompareEquals(r) == CmpBool::CmpTrue) {
        continue;
      }

      switch (plan_->GetOrderBy().at(i).first) {
        case OrderByType::ASC:
        case OrderByType::DEFAULT: {
          s = l.CompareLessThan(r);
          break;
        }

        case OrderByType::DESC: {
          s = l.CompareGreaterThan(r);
          break;
        }

        default: {
        }
      }
      return s == CmpBool::CmpTrue;
    }
    return false;
  });

  it_ = output_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ != output_tuples_.end()) {
    *tuple = *it_;
    *rid = it_->GetRid();
    ++it_;
    return true;
  }

  return false;
}

}  // namespace bustub