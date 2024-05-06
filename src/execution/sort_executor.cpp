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

  while (child_executor_->Next(&tuple, &rid)) {
    output_tuples_.emplace_back(tuple);    
  }

  for (const auto &order_by : plan_->GetOrderBy()) {
    switch (order_by.first) {
      case OrderByType::ASC: 
      case OrderByType::DEFAULT: {
        std::sort(output_tuples_.begin(), output_tuples_.end(), [&order_by, this](const auto &x, const auto &y) {
          
          auto s = order_by.second->Evaluate(&x, GetOutputSchema()).CompareLessThan(order_by.second->Evaluate(&y, GetOutputSchema()));
          return s == CmpBool::CmpTrue;
        });
      }
      break;

      case OrderByType::DESC: {
        std::sort(output_tuples_.begin(), output_tuples_.end(), [&order_by, this](const auto &x, const auto &y) {
          auto s = order_by.second->Evaluate(&x, GetOutputSchema()).CompareGreaterThan(order_by.second->Evaluate(&y, GetOutputSchema()));
          return s == CmpBool::CmpTrue;
        });
      }
      break;

      default: { }
    }

  }


  std::sort(output_tuples_.begin(), output_tuples_.end(), [this](const auto &x, const auto &y) {
    CmpBool s;
    for (size_t i = 0; i< plan_->GetOrderBy().size(); i++) {
      switch (plan_->GetOrderBy().at(i).first) {
        case OrderByType::ASC: 
        case OrderByType::DEFAULT: {
          s = plan_->GetOrderBy().at(i).second->
              Evaluate(&x, GetOutputSchema()).CompareLessThan(plan_->GetOrderBy().at(i).second->Evaluate(&y, GetOutputSchema()));
          break;
        }

        case OrderByType::DESC: {
          s = plan_->GetOrderBy().at(i).second->
              Evaluate(&x, GetOutputSchema()).CompareGreaterThan(plan_->GetOrderBy().at(i).second->Evaluate(&y, GetOutputSchema()));
          break;
        }

        default: { }
      }
    }
    return s == CmpBool::CmpTrue;
  });



  it_ = output_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ != output_tuples_.end()) {
    *tuple = *it_;
    ++it_;
    return true;
  }

  return false;
}

}  // namespace bustub

// namespace std {
//   template<>
//   struct less {

//   };
// }