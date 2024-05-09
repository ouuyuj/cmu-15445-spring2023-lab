#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  tuples_.clear();
  it_ = tuples_.crend();
  heap_size_ = 0;

  auto cmp = [&](const Tuple &x, const Tuple &y) -> bool {
    CmpBool s = CmpBool::CmpNull;
    Value l;
    Value r;
    for (const auto &order_by : plan_->GetOrderBy()) {
      l = order_by.second->Evaluate(&x, GetOutputSchema());
      r = order_by.second->Evaluate(&y, GetOutputSchema());

      if (l.CompareEquals(r) == CmpBool::CmpTrue) {
        continue;
      }

      switch (order_by.first) {
        case OrderByType::ASC:
        case OrderByType::DEFAULT: {
          s = l.CompareLessThan(r);
          break;
        }

        case OrderByType::DESC: {
          s = l.CompareGreaterThan(r);
          break;
        }

        default:
          return false;
      }
      return (s == CmpBool::CmpTrue);
    }
    return false;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq{cmp};

  while (child_executor_->Next(&tuple, &rid)) {
    pq.push(tuple);
    ++heap_size_;
    if (plan_->GetN() < pq.size()) {
      pq.pop();
      --heap_size_;
    }
  }

  while (!pq.empty()) {
    tuples_.emplace_back(pq.top());
    pq.pop();
  }

  it_ = tuples_.crbegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ != tuples_.crend()) {
    *tuple = *it_;
    *rid = tuple->GetRid();
    ++it_;
    return true;
  }

  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_size_; };

}  // namespace bustub
