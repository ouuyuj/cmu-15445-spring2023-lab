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

        default: { }
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
    ++it_;
    return true;
  }

  return false;
}

}

/*
#include <tuple>
#include <algorithm>

// 自定义比较函数，使用折叠表达式处理不定长元组的比较
template<typename... Ts>
bool tuple_less(const std::tuple<Ts...> &t1, const std::tuple<Ts...> &t2) {
    return std::apply([](const auto&... args1) {
        return std::apply([&](const auto&... args2) {
            return ((args1 < args2) && ...);
        }, t2);
    }, t1);
}

int main() {
    std::tuple<int, float, std::string> tuple1{1, 3.14f, "hello"};
    std::tuple<int, float, std::string> tuple2{2, 2.71f, "world"};

    bool result = tuple_less(tuple1, tuple2);

    return 0;
}
*/