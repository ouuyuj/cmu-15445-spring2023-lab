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
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  RID rid;
  left_executor_->Init();
  right_executor_->Init();
  left_status_ = left_executor_->Next(&l_tuple_, &rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple r_tuple{};
  RID l_rid{}, r_rid{};

  while (true) {
    if (left_status_) {
      while (true) {
        if (right_executor_->Next(&r_tuple, &r_rid)) {
          Value join_status = plan_->predicate_->EvaluateJoin(&l_tuple_, left_executor_->GetOutputSchema(), &r_tuple,
                                                              right_executor_->GetOutputSchema());

          if (!join_status.IsNull() && join_status.GetAs<bool>()) {
            std::vector<Value> values;
            values.reserve(GetOutputSchema().GetColumnCount());
            for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
              values.push_back(l_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
            }

            for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
              values.push_back(r_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
            }

            *tuple = Tuple(values, &GetOutputSchema());
            left_join_ = true;

            return true;
          }
        } else {
          if (plan_->GetJoinType() == JoinType::LEFT && !left_join_) {
            std::vector<Value> values;
            values.reserve(GetOutputSchema().GetColumnCount());
            for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
              values.push_back(l_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
            }

            for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
              values.push_back(
                  ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
            }

            *tuple = Tuple(values, &GetOutputSchema());
            left_join_ = false;

            right_executor_->Init();
            left_status_ = left_executor_->Next(&l_tuple_, &l_rid);

            return true;
          }

          right_executor_->Init();
          left_status_ = left_executor_->Next(&l_tuple_, &l_rid);
          left_join_ = false;

          break;
        }
      }
    } else {
      return false;
    }
  }

  return false;
}

}  // namespace bustub
