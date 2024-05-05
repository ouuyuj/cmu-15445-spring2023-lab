//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();

  Tuple o_tuple{};
  RID o_rid{};

  while (child_executor_->Next(&o_tuple, &o_rid)) {
    auto agg_key = MakeAggregateKey(&o_tuple);
    auto agg_val = MakeAggregateValue(&o_tuple);
    aht_.InsertCombine(agg_key, agg_val);
  }

  aht_iterator_ = aht_.Begin();
  is_empty_table_ = false;

  if (aht_iterator_ == aht_.End()) {
    is_empty_table_ = true;
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> agg_values;
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());

  if (aht_iterator_ != aht_.End()) {
    agg_values = aht_iterator_.Val().aggregates_;

    for (const auto &x : aht_iterator_.Key().group_bys_) {
      values.push_back(x);
    }

    for (const auto &x : agg_values) {
      values.push_back(x);
    }
    *tuple = Tuple(values, &GetOutputSchema());

    ++aht_iterator_;

    return true;
  }

  if (is_empty_table_ && !is_executed_) {
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }

    for (const auto &t : plan_->agg_types_) {
      if (t == AggregationType::CountStarAggregate) {
        values.push_back(ValueFactory::GetIntegerValue(0));
      } else {
        values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
    }

    *tuple = Tuple(values, &GetOutputSchema());
    is_executed_ = true;
    return true;
  }

  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
