//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  const auto *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
  return;

  throw NotImplementedException("SeqScanExecutor is not implemented");
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_->IsEnd()) {
    return false;
  }

  const auto& tuple_meta = iter_->GetTuple();

  if (tuple_meta.first.is_deleted_) {
    ++(*iter_);
    return false;
  }
  
  *tuple = std::move(tuple_meta.second);
  *rid = iter_->GetRID();

  ++(*iter_);

  return true;
}

}  // namespace bustub
