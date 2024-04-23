//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { 
  Catalog* catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  table_info_ = catalog->GetTable(index_info_->table_name_);
  auto* tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  iter_ = tree->GetBeginIterator();
  // iter_ = &iter;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_.IsEnd()) {
    return false;
  }

  auto pair = *iter_;
  *rid = pair.second;

  auto tuple_pair = table_info_->table_->GetTuple(*rid);

  while (tuple_pair.first.is_deleted_) {
    ++iter_;
    if (iter_.IsEnd()) {
      return false;
    }
    pair = *iter_;
    *rid = pair.second;

    tuple_pair = table_info_->table_->GetTuple(*rid);
  }

  *tuple = tuple_pair.second;

  ++iter_;
  
  return true;
}

}  // namespace bustub
