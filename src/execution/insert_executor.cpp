//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
  child_executor_->Init();
  Catalog* catalog = exec_ctx_->GetCatalog();
  table_ = catalog->GetTable(plan_->GetTableOid())->table_.get();
  index_info_ = catalog->GetTableIndexes(catalog->GetTable(plan_->GetTableOid())->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
  Tuple child_tuple{};
  int64_t cnt = 0;

  while (child_executor_->Next(&child_tuple, nullptr)) {
    TupleMeta tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    try {
      table_->InsertTuple(tuple_meta, child_tuple);
      cnt++;
    } catch (...) {
      throw;
    }
  }
  std::vector<Value> inserted_cnt;
  inserted_cnt.emplace_back(Value(TypeId::INTEGER, cnt));
  std::vector<Column> column;
  column.emplace_back("inserted count", TypeId::INTEGER);
  Schema schema(column);
  *tuple = Tuple(inserted_cnt, &schema);

  if (cnt == 0 && is_executed_) {
    return false;
  }

  is_executed_ = true;

  return true;
}

}  // namespace bustub
