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
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  index_info_ = catalog->GetTableIndexes(catalog->GetTable(plan_->GetTableOid())->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  int64_t cnt = 0;
  bool index_info_is_empty = index_info_.empty();
  auto table = table_info_->table_.get();

  while (child_executor_->Next(&child_tuple, nullptr)) {
    TupleMeta tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    try {
      auto rid = table->InsertTuple(tuple_meta, child_tuple);
      if (!index_info_is_empty && rid) {
        for (auto &x : index_info_) {
          Tuple key_tuple =
              child_tuple.KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
          x->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
        }
      }

      cnt++;
    } catch (...) {
      throw;
    }
  }
  std::vector<Value> inserted_cnt;
  inserted_cnt.emplace_back(Value(TypeId::INTEGER, cnt));
  *tuple = Tuple(inserted_cnt, &GetOutputSchema());

  if (cnt == 0 && is_executed_) {
    return false;
  }

  is_executed_ = true;

  return true;
}

}  // namespace bustub
