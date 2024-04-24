//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  index_info_ = catalog->GetTableIndexes(catalog->GetTable(plan_->GetTableOid())->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  int64_t cnt = 0;
  bool index_info_is_empty = index_info_.empty();
  auto table = table_info_->table_.get();
  RID del_rid;

  while (child_executor_->Next(&child_tuple, &del_rid)) {
    std::vector<Value> values{};
    values.reserve(table_info_->schema_.GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, table_info_->schema_));
    }

    Tuple new_tuple = Tuple{values, &table_info_->schema_};

    try {
      auto del_tuple_meta = table->GetTupleMeta(del_rid);
      del_tuple_meta.is_deleted_ = true;
      table->UpdateTupleMeta(del_tuple_meta, del_rid);

      auto new_rid = table->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple, exec_ctx_->GetLockManager(),
                                        exec_ctx_->GetTransaction(), table_info_->oid_);
      // update indexes
      if (!index_info_is_empty && new_rid) {
        for (auto &x : index_info_) {
          Tuple del_key_tuple =
              child_tuple.KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
          x->index_->DeleteEntry(del_key_tuple, del_rid, exec_ctx_->GetTransaction());

          Tuple key_tuple =
              new_tuple.KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
          x->index_->InsertEntry(key_tuple, *new_rid, exec_ctx_->GetTransaction());
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
