//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { 
  child_executor_->Init();
  Catalog* catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  index_info_ = catalog->GetTableIndexes(catalog->GetTable(plan_->GetTableOid())->name_);
 }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
  Tuple child_tuple{};
  int64_t cnt = 0;
  bool index_info_is_empty = index_info_.empty();
  auto table = table_info_->table_.get();
  RID del_rid;

  while (child_executor_->Next(&child_tuple, &del_rid)) {
    try {
      auto del_tuple_meta = table->GetTupleMeta(del_rid);
      del_tuple_meta.is_deleted_ = true;
      table->UpdateTupleMeta(del_tuple_meta, del_rid);
      
      // update indexes
      if (!index_info_is_empty) { 
        for (auto &x : index_info_) {
          Tuple del_key_tuple = child_tuple.KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
          x->index_->DeleteEntry(del_key_tuple, del_rid, exec_ctx_->GetTransaction());
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
