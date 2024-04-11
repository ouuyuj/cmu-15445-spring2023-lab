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
    : AbstractExecutor(exec_ctx), plan_(plan) {
    
}

void SeqScanExecutor::Init() {
  // auto table_oid = plan_->GetTableOid();

  // auto catalog = exec_ctx_->GetCatalog();
  // auto table_info = catalog->GetTable(table_oid);
  // auto &table = table_info->table_;

  
  
  throw NotImplementedException("SeqScanExecutor is not implemented");

}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  

  // std::vector<Value> values{};
  // values.reserve(GetOutputSchema().GetColumnCount());
  // (*tuple);

  
  return false; 
}

}  // namespace bustub
