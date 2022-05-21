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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  table_iter_ = std::make_unique<TableIterator>(table_heap_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto &iter = *table_iter_;
  for (; iter != table_heap_->End();) {
    auto tup = *(iter++);
    if (plan_->GetPredicate() != nullptr) {
      auto eva = plan_->GetPredicate()->Evaluate(&tup, GetOutputSchema()).GetAs<bool>();
      if (!eva) {
        continue;
      }
    }
    *tuple = Tuple(tup);
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

}  // namespace bustub
