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
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto txn = exec_ctx_->GetTransaction();
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple del_tup;
  RID del_rid;
  auto lock_man = exec_ctx_->GetLockManager();
  while (child_executor_->Next(&del_tup, &del_rid)) {
    if (lock_man != nullptr) {
      if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if (txn->IsSharedLocked(del_rid)) {
          lock_man->LockUpgrade(txn, del_rid);
        } else {
          lock_man->LockExclusive(txn, del_rid);
        }
      }
    }
    table_heap_->MarkDelete(del_rid, txn);
    for (auto idx : indexes) {
      idx->index_->DeleteEntry(
          del_tup.KeyFromTuple(table_info_->schema_, *idx->index_->GetKeySchema(), idx->index_->GetKeyAttrs()), del_rid,
          txn);
      txn->GetIndexWriteSet()->emplace_back(IndexWriteRecord(del_rid, table_info_->oid_, WType::DELETE, del_tup,
                                                             idx->index_oid_, exec_ctx_->GetCatalog()));
    }
    //    if (lock_man != nullptr && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)
  }
  return false;
}

}  // namespace bustub
