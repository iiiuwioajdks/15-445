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
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  table_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_heap_ = table_->table_.get();
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

// hash table : key is KeyFromTuple(), value is rid
// RID对应page_id及slot_num，记录一个tuple的物理位置
bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto txn = exec_ctx_->GetTransaction();
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_->name_);
  auto lock_man = exec_ctx_->GetLockManager();
  auto schema = &table_->schema_;

  if (plan_->IsRawInsert()) {
    auto values = plan_->RawValues();
    for (auto const &value : values) {
      Tuple tup{value, schema};
      table_heap_->InsertTuple(tup, rid, txn);
      //            std::cout<<rid->ToString()<<std::endl;
      if (lock_man != nullptr) {
        if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
          if (txn->IsSharedLocked(*rid)) {
            lock_man->LockUpgrade(txn, *rid);
          } else {
            lock_man->LockExclusive(txn, *rid);
            //                    std::cout<<l<<" "<<rid->ToString()<<std::endl;
          }
        }
      }
      for (auto index : indexes) {
        index->index_->InsertEntry(
            tup.KeyFromTuple(*schema, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()), *rid, txn);
        txn->GetIndexWriteSet()->emplace_back(
            IndexWriteRecord(*rid, table_->oid_, WType::INSERT, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
      }
    }
    return false;
  }
  // select
  while (child_executor_->Next(tuple, rid)) {
    if (tuple != nullptr) {
      if (lock_man != nullptr) {
        if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
          if (txn->IsSharedLocked(*rid)) {
            lock_man->LockUpgrade(txn, *rid);
          } else {
            lock_man->LockExclusive(txn, *rid);
            //                    std::cout<<l<<" "<<rid->ToString()<<std::endl;
          }
        }
      }
      table_heap_->InsertTuple(*tuple, rid, txn);
      for (auto index : indexes) {
        index->index_->InsertEntry(
            tuple->KeyFromTuple(*schema, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()), *rid, txn);
        txn->GetIndexWriteSet()->emplace_back(
            IndexWriteRecord(*rid, table_->oid_, WType::INSERT, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
      }
    }
  }
  return false;
}

}  // namespace bustub
