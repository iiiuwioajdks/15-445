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

#include <sstream>

#include "concurrency/transaction.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), cur_(nullptr, RID{}, nullptr) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  try {
    for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
      auto col_name = plan_->OutputSchema()->GetColumn(i).GetName();
      out_schema_idx_.push_back(table_info_->schema_.GetColIdx(col_name));
    }
  } catch (const std::logic_error &error) {
    for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
      out_schema_idx_.push_back(i);
    }
  }

  if (plan_->GetPredicate() != nullptr) {
    predicate_ = plan_->GetPredicate();
  } else {
    is_alloc_ = true;
    // always true
    predicate_ = new ConstantValueExpression(ValueFactory::GetBooleanValue(true));
  }
}

SeqScanExecutor::~SeqScanExecutor() {
  if (is_alloc_) {
    delete predicate_;
  }
  predicate_ = nullptr;
}

void SeqScanExecutor::Init() { cur_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto lock_man = GetExecutorContext()->GetLockManager();
  auto txn = GetExecutorContext()->GetTransaction();

  while (cur_ != table_info_->table_->End()) {
    auto temp = cur_++;
    out_schema_idx_.reserve(plan_->OutputSchema()->GetColumnCount());

    if (lock_man != nullptr) {
      if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if (!txn->IsSharedLocked(temp->GetRid()) && !txn->IsExclusiveLocked(temp->GetRid())) {
          lock_man->LockShared(txn, temp->GetRid());
        }
      }
    }

    auto value = predicate_->Evaluate(&(*temp), &table_info_->schema_);
    if (value.GetAs<bool>()) {
      // Only keep the columns of the out schema
      std::vector<Value> values;
      values.reserve(out_schema_idx_.size());
      for (auto i : out_schema_idx_) {
        values.push_back(temp->GetValue(&table_info_->schema_, i));
      }
      *tuple = Tuple(values, plan_->OutputSchema());
      *rid = temp->GetRid();

      if (lock_man != nullptr && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        lock_man->Unlock(txn, temp->GetRid());
      }
      return true;
    }
    if (lock_man != nullptr) {
      lock_man->Unlock(txn, temp->GetRid());
    }
  }
  return false;
}

}  // namespace bustub
