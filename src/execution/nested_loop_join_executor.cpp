//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
  result_idx_ = 0;
}

void NestedLoopJoinExecutor::Init() {
  Tuple out_tuple;
  Tuple inner_tuple;
  RID out_rid;
  RID inner_rid;
  left_executor_->Init();
  while (left_executor_->Next(&out_tuple, &out_rid)) {
    right_executor_->Init();
    while (right_executor_->Next(&inner_tuple, &inner_rid)) {
      if (plan_->Predicate() != nullptr && plan_->Predicate()
                                               ->EvaluateJoin(&out_tuple, left_executor_->GetOutputSchema(),
                                                              &inner_tuple, right_executor_->GetOutputSchema())
                                               .GetAs<bool>()) {
        std::vector<Value> output;
        // GetOutputSchema()->GetColumns() is just the columns we want, not all the columns
        for (auto const &column : GetOutputSchema()->GetColumns()) {
          output.push_back(column.GetExpr()->EvaluateJoin(&out_tuple, left_executor_->GetOutputSchema(), &inner_tuple,
                                                          right_executor_->GetOutputSchema()));
        }
        result_.emplace_back(Tuple(output, GetOutputSchema()));
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (result_idx_ >= result_.size()) {
    return false;
  }
  *tuple = result_[result_idx_];
  result_idx_++;
  return true;
}
}  // namespace bustub
