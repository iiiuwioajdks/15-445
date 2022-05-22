//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx) {
  child_ = std::move(child);
  plan_ = plan;
  aht_ = std::make_unique<SimpleAggregationHashTable>(
      SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes()));
  aht_iterator_ =
      std::make_unique<SimpleAggregationHashTable::Iterator>(SimpleAggregationHashTable::Iterator(aht_->Begin()));
}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_->InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  *aht_iterator_ = aht_->Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (*aht_iterator_ == aht_->End()) {
    return false;
  }
  auto key = aht_iterator_->Key();
  auto value = aht_iterator_->Val();
  ++(*aht_iterator_);
  if (plan_->GetHaving() == nullptr ||
      plan_->GetHaving()->EvaluateAggregate(key.group_bys_, value.aggregates_).GetAs<bool>()) {
    std::vector<Value> output;
    for (auto const &column : plan_->OutputSchema()->GetColumns()) {
      output.push_back(column.GetExpr()->EvaluateAggregate(key.group_bys_, value.aggregates_));
    }
    *tuple = Tuple(output, plan_->OutputSchema());
    return true;
  }
  return Next(tuple, rid);
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
