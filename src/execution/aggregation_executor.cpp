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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()){}
/*
因此，Aggregation 需要在 Init() 中直接计算出全部结果，将结果暂存，再在 Next() 中一条一条地 emit
*/

void AggregationExecutor::Init() {
    // 从子计划中获取值, 计算, 计算后存在hash中, next 发出值
    Tuple tuple;
    RID rid;
    printf("AggregationExecutor::Init\n");
    if (plan_->GetAggregates().size() == 0) {
        return;
    }

    while (true) {
        if (!(child_.get()->Next(&tuple, &rid))){
            break;
        }
        aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
    }
    aht_iterator_ = aht_.Begin();
    printf("AggregationExecutor::Init done\n");
    return;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    printf("AggregationExecutor::Next\n");
    std::vector<Value> values;
    std::vector<Value> keys ;

    if (plan_->GetAggregates().size() == 0) {
        return child_->Next(tuple, rid);
    }
    const Schema schema  = GetOutputSchema();
    //const Schema schema = plan_->InferAggSchema(plan_->GetGroupBys(), plan_->GetAggregates(), plan_->GetAggregateTypes());

    if (aht_iterator_ != aht_.End()) {                // 未结束, 返回true
        // 将key , val 组合成tuple , 传出, 判断是否可以传出
        values = aht_iterator_.Val().aggregates_;
        *tuple = Tuple(values, &schema);
        ++aht_iterator_;
        printf("AggregationExecutor::Next true\n");
        return true;
    }
    printf("AggregationExecutor::Next false\n");
    return false;                                           // 结束, 返回false
}

/*

void AggregationExecutor::Init() {
    printf("AggregationExecutor::Init\n");
    child_->Init();
    Tuple cur_tuple{};
    RID rid{};
    if( plan_->GetAggregates().size() != 0){
        while (child_->Next(&cur_tuple,&rid)) {
            aht_.InsertCombine(MakeAggregateKey(&cur_tuple), MakeAggregateValue(&cur_tuple));
        }
        aht_iterator_ = aht_.Begin();
    }
    printf("AggregationExecutor::Init done\n");
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    printf("AggregationExecutor::Next\n");
    std::vector<Value> values;
    const Schema output_schema = GetOutputSchema();

    if( plan_->GetAggregates().size() != 0 ){
        if( aht_iterator_ != aht_.End() ){
            values = aht_iterator_.Val().aggregates_;
            *tuple = Tuple(values, &GetOutputSchema());
            *rid = tuple->GetRid();
            ++aht_iterator_;
            printf("AggregationExecutor::Next true\n");
            return true;
        }
        printf("AggregationExecutor::Next false\n");
        return false;
    } else {
        printf("AggregationExecutor::Next child_->Next\n");
        return child_->Next(tuple, rid);
    } 
    
}*/

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
