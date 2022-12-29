//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.   获取下一个tuple
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */        // 输出schema 为了 scan
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  TableIterator table_iter_;
  TableHeap *thp_;
};
}  // namespace bustub




namespace bustub {

// 子类构造调用了父类构造
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    ExecutorContext *ctx;
    BufferPoolManager *bpm;
    table_oid_t tableId;
    TableInfo *tinf;
    page_id_t fpId;
    TablePage *tp;
    RID rid;
    

    ctx = GetExecutorContext();
    bpm = ctx->GetBufferPoolManager();
    tableId = plan_->GetTableOid();
    tinf = ctx->GetCatalog()->GetTable(tableId);
    thp_ = tinf->table_.get();
    fpId = thp_->GetFirstPageId();
    tp = reinterpret_cast<TablePage *>(bpm->FetchPage(fpId));       // 上面的所有, 只是为了后去要遍历的 TablePage
    
    tp->GetFirstTupleRid(&rid);                                     // 获取本表页的第一个元组的位置, 页, 槽位
    bpm->UnpinPage(fpId, false);

    table_iter_ = TableIterator(thp_, rid, ctx->GetTransaction());   // 根据第一个 tuple 构造了了一个迭代器
    return;

    //throw NotImplementedException("SeqScanExecutor is not implemented haha"); 
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (table_iter_ != thp_->End()) {
        *tuple = *table_iter_;
        *rid = tuple->GetRid();

        table_iter_++;

        
            // todo: 根据 plan_->OutputSchema() 返回tupe, 因为返回的东西可能只是是 schema 的一部分, 根据输出schema 返回数据, 这里是返回了所有的数据
         return true;
    }   
    
    return false;
}

}  // namespace bustub
