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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    ExecutorContext *ctx;
    //BufferPoolManager *bpm;
    table_oid_t tableId;
    TableInfo *tinf;
    //page_id_t fpId;
    //TablePage *tp;
    RID rid;

    ctx = GetExecutorContext();
    //bpm = ctx->GetBufferPoolManager();
    tableId = plan_->GetTableOid();
    tinf = ctx->GetCatalog()->GetTable(tableId);
    thp_ = tinf->table_.get();                                      // 获取  TableHeap 结构指针
    table_iter_ = thp_->Begin(ctx->GetTransaction());               // TableHeap 实现了迭代器, 获取迭代器

    return;
    /*
    fpId = thp_->GetFirstPageId();
    tp = reinterpret_cast<TablePage *>(bpm->FetchPage(fpId));       // 上面的所有, 只是为了后去要遍历的 TablePage

    tp->GetFirstTupleRid(&rid);                                     // 获取本表页的第一个元组的位置, 页, 槽位
    bpm->UnpinPage(fpId, false);

    table_iter_ = TableIterator(thp_, rid, ctx->GetTransaction());   // 根据第一个 tuple 构造了了一个迭代器
    
    return;
    */
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {        // 通过出参, 返回tuple, rid
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
