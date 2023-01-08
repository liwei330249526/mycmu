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
    tableId = plan_->GetTableOid();                                 // 比如我要查询一个表 SELECT * FROM t1; 这个表的id是多少呢,在plan_中可以得到
    tinf = ctx->GetCatalog()->GetTable(tableId);                    // exec_ctx 中可以获取 Catalog 就把他当做一个目录, 一些 表信息, index 信息从这里面获取
                                                                    // Catalog 中获取表信息 TableInfo, 即表的schema, 表名字, 表id, 表存储 TableHeap
    thp_ = tinf->table_.get();                                      // 获取  TableHeap 结构指针, 即表存储, 这个结构可以对KV数据 添删改查
    table_iter_ = thp_->Begin(ctx->GetTransaction());               // TableHeap 实现了迭代器, 获取迭代器, 存入到成员变量中 table_iter_

    return;                                                         // 做完上述准备工作, 即可已返回, 起始就做了两件事 
                                                                    // 1 thp_; 2 table_iter_, 因为next 函数只需这两个信息即可
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
    if (table_iter_ != thp_->End()) {                               // 迭代器重载了 *, 即获取迭代器指向的 tuple
        *tuple = *table_iter_;                                      // 返回 rid,  RID包含两个元素 pageid slotnum, 即某个页的的某个位置, 即tupe的位置
        *rid = tuple->GetRid();

        table_iter_++;                                              // 更新迭代器的 tuple 和 tuple 的 rid
            // todo: 根据 plan_->OutputSchema() 返回tupe, 因为返回的东西可能只是是 schema 的一部分, 根据输出schema 返回数据, 这里是返回了所有的数据
        return true;
    }

    return false;
}

}  // namespace bustub
