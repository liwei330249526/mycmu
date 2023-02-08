//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
    index_oid_t indexOid;               // 索引的 id
    Catalog *clog;                      // 目录
    IndexInfo *idInfo;
    TableInfo *tInfo;
    printf("IndexScanExecutor::Init start\n");

    //BPlusTreeIndexForOneIntegerColumn *tree;    // 索引树

    indexOid = plan_->GetIndexOid();
    clog = GetExecutorContext()->GetCatalog();
    idInfo = clog->GetIndex(indexOid);

    tInfo = clog->GetTable(idInfo->table_name_);
    tHeap_ = tInfo->table_.get();
    tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(idInfo->index_.get());
    indexIter_ = tree_->GetBeginIterator();   // 获取迭代器; init函数就做了两件事, 1 获取 TableHeap; 为了获取 tuple; , 2 获取
                           //tree_ b+ 树,  b+ 树中是存在着我们的索引能够快速查找key:val, 其中val 貌似是 rid; 然后用 TableHeap 获取tuple 
    //IndexIterator(int index, B_PLUS_TREE_LEAF_PAGE_TYPE *page, BufferPoolManager *bpm)
    //bpm = exec_ctx->GetBufferPoolManager();
    //firstPage = reinterpret_cast<BPlusTree<BPlusTreePage<GenericKey<8>, RID, GenericComparator<8>> *>(bpm->FetchPage(tHeap->GetFirstPageId());)
    //firstPage->
    printf("IndexScanExecutor::Init done\n");
    return; 
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {      // 这俩参数应该是出参
    printf("IndexScanExecutor::Next start\n");
    if (indexIter_ != tree_->GetEndIterator()) {                    // 不等于结束的话, 可以查询出
        // 从indexIter 获取 tuple 和 rid
        IntegerValueType ridb = (*indexIter_).second;              // 返回的是 rid

        tHeap_->GetTuple(ridb, tuple, GetExecutorContext()->GetTransaction());      // TableHeap 获取tupple
        *rid = ridb;
        printf("IndexScanExecutor::Next done ok\n");
        return true;
    }
    printf("IndexScanExecutor::Next done not ok\n");
    return false;     
}

}  // namespace bustub
