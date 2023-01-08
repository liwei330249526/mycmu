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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  ExecutorContext *ctx;
  table_oid_t tableId;
  TableInfo *tinf;
  RID rid;
  Catalog *clog;
  printf("DeleteExecutor::Init() start\n");
  ctx = GetExecutorContext();
  // bpm = ctx->GetBufferPoolManager();
  tableId = plan_->TableOid();
  clog = ctx->GetCatalog();
  tinf = clog->GetTable(tableId);
  thp_ = tinf->table_.get();  // 获取  TableHeap 结构指针, 即存储接口

  indexes_ = clog->GetTableIndexes(tinf->name_);  // 所有 index, 为了向b+树中插入KV

  child_executor_->Init();  // 子计划初始化, 子计划是啥, 咱也不知道, 反正这玩意儿总得初始化吧

  printf("DeleteExecutor::Init() done\n");
  return;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  bool ret;
  printf("DeleteExecutor::Next start\n");
  ret = child_executor_->Next(tuple, rid);  // 另个参数是出参, 也就是说从这里获取要插入的 tuple 和 rid
  if (!ret) {
    return false;
  }
  //  auto InsertTuple(const Tuple &tuple, RID *rid, Transaction *txn) -> bool;
  Delete(tuple, rid);  // 这里插入数据
  printf("DeleteExecutor::Next done\n");
  return true;
    return false;
}

void DeleteExecutor::Delete(Tuple *tuple, RID *rid) {
  Catalog *clog;
  BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *bpIndex;
  Transaction *txn;
  TableInfo *tinf;
  table_oid_t tableId;

  txn = GetExecutorContext()->GetTransaction();
  clog = GetExecutorContext()->GetCatalog();
  tableId = plan_->TableOid();
  tinf = clog->GetTable(tableId);
  // 插入表中数据, 这里已经将要插入的数据, 插入到表中了, 例如: INSERT INTO t1 VALUES (1, 'a');
  thp_->MarkDelete(*rid, txn);  // auto InsertTuple(const Tuple &tuple, RID *rid, Transaction *txn) -> bool;

  // 遍历 indexs, 用入参 tuple 构造K 插入到b+树中;  上面是插入了一行值, 但一行有多列, 改行的每个列吸入b+树中;
  // 并事务记录操作 事务操作肯定事务作用, 具体咋作用的咱也没具体分析; 插入b+树肯定是为了查询, 但查询怎么用的咱也没搞,

  for (auto it = indexes_.begin(); it != indexes_.end(); it++) {
    //(RID rid, table_oid_t table_oid, WType wtype, const Tuple &tuple, index_oid_t index_oid,
    //            Catalog *catalog)
    IndexWriteRecord deleteRecord =
        IndexWriteRecord(*rid, plan_->TableOid(), WType::DELETE, *tuple, (*it)->index_oid_, clog);

    txn->AppendIndexWriteRecord(deleteRecord);  // 事务记录插入
    // 获取b+树, 插入b+树
    // index_oid - > b+树
    bpIndex = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(
        (*it)->index_.get());  // 模板使用, key 是一个8位的数字key; 获取b+树索引
    // auto Tuple::KeyFromTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs)
    //  b+树索引插入KV
    bpIndex->DeleteEntry(
        tuple->KeyFromTuple(tinf->schema_, *((*it)->index_->GetKeySchema()), (*it)->index_->GetKeyAttrs()), *rid,
        txn);  // InsertEntry(const Tuple &key, RID rid, Transaction *transaction) override;
  }
}

}  // namespace bustub
