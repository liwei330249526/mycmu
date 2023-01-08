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

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,  // 要插入的数据在 plan
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      childExecutor_(std::move(child_executor)) {}  // std::move 对于 unique_ptr
                                                    // 初始化列表中可以用 std::move
/*
    1 获取表的所有indexs
    2 子执行器初始化
*/
void InsertExecutor::Init() {
  ExecutorContext *ctx;
  table_oid_t tableId;
  TableInfo *tinf;
  RID rid;
  Catalog *clog;
  printf("InsertExecutor::Init() start\n");
  ctx = GetExecutorContext();
  // bpm = ctx->GetBufferPoolManager();
  tableId = plan_->TableOid();
  clog = ctx->GetCatalog();
  tinf = clog->GetTable(tableId);
  thp_ = tinf->table_.get();  // 获取  TableHeap 结构指针, 即存储接口

  indexes_ = clog->GetTableIndexes(tinf->name_);  // 所有 index, 为了向b+树中插入KV

  childExecutor_->Init();  // 子计划初始化, 子计划是啥, 咱也不知道, 反正这玩意儿总得初始化吧

  printf("InsertExecutor::Init() done\n");
  return;
}
// 要插入的值来自 plan
/*
    1 事务记录操作
    2 从子计划获取kv, 插入表中 TableHeap
*/
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  bool ret;
  printf("InsertExecutor::Next start\n");
  // std::cout<< "InsertExecutor::Next start tuple={" <<
  // tuple->ToString(&(GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())->schema_))
  //             << "}, rid={" << *rid << "}" << std::endl;

  // printf("InsertExecutor::Next start tuple=%s , rid=%s\n",
  // tuple->ToString(&(GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())->schema_)), rid->ToString());
  ret = childExecutor_->Next(tuple, rid);  // 另个参数是出参, 也就是说从这里获取要插入的 tuple 和 rid
  if (!ret) {
    return false;
  }
  //  auto InsertTuple(const Tuple &tuple, RID *rid, Transaction *txn) -> bool;
  Insert(tuple, rid);  // 这里插入数据
  printf("InsertExecutor::Next done\n");
  // std::cout<< "InsertExecutor::Next done tuple={" <<
  // tuple->ToString(&(GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())->schema_))
  //             << "}, rid={" << *rid << "}" << std::endl;
  // printf("InsertExecutor::Next sucess tuple=%s, rid=%s\n",
  // tuple->ToString(&(GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid())->schema_)), rid->ToString());
  return true;
}

/*
    1 插入表中数据
    2 事务处理; 遍历index, 将每个事务记录插入;
*/
void InsertExecutor::Insert(Tuple *tuple, RID *rid) {
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
  thp_->InsertTuple(*tuple, rid, txn);  // auto InsertTuple(const Tuple &tuple, RID *rid, Transaction *txn) -> bool;

  // 遍历 indexs, 用入参 tuple 构造K 插入到b+树中;  上面是插入了一行值, 但一行有多列, 改行的每个列吸入b+树中;
  // 并事务记录操作 事务操作肯定事务作用, 具体咋作用的咱也没具体分析; 插入b+树肯定是为了查询, 但查询怎么用的咱也没搞,

  for (auto it = indexes_.begin(); it != indexes_.end(); it++) {
    //(RID rid, table_oid_t table_oid, WType wtype, const Tuple &tuple, index_oid_t index_oid,
    //            Catalog *catalog)
    IndexWriteRecord writeRecord =
        IndexWriteRecord(*rid, plan_->TableOid(), WType::INSERT, *tuple, (*it)->index_oid_, clog);

    txn->AppendIndexWriteRecord(writeRecord);  // 事务记录插入
    // 获取b+树, 插入b+树
    // index_oid - > b+树
    bpIndex = reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(
        (*it)->index_.get());  // 模板使用, key 是一个8位的数字key; 获取b+树索引
    // auto Tuple::KeyFromTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs)
    //  b+树索引插入KV
    bpIndex->InsertEntry(
        tuple->KeyFromTuple(tinf->schema_, *((*it)->index_->GetKeySchema()), (*it)->index_->GetKeyAttrs()), *rid,
        txn);  // InsertEntry(const Tuple &key, RID rid, Transaction *transaction) override;
  }

  return;
}

}  // namespace bustub
