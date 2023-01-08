//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * InsertExecutor executes an insert on a table.
 * Inserted values are always pulled from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new InsertExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the insert */    //初始化 insert
  void Init() override;

  /**
   * Yield the number of rows inserted into the table.
   * @param[out] tuple The integer tuple indicating the number of rows inserted into the table 整数元组，指示插入到表中的行数
   * @param[out] rid The next tuple RID produced by the insert (ignore, not used)  插入生成的下一个元组RID（忽略，不使用）
   * @return `true` if a tuple was produced, `false` if there are no more tuples `true`如果生成了元组，如果没有更多元组，则为`false`
   *
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
   */
  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */     // 插入的输出schema
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  void Insert(Tuple *tuple, RID *rid);

 private:
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;          // insert 计划节点
  TableHeap *thp_;                      // 表 heap

  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<IndexInfo *> indexes_;
};

}  // namespace bustub
