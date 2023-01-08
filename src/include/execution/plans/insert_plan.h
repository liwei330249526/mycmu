//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_plan.h
//
// Identification: src/include/execution/plans/insert_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * The InsertPlanNode identifies a table into which tuples are inserted.
 *InsertPlanNode 标识插入元组的表。
 * The values to be inserted will come from the child of the node.
 * //要插入的值将来自节点的子节点。
 */
class InsertPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new insert plan node for inserting values from a child plan.
   * @param child the child plan to obtain values from
   * @param table_oid the identifier of the table that should be inserted into
   * 创建新的插入计划节点，用于插入 子计划中的值。
   * 从此自己花获取值
   * 表的标识, 应该向词表插入值
   */
  InsertPlanNode(SchemaRef output, AbstractPlanNodeRef child, table_oid_t table_oid)
      : AbstractPlanNode(std::move(output), {std::move(child)}), table_oid_(table_oid) {}

  /** @return The type of the plan node */   // plan node 类型
  auto GetType() const -> PlanType override { return PlanType::Insert; }

  /** @return The identifier of the table into which tuples are inserted */   // 表的标识符, tuples要插入这里
  auto TableOid() const -> table_oid_t { return table_oid_; }

  /** @return the child plan providing tuples to be inserted */
  // 子计划提供tupes 将被插入
  auto GetChildPlan() const -> AbstractPlanNodeRef {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Insert should have only one child plan.");        // 插入的值来自子计划, 至少有一个子计划
    return GetChildAt(0);
  }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(InsertPlanNode);

 protected:
  auto PlanNodeToString() const -> std::string override { return fmt::format("Insert {{ table_oid={} }}", table_oid_); }

 private:
  /** The table to be inserted into. */
  table_oid_t table_oid_;
};

}  // namespace bustub
