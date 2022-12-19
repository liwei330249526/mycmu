//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>    // 类，模板
#define INTERNAL_PAGE_HEADER_SIZE 24                                                               //page header 大小
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))  //page大小， 能容纳多少kv
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.   //n个key， n+1个子节点指针
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,   //第一个指针为无效
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):      //内部page 格式
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS    //模板类
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;

  auto IndexofInsert(KeyType key, KeyComparator &comparator) -> int;

  auto IndexByVal(ValueType val) -> int;

  auto Insert(KeyType key, ValueType value, KeyComparator &kcomparator) ->int;

  auto Indexof(KeyType key, KeyComparator &kcomparator) -> int;

  auto Remove(KeyType key, KeyComparator &kcomparator) -> int;

  auto RemoveByIndex(int index) -> int;

  auto RemoveFisrtNullKey() -> int;

  auto InsertFisrtNullKey(ValueType value) -> int;

  auto SetFisrtNullKey(ValueType value) -> int ;

  auto GetLastSmallerKey(KeyType key, int &index, KeyType &rtkey, KeyComparator &kcomparator) -> int;

  auto GetValByKey(KeyType key, ValueType &value, KeyComparator &kcomparator) -> bool;

  
  void InsertElemLast(MappingType elem);

  void MoveOutRightHalf(BPlusTreeInternalPage *to);

  void MoveInLeftHalf(MappingType *array, int start, int count);

  auto ItemAt(int index) const -> const MappingType &;

  auto IndexByKey(KeyType key, KeyComparator &kcomparator) -> int;

  void SetKeyByIndex(KeyType key, int index);


 private:
  // Flexible array member for page data.       //弹性数组，页数据
  MappingType array_[1];
};
}  // namespace bustub
