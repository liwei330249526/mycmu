//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * t < f l c h c
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageId(page_id);
  this->SetNextPageId(INVALID_PAGE_ID);
  this->SetParentPageId(parent_id);

  this->SetMaxSize(max_size);
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return this->next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  this->next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  if (index < this->GetSize()) {
    MappingType mt = this->array_[index];
    return mt.first;
  }
  KeyType key{};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  MappingType mt = this->array_[index];
  return mt.second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertElemLast(MappingType elem) {
  int id = this->GetSize();
  this->array_[id] = elem;
  this->IncreaseSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveOutRightHalf(BPlusTreeLeafPage *to)   {
  int midId = this->GetSize() / 2;
  int count = this->GetSize() - midId;  // 10 个元素, 最大 index=9, mid=5, 共5个. 
  to->MoveInLeftHalf(this->array_, midId, count);
  this->DecreaseSize(count);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveInLeftHalf(MappingType *array, int start, int count)  {
  for (int i = start; i < start + count; i++) {
    this->InsertElemLast(array[i]);
  }
}

//找到第一个大于或等于 key 的 key 在数组 array_ 的索引,  1 3 5 7    
//                                                        4     return 2
//                                                        5     return 2
 
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IndexByKey(KeyType key, KeyComparator &kcomparator) -> int {
  int i = 0;
  for ( i = 0; i < this->GetSize(); i++) {
    if(kcomparator(key, this->array_[i].first) <= 0) {     // 找这么一个 key, 此 key 第一次大于 入参 key; 找第一个大于入参 key 的 key
      break;
    }
  }
  return i;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(KeyType key, ValueType value, KeyComparator &kcomparator) -> int {
  int index = this->IndexByKey(key, kcomparator);
  if (kcomparator(key, this->array_[index].first) == 0) {
    return 0;
  }
  for (int i = this->GetSize()-1; i >= index; i--) {
    this->array_[i+1] =this->array_[i];
  }
  this->array_[index] = MappingType(key, value);

  this->IncreaseSize();
  return 1;
  //如果到了最大, 则分裂
}


//找到第一个大于或等于 key 的 key 在数组 array_ 的索引,  1 3 5 7    
//                                                        4     return 2
//                                                        5     return 2
 
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValByKey(KeyType key, ValueType &value, KeyComparator &kcomparator) -> bool {
  int i = 0;
  bool find = false;
  for ( i = 0; i < this->GetSize(); i++) {
    if(kcomparator(key, this->array_[i].first) <= 0) {     // 找这么一个 key, 此 key 第一次大于 入参 key; 找第一个大于入参 key 的 key
      if (kcomparator(key, this->array_[i].first) == 0) {
        value = this->array_[i].second;
        find = true;
      }
      break;
    } 
  }
  if (find) {
    return true;
  }
  return false;

}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ItemAt(int index) const -> const MappingType & {
  // replace with your own code
    return this->array_[index];
}



INDEX_TEMPLATE_ARGUMENTS 
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(KeyType key, KeyComparator &kcomparator) -> int {
  int index = this->IndexByKey(key, kcomparator);
  if (kcomparator(key, this->array_[index].first) != 0) {
    return 0;
  }

  // 删除 index 处元素
  for (int i = index+1; i < GetSize(); i++) {
    this->array_[i-1] = this->array_[i];
  }

  this->DecreaseSize();
  return 1;
  //如果到了最小, 合并
}




template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
