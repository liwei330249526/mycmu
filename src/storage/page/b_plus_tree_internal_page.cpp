//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 * 创建内部页，包括页类型， 设置当前大小， 页id， 父id， 最大大小
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);

  this->SetMaxSize(max_size);
  this->SetPageType(IndexPageType::INVALID_INDEX_PAGE);
  this->SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  if (index < this->GetSize()) {
    MappingType mt = this->array_[index];
    return mt.first;
  }
  KeyType key{};
  return key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  if (index < this->GetSize()) {
    MappingType mt = this->array_[index];
    return mt.second;
  }
  ValueType val{};
  return val;
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ItemAt(int index) const -> const MappingType & {
  // replace with your own code{
    return this->array_[index];
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    if (index < this->GetSize()) {
      MappingType &mt = this->array_[index];
      mt.first = key;
      return ;
  }
  return;
}





/*
 * 
 * 找到key的page
 *          key1        key2       key3
 * page0    page1       page1      page3
 *  < key1   >=key1     >=key2     >=key3
 * 
 * -1 1 3 5 7  
 *       4
 *        5
 * return   1 和key相等的index 或 大于key 的index
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IndexofInsert(KeyType key, KeyComparator &comparator) -> int {   // 返回的是我要插入的位置
  int i = 1;
  for ( i = 1; i < this->GetSize(); i++) {
    if(comparator(key, this->array_[i].first) <= 0) {     // 找这么一个 key, 此 key 第一次大于 入参 key; 找第一个大于入参 key 的 key
      break;
    }
  }
  return i;
}

/**
 * @brief 
 * 
 * @param val 
 * @param kcomparator 
 * @return int 
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IndexByVal(ValueType val) -> int {
  int i = 0;
  for ( i = 1; i < this->GetSize(); i++) {
    if(this->array_[i].second == val) {     // 找这么一个 key, 此 key 第一次大于 入参 key; 找第一个大于入参 key 的 key
      return i;
    }
  }
  return -1;
}



/*
  internal node 插入KV
  return， 1 成功， 1
           2 失败， -1
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(KeyType key, ValueType value, KeyComparator &kcomparator) ->int {
  int index = this->IndexofInsert(key, kcomparator);
  if (kcomparator(this->array_[index].first, key) == 0) {
    return -1;
  }
  for (int i = this->GetSize(); i >= index; i--) {
    this->array_[i] = this->array_[i+1];
  }
  this->array_[index] = MappingType(key, value);
  this->IncreaseSize();
  return 1;
}

/*
  leaf node 插入KV
  return, 1 成功，index
          2 失败 -1
*/

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Indexof(KeyType key, KeyComparator &kcomparator) -> int {   // 返回的是我要插入的位置
  int i = 1;
  for ( i = 1; i < this->GetSize(); i++) {
    if(kcomparator(key, this->array_[i].first) == 0) {     // 找这么一个 key, 此 key 第一次大于 入参 key; 找第一个大于入参 key 的 key
      return i;
    } else if (kcomparator(key, this->array_[i].first) < 0) {
      return -1;
    } 
    
  }
  return -1;
}

/**
 * @brief 将elem 插入array_ 尾部
 * 
 * @param elem 
 * @return INDEX_TEMPLATE_ARGUMENTS 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertElemLast(MappingType elem) {
  int id = this->GetSize();
  this->array_[id] = elem;
  this->IncreaseSize();
}

/**
 * @brief 将右边一般移动到 to 节点的 左边
 * 
 * @param to 
 * @return INDEX_TEMPLATE_ARGUMENTS 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveOutRightHalf(BPlusTreeInternalPage *to)   {
  int count = this->GetSize() / 2;
  int midId = this->GetSize() - count;  // 10 个元素, 最大 index=9, mid=5, 共5个. 
  to->MoveInLeftHalf(this->array_, midId, count);
  this->DecreaseSize(count);
}

/**
 * @brief 将 array 从start 的connt 的elem 插入本地array尾部
 * 
 * @param array 
 * @param start 
 * @param count 
 * @return INDEX_TEMPLATE_ARGUMENTS 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveInLeftHalf(MappingType *array, int start, int count)  {
  for (int i = start; i < start + count; i++) {
    this->InsertElemLast(array[i]);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IndexByKey(KeyType key, KeyComparator &kcomparator) -> int {
  int i = 0;
  for ( i = 1; i < this->GetSize(); i++) {
    if(kcomparator(key, this->array_[i].first) <= 0) {     // 找这么一个 key, 此 key 第一次大于 入参 key; 找第一个大于入参 key 的 key
      break;
    }
  }
  return i;
}

/*
    删除key
        return,  1 成功， 1
                 2 失败， -1
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(KeyType key, KeyComparator &kcomparator) -> int {
  int index = this->IndexByKey(key, kcomparator);
  if (index == -1) {
    return -1;
  }

  // 删除 index 处元素
  for (int i = index+1; i < GetSize(); i++) {
    this->array_[i-1] = this->array_[i];
  }

  this->DecreaseSize();
  return 1;
  //如果到了最小, 合并
  
}

/*
  删除指定 index 处元素
  return,  1 成功, 1
           2 失败, -1
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveByIndex(int index) -> int {
  if (index >= this->GetSize()) {
    return -1;
  }

  // 删除 index 处元素
  for (int i = index+1; i < GetSize(); i++) {
    this->array_[i-1] = this->array_[i];
  }

  this->DecreaseSize();
  return 1;
  //如果到了最小, 合并
  
}

/*
    删除头部[0] kv
      return ,   1 成功， 1
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveFisrtNullKey() -> int {

  int index = 0;
  // 删除 index 处元素
  for (int i = index+1; i < GetSize(); i++) {
    this->array_[i-1] = this->array_[i];
  }

  this->DecreaseSize();
  return 1;
  //如果到了最小, 合并
}

/*
    插入头部[0], kv， 平移
        return, 1 成功， 1
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFisrtNullKey(ValueType value) -> int {
  int index = 0;

  for (int i = this->GetSize(); i >= index; i--) {
    this->array_[i] = this->array_[i+1];
  }
  this->array_[index] = MappingType(KeyType{}, value);
  this->IncreaseSize();
  return 1;
  //如果到了最小, 合并
}

/*
    设置头部[0], kv， 不平移
        return, 1 成功， 1
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetFisrtNullKey(ValueType value) -> int {
  int index = 0;

  this->array_[index] = MappingType(KeyType{}, value);
    // this->IncreaseSize();       // set 无需递增计数
  return 1;
  //如果到了最小, 合并
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyByIndex(KeyType key, int index) {
  // 校验 index
  this->array_[index].first = key;
  return;
}




/*
  返回一个key， 这key是小于传入 key 的最大key, 用与寻找父结点中指向本届点的key
  0 10 20 30 40  
         21         返回20
      11            返回19
    9               不可能出现     

    肯定能找到
        return   1 成功， 1
*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetLastSmallerKey(KeyType key, int &index, KeyType &rtkey, KeyComparator &kcomparator) -> int {
  int id = 0;
  for (int i = 1; i < this->GetSize(); i++) {
    if(kcomparator(this->array_[i].first, key) <= 0) {
      id = i;
      continue;
    } else {
      break;
    }
  }
  if (id == 0) {
    return 0;
  }
  id--;
  index = id;
  rtkey = this->array_[id].first;
  return 1;
}


// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
