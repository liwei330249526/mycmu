//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>                // pair 模板, kv 类型可变

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum， 页枚举
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * Both internal and leaf page are inherited from this page.
 *内部页 和 叶子页 都继承此页
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *它实际上是每个B+树页的标题部分，包含叶页和内部页共享的信息。
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
class BPlusTreePage {
 public:
  auto IsLeafPage() const -> bool;
  auto IsRootPage(page_id_t rootId) const -> bool;
  void SetPageType(IndexPageType page_type);

  auto GetSize() const -> int;
  void SetSize(int size);
  void IncreaseSize();
  void IncreaseSize(int amount);

  void DecreaseSize();
  void DecreaseSize(int amount);

  auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);
  auto GetMinSize() const -> int;

  auto GetParentPageId() const -> page_id_t;
  void SetParentPageId(page_id_t parent_page_id);

  auto GetPageId() const -> page_id_t;
  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);


  auto IsInternalPage() const -> bool;

 private:
  // member variable, attributes that both internal and leaf page share
  IndexPageType page_type_;               //页类型
  lsn_t lsn_ ;                            //日志序列
  int size_ ;                             //大小， kv对数量
  int max_size_;                          //最大大小
  page_id_t parent_page_id_;              //父页id
  page_id_t page_id_;                     //页id          
};

}  // namespace bustub
