//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(int index, B_PLUS_TREE_LEAF_PAGE_TYPE *page, BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (this->bptLeafPage_ == nullptr || itr.bptLeafPage_ == nullptr) {
      if (this->bptLeafPage_ == nullptr && itr.bptLeafPage_ == nullptr) {
        return true;
      }
      return false;
    }

    return (this->bptLeafPage_->GetPageId() == itr.bptLeafPage_->GetPageId()) && (this->index_ == itr.index_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    if (*this == itr) {
      return false;
    }
    return true;
  }

 private:
  // add your own private member variables here
  int index_;                // 本节点的第多少个?
  Page *page_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *bptLeafPage_;       // 模板类leaf page 的对象指针
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
