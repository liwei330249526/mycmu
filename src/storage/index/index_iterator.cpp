/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(int index, B_PLUS_TREE_LEAF_PAGE_TYPE *btpage, BufferPoolManager *bpm):
                              index_(index), bptLeafPage_(btpage), buffer_pool_manager_(bpm) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

/**
 * @brief 首先要明白, 迭代器是为了扫描的时候, 有可能是跨叶子页的.
 * 结束条件, 首先index_是最后一个了,  size = 10, index = 10, 且
 *          next page 为非法, 即没有下一个页
 * 使用方法一般为, for it = begin; !it.isEnd(); it++ {
 *                } 
 *                it++ 后
 * 
 * @return true 
 * @return false 
 */
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    if (this->bptLeafPage_ == nullptr) {
        return true;
    }
    return false;
}


INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
    assert(this->index_>=0 && this->index_ < this->bptLeafPage_->GetSize());
    return this->bptLeafPage_->ItemAt(this->index_);
}

/**
 * @brief 跌跌器递增.
 * 如果增到该页的末尾, 则获取新页的next 页, 将next 页设置为本迭代页,
 *                   设置index
 * 
 * @return INDEXITERATOR_TYPE& 
 *          1. 成功, 迭代器
 *          2. 失败, 空
 */
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    this->index_++;
    if (this->bptLeafPage_->GetSize() == this->index_ ) {
        if (this->bptLeafPage_->GetNextPageId() == INVALID_PAGE_ID) {
            this->bptLeafPage_ = nullptr;
            return *this;
        }
        Page *nextPage = this->buffer_pool_manager_->FetchPage(this->bptLeafPage_->GetNextPageId()); 
        if (nextPage == nullptr) {      // 非正常逻辑, 不会走进来
            printf("非正常逻辑");
        }
        B_PLUS_TREE_LEAF_PAGE_TYPE *nextLeafPage = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(nextPage->GetData());
        this->buffer_pool_manager_->UnpinPage(this->bptLeafPage_->GetPageId(), false);

        this->bptLeafPage_ = nextLeafPage;
        this->index_ = 0;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
