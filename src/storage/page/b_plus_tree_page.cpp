//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 * 是叶子页吗
 */
auto BPlusTreePage::IsLeafPage() const -> bool {
    if (this->page_type_ == IndexPageType::LEAF_PAGE) {
        return true;
    }
    return false;
}

auto BPlusTreePage::IsInternalPage() const -> bool {
    if (this->page_type_ == IndexPageType::INTERNAL_PAGE) {
        return true;
    }
    return false;
}

//是root 页吗
auto BPlusTreePage::IsRootPage(page_id_t rootId) const -> bool {
    if (this->parent_page_id_ == INVALID_PAGE_ID && 
        this->GetPageId()== rootId) {
        return true;
    }
    return false;
}
//设置页类型
void BPlusTreePage::SetPageType(IndexPageType page_type) {
    this->page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 * 获取大小
 */
auto BPlusTreePage::GetSize() const -> int {
    return this->size_;
}
//设置大小
void BPlusTreePage::SetSize(int size) {
    this->size_ = size;
}
//增加
void BPlusTreePage::IncreaseSize() {
    this->size_++;
}

void BPlusTreePage::IncreaseSize(int amount) {
    this->size_ = this->size_ + amount;
}

//增加
void BPlusTreePage::DecreaseSize() {
    this->size_--;
}

void BPlusTreePage::DecreaseSize(int amount) {
    this->size_ = this->size_ - amount;
}


/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int {
    return this->max_size_;
}
void BPlusTreePage::SetMaxSize(int size) {
    this->max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * 最小大小
 */
auto BPlusTreePage::GetMinSize() const -> int {
    return this->max_size_ / 2;
}

/*
 * Helper methods to get/set parent page id
 * 父节点id
 */
auto BPlusTreePage::GetParentPageId() const -> page_id_t {
    return this->parent_page_id_;
}
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
    this->parent_page_id_ = parent_page_id;
    return;
}

/*
 * Helper methods to get/set self page id
 */
auto BPlusTreePage::GetPageId() const -> page_id_t {
    return this->page_id_;
}
void BPlusTreePage::SetPageId(page_id_t page_id) {
    this->page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

}  // namespace bustub
