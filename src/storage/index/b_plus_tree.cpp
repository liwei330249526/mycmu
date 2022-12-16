#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

/*
  疑问?
  1 获取page, 强转为 btpage,  判断是leaf, 或internal, 强转为 leafpage 或 internalpage
  2 怎么用多态, 父类指针指向子类对象, 调用子类的方法.
  3 内部节点, recordid, pageid, 怎么比较

  优化, 1 valueindex() 函数替换
        2 page 的 array_ 私有变量只能内部操作.
        3 总结leaf 和 internal 的 split 和 merge 的却别

*/
namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
  b+ 树是空的吗?
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // 1 用root page id 获取页, 强转为 BPlusTreePage, 判断是不是 leaf , 一直想左下搜索
  if (this->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  Page *page = this->buffer_pool_manager_->FetchPage(this->root_page_id_);   //获取page
  BPlusTreePage *btPage = reinterpret_cast<BPlusTreePage *>(page->GetData());         //page 的data 强转为btpage
  if (btPage->GetSize() == 0) {
    this->buffer_pool_manager_->UnpinPage(btPage->GetPageId(), false);
    return true;
  }
  this->buffer_pool_manager_->UnpinPage(btPage->GetPageId(), false);
  return false;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 * 获取key 的值
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  bool ret;
  LeafPage *lpage = reinterpret_cast<LeafPage *>(this->FindLeafPageByKey(key)->GetData());
  ValueType value;
  ret = lpage->GetValByKey(key, value, this->comparator_);
  return ret;
}

/*
  分裂内部节点
*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternalNode(InternalPage *bptPage) {
    if (bptPage->GetSize() != bptPage->GetMaxSize()) {
      this->buffer_pool_manager_->UnpinPage(bptPage->GetPageId(), true);
      return;
    }

    page_id_t newPageId;
    Page *newPage;
    newPage = this->buffer_pool_manager_->NewPage(&newPageId);                        // 1 创建新节点页; 右节点
    InternalPage *rightInternalPage;

    InternalPage *leftInternalPage =  bptPage;
    rightInternalPage = reinterpret_cast<InternalPage *>(newPage->GetData());
    rightInternalPage->Init(newPageId, bptPage->GetParentPageId(), bptPage->GetMaxSize());
                                                                                      // 2 将 leftInternalPage 的右边部分加入到 rightInternalPage
    bptPage->MoveOutRightHalf(rightInternalPage);
    KeyType rightMin = rightInternalPage->ItemAt(0).first;                             // [0],没有key, [1]右边节点最小 key

    // 如果没有父节点, 即本身是root 节点
    Page *parentPage;
    page_id_t parentId;
    InternalPage *parentInternalPage;
    if (bptPage->IsRootPage(this->GetRootPageId())) {
      parentPage = this->buffer_pool_manager_->NewPage(&parentId);                    // 3 创建父节点
      parentInternalPage = reinterpret_cast <InternalPage *>(parentPage->GetData());

      bptPage->SetParentPageId(parentId);
      rightInternalPage->SetParentPageId(parentId);

      parentInternalPage->Insert(KeyType{}, bptPage->GetPageId(), this->comparator_);                    // 4 创建了心的父节点, 插入左右孩子节点
      parentInternalPage->Insert(rightMin, rightInternalPage->GetPageId(), this->comparator_);

      //rightInternalPage->Remove(rightMin);                                            // 5 内部节点, 分裂, 右节点的最小值移动到父节点, 查询的不查这key即可, 无需操作

      this->root_page_id_ = parentId;
      this->UpdateRootPageId(0);
    } else {
      parentPage = this->buffer_pool_manager_->FetchPage(bptPage->GetParentPageId());    // 3 获取父节点页
      InternalPage *parentInternalPage = reinterpret_cast <InternalPage *>(parentPage->GetData());
      parentInternalPage->Insert(rightMin, rightInternalPage->GetPageId(), this->comparator_);           // 4  内部页, 新建右节点的首个KV移动到父节点
      
      //rightInternalPage->SetKeyAt(0, KeyType{});                                        // 5 内部节点, 分裂, 右节点的最小值移动到父节点, 右节点第一个key 设置为空, 但有val 为指向一个子节点的page
    }

    for (int i = 0; i < rightInternalPage->GetSize(); i++) {                         // 6 遍历右节点, 将其所有子节点的父节点设置为新的右节点id
      Page *childPage = this->buffer_pool_manager_->FetchPage(rightInternalPage->ItemAt(i).second);
      if (!bptPage->IsInternalPage()) {
        InternalPage *childInternalPage = reinterpret_cast <InternalPage *>(childPage->GetData());
        childInternalPage->SetParentPageId(rightInternalPage->GetPageId());
      } else {
        LeafPage *childLeafPage = reinterpret_cast <LeafPage *>(childPage->GetData());
        childLeafPage->SetParentPageId(rightInternalPage->GetPageId());
      }
      this->buffer_pool_manager_->UnpinPage(childPage->GetPageId(), true);
    }

    this->buffer_pool_manager_->UnpinPage(leftInternalPage->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(rightInternalPage->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(parentInternalPage->GetPageId(), true);

    this->SplitInternalNode(parentInternalPage);
    return;

}


/*
  分裂leaf 节点
*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeafNode(LeafPage *bptPage) {
    if (bptPage->GetSize() != bptPage->GetMaxSize()) {
      this->buffer_pool_manager_->UnpinPage(bptPage->GetPageId(), false);
      return;
    }

    page_id_t newPageId;
    Page *newPage;
    newPage = this->buffer_pool_manager_->NewPage(&newPageId);                        // 1 创建新节点页; 右节点 rightLeafPage

    LeafPage *leftLeafPage = bptPage;
    LeafPage *rightLeafPage;
    rightLeafPage = reinterpret_cast <LeafPage *>(newPage->GetData());
    rightLeafPage->Init(newPageId, bptPage->GetParentPageId(), bptPage->GetMaxSize());
                                                                                      // 2 将 leftLeafPage 右边部分加入到 rightLeafPage   
    for (int i = leftLeafPage->GetSize() / 2; i < leftLeafPage->GetSize() ; i++) {
      rightLeafPage->Insert(leftLeafPage->ItemAt(i).first, leftLeafPage->ItemAt(i).second, this->comparator_); 
      leftLeafPage->Remove(leftLeafPage->ItemAt(i).first, this->comparator_);
    }
    KeyType rightMin = rightLeafPage->ItemAt(0).first;                              // 右节点最小key

  
    rightLeafPage->SetNextPageId(leftLeafPage->GetNextPageId());                    // 3  设置 right 节点的 next 节点id 为原left 的next id
    leftLeafPage->SetNextPageId(rightLeafPage->GetPageId());                        // 设置 left  节点的 next 节点id 为 right 节点的 id


    // 如果没有父节点, 即本身是 root 节点
    Page *parentPage;
    page_id_t parentId;
    InternalPage *parentInternalPage = nullptr;
    if (bptPage->IsRootPage(this->GetRootPageId())) {
      parentPage = this->buffer_pool_manager_->NewPage(&parentId);                              // 4  新建父节点页
      InternalPage *parentInternalPage = reinterpret_cast <InternalPage *>(parentPage->GetData());
      leftLeafPage->SetParentPageId(parentId);                                           // 设置父节点pageid
      rightLeafPage->SetParentPageId(parentId);

      parentInternalPage->Insert(KeyType{}, bptPage->GetPageId(), this->comparator_);                  // 5 创建了心的父节点, 插入左右孩子节点
      parentInternalPage->Insert(rightMin, rightLeafPage->GetPageId(), this->comparator_);
      this->root_page_id_ = parentId;
      this->UpdateRootPageId(0);
      
    } else {
      parentPage = this->buffer_pool_manager_->FetchPage(this->GetRootPageId());                 // 4 获取父节点页
      InternalPage *parentInternalPage = reinterpret_cast <InternalPage *>(parentPage->GetData());
      parentInternalPage->Insert(rightMin, rightLeafPage->GetPageId(), this->comparator_);             // 5 叶子页, 新建的右侧节点的首个KV插入到父节点
    }
    this->buffer_pool_manager_->UnpinPage(leftLeafPage->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(rightLeafPage->GetPageId(), true);

    this->SplitInternalNode(parentInternalPage);

    return;
    //释放页
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 * 插入 key:val 对
 * 1 如果当前树为空, 新建树, 更新pageid, 插入 entry, 否则插入到leaf page
 * 我们支持唯一键, 如果插入重复键, false.  否则true
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {

  BPlusTreePage *bptPage = nullptr;
  LeafPage *leafPage;
  if (this->IsEmpty()) {                              // 如果树是空的
    page_id_t newPageId;
    Page *newPage;
    newPage = this->buffer_pool_manager_->NewPage(&newPageId);    // 1 创建新页
    this->root_page_id_ = newPageId;
    leafPage = reinterpret_cast<LeafPage *>(newPage);                   // 新页为叶子页
    leafPage->Init(newPageId);
    leafPage->Insert(key, value, this->comparator_);              // 插入K:V
    bptPage = leafPage;
    this->UpdateRootPageId(1);
    
  } else {
    leafPage = reinterpret_cast<LeafPage *>(this->FindLeafPageByKey(key)->GetData());                     // 找到这个key 所在的叶子页
    leafPage->Insert(key, value, this->comparator_);              // 插入K:V
    bptPage = leafPage;
    
  }
  // 尝试分裂节点
  if (!bptPage->IsInternalPage()) {
    this->SplitLeafNode(reinterpret_cast<LeafPage *>(bptPage));
  } else {
    this->SplitInternalNode(reinterpret_cast<InternalPage *>(bptPage));
  }
  return true;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StealLeafBrother(LeafPage *leafPage) -> bool {
  Page * parentPage = this->buffer_pool_manager_->FetchPage(leafPage->GetParentPageId());           // 1 获取父节点
  InternalPage *parentInternalPage = reinterpret_cast <InternalPage *>(parentPage->GetData());
  LeafPage *leftLeafPage;
  LeafPage *rightLeafPage;

  int index = 0;
  KeyType zkey;
  parentInternalPage->GetLastSmallerKey(leafPage->ItemAt(0).first, index, zkey,
                                                  this->comparator_);   
  //int index = parentInternalPage->IndexByKey(leafPage->ItemAt(0).first);                            // 2 获取本节点(叶子)第[0]个key在父节点的 index
  // parentInternalPage 大小应该 >=2, (包括[0]的空key)
  // 应该 (parentInternalPage->array_[index].first == leafPage->ItemAt(0).first)

  // 偷取左侧
  if (this->comparator_(parentInternalPage->ItemAt(index).first, leafPage->ItemAt(0).first) == 0 ) {
      leftLeafPage = reinterpret_cast<LeafPage *>(this->buffer_pool_manager_->FetchPage(parentInternalPage->ItemAt(index-1).second)->GetData());    // 3 获取左边[i-1]兄弟节点
  }
  if (leftLeafPage->GetSize() - 1 > leftLeafPage->GetMinSize()) {                                   // 4 如果左边兄弟节点可以借给我一个, 则借左边的尾部元素
    MappingType elem = leftLeafPage->ItemAt(leftLeafPage->GetSize()-1);
    leftLeafPage->Remove(elem.first, this->comparator_);
    parentInternalPage->Remove(leafPage->ItemAt(0).first, this->comparator_);                                          // 5 父节点删除原来右子索引

    leafPage->Insert(elem.first, elem.second, this->comparator_);                                                      // 6 本节点插入借来的KV
    parentInternalPage->Insert(elem.first, leafPage->GetPageId(), this->comparator_);                                  // 7 父节点插入借来的KV索引
    
    this->buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(parentInternalPage->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(leftLeafPage->GetPageId(), true);
    return true;
  }


  // 偷取右侧
  if (this->comparator_(parentInternalPage->ItemAt(index).first, leafPage->ItemAt(0).first) == 0 &&
      parentInternalPage->GetSize() - 1 >= index+1) {
      rightLeafPage = reinterpret_cast<LeafPage *>(this->buffer_pool_manager_->FetchPage(parentInternalPage->ItemAt(index+1).second)->GetData());   // 1 获取右侧节点
  }

  if (rightLeafPage->GetSize() - 1 > leftLeafPage->GetMinSize()) {                                  // 2 如果右侧节点可以借给我一个, 则借右边兄弟头部的元素                     
    MappingType elem = leftLeafPage->ItemAt(0);
    rightLeafPage->Remove(elem.first, this->comparator_);
    parentInternalPage->Remove(elem.first, this->comparator_);                                                         // 3 父节点删除右侧头部索引

    leafPage->Insert(elem.first, elem.second, this->comparator_);                                                      // 4 本节点加入借来的元素
    parentInternalPage->Insert(elem.first, leafPage->GetPageId(), this->comparator_);                                  // 5 父节点加入右侧的新头部索引

    this->buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(parentInternalPage->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(rightLeafPage->GetPageId(), true);
    return true;
  }
  return false;
}



INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StealInternalBrother(InternalPage *internalPage) -> bool {
  Page * parentPage = this->buffer_pool_manager_->FetchPage(internalPage->GetParentPageId());     // 1 获取父节点
  InternalPage *parentInternalPage = reinterpret_cast <InternalPage *>(parentPage->GetData());
  InternalPage *leftInternalPage;
  InternalPage *rightInternalPage;
  int index = parentInternalPage->IndexByKey(internalPage->ItemAt(0).first, this->comparator_);                      // 2 获取本节点
  // parentInternalPage 大小应该 >=2, (包括[0]的空key)
  // 应该 (parentInternalPage->array_[index].first == leafPage->ItemAt(0).first)

  // 偷取左侧
  if (this->comparator_(parentInternalPage->ItemAt(index).first, internalPage->ItemAt(0).first) == 0) {                
      leftInternalPage = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(parentInternalPage->ItemAt(index-1).second)->GetData());

      if (leftInternalPage->GetSize() - 1 > leftInternalPage->GetMinSize()) {                              // 3 如果左侧节点可以借给我一个
        auto elem = leftInternalPage->ItemAt(leftInternalPage->GetSize()-1);
        // 寻找父节点是哪一个 key 的指针指向我？ 找到这个key， 将这个key和[0] 的value 结合向后移动到[1]
        // 左侧借果来的 K:V 移动到右侧的[0] 位置， 并将K 移动到父节点。
        int zkeyIndex = 0;
        KeyType zkey;
        parentInternalPage->GetLastSmallerKey(internalPage->ItemAt(1).first, zkeyIndex, zkey,
                                                        this->comparator_);                           // 4 找到父节点指向本节点的指针的key
        internalPage->Insert(zkey, internalPage->ItemAt(0).second, this->comparator_);                // 5 将zkey和本届点[0]的val组成K:V，向后平移1
        internalPage->SetFisrtNullKey(elem.first, elem.second);                                       // 6 将借来的KV，放入[0]位置

        parentInternalPage->Remove(zkey, this->comparator_);                                                             // 7 父节点移除原先指向本节点指针的key
        parentInternalPage->Insert(elem.first, internalPage->GetPageId(), this->comparator_);                            // 8 父节点插入指向本节点指针的新K:V

        leftInternalPage->Remove(elem.first, this->comparator_);                                                         // 9 左节点删除借给我的K:V

        this->buffer_pool_manager_->UnpinPage(internalPage->GetPageId(), true);
        this->buffer_pool_manager_->UnpinPage(parentInternalPage->GetPageId(), true);
        this->buffer_pool_manager_->UnpinPage(leftInternalPage->GetPageId(), true);
        return 1;
     }
  }

  // 偷取右侧
  if (this->comparator_(parentInternalPage->ItemAt(index).first, internalPage->ItemAt(0).first) == 0 &&
      parentInternalPage->GetSize() - 1 >= index+1) {
    rightInternalPage = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(parentInternalPage->ItemAt(index+1).second)->GetData());   // 1 获取右侧节点
    
    if (rightInternalPage->GetSize() - 1 > leftInternalPage->GetMinSize()) {                          // 2 如果右侧节点可以借给我一个
      // 找父节点指针 zkey 
      // 构造KV， 加入本届点
      // 父节点删除原K， insert 新K
      // 右节点删除[0]
      int zkeyIndex = 0;
      KeyType zkey;
      parentInternalPage->GetLastSmallerKey(rightInternalPage->ItemAt(1).first, zkeyIndex,
                                                            zkey, this->comparator_);                 // 3 找到父节点指向本届点右侧节点的指针的key
                                                                                                   // 4 zkey 和右侧节点[0]的val 构造K:V
      
      auto elem = std::pair<KeyType, page_id_t>(zkey, rightInternalPage->ItemAt(0).second);
      internalPage->Insert(elem.first, elem.second, this->comparator_);
      parentInternalPage->Remove(zkey, this->comparator_);                                                               // 6 父节点删除 zkey， 并插入新的K:V， k为右侧节点array_[1].first, v为右侧节点的pageid
      parentInternalPage->Insert(rightInternalPage->ItemAt(1).first, rightInternalPage->GetPageId(), this->comparator_);

      rightInternalPage->RemoveFisrtNullKey();                                                        // 7 右侧节点移除[0] 位置                       


      this->buffer_pool_manager_->UnpinPage(internalPage->GetPageId(), true);
      this->buffer_pool_manager_->UnpinPage(parentInternalPage->GetPageId(), true);
      this->buffer_pool_manager_->UnpinPage(rightInternalPage->GetPageId(), true);
      return 1;
    }
  } 
  return 0;
}

/*
  尝试合并节点, leafNode

*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeLeafNode(LeafPage *leafPage) {
  int ret = 0;
  if (leafPage->GetSize() != leafPage->GetMinSize()) {                        // 如果本届点不需要 merge ， 则返回
    this->buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
    return;
  }
  ret = StealLeafBrother(leafPage);                                         // 1 先偷取
  if (ret) {
    return;
  }

  // 如果是根节点， 返回, 调整根节点
  if (leafPage->IsRootPage(this->GetRootPageId())){
    return;
  }

  Page * parentPage = this->buffer_pool_manager_->FetchPage(leafPage->GetParentPageId());           // 1 获取父节点
  InternalPage *parentInternalPage = reinterpret_cast <InternalPage *>(parentPage->GetData());
  LeafPage *leftLeafPage;
  LeafPage *rightLeafPage;
  int index = 0;
  KeyType zkey;
  parentInternalPage->GetLastSmallerKey(leafPage->ItemAt(0).first, index, zkey, 
                                                            this->comparator_);                    // 2 获取本节点(叶子)第[0]个key在父节点的 index
  if (index != 0) {       // 和左边合并
    Page *leftPage = this->buffer_pool_manager_->FetchPage(parentInternalPage->ItemAt(index-1).second);       // 3 获取左边[i-1]兄弟节点
    leftLeafPage = reinterpret_cast <LeafPage *>(leftPage->GetData());
    parentInternalPage->RemoveByIndex(index);                                                           // 4 父节点删除 zkey

    for (int i = 0; i < leafPage->GetSize(); i++) {                                                  // 5 将本节点的kv复制到左节点尾部， 本节点清空
      leftLeafPage->Insert(leafPage->ItemAt(i).first, leafPage->ItemAt(i).second, this->comparator_);
    }
    this->buffer_pool_manager_->DeletePage(leafPage->GetPageId());
    //this->buffer_pool_manager_->UnpinPage(parentInternalPage->GetPageId());
    this->buffer_pool_manager_->UnpinPage(leftLeafPage->GetPageId(), true);
  } else if (index != leafPage->GetSize()-1) {      // 和右边合并
    Page *rightPage = this->buffer_pool_manager_->FetchPage(parentInternalPage->ItemAt(index+1).second);       // 3 获取右边[i+1]兄弟节点
    rightLeafPage = reinterpret_cast <LeafPage *>(rightPage->GetData());
    parentInternalPage->RemoveByIndex(index);                                                                // 4 父节点删除 zkey

    for (int i = 0; i < leafPage->GetSize(); i++) {                                                  // 5 将本节点的kv复制到右节点尾部， 本节点清空
      rightLeafPage->Insert(leafPage->ItemAt(i).first, leafPage->ItemAt(i).second, this->comparator_);
    }
    this->buffer_pool_manager_->DeletePage(leafPage->GetPageId());
    this->buffer_pool_manager_->UnpinPage(rightLeafPage->GetPageId(), true);
  }


  MergeInternalNode(parentInternalPage);
}

/*
  尝试节点合并，internal
*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeInternalNode(InternalPage *internalPage) {
  int ret = 0;
  if (internalPage->GetSize() != internalPage->GetMinSize()) {                        // 如果本节点不需要 merge ， 则返回
    this->buffer_pool_manager_->UnpinPage(internalPage->GetPageId(), true);
    return;
  }

  ret = StealInternalBrother(internalPage);                                         // 1 先偷取
  if (ret) {
    return;
  }

  
  // 如果是根节点， 返回, 如果根节点, 则调整根节点
  if (internalPage->IsRootPage(this->GetRootPageId())){
    return;
  }

  Page * parentPage = this->buffer_pool_manager_->FetchPage(internalPage->GetParentPageId());           // 1 获取父节点
  InternalPage *parentInternalPage = reinterpret_cast<InternalPage *>(parentPage->GetData());
  
  int index = 0;
  KeyType zkey;
  parentInternalPage->GetLastSmallerKey(internalPage->ItemAt(1).first, index, zkey, 
                                                            this->comparator_);                    // 2 获取本节点(叶子)第[0]个key在父节点的 index

  InternalPage *leftInternalPage;
  InternalPage *rightInternalPage;
  if (index != 0) {       // 和左边合并
    Page *leftPage = this->buffer_pool_manager_->FetchPage(parentInternalPage->ItemAt(index-1).second);       // 3 获取左边[i-1]兄弟节点
    leftInternalPage = reinterpret_cast <InternalPage *>(leftPage->GetData());
    parentInternalPage->RemoveByIndex(index);                                                                // 4 父节点删除 zkey

    internalPage->SetFisrtNullKey(zkey, internalPage->ItemAt(0).second);
    for (int i = 0; i < internalPage->GetSize(); i++) {                                                  // 5 将本节点的kv复制到左节点尾部， 本节点清空
      leftInternalPage->Insert(internalPage->ItemAt(i).first, internalPage->ItemAt(i).second, this->comparator_);
    }
    this->buffer_pool_manager_->DeletePage(internalPage->GetPageId());
    //this->buffer_pool_manager_->UnpinPage(parentInternalPage->GetPageId());
    this->buffer_pool_manager_->UnpinPage(leftInternalPage->GetPageId(),true);
  } else if (index != internalPage->GetSize()-1) {      // 和右边合并, 将右边复制到左边
    Page *rightPage = this->buffer_pool_manager_->FetchPage(parentInternalPage->ItemAt(index+1).second);       // 3 获取右边[i+1]兄弟节点
    rightInternalPage = reinterpret_cast <InternalPage *>(rightPage->GetData());
    
    parentInternalPage->RemoveByIndex(index+1);                                                           // 4 父节点删除指向右侧节点的KV
    rightInternalPage->SetFisrtNullKey(parentInternalPage->ItemAt(index+1).first, rightInternalPage->ItemAt(0).second);
    for (int i = 0; i < rightInternalPage->GetSize(); i++) {                                                  // 5 将本节点的kv复制到右节点头部， 本节点清空
      internalPage->Insert(rightInternalPage->ItemAt(i).first, rightInternalPage->ItemAt(i).second, this->comparator_);
    }
    this->buffer_pool_manager_->DeletePage(rightInternalPage->GetPageId());
    this->buffer_pool_manager_->UnpinPage(internalPage->GetPageId(), true);
  }

  MergeInternalNode(internalPage);

}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 * 移除key:val
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    LeafPage *leafPage;
    leafPage = reinterpret_cast<LeafPage *>(this->FindLeafPageByKey(key)->GetData());                     // 找到这个key 所在的叶子页
    leafPage->Remove(key, this->comparator_);                                 // 删除K:V

    // 尝试偷取节点
    int ret = this->StealLeafBrother(leafPage);
    if (ret) {
      return;
    }
    // 尝试合并节点
    this->MergeLeafNode(leafPage);

    return;

}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftMostLeftLeafPage() -> Page* {
  // 1 用root page id 获取页, 强转为 BPlusTreePage, 判断是不是 leaf , 一直想左下搜索
  Page *page = this->buffer_pool_manager_->FetchPage(this->GetRootPageId());   //获取page
  BPlusTreePage *btPage = reinterpret_cast<BPlusTreePage *>(page->GetData());         //page 的data 强转为btpage

  BPlusTreePage *ctpage = btPage;
  // 2 遍历寻找, 条件不是leaf, 则继续寻找
  while (!ctpage->IsLeafPage()) {
    InternalPage *bintnal_page = reinterpret_cast <InternalPage *>(ctpage);    //btpage 强转为内部节点
    page_id_t leftPageId =  bintnal_page->ValueAt(0);                                 //获取第一个kv的v, 即pageid
    Page *leftPage = (this->buffer_pool_manager_->FetchPage(leftPageId));                  //或对page
    BPlusTreePage *leftBtPage  = reinterpret_cast <BPlusTreePage *>(leftPage->GetData());        //强转为btpage
    this->buffer_pool_manager_->UnpinPage(ctpage->GetPageId(), false);                            //获取了子接地那的page, 将父节点的page unpin

    ctpage = leftBtPage;                                                              //遍历, 则左侧
    page = leftPage;                                                                               //保存本次的page
  }
  //返回值怎么构造?  转化为leaf 节点
  return page;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 * 找到最左边的page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // 1 用root page id 获取页, 强转为 BPlusTreePage, 判断是不是 leaf , 一直想左下搜索
  Page *bptLeafPge = this->FindLeftMostLeftLeafPage();
  return INDEXITERATOR_TYPE(0, bptLeafPge, this->buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 * 
 * 找到key的page
 *          key1        key2       key3
 * page0    page1       page1      page3
 *  < key1   >=key1     >=key2     >=key3
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageByKey(KeyType key) -> Page* {
  // 1 用root page id 获取页, 强转为 BPlusTreePage, 判断是不是 leaf , 一直想左下搜索
  Page *page = (this->buffer_pool_manager_->FetchPage(this->GetRootPageId()));   //获取page
  BPlusTreePage *btPage = reinterpret_cast <BPlusTreePage *>(page->GetData());         //page 的data 强转为btpage

  // 2 遍历寻找, 条件不是leaf, 则继续寻找
  while (!btPage->IsLeafPage()) {
    InternalPage *inernalPage = reinterpret_cast <InternalPage *>(btPage);    //btpage 强转为内部节点

    // 3 找到key 应该的的页的页id,   找的是某个key， 我小于这个key， 则我是这个key 左边位置对应的page
    int id = 1; 
    bool find = false;
    for (id = 1; id < inernalPage->GetSize(); id++) {
      KeyType curKey = inernalPage->KeyAt(id);    // [小于 curKey]  [大于等于curKey]
      if (this->comparator_(key, curKey) < 0) {         //如果搜索的key 小于 curKey, 则位于前面的区间
        find = true;
        break;
      }
    }
    id = id - 1;
    if (!find) {                  //如果没有找到，则插入最后一个子节点
      id = inernalPage->GetSize() - 1;
    }
    page_id_t leftPageId = inernalPage->ValueAt(id);                                 //获取第一个kv的v, 即pageid


    Page *leftPage = (this->buffer_pool_manager_->FetchPage(leftPageId));                  //获取page
    BPlusTreePage *leftBtPage  = reinterpret_cast <BPlusTreePage *>(leftPage->GetData());        //强转为btpage
    this->buffer_pool_manager_->UnpinPage(btPage->GetPageId(), false);                              //获取了子节点的page, 将父节点的page unpin

    btPage = leftBtPage;                                                              //遍历, 则左侧
    page = leftPage;                                                                  //保存本次的page
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Page *page = FindLeafPageByKey(key);
  return INDEXITERATOR_TYPE(0, page, this->buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 * 直到最后的page 
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 * 获取root pageid
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  return this->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 * 更新root page id
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {    // 第一次是1, 插入新的; 后续的都是0, 更新
  //获取 0 page 的内存, 强转为 HeaderPage, ran后操作
  auto *header_page = reinterpret_cast <HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;


} // namespace bustub

