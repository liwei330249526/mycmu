//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"
#include <iostream>

using namespace std;

namespace bustub {


//#define TRACE       //debug
#ifndef TRACE
 #define tcout 0 && cout//或者NULL && cout
#else
 #define tbpcout cout
#endif

/**
  pagetable: [pageid:page_fremeid]         <frame_id_t> free_list_      pages_[0]             LRU-K [0,1,2,3,4] 驱逐一个 framid
             [pageid:page_fremeid]                                      pages_[1]                
             [pageid:page_fremeid]                                      pages_[2]
             [pageid:page_fremeid]                                      pages_[3]
*/
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool   分配连续内存空间
  pages_ = new Page[pool_size_];                 //size 个页
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);       //page table
  replacer_ = new LRUKReplacer(pool_size, replacer_k);         //lru
  next_page_id_ = 0;    //页从1开始
  // Initially, every page is in the free list.     //初始化 free list, 1, 2, 3 ,4, 都是空闲的
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  return;
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

/*
  1 如果 freelist 不为空, 则 pages_ 存在空闲的位置, 获取一个空闲位置, 将 pageid 和 pagefremeid 对应关系存在map中, 返回对应 pages_ 的内存地址
  2 如果 freelist 为空, 则 pages_ 不存在空闲的位置, 则用lru-k 驱逐一个页, 获取 framid, 获取page对象, 如果page脏了, 写磁盘, 返回 page内存地址
*/
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  //
  bool ret;
  frame_id_t page_fremeid;
  if (!free_list_.empty()) {
    page_fremeid = this->free_list_.front();
    free_list_.pop_front();

    page_id_t npid = AllocatePage();
    page_table_->Insert(npid, page_fremeid);

    this->replacer_->RecordAccess(page_fremeid);
    this->replacer_->SetEvictable(page_fremeid, false);

    Page *npg = &(pages_[page_fremeid]);
    npg->pin_count_ = 1;
    
    npg->page_id_ = npid;

    *page_id = npid;
    return npg;
  } else {
    ret = this->replacer_->Evict(&page_fremeid);     // 驱逐一个页帧, 获取 page_fremeid
    if (!ret) {
      return nullptr;
    }
    Page * evp = &(this->pages_[page_fremeid]);     // 驱逐的页
    if (evp->is_dirty_) {
      this->disk_manager_->WritePage(evp->page_id_, evp->GetData());
    }

    page_id_t npid = AllocatePage();
    this->page_table_->Remove(evp->page_id_);         //删除原来的page_id
    this->page_table_->Insert(npid, page_fremeid);   //加入新的pageid

    this->replacer_->RecordAccess(page_fremeid);
    this->replacer_->SetEvictable(page_fremeid, false);
    
    evp->ResetMemory();
    evp->pin_count_ = 1;
    evp->page_id_ = npid;
    evp->is_dirty_ = false;

    *page_id = npid;

    return evp;
  }
}
/*
  1 从pagetable 中查找, 如果存在则返回此页
  2 从freelist 中找到一个空的 pages_的 位置, 如果能找到到, 使用 diskmanager 读取pageid的页到该 pageframid位置     -->4
  3    freelist为空                                     , 则使用lru-k 驱逐一个, 获取 framid  如果page脏了, 写磁盘   -->4
  4 返回该页
*/
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  bool ret;
  frame_id_t frame_id;
  if (this->page_table_->Find(page_id, frame_id)) {
    return &(this->pages_[frame_id]);
  }

  if (!this->free_list_.empty()) {
    frame_id = this->free_list_.front();
    this->free_list_.pop_front();

    Page *fpage = &(this->pages_[frame_id]);
    this->disk_manager_->ReadPage(page_id, fpage->GetData());
    fpage->pin_count_ = 1;
    fpage->page_id_ = page_id;
    fpage->is_dirty_ = false;

    this->page_table_->Insert(page_id, frame_id);
    this->replacer_->RecordAccess(frame_id);
    return fpage;
  }

  ret = this->replacer_->Evict(&frame_id);
  if (!ret) {
    return nullptr;
  }
  Page * evp = &(this->pages_[frame_id]);   // 驱逐的页
  if (evp->is_dirty_) {
    this->disk_manager_->WritePage(evp->page_id_, evp->GetData());   //将驱逐页写入
  }

  this->page_table_->Remove(page_id);
  this->page_table_->Insert(page_id, frame_id);

  this->replacer_->RecordAccess(frame_id);
  this->replacer_->SetEvictable(frame_id, false);

  evp->ResetMemory();
  this->disk_manager_->ReadPage(page_id, evp->GetData());    //读取要读的页
  evp->pin_count_ = 1;
  evp->page_id_ = page_id;
  evp->is_dirty_ = false;


  return evp;
}

/*
  1 从pagetable 中查找, 如果不存在, 则返回; 或页的 pincount为0, 则返回
  2 pincount--, 如果pincount 到了0, 则加入 lru-k, 设置可驱逐
  3 传入为脏, 则设置脏页
*/
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  if (!this->page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *fpage = &(this->pages_[frame_id]);
  if (fpage->pin_count_ == 0) {
    return false;
  }

  fpage->pin_count_--;
  if (fpage->pin_count_ == 0) {
    this->replacer_->SetEvictable(frame_id, true);
  }

  fpage->is_dirty_ = is_dirty;

  return true; 
}

/*
  1 从pagetable中找 page_id, 如果没有, 则返回false
  2 如果有, 则刷到磁盘, 重置脏标记
*/
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  if (!this->page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page * fpg = &(this->pages_[frame_id]);
  if (fpg->page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  this->disk_manager_->WritePage(fpg->page_id_, fpg->GetData());
  fpg->is_dirty_ = false;
  return true; 
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < this->pool_size_; i++) {
    Page *fpg = &(this->pages_[i]);
    this->FlushPgImp(fpg->page_id_);
  }
  return;
}

/*
  1 从pagetable中找 page_id, 如果没有, 则返回false; 如果页被pin , 则不可删除, 返回false
  2 删除页在pagetable, lru, 加入freelist 中, 
*/
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  frame_id_t frame_id;
  if (!this->page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *dpg = &(this->pages_[frame_id]);
  if (dpg->pin_count_ != 0) {
    return false;
  }

  this->page_table_->Remove(page_id);
  this->replacer_->Remove(frame_id);
  this->free_list_.push_front(frame_id);
  this->DeallocatePage(page_id);

  return true; 
}

void BufferPoolManagerInstance::MyPrintData() {
  
  for (size_t i = 0; i < this->pool_size_; i++) {
    Page *fpg = &(this->pages_[i]);
    tcout<< "fremid: " << i << " pageid: " << fpg->page_id_ << " pin_count_ " << fpg->pin_count_ << endl;
  }
  tcout << "-----------------------------------------" << endl;
  tcout << "-----------------------------------------" << endl;
  return;
}

// 找到第一个为 0 的id
auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  page_id_t ret;
  if (!this->free_pageid_.empty()) {
    page_id_t ret = this->free_pageid_.front();
    this->free_pageid_.pop_front();
    return ret;
  }
  ret = this->next_page_id_;
  next_page_id_++;
  return ret; 
}

// 将此id 返回来
void BufferPoolManagerInstance::DeallocatePage(page_id_t page_id) {
  this->free_pageid_.push_back(page_id);
  return;
}



}  // namespace bustub
