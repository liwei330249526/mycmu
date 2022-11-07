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

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool   分配连续内存空间
  pages_ = new Page[pool_size_];                 //size 个页
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);       //page table
  replacer_ = new LRUKReplacer(pool_size, replacer_k);         //lru
  next_page_id_ = 1;    //页从1开始
  // Initially, every page is in the free list.     //初始化 free list, 1, 2, 3 ,4
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  //todo: 判断buffpoool 满了, 且不可驱逐, 则返回null
  //检查freelist, freelist 一开始是所有帧
  frame_id_t page_fremeid;
  if (!free_list_.empty()) {
    page_fremeid = free_list_.front();
    free_list_.pop_front();
    page_table_->Insert(next_page_id_, page_fremeid);
    next_page_id_++;
    return &(pages_[page_fremeid]);
  } else {
    replacer_->Evict(&page_fremeid);      //驱逐一个页帧, 获取 page_fremeid
    //todo: 判断页是否脏了, 如果脏则写入disk
    page_table_->Insert(next_page_id_, page_fremeid);
    next_page_id_++;
    return &(pages_[page_fremeid]);
  }
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { return nullptr; }

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { return false; }

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { return false; }

void BufferPoolManagerInstance::FlushAllPgsImp() {}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { return false; }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
