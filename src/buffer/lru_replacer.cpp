//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <iostream>

namespace bustub {

/*
std::map,  map  :  m.erase(ret),删除ret键   ;  if (this->cache_.find(frame_id) != this->cache_.end()), 判断键frame_id 在mmap中是否存在
                   this->cache_[frame_id] = 1;  map插入键值对

std::list, 双向链表: this->dlist_.back(), 获取链表尾部元素;  this->dlist_.pop_back(), 弹出链表尾部元素
                    this->dlist_.remove(frame_id), 删除值为 frame_id 的链表节点;  this->dlist_.push_front(frame_id), 向链表头部插入节点
*/

LRUReplacer::LRUReplacer(size_t num_pages) {
    size_ = 0;
    cmpality_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

//牺牲掉一个页, 尾部的页
auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
    if (this->size_ != 0) {
        size_t ret = this->dlist_.back();
        *frame_id = ret;
        this->dlist_.pop_back();

        this->cache_.erase(ret);
        this->size_--;
        return true;
    }

    return false;
}

//使用一个页, 从LRU删除
void LRUReplacer::Pin(frame_id_t frame_id) {
    //1 如果存在, 则删除
    if (this->cache_.find(frame_id) != this->cache_.end()) {
        this->cache_.erase(frame_id);
        this->dlist_.remove(frame_id);
        this->size_--;
        return;
    }
    return;
}

//加入LRU, 加入链表头部
void LRUReplacer::Unpin(frame_id_t frame_id) {
    //1 如果存在, 则删除链表中删除老的, 向头部重新插入
    if (this->cache_.find(frame_id) != this->cache_.end()) {
        //this->dlist_.remove(frame_id);
        //this->dlist_.push_front(frame_id);
        return;
    }
    //2 如果不存在, 则加入cache, 加入链表头部
    this->cache_[frame_id] = 1;
    this->dlist_.push_front(frame_id);
    this->size_++;
    //3 如果超出容量, 则移除尾部
    if (this->size_ > this->cmpality_) {
        this->cache_.erase(frame_id);
        this->dlist_.pop_back();
        this->size_--;
    }
    std::cout<< this->size_ << std::endl;
    return;
}

//LRU 大小
auto LRUReplacer::Size() -> size_t {
    return this->size_;
}

}  // namespace bustub
