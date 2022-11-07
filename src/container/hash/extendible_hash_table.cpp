//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include <iostream>


#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

using namespace std;

namespace bustub {


//#define TRACE       //debug
#ifndef TRACE
 #define tcout 0 && cout//或者NULL && cout
#else
 #define tcout cout
#endif


template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)   //构造, 全局深度为0, bucket 大小为2, 1个bucekt
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // 1 创建, bucket
  std::shared_ptr<Bucket> b = std::make_shared<Bucket>(bucket_size_, 0);
  dir_.push_back(b);
  return;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {   //id, 指向哪一个buckt
  int mask = (1 << global_depth_) - 1;  // 0, 1 , 11 , 111,  1111,
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {   //全局深度
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {   //local深度
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {       //bucket 数量
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {    //获取kv
  size_t id = IndexOf(key);                 // 获取桶index
  if ((int)id >= num_buckets_) {
    return false;
  }
  std::shared_ptr<Bucket> bp = dir_[id];   // 获取桶
  if (!bp) {   //指针为空
    return false;
  }

  return bp->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {         //删除kv
  size_t id = IndexOf(key);                 // 获取桶index
  if ((int)id >= num_buckets_) {
    return false;
  }
  std::shared_ptr<Bucket> bp = dir_[id];   //获取桶
  if (!bp) {
    return false;
  }
  return bp->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {   //插入kv
  //1 如果全局深度为0, 则直接取dir_[0]的 bucket
  size_t id = 0;
  tcout << "即将插入数据 " << "key " << key << " hash: " << std::hash<K>()(key) << endl;
  if (global_depth_ == 0) {
    std::shared_ptr<Bucket> bk = dir_[0];
    if (bk->IsFull()) {                       // 如果满了, 则分裂桶
      tcout << "global_depth_:" << global_depth_ << " 桶: " << 0 << " 满了需要分裂 " << endl;
      RedistributeBucket(bk, 0);
    }

    id = IndexOf(key);                         // 分裂桶后, 有可能dir_扩容, 所以要重新获取桶 index
    bk = dir_[id];
    tcout << "3 key: "<< key << "id: " << id << endl;
    tcout << "global_depth_:" << global_depth_ << " 即将向桶: " << id << " 插入数据: " << key << endl;
    bk->Insert(key, value);
    return;
  }
  //2 如果全局深不为 0, 则使用 hash 获取 桶index
  id = IndexOf(key);
  tcout << "1 key: "<< key << "id: " << id << endl;
  std::shared_ptr<Bucket> bk = dir_[id];
  if (bk->IsFull()) {                           // 如果满了, 则分裂桶
    tcout << "global_depth_:" << global_depth_ << " 桶: " << id << " 满了需要分裂 " << endl;
    RedistributeBucket(bk, id);
  }
  tcout << "2 key: "<< key << "id: " << id << endl;
  id = IndexOf(key);                            // 分裂桶后, 有可能dir_扩容, 所以要重新获取桶 index
  bk = dir_[id];

  tcout << "global_depth_:" << global_depth_ << " 即将向桶: " << id << " 插入数据: " << key << endl;
  bk->Insert(key, value);

  return;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, size_t dirId) -> void {   // 核心函数
  bool capacityExpansion = false;

  int dpth = bucket->GetDepth();
  if(dpth < this->global_depth_){   // 1 如果桶的depth 小于全局的depth, 则只做bucket分裂, 增加bucekt depth
    tcout << "RedistributeBucket: 只做bucket分裂" << endl;
    bucket->IncrementDepth();
  } else {                         // 2 如果桶的depth 等于全局的depth, 则 dir_ 扩容 + bucket分裂,  dir_ 大小扩容一倍,  bucket depth 和 全局depth 都增加 1
    tcout << "RedistributeBucket: dir扩容 + bucket分裂, 扩容" << endl;
    capacityExpansion = true;
    this->dir_.resize(dir_.size() * 2);
    bucket->IncrementDepth();
    this->global_depth_++;
    this->num_buckets_ = 1 << global_depth_;
  }

  std::shared_ptr<Bucket> newBp = std::make_shared<Bucket>(this->bucket_size_, bucket->GetDepth());  // 3 创建新桶
  std::list<std::pair<K, V>> &oldl = bucket->GetItems();                                             // 4 获取老桶的链表

  // 5 用 dirId 和 新的local depth 的 hash值, 重新分配 bucket 的数据, hash 值和dirId相等的说明最高位即 local depth 位为0, 留在原 bucket
  //   和 dirId 不等的说明最高位即local depth 位为1, 从原bucket 删除, 加入新 bucekt
  tcout << "RedistributeBucket: 遍历bucket, 重新分配" << endl;
  size_t newHash = 0;
  for (auto ft = oldl.begin(); ft != oldl.end(); ft++) {
    size_t ftHash = bucket->IndexV(((*ft).first));          // 每个key 的hash
    if (ftHash != dirId) {                                  // 每个key 的hash 和 dir_的id相等, 则留在原处, 否则插入新的bucket
      if (newHash == 0) {
        newHash = ftHash;
      }
      tcout << "RedistributeBucket: 即将插入输入 newBq key: " << (*ft).first << endl;
      newBp->Insert((*ft).first, (*ft).second);

      tcout << "RedistributeBucket: 删除老的 key: " << (*ft).first  << endl;
      ft = oldl.erase(ft);
    }
  }

  tcout << "RedistributeBucket: 设置新桶, dir_:  " << newHash << " 的对象: " << newBp  << " dir_size: " << dir_.size() << endl;
  dir_[newHash] = newBp;

  // 6 如果扩容了, 则从dir_ 1/2 处向下遍历, 遇到为null的则, 设置一个bucket, 设置到即前一个 depth 的dir_指向的地方,
  //   即设置到 hash 值最高位 depth 位为0 的位置
  tcout << "RedistributeBucket- capacityExpansion: 遍历 dir_, 重新分配" << endl;
  if (capacityExpansion) {
    size_t newStartId = 1 << (global_depth_ - 1);       //原先共多少个, 举个栗子:现在是 global_depth_是3, 共8个 dir_项; 原来是2, 共2个 dir_项; 则现在从[1<<2]->[4]开始遍历

    int mask = (1 << (global_depth_ - 1)) - 1;  // 0, 1 , 11 , 111,  1111,   原来是1<<(2)-1, 即: 11 , 对每个 i 取两位 hash值

    for (size_t i = newStartId; i < dir_.size(); i++) {
      if (dir_[i] == nullptr) {
        size_t oldId =  i & mask;
        tcout << "RedistributeBucket, 设置 dir: " << i << " 为老的 "<< oldId  << endl;
        dir_[i] = dir_[oldId];
      }
    }
  }

  return;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // 1 遍历链表, 发现则返回true
  for (auto b = list_.begin(); b != list_.end(); b++) {
    if ((*b).first == key) {
      value = (*b).second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // 1 遍历链表, 发现则返回true
  for (auto b = list_.begin(); b != list_.end(); b++) {
    if ((*b).first == key) {
      list_.erase(b);
      size_--;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // 1 链表操作
  for (auto b = list_.begin(); b != list_.end(); b++) {
    if ((*b).first == key) {
      (*b).second = value;
      return true;
    }
  }
  std::pair<K, V> pr = std::make_pair(key, value);
  this->list_.push_back(pr);
  tcout << "real insert key: " << key  << " bucket now depth_: " << this->depth_ << " size: " << list_.size() << endl;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::IndexV(const K &key) -> size_t {   // id, 指向哪一个buckt
  int mask = (1 << depth_) - 1;  // 0, 1 , 11 , 111,  1111,
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::GetTwoIndexByDepth(const K &key, int depth , int &id0, int &id1) -> void {    // 例如 depth 为3, 则id0, 取hash key 的前2位;  id1区 hash key 的3位
  int mask = (1 << depth) - 1;         // 例如depth 为 3, id0最高位设置为0, id1最高位设置1
  int tmp = std::hash<K>()(key) & mask;
  id0 = tmp | (1 <<(depth-1));
  id1 = tmp & ~(1 <<(depth-1));

  return ;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::PrintData() ->void {       // 测试函数打印桶内数据
  for (auto f = list_.begin(); f != list_.end(); f++) {
    tcout << "key: " << (*f).first << " hash: " << std::hash<K>()((*f).first) << endl;
  }
  return;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
