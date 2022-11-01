//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <map>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 *
  chace,  list,  cap, cout

  1 cache 存在, 移动节点到链表头, 更新val
  2 cache 不存在, 新建节点, 加入链表头, 加入cache, size++
  3 如果size 超出, 移除tail node, 移除cache 中, size--
 */


class LRUReplacer : public Replacer {
 private:
  size_t cmpality_;                 //容量
  size_t size_;                     //大小
  std::map<size_t, size_t> cache_;    //缓存
  std::list<size_t> dlist_;       //双向链表

 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
};

}  // namespace bustub
