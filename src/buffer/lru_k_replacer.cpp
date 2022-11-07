//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

using namespace std;

namespace bustub {


LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k), 
                            size_victable1_(0), size_victable2_(0) {}

/* 
    驱逐一个, list1, 或 list2 
    1 在list1 中有可驱逐的, 反向遍历list1, 找到第一个可驱逐的节点, 返回给frame_id, 并删除该节点, 并 size_victable1_--
    2 list2 中同样操作 size_victable2_--
*/

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    // 1 优先驱逐list1
    if (this->size_victable1_ != 0) {
        //2 倒序遍历链表list1_, 找到第一个可驱逐的, 并驱逐
        for (auto it = this->list1_.rbegin(); it != this->list1_.rend(); it++) {        // 反向遍历删除第一个遇到的符合的节点
            frame_id_t fid = *it;
            *frame_id = fid;

            if (this->cache1_[fid]->isEvictable_) {
                it = std::list<frame_id_t>::reverse_iterator(this->list1_.erase((++it).base()));
                this->cache1_.erase(fid);
                break;
            }
        }
        this->size_victable1_--;
        return true;

    } else if (this->size_victable2_ != 0) {
        // 2 驱逐list2
        for (auto it = this->list2_.rbegin(); it != this->list2_.rend(); it++) {
            frame_id_t fid = *it;
            *frame_id = fid;
            if (this->cache2_[fid]->isEvictable_) {
                it = std::list<frame_id_t>::reverse_iterator(this->list2_.erase((++it).base()));
                this->cache2_.erase(fid);
                break;
            }
        }

        this->size_victable2_--;
        return true;
    }
    // 3 如果都为0, 则驱逐 
    return false; 
}

/*
 访问一个页, 记录事件 
 1 在list中, 
    1.1 到达k次访问, 移动节点到list2头部
    1.2 没有到达k次, 移动节点到list1头部
 2 在list2 中, 移动节点到list2头部  
 3 不在前者中 
    3.1 满了, 驱逐,  
    3.2 插入list1 

*/
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    // 1 cache1 中存在, 移动到头部, 访问次数++, 根据访问次数, 移动到cache2
    if (this->cache1_.find(frame_id) != this->cache1_.end()) {
        std::shared_ptr<Node> fnode = this->cache1_[frame_id];
        fnode->times_++;
        if (fnode->times_ == this->k_) {
            // 移动到缓存 cache 2
            this->list1_.remove(frame_id);
            this->list2_.push_front(frame_id);

            this->cache1_.erase(frame_id);
            std::pair<frame_id_t, std::shared_ptr<Node>> inPiar = std::make_pair(frame_id, fnode);
            this->cache2_.insert(inPiar);

            if (fnode->isEvictable_) {
                this->size_victable1_--;
                this->size_victable2_++;
            }

        } else {
            // 移动到头部
            this->list1_.remove(frame_id);
            this->list1_.push_front(frame_id);
        }
        return;
    } else if (this->cache2_.find(frame_id) != this->cache2_.end()) {  
        // 2 cache2 中存在, 则节点移动到头部
        this->list2_.remove(frame_id);
        this->list2_.push_front(frame_id);
        return;
    }

    //3 cache1, cache2 中都不存在, 如果 cache 满了, 则驱逐一个, 然后插入新的到头部; 如果不满, 则直接加入到头部
    if (this->list1_.size() + this->list2_.size() == this->replacer_size_) {
        frame_id_t ev_fid;
        if (!this->Evict(&ev_fid)) {
            return;
        }
    }

    std::shared_ptr<Node> fnodeIn = std::make_shared<Node>(frame_id, 1, false);        //智能指针 新节点Node, 访问1次, false 不可驱逐 
    this->list1_.push_front(frame_id);                                            //加入到 list1 头部
    std::pair<frame_id_t, std::shared_ptr<Node>> inPiar = std::make_pair(frame_id, fnodeIn);
    this->cache1_.insert(inPiar); // 加入 cache1_
    return;
}

/*
    设置可驱逐
    1 list1, 或list2, 找到节点, 设置 isEvictable_
    2 根据前后状态, 设置 size_victable1_-- 或size_victable1_++
                        size_victable2_-- 或size_victable2_++
*/
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    // 1 查找这个节点, 可能在list1, 可能在list2
    if (this->cache1_.find(frame_id) != this->cache1_.end()) {
        // 2 如果原来是true, 现在为false, 则 Evict 个数 --; 如果原来是false, 现在true, 则Evict个数++
        std::shared_ptr<Node> fnode = this->cache1_[frame_id];
        bool oldState =  fnode->isEvictable_;
        fnode->isEvictable_ = set_evictable;
        if (oldState && !set_evictable ){
            this->size_victable1_--;
        } else if (!oldState && set_evictable) {
            this->size_victable1_++;
        }
    } else if (this->cache2_.find(frame_id) != this->cache2_.end()) {
        std::shared_ptr<Node> fnode = this->cache2_[frame_id];
        bool oldState =  fnode->isEvictable_;
        fnode->isEvictable_ = set_evictable;
        if (oldState && !set_evictable ){
            this->size_victable2_--;
        } else if (!oldState && set_evictable) {
            this->size_victable2_++;
        }
    } 

    return;
}

/*
    删除
    1 list1 或 list2中, 找到节点删除, 
    2 如果是 isEvictable_ , size_victable1_--
*/
void LRUKReplacer::Remove(frame_id_t frame_id) {
    // 1 如果在list1 中, 删除
    if (this->cache1_.find(frame_id) != this->cache1_.end()) {
        std::shared_ptr<Node> fnode = this->cache1_[frame_id];
        this->cache1_.erase(frame_id);
        this->list1_.remove(frame_id);
        if (fnode->isEvictable_) {
            this->size_victable1_--;
        }
    } else if (this->cache2_.find(frame_id) != this->cache2_.end()) {
        // 2 如果在list2 中, 删除
        std::shared_ptr<Node> fnode = this->cache2_[frame_id];
        this->cache2_.erase(frame_id);
        this->list2_.remove(frame_id);
        if (fnode->isEvictable_) {
            this->size_victable2_--;
        }
    }

    return;
}

auto LRUKReplacer::Size() -> size_t {
    size_t ret = this->size_victable1_ + this->size_victable2_;
    return ret; 
}


void LRUKReplacer::MyPrintData() {
    tcout<< "----------------------------- " << endl;
    tcout << "list1_:" ;
    for (auto it = this->list1_.rbegin(); it != this->list1_.rend(); it++) {
        tcout << *it << " ";
    }
    tcout<< " " << endl;
    tcout<< "----------------------------- " << endl;
    tcout << "list2_:";
    for (auto it = this->list2_.rbegin(); it != this->list2_.rend(); it++) {
        tcout << *it << " ";
    }
    tcout<< " " << endl;
    tcout<< "----------------------------- " << endl;

    tcout<< "size_victable1_ "<<this->size_victable1_ << endl;
    tcout<< "size_victable2_ "<<this->size_victable2_ << endl;

    tcout<< " " << endl;
    tcout<< "----------------------------- " << endl;
}

//===--------------------------------------------------------------------===//
// Node
//===--------------------------------------------------------------------===//
LRUKReplacer::Node::Node(frame_id_t frid, size_t times, bool evtable) : frid_(frid), times_(times), isEvictable_(evtable) {}

}  // namespace bustub
