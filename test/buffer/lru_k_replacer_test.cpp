/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"
#include <iostream>

using namespace std;

namespace bustub {

//#define TRACE       //debug
#ifndef TRACE
 #define testcout 0 && cout//或者NULL && cout
#else
 #define testcout cout
#endif

TEST(LRUKReplacerTest, SampleTest) {
  LRUKReplacer lru_replacer(7, 2);

  // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.   增加6个数, 帧6是不可驱逐
  lru_replacer.RecordAccess(1);
  testcout<< 1 << endl;
  lru_replacer.MyPrintData();
  lru_replacer.RecordAccess(2);
  testcout<< 2 << endl;
  lru_replacer.MyPrintData();
  lru_replacer.RecordAccess(3);
  testcout<< 3 << endl;
  lru_replacer.MyPrintData();
  lru_replacer.RecordAccess(4);
  testcout<< 4 << endl;
  lru_replacer.MyPrintData();
  lru_replacer.RecordAccess(5);
  testcout<< 5 << endl;
  lru_replacer.MyPrintData();
  lru_replacer.RecordAccess(6);
  testcout<< 6 << endl;
  lru_replacer.MyPrintData();

  lru_replacer.SetEvictable(1, true);
  testcout<< "SetEvictable 1" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.SetEvictable(2, true);
  testcout<< "SetEvictable 2" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.SetEvictable(3, true);
  testcout<< "SetEvictable 3" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.SetEvictable(4, true);
  testcout<< "SetEvictable 4" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.SetEvictable(5, true);
  testcout<< "SetEvictable 5" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.SetEvictable(6, false);
  testcout<< "SetEvictable 6" << endl;
  lru_replacer.MyPrintData();

  ASSERT_EQ(5, lru_replacer.Size());    // size 不包括不可驱逐的

  // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.    帧1有2次访问
  // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].      其他帧有max backward k-dist,  可驱逐序列为
  lru_replacer.RecordAccess(1);

  // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped    驱逐3个页
  // first based on LRU.
  int value;
  lru_replacer.Evict(&value);             //[2,3,4,5,]         [1]
  ASSERT_EQ(2, value);
  testcout<< "Evict 2" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.Evict(&value);             //[3,4,5,]         [1]
  ASSERT_EQ(3, value);
  testcout<< "Evict 3" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.Evict(&value);             //之前 [4,5]         [1]
  ASSERT_EQ(4, value);
  testcout<< "Evict 4" << endl;
  lru_replacer.MyPrintData();
  ASSERT_EQ(2, lru_replacer.Size());                            //还有2个

  // Scenario: Now replacer has frames [5,1].
  // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
  lru_replacer.RecordAccess(3);           //之后-- [5,3]         [1]
  testcout<< "RecordAccess 3" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.RecordAccess(4);           //[5,3,4]       [1]
  testcout<< "RecordAccess 4" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.RecordAccess(5);           //[3,4]         [1,5]
  testcout<< "RecordAccess 5" << endl;
  lru_replacer.MyPrintData();
  
  lru_replacer.RecordAccess(4);           //[3]           [1,5,4]
  testcout<< "RecordAccess 4" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.SetEvictable(3, true);     
  testcout<< "SetEvictable 3" << endl;
  lru_replacer.MyPrintData();

  lru_replacer.SetEvictable(4, true);
  testcout<< "SetEvictable 4" << endl;
  lru_replacer.MyPrintData();
  ASSERT_EQ(4, lru_replacer.Size());

  // Scenario: continue looking for victims. We expect 3 to be evicted next.
  lru_replacer.Evict(&value);             //[]            [1,5,4]
  ASSERT_EQ(3, value);
  testcout<< "Evict -" << endl;
  lru_replacer.MyPrintData();

  ASSERT_EQ(3, lru_replacer.Size());
  testcout<< "Evict 3" << endl;
  lru_replacer.MyPrintData();

  // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
  lru_replacer.SetEvictable(6, true);     //[6]            [1,5,4]
  testcout<< "SetEvictable 6" << endl;
  lru_replacer.MyPrintData();

  ASSERT_EQ(4, lru_replacer.Size());    
  lru_replacer.Evict(&value);             //[]            [1,5,4]
  testcout<< "Evict -" << endl;
  lru_replacer.MyPrintData();
  ASSERT_EQ(6, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Now we have [1,5,4]. Continue looking for victims.
  lru_replacer.SetEvictable(1, false);    //[]            [(1),5,4]
  testcout<< "SetEvictable 1" << endl;
  lru_replacer.MyPrintData();

  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));  //[]            [(1),4]
  testcout<< "Evict -" << endl;
  lru_replacer.MyPrintData();
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, lru_replacer.Size());

  // Update access history for 1. Now we have [4,1]. Next victim is 4.
  lru_replacer.RecordAccess(1);           //[]            [4,(1)]
  lru_replacer.RecordAccess(1);           //[]            [4,(1)]
  lru_replacer.SetEvictable(1, true);     //[]            [4, 1]
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));   //[]            [1]
  ASSERT_EQ(value, 4);

  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Evict(&value);                     //[]            []
  ASSERT_EQ(value, 1);
  ASSERT_EQ(0, lru_replacer.Size());

  // These operations should not modify size
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());
  lru_replacer.Remove(1);
  ASSERT_EQ(0, lru_replacer.Size());
}
}  // namespace bustub
