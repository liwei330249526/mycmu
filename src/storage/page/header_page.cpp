//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/header_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>

#include "storage/page/header_page.h"

namespace bustub {

/**
 * Record related
 */
auto HeaderPage::InsertRecord(const std::string &name, const page_id_t root_id) -> bool {
  assert(name.length() < 32);
  assert(root_id > INVALID_PAGE_ID);
  // 几个record, 假设 5 个
  int record_num = GetRecordCount();
  // 地址, 4 + 5 * 36, 尾部添加到这个位置
  int offset = 4 + record_num * 36;
  // check for duplicate name
  // 检查重复, 如果里面原来有, 则返回false
  if (FindRecord(name) != -1) {
    return false;
  }
  // copy record content
  // 将 name 复制到这里, name 是32位大小, 将id 复制到这里
  memcpy(GetData() + offset, name.c_str(), (name.length() + 1));
  memcpy((GetData() + offset + 32), &root_id, 4);
  // 设置大小, 共 6 个record
  SetRecordCount(record_num + 1);
  return true;
}

auto HeaderPage::DeleteRecord(const std::string &name) -> bool {
  int record_num = GetRecordCount();
  assert(record_num > 0);

  int index = FindRecord(name);
  // record does not exsit
  if (index == -1) {
    return false;
  }
  int offset = index * 36 + 4;
  memmove(GetData() + offset, GetData() + offset + 36, (record_num - index - 1) * 36);

  SetRecordCount(record_num - 1);
  return true;
}

auto HeaderPage::UpdateRecord(const std::string &name, const page_id_t root_id) -> bool {
  assert(name.length() < 32);

  int index = FindRecord(name);
  // record does not exsit
  if (index == -1) {
    return false;
  }
  int offset = index * 36 + 4;
  // update record content, only root_id
  memcpy((GetData() + offset + 32), &root_id, 4);

  return true;
}

auto HeaderPage::GetRootId(const std::string &name, page_id_t *root_id) -> bool {
  assert(name.length() < 32);

  int index = FindRecord(name);
  // record does not exsit
  if (index == -1) {
    return false;
  }
  int offset = (index + 1) * 36;
  *root_id = *reinterpret_cast<page_id_t *>(GetData() + offset);

  return true;
}

/**
 * helper functions
 */
// record count
//data_ 的前4个字节为 count
auto HeaderPage::GetRecordCount() -> int { return *reinterpret_cast<int *>(GetData()); }
//data_ 的前4个字节为 count
void HeaderPage::SetRecordCount(int record_count) { memcpy(GetData(), &record_count, 4); }
//每个record 是36 个字节, 遍历每个record, 用 raw_name 匹配一个record; 如果找不到返回-1
auto HeaderPage::FindRecord(const std::string &name) -> int {
  int record_num = GetRecordCount();

  for (int i = 0; i < record_num; i++) {
    char *raw_name = reinterpret_cast<char *>(GetData() + (4 + i * 36));
    if (strcmp(raw_name, name.c_str()) == 0) {
      return i;
    }
  }
  return -1;
}
}  // namespace bustub
