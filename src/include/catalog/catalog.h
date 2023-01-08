//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog.h
//
// Identification: src/include/catalog/catalog.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "container/hash/hash_function.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;
using index_oid_t = uint32_t;

/**
 * The TableInfo class maintains metadata about a table.
 */
struct TableInfo {
  /**
   * Construct a new TableInfo instance.
   * @param schema The table schema
   * @param name The table name
   * @param table An owning pointer to the table heap
   * @param oid The unique OID for the table
   */
  TableInfo(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_{std::move(schema)}, name_{std::move(name)}, table_{std::move(table)}, oid_{oid} {}
  /** The table schema */
  Schema schema_;
  /** The table name */
  const std::string name_;
  /** An owning pointer to the table heap */       // table指针
  std::unique_ptr<TableHeap> table_;
  /** The table OID */
  const table_oid_t oid_;
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
struct IndexInfo {
  /**
   * Construct a new IndexInfo instance.
   * @param key_schema The schema for the index key
   * @param name The name of the index
   * @param index An owning pointer to the index
   * @param index_oid The unique OID for the index
   * @param table_name The name of the table on which the index is created
   * @param key_size The size of the index key, in bytes
   */
  IndexInfo(Schema key_schema, std::string name, std::unique_ptr<Index> &&index, index_oid_t index_oid,
            std::string table_name, size_t key_size)
      : key_schema_{std::move(key_schema)},
        name_{std::move(name)},
        index_{std::move(index)},
        index_oid_{index_oid},
        table_name_{std::move(table_name)},
        key_size_{key_size} {}
  /** The schema for the index key */
  Schema key_schema_;
  /** The name of the index */
  std::string name_;
  /** An owning pointer to the index */
  std::unique_ptr<Index> index_;
  /** The unique OID for the index */
  index_oid_t index_oid_;
  /** The name of the table on which the index is created */
  std::string table_name_;
  /** The size of the index key, in bytes */
  const size_t key_size_;
};

/**
 * The Catalog is a non-persistent catalog that is designed for
 * use by executors within the DBMS execution engine. It handles
 * table creation, table lookup, index creation, and index lookup.
 * 非持久, 用于 DBMS 执行引擎的执行器; 就把他当做一个目录, 一些 表信息, index 信息从这里面获取
 */
class Catalog {
 public:
  /** Indicates that an operation returning a `TableInfo*` failed */
  static constexpr TableInfo *NULL_TABLE_INFO{nullptr};

  /** Indicates that an operation returning a `IndexInfo*` failed */
  static constexpr IndexInfo *NULL_INDEX_INFO{nullptr};

  /**
   * Construct a new Catalog instance.
   * @param bpm The buffer pool manager backing tables created by this catalog
   * @param lock_manager The lock manager in use by the system
   * @param log_manager The log manager in use by the system
   */
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.   创建表, 返回 meta
   * @param txn The transaction in which the table is being created
   * @param table_name The name of the new table, note that all tables beginning with `__` are reserved for the system.
   * @param schema The schema of the new table
   * @param create_table_heap whether to create a table heap for the new table
   * @return A (non-owning) pointer to the metadata for the table
   */
  auto CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema, bool create_table_heap = true)
      -> TableInfo * {
    if (table_names_.count(table_name) != 0) {      // 已经有 "bustub" 表, 则无需创建
      return NULL_TABLE_INFO;
    }

    // Construct the table heap
    std::unique_ptr<TableHeap> table = nullptr;

    // TODO(Wan,chi): This should be refactored into a private ctor for the binder tests, we shouldn't allow nullptr.
    // When create_table_heap == false, it means that we're running binder tests (where no txn will be provided) or
    // we are running shell without buffer pool. We don't need to create TableHeap in this case.
    if (create_table_heap) {
      table = std::make_unique<TableHeap>(bpm_, lock_manager_, log_manager_, txn);
    }

    // Fetch the table OID for the new table
    const auto table_oid = next_table_oid_.fetch_add(1);      // 将参数原子的加入到原有值, 并返回原来的值, 新表id

    // Construct the table information
    auto meta = std::make_unique<TableInfo>(schema, table_name, std::move(table), table_oid);   // 得到表信息 TableInfo
    auto *tmp = meta.get();

    // Update the internal tracking mechanisms
    tables_.emplace(table_oid, std::move(meta));            // 保存 表id - 表信息 TableInfo 映射 
    table_names_.emplace(table_name, table_oid);            // 保存 表名 - id 映射
    index_names_.emplace(table_name, std::unordered_map<std::string, index_oid_t>{}); // 占位, 表名 - index 映射

    return tmp;
  }

  /**
   * Query table metadata by name.       根据名字获取 tableinfo
   * @param table_name The name of the table
   * @return A (non-owning) pointer to the metadata for the table
   */
  auto GetTable(const std::string &table_name) const -> TableInfo * {     // 获取表信息 TableInfo
    auto table_oid = table_names_.find(table_name);   // 获取 表id
    if (table_oid == table_names_.end()) {
      // Table not found
      return NULL_TABLE_INFO;
    }

    auto meta = tables_.find(table_oid->second);    // 获取表信息
    BUSTUB_ASSERT(meta != tables_.end(), "Broken Invariant");

    return (meta->second).get();
  }

  /**
   * Query table metadata by OID      根据id 获取tableinfo
   * @param table_oid The OID of the table to query
   * @return A (non-owning) pointer to the metadata for the table
   */
  auto GetTable(table_oid_t table_oid) const -> TableInfo * {       // 获取表信息 TableInfo
    auto meta = tables_.find(table_oid);
    if (meta == tables_.end()) {
      return NULL_TABLE_INFO;
    }

    return (meta->second).get();
  }

  /**
   * Create a new index, populate existing data of the table and return its metadata.  创建索引, 填充表的现有数据, 返回 meta
   * @param txn The transaction in which the table is being created
   * @param index_name The name of the new index      index 名字
   * @param table_name The name of the table          表名字
   * @param schema The schema of the table            schema
   * @param key_schema The schema of the key          key schema
   * @param key_attrs Key attributes                  key属性
   * @param keysize Size of the key                   key 大小
   * @param hash_function The hash function for the index
   * @return A (non-owning) pointer to the metadata of the new table
   */
  template <class KeyType, class ValueType, class KeyComparator>
  auto CreateIndex(Transaction *txn, const std::string &index_name, const std::string &table_name, const Schema &schema,
                   const Schema &key_schema, const std::vector<uint32_t> &key_attrs, std::size_t keysize,
                   HashFunction<KeyType> hash_function) -> IndexInfo * {
    // Reject the creation request for nonexistent table     要为词表创建index, 所以没有此表, 则返回
    if (table_names_.find(table_name) == table_names_.end()) {
      return NULL_INDEX_INFO;
    }

    // If the table exists, an entry for the table should already be present in index_names_ , 应该有index
    BUSTUB_ASSERT((index_names_.find(table_name) != index_names_.end()), "Broken Invariant");

    // Determine if the requested index already exists for this table   如果已经有了这个index, 则无需创建
    auto &table_indexes = index_names_.find(table_name)->second;
    if (table_indexes.find(index_name) != table_indexes.end()) {
      // The requested index already exists for this table          // 拒绝已经存在的 index_name
      return NULL_INDEX_INFO;
    }

    // Construct index metdata
    auto meta = std::make_unique<IndexMetadata>(index_name, table_name, &schema, key_attrs);    // 获取了 index meta

    // Construct the index, take ownership of metadata
    // TODO(Kyle): We should update the API for CreateIndex
    // to allow specification of the index type itself, not
    // just the key, value, and comparator types

    // TODO(chi): support both hash index and btree index         b+index         得到 b+ 树的 index
    auto index = std::make_unique<BPlusTreeIndex<KeyType, ValueType, KeyComparator>>(std::move(meta), bpm_);

    // Populate the index with all tuples in table heap    计算index
    auto *table_meta = GetTable(table_name);
    auto *heap = table_meta->table_.get();
    for (auto tuple = heap->Begin(txn); tuple != heap->End(); ++tuple) {          // 遍历表的tuple, 将K:V插入 b+ 树; -> 重载过了, 返回tuple
      index->InsertEntry(tuple->KeyFromTuple(schema, key_schema, key_attrs), tuple->GetRid(), txn);  // 将每个tuple 和 3 个入参生成新的 tuple
    }

    // Get the next OID for the new index                         获取index id
    const auto index_oid = next_index_oid_.fetch_add(1);

    // Construct index information; IndexInfo takes ownership of the Index itself    获取 indexinfo
    auto index_info =
        std::make_unique<IndexInfo>(key_schema, index_name, std::move(index), index_oid, table_name, keysize);
    auto *tmp = index_info.get();

    // Update internal tracking
    indexes_.emplace(index_oid, std::move(index_info));             // 保存 索引 id - 索引 信息 index_info 映射
    table_indexes.emplace(index_name, index_oid);                   // 保存 索引 名字 - 索引 id 映射, 保存在了 table_indexes 中

    return tmp;
  }

  /**
   * Get the index `index_name` for table `table_name`.       // 获取 indexname
   * @param index_name The name of the index for which to query
   * @param table_name The name of the table on which to perform query
   * @return A (non-owning) pointer to the metadata for the index
   *   Map table name -> index names -> index identifiers. 
   *  Map table name -> index names -> index identifiers.   获取index id
   * index identifier -> index metadata.
   */
  auto GetIndex(const std::string &index_name, const std::string &table_name) -> IndexInfo * {  // 根据 table_name 获取 index 集合
    auto table = index_names_.find(table_name);       // 根据表名为 key 找 index(name, id)       // 根据 index_name 获取 index id
    if (table == index_names_.end()) {                                                          // 根据 index id 获取 index info
      BUSTUB_ASSERT((table_names_.find(table_name) == table_names_.end()), "Broken Invariant");
      return NULL_INDEX_INFO;
    }

    auto &table_indexes = table->second;

    auto index_meta = table_indexes.find(index_name);     // 根据 index name 为key,  V 是 index id
    if (index_meta == table_indexes.end()) {
      return NULL_INDEX_INFO;
    }

    auto index = indexes_.find(index_meta->second);       // 根据 index id 为key 寻找, index 信息, info
    BUSTUB_ASSERT((index != indexes_.end()), "Broken Invariant");

    return index->second.get();
  }

  /**
   * Get the index `index_name` for table identified by `table_oid`.
   * @param index_name The name of the index for which to query
   * @param table_oid The OID of the table on which to perform query
   * @return A (non-owning) pointer to the metadata for the index
   * table identifier -> table metadata.  获取了 table 名字 table name
   *  Map table name -> index names -> index identifiers.   获取index id    -- index_names_
   * index identifier -> index metadata.                                    --- indexes_
   */
  auto GetIndex(const std::string &index_name, const table_oid_t table_oid) -> IndexInfo * {
    // Locate the table metadata for the specified table OID
    auto table_meta = tables_.find(table_oid);        //  获取 table 信息 Tableinfo
    if (table_meta == tables_.end()) {
      // Table not found
      return NULL_INDEX_INFO;
    }

    return GetIndex(index_name, table_meta->second->name_);
  }

  /**
   * Get the index identifier by index OID.     根据index_oid 获取 index 信息
   * @param index_oid The OID of the index for which to query
   * @return A (non-owning) pointer to the metadata for the index
   */
  auto GetIndex(index_oid_t index_oid) -> IndexInfo * {
    auto index = indexes_.find(index_oid);
    if (index == indexes_.end()) {
      return NULL_INDEX_INFO;
    }

    return index->second.get();
  }

  /**
   * Get all of the indexes for the table identified by `table_name`.         根据表名获取所有 indexs
   * @param table_name The name of the table for which indexes should be retrieved
   * @return A vector of IndexInfo* for each index on the given table, empty vector
   * in the event that the table exists but no indexes have been created for it    返回索引数组
   *  Map table name -> index names -> index identifiers. 
   */
  auto GetTableIndexes(const std::string &table_name) const -> std::vector<IndexInfo *> {
    // Ensure the table exists
    if (table_names_.find(table_name) == table_names_.end()) {
      return std::vector<IndexInfo *>{};
    }

    auto table_indexes = index_names_.find(table_name);       // 以表名为key, 找到 index 集合
    BUSTUB_ASSERT((table_indexes != index_names_.end()), "Broken Invariant");

    std::vector<IndexInfo *> indexes{};
    indexes.reserve(table_indexes->second.size());          // 数组这么大
    for (const auto &index_meta : table_indexes->second) {  // 遍历 index 集合
      auto index = indexes_.find(index_meta.second);              // indexe id 获取 index info
      BUSTUB_ASSERT((index != indexes_.end()), "Broken Invariant");
      indexes.push_back(index->second.get());                     // index info 集合
    }

    return indexes;
  }
  //   /** Map table name -> table identifiers. */
  auto GetTableNames() -> std::vector<std::string> {
    std::vector<std::string> result;
    for (const auto &x : table_names_) {                    // 获取 table 名字 集合
      result.push_back(x.first);
    }
    return result;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /**
   * Map table identifier -> table metadata.
   *
   * NOTE: `tables_` owns all table metadata.
   */
  std::unordered_map<table_oid_t, std::unique_ptr<TableInfo>> tables_;

  /** Map table name -> table identifiers. */
  std::unordered_map<std::string, table_oid_t> table_names_;

  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};

  /**
   * Map index identifier -> index metadata.
   *
   * NOTE: that `indexes_` owns all index metadata.
   */
  std::unordered_map<index_oid_t, std::unique_ptr<IndexInfo>> indexes_;

  /** Map table name -> index names -> index identifiers. */
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>> index_names_;

  /** The next index identifier to be used. */
  std::atomic<index_oid_t> next_index_oid_{0};
};

}  // namespace bustub
