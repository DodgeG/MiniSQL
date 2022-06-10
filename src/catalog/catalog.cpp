#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t ofs = sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t, buf, CATALOG_METADATA_MAGIC_NUM);

  MACH_WRITE_TO(size_t, buf + ofs, table_meta_pages_.size());
  ofs += sizeof(size_t);
  for (auto it = table_meta_pages_.begin(); it != table_meta_pages_.end(); ++it) {
    MACH_WRITE_TO(table_id_t, buf + ofs, it->first);
    ofs += sizeof(table_id_t);
    MACH_WRITE_TO(page_id_t, buf + ofs, it->second);
    ofs += sizeof(page_id_t);
  }

  MACH_WRITE_TO(size_t, buf + ofs, index_meta_pages_.size());
  ofs += sizeof(size_t);
  for (auto it = index_meta_pages_.begin(); it != index_meta_pages_.end(); ++it) {
    MACH_WRITE_TO(index_id_t, buf + ofs, it->second);
    ofs += sizeof(index_id_t);
    MACH_WRITE_TO(page_id_t, buf + ofs, it->first);
    ofs += sizeof(page_id_t);
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  uint32_t MAGIC_NUM = MACH_READ_FROM(uint32_t, buf);
  ASSERT(MAGIC_NUM == CATALOG_METADATA_MAGIC_NUM, "CATALOGMETA FORMAT ERROR!");
  //有问题
  CatalogMeta *Catalog = CatalogMeta::NewInstance(heap);
  uint32_t ofs = sizeof(uint32_t);

  size_t table_size = MACH_READ_FROM(size_t, buf + ofs);
  ofs += sizeof(size_t);
  table_id_t table_id;
  page_id_t page_id;
  for (size_t i = 0; i < table_size; i++) {
    table_id = MACH_READ_FROM(table_id_t, buf + ofs);
    ofs += sizeof(table_id_t);
    page_id = MACH_READ_FROM(page_id_t, buf + ofs);
    ofs += sizeof(page_id_t);

    Catalog->table_meta_pages_.insert({table_id, page_id});
  }
  size_t index_size = MACH_READ_FROM(size_t, buf + ofs);
  ofs += sizeof(size_t);
  index_id_t index_id;
  page_id_t index_page_id;

  for (size_t i = 0; i < index_size; i++) {
    index_id = MACH_READ_FROM(index_id_t, buf + ofs);
    ofs += sizeof(index_id_t);
    index_page_id = MACH_READ_FROM(page_id_t, buf + ofs);
    ofs += sizeof(page_id_t);
    Catalog->index_meta_pages_.insert({index_page_id, index_id});
  }
  return Catalog;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  return sizeof(size_t) + sizeof(size_t) * 2 + table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t)) +
         index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      heap_(new SimpleMemHeap()) {
  if (init == true) {
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
  } else {
    char *buf = reinterpret_cast<char *>(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
    catalog_meta_ = CatalogMeta::DeserializeFrom(buf, heap_);

    //从metadata里取出table_meta_data
    auto table_pages = catalog_meta_->GetTableMetaPages();
    for (auto &page : *table_pages) {
      auto table_page = buffer_pool_manager_->FetchPage(page.second);  //取page
      auto table_info = TableInfo::Create(heap_);                      //创建table_info
      // table_meta
      TableMetadata *table_meta = NULL;
      TableMetadata::DeserializeFrom(table_page->GetData(), table_meta, table_info->GetMemHeap());
      // table_heap
      auto table_heap =
          TableHeap::Create(buffer_pool_manager_, (page_id_t)table_meta->GetFirstPageId(), table_meta->GetSchema(),
                            log_manager_, lock_manager_, table_info->GetMemHeap());

      table_info->Init(table_meta, table_heap);

      table_names_.emplace(std::make_pair(table_meta->GetTableName(), page.first));
      tables_.emplace(std::make_pair(page.first, table_info));

      buffer_pool_manager_->UnpinPage(page.second, false);
    }
    //取出index_meta_data
    auto index_pages = catalog_meta_->GetIndexMetaPages();
    for (auto &page : *index_pages) {
      auto index_page = buffer_pool_manager_->FetchPage(page.second);
      index_page->RLatch();
      auto heap = new SimpleMemHeap();

      IndexMetadata *index_meta = nullptr;
      IndexMetadata::DeserializeFrom(index_page->GetData(), index_meta, heap);

      auto index_info = IndexInfo ::Create(heap);

      index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
      auto table_name = tables_[index_meta->GetTableId()]->GetTableName();

      index_names_[table_name].insert(std::make_pair(index_meta->GetIndexName(), page.first));
      indexes_.emplace(page.first, index_info);
      index_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page.second, false);
    }
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,false);
  }
}

CatalogManager::~CatalogManager() { 
  char *buf = reinterpret_cast<char *>(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());
  catalog_meta_->SerializeTo(buf);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  delete heap_; 
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  if (table_names_.count(table_name) != 0) {
    return DB_TABLE_ALREADY_EXIST;
  }
  // table name id
  table_id_t new_table_id = catalog_meta_->GetNextTableId();
  // table_names_
  table_names_.insert({table_name, new_table_id});

  // table
  page_id_t new_page_id;  //新页
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);

  // table_info分配内存
  table_info = TableInfo::Create(heap_);
  // metadata heap 构造
  TableMetadata *new_table_metadata = TableMetadata::Create(new_table_id, table_name, new_page_id, schema, heap_);
  TableHeap *new_table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);
  table_info->Init(new_table_metadata, new_table_heap);
  // tables_
  tables_.insert({new_table_id, table_info});

  // table_meta_data   catalog_meta序列化
  //数据字节流的位置
  char *buf = reinterpret_cast<char *>(new_page->GetData());
  new_table_metadata->SerializeTo(buf);
  catalog_meta_->table_meta_pages_.insert({new_table_id, new_page_id});
  catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->UnpinPage(new_page_id, true);

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if (table_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = 0;
  for (auto table : table_names_) {
    if (table.first == table_name) {
      table_id = table.second;
      break;
    }
  }
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (tables_.count(table_id) == 0) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (tables_.size() == 0) {
    return DB_FAILED;
  }
  for (auto table : tables_) {
    tables.push_back(table.second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }

  // tables_
  table_id_t new_table_id;
  for (auto table : table_names_) {
    if (table.first == table_name) {
      new_table_id = table.second;
      break;
    }
  }

  table_names_.erase(table_name);

  TableInfo *tmp;
  auto iter = tables_.find(new_table_id);
  tmp = iter->second;
  tables_.erase(new_table_id);
  // free
  page_id_t new_page_id = catalog_meta_->table_meta_pages_[new_table_id];

  tmp->GetTableHeap()->FreeHeap();
  buffer_pool_manager_->DeletePage(new_page_id);

  // catalog_meta_
  catalog_meta_->table_meta_pages_.erase(new_table_id);
  catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  if(table_names_.count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;
    
  std::vector<uint32_t> key_map;
  TableInfo *table_info = tables_.find(table_names_.find(table_name)->second)->second;
  Schema *schema = table_info->GetSchema();
  for(auto key : index_keys){
    uint32_t key_id;
    if(schema->GetColumnIndex(key,key_id) == DB_COLUMN_NAME_NOT_EXIST)
      return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(key_id);
  }

  index_id_t new_index_id;
  auto iter = index_names_.find(table_name);
  if(iter == index_names_.end()){
    new_index_id = catalog_meta_->GetNextIndexId();
    std::unordered_map<std::string,index_id_t> new_index;
    new_index.insert({index_name,new_index_id});
    index_names_.insert({table_name,new_index});
  }else{
    //auto index = iter->second;
    if((iter->second).count(index_name) != 0)
      return DB_INDEX_ALREADY_EXIST;
    new_index_id = catalog_meta_->GetNextIndexId();
    (iter->second).insert({index_name,new_index_id});
  }

  // new_page
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  // heap
  auto heap = new SimpleMemHeap();
  index_info = IndexInfo::Create(heap);

  IndexMetadata *new_index_meta =
      IndexMetadata::Create(new_index_id, index_name, table_names_.find(table_name)->second, key_map, heap);
  // init
  index_info->Init(new_index_meta, table_info, buffer_pool_manager_);
  // indexes_
  indexes_.insert({new_index_id, index_info});
  //序列化
  new_index_meta->SerializeTo(reinterpret_cast<char *>(new_page->GetData()));
  catalog_meta_->index_meta_pages_.insert({new_index_id, new_page_id});
  catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->UnpinPage(new_page_id, true);

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto iter1 = index_names_.find(table_name);
  if(iter1 == index_names_.end())
    return DB_INDEX_NOT_FOUND;
  auto iter2 = (iter1->second).find(index_name);
  if(iter2 == (iter1->second).end())
    return DB_INDEX_NOT_FOUND;

  index_id_t new_index_id = iter2->second;
  index_info = indexes_.find(new_index_id)->second;

  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (index_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }
  auto iter = index_names_.find(table_name);
  int count = (iter->second).count(index_name);
  if (count == 0)
    return DB_INDEX_NOT_FOUND;
  else {
    index_id_t deleted = ((iter->second).find(index_name))->second;
    if ((iter->second).size() == 1)
      index_names_.erase(table_name);
    else
      (iter->second).erase(index_name);

    // tables_
    IndexInfo *tmp;
    tmp = (indexes_.find(deleted))->second;
    indexes_.erase(deleted);

    Index *bpindex = tmp->GetIndex();
    bpindex->Destroy();

    page_id_t new_page_id = catalog_meta_->index_meta_pages_[deleted];
    
    buffer_pool_manager_->DeletePage(new_page_id);

    // catalog_meta_
    catalog_meta_->index_meta_pages_.erase(deleted);
    catalog_meta_->SerializeTo(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());

    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
    return DB_SUCCESS;
  }
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto iter = index_names_.find(table_name);
  if(iter == index_names_.end())
    return DB_INDEX_NOT_FOUND;
  
  auto index_map = iter->second;
  for(auto ind : index_map){
    IndexInfo *tmp = indexes_.find(ind.second)->second;
    indexes.push_back(tmp);
  }
  return DB_SUCCESS;
}
