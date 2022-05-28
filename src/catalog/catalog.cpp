#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t ofs=sizeof(size_t);
  MACH_WRITE_TO(uint32_t, buf, CATALOG_METADATA_MAGIC_NUM);

  MACH_WRITE_TO(size_t, buf+ofs, table_meta_pages_.size());
  ofs+=sizeof(size_t);
  for (auto it = table_meta_pages_.begin(); it != table_meta_pages_.end(); ++it){
    MACH_WRITE_TO(table_id_t, buf+ofs, it->first);
    ofs+=sizeof(table_id_t);
    MACH_WRITE_TO(page_id_t, buf+ofs, it->second);
    ofs+=sizeof(page_id_t);
  }

  MACH_WRITE_TO(size_t, buf+ofs, index_meta_pages_.size());
  ofs+=sizeof(size_t);
  for (auto it = index_meta_pages_.begin(); it != index_meta_pages_.end(); ++it){
   
    MACH_WRITE_TO(index_id_t, buf+ofs, it->second);
    ofs+=sizeof(index_id_t);
    MACH_WRITE_TO(page_id_t, buf+ofs, it->first);
    ofs+=sizeof(page_id_t);
  }
  return ofs;
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  uint32_t MAGIC_NUM = MACH_READ_FROM(uint32_t, buf);
  ASSERT(MAGIC_NUM == CATALOG_METADATA_MAGIC_NUM, "CATALOGMETA FORMAT ERROR!");
  //有问题
  CatalogMeta *Catalog = CatalogMeta::NewInstance(heap);
  uint32_t ofs = sizeof(size_t);

  size_t table_size = MACH_READ_FROM(size_t, buf+ofs);
  ofs+=sizeof(size_t);
  table_id_t table_id;
  page_id_t page_id;
  for (int i=0;i<table_size;i++){
    table_id = MACH_READ_FROM(table_id_t,buf+ofs);
    ofs+=sizeof(table_id_t);
    page_id = MACH_READ_FROM(page_id_t, buf+ofs);
    ofs+=sizeof(page_id_t);
    Catalog->table_meta_pages_.emplace(pair<table_id_t,page_id_t>(table_id,page_id));

  }
  size_t index_size = MACH_READ_FROM(size_t, buf+ofs);
  ofs+=sizeof(size_t);
  index_id_t index_id;
  page_id_t index_page_id;

  for (int i=0;i<index_size;i++){
    index_id = MACH_READ_FROM(table_id_t,buf+ofs);
    ofs+=sizeof(index_id_t);
    index_page_id = MACH_READ_FROM(page_id_t, buf+ofs);
    ofs+=sizeof(page_id_t);
    Catalog->index_meta_pages_.emplace(pair<index_id_t,page_id_t>(index_id,index_page_id));
  }
  return Catalog;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  return sizeof(size_t)+sizeof(size_t)*2+table_meta_pages_.size()*(sizeof(table_id_t)+sizeof(page_id_t))+index_meta_pages_.size()*(sizeof(index_id_t)+sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}