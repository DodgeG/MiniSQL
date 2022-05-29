#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t ofs = sizeof(TABLE_METADATA_MAGIC_NUM);
  MACH_WRITE_TO(uint32_t, buf, TABLE_METADATA_MAGIC_NUM);
  MACH_WRITE_TO(table_id_t, buf+ofs, table_id_);
  ofs+=sizeof(table_id_t);
  MACH_WRITE_TO(size_t, buf+ofs, table_name_.size());
  ofs+=sizeof(size_t);
  MACH_WRITE_STRING(buf+ofs, table_name_);
  ofs+=table_name_.size();
  MACH_WRITE_TO(page_id_t,buf+ofs,root_page_id_);
  ofs+=sizeof(page_id_t);
  (*schema_).SerializeTo(buf+ofs);
  ofs+=(*schema_).GetSerializedSize();
  
  return ofs;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(TABLE_METADATA_MAGIC_NUM)+sizeof(size_t)+table_name_.size()+sizeof(table_id_t)+sizeof(page_id_t)+(*schema_).GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t MAGIC_NUM = MACH_READ_FROM(uint32_t, buf);
  ASSERT(TABLE_METADATA_MAGIC_NUM == MAGIC_NUM,"TABLE FORMAT ERROR!!");
  uint32_t ofs = sizeof(TABLE_METADATA_MAGIC_NUM);
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf+ofs);
  ofs += sizeof(table_id_t);
  size_t size = MACH_READ_FROM(size_t, buf+ofs);
  ofs+=sizeof(size_t);
  char ch[size+1];
  memcpy(ch, buf+ofs, size);
  ch[size]='\0';
  std::string table_name(ch);
  ofs+=size;
  page_id_t root_page_id = MACH_READ_FROM(page_id_t, buf+ofs);
  ofs+=sizeof(page_id_t);
  Schema *schema;
  ofs+=Schema::DeserializeFrom(buf+ofs, schema, heap);
  //
  table_meta = ALLOC_P(heap,TableMetadata)(table_id, table_name, root_page_id, schema);
  return ofs;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
