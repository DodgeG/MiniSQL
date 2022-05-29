#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t ofs=sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t, buf, INDEX_METADATA_MAGIC_NUM);
  MACH_WRITE_TO(index_id_t, buf+ofs, index_id_);
  ofs+=sizeof(index_id_t);
  MACH_WRITE_TO(size_t, buf+ofs, index_name_.size());
  ofs+=sizeof(size_t);
  MACH_WRITE_STRING(buf+ofs, index_name_);
  ofs+=index_name_.size();
  MACH_WRITE_TO(table_id_t, buf+ofs, table_id_);
  ofs+=sizeof(table_id_t);
  
  MACH_WRITE_TO(size_t,buf+ofs,key_map_.size());
  ofs+=sizeof(size_t);
  for (auto key : key_map_){
    MACH_WRITE_TO(uint32_t,buf+ofs,key);
    ofs+=sizeof(uint32_t);
  }
  
  return ofs;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  uint32_t ofs=sizeof(uint32_t)+sizeof(index_id_t)+sizeof(size_t)+index_name_.size()+sizeof(table_id_t)+sizeof(size_t)+key_map_.size()*sizeof(uint32_t);

  return ofs;
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  uint32_t MAGIC_NUM = MACH_READ_FROM(uint32_t, buf);
  ASSERT(MAGIC_NUM == INDEX_METADATA_MAGIC_NUM, "INDEX FORMAT ERROR!");
  uint32_t ofs = sizeof(uint32_t); 
  index_id_t index_id = MACH_READ_FROM(index_id_t, buf+ofs);
  ofs+=sizeof(index_id_t);
  size_t size = MACH_READ_FROM(size_t,buf+ofs);
  ofs+=sizeof(size_t);
  char ch[size+1];
  memcpy(ch, buf+ofs, size);
  ch[size]='\0';
  std::string index_name(ch);
  ofs+=size;
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf+ofs);
  ofs+=sizeof(table_id_t);
  uint32_t sizeV = MACH_READ_FROM(size_t,buf+ofs);
  ofs+=sizeof(size_t);
  //Vector
  vector<uint32_t> key_map;
  for (uint32_t i=0;i<sizeV;i++){
    uint32_t temp = MACH_READ_FROM(uint32_t,buf+ofs);
    ofs+=sizeof(uint32_t);
    key_map.push_back(temp);
  }
  
  index_meta = Create(index_id,index_name,table_id,key_map,heap);
  return ofs;
}