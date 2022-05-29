#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t ofs = sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM);
  MACH_WRITE_TO(size_t, buf + ofs, columns_.size());
  ofs += sizeof(size_t);
  for (auto col : columns_)
    ofs += col->SerializeTo(buf + ofs);
  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t ofs = sizeof(uint32_t) + sizeof(size_t);
  for (auto col : columns_)
    ofs += col->GetSerializedSize();
  return ofs;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  uint32_t MAGIC_NUM = MACH_READ_FROM(uint32_t, buf);
  ASSERT(MAGIC_NUM == SCHEMA_MAGIC_NUM, "SCHEMA FORMAT ERROR!");
  uint32_t ofs = sizeof(uint32_t);
  size_t size = MACH_READ_FROM(size_t, buf + ofs);
  ofs += sizeof(size_t);
  std::vector<Column *> columns;
  for (size_t i = 0; i < size; ++i) {
    Column *column;
    ofs += Column::DeserializeFrom(buf + ofs, column, heap);
    columns.emplace_back(column);
  }
  schema = ALLOC_P(heap, Schema)(columns);
  return ofs;
}