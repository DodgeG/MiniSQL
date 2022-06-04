#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  MACH_WRITE_TO(int64_t, buf, rid_.Get());
  uint32_t ofs = sizeof(int64_t);
  MACH_WRITE_TO(size_t, buf + ofs, fields_.size());
  ofs += sizeof(size_t);
  // unsigned char bitmap[(fields_.size() + 7) / 8] = {}; // malloc in stack
  unsigned char *bitmap = new unsigned char[(fields_.size() + 7) / 8];
  for (size_t i = 0; i < fields_.size(); ++i)
    if (fields_[i]->IsNull()) bitmap[i / 8] |= 1u << (i % 8);
  memcpy(buf + ofs, bitmap, (fields_.size() + 7) / 8);
  ofs += (fields_.size() + 7) / 8;
  for (size_t i = 0; i < fields_.size(); ++i) {
    // MACH_WRITE_TO(TypeId, buf + ofs, fields_[i]->GetTypeId());
    // ofs += sizeof(TypeId);
    ofs += fields_[i]->SerializeTo(buf + ofs);
  }
  delete [] bitmap;
  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  int64_t rid = MACH_READ_FROM(int64_t, buf);
  rid_ = RowId(rid);
  uint32_t ofs = sizeof(int64_t);
  size_t size = MACH_READ_FROM(size_t, buf + ofs);
  ofs += sizeof(size_t);
  // unsigned char bitmap[(size + 7) / 8] = {}; // malloc in stack
  ASSERT(fields_.size() == 0, "Row is not empty");
  unsigned char *bitmap = new unsigned char[(size + 7) / 8];
  memcpy(bitmap, buf + ofs, (size + 7) / 8);
  ofs += (size + 7) / 8;
  for (size_t i = 0; i < size; ++i) {
    // TypeId type = MACH_READ_FROM(TypeId, buf + ofs);
    TypeId type = (schema->GetColumn(i))->GetType();
    // ofs += sizeof(TypeId);
    Field *field = nullptr;
    ofs += Field::DeserializeFrom(buf + ofs, type, &field, (bitmap[i / 8] >> i) & 1, heap_);
    fields_.emplace_back(field);
  }
  delete [] bitmap;
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t ofs = sizeof(int64_t) + sizeof(size_t) + (fields_.size() + 7) / 8;
  // uint32_t ofs = sizeof(int64_t) + sizeof(size_t) + (fields_.size() + 7) / 8 + fields_.size() * sizeof(TypeId);
  for (size_t i = 0; i < fields_.size(); ++i)
    ofs += fields_[i]->GetSerializedSize();
  return ofs;
}
