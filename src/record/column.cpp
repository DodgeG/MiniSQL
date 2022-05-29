#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  //ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  //ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  uint32_t ofs = sizeof(size_t);
  MACH_WRITE_TO(uint32_t, buf, COLUMN_MAGIC_NUM);
  MACH_WRITE_TO(size_t, buf + ofs, name_.size());
  ofs += sizeof(size_t);
  MACH_WRITE_STRING(buf + ofs, name_);
  ofs += name_.size();
  MACH_WRITE_TO(TypeId, buf + ofs, type_);
  ofs += sizeof(TypeId);
  MACH_WRITE_TO(uint32_t, buf + ofs, len_);
  ofs += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t, buf + ofs, table_ind_);
  ofs += sizeof(uint32_t);
  MACH_WRITE_TO(bool, buf + ofs, nullable_);
  ofs += sizeof(bool);
  MACH_WRITE_TO(bool, buf + ofs, unique_);
  ofs += sizeof(bool);
  return ofs;
}

uint32_t Column::GetSerializedSize() const {
  return sizeof(uint32_t) + sizeof(size_t) + name_.size() + sizeof(TypeId) + sizeof(uint32_t) * 2 + sizeof(bool) * 2;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  uint32_t MAGIC_NUM = MACH_READ_FROM(uint32_t, buf);
  ASSERT(MAGIC_NUM == COLUMN_MAGIC_NUM, "COLUMN FORMAT ERROR!");
  uint32_t ofs = sizeof(size_t);
  size_t size = MACH_READ_FROM(size_t, buf + ofs);
  ofs += sizeof(size_t);
  char ch[size + 1];
  memcpy(ch, buf + ofs, size);
  ch[size] = '\0';
  std::string column_name(ch);
  ofs += size;
  TypeId type = MACH_READ_FROM(TypeId, buf + ofs);
  ofs += sizeof(TypeId);
  uint32_t length = MACH_READ_FROM(uint32_t, buf + ofs);
  ofs += sizeof(uint32_t);
  uint32_t index = MACH_READ_FROM(uint32_t, buf + ofs);
  ofs += sizeof(uint32_t);
  bool nullable = MACH_READ_FROM(bool, buf + ofs);
  ofs += sizeof(bool);
  bool unique = MACH_READ_FROM(bool, buf + ofs);
  ofs += sizeof(bool);
  column = ALLOC_COLUMN((*heap))(column_name, type, length, index, nullable, unique);
  return ofs;
}
