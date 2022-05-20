#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  const size_t MaxSize = GetMaxSupportedSize();
  if (page_allocated_ == MaxSize) return false;
  page_offset = next_free_page_++;
  SetPage(page_offset, 1);
  while (page_allocated_ < MaxSize && !IsPageFree(page_allocated_)) ++page_allocated_;
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (IsPageFree(page_offset)) return false;
  SetPage(page_offset, 0);
  page_allocated_--;
  if (page_offset < next_free_page_) next_free_page_ = page_offset;
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return !((bytes[byte_index] >> bit_index & 1));
}

template<size_t PageSize>
void BitmapPage<PageSize>::SetPage(uint32_t page_offset, uint8_t s) {
  SetPage(page_offset / 8, page_offset % 8, s);
}

template<size_t PageSize>
void BitmapPage<PageSize>::SetPage(uint32_t byte_index, uint8_t bit_index, uint8_t s) {
  bytes[byte_index] = (bytes[byte_index] & ~(1u << bit_index)) | (s << bit_index);
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;