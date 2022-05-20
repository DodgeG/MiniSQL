#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  capacity = num_pages;
  size = 0;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (!size) return false;
  while (replace_pool.front().second != uid[replace_pool.front().first]) replace_pool.pop();
  uid.erase(*frame_id = replace_pool.front().first);
  replace_pool.pop();
  size--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  auto iter = uid.find(frame_id);
  if (iter == uid.end() || !((*iter).second & 1)) return;
  size--;
  ++uid[frame_id];
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  auto iter = uid.find(frame_id);
  if (iter != uid.end() && ((*iter).second & 1)) return;
  size++;
  replace_pool.push(make_pair(frame_id, ++uid[frame_id]));
}

size_t LRUReplacer::Size() {
  return size;
}