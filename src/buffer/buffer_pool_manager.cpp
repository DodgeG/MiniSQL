#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  auto iter = page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  if (iter != page_table_.end()) {
    pages_[(*iter).second].pin_count_++;
    replacer_->Pin((*iter).second);
    return pages_ + (*iter).second;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t R = INVALID_FRAME_ID;
  if (!free_list_.empty()) {
    R = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&R)) return nullptr;
  if (R == INVALID_FRAME_ID) return nullptr;
  // 2.     If R is dirty, write it back to the disk.
  if (pages_[R].is_dirty_) FlushPage(pages_[R].page_id_);
  // 3.     Delete R from the page table and insert P.
  if (pages_[R].page_id_ != INVALID_FRAME_ID) page_table_.erase(pages_[R].page_id_);
  page_table_[page_id] = R;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[R].page_id_ = page_id;
  pages_[R].pin_count_ = 1;
  pages_[R].is_dirty_ = false;
  replacer_->Pin(R);
  disk_manager_->ReadPage(page_id, pages_[R].data_);
  return pages_ + R;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t P = INVALID_FRAME_ID;
  if (!free_list_.empty()) {
    P = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&P)) return nullptr;
  if (P == INVALID_FRAME_ID) return nullptr;
  if (pages_[P].is_dirty_) FlushPage(pages_[P].page_id_);
  // printf("NewPage: pages_[%d].page_id_ = %d\n", P, pages_[P].page_id_);
  if (pages_[P].page_id_ != INVALID_FRAME_ID) page_table_.erase(pages_[P].page_id_);

  page_id = AllocatePage();
  page_table_[page_id] = P;

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  pages_[P].page_id_ = page_id;
  pages_[P].pin_count_ = 1;
  pages_[P].is_dirty_ = true; // ???
  replacer_->Pin(P);
  pages_[P].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[P].data_);
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return pages_ + P;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  auto iter = page_table_.find(page_id);
  // 1.   If P does not exist, return true.
  if (iter == page_table_.end()) return true;
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t P = (*iter).second;
  if (pages_[P].pin_count_) return false;
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  // reset P's metadata ?
  pages_[P].ResetMemory();
  pages_[P].page_id_ = INVALID_PAGE_ID;
  pages_[P].pin_count_ = 0;
  pages_[P].is_dirty_ = false;
  page_table_.erase(iter);
  free_list_.emplace_back(P);
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) return false;
  frame_id_t P = (*iter).second;
  if(pages_[P].pin_count_>0){
    pages_[P].pin_count_--;
    replacer_->Unpin(P);
  }
  if (is_dirty) pages_[P].is_dirty_ = true;
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (page_table_.find(page_id) == page_table_.end()) return false;
  frame_id_t P = page_table_[page_id];
  if (pages_[P].is_dirty_) disk_manager_->WritePage(page_id, pages_[P].GetData());
  pages_[P].is_dirty_ = false;
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}