#include "storage/table_heap.h"

/*
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  page_id_t last_page_id;
  for (page_id_t page_id = first_page_id_; page_id != INVALID_PAGE_ID; ) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    page->WLatch();
    bool status = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    page_id_t next_page_id = page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page_id, status);
    if (status) return true;
    last_page_id = page_id;
    page_id = next_page_id;
  }
  page_id_t new_page_id;
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id));
  last_page->WLatch();
  last_page->SetNextPageId(new_page_id);
  last_page->WUnlatch();
  new_page->WLatch();
  new_page->Init(new_page_id, last_page_id, log_manager_, txn);
  bool status = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  new_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  buffer_pool_manager_->UnpinPage(last_page_id, true);
  return status;
}
*/

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  page->WLatch();
  bool status = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  if (status) {
    buffer_pool_manager_->UnpinPage(first_page_id_, true);
    return true;
  }
  page_id_t new_page_id;
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  page->WLatch();
  page->SetPrevPageId(new_page_id);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, true);
  new_page->WLatch();
  new_page->Init(new_page_id, INVALID_PAGE_ID, log_manager_, txn);
  new_page->SetNextPageId(first_page_id_);
  status = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  new_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  first_page_id_ = new_page_id;
  return status;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark
  Row *old_row = new Row(rid);
  row.SetRowId(rid);
  //GetTuple(old_row, txn);
  page->WLatch();
  int status = page->UpdateTuple(row, old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  delete old_row;
  if (status < 0) {
    return MarkDelete(rid, txn) && InsertTuple(row, txn);
  }
  return status;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return;
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  for (page_id_t page_id = first_page_id_; page_id != INVALID_PAGE_ID; ) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    page_id_t next_page_id = page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
    page_id = next_page_id;
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage((row->GetRowId()).GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, get the tuple.
  page->RLatch();
  page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  RowId first_rid;
  for (page_id_t page_id = first_page_id_; page_id != INVALID_PAGE_ID; ) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    page->RLatch();
    bool status = page->GetFirstTupleRid(&first_rid);
    page->RUnlatch();
    page_id_t next_page_id = page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page_id, status);
    if (status) return TableIterator(this, first_rid, txn);
    page_id = next_page_id;
  }
  return TableIterator(this, first_rid, txn);
}

TableIterator TableHeap::End() {
  return TableIterator(this, RowId(), nullptr);
}
