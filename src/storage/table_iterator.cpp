#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {

}

TableIterator::TableIterator(TableHeap *tableheap, RowId rid, Transaction *txn)
    : tableheap_(tableheap), row_(new Row(rid)), txn_(txn) {
  if (rid.GetPageId() != INVALID_PAGE_ID) {
    tableheap_->GetTuple(row_, txn_);
  }
}

TableIterator::TableIterator(const TableIterator &other) {
  tableheap_ = other.tableheap_;
  row_ = other.row_;
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {
  if (row_ != nullptr) delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return tableheap_ == itr.tableheap_ && row_->GetRowId() == (itr.row_)->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(tableheap_ == itr.tableheap_ && row_->GetRowId() == (itr.row_)->GetRowId());
}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator++() {
  BufferPoolManager *buffer_pool_manager = tableheap_->buffer_pool_manager_;
  RowId cur_rid = row_->GetRowId(), next_rid;
  while (cur_rid.GetPageId() != INVALID_PAGE_ID && next_rid.GetPageId() == INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(cur_rid.GetPageId()));
    page->RLatch();
    bool status = page->GetNextTupleRid(cur_rid, &next_rid);
    page->RUnlatch();
    if (status) {
      buffer_pool_manager->UnpinPage(cur_rid.GetPageId(), false);
      break;
    }
    page->RLatch();
    page_id_t next_page_id = page->GetNextPageId();
    page->RUnlatch();
    buffer_pool_manager->UnpinPage(cur_rid.GetPageId(), false);
    cur_rid = RowId(next_page_id, -1);
  }
  delete row_;
  row_ = new Row(next_rid);
  if (next_rid.GetPageId() != INVALID_PAGE_ID) {
    tableheap_->GetTuple(row_, txn_);
  }
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator itr = TableIterator(*this);
  ++(*this);
  return TableIterator(itr);
}
