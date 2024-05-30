#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  table_heap_ = table_heap;
  rid_ = rid;
  txn_ = txn;
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  rid_ = other.rid_;
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {
  table_heap_ = nullptr;
  rid_ = RowId();
  txn_ = nullptr;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return this->rid_ == itr.rid_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
  if (page == nullptr) {
    table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);
    Row *row = new Row(INVALID_ROWID);
    return *row;
  }
  Row *row = new Row(rid_);
  bool res = page->GetTuple(row, table_heap_->schema_, txn_, table_heap_->lock_manager_);
  if(res){
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return *row;
  }else{
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    Row *row = new Row(INVALID_ROWID);
    return *row;
  }
}

Row *TableIterator::operator->() {
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
  if (page == nullptr) {
    table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);
    return nullptr;
  }
  Row *row = new Row(rid_);
  bool res = page->GetTuple(row, table_heap_->schema_, txn_, table_heap_->lock_manager_);
  if(res){
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return row;
  }else{
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return nullptr;
  }
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_ = itr.table_heap_;
  rid_ = itr.rid_;
  txn_ = itr.txn_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
  if (page == nullptr) {
    table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);
    this->table_heap_ = nullptr;
    this->rid_ = RowId();
    this->txn_ = nullptr;
    return *this;
  }
  bool res = page->GetNextTupleRid(rid_, &rid_);
  if(res){
    // 不用翻下一页
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return *this;
  }else{
    // 翻下一页
    page_id_t next_page_id = page->GetNextPageId();
    if(next_page_id == INVALID_PAGE_ID){
      this->table_heap_ = nullptr;
      this->rid_ = RowId();
      this->txn_ = nullptr;
      return *this;
    }else{
      auto next_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      page = next_page;
      page->GetFirstTupleRid(&rid_);
    }
  }
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return *this;  
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator p(*this);
  ++(*this);
  return TableIterator{p};
}
