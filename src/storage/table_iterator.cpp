#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap_,Row* row_,Txn *txn):row_(row_),table_heap_(table_heap_),txn(txn) {}

TableIterator::TableIterator(){}

TableIterator::TableIterator(TableHeap *table_heap_, RowId row_id, Txn *txn) {
  this->row_ = new Row(row_id);
  this->table_heap_ = table_heap_;
  table_heap_->GetTuple(row_, txn);
}

TableIterator::TableIterator(const TableIterator &other) {
  this->row_ = new Row(*other.row_);
  this->table_heap_ = other.table_heap_;
}

TableIterator::~TableIterator() {
  delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return this->row_->GetRowId() == itr.row_->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(this->row_->GetRowId() == itr.row_->GetRowId());
}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  row_ = new Row(*itr.row_);
  table_heap_ = itr.table_heap_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  RowId rid = row_->GetRowId();
  RowId next_rid;
  page_id_t page_id = rid.GetPageId();
  if(page_id == INVALID_PAGE_ID) {
    this->row_->SetRowId(INVALID_ROWID);
    return *this;
  }
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
  ASSERT(page != nullptr, "page is nullptr");
  if (page->GetNextTupleRid(rid, &next_rid)) {
    row_->SetRowId(next_rid);
    table_heap_->GetTuple(row_, txn);
    table_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
    return *this;
  } else {
    page_id_t next_page_id = page->GetNextPageId();
    while(next_page_id != INVALID_PAGE_ID) {
      page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      ASSERT(page != nullptr, "page is nullptr");
      if (page->GetFirstTupleRid(&next_rid)) {
        row_->SetRowId(next_rid);
        table_heap_->GetTuple(row_, txn);
        table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
        return *this;
      }else{
        next_page_id = page->GetNextPageId();
        table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
      }
    }
    this->row_->SetRowId(INVALID_ROWID);
    return *this;
  }
}

// iter++
TableIterator TableIterator::operator++(int) { 
  TableIterator temp(*this);
  ++(*this);
  return (const TableIterator)temp;
}
