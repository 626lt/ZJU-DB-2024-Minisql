#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  uint32_t size = row.GetSerializedSize(schema_);
  if(size >= PAGE_SIZE) return false;
  ASSERT(first_page_id_ != INVALID_PAGE_ID, "Invalid first page id.");
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));
  if(page == nullptr) return false;
  while(!page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page_id_t next_page_id = page->GetNextPageId();
    if(next_page_id != INVALID_PAGE_ID){
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    }else{
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if(new_page == nullptr) return false;
      new_page->Init(next_page_id, page->GetTablePageId(), log_manager_, txn);
      new_page->SetNextPageId(INVALID_PAGE_ID);
      page->SetNextPageId(next_page_id);
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      page = new_page;
    }
  }
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
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

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  Row old_row(rid);
  if(page->GetTuple(&old_row, schema_, txn, lock_manager_)){
    uint32_t res = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
    if(res == 0){
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      return true;
    }else if(res == 1 || res == 2){
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      return false;
    }else if (res == 3){
      if(InsertTuple(row, txn)){
        ApplyDelete(rid, txn);
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        return true;
      }
    }
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Step2: Delete the tuple from the page.
  page->ApplyDelete(rid, txn, log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { 
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  assert(page != nullptr);
  if(page->GetTuple(row, schema_, txn, lock_manager_)){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return true;
  }
  return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
//  */
TableIterator TableHeap::Begin(Txn *txn) { return TableIterator(nullptr, RowId(), nullptr); }

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(nullptr, RowId(), nullptr); }
