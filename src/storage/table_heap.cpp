#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  if(row.GetSerializedSize(schema_) >= PAGE_SIZE) return false;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));
  if (page == nullptr) {
    return false;
  }
  while(!page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page_id_t next_page_id = page->GetNextPageId();
    if(next_page_id == INVALID_PAGE_ID){
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      page->SetNextPageId(next_page_id);
      new_page->Init(next_page_id, page->GetTablePageId(), log_manager_, txn);
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      page = new_page;
    }else{
      auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      page = next_page;
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
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  Row old_row(rid);
  // Update the tuple in the page.
  int update_res = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if(update_res != 0){
    if(update_res == 3){
      if(InsertTuple(row, txn)){
        MarkDelete(rid, txn);
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        return true;
      }else{
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        return false;
      }
    }else{
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      return false;
    }
  }
  else{
    row.SetRowId(rid);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }
}
/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page != nullptr, "The page could not be found.");
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
  page_id_t page_id = row->GetRowId().GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    return false;
  }
  bool result = page->GetTuple(row, schema_, txn, lock_manager_);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return result;
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
 */
TableIterator TableHeap::Begin(Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));
  if (page == nullptr) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return TableIterator(this, RowId(), nullptr);
  }
  RowId rid;
  if(page->GetFirstTupleRid(&rid)){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return TableIterator(this, rid, txn);
  }else{
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return TableIterator(this, RowId(), nullptr);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(nullptr, RowId(), nullptr); }
