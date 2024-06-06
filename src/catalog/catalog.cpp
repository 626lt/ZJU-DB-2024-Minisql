#include "catalog/catalog.h"
#include "page/index_roots_page.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  //ASSERT(false, "Not Implemented yet");
  return sizeof(uint32_t)*3+(sizeof(table_id_t)+sizeof(page_id_t))*table_meta_pages_.size()+(sizeof(index_id_t)+sizeof(page_id_t))*index_meta_pages_.size();
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//    ASSERT(false, "Not Implemented yet");
  if(init)
  {
    catalog_meta_ = CatalogMeta::NewInstance();
    FlushCatalogMetaPage();
    next_index_id_=0;
    next_table_id_=0;
  }
  else
  {
    Page *catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    char* buf=catalog_meta_page->GetData();
    catalog_meta_=CatalogMeta::DeserializeFrom(buf);
    buffer_pool_manager_->UnpinPage(catalog_meta_page->GetPageId(), false);
    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();
    for (auto tableID_pageID : catalog_meta_->table_meta_pages_) {
      table_id_t table_id = tableID_pageID.first;
      page_id_t page_id = tableID_pageID.second;
      if (page_id == INVALID_PAGE_ID) {
        break;
      }
      LoadTable(table_id, page_id);
    }
    for (auto indexID_pageID : catalog_meta_->index_meta_pages_) {
      table_id_t index_id = indexID_pageID.first;
      page_id_t page_id = indexID_pageID.second;
      if (page_id == INVALID_PAGE_ID) {
        break;
      }
      LoadIndex(index_id, page_id);
    }
   }
  
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name)!=table_names_.end())
  {
    return DB_TABLE_ALREADY_EXIST;
  }
  page_id_t page_id;
  table_id_t table_id = catalog_meta_->GetNextTableId();
  page_id_t table_heap_root_id;
  TablePage* table_heap_root_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(table_heap_root_id));
  table_heap_root_page->Init(table_heap_root_id, INVALID_PAGE_ID, log_manager_, txn);
  auto page=buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->table_meta_pages_.emplace(table_id,page_id);
  Schema *tmp_schema = Schema::DeepCopySchema(schema);
  TableHeap *table = TableHeap::Create(buffer_pool_manager_,table_heap_root_id,tmp_schema,log_manager_,lock_manager_);
  if(page==nullptr)
  {
    return DB_FAILED;
  }
  auto table_meta=TableMetadata::Create(table_id,table_name,table_heap_root_id,tmp_schema);
  table_meta->SerializeTo(page->GetData());
  TableInfo* t_info = TableInfo::Create();
  t_info->Init(table_meta,table);
  table_names_.emplace(table_name,table_id);
  tables_.emplace(table_id,t_info);
  table_info=t_info;
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->UnpinPage(table_heap_root_id, true);
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto table=table_names_.find(table_name);
  if(table==table_names_.end())
  {
    table_info = nullptr;
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    table_info=tables_.find(table->second)->second;
    return DB_SUCCESS;
  }
  //return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  if (tables_.empty()) return DB_TABLE_NOT_EXIST;
  for(auto iter : tables_)
  {
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
  //return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  TableInfo* table_info=nullptr;
  if(GetTable(table_name,table_info)!=DB_SUCCESS)
  {
    return DB_TABLE_NOT_EXIST;
  }
  if(index_names_[table_name].find(index_name)!=index_names_[table_name].end())
  {
    return DB_INDEX_ALREADY_EXIST;
  }
  auto table_id=table_info->GetTableId();
  next_index_id_++;
  std::vector<uint32_t> key_map;
  for (auto key : index_keys) {
    index_id_t id;
    if (table_info->GetSchema()->GetColumnIndex(key, id) == DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(id);
  }
  index_id_t index_id = catalog_meta_->GetNextIndexId();
  page_id_t page_id;
  auto page=buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->index_meta_pages_.emplace(index_id,page_id);
  auto index_meta=IndexMetadata::Create(index_id,index_name,table_id,key_map);
  index_meta->SerializeTo(page->GetData());
  IndexInfo* i_info = IndexInfo::Create();
  i_info->Init(index_meta,table_info,buffer_pool_manager_);
  if(index_names_.find(table_name)==index_names_.end())
  {
    std::unordered_map<std::string, index_id_t> map;
    map.emplace(index_name,index_id);
    index_names_.emplace(table_name,map);
  }
  else
  {
    index_names_.find(table_name)->second.emplace(index_name,index_id);
  }
  indexes_.emplace(index_id,i_info);
  buffer_pool_manager_->UnpinPage(page_id, true);
  index_info=i_info;
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  auto table=index_names_.find(table_name);
  if(table==index_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  auto index=table->second.find(index_name);
  if(index==table->second.end())
  {
    return DB_INDEX_NOT_FOUND;
  }
  index_info=indexes_.find(index->second)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  auto table=index_names_.find(table_name);
  if(table==index_names_.end())
  {
    return DB_INDEX_NOT_FOUND;
  }
  auto index=table->second;
  for(auto iter : index)
  {
    indexes.push_back(indexes_.find(iter.second)->second);
  }
  if (indexes.empty()) return DB_INDEX_NOT_FOUND;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  TableInfo* table_info = nullptr;
  if(GetTable(table_name, table_info) != DB_SUCCESS){
    return DB_TABLE_NOT_EXIST;
  }
  auto table_index=index_names_.find(table_name);
  table_id_t table_id = table_info->GetTableId();
  page_id_t page_id = table_info->GetRootPageId();
  table_names_.erase(table_name);
  tables_.erase(table_id);
  buffer_pool_manager_->DeletePage(page_id);
  catalog_meta_->table_meta_pages_.erase(table_id);
  FlushCatalogMetaPage();
  delete table_info;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet")
  IndexInfo* index_info = nullptr;
  dberr_t res = GetIndex(table_name, index_name, index_info);
  if(res != DB_SUCCESS){
    return res;
  }
  index_id_t index_id = index_names_.find(table_name)->second.find(index_name)->second;
  index_names_.at(table_name).erase(index_name);
  indexes_.erase(index_id);
  catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, index_id);
  FlushCatalogMetaPage();
  delete index_info;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  auto meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  TableInfo* table_info;
  if(tables_.find(table_id)!=tables_.end())
  {
    return DB_TABLE_ALREADY_EXIST;
  }
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  Page* page=buffer_pool_manager_->FetchPage(page_id);
  TableMetadata* table_meta = nullptr;
  TableMetadata::DeserializeFrom(page->GetData(),table_meta);
  auto schema=Schema::DeepCopySchema(table_meta->GetSchema());
  TableHeap* heap=TableHeap::Create(buffer_pool_manager_,table_meta->GetFirstPageId(),schema,log_manager_,lock_manager_);
  table_info = TableInfo::Create();
  table_info->Init(table_meta,heap);
  table_names_.emplace(table_meta->GetTableName(),table_id);
  tables_.emplace(table_id,table_info);
  //buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  IndexInfo* index_info;
  if(indexes_.find(index_id)!=indexes_.end())
  {
    return DB_INDEX_ALREADY_EXIST;
  }
  auto page=buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata* index_meta=nullptr;
  IndexMetadata::DeserializeFrom(page->GetData(),index_meta);
  table_id_t table_id=index_meta->GetTableId();
  TableInfo* table_info=tables_.find(table_id)->second;
  index_info = IndexInfo::Create();
  index_info->Init(index_meta,table_info,buffer_pool_manager_);
  // auto *idx_roots = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  // page_id_t index_root_page_id = INVALID_PAGE_ID;
  // idx_roots->GetRootId(index_id, &index_root_page_id);
  auto table_name=table_info->GetTableName();
  auto index_name=index_info->GetIndexName();
  if(index_names_.find(table_name)==index_names_.end())
  {
    std::unordered_map<std::string, index_id_t> map;
    map.emplace(index_name,index_id);
    index_names_.emplace(table_name,map);
  }
  else
  {
    index_names_.find(table_name)->second.emplace(index_name,index_id);
  }
  indexes_.emplace(index_id,index_info);
  //buffer_pool_manager_->UnpinPage(page_id, false);
  //buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto table = tables_.find(table_id);
  if (table == tables_.end()) {
    table_info = nullptr;
    return DB_TABLE_NOT_EXIST;
  }
  table_info = table->second;
  return DB_SUCCESS;
}