#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  vector<RowId> row_ids;
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      row_ids.push_back(row.GetRowId());
      size++;
    }
    delete[] characters;
  }
  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values) {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  // test iterator
  ASSERT_EQ(size, 0);
  int count = 0;
  for(auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); ++iter){
    Row row(iter->GetRowId());
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    ASSERT_EQ(schema.get()->GetColumnCount(), iter->GetFields().size());
    for(size_t j = 0; j < schema.get()->GetColumnCount(); j++){
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(*iter->GetField(j)));
    }
    count++;
  }
  ASSERT_EQ(count, row_nums);
  
  // test update
  std::unordered_map<int64_t, Fields *> new_row_values;
  std::set<page_id_t> used_pages;
  for(int i = 0; i < row_nums; i++){
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields = new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                                Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_EQ(true, table_heap->UpdateTuple(row, row_ids.at(i), nullptr));
    ASSERT_EQ(false, row.GetRowId().GetPageId()== INVALID_PAGE_ID);
    new_row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
  }
  // check the update result
  for(auto row_kv2 : new_row_values){
    Row row(RowId(row_kv2.first));
    ASSERT_EQ(true, table_heap->GetTuple(&row, nullptr));
    used_pages.insert(row.GetRowId().GetPageId());
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for(size_t j = 0; j < schema.get()->GetColumnCount(); j++){
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv2.second->at(j)));
    }
    table_heap->ApplyDelete(row.GetRowId(), nullptr);
    ASSERT_EQ(false, table_heap->GetTuple(&row, nullptr));
    delete row_kv2.second;
  }
  // test free
  table_heap->FreeTableHeap();
  auto *disk = new DiskManager(db_file_name);
  auto *bpm = new BufferPoolManager(100, disk);
  ASSERT_EQ(true, bpm->IsPageFree(*used_pages.begin()));
  //cout << bpm->IsPageFree(*used_pages.begin()) << endl;
  for(auto page_id : used_pages){
    ASSERT_EQ(true, bpm->IsPageFree(page_id));
    // cout << bpm->IsPageFree(page_id) << endl;
  }
  delete disk;
  delete bpm;
  delete disk_mgr_;
  delete bpm_;
}