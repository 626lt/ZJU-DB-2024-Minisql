#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>
#include <fstream>
#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.**/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if(db_name == current_db_){
    current_db_ = "";
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database" << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    LOG(ERROR) << "No database selected." << std::endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) {
    LOG(ERROR) << "No database selected." << std::endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  vector<string> primary_keys, unique_keys, column_names;
  unordered_set<string> primary_key_set;
  vector<TypeId> column_types;
  vector<bool> is_unique;
  vector<int> column_data_len, column_id;
  pSyntaxNode column_list = ast->child_->next_;
  int cnt = 0;
  for (auto col_list = column_list->child_; col_list != nullptr; col_list = col_list->next_) {
    if (col_list->type_ == kNodeColumnDefinition) {
      is_unique.emplace_back(col_list->val_ != nullptr);
      column_names.emplace_back(col_list->child_->val_);
      column_id.emplace_back(cnt++);
      string column_type_name(col_list->child_->next_->val_);
      if (column_type_name == "int") {
        column_types.emplace_back(kTypeInt);
        column_data_len.emplace_back(0);
      } else if (column_type_name == "char") {
        int len = atoi(col_list->child_->next_->child_->val_);
        double length= atof(col_list->child_->next_->child_->val_ );
        if (len < 0) {
          LOG(ERROR) << "invalid char length: " << length;
          return DB_FAILED;
        }
        if(length!=len){
          LOG(ERROR) << "invalid char length: " << length;
          return DB_FAILED;
        }
        column_types.emplace_back(kTypeChar);
        column_data_len.emplace_back(len);
      } else if (column_type_name == "float") {
        column_types.emplace_back(kTypeFloat);
        column_data_len.emplace_back(0);
      } else {
        LOG(ERROR) << "invalid column type: " << column_type_name;
        return DB_FAILED;
      }
    } else if (col_list->type_ == kNodeColumnList) {
      pSyntaxNode key_list = col_list->child_;
      while (key_list != nullptr) {
        primary_keys.emplace_back(key_list->val_);
        primary_key_set.insert(string(key_list->val_));
        key_list = key_list->next_;
      }
    }
  }
  vector<Column *> columns;
  bool should_manage = false;
  for (int i = 0; i < column_names.size(); i++) {
    if (primary_key_set.find(column_names[i]) != primary_key_set.end()) {
      if (column_types[i] == kTypeChar) {
        columns.push_back(new Column(column_names[i], column_types[i], column_data_len[i], i, false, true));
        should_manage = true;
      } else {
        columns.push_back(new Column(column_names[i], column_types[i], i, false, true));
      }
    } else {
      if (column_types[i] == kTypeChar) {
        columns.push_back(new Column(column_names[i], column_types[i], column_data_len[i], i, false, is_unique[i]));
        should_manage = true;
      } else {
        columns.push_back(new Column(column_names[i], column_types[i], i, false, is_unique[i]));
      }
      if (is_unique[i]) {
        unique_keys.push_back(column_names[i]);
      }
    }
  }
  Schema *schema = new Schema(columns, should_manage);
  TableInfo *table_info;
  dberr_t res = context->GetCatalog()->CreateTable(table_name, schema, context->GetTransaction(), table_info);
  if (res != DB_SUCCESS) {
    return res;
  }
  if (!primary_keys.empty()) {
    IndexInfo *index_info;
    res = context->GetCatalog()->CreateIndex(table_info->GetTableName(), table_name + "_PK_IDX", primary_keys,
                                             context->GetTransaction(), index_info, "bptree");
    for (auto key : unique_keys) {
      string index_name = "UNIQUE_";
      index_name += key + "_";
      index_name += "ON_" + table_name;
      context->GetCatalog()->CreateIndex(table_name, index_name, unique_keys, context->GetTransaction(), index_info,
                                         "btree");
    }
    if (res != DB_SUCCESS) {
      return res;
    }
  }
  cout<<"table "<<table_name<<" created."<<endl;
  return DB_SUCCESS;



}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(current_db_.empty())
  {
    std::cout<<"No database selected."<<std::endl;
    return DB_FAILED;
  }
  string table_name(ast->child_->val_);
  dberr_t res = dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
  if (res != DB_SUCCESS) {
    return res;
  }
  std::vector<IndexInfo *> index_infos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, index_infos);
  for (auto index_info : index_infos) {
    res = dbs_[current_db_]->catalog_mgr_->DropIndex(table_name, index_info->GetIndexName());
    if (res != DB_SUCCESS) {
      return res;
    }
  }
  cout<<"table "<<table_name<<" dropped."<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty())
  {
    std::cout<<"No database selected."<<std::endl;
    return DB_FAILED;
  }
  std::vector<TableInfo *> tables;
  string table_name;
  stringstream ss;
  bool index_exist = false;
  ResultWriter writer(ss);
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto iter : tables){
    vector<IndexInfo *> indexes;
    vector<int> index_widths;
    index_widths.push_back(iter->GetTableName().length() + 10);
    dberr_t res = dbs_[current_db_]->catalog_mgr_->GetTableIndexes(iter->GetTableName(), indexes);
    if(res != DB_SUCCESS){
      return res;
    }
    if(indexes.empty()){
      continue;
    }
    index_exist = true;
    for(auto index : indexes){
      index_widths[0] = max(index_widths[0], int(index->GetIndexName().length()));
    }
    writer.Divider(index_widths);
    writer.BeginRow();
    writer.WriteHeaderCell("Indexes_in_" + iter->GetTableName(), index_widths[0]);
    writer.EndRow();
    writer.Divider(index_widths);
    for(auto index : indexes){
      writer.BeginRow();
      writer.WriteCell(index->GetIndexName(), index_widths[0]);
      writer.EndRow();
      writer.Divider(index_widths);
    }
    std::cout << writer.stream_.rdbuf();
  }
  if(!index_exist){
    cout << "No index exists" << endl;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_.empty())
  {
    std::cout<<"No database selected."<<std::endl;
    return DB_FAILED;
  }
  string index_name(ast->child_->val_);
  string table_name(ast->child_->next_->val_);
  string index_type("bptree");
  vector<string> column_names;
  for (auto column_name = ast->child_->next_->next_->child_; column_name != nullptr; column_name = column_name->next_) {
    column_names.emplace_back(column_name->val_);
  }
  if (ast->child_->next_->next_->next_ != nullptr) {
    index_type = string(ast->child_->next_->next_->next_->child_->val_);
  }
  TableInfo *table_info;
  dberr_t res = dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info);
  if (res != DB_SUCCESS) {
    return res;
  }
  IndexInfo *index_info;
  res = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, column_names, context->GetTransaction(), index_info,
                                           index_type);
  if (res != DB_SUCCESS) {
    return res;
  }

  // Insert old records into the new index.
  auto txn= context->GetTransaction();
  auto table_heap = table_info->GetTableHeap();
  for (auto row = table_heap->Begin(txn); row != table_heap->End(); row++) {
    auto row_id = row->GetRowId();
    // Get related fields.
    vector<Field> fields;
    for (auto col : index_info->GetIndexKeySchema()->GetColumns()) {
      fields.push_back(*(*row).GetField(col->GetTableInd()));
    }
    // The row to be inserted into index.
    Row row_idx(fields);
    res = index_info->GetIndex()->InsertEntry(row_idx, row_id, txn);
    if (res != DB_SUCCESS) {
      return res;
    }
  }
  cout<<"index "<<index_name<<" created."<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_.empty())
  {
    std::cout<<"No database selected."<<std::endl;
    return DB_FAILED;
  }
  string index_name(ast->child_->val_);
  string table_name;
  std::vector<TableInfo *> tables;
  std::vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto ite : tables){
    table_name = ite->GetTableName();
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name,indexes);
    for(auto ite1 : indexes){
      if(ite1->GetIndexName() == index_name){
        dbs_[current_db_]->catalog_mgr_->DropIndex(table_name,index_name);
        cout<<"index "<<index_name<<" dropped."<<endl;
        return DB_SUCCESS;
      }
    }
  }
  return DB_INDEX_NOT_FOUND;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  auto start_time = std::chrono::system_clock::now();
  string filename = ast->child_->val_;
  std::ifstream file(filename);
  if (!file.is_open()) {
    std::cout << "File not found." << std::endl;
    return DB_FAILED;
  }
  char buffer[1024];
  char c;
  int cnt=0;
  memset(buffer, 0, 1024);
  while (file.get(c)) {
    buffer[cnt++] = c;
    if (cnt >= 1024) {
      LOG(ERROR) << "Buffer overflow";
      return DB_FAILED;
    }
    if (c == ';'){
      // buffer[cnt] = 0;
      file.get(c);
      auto bp = yy_scan_string(buffer);
      

      yy_switch_to_buffer(bp);

      MinisqlParserInit();

      yyparse();

      if (MinisqlParserGetError()) {
        std::cout<<MinisqlParserGetErrorMessage()<<std::endl;
      }

      auto res = Execute(MinisqlGetParserRootNode());
      if (res != DB_SUCCESS) {
        return res;
      }

      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();

      if (res==DB_QUIT) {
        break;
      }
      memset(buffer, 0, 1024);
      cnt=0;
    }
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  std::cout << "Executed in " << duration_time << " ms." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
