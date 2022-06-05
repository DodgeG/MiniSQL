#include "executor/execute_engine.h"
#include <iomanip>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/index.h"
#include <typeinfo>

ExecuteEngine::ExecuteEngine() {}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

uint32_t StringToInt(char *s) {
  uint32_t len = 0;
  for (int i = 0; s[i] != '\0'; ++i) {
    len = len * 10 + s[i] - '0';
  }
  return len;
}

float StringToFloat(char *s) {
  float len = 0;
  int i = 0, j = 0;
  for (; s[i] != '.'; ++i) {
    len = len * 10 + s[i] - '0';
  }
  ++i;
  for (; s[i] != '\0'; ++i) {
    len = len * 10 + s[i] - '0';
    j++;
  }
  return len / (pow(10, j));
}

bool isFloat(string str) {
  for (int i = 0; str[i] != '\0'; i++) {
    if (str[i] == '.') return true;
  }
  return false;
}

bool DFS(pSyntaxNode ast, TableIterator iter, Schema *schema) {
  if (ast->type_ == kNodeCompareOperator) {
    pSyntaxNode attr = ast->child_;
    pSyntaxNode val = attr->next_;
    const char *r_value = val->val_;

    uint32_t index;
    string col_name = attr->val_;
    schema->GetColumnIndex(col_name, index);
    Field *fie = iter->GetField(index);
    const char *l_value = fie->GetData();
    auto item = ast->val_;
    if (item == (char *)"=" && l_value == r_value)
      return true;
    else if (item == (char *)">" && strcmp(l_value, r_value) > 0)
      return true;
    else if (item == (char *)">=" && strcmp(l_value, r_value) >= 0)
      return true;
    else if (item == (char *)"<=" && strcmp(l_value, r_value) <= 0)
      return true;
    else if (item == (char *)"<" && strcmp(l_value, r_value) < 0)
      return true;
    else if (item == (char *)"<>" && strcmp(l_value, r_value) != 0)
      return true;

    return false;
  } else if (ast->type_ == kNodeConnector) {
    if (ast->val_ == (char *)"and" && DFS(ast->child_, iter, schema) && DFS(ast->child_->next_, iter, schema))
      return true;
    else if (ast->val_ == (char *)"or" && (DFS(ast->child_, iter, schema) || DFS(ast->child_->next_, iter, schema)))
      return true;

    return false;
  } else
    return false;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  pSyntaxNode tmp = ast->child_;
  string db_name = tmp->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    printf("[INFO] This database has existed!\n");
    return DB_FAILED;
  } else {
    DBStorageEngine *engine = new DBStorageEngine(db_name, true);
    dbs_.insert({db_name, engine});
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  pSyntaxNode tmp = ast->child_;
  string db_name = tmp->val_;
  auto iter = dbs_.find(db_name);
  if (iter == dbs_.end()) {
    printf("[INFO] Database not exist!\n");
    return DB_FAILED;
  } else {
    dbs_.erase(iter);
    printf("[INFO] Drop database successfully!\n");
    if (db_name == current_db_) current_db_.clear();
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    printf("[INFO] Don't have any databases!\n");
    return DB_FAILED;
  } else {
    for (auto iter = dbs_.begin(); iter != dbs_.end(); ++iter) {
      std::cout << "[DATABASE] " << iter->first << '\n';
    }
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  pSyntaxNode tmp = ast->child_;
  string db_name = tmp->val_;
  auto iter = dbs_.find(db_name);
  if (iter == dbs_.end()) {
    printf("[INFO] No such database!\n");
    return DB_FAILED;
  } else {
    current_db_ = db_name;
    printf("[INFO] Use successfully!\n");
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;
  std::vector<TableInfo *> tables;
  if (cata->GetTables(tables) == DB_FAILED) {
    printf("[INFO] There aren't any tables!\n");
    return DB_FAILED;
  } else {
    printf("[TITLE] Tables_in_%s\n", current_db_.c_str());
    for (auto iter = tables.begin(); iter != tables.end(); ++iter) {
      printf("[TABLE] %s\n", (*iter)->GetTableName().c_str());
    }
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  string table_name;
  std::vector<Column *> columns;
  TableInfo *table_info;
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;
  pSyntaxNode tmp = ast->child_;
  table_name = tmp->val_;

  tmp = tmp->next_;
  tmp = tmp->child_;
  std::string column_name;
  TypeId type_;
  uint32_t index = 0;
  uint32_t len = 0;
  string constraint[3] = {"unique", "not null", "primary key"};
  string TYPE[3] = {"char", "int", "float"};

  while (tmp != nullptr) {
    bool nullable = true;
    bool unique = false;
    if (tmp->val_ != NULL) {
      if (tmp->val_ == constraint[0]) {
        unique = true;
      } else if (tmp->val_ == constraint[1]) {
        nullable = false;
      } else if (tmp->val_ == constraint[2]) {
        unique = true;
        nullable = false;
      }
    }
    pSyntaxNode attr = tmp->child_;
    column_name = attr->val_;
    pSyntaxNode type = attr->next_;

    if (type->val_ == TYPE[0]) {
      type_ = kTypeChar;
      pSyntaxNode size = type->child_;
      len = StringToInt(size->val_);

      Column *cur_col = new Column(column_name, type_, len, index, nullable, unique);
      columns.push_back(cur_col);
      index++;
      tmp = tmp->next_;
    } else {
      if (type->val_ == TYPE[1]) {
        type_ = kTypeInt;
      } else if (type->val_ == TYPE[2]) {
        type_ = kTypeFloat;
      }
      Column *cur_col = new Column(column_name, type_, index, nullable, unique);
      columns.push_back(cur_col);
      index++;
      tmp = tmp->next_;
    }
  }
  TableSchema *schema = new TableSchema(columns);
  if (cata->CreateTable(table_name, schema, nullptr, table_info) == DB_SUCCESS) {
    printf("[INFO] Create table successfully!\n");
    return DB_SUCCESS;
  } else {
    printf("[ERROR] Create table failed!\n");
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  pSyntaxNode tmp = ast->child_;
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;

  string table_name = tmp->val_;
  if (cata->DropTable(table_name) != DB_SUCCESS) {
    printf("[ERROR] No such table!\n");
    return DB_TABLE_NOT_EXIST;
  } else {
    printf("[INFO] Drop successfully!\n");
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;
  std::vector<TableInfo *> tables;
  std::vector<IndexInfo *> indexes;
  int flag = 0;

  if (cata->GetTables(tables) != DB_SUCCESS) {
    printf("[INFO] There aren't any indexes!\n");
    return DB_INDEX_NOT_FOUND;
  } else {
    for (auto table : tables) {
      indexes.clear();
      if (cata->GetTableIndexes(table->GetTableName(), indexes) == DB_SUCCESS) {
        flag = 1;
        printf("[TITLE] Indexes Of Table %s\n", table->GetTableName().c_str());
        for (auto index : indexes) {
          printf("[INDEX] %s\n", index->GetIndexName().c_str());
        }
      }
      printf("\n");
    }
  }
  if (!flag) {
    printf("[INFO] There aren't any indexes!\n");
    return DB_INDEX_NOT_FOUND;
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;
  pSyntaxNode tmp = ast->child_;
  string index_name = tmp->val_;

  tmp = tmp->next_;
  string table_name = tmp->val_;

  vector<string> index_keys;
  tmp = tmp->next_;
  tmp = tmp->child_;
  while (tmp != nullptr) {
    index_keys.push_back(tmp->val_);
    tmp = tmp->next_;
  }

  IndexInfo *index_info;
  if (cata->CreateIndex(table_name, index_name, index_keys, nullptr, index_info) != DB_SUCCESS) {
    printf("[ERROR] Create index failed!\n");
    return DB_FAILED;
  } else {
    IndexMetadata *meta = index_info->GetMetadata();
    std::vector<uint32_t> key_map = meta->GetKeyMapping();
    TableInfo *table_info = index_info->GetTableInfo();
    TableHeap *table_heap = table_info->GetTableHeap();
    TableIterator iter = table_heap->Begin(nullptr);
    Index *index_ = index_info->GetIndex();
    std::vector<Field> field;
    while (iter != table_heap->End()) {
      field.clear();
      for (auto ind : key_map) {
        field.push_back(*(iter->GetField(ind)));
      }
      Row entry(field);
      index_->InsertEntry(entry, iter->GetRowId(), nullptr);
    }

    printf("[INFO] Create index successfully!\n");
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;
  pSyntaxNode tmp = ast->child_;
  string index_name = tmp->val_;

  std::vector<TableInfo *> tables;
  cata->GetTables(tables);
  for (auto table : tables) {
    IndexInfo *tmp;
    if (cata->GetIndex(table->GetTableName(), index_name, tmp) == DB_SUCCESS) {
      cata->DropIndex(table->GetTableName(), index_name);
      printf("[ERROR] Drop index successfully!\n");
      return DB_SUCCESS;
    }
  }
  printf("[ERROR] Drop failed!\n");
  return DB_INDEX_NOT_FOUND;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  //pSyntaxNode tmp = ast->child_;
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;
  if (ast->type_ == kNodeAllColumns && ast->next_->next_ == NULL) {
    ast = ast->next_;
    string tablename = ast->val_;
    TableInfo *table_info = NULL;
    cata->GetTable(tablename, table_info);
    // column name
    Schema *schema = table_info->GetSchema();
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      cout << left << setw(15) << schema->GetColumn(i)->GetName();
    }
    cout << endl;

    TableHeap *table_heap = table_info->GetTableHeap();
    for (TableIterator iter = table_heap->Begin(NULL); iter != table_heap->End(); ++iter) {
      for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
        cout << left << setw(15) << (*iter).GetField(i)->GetData();
      }
      cout << endl;
    }
    return DB_SUCCESS;
  } else if (ast->type_ == kNodeColumnList && ast->next_->next_ == NULL) {
    string tablename = ast->next_->val_;
    TableInfo *table_info = NULL;
    cata->GetTable(tablename, table_info);
    ast = ast->child_;
    std::vector<string> column_name;
    while (ast != NULL) {
      column_name.push_back(ast->val_);
      ast = ast->next_;
    }
    // column name
    Schema *schema = table_info->GetSchema();
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      int j = 0;
      if (schema->GetColumn(i)->GetName() == column_name[j]) {
        cout << left << setw(15) << schema->GetColumn(i)->GetName();
        j++;
      }
    }
    cout << endl;

    TableHeap *table_heap = table_info->GetTableHeap();
    for (TableIterator iter = table_heap->Begin(NULL); iter != table_heap->End(); ++iter) {
      for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
        int j = 0;
        if (schema->GetColumn(i)->GetName() == column_name[j]) {
          cout << left << setw(15) << (*iter).GetField(i)->GetData();
          j++;
        }
      }
      cout << endl;
    }
    return DB_SUCCESS;
  } else if (ast->type_ == kNodeAllColumns && ast->next_->next_ != NULL) {
    //有索引

    //没索引
    ast = ast->next_;
    string tablename = ast->val_;
    TableInfo *table_info = NULL;
    cata->GetTable(tablename, table_info);
    // column name
    Schema *schema = table_info->GetSchema();
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      cout << left << setw(15) << schema->GetColumn(i)->GetName();
    }
    cout << endl;

    ast = ast->next_;
    ast = ast->child_;

    TableHeap *table_heap = table_info->GetTableHeap();
    for (TableIterator iter = table_heap->Begin(NULL); iter != table_heap->End(); ++iter) {
      if (DFS(ast, iter, schema)) {
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          cout << left << setw(15) << (*iter).GetField(i)->GetData();
        }
        cout << endl;
      }
    }
    return DB_SUCCESS;
  } else if (ast->type_ == kNodeColumnList && ast->next_->next_ != NULL) {
     string tablename = ast->next_->val_;
    //有索引
    
    
    
    //没索引
   
    pSyntaxNode tmp = ast;
    TableInfo *table_info = NULL;
    cata->GetTable(tablename, table_info);
    ast = ast->child_;
    std::vector<string> column_name;
    while (ast != NULL) {
      column_name.push_back(ast->val_);
      ast = ast->next_;
    }
    // column name
    Schema *schema = table_info->GetSchema();
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      int j = 0;
      if (schema->GetColumn(i)->GetName() == column_name[j]) {
        cout << left << setw(15) << schema->GetColumn(i)->GetName();
        j++;
      }
    }
    cout << endl;
    tmp = tmp->next_;
    tmp = tmp->next_;
    tmp = tmp->child_;
    TableHeap *table_heap = table_info->GetTableHeap();
    for (TableIterator iter = table_heap->Begin(NULL); iter != table_heap->End(); ++iter) {
      if (DFS(tmp, iter, schema)) {
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          int j = 0;
          if (schema->GetColumn(i)->GetName() == column_name[j]) {
            cout << left << setw(15) << (*iter).GetField(i)->GetData();
            j++;
          }
        }
        cout << endl;
      }
    }
    return DB_SUCCESS;
  }

  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  pSyntaxNode tmp = ast->child_;
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;

  string table_name;
  TableInfo *table_info;
  table_name = tmp->val_;
  std::vector<Field> fields_;

  tmp = tmp->next_;
  tmp = tmp->child_;
  cata->GetTable(table_name, table_info);

  while (tmp != NULL) {
    //加到fields_里
    Field *fie = NULL;
    if (tmp->type_ == kNodeNumber) {
      if (isFloat(tmp->val_)) {
        fie = new Field(kTypeFloat, StringToFloat(tmp->val_));

      } else {
        fie = new Field(kTypeInt, (int32_t)StringToInt(tmp->val_));
      }
    } else if (tmp->type_ == kNodeString) {
      fie = new Field(kTypeChar, tmp->val_, strlen(tmp->val_), false);
    } else if (tmp->type_ == kNodeNull) {
      fie = new Field(kTypeInvalid);
    }
    fields_.push_back(*fie);
    tmp = tmp->next_;
  }
  //构造row
  TableHeap *table_heap = table_info->GetTableHeap();
  Row row(fields_);
  table_heap->InsertTuple(row, nullptr);
  std::vector<IndexInfo *> indexes;
  if (cata->GetTableIndexes(table_name, indexes) == DB_SUCCESS) {
    for (auto index_info : indexes) {
      IndexMetadata *meta = index_info->GetMetadata();
      std::vector<uint32_t> key_map = meta->GetKeyMapping();
      Index *index_ = index_info->GetIndex();
      std::vector<Field> fies;
      for (auto id : key_map) {
        fies.push_back(fields_.at(id));
      }
      Row row_index(fies);
      if (index_->InsertEntry(row_index, row.GetRowId(), nullptr) == DB_SUCCESS) {
        printf("[INFO] Insert successfully!\n");
        return DB_SUCCESS;
      } else {
        printf("[INFO] Insert failed!\n");
        return DB_FAILED;
      }
    }
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  pSyntaxNode tmp = ast->child_;
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;
  if (tmp->next_ == NULL) {
    string table_name = tmp->val_;
    TableInfo *table_info;
    std::vector<IndexInfo *> indexes;
    cata->GetTable(table_name, table_info);
    cata->GetTableIndexes(table_name, indexes);
    TableHeap *table_heap = table_info->GetTableHeap();
    for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); iter++) {
      if (indexes.size() != 0) {
        for (auto index : indexes) {
          Index *idx = index->GetIndex();
          std::vector<Field> fields_;
          IndexMetadata *meta = index->GetMetadata();
          std::vector<uint32_t> key_map = meta->GetKeyMapping();
          for (auto id : key_map) {
            fields_.push_back(*(iter->GetField(id)));
          }
          Row delete_row(fields_);
          RowId tmp;
          idx->RemoveEntry(delete_row, tmp, nullptr);
        }
      }
      table_heap->MarkDelete(iter->GetRowId(), nullptr);
    }
  } else {
    string table_name = tmp->val_;
    TableInfo *table_info;
    std::vector<IndexInfo *> indexes;
    cata->GetTable(table_name, table_info);
    Schema *schema = table_info->GetSchema();
    TableHeap *table_heap = table_info->GetTableHeap();
    tmp = tmp->next_;
    tmp = tmp->child_;
    cata->GetTableIndexes(table_name, indexes);
    for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); iter++) {
      if (DFS(tmp, iter, schema)) {
        if (indexes.size() != 0) {
          for (auto index : indexes) {
            Index *idx = index->GetIndex();
            IndexMetadata *meta = index->GetMetadata();
            std::vector<uint32_t> key_map = meta->GetKeyMapping();
            std::vector<Field> fields;
            for (auto id : key_map) {
              fields.push_back(*(iter->GetField(id)));
            }
            Row delete_row(fields);
            RowId tmp;
            idx->RemoveEntry(delete_row, tmp, nullptr);
          
          }
        }
        table_heap->MarkDelete(iter->GetRowId(), nullptr);
      }
    }
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  /*pSyntaxNode tmp = ast->child_;
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;

  string table_name;
  TableInfo *table_info;
  pSyntaxNode tmp = ast->child_;
  table_name = tmp->val_;
  std::vector<Field *> fields_;

  tmp = tmp->next_;                  // upadate
  pSyntaxNode tmp_con = tmp->next_;  // condition
  tmp = tmp->child_;
  cata->GetTable(table_name, table_info);
  while (tmp != NULL) {
    //加到fields_里
    if (tmp->child_->next_->type_ == kNodeNumber) {
    } else if (tmp->child_->next_->type_ == kNodeString) {
    } else if (tmp->child_->next_->type_ == kNodeNull) {
    }

    tmp = tmp->next_;
  }*/

  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  // string file = ast->child_->val_;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
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