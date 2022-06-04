#include "executor/execute_engine.h"
#include "glog/logging.h"

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
  for (; s[i] != '\0'; ++i, ) {
    len = len * 10 + s[i] - '0';
    j++;
  }
  return len / (pow(10, j));
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
    DBStorageEngine engine(db_name);
    dbs_.insert({db_name, &engine});
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
  cata->GetTables(tables);

  if (tables.empty()) {
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
    if (tmp->val_ == constraint[0]) {
      unique = true;
    } else if (tmp->val_ == constraint[1]) {
      nullable = false;
    } else if (tmp->val_ == constraint[2]) {
      unique = true;
      nullable = false;
      // TODO:
    }
    pSyntaxNode attr = tmp->child_;
    column_name = attr->val_;
    pSyntaxNode type = tmp->next_;

    if (type->val_ == TYPE[0]) {
      type_ = kTypeChar;
      pSyntaxNode size = type->child_;
      len = StringToInt(size->val_);
      Column cur_col(column_name, type_, len, index, nullable, unique);
      columns.push_back(&cur_col);
      index++;
      tmp = tmp->next_;
    } else {
      if (type->val_ == TYPE[1]) {
        type_ = kTypeInt;
      } else if (type->val_ == TYPE[2]) {
        type_ = kTypeFloat;
      }
      Column cur_col(column_name, type_, index, nullable, unique);
      columns.push_back(&cur_col);
      index++;
      tmp = tmp->next_;
    }
  }
  TableSchema schema(columns);
  if (cata->CreateTable(table_name, &schema, nullptr, table_info) == DB_SUCCESS) {
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
    return DB_FAILED;
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
  cata->GetTables(tables);

  if (tables.empty()) {
    printf("[INFO] There aren't any tables!\n");
    return DB_FAILED;
  } else {
    printf("[TITLE] Index_in_%s\n", current_db_.c_str());
    for (auto iter = tables.begin(); iter != tables.end(); ++iter) {
      printf("[TITLE] _Index_in%s\n", (*iter)->GetTableName().c_str());
      cata->GetTableIndexes((*iter)->GetTableName(), indexes);
      for (auto iter = indexes.begin(); iter != indexes.end(); ++iter) {
        printf("[INDEX] %s\n", (*iter)->GetIndexName().c_str());
      }
    }
    return DB_SUCCESS;
  }
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
  index_.insert({index_name, table_name});

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
    printf("[INFO] Create index successfully!\n");
    return DB_SUCCESS;
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;
  pSyntaxNode tmp = ast->child_;
  string index_name = tmp->val_;

  string table_name = (index_.find(index_name))->first;

  cata->DropIndex(table_name, index_name);

  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif

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
  pSyntaxNode tmp = ast->child_;
  table_name = tmp->val_;
  std::vector<Field *> fields_;

  tmp = tmp->next_;
  tmp = tmp->child_;
  cata->GetTable(table_name, table_info);
  while (tmp != NULL) {
    //加到fields_里
    if (tmp->type_ == kNodeNumber) {
    } else if (tmp->type_ == kNodeString) {
    } else if (tmp->type_ == kNodeNull) {
    }

    tmp = tmp->next_;
  }
  //构造row
  Row &row();
  Transaction *txn;

  TableHeap *heap = table_info->GetTableHeap();

  if (heap->InsertTuple(row, txn)) {
    cout << "[INFO] Insert successfully!" << endl;
    return DB_SUCCESS;
  } else {
    cout << "[INFO] Insert Failed!" << endl;
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  pSyntaxNode tmp = ast->child_;
  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;
  if (tmp->next_ == NULL) {
    //全删
  } else {
    //条件
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  pSyntaxNode tmp = ast->child_;
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
  }

  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file = ast->child_->val_;
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