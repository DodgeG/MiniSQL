#include "executor/execute_engine.h"
#include <algorithm>
#include <iomanip>
#include <typeinfo>
#include "fstream"
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/index.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"
#include <set>
#include <chrono>
typedef std::chrono::high_resolution_clock Clock;

extern "C" {
int yyparse(void);
// FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

void InitGoogleLog(char *argv) {
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
}

void InputCommand(char *input, const int len) {
  memset(input, 0, len);
  printf("minisql > ");
  int i = 0;
  char ch;
  while ((ch = getchar()) != ';') {
    input[i++] = ch;
  }
  input[i] = ch;  // ;
  getchar();      // remove enter
}

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

uint32_t StringToInt(const char *s) {
  uint32_t len = 0;
  for (int i = 0; s[i] != '\0'; ++i) {
    len = len * 10 + (s[i] - '0');
  }
  return len;
}

float StringToFloat(const char *s) {
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
  return len * 1.0 / (pow(10, j));
}

bool isFloat(string str) {
  for (int i = 0; str[i] != '\0'; i++) {
    if (str[i] == '.') return true;
  }
  return false;
}

int compare(const char *s,const char *t){
  int i = 0;
  int flag = 0;
  while(s[i]!='\0'){
    if(s[i] == '.'){
      flag = 1;
      break;
    }else if(s[i]<'0' || s[i] > '9'){
      flag = 2;
      break;
    }
    i++;
  }
  if(flag == 0){
    uint32_t l = 0;
    for (int i = 0; s[i] != '\0'; ++i) {
      l = l * 10 + (s[i] - '0');
    }
    uint32_t r = StringToInt((char *)t);
    if(l > r) return 1;
    if(strcmp(s,t) == 0) return 0;
    else return -1;
  }else if(flag == 1){
    float l = StringToFloat((char *)s);
    float r = StringToFloat((char *)t);
    if(l-r>0) return 1;
    else if(l-r==0) return 0;
    else if(l-r<0) return -1;
  }else return strcmp(s,t);
  return 0;
}

bool DFS(pSyntaxNode ast, TableIterator &iter, Schema *schema) {
  if (ast->type_ == kNodeCompareOperator) {
    pSyntaxNode attr = ast->child_;
    pSyntaxNode val = attr->next_;
    const char *r_value = val->val_;

    uint32_t index;
    string col_name = attr->val_;
    schema->GetColumnIndex(col_name, index);
    Field *fie = iter->GetField(index);

    const char *l_value = fie->GetData();
    char *item = ast->val_;
    if (strcmp(item, "=") == 0 && strcmp(l_value, r_value) == 0)
      return true;
    else if (strcmp(item, ">") == 0 && compare(l_value, r_value) > 0)
      return true;
    else if (strcmp(item, ">=") == 0 && compare(l_value, r_value) >= 0)
      return true;
    else if (strcmp(item, "<=") == 0 && compare(l_value, r_value) <= 0)
      return true;
    else if (strcmp(item, "<") == 0 && compare(l_value, r_value) < 0)
      return true;
    else if (strcmp(item, "<>") == 0 && strcmp(l_value, r_value) != 0)
      return true;
    else if (strcmp(item, "is") == 0 && compare(l_value,r_value) == 0)
      return true;
    else if (strcmp(item ,"is not") == 0 && compare(l_value,r_value)!=0)
      return true;

    return false;
  } else if (ast->type_ == kNodeConnector) {
    char *connector = ast->val_;
    if (strcmp(connector, "and") == 0 && DFS(ast->child_, iter, schema) && DFS(ast->child_->next_, iter, schema))
      return true;
    else if (strcmp(connector, "or") == 0 && (DFS(ast->child_, iter, schema) || DFS(ast->child_->next_, iter, schema)))
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
    cout << tables.size() << endl;
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
  pSyntaxNode tmp = ast->child_;//NodeIdentifier
  table_name = tmp->val_;

  tmp = tmp->next_;
  tmp = tmp->child_;//NodeDefination
  std::string column_name;
  TypeId type_;
  uint32_t index = 0;
  uint32_t len = 0;
  string constraint[2] = {"unique", "not null"};
  string TYPE[3] = {"char", "int", "float"};

  set<string> primary;
  pSyntaxNode flag = tmp->next_;
  while(flag!=nullptr){
    if(flag->type_ == kNodeColumnList && strcmp(flag->val_,"primary keys") == 0)
      primary.insert(flag->child_->val_);
    flag = flag->next_;
  }

  bool nullable;
  bool unique;
  vector<string> index_column;

  while (tmp != nullptr && tmp->type_!=kNodeColumnList) {
    nullable = true;
    unique = false;
    if (tmp->val_ != NULL) {
      if (tmp->val_ == constraint[0]) {
        unique = true;
      } else if (tmp->val_ == constraint[1]) {
        nullable = false;
      }
    }

    pSyntaxNode attr = tmp->child_;//属性名
    column_name = attr->val_;
    if(primary.count(column_name)!=0){
      unique = true;
      nullable = false;
    }
    pSyntaxNode type = attr->next_;//类型

    if(unique){
      index_column.push_back(column_name);
    }

    if (type->val_ == TYPE[0]) {  //如果是字符串型
      type_ = kTypeChar;
      pSyntaxNode size = type->child_;
      len = StringToInt(size->val_);

      Column *cur_col = new Column(column_name, type_, len, index, nullable, unique);
      columns.push_back(cur_col);
      index++;
      tmp = tmp->next_;
    } else {
      if (type->val_ == TYPE[1]) {//如果是Int型
        type_ = kTypeInt;
      } else if (type->val_ == TYPE[2]) {//如果是float型
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

    for(auto column : index_column){
      IndexInfo *index_info;
      vector<string> index_key;
      index_key.clear();
      index_key.push_back(column);
      cata->CreateIndex(table_name,column,index_key,nullptr,index_info);
    }
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
    double total_time = 0;
    while (iter != table_heap->End()) {
      field.clear();
      for (auto ind : key_map) {
        field.push_back(*(iter->GetField(ind)));
      }
      Row entry(field);
      auto t1 = Clock::now();
      index_->InsertEntry(entry, iter->GetRowId(), nullptr);
      auto t2 = Clock::now();
      total_time = total_time + std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();
      ++iter;
    }

    printf("[INFO] Create index successfully!\n");
    cerr<<"\ntotal time:"<<total_time/1e+6<<"ms\n";
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
      printf("[INFO] Drop index successfully!\n");
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

  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;

  ast = ast->child_;
  string tablename = ast->next_->val_;
  TableInfo *table_info = NULL;
  cata->GetTable(tablename, table_info);
  Schema *schema = table_info->GetSchema();


  if (ast->type_ == kNodeAllColumns && ast->next_->next_ == NULL) {  //无投影且无条件
    int size_table = 20 * schema->GetColumnCount() + schema->GetColumnCount() + 1;
    cout << left << setfill('-') << setw(size_table) << '-';
    cout << endl;
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      cout << "|";
      cout << left << setfill(' ') << setw(20) << schema->GetColumn(i)->GetName();
    }
    cout << "|" << endl;
    cout << left << setfill('-') << setw(size_table) << '-';
    cout << endl;

    ast = ast->next_;
    ast = ast->child_;

    TableHeap *table_heap = table_info->GetTableHeap();
    auto iter1 = table_heap->Begin(nullptr);
    for (TableIterator iter = table_heap->Begin(NULL); iter != table_heap->End(); ++iter) {
      for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
        cout << "|";
        cout << left << setfill(' ') << setw(20) << (*iter).GetField(i)->GetData();
      }
      cout << "|" << endl;
      cout << left << setfill('-') << setw(size_table) << '-';
      cout << endl;
    }
    return DB_SUCCESS;
  } else if (ast->type_ == kNodeColumnList && ast->next_->next_ == NULL) {  //有投影但无条件

    ast = ast->child_;

    std::vector<string> column_name;
    while (ast != NULL) {
      column_name.push_back(ast->val_);
      ast = ast->next_;
    }

    int size_table = 20 * column_name.size() + column_name.size() + 1;
    cout << left << setfill('-') << setw(size_table) << '-';
    cout << endl;
    for (uint32_t i = 0; i < column_name.size(); i++) {
      cout << "|";
      cout << left << setfill(' ') << setw(20) << column_name.at(i);
    }
    cout << "|" << endl;
    cout << left << setfill('-') << setw(size_table) << '-';
    cout << endl;


    TableHeap *table_heap = table_info->GetTableHeap();
    for (TableIterator iter = table_heap->Begin(NULL); iter != table_heap->End(); ++iter) {
      
      for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {   
        for (auto name : column_name){
          if (schema->GetColumn(i)->GetName() == name) {
          cout << "|";
          cout << left << setfill(' ')<<setw(20) << (*iter).GetField(i)->GetData();
          
        }
        }    
        
      }
      cout << "|" << endl;
      cout << left << setfill('-') << setw(size_table) << '-';
      cout << endl;
    }
    return DB_SUCCESS;
  } else if (ast->type_ == kNodeAllColumns && ast->next_->next_ != NULL) {  //无投影但有条件
    //有索引
    int size_table = 20 * schema->GetColumnCount() + schema->GetColumnCount() + 1;
    cout << left << setfill('-') << setw(size_table) << '-';
    cout << endl;
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      cout << "|";
      cout << left << setfill(' ') << setw(20) << schema->GetColumn(i)->GetName();
    }
    cout << "|" << endl;
    cout << left << setfill('-') << setw(size_table) << '-';
    cout << endl;

    pSyntaxNode tmp = ast->next_->next_->child_;
    if (tmp->type_ == kNodeCompareOperator) {
      string attr_name = ast->next_->next_->child_->child_->val_;
      vector<IndexInfo *> indexes;
      auto compare = tmp->val_;
      auto val = tmp->child_->next_->val_;
      cata->GetTableIndexes(tablename, indexes);
      for (auto index : indexes) {
        Schema *schema = index->GetIndexKeySchema();
        vector<Column *> cols;
        cols = schema->GetColumns();
        if (cols.size() == 1 && cols.at(0)->GetName() == attr_name) {
          uint32_t id = cols.at(0)->GetTableInd();
          TypeId type = cols.at(0)->GetType();
          auto idx = dynamic_cast<BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>> *>(index->GetIndex());
          vector<Field *> tmp_fie;
          if (type == kTypeInt) {
            auto fie = new Field(kTypeInt, (int32_t)StringToInt(val));
            tmp_fie.push_back(fie);
          } else if (type == kTypeFloat) {
            auto fie = new Field(kTypeFloat, StringToFloat(val));
            tmp_fie.push_back(fie);
          } else {
            auto fie = new Field(kTypeChar, val, strlen(val), false);
            tmp_fie.push_back(fie);
          }
          vector<Field> fields;
          for (auto fie : tmp_fie) fields.push_back(*fie);

          Row key(fields);
          GenericKey<64> Key;
          Key.SerializeFromKey(key, schema);
          if (strcmp(compare, "=") == 0) {
            auto iter = idx->GetBeginIterator(Key);
            Row row((*iter).second);
            TableHeap *table_heap = table_info->GetTableHeap();
            table_heap->GetTuple(&row, nullptr);
            vector<Field *> field_;
            field_ = row.GetFields();
            for (auto field : field_) {
              cout << "|";
              cout << left <<setfill(' ')<< setw(20) << field->GetData();
            }
            cout << "|"<<endl;
            cout << left << setfill('-') << setw(size_table) << '-';
            cout << endl;
          } else if (strcmp(compare, "<") == 0) {
            auto iter = idx->GetBeginIterator();
            for (; iter != idx->GetEndIterator(); ++iter) {
              Row row((*iter).second);
              TableHeap *table_heap = table_info->GetTableHeap();
              table_heap->GetTuple(&row, nullptr);
              vector<Field *> field_;
              field_ = row.GetFields();
              if (strcmp(field_[id]->GetData(), val) < 0) {
                for (auto field : field_) {
                  cout << "|";
                  cout << left << setfill(' ')<<setw(20) << field->GetData();
                }
                cout << "|"<<endl;
                cout << left << setfill('-') << setw(size_table) << '-';
                cout << endl;
              } else break;
            }
          } else if (strcmp(compare, "<=") == 0) {
            auto iter = idx->GetBeginIterator();
            for (; iter != idx->GetEndIterator(); ++iter) {
              Row row((*iter).second);
              TableHeap *table_heap = table_info->GetTableHeap();
              table_heap->GetTuple(&row, nullptr);
              vector<Field *> field_;
              field_ = row.GetFields();
              if (strcmp(field_[id]->GetData(), val) <= 0) {
                for (auto field : field_) {
                  cout << "|";
                  cout << left << setfill(' ')<<setw(20) << field->GetData();
                  /*for (auto x = field->GetData(); *x != '\0'; x++)
                    printf("%d ", (int)*x);*/
                }
                cout << "|"<<endl;
                cout << left << setfill('-') << setw(size_table) << '-';
                cout << endl;
              } else break;
            }
          } else if (strcmp(compare, "<>") == 0) {
            auto tmp = idx->GetBeginIterator(Key);
            if (tmp.GetFlag() == 1) {
              for (auto iter = idx->GetBeginIterator(); iter != idx->GetEndIterator(); ++iter) {
                if (iter == tmp) ++iter;
                Row row((*iter).second);
                TableHeap *table_heap = table_info->GetTableHeap();
                table_heap->GetTuple(&row, nullptr);
                vector<Field *> field_;
                field_ = row.GetFields();
                for (auto field : field_) {
                  cout <<"|";
                  cout << left << setfill(' ') << setw(20) << field->GetData();
                }
                cout << "|"<<endl;
                cout << left << setfill('-') << setw(size_table) << '-';
                cout << endl;
              }
            }
          }else {
            auto iter = idx->GetBeginIterator(Key);
            if (iter.GetFlag() == 1 && strcmp(compare, ">") == 0) ++iter;
            while (iter != idx->GetEndIterator()) {
              Row row((*iter).second);
              TableHeap *table_heap = table_info->GetTableHeap();
              table_heap->GetTuple(&row, nullptr);
              vector<Field *> field_;
              field_ = row.GetFields();
              for (auto field : field_) {
                cout << "|";
                cout << left << setfill(' ')<<setw(20) << field->GetData();
              }
              cout << "|"<<endl;
              cout << left << setfill('-') << setw(size_table) << '-';
              cout << endl;
              ++iter;
            }
          }
          return DB_SUCCESS;
        }
      }
    }

    //没索引
    ast = ast->next_;
    ast = ast->next_;
    ast = ast->child_;

    TableHeap *table_heap = table_info->GetTableHeap();
    auto iter1 = table_heap->Begin(nullptr);
    for (TableIterator iter = table_heap->Begin(NULL); iter != table_heap->End(); ++iter) {
      if (DFS(ast, iter, schema)) {
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          cout << "|";
          cout << left << setfill(' ')<<setw(20) << (*iter).GetField(i)->GetData();
        }
        cout << "|"<<endl;
        cout << left << setfill('-') << setw(size_table) << '-';
        cout << endl;
      }
    }
    return DB_SUCCESS;
  } else if (ast->type_ == kNodeColumnList && ast->next_->next_ != NULL) {  //有投影且有条件
    //有索引
    pSyntaxNode list = ast->child_;
    std::vector<string> column_name;
    vector<uint32_t> ind;
    while (list != NULL) {
      uint32_t id_;
      column_name.push_back(list->val_);
      schema->GetColumnIndex(list->val_, id_);
      ind.push_back(id_);
      list = list->next_;
    }

    int size_table = 20 * column_name.size() + column_name.size() + 1;
    cout << left << setfill('-') << setw(size_table) << '-';
    cout << endl;
    for (uint32_t i = 0; i < column_name.size(); i++) {
      cout << "|";
      cout << left << setfill(' ') << setw(20) << column_name.at(i);
    }
    cout << "|" << endl;
    cout << left << setfill('-') << setw(size_table) << '-';
    cout << endl;

    pSyntaxNode tmp = ast->next_->next_->child_;
    if (tmp->type_ == kNodeCompareOperator) {
      string attr_name = ast->next_->next_->child_->child_->val_;
      vector<IndexInfo *> indexes;
      auto compare = tmp->val_;
      auto val = tmp->child_->next_->val_;
      cata->GetTableIndexes(tablename, indexes);

      for (auto index : indexes) {
        Schema *sch = index->GetIndexKeySchema();
        vector<Column *> cols;
        cols = sch->GetColumns();
        if (cols.size() == 1 && cols.at(0)->GetName() == attr_name) {
          uint32_t id = cols.at(0)->GetTableInd();
          TypeId type = cols.at(0)->GetType();
          auto idx = dynamic_cast<BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>> *>(index->GetIndex());
          vector<Field *> tmp_fie;
          if (type == kTypeInt) {
            auto fie = new Field(kTypeInt, (int32_t)StringToInt(val));
            tmp_fie.push_back(fie);
          } else if (type == kTypeFloat) {
            auto fie = new Field(kTypeFloat, StringToFloat(val));
            tmp_fie.push_back(fie);
          } else {
            auto fie = new Field(kTypeChar, val, strlen(val), false);
            tmp_fie.push_back(fie);
          }
          vector<Field> fields;
          for (auto fie : tmp_fie) fields.push_back(*fie);

          Row key(fields);
          GenericKey<64> Key;
          Key.SerializeFromKey(key, sch);
          if (strcmp(compare, "=") == 0) {
            auto iter = idx->GetBeginIterator(Key);
            Row row((*iter).second);
            TableHeap *table_heap = table_info->GetTableHeap();
            table_heap->GetTuple(&row, nullptr);
            for (auto id_ : ind) {
              cout<< "|";
              cout << left << setfill(' ')<<setw(20) << row.GetField(id_)->GetData();
            }
            cout << "|" << endl;
            cout << left << setfill('-') << setw(size_table) << '-';
            cout << endl;
          } else if (strcmp(compare, "<") == 0) {
            auto iter = idx->GetBeginIterator();
            for (; iter != idx->GetEndIterator(); ++iter) {
              Row row((*iter).second);
              TableHeap *table_heap = table_info->GetTableHeap();
              table_heap->GetTuple(&row, nullptr);
              vector<Field *> field_;
              field_ = row.GetFields();
              if (strcmp(field_[id]->GetData(), val) < 0) {
                for (auto id_ : ind) {
                  cout << "|";
                  cout << left << setfill(' ')<<setw(20) << row.GetField(id_)->GetData();
                }
                cout << "|" << endl;
                cout << left << setfill('-') << setw(size_table) << '-';
                cout << endl;
              } else break;
            }
          } else if (strcmp(compare, "<=") == 0) {
            auto iter = idx->GetBeginIterator();
            for (; iter != idx->GetEndIterator(); ++iter) {
              Row row((*iter).second);
              TableHeap *table_heap = table_info->GetTableHeap();
              table_heap->GetTuple(&row, nullptr);
              vector<Field *> field_;
              field_ = row.GetFields();
              if (strcmp(field_[id]->GetData(), val) <= 0) {
                for (auto id_ : ind) {
                  cout << "|";
                  cout << left << setfill(' ')<<setw(20) << row.GetField(id_)->GetData();
                }
                cout << "|" << endl;
                cout << left << setfill('-') << setw(size_table) << '-';
                cout << endl;
              } else break;
            }
          } else if (strcmp(compare, "<>") == 0) {
            auto tmp = idx->GetBeginIterator(Key);
            if (tmp.GetFlag() == 1) {
              for (auto iter = idx->GetBeginIterator(); iter != idx->GetEndIterator(); ++iter) {
                if (iter == tmp) ++iter;
                Row row((*iter).second);
                TableHeap *table_heap = table_info->GetTableHeap();
                table_heap->GetTuple(&row, nullptr);
                vector<Field *> field_;
                field_ = row.GetFields();
                for (auto id_ : ind) {
                  cout << "|";
                  cout << left << setfill(' ')<<setw(20) << row.GetField(id_)->GetData();
                }
                cout << "|" << endl;
                cout << left << setfill('-') << setw(size_table) << '-';
                cout << endl;
              }
            }
          } else {
            auto iter = idx->GetBeginIterator(Key);
            if (iter.GetFlag() == 1 && strcmp(compare, ">") == 0) ++iter;
            while (iter != idx->GetEndIterator()) {
              Row row((*iter).second);
              TableHeap *table_heap = table_info->GetTableHeap();
              table_heap->GetTuple(&row, nullptr);
              vector<Field *> field_;
              field_ = row.GetFields();
              for (auto id_ : ind) {
                cout<< "|";
                cout << left << setfill(' ')<<setw(20) << row.GetField(id_)->GetData();
              }
              cout << "|" << endl;
              cout << left << setfill('-') << setw(size_table) << '-';
              cout << endl;
              ++iter;
            }
          }
          return DB_SUCCESS;
        }
      }
    }

    //没索引

    // column name
    tmp = ast->next_->next_->child_;
    // tmp = tmp->child_;  // Operator or connector
    TableHeap *table_heap = table_info->GetTableHeap();
    for (TableIterator iter = table_heap->Begin(NULL); iter != table_heap->End(); ++iter) {
      if (DFS(tmp, iter, schema)) {
        
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          for (auto name : column_name){
          if (schema->GetColumn(i)->GetName() == name) {
          cout << "|";
          cout << left << setfill(' ')<<setw(20) << (*iter).GetField(i)->GetData();
        }
        } 
        }
        cout << "|" << endl;
        cout << left << setfill('-') << setw(size_table) << '-';
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
  Row *row = new Row(fields_);
  table_heap->InsertTuple(*row, nullptr);
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
      if (index_->InsertEntry(row_index, row->GetRowId(), nullptr) == DB_SUCCESS) {
        //printf("[INFO] Insert successfully!\n");
        //return DB_SUCCESS;
      } else {
        table_heap->MarkDelete(row->GetRowId(),nullptr);
        printf("[INFO] Insert failed!\n");
        return DB_FAILED;
      }
    }
  }
  printf("[INFO] Insert successfully!\n");
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
    for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); ++iter) {
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

    for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); ++iter) {
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

  DBStorageEngine *engine = (dbs_.find(current_db_))->second;
  CatalogManager *cata = engine->catalog_mgr_;
  string table_name;
  pSyntaxNode tmp = ast->child_;
  pSyntaxNode tmp2 = ast->child_->next_->next_;
  table_name = tmp->val_;
  TableInfo *table_info;
  cata->GetTable(table_name, table_info);

  std::vector<IndexInfo *> indexes;
  cata->GetTableIndexes(table_name, indexes);

  TableHeap *table_heap = table_info->GetTableHeap();

  tmp = ast->child_;
  table_name = tmp->val_;
  std::vector<Field *> fields_;
  Schema *schema = table_info->GetSchema();
  bool res = true;
  std::unordered_map<string,Field*> map_;

  tmp = tmp->next_;
  tmp = tmp->child_;
  
  while (tmp!=NULL){
        Field *fie = NULL;
        pSyntaxNode tmp_ = tmp->child_->next_;
        
        if (tmp_->type_ == kNodeNumber){
          if (isFloat(tmp_->val_)){

            fie = new Field(kTypeFloat, StringToFloat(tmp_->val_));
          } else {
            fie = new Field(kTypeInt, (int32_t)StringToInt(tmp_->val_));
          }
        } else if (tmp_->type_ == kNodeString) {
          fie = new Field(kTypeChar, tmp_->val_, strlen(tmp_->val_), false);
        } else if (tmp_->type_ == kNodeNull) {
          fie = new Field(kTypeInvalid);
        }
        map_.insert(map<string, Field*>:: value_type(tmp->child_->val_, fie));
        tmp = tmp->next_;
      }
  
  if (tmp2 == NULL){// no conditionsiter++
  
    for (auto iter = table_heap->Begin(NULL); iter!=table_heap->End(); ++iter){
      //cout << "7777" << endl;
      for (auto index : indexes){
          Index* idx = index->GetIndex();
          std::vector<Field> fields_1;
          std::vector<Field> fields_2;
          IndexMetadata * meta = index->GetMetadata();
          std::vector<uint32_t> key_map = meta->GetKeyMapping();
          for (auto id: key_map){
            fields_1.push_back(*(iter->GetField(id)));
            if(map_.count(schema->GetColumn(id)->GetName())){
              fields_2.push_back(*map_[schema->GetColumn(id)->GetName()]);
            }
            else {
              fields_2.push_back(*(iter->GetField(id)));
            }
          }
          Row delete_row(fields_1);
          Row insert_row(fields_2);
          RowId tmp(iter->GetRowId());
          idx->RemoveEntry(delete_row, tmp, NULL);
          idx->InsertEntry(insert_row, tmp, NULL);
      }
    
      std::vector<Field> fields_;
  
      for (uint32_t i=0; i<schema->GetColumnCount();i++){
        //cout << schema->GetColumnCount() << endl;
        if (map_.count(schema->GetColumn(i)->GetName())) {
          fields_.push_back(*map_[schema->GetColumn(i)->GetName()]);
        }
        else {
          fields_.push_back(*((*iter).GetField(i)));
        }
      }
      Row row(fields_);

      res*=table_heap->UpdateTuple(row, iter->GetRowId(), NULL);

  }
  
  if (res){
    cout << "[INFO] Update successfully!" << endl;
    return DB_SUCCESS;
  }
    
  else{
    cout << "[INFO] Update failed" << endl;
    return DB_FAILED;
  }
}
  //condition
      
    for (auto iter = table_heap->Begin(NULL); iter!=table_heap->End(); ++iter){
      bool flag=DFS(ast->child_->next_->next_->child_, iter, schema);
      if (flag){
        for (auto index : indexes){
          Index* idx = index->GetIndex();
          std::vector<Field> fields_1;
          std::vector<Field> fields_2;
          IndexMetadata * meta = index->GetMetadata();
          std::vector<uint32_t> key_map = meta->GetKeyMapping();
          for (auto id: key_map){
            fields_1.push_back(*(iter->GetField(id)));
            if(map_.count(schema->GetColumn(id)->GetName())){
              fields_2.push_back(*map_[schema->GetColumn(id)->GetName()]);
            }
            else {
              fields_2.push_back(*(iter->GetField(id)));
            }
          }
          Row delete_row(fields_1);
          Row insert_row(fields_2);
          RowId tmp = iter->GetRowId();
          idx->RemoveEntry(delete_row, tmp, NULL);
          idx->InsertEntry(insert_row, tmp, NULL);
      }
        std::vector<Field> fields_;
      
      for (uint32_t i=0; i<schema->GetColumnCount();i++){
        if (map_.count(schema->GetColumn(i)->GetName())){
          
          fields_.push_back(*map_[schema->GetColumn(i)->GetName()]);
        }
        else {
          fields_.push_back(*(*iter).GetField(i));
        }
      }
      Row row(fields_);
      res *= table_heap->UpdateTuple(row, iter->GetRowId(), NULL);
    }
  }
  if (res) {
    cout << "[INFO] Update successfully!" << endl;
    return DB_SUCCESS;
  }

  else {
    cout << "[INFO] Update failed" << endl;
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name = ast->child_->val_;
  fstream stream;
  stream.open(file_name);
  cout << stream.is_open() << endl;

  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];

  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  [[maybe_unused]] uint32_t syntax_tree_id = 0;
  double total_time = 0;

  int count = 0;
  while (1) {
    stream.getline(cmd, 1025);
    count++;
    cout<<count<<endl;

    // read from buffer
    //cout << cmd << endl;

    // InputCommand(cmd, buf_size);
    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
#ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
#endif
    }

    ExecuteContext context;
    auto t1 = Clock::now();
    Execute(MinisqlGetParserRootNode(), &context);
    auto t2 = Clock::now();
    total_time = total_time + std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();
    //sleep(1);

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    if (context.flag_quit_) {
      printf("bye!\n");
      break;
    }
    if (stream.eof()) {
      break;
    }
  }
  cerr<<'\n'<<"total time:"<<total_time / 1e+6<<"ms\n";
  return DB_SUCCESS;
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
