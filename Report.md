





<br/><br/><br/><br/><br/><br/><br/><br/><br/><br/>

# <center>**MiniSQL**<center>

#### <center>成员信息:<center>

巩德志 3200105088


<div STYLE="page-break-after: always;"></div>


## <center>目录<center>

### **第1章 MiniSQL总体框架**

**1.1** MiniSQL实现功能分析

**1.2** MiniSQL系统体系结构

**1.3** 设计语言与运行环境

### **第2章 MiniSQL各模块实现功能**

**2.1** Buffer and disk managaer

**2.2** Record manager

**2.3** Index manager

**2.4** Catalog manager

**2.5** Executor


### **第3章 MiniSQL各模块接口与详细实现**
**3.1** Buffer and disk managaer

**3.2** Record manager

**3.3** Index manager

**3.4** Catalog manager

**3.5** Executor

### **第4章 MiniSQL测试结果**
**4.1** 各模块测试样例与测试结果

**4.2** 验收样例测试结果

### **第5章 优化方法**



<div STYLE="page-break-after: always;"></div>

## <center>分工说明<center>

#### 1. Buffer and disk manager ----------------
#### 2. Record manager--------------------------
#### 3. Index manager---------------------------
#### 4. Catalog manager-------------------------
#### 5. Executor-----------------------------------

<div STYLE="page-break-after: always;"></div>

## <center>第一章<center>
### **1.1 MiniSQL实现功能分析**
### **1.2 MiniSQL系统体系结构**
### **1.3 设计语言与运行环境**

1. 设计语言： C++

2. 开发工具： VSCODE

3. 编译&开发环境： 

   WSL-Ubuntu 20.04

   gcc (Ubuntu 9.4.0-1ubuntu1~20.04.1) 9.4.0

   g++ (Ubuntu 9.4.0-1ubuntu1~20.04.1) 9.4.0

   GNU gdb (Ubuntu 9.2-0ubuntu1~20.04.1) 9.2

   cmake version 3.16.3

<div STYLE="page-break-after: always;"></div>

## <center>第二章<center>
### **2.1 Buffer and disk manager**
### **2.2 Record manager**
### **2.3 Index manager**
### **2.4 Catalog manager**

#### 2.4.1 对表、索引、目录源信息的序列化和反序列化

数据库中定义的表、索引和目录在内存中以`TableInfo`、`IndexInfo`和`CatelogInfo`的形式表现，分别维护了`TableMetadata`、`IndexMetadata`和`CatalogMeta`，各个源信息分别实现了序列化和反序列化，从而将表、索引和目录的所有定义信息持久化到数据库文件并在重启时从数据库文件中恢复。

#### 2.4.2 对表的维护和管理

##### 2.4.2.1 建立表

根据传入的表名、模式创建一个表，若创建成功，返回信息创建成功；若该表已经存在，则返回信息该表已经存在

##### 2.4.2.2 获取表

1. 传入参数表名，获得该表。若获得成功，则返回信息获得成功；若获得失败，则返回信息该表不存在。
2. 传入参数表的序号，获得该表。若获得成功，则返回信息获得成功；若获得失败，则返回信息该表不存在。
3. 获得目录下的所有表。若获得成功，则返回信息获得成功；若获得失败，则返回信息获得失败。

##### 2.4.2.3 删除表

根据传入的表名，删除该表。若删除失败，则返回信息该表不存在；若删除成功，则返回信息删除成功。

#### 2.4.3 对索引的维护和管理

##### 2.4.3.1 建立索引

根据传入的表名、索引名、键值，创建一个该表上的索引。若创建成功，则返回信息创建成功；若该表已经存在，则返回信息该表已经存在

##### 2.4.3.2 获得索引

传入表名、索引名，获得对应的索引。若获得成功，则返回信息获得成功；若获得失败，则返回信息获得失败。

##### 2.4.3.3 删除索引

传入表名、索引名，删除对应的索引。若删除成功，则返回信息删除成功；若删除失败，则返回信息获删除失败。

### **2.5 Executor**

#### 2.5.1 对数据库的维护和管理

1. 创建数据库
   根据语法树解析结果，创建数据库。若创建成功，返回信息创建成功；若创建失败，返回信息创建失败。

2. 删除数据库
   根据语法树解析结果，删除数据库。若删除成功，返回信息删除成功；若删除失败，返回信息删除失败。

3. 查看数据库
   根据语法树解析结果，查看数据库，打印所有数据库的名称。若查看成功，返回信息查看成功；若查看失败，返回信息查看失败。

4. 使用数据库

   根据语法树解析结果，使用该数据库。若使用成功，返回信息使用成功；若使用失败，返回信息该数据库不存在。

#### 2.5.2 对表的维护和管理

1. 查看表
   根据语法树解析结果，查看当前数据库中所有表，打印所有表名。若查看成功，返回信息查看成功；若查看失败，返回信息查看失败。
2. 创建表
   根据语法树解析结果，创建数据库。若创建成功，返回信息创建成功；若创建失败，返回信息创建失败。
3. 删除表
   根据语法树解析结果，删除表。若删除成功，返回信息删除成功；若删除失败，返回信息删除失败。

#### 2.5.3 对索引的维护和管理

1. 查看索引
   根据语法树解析结果，查看所有表上的所有索引，打印所有索引名。若查看成功，返回信息查看成功；若查看失败，返回信息查看失败。
2. 创建索引
   根据语法树解析结果，在表上创建索引。若创建成功，返回信息创建成功；若创建失败，返回信息创建失败。
3. 删除索引
   根据语法树解析结果，删除表上的索引。若删除成功，返回信息删除成功；若删除失败，返回信息删除失败。

#### 2.5.4 查找操作

根据语法树解析结果进行查找。查找分为四种情况，分别是无投影且无条件、有投影且无条件、无投影且有条件、有投影且有条件。当进行有条件查询时，判断是否可以通过索引查找。若查找成功，则返回信息查找成功，打印查找信息。若查找失败，则返回信息查找失败。

#### 2.5.5 插入操作

根据语法树解析结果插入数据，并更新索引。若插入成功，则返回信息插入成功。若插入失败，则返回信息插入失败。

#### 2.5.6 删除操作

根据语法树解析结果删除数据，并更新索引。若删除成功，则返回信息删除成功。若删除失败，则返回信息删除失败。

#### 2.5.7 更新操作

根据语法树解析结果更新数据，并判断是否需要更新索引。若更新的field为索引键值，则需要更新索引。若更新成功，则返回信息更新成功。若更新失败，则返回信息更新失败。

#### 2.5.8 终止操作

退出程序

<div STYLE="page-break-after: always;"></div>

## <center>第三章<center>
### **3.1 Buffer and disk manager**
### **3.2 Record manager**
### **3.3 Index manager**
### **3.4 Catalog manager**

#### 3.4.1 table

`uint32_t TableMetadata::SerializeTo(char *buf) const`

TableMetaData的序列化，将MAGIC_NUM、table_id_t、size_t、page_id_t、schema依次写入字节流buf，每次读出后buf增加相应的推进字节数，写入table_name前先写入其大小size_t，返回buf指针推进的字节数ofs



`uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap）`

TableMetaData的反序列化，将MAGIC_NUM、table_id_t、table_name、page_id_t、schema依次读出字节流buf，读出table_name前读出其大小size_t，并通过读出的各个参数构造TableMetaData。返回buf指针推进的字节数ofs



`uint32_t TableMetadata::GetSerializedSize() const`

获得TableMetaData的序列化长度，返回值等于序列化中的返回值。

#### 3.4.2 index

`uint32_t IndexMetadata::SerializeTo(char *buf) const`

IndexMetaData的序列化，将MAGIC_NUM、index_id_t、index_name、table_id_t、key_map依次写入字节流buf，每次读出后buf增加相应的推进字节数，写入index_name、key_map前先写入其大小size_t，返回buf指针推进的字节数ofs



`uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap)`

IndexMetaData的反序列化，将MAGIC_NUM、index_id_t、index_name、table_id_t、key_map依次读出字节流buf，读出index_name、key_map前读出其大小size_t，并通过读出的各个参数构造TableMetaData。返回buf指针推进的字节数ofs



`uint32_t IndexMetadata::GetSerializedSize() const`

获得IndexMetaData的序列化长度，返回值等于序列化中的返回值。

#### 3.4.3 catalog

`void CatalogMeta::SerializeTo(char *buf) const`

CatalogMeta的序列化，将MAGIC_NUM、table_meta_pages、index_meta_pages依次写入字节流buf，每次读入后buf增加相应的推进字节数，写入table_meta_pages、index_meta_pages前先写入其大小size_t



`CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap)`

CatalogMeta的反序列化，将MAGIC_NUM、table_meta_pages、index_meta_pages依次读出字节流buf，每次读入后buf增加相应的推进字节数，读出table_meta_pages、index_meta_pages前先读出其大小size_t，根据读出的参数构造catalogmeta。



`uint32_t CatalogMeta::GetSerializedSize() const`

获得CatalogMeta的序列化长度，返回值等于序列化中的buf推进buf长度。



`CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager, LogManager *log_manager, bool init)`

CatalogManager构造函数，将catalogmeta进行反序列化，从page里取出来。遍历所有表的数据页，反序列化得到tablemetadata，创建tableheap，并初始化tableinfo，并压入tables_ 和 table_names_ 。遍历所有索引的数据页，反序列化得到indexmetadata，初始化indexinfo，并压入index_names_ 和 indexes_ 。



`CatalogManager::~CatalogManager()`

将catalog_meta序列化罗盘，并删除堆



`dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,TableInfo *&table_info)`

调用GetNextTableId()得到新表id，push进table_names。调用内存池分配新页，调用TableInfo::Create给table_info分配内存，构造tablemetadata和tableheap，并初始化table_info，压入tables_。将tablemetadata和catalog_meta序列化落盘。



`dberr_t CatalogManager::GetTable(const string &table_name, TableInfo &table_info)`

遍历tables_names_ ，找到对应的table_id，返回*table_info* = tables_[table_id];



`dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo &table_info)`

返回*table_info* = tables_[table_id];



`dberr_t CatalogManager::GetTables(vector<TableInfo > &tables) const`

遍历tables_，压入tables。



`dberr_t CatalogManager::DropTable(const string &table_name)`

在table_names_ ，talbles_ ，table_meta_pages中删除该表，释放table_metadata的数据页，将更新后的catalog_meta序列化落盘。



`dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name, const std::vector<std::string> &index_keys, Transaction txn, IndexInfo &index_info)`

遍历index_names_ ，如果没有建立在table_name上的索引，则调用GetNextIndexId()，建立new_index，将new_index压入index_names_ 。如果存在建立在table_name上的索引，则将new_index压入index_names。调用内存池分配新页，调用IndexInfo::Create给index_info分配内存，构造indexmetadata，并初始化index_info，压入indexes_。将tablemetadata和catalog_meta序列化落盘。



`dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name, IndexInfo &index_info) const`

遍历index_names，查看是否有建立在该表上的索引。再遍历indexes，查看该索引是否存在。index_info = indexes_.find(new_index_id)->second;



`dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name)`

遍历index_names，查看是否有建立在该表上的索引。再遍历indexes，查看该索引是否存在。在index_names，indexes中删除该索引。释放数据页，将catalogmeta序列化落盘。



`dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo > &indexes) const`

遍历index_names， 找到对应表上的索引集，压入传入的indexes。



### **3.5 Executor**

 `dberr_t ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context);`

若dbs_ 中存在传入的db_name，返回创建失败。否则新建DBStorageEngine，并插入dbs_ 



 `dberr_t ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context);`

若dbs_ 中不存在传入的db_name，返回删除失败。否则在dbs_ 删除db_name，如果被删除是当前使用的数据库，则  current_db_.clear()



 `dberr_t ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context);`

判断dbs_ 是否为空。遍历dbs_，打印数据库名字



 `dberr_t ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context);`

判断db_name是否存在。将传入的db_name设为current_name



 `dberr_t ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context);`

在current_db中得到catalog，调用GetTables（）获得tables。遍历tables，打印表名。



 `dberr_t ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context);`

在current_db中得到catalog，遍历语法树，得到column_name, type_, length, table_position, 得到nullable, unique的状态，构造column，再调用TableSchema构造schema，最后调用 CreateTable创建新表。



 `dberr_t ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context);`

在current_db中得到catalog，调用DropTable，删除表。



 `dberr_t ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context);`

在current_db中得到catalog，遍历tables，调用GetTableIndexes获得每张表上的表名并打印。



 `dberr_t ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context);`

在current_db中得到catalog，遍历语法树，得到table_name, index_name, index_keys，再调用CreateIndex构造indexinfo。遍历堆表，构造key_map上的entry，调用InsertEntry插入数据库条目。



 `dberr_t ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context);`

在current_db中得到catalog，调用GetTables获得tables，遍历tables，找到索引所在的表，调用DropIndex删除索引。



`bool DFS(pSyntaxNode ast, TableIterator &iter, Schema *schema)`

为了实现sql语句中的条件判断，在此实现了判断函数DFS。传入首个类型为kNodeCompareOperator或kNodeConnector的语法树节点ast，table迭代器iter，表的schema。递归方式如下，若connector为and，则返回子节点和子节点的右节点的返回结果的交集。若connector为or，则返回子节点和子节点的右节点的返回结果的并集。

```c++
if (strcmp(connector, "and") && DFS(ast->child_, iter, schema) && DFS(ast->child_->next_, iter, schema))
      return true;
    else if (strcmp(connector, "or") && (DFS(ast->child_, iter, schema) || DFS(ast->child_->next_, iter, schema)))
      return true;
```

递归出口为ast类型为kNodeCompareOperator，根据操作符不同进行不同的判断

```c++
if (strcmp(item, "=") == 0 && strcmp(l_value, r_value) == 0)
      return true;
    else if (strcmp(item, ">") == 0 && strcmp(l_value, r_value) > 0)
      return true;
    else if (strcmp(item, ">=") == 0 && strcmp(l_value, r_value) >= 0)
      return true;
    else if (strcmp(item, "<=") == 0 && strcmp(l_value, r_value) <= 0)
      return true;
    else if (strcmp(item, "<") == 0 && strcmp(l_value, r_value) < 0)
      return true;
    else if (strcmp(item, "<>") == 0 && strcmp(l_value, r_value) != 0)
      return true;
```



 `dberr_t ExecuteSelect(pSyntaxNode ast, ExecuteContext *context);`

解析语法树，select分为四种情况，分别是无投影且无条件、有投影且无条件、无投影且有条件、有投影且有条件。

无投影无条件时，调用堆表迭代器，打印每条column的field。

有投影无条件时，记录投影的column_name，调用堆表迭代器，打印投影column_name对应的field

无投影有条件时，根据条件判断是否可以通过索引加速查找。调用堆表迭代器，调用DFS判断row是否符合条件，打印每条column的field。

有投影有条件时，记录投影的column_name，根据条件判断是否可以通过索引加速查找。调用堆表迭代器，调用DFS判断row是否符合条件，打印投影column_name对应的field



 `dberr_t ExecuteInsert(pSyntaxNode ast, ExecuteContext *context);`

解析语法树，读入每一个field，构造row，调用InsertTuple插入tuple。调用InsertEntry插入索引条目。



 `dberr_t ExecuteDelete(pSyntaxNode ast, ExecuteContext *context);`

解析语法树，读入每一个field，如果有条件约束，调用DFS判断是否满足条件。读入每一个field，构造row，调用UpdateTuple更新tuple。调用InsertEntry、RemoveEntry来更新索引条目。



 `dberr_t ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context);`

解析语法树，读入每一个field，如果有条件约束，调用DFS判断是否满足条件。读入每一个field，构造row，调用MarkDelete删除tuple。调用RemoveEntry删除索引条目。



 `dberr_t ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context);`

按行从文件中读入sql语句，借用main中的程序运行sql语句。



 `dberr_t ExecuteQuit(pSyntaxNode ast, ExecuteContext *context);`

设置flag_quit，*context*->flag_quit_ = true;

<div STYLE="page-break-after: always;"></div>

## <center>第四章<center>
### **4.1 各模块测试样例说明及结果**

#### 4.1.4 catalog

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609204927157.png)

#### 4.1.5 executor

database相关操作和测试结果如下：

创建三个数据库db0, db1, db2

展示数据库

删除数据库db2

展示数据库

使用数据库db0

 ![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609111637656.png)



table相关操作和测试结果如下：

创建一系列表，格式不正确的表返回结果创建失败

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609112601961.png)



展示所有表

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609112649457.png)



插入三条数据

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609192612171.png)

不同的select操作

无投影无条件

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609192658193.png)

有投影无条件

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609195321719.png)

无投影有条件

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609195426645.png)

有投影有条件

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609215819029.png)

无条件更新

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609220123192.png)

查看结果

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609220141207.png)

有条件更新

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609220241127.png)

查看结果

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609220305430.png)

删除

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609220515992.png)

查看结果

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609220538309.png)

创建并查看索引

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609220741100.png)

删除索引

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609220844591.png)

删除表的全部内容

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609220949258.png)

删除表

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220609231810146.png)

### **4.2 验收样例测试结果**

1.创建三个数据库:
create database db0;
create database db1;
create database db2;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610182416263.png)

2.展示数据库:
show databases;
选定数据库:use db0;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610182458514.png)



3.创建两个表:
create table account(
id int,
name char(30) unique,
balance float,
primary key(id));

create table test(
id int,
cc float);

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610182609840.png)



4.展示所有表
show tables;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610182628286.png)



5.插入二十条记录
execfile "/mnt/e/minisql/src/exe.txt";

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610183334956.png)



6.显示全部的record查看插入结果
select * from account;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610183419286.png)



7.点查询操作:
select * from account where id = 12500008;
select * from account where balance = 57.41000;
select * from account where name = "name6";

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185323577.png)

select * from account where id <> 12500007;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185350320.png)

select * from account where balance <> 57.41000;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185415536.png)

select * from account where name <> "name0";

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185431086.png)

select * from account where id >= 12500006;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185446532.png)

select * from account where id <= 12500009;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185500210.png)



8.多条件查询与投影
select id,name from account where id>=12500004 and name <="name7";

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185600799.png)

select name,id from account where id>=12500004 and name <="name7" or id = 125000008;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185626429.png)

select balance from account where name <> "name8" and id <> 125000009 or id <= 12500004;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185649344.png)

9.唯一约束
insert into account values(12500000,"name21",23.3);
insert into account values(12500021,"name0",25.12);

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185745437.png)

10.删除
delete from account where id = 12500000;
delete from account where id >=12500004 or id <=12500001;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185827307.png)

11.更新
update account set balance = 12.5,name = "name22" where id = 12500002;
update account set balace = 17.5 where id = 12500003;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185913746.png)

12.索引
show indexes;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610185931215.png)

delete from account;

![image](https://images-tc.oss-cn-beijing.aliyuncs.com/20220610190023205.png)



<div STYLE="page-break-after: always;"></div>

## <center>第五章<center>

<div STYLE="page-break-after: always;"></div>



<script type="text/javascript" src="http://cdn.mathjax.org/mathjax/latest/MathJax.js?config=TeX-AMS-MML_HTMLorMML"></script>
<script type="text/x-mathjax-config">
  MathJax.Hub.Config({ tex2jax: {inlineMath: [['$', '$']]}, messageStyle: "none" });
</script>