# MiniSQL

## 项目简介

MiniSQL 是基于 CMU-15445 BusTub 思路改造的课程数据库系统实现，面向“从存储到执行”的完整链路实践。项目不仅实现了基础 SQL 解析与执行，还在页管理、索引、记录管理和系统目录等核心模块上做了工程化扩展。

项目重点在于把课堂中的数据库核心概念（页、缓冲池、B+ 树、目录与执行器）落地成可编译、可测试、可交互运行的系统原型。

## 项目架构

系统可分为五层：

- Parser 层
  - SQL 词法/语法分析
  - 语法树构建
- Executor 层
  - 按语法树调度具体执行逻辑
- Catalog 层
  - 维护表结构、索引等元数据，并支持持久化
- Storage 层
  - Record/Table 管理
  - Buffer Pool、Disk Manager、Page 读写
- Index 层
  - B+ 树索引结构与迭代器

典型执行流程：

1. 命令行输入 SQL
2. Parser 生成语法树
3. Executor 根据语法树执行操作
4. Catalog/Storage/Index 协作完成数据读写
5. 返回执行结果

## 项目特性

- 扩展 Disk Manager，支持位图页与元数据页的持久化管理
- Record / Index / Catalog 模块支持对象序列化与反序列化
- 重构 `Row`、`Field`、`Schema`、`Column` 等核心数据结构
- Catalog 模块支持持久化并为执行层提供统一接口
- Parser 可输出语法树供执行器使用
- 提供覆盖 Buffer、Index、Catalog、Record、Storage 的测试用例

## 目录结构

- `src/`: 主体源码（buffer/index/record/catalog/parser/executor/storage 等）
- `test/`: 单元测试
- `thirdparty/`: 第三方依赖（gtest/glog）
- `Report.md`: 课程报告

## 开发环境

- macOS: Apple clang 11.0+
- Linux: gcc / g++ 8.0+
- cmake 3.20+
- 可选: gdb、flex、bison

## 构建

```bash
mkdir build
cd build
cmake ..
make -j
```

构建产物说明：

- `build/bin/main`: MiniSQL 交互式执行程序
- `build/test/minisql_test`: 测试程序

Release 构建：

```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
```

## 运行

进入 `build` 目录后执行：

```bash
./bin/main
```

程序会进入 `minisql >` 交互提示符。SQL 语句以分号 `;` 结束，`quit;` 可退出。

## 测试

运行全部测试：

```bash
./test/minisql_test
```

构建并运行单个测试（示例）：

```bash
make lru_replacer_test
./test/buffer/lru_replacer_test
```

## 备注

- Windows 建议通过 WSL（Ubuntu）进行构建与调试。
- 该仓库为课程学习用途。
