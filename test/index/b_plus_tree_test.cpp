#include "index/b_plus_tree.h"
#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 1000000;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }

  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }

  /*for(int i=0;i < n;i++)
    std::cout<<keys[i]<<' ';
  std::cout<<'\n';
  for(int i=0;i < n;i++)
    std::cout<<values[i]<<' ';
  std::cout<<'\n';
  for(int i=0;i < n;i++)
    std::cout<<delete_seq[i]<<' ';
  std::cout<<'\n';*/
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }

  ASSERT_TRUE(tree.Check());

  //int key[30]={11,16,24,2,7,25,27,8,5,9,6,0,18,4,17,15,26,20,13,22,28,19,3,1,21,12,29,23,14,10};
  //int value[30] = {21,3,23,10,6,0,18,15,16,11,5,4,13,24,25,17,8,9,28,19,29,22,27,12,26,1,2,20,14,7};
  //int DELETE[30]={24,7,14,17,5,19,25,16,26,0,18,20,13,23,3,22,8,10,12,15,2,9,29,28,1,27,21,4,6,11};

  //for(int i=0;i<30;i++)
    //tree.Insert(key[i],value[i]);
  
  
  ASSERT_TRUE(tree.Check());


  //tree.PrintTree(mgr[0]);
  
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
      tree.GetValue(i, ans);
      ASSERT_EQ(kv_map[i], ans[i]);
  }

  for (int i = 0; i < n/2; i++) {
      tree.Remove(delete_seq[i]);
  }

  ASSERT_TRUE(tree.Check());

  //tree.PrintTree(mgr[1]);

  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT(tree.GetValue(delete_seq[i], ans) == false,"Delete failed!");
  }

  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}
