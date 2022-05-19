#include "index/b_plus_tree.h"
#include <string>
#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  if (root_page_id_ == INVALID_PAGE_ID)
    return true;
  else
    return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  if (IsEmpty()) return false;

  auto *leaf_page = FindLeafPage(key, false);
  if (leaf_page != NULL) {
    ValueType tmp;
    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
    if (leaf->Lookup(key, tmp, comparator_)) {
      result.push_back(tmp);
      return true;
    }
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (!IsEmpty()) {
    if (InsertIntoLeaf(key, value))
      return true;
    else
      return false;
  } else {
    StartNewTree(key, value);
    return true;
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *root = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  UpdateRootPageId(true);
  root->Init(root_page_id_, INVALID_PAGE_ID);
  root->Insert(key, value, comparator_);
  
  buffer_pool_manager_->UnpinPage(root->GetPageId(),true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto *leaf_page = FindLeafPage(key);
  ValueType tmp;
  if (leaf_page == nullptr)  //没有复合插入条件的叶节点
    return false;
  else {
    auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
    if (leaf->Lookup(key, tmp, comparator_))  //在得到的叶节点中查找是否已存在该节点
      return false;
    else {
      //若结点未满，直接插入
      if (leaf->GetSize() < leaf->GetMaxSize()) {
        leaf->Insert(key, value, comparator_);
      } else {  //若结点已满，分裂出一个新节点
        auto *new_leaf = Split(leaf);
        //根据新叶节点的首元素和待插入元素的大小关系，决定要插入哪一个叶节点
        if (comparator_(key, new_leaf->KeyAt(0)) < 0)
          leaf->Insert(key, value, comparator_);
        else
          new_leaf->Insert(key, value, comparator_);
        //新的叶结点中含有原叶节点的后一半元素，因此按如下方式更新两个节点的信息
        new_leaf->SetNextPageId(leaf->GetNextPageId());
        leaf->SetNextPageId(new_leaf->GetPageId());
        //将分裂键递归上移
        InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf);
      }
    }
  }

  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {  // N表示要么为叶节点，要么为中间结点，因为分裂可以发生在叶节点或者中间节点
  page_id_t new_id;
  auto *page = buffer_pool_manager_->NewPage(new_id);

  /*if (page == NULL) {
    throw out_of_memory;
  }*/

  auto new_page = reinterpret_cast<N *>(page->GetData());
  new_page->Init(new_id);
  node->MoveHalfTo(new_page, buffer_pool_manager_);

  return new_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());

    new_root->Init(root_page_id_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());  //填充一个新的根节点

    old_node->SetParentPageId(root_page_id_);  //设置分裂后结点的父页
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(false);  //根节点内容发生了改变，更新根节点PageId

    buffer_pool_manager_->UnpinPage(old_node->GetPageId(),true);  //释放old_node和new_node
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
  } else {
    auto *page = buffer_pool_manager_->FetchPage(old_node->GetPageId());
    if (page != NULL) {
      auto *parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
      if (parent->GetSize() < parent->GetMaxSize()) {
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(parent->GetPageId());
      } else {
        auto tmp_key = parent->KeyAt(0);  //保存最左侧的指针
        auto tmp_value = parent->ValueAt(0);

        for (int i = 0; i <parent->GetSize() - 1; i++) {  //将parent中的元素整体左移
          parent->SetValueAt(i, parent->ValueAt(i + 1));
          parent->SetKeyAt(i, parent->KeyAt(i + 1));
        }

        parent->SetSize(parent->GetSize() - 1);  //左移后parent中元素数量减少

        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());  //在parent中插入新key
        auto *new_page = Split(parent);                                          //分裂parent

        parent->SetSize(parent->GetSize() + 1);         //右移parent前容量+1
        for (int i = parent->GetSize(); i >= 1; i--) {  //整体右移
          parent->SetKeyAt(i, parent->KeyAt(i - 1));
          parent->SetValueAt(i, parent->ValueAt(i - 1));
        }
        parent->SetKeyAt(0, tmp_key);  //恢复第一个pair
        parent->SetValueAt(0, tmp_value);

        for (int i = 0; i < new_page->GetSize() - 1; i++) {  //新建的结点整体左移
          new_page->SetKeyAt(i, new_page->KeyAt(i + 1));
          new_page->SetValueAt(i, new_page->ValueAt(i + 1));
        }

        new_page->SetParentPageId(parent->GetParentPageId());

        InsertIntoParent(parent, new_page->KeyAt(0), new_page);
        buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
      }
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }

  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  auto *leaf_page = FindLeafPage(key, false);
  auto *leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
  int tmp = leaf->GetSize();
  if (leaf->RemoveAndDeleteRecord(key, comparator_) < tmp)  //叶节点中存在该key值
    CoalesceOrRedistribute(leaf);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) return AdjustRoot(node);


  if (node->IsLeafPage()) {
    if (node->GetSize() >= node->GetMinSize()) {
      return false;
    }
  } else {
    if (node->GetSize() > node->GetMinSize()) {
      return false;
    }
  }


  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());  //获取父节点
  int index = parent->ValueIndex(node->GetPageId());

  int sibling_id;
  if (index == 0) {
    sibling_id = parent->ValueAt(index + 1);  //当指向node的指针在位置0时，sibling页需要使用下一个页
  } else {
    sibling_id = parent->ValueAt(index - 1);  //当指向node的指针不在位置0时，sibling页指向上一页
  }

  page = buffer_pool_manager_->FetchPage(sibling_id);
  auto sibling = reinterpret_cast<N *>(page->GetData());

  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
    if (index == 0) {  // sibling是node的下一个结点
      Redistribute(sibling, node, 0);
    } else {  // sibling是node的上一个结点
      Redistribute(sibling, node, 1);
    }
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return false;
  }

  if (index == 0) {  //因为MoveFirstToEndOf和MoveLastToFrontOf都是从前一个移动到后一个，因此sibling为下页时需要换位考虑
    Coalesce<N>(&node, &sibling, &parent, 1);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return false;  //换位后，被删除的是sibling，因此返回false
  } else {
    Coalesce<N>(&sibling, &node, &parent, index);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return true;  //不换位时，被删除的是node，返回false
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  KeyType middle_key = (*node)->KeyAt(index);
  auto *neighbor = reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator> *>(*neighbor_node);
  auto *Node = reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator> *>(*node);
  Node->MoveAllTo(neighbor,middle_key,buffer_pool_manager_);
  /*KeyType middle_key = node->KeyAt(index);

  if(node->IsLeafPage()){

    node->MoveAllTo(neighbor_node);
  }
  else{
    node = reinterpret_cast<N *>(*node);
    node->MoveAllTo(neighbor_node,middle_key,buffer_pool_manager_);
  }

  parent->Remove(index);

  CoalesceOrRedistribute(parent);  // deal with coalesce or redistribute recursively*/

  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  //获得middle_key
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
  int middle_index = parent->ValueIndex(node->GetPageId()) + 1;
  KeyType middle_key = parent->KeyAt(middle_index);

  buffer_pool_manager_->UnpinPage(parent->GetPageId(),false);
  if(node->IsLeafPage()){
    if (index == 0) {
      //node = reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator> *>(node);
      auto *Node = reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator> *>(node);
      neighbor_node->MoveFirstToEndOf(Node);  // sibling在后,移动sibling的第一个
      parent->SetKeyAt(middle_index,neighbor_node->KeyAt(1));
    } else {
      neighbor_node->MoveLastToFrontOf(node);  // sibling在前，移动sibling的后一个
    }
  }else{
    auto *neighbor_node = reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator> *>(*neighbor_node);
    if(index == 0) {
      neighbor_node->MoveFirstToEndOf(node,middle_key,buffer_pool_manager_);
    }else{
      neighbor_node->MoveLastToFrontOf(node,middle_key,buffer_pool_manager_);
    }
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // case 1
  if (old_root_node->GetSize() == 1) {
    auto old_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
    page_id_t one_last_child = old_root->ValueAt(0);
    UpdateRootPageId(false);

    auto *page = buffer_pool_manager_->FetchPage(one_last_child);
    if (page != NULL) {
      auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
      root_page_id_ = one_last_child;
      root->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
      return true;
    }
  }
  // case 2
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      // TODO:
      // root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      root_page_id_ = INVALID_PAGE_ID;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
/*INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType tmp;
  auto *leaf = FindLeafPage(tmp, true);

  return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, 0, buffer_pool_manager_);
}*/

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
/*INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto *leaf_page = FindLeafPage(key, false);
  auto leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
  int index = leaf->KeyIndex(key, comparator_);
  return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, index, buffer_pool_manager_);
}*/

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
/*INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() { return INDEXITERATOR_TYPE(); }*/

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // TODO:
  if (IsEmpty()) return nullptr;

  auto *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page != NULL) {
    auto *cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
    while (!cur_node->IsLeafPage()) {
      auto cur_inter = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(cur_node);
      page_id_t child_id;

      if (leftMost)  //找到子结点
        child_id = cur_inter->ValueAt(0);
      else
        child_id = cur_inter->Lookup(key, comparator_);

      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(),false);  //释放父节点

      cur_page = buffer_pool_manager_->FetchPage(child_id);  //下移至子节点

      cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
    }
    //auto res = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(cur_node);
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);

    return cur_page;
  }

  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  if (page != nullptr) {
    auto *header = reinterpret_cast<IndexRootsPage *>(page->GetData());
    if (insert_record) {
      header->Insert(index_id_, root_page_id_);
    } else {
      header->Update(index_id_, root_page_id_);
    }

    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template class BPlusTree<int, int, BasicComparator<int>>;

template class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
