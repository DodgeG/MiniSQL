#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  if(index>=0&&index<GetSize())
    return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {

  if(index>=0&&index<GetSize())
    array[index].first = Key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for(int i=0;i<GetSize();i++){
    if(array[i].second == value) return i;
  }

  return i;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  if(index >=0 && index < GetSize())
    return array[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType& value){
  if(index>=0 && index<GetMaxSize())
    array_[index].second = value;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // replace with your own code
  int lim = GetSize();
  if(KeyComparator(key,array[1].first)<0)
    return array[0].second;
  if(KeyComparator(key,array[lim-1].first)>0)
    return array[lim-1].second;
  
  for(int i=1;i<lim;i++)
    if(KeyComparator(key,array[i].first) == 0)
      return array[i].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  IncreaseSize();

}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int lim = GetSize();
  for(i=0;i<lim;i++){
    if(array_[i].second == old_value)
      break;
  }
  for(int j = lim-1;j>i+1;j--){
    array_[j] = array_[j-1];
  }
  array_[i+1].first = new_key;
  array_[i+1].second = new_value;

  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  auto half = (GetSize() + 1)/2;
  recipient->CopyNFrom(array + GetSize() - half, half, buffer_pool_manager);

  IncreaseSize(-1*half);

}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {

  int start = GetSize();
  for(int i=0;i<size;i++)
    array_[start+i] = item[i];
  
  for (int i=0; i<size;i++) {
    auto *page = buffer_pool_manager->FetchPage(item[i].second);

    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());  //强制类型转换
    child->SetParentPageId(GetPageId());

    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }

  IncreaseSize(size);

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {

  if(index<0||index>=GetSize())
    cout<<"Error";

  for(int i=index;i<GetSize()-1;i++)
    array_[i] = array_[i+1];
  
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  IncreaseSize(-1);
  ValueType val = ValueAt(0);
  return val;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId()); //得到父页
  auto *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());  //获取父页数据并将其填充到类中

  array_[0].first = middle_key; //为了使第0个指针可以移入目标页

  buffer_pool_manager->UnpinPage(parent->GetPageId(),true);

  recipient->CopyNFrom(array_,GetSize(),buffer_pool_manager);



}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  MappingType first = {middle_key,ValueAt(0)};

  auto *Page = buffer_pool_manager->FetchPage(recipient->GetParentPageId());
  if(Page!=NULL){
    auto *parent = reinterpret_cast<BPlusTreeInternalPage *>(Page->GetData());
    int tmp_index = recipient->ValueIndex(recipient->GetPageId());
    recipient->array_[tmp_index+1].first = KeyAt(1);
    buffer_pool_manager->UnpinPage(parent->GetPageId(),true);
  }

  array_[0].second = array_[1].second;
  Remove(1);

  recipient->CopyLastFrom(first,buffer_pool_manager);

}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {

  array_[GetSize()] = pair;
  IncreaseSize(1);
  
  auto *page = buffer_pool_manager->FetchPage(pair.second);
  if(page!=NULL){
    auto *child = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(),true);
  }

}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  IncreaseSize(-1);
  MappingType last = {middle_key,ValueAt(GetSize())};

  auto *Page = buffer_pool_manager->FetchPage(recipient->GetParentPageId());
  if(Page!=NULL){
    auto *parent = reinterpret_cast<BPlusTreeInternalPage *>(Page->GetData());
    int tmp_index = recipient->ValueIndex(recipient->GetPageId());
    recipient->array_[tmp_index-1].first = KeyAt(GetSize());
    buffer_pool_manager->UnpinPage(parent->GetPageId(),true);
  }

  recipient->CopyFirstFrom(first,buffer_pool_manager);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {

  InsertNodeAfter(array_[0].second,pair.first,array_[0].second);
  array_[0].second = pair.second;
  IncreaseSize(1);

  auto *page = buffer_pool_manager->FetchPage(pair.second);
  if(page!=NULL){
    auto *child = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(),true);
  }


}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;