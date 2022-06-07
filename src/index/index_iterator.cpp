#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf,int index,BufferPoolManager *buff_pool_manager,int flag) {

  leaf_ = leaf;
  index_ = index;
  buff_pool_manager_ = buff_pool_manager;
  this->flag = flag;

}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {

  buff_pool_manager_->UnpinPage(leaf_->GetPageId(),false);

}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {

  return leaf_->GetItem(index_);

}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {

  if(index_ == leaf_->GetSize() - 1){
    page_id_t next_id = leaf_->GetNextPageId();
    buff_pool_manager_->UnpinPage(leaf_->GetPageId(),false);
    if(next_id != INVALID_PAGE_ID){
      auto *page = buff_pool_manager_->FetchPage(next_id);
      if(page != nullptr){
        auto next = reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> *>(page->GetData());
        index_ = 0;
        leaf_ = next;
      }
    }else index_++;
  }else{
    index_++;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {

  if(leaf_ == itr.leaf_ && index_ == itr.index_)
    return true;
  else 
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  
  if(leaf_ != itr.leaf_ || index_ !=itr.index_)
    return true;
  else
    return false;
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
