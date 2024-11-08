/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t cur, int index) {
  bpm_ = bpm;
  index_ = index;
  cur_ = cur;
  if (cur != INVALID_PAGE_ID) {
    auto cur_page_guard = bpm_->FetchPageRead(cur_);
    auto cur_leaf_page = cur_page_guard.As<LeafPage>();
    item_ = {cur_leaf_page->KeyAt(index_), cur_leaf_page->ValueAt(index_)};
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return cur_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return item_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  auto cur_page_guard = bpm_->FetchPageRead(cur_);
  auto cur_leaf_page = cur_page_guard.As<LeafPage>();

  if (index_ < cur_leaf_page->GetSize()) {
    item_ = {cur_leaf_page->KeyAt(index_), cur_leaf_page->ValueAt(index_)};
  } else {
    cur_ = cur_leaf_page->GetNextPageId();
    if (cur_ != INVALID_PAGE_ID) {
      index_ = 0;
      cur_page_guard = bpm_->FetchPageRead(cur_);
      cur_leaf_page = cur_page_guard.As<LeafPage>();
      item_ = {cur_leaf_page->KeyAt(index_), cur_leaf_page->ValueAt(index_)};
    } else {
      index_ = -1;
      item_ = {};
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
