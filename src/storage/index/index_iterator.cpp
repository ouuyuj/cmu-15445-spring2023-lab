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
INDEXITERATOR_TYPE::IndexIterator() : current_page_id_(-1), index_(-1), current_() {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t current_page_id, int index)
    : bpm_(bpm), current_page_id_(current_page_id), index_(index) {
  if (current_page_id_ == -1 && index == -1) {
    current_ = {};
  } else {
    guard_ = bpm_->FetchPageRead(current_page_id_);
    current_leaf_page_ = guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    current_ = current_leaf_page_->GetMapPointorAt(index_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(const IndexIterator &itr)
    : bpm_(itr.bpm_), current_page_id_(itr.current_page_id_), index_(itr.index_) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return index_ == -1;
  throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  return *current_;
  throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ++index_;

  if (index_ < current_leaf_page_->GetSize()) {
    ++current_;
  } else {
    page_id_t next_page_id = current_leaf_page_->GetNextPageId();
    if (next_page_id != -1) {
      current_page_id_ = next_page_id;
      guard_ = bpm_->FetchPageRead(current_page_id_);
      current_leaf_page_ = guard_.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();

      index_ = 0;
      current_ = current_leaf_page_->GetMapPointorAt(index_);
    } else {
      current_page_id_ = -1;
      index_ = -1;
      current_ = {};
    }
  }

  return *this;
  throw std::runtime_error("unimplemented");
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
