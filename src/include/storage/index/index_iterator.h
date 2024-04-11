//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For mk of b+ tree
 */
#pragma once
// #include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {  //  key value 在另外一页的问题
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(const IndexIterator &itr);
  IndexIterator(BufferPoolManager *bpm, page_id_t current_page_id, int index);

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    // KeyComparator key_comparator;
    return itr.current_ == current_;
    throw std::runtime_error("unimplemented");
  }

  // auto operator==(IndexIterator &itr) const -> bool {
  //   // KeyComparator key_comparator;
  //   return itr.current_ == current_;
  //   throw std::runtime_error("unimplemented");
  // }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return itr.current_ != current_;
    throw std::runtime_error("unimplemented");
  }

 private:
  // add your own private member variables here
  // const B_PLUS_TREE_LEAF_PAGE_TYPE *current_leaf_page_;
  BufferPoolManager *bpm_;
  page_id_t current_page_id_;
  int index_;
  MappingType *current_;
  ReadPageGuard guard_;
  const B_PLUS_TREE_LEAF_PAGE_TYPE *current_leaf_page_;
};

}  // namespace bustub
