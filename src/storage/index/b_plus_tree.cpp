#include <cmath>
#include <cstdlib>
#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->FetchPageBasic(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();

  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(const InternalPage *interanl_page, const KeyType &key) const -> int {
  if (interanl_page == nullptr) {
    return -1;
  }

  int left = 1;
  int right = interanl_page->GetSize() - 1;
  int mid;

  if (comparator_(key, interanl_page->KeyAt(1)) == -1) {
    return 0;
  }

  while (left < right) {
    mid = (right + left + 1) / 2;
    if (comparator_(interanl_page->KeyAt(mid), key) != 1) {
      left = mid;
    } else {  // array_[mid] > key
      right = mid - 1;
    }
  }

  return right;
}

/**
 * @return index that target key >= first key
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(const LeafPage *leaf_page, const KeyType &key) const -> int {
  if (leaf_page == nullptr) {
    return -2;
  }
  if (comparator_(leaf_page->KeyAt(0), key) == 1) {
    return -1;
  }
  int left = 0;
  int right = leaf_page->GetSize() - 1;
  int mid;

  while (left < right) {
    mid = (right + left + 1) / 2;
    if (comparator_(leaf_page->KeyAt(mid), key) != 1) {
      left = mid;
    } else {  // array_[mid] > key
      right = mid - 1;
    }
  }

  return right;
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
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();

  ctx.root_page_id_ = header_page->root_page_id_;

  FindLeafNodeRead(key, &ctx);

  auto leaf_page = ctx.read_set_.back().template As<LeafPage>();
  int index = BinarySearch(leaf_page, key);

  if (comparator_(leaf_page->KeyAt(index), key) == 0) {
    result->emplace_back(leaf_page->ValueAt(index));
    ctx.read_set_.back().Drop();
    ctx.read_set_.pop_back();
    return true;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertOptimal(const KeyType &key, Context *ctx) -> void {
  page_id_t page_id = ctx->root_page_id_;
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "root page id is invalid page id");
  int index;
  const InternalPage *b_plus_tree_internal_page;

  // acquire root read lock and then release header lock
  {
    auto read_guard = bpm_->FetchPageRead(page_id);
    auto b_plus_tree_page = read_guard.As<BPlusTreePage>();
    ctx->read_set_.emplace_back(std::move(read_guard));

    if (!b_plus_tree_page->IsLeafPage()) {
      ctx->header_page_ = std::nullopt;
    }

    while (true) {
      if (!b_plus_tree_page->IsLeafPage()) {
        b_plus_tree_internal_page = reinterpret_cast<const InternalPage *>(b_plus_tree_page);
      } else {
        // b_plus_tree_page = reinterpret_cast<const LeafPage *> (b_plus_tree_page);
        ctx->read_set_.back().Drop();
        ctx->read_set_.pop_back();
        auto write_guard = bpm_->FetchPageWrite(page_id);
        b_plus_tree_page = write_guard.AsMut<LeafPage>();
        if (ctx->header_page_.has_value()) {
          ctx->header_page_ = std::nullopt;
        }
        while (!ctx->read_set_.empty()) {
          ctx->read_set_.front().Drop();
          ctx->read_set_.pop_front();
        }

        ctx->write_set_.emplace_back(std::move(write_guard));

        if (b_plus_tree_page->GetSize() < b_plus_tree_page->GetMaxSize()) {
          // fs << "optimism\tsize: " << b_plus_tree_page->GetSize() << "/" << b_plus_tree_page->GetMaxSize() << " is
          // leaf page: " << b_plus_tree_page->IsLeafPage() << " ";
          return;
        }
        break;
      }

      index = BinarySearch(b_plus_tree_internal_page, key);
      page_id = b_plus_tree_internal_page->ValueAt(index);

      read_guard = bpm_->FetchPageRead(page_id);  // leak of 58352 byte(s)
      b_plus_tree_page = read_guard.template As<BPlusTreePage>();
      ctx->read_set_.emplace_back(std::move(read_guard));
      if (ctx->read_set_.size() >= 2 && !b_plus_tree_page->IsLeafPage()) {
        ctx->read_set_.front().Drop();
        ctx->read_set_.pop_front();
      }
    }
  }

  // restart with pessimistic lock method
  while (!ctx->read_set_.empty()) {
    ctx->read_set_.front().Drop();
    ctx->read_set_.pop_front();
  }

  while (!ctx->write_set_.empty()) {
    ctx->write_set_.front().Drop();
    ctx->write_set_.pop_front();
  }

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  ctx->header_page_ = std::move(header_guard);
  ctx->root_page_id_ = header_page->root_page_id_;
  page_id = header_page->root_page_id_;

  auto write_guard = bpm_->FetchPageWrite(page_id);
  auto b_plus_tree_page_mut = write_guard.AsMut<BPlusTreePage>();
  ctx->write_set_.emplace_back(std::move(write_guard));

  // if root page is safe, release header page lock
  if (b_plus_tree_page_mut->GetSize() < b_plus_tree_page_mut->GetMaxSize()) {
    ctx->header_page_ = std::nullopt;
  }

  InternalPage *b_plus_tree_internal_page_mut = nullptr;

  // fs << "pessimistic\t";

  while (true) {
    // if safe, release top locks of this node
    if (b_plus_tree_page_mut->GetSize() < b_plus_tree_page_mut->GetMaxSize()) {
      if (ctx->header_page_.has_value()) {
        ctx->header_page_ = std::nullopt;
      }
      while (ctx->write_set_.size() >= 2) {
        ctx->write_set_.front().Drop();
        ctx->write_set_.pop_front();
      }
    }

    if (!b_plus_tree_page_mut->IsLeafPage()) {
      b_plus_tree_internal_page_mut = reinterpret_cast<InternalPage *>(b_plus_tree_page_mut);
    } else {
      break;
    }

    index = BinarySearch(b_plus_tree_internal_page_mut, key);
    page_id = b_plus_tree_internal_page_mut->ValueAt(index);

    write_guard = bpm_->FetchPageWrite(page_id);
    b_plus_tree_page_mut = write_guard.AsMut<BPlusTreePage>();
    ctx->write_set_.emplace_back(std::move(write_guard));
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafNodeRead(const KeyType &key, Context *ctx) const -> void {
  page_id_t page_id = ctx->root_page_id_;
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "root page id is invalid page id");
  int index;
  const BPlusTreePage *b_plus_tree_page;
  const InternalPage *b_plus_tree_internal_page;

  auto guard = bpm_->FetchPageRead(page_id);
  b_plus_tree_page = guard.As<BPlusTreePage>();
  ctx->read_set_.emplace_back(std::move(guard));

  ctx->read_header_page_ = std::nullopt;

  while (true) {
    // crabbing latch
    if (ctx->read_set_.size() >= 2) {
      ctx->read_set_.front().Drop();
      ctx->read_set_.pop_front();
    }

    if (!b_plus_tree_page->IsLeafPage()) {
      b_plus_tree_internal_page = reinterpret_cast<const InternalPage *>(b_plus_tree_page);
    } else {
      break;
    }

    index = BinarySearch(b_plus_tree_internal_page, key);
    page_id = b_plus_tree_internal_page->ValueAt(index);

    guard = bpm_->FetchPageRead(page_id);
    b_plus_tree_page = guard.As<BPlusTreePage>();
    ctx->read_set_.emplace_back(std::move(guard));
  }
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
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  bool flag = false;
  Context ctx;
  auto fs = std::fstream();

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();

  ctx.header_page_ = std::make_optional<WritePageGuard>(std::move(header_guard));
  // ctx.header_page_.value().Drop();
  ctx.root_page_id_ = header_page->root_page_id_;

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // it is a empty tree
    auto new_page_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
    auto new_leaf = new_page_guard.AsMut<LeafPage>();
    new_page_guard.Drop();

    auto write_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
    new_leaf = write_guard.AsMut<LeafPage>();

    new_leaf->Init(leaf_max_size_);
    new_leaf->InsertMap2Leaf(0, key, value);
    flag = true;
  } else {
    // 找到插入的leaf node index
    // fs.open("/home/heibai/projects/cmu-15445/project/bustub/test/storage/log.log", std::ios::app | std::ios::out);
    InsertOptimal(key, &ctx);

    // header_page = ctx.header_page_.AsMut<BPlusTreeHeaderPage>();

    // fs << "thread " << std::this_thread::get_id() << " | insert" << ": "
    //     << key << " [id: " << ctx.write_set_.back().PageId()
    //     << ", size: " << ctx.write_set_.back().As<BPlusTreePage>()->GetSize() << "/"
    //     << ctx.write_set_.back().As<BPlusTreePage>()->GetMaxSize() << "] | parent ids: ";
    // for (auto &page_guard : ctx.write_set_) {
    //   fs << page_guard.PageId() << " → ";
    // }

    auto leaf_page_guard = std::move(ctx.write_set_.back());
    auto leaf_page = leaf_page_guard.AsMut<LeafPage>();

    // if you try to reinsert an existing key into the index,
    // it should not perform the insertion, and should return false.
    if (comparator_(leaf_page->KeyAt(BinarySearch(leaf_page, key)), key) == 0) {
      // fs << "失败\n";
      return false;
    }

    auto optional = SplitLeaf(leaf_page, key, value);
    std::optional<std::pair<KeyType, page_id_t>> opt = optional;
    WritePageGuard write_guard;

    ctx.write_set_.pop_back();
    auto page_id = leaf_page_guard.PageId();

    if (optional.has_value()) {                        // leaf node split operation has happened
      if (ctx.IsRootPage(leaf_page_guard.PageId())) {  // leaf page is the root page
        auto &header_guard1 = ctx.header_page_.value();
        header_page = header_guard1.AsMut<BPlusTreeHeaderPage>();
        auto root_page_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
        auto root_page = root_page_guard.AsMut<InternalPage>();
        ctx.root_page_id_ = header_page->root_page_id_;
        root_page_guard.Drop();

        auto _ = bpm_->FetchPageWrite(header_page->root_page_id_);
        root_page = _.AsMut<InternalPage>();

        root_page->Init(internal_max_size_);
        root_page->InsertMap2Internal(1, optional.value());
        root_page->SetValueAt(0, page_id);
      } else if (!ctx.write_set_.empty()) {  // leaf page is not the root page
        leaf_page_guard.Drop();
        do {
          write_guard = std::move(ctx.write_set_.back());
          ctx.write_set_.pop_back();
          page_id = write_guard.PageId();
          opt = optional;
          optional = SplitInternal(write_guard.AsMut<InternalPage>(), optional.value());
          // fs << write_guard.AsMut<InternalPage>()->ToString() << "\n";
        } while (!ctx.write_set_.empty() && optional.has_value());

        if (ctx.write_set_.empty() && optional.has_value() && ctx.header_page_.has_value()) {  // root page is split
          auto &header_guard1 = ctx.header_page_.value();
          header_page = header_guard1.AsMut<BPlusTreeHeaderPage>();
          auto root_page_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
          auto root_page = root_page_guard.AsMut<InternalPage>();

          ctx.root_page_id_ = header_page->root_page_id_;
          root_page_guard.Drop();
          auto _ = bpm_->FetchPageWrite(header_page->root_page_id_);
          root_page = _.AsMut<InternalPage>();

          root_page->Init(internal_max_size_);
          root_page->InsertMap2Internal(1, optional.value());
          root_page->SetValueAt(0, page_id);
        }

        if (!optional.has_value()) {
          auto internal_page = write_guard.AsMut<InternalPage>();
          internal_page->InsertMap2Internal(BinarySearch(internal_page, opt.value().first) + 1, opt.value());
          // write_guard.Drop();
          // fs << "insert up key to internal" << internal_page->ToString() << std::endl;
        }
      }
      flag = true;
      // fs << "\n";
    } else {
      leaf_page->InsertMap2Leaf(BinarySearch(leaf_page, key) + 1, key, value);
      flag = true;
    }
  }
  // fs << "\n";

  return flag;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *internal_page, std::pair<KeyType, page_id_t> internal_pair)
    -> std::optional<std::pair<KeyType, page_id_t>> {
  if (internal_page == nullptr) {
    return std::nullopt;
  }

  if (internal_page->GetSize() == internal_max_size_) {
    page_id_t second_page_id;
    auto new_page_guard1 = bpm_->NewPageGuarded(&second_page_id);  // leak of 16672 byte(s)
    auto second_internal_page = new_page_guard1.AsMut<InternalPage>();
    new_page_guard1.Drop();

    auto write_guard = bpm_->FetchPageWrite(second_page_id);
    second_internal_page = write_guard.AsMut<InternalPage>();
    second_internal_page->Init(internal_max_size_);

    int pos = BinarySearch(internal_page, internal_pair.first);
    int first_node_size = static_cast<int>(ceil(internal_max_size_ / 2.0));

    std::pair<KeyType, page_id_t> father_pair;

    if (first_node_size - 1 == pos) {  // pair on the pos is move to the father page
      for (int i = first_node_size; i < internal_max_size_; i++) {
        second_internal_page->SequentialInsert(i - first_node_size + 1, internal_page->RemoveMapAt(i));
      }
      second_internal_page->SetValueAt(0, internal_pair.second);
      father_pair.first = internal_pair.first;
      father_pair.second = second_page_id;

    } else if (first_node_size - 1 > pos) {                         // insert to first node;
      for (int i = first_node_size; i < internal_max_size_; i++) {  // copy remaining to the second page;
        second_internal_page->SequentialInsert(i - first_node_size + 1, internal_page->RemoveMapAt(i));
      }

      second_internal_page->SetValueAt(0, internal_page->ValueAt(first_node_size - 1));
      internal_page->SetValueAt(first_node_size - 1, second_page_id);
      father_pair = internal_page->RemoveMapAt(first_node_size - 1);
      internal_page->InsertMap2Internal(pos + 1, internal_pair);
    } else {                                                            // insert to second node;
      for (int i = first_node_size + 1; i < internal_max_size_; i++) {  // copy remaining to the second page;
        second_internal_page->SequentialInsert(i - first_node_size, internal_page->RemoveMapAt(i));
      }
      second_internal_page->SetValueAt(0, internal_page->ValueAt(first_node_size));
      internal_page->SetValueAt(first_node_size, second_page_id);
      father_pair = internal_page->RemoveMapAt(first_node_size);
      second_internal_page->InsertMap2Internal(pos - first_node_size + 1, internal_pair);
    }

    return std::optional<std::pair<KeyType, page_id_t>>{std::move(father_pair)};
  }

  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf_page, const KeyType &key, const ValueType &value)
    -> std::optional<std::pair<KeyType, page_id_t>> {
  if (leaf_page == nullptr) {
    return std::nullopt;
  }

  if (leaf_page->GetSize() == leaf_max_size_) {
    page_id_t second_page_id;
    auto new_page_guard1 = bpm_->NewPageGuarded(&second_page_id);
    auto second_leaf_page = new_page_guard1.AsMut<LeafPage>();
    new_page_guard1.Drop();

    auto write_guard = bpm_->FetchPageWrite(second_page_id);
    second_leaf_page = write_guard.AsMut<LeafPage>();
    second_leaf_page->Init(leaf_max_size_);

    int pos = BinarySearch(leaf_page, key);
    // if (pos == -1) pos = 0;
    BUSTUB_ASSERT(pos != -2, "pos err");
    int first_node_size = static_cast<int>((ceil((leaf_max_size_) / 2.0)));

    std::pair<KeyType, page_id_t> internal_pair;

    MappingType leaf_pair;
    leaf_pair.first = key;
    leaf_pair.second = value;

    if (first_node_size - 1 <= pos) {  // insert to second node
      /// here can be optimized, the pos where will be insert can be set a empty pair, in other words, jump the pos when
      /// move reamaining values
      // move reamaining values from first node to second node
      for (int i = first_node_size; i < leaf_max_size_; i++) {
        second_leaf_page->SequentialInsert(i - first_node_size, leaf_page->RemoveMapAt(i));
      }
      second_leaf_page->InsertMap2Leaf(pos - first_node_size + 1, leaf_pair);
    } else {  // insert to first node
              // move reamaining values from first node to second node
      for (int i = first_node_size - 1; i < leaf_max_size_; i++) {
        second_leaf_page->SequentialInsert(i - first_node_size + 1, leaf_page->RemoveMapAt(i));
      }
      leaf_page->InsertMap2Leaf(pos + 1, leaf_pair);
    }

    // Copy the smallest search key value from second node to the parent node.(Right biased)
    internal_pair.first = second_leaf_page->KeyAt(0);
    internal_pair.second = second_page_id;

    second_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    // fs << " second_laef_page next p_id: " << leaf_page->GetNextPageId();
    leaf_page->SetNextPageId(second_page_id);
    // fs << " leaf_page next p_id: " << second_page_id<< " ";

    return std::optional<std::pair<KeyType, page_id_t>>{std::move(internal_pair)};
  }

  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveOptimal(const KeyType &key, Context *ctx) -> void {
  page_id_t page_id = ctx->root_page_id_;
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "root page id is invalid page id");
  int index;
  const InternalPage *b_plus_tree_internal_page;

  // acquire root read lock and then release header lock
  {
    auto read_guard = bpm_->FetchPageRead(page_id);
    auto b_plus_tree_page = read_guard.As<BPlusTreePage>();
    ctx->read_set_.emplace_back(std::move(read_guard));

    if (!b_plus_tree_page->IsLeafPage()) {
      ctx->header_page_ = std::nullopt;
    }

    while (true) {
      if (!b_plus_tree_page->IsLeafPage()) {
        b_plus_tree_internal_page = reinterpret_cast<const InternalPage *>(b_plus_tree_page);
      } else {
        ctx->read_set_.back().Drop();
        ctx->read_set_.pop_back();
        auto write_guard = bpm_->FetchPageWrite(page_id);
        b_plus_tree_page = write_guard.AsMut<LeafPage>();
        if (ctx->header_page_.has_value()) {
          ctx->header_page_ = std::nullopt;
        }
        while (!ctx->read_set_.empty()) {
          ctx->read_set_.front().Drop();
          ctx->read_set_.pop_front();
        }

        ctx->write_set_.emplace_back(std::move(write_guard));

        if (b_plus_tree_page->GetSize() - 1 >= b_plus_tree_page->GetMinSize()) {
          return;
        }
        break;
      }

      index = BinarySearch(b_plus_tree_internal_page, key);
      page_id = b_plus_tree_internal_page->ValueAt(index);

      read_guard = bpm_->FetchPageRead(page_id);
      b_plus_tree_page = read_guard.template As<BPlusTreePage>();
      ctx->read_set_.emplace_back(std::move(read_guard));
      if (ctx->read_set_.size() >= 2 && !b_plus_tree_page->IsLeafPage()) {
        ctx->read_set_.front().Drop();
        ctx->read_set_.pop_front();
      }
    }
  }

  // restart with pessimistic lock method
  while (!ctx->read_set_.empty()) {
    ctx->read_set_.front().Drop();
    ctx->read_set_.pop_front();
  }

  while (!ctx->write_set_.empty()) {
    ctx->write_set_.front().Drop();
    ctx->write_set_.pop_front();
  }

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  ctx->header_page_ = std::move(header_guard);
  ctx->root_page_id_ = header_page->root_page_id_;
  page_id = header_page->root_page_id_;

  auto write_guard = bpm_->FetchPageWrite(page_id);
  auto b_plus_tree_page_mut = write_guard.AsMut<BPlusTreePage>();
  ctx->write_set_.emplace_back(std::move(write_guard));

  // if root page is safe, release header page lock
  if (b_plus_tree_page_mut->GetSize() - 1 >= b_plus_tree_page_mut->GetMinSize()) {
    ctx->header_page_ = std::nullopt;
  }

  InternalPage *b_plus_tree_internal_page_mut = nullptr;

  while (true) {
    // if safe, release top locks of this node
    if (b_plus_tree_page_mut->GetSize() - 1 >= b_plus_tree_page_mut->GetMinSize()) {
      if (ctx->header_page_.has_value()) {
        ctx->header_page_ = std::nullopt;
      }
      while (ctx->write_set_.size() >= 2) {
        ctx->write_set_.front().Drop();
        ctx->write_set_.pop_front();
        ctx->index_set_.pop_front();
      }
    }

    if (!b_plus_tree_page_mut->IsLeafPage()) {
      b_plus_tree_internal_page_mut = reinterpret_cast<InternalPage *>(b_plus_tree_page_mut);
    } else {
      break;
    }

    index = BinarySearch(b_plus_tree_internal_page_mut, key);
    page_id = b_plus_tree_internal_page_mut->ValueAt(index);
    ctx->index_set_.push_back(index);

    write_guard = bpm_->FetchPageWrite(page_id);
    b_plus_tree_page_mut = write_guard.AsMut<BPlusTreePage>();
    ctx->write_set_.emplace_back(std::move(write_guard));
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  auto log = std::stringstream();

  // If current tree is empty, return immediately.
  if (IsEmpty()) {
    return;
  }

  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.header_page_ = std::make_optional<WritePageGuard>(std::move(guard));

  RemoveOptimal(key, &ctx);
  log << key << " write set size: " << ctx.write_set_.size() << " | "
      << "index set size: " << ctx.index_set_.size();
  auto leaf_page_guard = std::move(ctx.write_set_.back());
  auto leaf_page = leaf_page_guard.AsMut<LeafPage>();
  ctx.write_set_.pop_back();
  int index = BinarySearch(leaf_page, key);

  if (index < 0) {
    leaf_page_guard.Drop();
    while (!ctx.write_set_.empty()) {
      ctx.write_set_.front().Drop();
      ctx.write_set_.pop_back();
    }
    if (ctx.header_page_) {
      ctx.header_page_->Drop();
    }
    return;
  }

  if (comparator_(leaf_page->KeyAt(index), key) != 0) {
    leaf_page_guard.Drop();
    while (!ctx.write_set_.empty()) {
      ctx.write_set_.front().Drop();
      ctx.write_set_.pop_back();
    }
    if (ctx.header_page_) {
      ctx.header_page_->Drop();
    }
    return;
  }

  int leaf_page_size = leaf_page->GetSize();
  leaf_page->RemoveMapAt(index);
  for (int i = index; i < leaf_page_size - 1; i++) {
    leaf_page->Move(i + 1, i);
  }
  leaf_page->SetSize(leaf_page_size - 1);

  int father_page_index = -1;
  if (!ctx.index_set_.empty()) {
    father_page_index = ctx.index_set_.back();
    ctx.index_set_.pop_back();
  } else {
    if (ctx.header_page_ && ctx.IsRootPage(leaf_page_guard.PageId()) && leaf_page->GetSize() == 0) {
      auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

      page_id_t delete_page_id = leaf_page_guard.PageId();
      header_page->root_page_id_ = INVALID_PAGE_ID;

      bpm_->UnpinPage(delete_page_id, true);
      bpm_->DeletePage(delete_page_id);
    }
    leaf_page_guard.Drop();
    ctx.header_page_ = std::nullopt;
    return;
  }

  WritePageGuard father_guard;
  InternalPage *father_internal_page = nullptr;

  log << "write set size: " << ctx.write_set_.size() << " | "
      << "index set size: " << ctx.index_set_.size();
  LOG_DEBUG("%s", log.str().c_str());
  father_guard = std::move(ctx.write_set_.back());
  father_internal_page = father_guard.AsMut<InternalPage>();
  ctx.write_set_.pop_back();

  int leaf_min_size = static_cast<int>((ceil((leaf_max_size_) / 2.0)));
  std::optional<int> check_opt_merge = std::nullopt;
  std::optional<std::pair<KeyType, int>> pair;

  // try to steal key from sibling or merge two leaf node

  LeafPage *sibling = nullptr;
  std::optional<WritePageGuard> sibling_guard_opt = std::nullopt;
  // whether can be stole a key from right sibling
  if ((sibling_guard_opt = std::move(IsStoleFromRight(father_internal_page, father_page_index, leaf_min_size)))) {
    sibling = sibling_guard_opt.value().template AsMut<LeafPage>();
    MappingType map = sibling->RemoveMapAt(0);
    for (int i = 1; i <= sibling->GetSize(); i++) {
      sibling->Move(i, i - 1);
    }
    father_internal_page->SetKeyAt(father_page_index + 1, sibling->KeyAt(0));
    leaf_page->SequentialInsert(leaf_page->GetSize(), std::move(map));
  } else if ((sibling_guard_opt = std::move(IsStoleFromLeft(father_internal_page, father_page_index, leaf_min_size)))) {
    sibling = sibling_guard_opt.value().template AsMut<LeafPage>();
    MappingType map = sibling->RemoveMapAt(sibling->GetSize() - 1);
    father_internal_page->SetKeyAt(father_page_index, map.first);
    leaf_page->InsertMap2Leaf(0, std::move(map));
  } else {
    pair = MergeLeaf(leaf_page, father_internal_page, father_page_index);
    if (comparator_(pair.value().first, key) != 0) {
      check_opt_merge = Check(father_internal_page, pair.value().second, pair.value().first, &ctx);
      pair = std::nullopt;
    }
  }

  sibling_guard_opt = std::nullopt;
  leaf_page_guard.Drop();

  int is_check = 0;
  int check_opt_merge_flag = 0;
  std::optional<int> check_opt = std::nullopt;

  for (int i = 0; !ctx.write_set_.empty(); i++) {
    if (check_opt_merge.has_value()) {
      if (i == 1 && check_opt_merge_flag == 0 && check_opt_merge.has_value()) {
        check_opt = Check(father_internal_page, check_opt_merge.value(),
                          father_internal_page->KeyAt(check_opt_merge.value()), &ctx);
        check_opt_merge_flag = 1;
        father_guard = std::move(ctx.write_set_.back());
        father_internal_page = father_guard.AsMut<InternalPage>();
        father_page_index = ctx.index_set_.back();
        ctx.write_set_.pop_back();
        ctx.index_set_.pop_back();
        check_opt_merge = std::nullopt;
        continue;
      }
    }

    if (check_opt.has_value()) {
      check_opt = Check(father_internal_page, check_opt.value(), father_internal_page->KeyAt(check_opt.value()), &ctx);
    }

    if (is_check == 0) {
      check_opt = Check(father_internal_page, father_page_index, key, &ctx);
      if (check_opt != std::nullopt) {
        is_check = 1;
      }
    }
    father_guard = std::move(ctx.write_set_.back());
    father_internal_page = father_guard.AsMut<InternalPage>();
    father_page_index = ctx.index_set_.back();
    ctx.write_set_.pop_back();
    ctx.index_set_.pop_back();
  }

  // process root page here
  if (pair.has_value()) {
    check_opt_merge = Check(father_internal_page, pair.value().second, pair.value().first, &ctx);
  }

  if (check_opt.has_value() && check_opt.value() != -1) {
    auto remove_pair = father_internal_page->RemoveMapAt(check_opt.value());
    (void)remove_pair;
    for (int i = check_opt.value(); i < father_internal_page->GetSize(); i++) {
      father_internal_page->Move(i + 1, i);
    }
  }

  if (check_opt_merge.has_value() && check_opt_merge.value() != -1) {
    auto remove_pair = father_internal_page->RemoveMapAt(check_opt_merge.value());
    (void)remove_pair;
    for (int i = check_opt_merge.value(); i < father_internal_page->GetSize(); i++) {
      father_internal_page->Move(i + 1, i);
    }
  }

  // if root page size is equel to 1 delete this page
  if (father_internal_page->GetSize() == 1) {
    if (ctx.header_page_) {
      header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

      page_id_t delete_page_id = header_page->root_page_id_;
      header_page->root_page_id_ = father_internal_page->ValueAt(0);

      bpm_->UnpinPage(delete_page_id, true);
      bpm_->DeletePage(delete_page_id);
    }
  }

  father_guard.Drop();
  if (ctx.header_page_) {
    ctx.header_page_->Drop();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeLeaf(LeafPage *leaf_page, InternalPage *father_internal_page, int father_page_index)
    -> std::pair<KeyType, int> {
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  KeyType key;
  WritePageGuard guard;
  int index;
  if (father_page_index >= 1) {  // can merge the node into its left sibling node
    leaf_page_id = father_internal_page->ValueAt(father_page_index - 1);
    guard = bpm_->FetchPageWrite(leaf_page_id);
    auto left_sibling = guard.template AsMut<LeafPage>();
    int left_size = left_sibling->GetSize();
    int leaf_size = leaf_page->GetSize();
    for (int i = 0; i < leaf_size; i++) {
      left_sibling->SequentialInsert(i + left_size, leaf_page->RemoveMapAt(i));
    }

    left_sibling->SetNextPageId(leaf_page->GetNextPageId());

    key = father_internal_page->KeyAt(father_page_index);
    index = father_page_index;

    page_id_t delete_page_id = father_internal_page->ValueAt(father_page_index);
    bpm_->UnpinPage(delete_page_id, true);
    bpm_->DeletePage(delete_page_id);
  } else {  // can merge the node into its left sibling node
    leaf_page_id = father_internal_page->ValueAt(father_page_index + 1);
    guard = bpm_->FetchPageWrite(leaf_page_id);
    auto right_sibling = guard.template AsMut<LeafPage>();
    int right_size = right_sibling->GetSize();
    int leaf_size = leaf_page->GetSize();

    for (int i = 0; i < right_size; i++) {
      leaf_page->SequentialInsert(i + leaf_size, right_sibling->RemoveMapAt(i));
    }

    leaf_page->SetNextPageId(right_sibling->GetNextPageId());

    key = father_internal_page->KeyAt(father_page_index + 1);
    index = father_page_index + 1;

    page_id_t delete_page_id = father_internal_page->ValueAt(father_page_index + 1);
    bpm_->UnpinPage(delete_page_id, true);
    bpm_->DeletePage(delete_page_id);
  }

  return {key, index};
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeInternal(InternalPage *internal_page, InternalPage *father_internal_page,
                                   int father_page_index) -> int {
  // be merged right into left
  std::pair<KeyType, page_id_t> map;
  int ret = 0;
  if (father_internal_page->GetSize() - 1 > father_page_index) {
    auto guard = bpm_->FetchPageWrite(father_internal_page->ValueAt(father_page_index + 1));
    auto right_sibling = guard.template AsMut<InternalPage>();

    map.first = father_internal_page->KeyAt(father_page_index + 1);
    map.second = right_sibling->ValueAt(0);

    internal_page->SequentialInsert(internal_page->GetSize(), std::move(map));

    for (int i = 1; i < right_sibling->GetSize(); i++) {
      internal_page->SequentialInsert(internal_page->GetSize(), right_sibling->RemoveMapAt(i));
    }

    // delete internal key value pair
    page_id_t to_delete_page_id = father_internal_page->ValueAt(father_page_index + 1);
    bpm_->UnpinPage(to_delete_page_id, true);
    bpm_->DeletePage(to_delete_page_id);

    ret = 1;
  } else if (father_internal_page->GetSize() - 1 == father_page_index) {  // be merged left into it
    auto guard = bpm_->FetchPageWrite(father_internal_page->ValueAt(father_page_index - 1));
    auto left_sibling = guard.template AsMut<InternalPage>();

    map.first = father_internal_page->KeyAt(father_page_index);
    map.second = internal_page->ValueAt(0);

    int internal_size = internal_page->GetSize();

    for (int i = internal_size + left_sibling->GetSize() - 1; i > left_sibling->GetSize(); i--) {
      internal_page->Move(i - left_sibling->GetSize(), i);
    }
    internal_page->SetMapAt(left_sibling->GetSize(), std::move(map));

    auto left_size = left_sibling->GetSize();
    for (int i = left_size - 1; i >= 1; i--) {
      internal_page->SetMapAt(i, left_sibling->RemoveMapAt(i));
    }
    internal_page->SetValueAt(0, left_sibling->ValueAt(0));

    internal_page->SetSize(internal_size + left_size);

    page_id_t to_delete_page_id = father_internal_page->ValueAt(father_page_index - 1);
    father_internal_page->SetValueAt(father_page_index - 1, father_internal_page->ValueAt(father_page_index));
    bpm_->UnpinPage(to_delete_page_id, true);
    bpm_->DeletePage(to_delete_page_id);

    ret = -1;
  }

  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Check(InternalPage *internal_page, int index, const KeyType key, Context *ctx)
    -> std::optional<int> {
  // if Key at index eq key to be deleted, remove this key value pair in the internal page
  std::pair<KeyType, page_id_t> pair;
  std::optional<int> ret = std::nullopt;
  if (comparator_(internal_page->KeyAt(index), key) == 0) {
    if (ctx->write_set_.empty()) {
      ret = index;
      return ret;
    }

    pair = internal_page->RemoveMapAt(index);

    for (int i = index; i < internal_page->GetSize(); i++) {
      internal_page->Move(i + 1, i);
    }

    // if need to unpin the page been deleted
    int internal_min_size = static_cast<int>(ceil(internal_max_size_ / 2.0));

    // steal node from sibling or merge internal page if internal page size less than ceil(internal_max_size_ / 2.0)
    if (internal_page->GetSize() < internal_min_size) {
      InternalPage *father_internal_page = nullptr;
      WritePageGuard guard;
      int father_index = -1;

      if (!ctx->write_set_.empty()) {
        guard = std::move(ctx->write_set_.back());
        ctx->write_set_.pop_back();
        father_internal_page = guard.template AsMut<InternalPage>();
        father_index = ctx->index_set_.back();
        ctx->index_set_.pop_back();
      } else {
        return {};
      }

      if (StealInter(internal_page, index, father_internal_page, father_index)) {
      } else {
        int merge_flag;
        merge_flag = MergeInternal(internal_page, father_internal_page, father_index);
        // if merge internal page, the key value pair in the father page is not removed temporarily, to set return value
        // `ret` to point it
        if (merge_flag == 1) {
          ret = std::make_optional<int>(father_index + 1);
        }
        if (merge_flag == -1) {
          ret = std::make_optional<int>(father_index);
        }
      }

      ctx->write_set_.emplace_back(std::move(guard));
      ctx->index_set_.push_back(father_index);
    }
  }

  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StealInter(InternalPage *internal_page, int index, InternalPage *father_internal_page,
                                int father_index) -> bool {
  int internal_min_size = static_cast<int>(ceil(internal_max_size_ / 2.0));
  InternalPage *sibling = nullptr;
  std::optional<WritePageGuard> sibling_guard_opt{};
  bool ret = false;
  if ((sibling_guard_opt = std::move(IsStoleFromRightInter(father_internal_page, father_index, internal_min_size)))) {
    sibling = sibling_guard_opt.value().template AsMut<InternalPage>();
    internal_page->SetMapAt(internal_page->GetSize(), father_internal_page->KeyAt(1), sibling->ValueAt(0));
    internal_page->SetSize(internal_page->GetSize() + 1);
    father_internal_page->SetKeyAt(father_index + 1, sibling->KeyAt(1));
    sibling->SetValueAt(0, sibling->ValueAt(1));

    // move sibling page to fit its space
    sibling->RemoveMapAt(1);
    for (int i = 1; i < sibling->GetSize(); i++) {
      sibling->Move(i + 1, i);
    }

    ret = true;
  } else if ((sibling_guard_opt =
                  std::move(IsStoleFromLeftInter(father_internal_page, father_index, internal_min_size)))) {
    sibling = sibling_guard_opt.value().template AsMut<InternalPage>();
    for (int i = internal_page->GetSize(); i > 1; i--) {
      internal_page->Move(i - 1, i);
    }

    internal_page->SetSize(internal_page->GetSize() + 1);

    internal_page->SetKeyAt(1, father_internal_page->KeyAt(father_index));
    internal_page->SetValueAt(1, internal_page->ValueAt(0));
    internal_page->SetValueAt(0, sibling->ValueAt(sibling->GetSize() - 1));

    father_internal_page->SetKeyAt(father_index, sibling->KeyAt(sibling->GetSize() - 1));

    sibling->RemoveMapAt(sibling->GetSize() - 1);

    ret = true;
  }

  if (sibling_guard_opt) {
    sibling_guard_opt->Drop();
  }

  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsStoleFromRight(InternalPage *internal_page, int father_page_index, int leaf_min_size)
    -> std::optional<WritePageGuard> {
  if (father_page_index + 1 < internal_page->GetSize()) {
    auto guard = bpm_->FetchPageWrite(internal_page->ValueAt(father_page_index + 1));
    auto right_sibling = guard.template AsMut<LeafPage>();
    if (right_sibling->GetSize() > leaf_min_size) {
      return std::make_optional<WritePageGuard>(std::move(guard));
    }
  }

  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsStoleFromLeft(InternalPage *internal_page, int father_page_index, int leaf_min_size)
    -> std::optional<WritePageGuard> {
  if (father_page_index != 0) {
    auto guard = bpm_->FetchPageWrite(internal_page->ValueAt(father_page_index - 1));
    auto left_sibling = guard.template AsMut<LeafPage>();
    if (left_sibling->GetSize() > leaf_min_size) {
      return std::make_optional<WritePageGuard>(std::move(guard));
    }
  }

  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsStoleFromRightInter(InternalPage *internal_page, int father_page_index, int internal_min_size)
    -> std::optional<WritePageGuard> {
  if (father_page_index + 1 < internal_page->GetSize()) {
    auto guard = bpm_->FetchPageWrite(internal_page->ValueAt(father_page_index + 1));
    auto right_sibling = guard.template AsMut<InternalPage>();
    if (right_sibling->GetSize() > internal_min_size) {
      return std::make_optional<WritePageGuard>(std::move(guard));
    }
  }

  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsStoleFromLeftInter(InternalPage *internal_page, int father_page_index, int internal_min_size)
    -> std::optional<WritePageGuard> {
  if (father_page_index != 0) {
    auto guard = bpm_->FetchPageWrite(internal_page->ValueAt(father_page_index - 1));
    auto left_sibling = guard.template AsMut<InternalPage>();
    if (left_sibling->GetSize() > internal_min_size) {
      return std::make_optional<WritePageGuard>(std::move(guard));
    }
  }

  return std::nullopt;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  page_id_t root_page_id = bpm_->FetchPageRead(header_page_id_).As<BPlusTreeHeaderPage>()->root_page_id_;

  if (root_page_id == INVALID_PAGE_ID) {
    return End();
  }

  auto guard = bpm_->FetchPageRead(root_page_id);
  auto b_plus_tree_page = guard.As<BPlusTreePage>();

  if (b_plus_tree_page->IsLeafPage()) {
    return INDEXITERATOR_TYPE(bpm_, root_page_id, 0);
  }

  page_id_t subtree_page_id;

  do {
    subtree_page_id = reinterpret_cast<const InternalPage *>(b_plus_tree_page)->ValueAt(0);
    guard = bpm_->FetchPageRead(subtree_page_id);
    b_plus_tree_page = guard.As<BPlusTreePage>();
  } while (!b_plus_tree_page->IsLeafPage());

  return INDEXITERATOR_TYPE(bpm_, subtree_page_id, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  page_id_t page_id = bpm_->FetchPageRead(header_page_id_).As<BPlusTreeHeaderPage>()->root_page_id_;

  if (page_id == INVALID_PAGE_ID) {
    return End();
  }

  int index;
  const BPlusTreePage *b_plus_tree_page;
  const InternalPage *b_plus_tree_internal_page;

  while (true) {
    auto guard = bpm_->FetchPageRead(page_id);
    b_plus_tree_page = guard.As<BPlusTreePage>();

    if (!b_plus_tree_page->IsLeafPage()) {
      b_plus_tree_internal_page = reinterpret_cast<const InternalPage *>(b_plus_tree_page);
    } else {
      break;
    }

    index = BinarySearch(b_plus_tree_internal_page, key);
    page_id = b_plus_tree_internal_page->ValueAt(index);
  }
  auto leaf_page = reinterpret_cast<const LeafPage *>(b_plus_tree_page);
  index = BinarySearch(leaf_page, key);
  BUSTUB_ASSERT(index >= 0, "index less than 0");
  BUSTUB_ASSERT(comparator_(leaf_page->KeyAt(index), key) == 0, "key is not exist");

  return INDEXITERATOR_TYPE(bpm_, page_id, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->FetchPageBasic(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page Id: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page Id: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << "<" << internal->KeyAt(i) << "," << internal->ValueAt(i) << "> ";
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
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
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
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
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
