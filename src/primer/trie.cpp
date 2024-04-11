#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto node = root_;

  if (node == nullptr) {
    return nullptr;
  }

  for (auto c : key) {
    if (node->children_.find(c) == node->children_.end()) {
      return nullptr;
    }

    node = node->children_.at(c);
  }

  if (!node->is_value_node_) {
    return nullptr;
  }
  auto node_value = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  return node_value ? node_value->value_.get() : nullptr;

  throw NotImplementedException("Trie::Get is not implemented.");
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // 总而言之就是先遍历要插入的字符串和trie,把遍历到的节点入栈,再遍历栈,
  // 修改节点的子节点可以复用,父节点依次复制指向上一次遍历复制的节点.
  auto trie = std::make_shared<Trie>();
  std::shared_ptr<TrieNode> node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  std::shared_ptr<TrieNode> new_node;

  if (this->root_ == nullptr) {
    for (auto it = key.rbegin(); it != key.rend(); ++it) {
      new_node = std::make_shared<TrieNode>(TrieNode(std::map<char, std::shared_ptr<const TrieNode>>{{*it, node}}));
      node = new_node;
    }

    trie->root_ = node;
  } else {
    std::shared_ptr<TrieNode> p;
    trie->root_ = p = this->root_->Clone();

    std::shared_ptr<TrieNode> pre_node = p;
    for (auto c : key) {
      if (p->children_.count(c)) {
        new_node = p->children_[c]->Clone();
        p->children_[c] = new_node;
      } else {
        new_node = std::make_shared<TrieNode>();
        p->children_[c] = new_node;
      }
      pre_node = p;
      p = new_node;
    }
    if (key.empty()) {
      node->children_ = this->root_->children_;
      trie->root_ = node;
    } else {
      if (!p->children_.empty()) {
        node->children_ = p->children_;
      }
      pre_node->children_[key.back()] = node;
    }
  }
  // You should walk through the trie and create new nodes if necessary.
  // If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  return *trie;

  throw NotImplementedException("Trie::Put is not implemented.");
}

auto Trie::Remove(std::string_view key) const -> Trie {
  std::vector<std::shared_ptr<const TrieNode>> path;  // 删除路径
  auto node = root_;
  path.emplace_back(node);

  for (auto c : key) {  // 路径入栈
    if (node->children_.count(c) != 0U) {
      path.emplace_back(node->children_.at(c));
    } else {
      return *this;
    }

    node = node->children_.at(c);
  }

  auto clone_node = std::make_shared<TrieNode>();
  std::size_t pos = key.length() - 1;

  bool copy_rest = false;

  if (path.back()->children_.empty()) {                                  // 删除尾结点
    for (auto it = path.rbegin() + 1; it != path.rend(); ++it, pos--) {  // 回溯
      if (((*it)->is_value_node_ || (*it)->children_.size() > 1) && !copy_rest) {
        copy_rest = true;
        clone_node = (*it)->Clone();
        clone_node->children_.erase(key[pos]);
      } else if (copy_rest) {
        auto next_node = clone_node;
        clone_node = (*it)->Clone();
        clone_node->children_[key[pos]] = next_node;
      }
    }
  } else {
    auto it = path.rbegin();

    if ((*it)->is_value_node_) {  // 将带值结点转换为非值结点
      clone_node = (*it)->Clone();
      auto new_node = std::make_shared<TrieNode>(clone_node->children_);
      clone_node = new_node;
    }

    for (it = it + 1; it != path.rend(); ++it, pos--) {
      auto next_node = clone_node;
      clone_node = (*it)->Clone();
      clone_node->children_[key[pos]] = next_node;
    }
  }

  return Trie(clone_node);

  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.

// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
