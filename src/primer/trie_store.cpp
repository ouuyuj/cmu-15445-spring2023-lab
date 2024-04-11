#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.

  this->root_lock_.lock();
  Trie trie = this->root_;
  this->root_lock_.unlock();

  const T *value = trie.Get<T>(key);

  if (value) {
    return ValueGuard<T>(trie, *value);
  }
  return std::nullopt;

  throw NotImplementedException("TrieStore::Get is not implemented.");
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  Trie trie;
  this->write_lock_.lock();

  trie = this->root_;

  // this->write_lock_.lock();
  Trie new_trie = trie.Put(key, std::move(value));
  // this->write_lock_.unlock();
  this->root_lock_.lock();
  this->root_ = new_trie;
  this->root_lock_.unlock();

  this->write_lock_.unlock();
  return;

  throw NotImplementedException("TrieStore::Put is not implemented.");
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  Trie trie;
  this->write_lock_.lock();

  trie = this->root_;

  // this->write_lock_.lock();
  Trie new_trie = trie.Remove(key);
  // this->write_lock_.unlock();

  this->root_lock_.lock();
  this->root_ = new_trie;

  this->root_lock_.unlock();
  this->write_lock_.unlock();
  return;

  throw NotImplementedException("TrieStore::Remove is not implemented.");
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
