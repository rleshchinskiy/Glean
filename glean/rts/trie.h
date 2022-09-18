/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "glean/rts/lookup.h"

#include <folly/Memory.h>

namespace facebook {
namespace glean {
namespace rts {

namespace roart {

class Tree final {
  struct Node;
  struct Node0;
  struct Node4;
  struct Node16;
  struct Node48;
  struct Node256;

public:
  struct Value {
    Id id;
    Pid type;
    uint32_t key_size;
    uint32_t value_size;
    mutable Node *node;

    Value() = default;
    explicit Value(Fact::Ref fact) {
      id = fact.id;
      type = fact.type;
      key_size = fact.clause.key_size;
      value_size = fact.clause.value_size;
    }

    folly::ByteRange value() const;

    Fact::Ref get(
      FactIterator::Demand demand,
      std::vector<unsigned char>& buf) const;
  };

private:
  struct Allocator;

  Node * FOLLY_NULLABLE root = nullptr;
  Allocator * allocator;
  uint32_t max_key_size = 0;
  uint32_t max_value_size = 0;
  size_t count = 0;
  size_t keymem = 0;

public:
  struct Stats {
    size_t node0 = 0;
    size_t node4 = 0;
    size_t node16 = 0;
    size_t node48 = 0;
    size_t node256 = 0;

    size_t node4_children = 0;
    size_t node16_children = 0;
    size_t node48_children = 0;
    size_t node256_children = 0;

    size_t prefix_length = 0;

    size_t bytes = 0;
    size_t key_size = 0;
    size_t arena_size = 0;

    size_t nodes() const { return node0 + node4 + node16 + node48 + node256; }
  };

  struct Iterator final : FactIterator {
    const Node * FOLLY_NULLABLE node = nullptr;
    std::vector<unsigned char> buf;
    uint32_t key_size = 0;
    size_t prefixlen = 0;

    Iterator() = default;
    Iterator(
        const Node *node,
        uint32_t buf_size,
        folly::ByteRange start,
        size_t prefixlen = 0);

    Id id() const;

    bool done() const { return node == nullptr; }
    // const std::string& getKey() const { return buf; }
    folly::ByteRange getKey() const { return {buf.data(), key_size}; }
    void next() override;

    Fact::Ref get(Demand demand = KeyValue) override;

    std::optional<Id> lower_bound() override { return {}; }
    std::optional<Id> upper_bound() override { return {}; }

    void leftmost();

    static Iterator leftmost(
      const Node * FOLLY_NULLABLE node,
      uint32_t buf_size,
      folly::ByteRange prefix);
    static Iterator right(
      const Node * FOLLY_NULLABLE node,
      uint32_t buf_size,
      folly::ByteRange prefix);

    void down(unsigned char byte, const Node *node);

    const Value *operator*() const;
    Iterator& operator++() {
      next();
      return *this;
    }

    Iterator operator++(int) {
      auto i = *this;
      next();
      return i;
    }

    bool operator==(const Iterator& other) const {
      return node == other.node;
    }

    bool operator!=(const Iterator& other) const {
      return node != other.node;
    }
  };

  friend struct Iterator;

  using iterator = Iterator;
  using const_iterator = Iterator;

  Tree();
  Tree(const Tree& other) = delete;
  Tree(Tree&& other) noexcept {
    root = other.root;
    max_key_size = other.max_key_size;
    max_value_size = other.max_value_size;
    allocator = other.allocator;
    count = other.count;
    keymem = other.keymem;
    other.root = nullptr;
    other.count = 0;
    other.keymem = 0;
    other.max_key_size = 0;
    other.max_value_size = 0;
    other.allocator = nullptr;
  }
  Tree& operator=(const Tree& other) = delete;
  Tree& operator=(Tree&& other) noexcept {
    if (this != &other) {
      clear();
      root = other.root;
      max_key_size = other.max_key_size;
      max_value_size = other.max_value_size;
      allocator = other.allocator;
      count = other.count;
      keymem = other.keymem;
      other.root = nullptr;
      other.count = 0;
      other.keymem = 0;
      other.allocator = nullptr;
    }
    return *this;
  }
  ~Tree() noexcept;

  void clear() noexcept;

  bool empty() const noexcept {
    return root != nullptr;
  }

  size_t size() const noexcept {
    return count;
  }

  Iterator begin() const;
  Iterator end() const {
    return Iterator();
  }
  Iterator find(folly::ByteRange key) const;
  Iterator lower_bound(folly::ByteRange key) const;
  Iterator lower_bound(folly::ByteRange key, size_t prefix_size) const;

  const Value * FOLLY_NULLABLE insert(Fact::Clause clause, const Value *fact);

  std::vector<std::string> keys() const;

  void validate() const;

  Stats stats() const;

  void dump(std::ostream& s) const;
};

inline std::ostream& operator<<(std::ostream& s, const Tree& tree) {
  tree.dump(s);
  return s;
}

inline std::ostream& operator<<(std::ostream& s, const Tree::Stats& stats) {
  return s << "total: " << stats.nodes()
    << " n0: " << stats.node0
    << " n4: " << stats.node4
    << " n16: " << stats.node16
    << " n48: " << stats.node48
    << " n256: " << stats.node256
    << " size: " << stats.bytes
    << " plen: " << stats.prefix_length
    << " keysz: " << stats.key_size;
}

}

}
}
}