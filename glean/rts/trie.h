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
    mutable Node0 *node0;

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

  struct Buffer {
    std::unique_ptr<unsigned char[]> buf;
    uint32_t capacity;
    uint32_t size;

    Buffer() : capacity(0), size(0) {}

    explicit Buffer(uint32_t cap)
      : buf(new unsigned char[cap])
      , capacity(cap)
      , size(0)
      {}

    Buffer(uint32_t cap, folly::ByteRange prefix) : Buffer(cap) {
      assert(prefix.size() <= cap);
      if (prefix.data() != nullptr) {
        std::memcpy(buf.get(), prefix.data(), prefix.size());
        size = static_cast<uint32_t>(prefix.size());
      }
    }

    void enter(const Node *node);
    void enter(unsigned char byte, const Node *node);
    void leave(const Node *node);
    void copyValue(const Node0 *node);

    const unsigned char *data() const {
      return buf.get();
    }

    folly::ByteRange get() const {
      return {buf.get(), size};
    }

    bool fits(size_t n) const {
      return n <= capacity - size;
    }
  };

  Buffer buffer() const {
    return Buffer(max_key_size + max_value_size);
  }

  Buffer buffer(folly::ByteRange prefix) const {
    return Buffer(max_key_size + max_value_size, prefix);
  }

  struct Iterator final : FactIterator {
    const Node0 * FOLLY_NULLABLE node = nullptr;
    Buffer buffer;
    size_t prefixlen = 0;

    Iterator() = default;
    Iterator(const Node0 *node, Buffer buffer, size_t prefixlen = 0);

    Id id() const;

    bool done() const { return node == nullptr; }
    folly::ByteRange getKey() const { return buffer.get(); }
    void next() override;

    Fact::Ref get(Demand demand = KeyValue) override;

    std::optional<Id> lower_bound() override { return {}; }
    std::optional<Id> upper_bound() override { return {}; }

    const Value *operator*() const;
    Iterator& operator++() {
      next();
      return *this;
    }

    bool operator==(const Iterator& other) const {
      return node == other.node;
    }

    bool operator!=(const Iterator& other) const {
      return node != other.node;
    }
  };

  friend struct Iterator;


  struct Cursor final {
    const Node * FOLLY_NULLABLE node = nullptr;
    Buffer buffer;
    size_t prefixlen = 0;

    Cursor() = default;
    Cursor(
      const Node * FOLLY_NULLABLE node,
      Buffer buffer,
      size_t prefixlen = 0);

    Cursor& down(unsigned char byte, const Node *child) &;
    Cursor&& down(unsigned char byte, const Node *child) &&;
    Iterator leftmost() &&;
    Iterator next() &&;

    unsigned char up();
  };

  Cursor cursorAt(folly::ByteRange prefix, const Node *node) const;

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