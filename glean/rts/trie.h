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
  struct Node4;
  struct Node16;
  struct Node48;
  struct Node256;

public:
  struct Node0;

  static Fact::Ref get(
    const Node0 *node,
    FactIterator::Demand demand,
    std::vector<unsigned char>& buf);
  static folly::ByteRange value(const Node0 *node);
  struct FactInfo { Pid type; uint32_t key_size; uint32_t value_size; };
  static FactInfo info(const Node0 *node);
  static Pid type(const Node0 *node);

private:
  struct Impl;
  friend class Impl;

  struct Allocator;

  /*
  uintptr_t root = 0;
  // Node * FOLLY_NULLABLE root = nullptr;
  Allocator * allocator;
  Pid pid;
  uint32_t max_key_size = 0;
  uint32_t max_value_size = 0;
  size_t count = 0;
  size_t keymem = 0;
  */

  Impl * FOLLY_NULLABLE impl;

public:
  struct Stats {
    struct Ty {
      size_t count = 0;
      size_t bytes = 0;
      size_t children = 0;
    };

    Ty node0;
    Ty node4;
    Ty node16;
    Ty node48;
    Ty node256;

    size_t bytes = 0;
    size_t data_size = 0;
    size_t data_used = 0;
    size_t key_size = 0;
    size_t wasted = 0;

    Stats& operator+=(const Stats& other) {
      for (auto i = 0; i < sizeof(Stats) / sizeof(size_t); ++i) {
        reinterpret_cast<size_t *>(this)[i] +=
          reinterpret_cast<const size_t *>(&other)[i];
      }
      return *this;
    }

    Stats operator+(const Stats& other) const {
      Stats s(*this);
      s += other;
      return s;
    }
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

  /*
  Buffer buffer() const {
    return Buffer(max_key_size + max_value_size);
  }

  Buffer buffer(folly::ByteRange prefix) const {
    return Buffer(max_key_size + max_value_size, prefix);
  }
  */

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

    const Node0 * operator*() const {
      assert(node != nullptr);
      return node;
    }

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

  struct Cursor;
  /*
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
  */

  using iterator = Iterator;
  using const_iterator = Iterator;

  explicit Tree(Pid pid);
  Tree(const Tree& other) = delete;
  Tree(Tree&& other) noexcept {
    impl = other.impl;
    other.impl = nullptr;
  }
  Tree& operator=(const Tree& other) = delete;
  Tree& operator=(Tree&& other) noexcept {
    if (impl != other.impl) {
      impl = other.impl;
      other.impl = nullptr;
    }
    return *this;
  }
  ~Tree() noexcept;

  void clear() noexcept;

  /*
  bool empty() const noexcept {
    return root != 0;
  }

  size_t size() const noexcept {
    return count;
  }
  */

  bool empty() const noexcept;

  size_t size() const noexcept;

  Iterator begin() const;
  Iterator end() const {
    return Iterator();
  }
  Iterator find(folly::ByteRange key) const;
  Iterator lower_bound(folly::ByteRange key) const;
  Iterator lower_bound(folly::ByteRange key, size_t prefix_size) const;

  std::pair<const Node0 * FOLLY_NULLABLE, bool> insert(Id id, Fact::Clause clause);

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
  return s
    << " n0: " << stats.node0.count
    << " n4: " << stats.node4.count
    << " n16: " << stats.node16.count
    << " n48: " << stats.node48.count
    << " n256: " << stats.node256.count
    << " size: " << stats.bytes
    << " keysz: " << stats.key_size;
}

}

}
}
}