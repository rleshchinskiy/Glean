/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "glean/rts/fact.h"

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

  Node * FOLLY_NULLABLE root = nullptr;

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

    size_t nodes() const { return node0 + node4 + node16 + node48 + node256; }
  };

  struct Iterator {
    const Node * FOLLY_NULLABLE node = nullptr;
    std::string buf;
    size_t prefixlen = 0;

    bool done() const { return node == nullptr; }
    const std::string& getKey() const { return buf; }
    void next();

    static Iterator leftmost(
      const Node * FOLLY_NULLABLE node, std::string prefix);

    void down(unsigned char byte, const Node *node);

    bool operator==(const Iterator& other) const {
      return node == other.node;
    }

    bool operator!=(const Iterator& other) const {
      return node != other.node;
    }
  };

  friend struct Iterator;

  Tree() noexcept = default;
  Tree(Tree&& other) noexcept {
    root = other.root;
    other.root = nullptr;
  }
  Tree& operator=(Tree&& other) noexcept {
    if (this != &other) {
      clear();
      root = other.root;
      other.root = nullptr;
    }
    return *this;
  }
  ~Tree() noexcept {
    clear();
  }

  void clear() noexcept;

  Iterator begin() const;
  Iterator find(folly::ByteRange key) const;
  Iterator lower_bound(folly::ByteRange key) const;

  void insert(folly::ByteRange key, const Fact *fact);

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
    << " size: " << stats.bytes;
}

}

}
}
}