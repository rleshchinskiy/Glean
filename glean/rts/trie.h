/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "glean/rts/fact.h"

namespace facebook {
namespace glean {
namespace rts {

namespace roart {

struct Tree;

struct Node {
  virtual ~Node() = default;
  virtual void insert(
    Tree *parent,
    std::unique_ptr<Node>& ptr,
    unsigned char byte,
    folly::ByteRange key,
    const Fact *f) = 0;

  virtual void keys(std::string& buf, std::vector<std::string>& v) const = 0;

  virtual void validate() const = 0;
};

// struct Iterator;

struct Tree final {
  std::string prefix;
  const Fact * FOLLY_NULLABLE value;

  Tree *parent;
  unsigned char byte;
  unsigned char index;

  std::unique_ptr<Node> node;

  void insert(folly::ByteRange key, const Fact *fact);
  // Iterator seek(folly::ByteRange prefix);

  void keys(std::string& buf, std::vector<std::string>& v) const;

  void validate() const;
};

}

}
}
}