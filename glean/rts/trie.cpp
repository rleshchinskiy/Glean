/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "glean/rts/trie.h"

namespace facebook {
namespace glean {
namespace rts {

namespace roart {

struct Node4 final : Node {
  unsigned char bytes[4];
  std::unique_ptr<Tree> children[4];

  void insert(
    Tree *parent,
    std::unique_ptr<Node>& ptr,
    unsigned char byte,
    folly::ByteRange key,
    const Fact *f) override;

  virtual void keys(
    std::string& buf, std::vector<std::string>& v) const override;

  virtual void validate() const override;
};

struct Node16 final : Node {
  unsigned char bytes[16];
  std::unique_ptr<Tree> children[16];

  void insert(
    Tree *parent,
    std::unique_ptr<Node>& ptr,
    unsigned char byte,
    folly::ByteRange key,
    const Fact *f) override;

  virtual void keys(
    std::string& buf, std::vector<std::string>& v) const override;

  virtual void validate() const override;
};

struct Node48 final : Node {
  unsigned char indices[256];
  std::unique_ptr<Tree> children[48];

  void insert(
    Tree *parent,
    std::unique_ptr<Node>& ptr,
    unsigned char byte,
    folly::ByteRange key,
    const Fact *f) override;

  virtual void keys(
    std::string& buf, std::vector<std::string>& v) const override;

  virtual void validate() const override;
};

struct Node256 final : Node {
  std::unique_ptr<Tree> children[256];

  void insert(
    Tree *parent,
    std::unique_ptr<Node>& ptr,
    unsigned char byte,
    folly::ByteRange key,
    const Fact *f) override;

  virtual void keys(
    std::string& buf, std::vector<std::string>& v) const override;

  virtual void validate() const override;
};

#if 0
struct Iterator {
  Tree * FOLLY_NULLABLE tree;
  bool here;
  folly::ByteRange prefix;
  size_t suffix;

  bool done() const {
    return tree == nullptr;
  }

  void next() {
    assert(!done);
    if (here) {
      here = false;
      // tree->node->seek(what?)
    } else {
      // what?
    }
  }
};
#endif

/*
Iterator Tree::seek(folly::ByteRange wanted) {
  const auto [p,k] = std::mismatch(
    prefix.begin(), prefix.end(),
    wanted.begin(), wanted.end()
  );
  if (k < wanted.end()) {
    if (p < prefix.end()) {
      // return null
    } else {
      // return node->seek({k, wanted.end()});
    }
  } else if (value) {
    // return here;
  } else {
    // return node->seek({});
  }
}
*/

void Tree::validate() const {
  if (!value && !node) {
    LOG(ERROR) << "Tree::validate: !fact && !node";
  }
  if (node) {
    node->validate();
  }
}

void Node4::validate() const {
  if (children[0] == nullptr) {
    LOG(ERROR) << "Node4::validate: empty";
  }
  auto b = bytes[0];
  for (auto i = 1; i < 4 && children[i]; ++i) {
    if (bytes[i] < b) {
      LOG(ERROR) << "Node4::validate: not sorted";
      break;
    } else if (bytes[i] == b) {
      LOG(ERROR) << "Node4::validate: duplicate";
      break;
    }
    b = bytes[i];
  }
  for (auto i = 0; i < 4 && children[i]; ++i) {
    children[i]->validate();
  }
}

void Node16::validate() const {
  if (children[0] == nullptr) {
    LOG(ERROR) << "Node16::validate: empty";
  }
  auto b = bytes[0];
  int i;
  for (i = 1; i < 16 && children[i]; ++i) {
    if (bytes[i] < b) {
      LOG(ERROR) << "Node16::validate: not sorted";
    } else if (bytes[i] == b) {
      LOG(ERROR) << "Node16::validate: duplicate";
    }
    b = bytes[i];
  }
  if (i < 5) {
    LOG(ERROR) << "Node16::validate: only " << i << "children";
  }
  for (auto i = 0; i < 16 && children[i]; ++i) {
    children[i]->validate();
  }
}

void Node48::validate() const {
  int idx = -1;
  size_t count = 0;
  for (auto i = 0; i < 256; ++i) {
    if (indices[i] != 0xFF) {
      if (indices[i] != idx+1) {
        LOG(ERROR) << "Node48::validate: index out of order";
      }
      idx = indices[i];
      ++count;
    }
  }
  if (count < 17) {
    LOG(ERROR) << "Node48::validate: only " << count << "children";
  }
  for (auto i = 0; i < count; ++i) {
    if (children[i]) {
      children[i]->validate();
    } else {
      LOG(ERROR) << "Node48::validate: missing child";
    }
  }
}

void Node256::validate() const {
  size_t count = 0;
  for (auto i = 0; i < 256; ++i) {
    if (children[i]) {
      ++count;
    }
  }
  if (count < 49) {
    LOG(ERROR) << "Node256::validate: only " << count << "children";
  }
  for (auto i = 0; i < 256; ++i) {
    if (children[i]) {
      children[i]->validate();
    }
  }  
}

void Tree::insert(folly::ByteRange key, const Fact *fact) {
  const auto [p,k] = std::mismatch(
    prefix.begin(), prefix.end(),
    key.begin(), key.end()
  );
  if (p < prefix.end()) {
    auto t = std::make_unique<Tree>();
    t->prefix = {p+1, prefix.end()};
    t->value = value;
    t->parent = this;
    t->byte = *p;
    t->node = std::move(node);
    auto c = std::make_unique<Node4>();
    if (k == key.end()) {
      value = fact;
      t->index = 0;
      c->bytes[0] = *p;
      c->children[0] = std::move(t);
    } else {
      value = nullptr;
      auto u = std::make_unique<Tree>();
      u->prefix = {k+1, key.end()};
      u->value = fact;
      u->parent = this;
      u->byte = *k;
      if (*p < *k) {
        c->bytes[0] = *p;
        c->bytes[1] = *k;
        t->index = 0;
        u->index = 1;
        c->children[0] = std::move(t);
        c->children[1] = std::move(u);
      } else {
        c->bytes[0] = *k;
        c->bytes[1] = *p;
        u->index = 0;
        t->index = 1;
        c->children[0] = std::move(u);
        c->children[1] = std::move(t);        
      }
    }
    prefix = {prefix.begin(), p};
    node = std::move(c);
  } else if (k < key.end()) {
    if (!node) {
      node = std::make_unique<Node4>();
    }
    node->insert(this, node, *k, {k+1,key.end()}, fact);
  } else {
    value = fact;
  }
}

void Node4::insert(
    Tree *parent,
    std::unique_ptr<Node>& ptr,
    unsigned char byte,
    folly::ByteRange key,
    const Fact *fact) {
  unsigned char i;
  for (i = 0; i < 4 && byte != bytes[i]; ++i) {}
  if (i < 4 && children[i]) {
    children[i]->insert(key, fact);
  } else {
    auto t = std::make_unique<Tree>();
    t->prefix = binary::mkString(key);
    t->value = fact;
    t->parent = parent;
    t->byte = byte;
    for (i = 0; i < 4 && children[i]; ++i) {}
    if (i < 4) {
      while (i > 0 && bytes[i-1] > byte) {
        bytes[i] = bytes[i-1];
        children[i] = std::move(children[i-1]);
        children[i]->index = i;
        --i;
      }
      t->index = i;
      bytes[i] = byte;
      children[i] = std::move(t);
    } else {
      auto c = std::make_unique<Node16>();
      for (i = 0; i < 4 && bytes[i] < byte; ++i) {
        c->bytes[i] = bytes[i];
        c->children[i] = std::move(children[i]);
      }
      t->index = i;
      c->bytes[i] = byte;
      c->children[i] = std::move(t);
      while (i<4) {
        c->bytes[i+1] = bytes[i];
        c->children[i+1] = std::move(children[i]);
        c->children[i+1]->index = i+1;
        ++i;
      }
      ptr = std::move(c);
    }
  }
}

void Node16::insert(
    Tree *parent,
    std::unique_ptr<Node>& ptr,
    unsigned char byte,
    folly::ByteRange key,
    const Fact *fact) {
  unsigned char i;
  for (i = 0; i < 16 && byte != bytes[i]; ++i) {}
  if (i < 16 && children[i]) {
    children[i]->insert(key, fact);
  } else {
    auto t = std::make_unique<Tree>();
    t->prefix = binary::mkString(key);
    t->value = fact;
    t->parent = parent;
    t->byte = byte;
    for (i = 4; i < 16 && children[i]; ++i) {}
    if (i < 16) {
      while (i > 0 && bytes[i-1] > byte) {
        bytes[i] = bytes[i-1];
        children[i] = std::move(children[i-1]);
        children[i]->index = i;
        --i;
      }
      t->index = i;
      bytes[i] = byte;
      children[i] = std::move(t);
    } else {
      auto c = std::make_unique<Node48>();
      std::fill(c->indices, c->indices+256, 0xFF);
      for (i = 0; i < 16 && bytes[i] < byte; ++i) {
        c->indices[bytes[i]] = i;
        c->children[i] = std::move(children[i]);
      }
      t->index = i;
      c->indices[byte] = i;
      c->children[i] = std::move(t);
      while (i < 16) {
        c->indices[bytes[i]] = i+1;
        c->children[i+1] = std::move(children[i]);
        c->children[i+1]->index = i+1;
      }
      ptr = std::move(c);
    }
  }
}

void Node48::insert(
    Tree *parent,
    std::unique_ptr<Node>& ptr,
    unsigned char byte,
    folly::ByteRange key,
    const Fact *fact) {
  if (indices[byte] != 0xFF) {
    children[indices[byte]]->insert(key,fact);
  } else {
    auto t = std::make_unique<Tree>();
    t->prefix = binary::mkString(key);
    t->value = fact;
    t->parent = parent;
    t->byte = byte;

    size_t i;
    for (i = 16; i < 48 && children[i]; ++i) {}
    if (i < 48) {
      for (unsigned char c = 0xFF; c > byte; --c) {
        if (indices[c] != 0xFF) {
          assert(indices[c] == i-1);
          children[i] = std::move(children[i-1]);
          indices[c] = i;
          children[i]->index = i;
          --i;
        }
      }
      indices[byte] = i;
      children[i] = std::move(t);
    } else {
      auto c = std::make_unique<Node256>();
      for (i = 0; i < 256; ++i) {
        if (indices[i] != 0xFF) {
          c->children[i] = std::move(children[indices[i]]);
        }
      }
      c->children[byte] = std::move(t);
      ptr = std::move(c);
    }
  }
}

void Node256::insert(
    Tree *parent,
    std::unique_ptr<Node>& ptr,
    unsigned char byte,
    folly::ByteRange key,
    const Fact *fact) {
  if (children[byte]) {
    children[byte]->insert(key,fact);
  } else {
    auto t = std::make_unique<Tree>();
    t->prefix = binary::mkString(key);
    t->value = fact;
    t->parent = parent;
    t->byte = byte;
    children[byte] = std::move(t);
  }
}

void Tree::keys(std::string& buf, std::vector<std::string>& v) const {
  buf.append(prefix.begin(), prefix.end());
  if (value) {
    v.push_back(buf);
  }
  if (node) {
    node->keys(buf,v);
  }
  buf.resize(buf.size() - prefix.size());
}

void Node4::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 4 && children[i]; ++i) {
    buf.push_back(bytes[i]);
    children[i]->keys(buf, v);
    buf.pop_back();
  }
}

void Node16::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 16 && children[i]; ++i) {
    buf.push_back(bytes[i]);
    children[i]->keys(buf, v);
    buf.pop_back();
  }
}

void Node48::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 256; ++i) {
    if (indices[i] != 0xFF) {
      buf.push_back(i);
      children[indices[i]]->keys(buf, v);
      buf.pop_back();
    }
  }
}

void Node256::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 256; ++i) {
    if (children[i]) {
      buf.push_back(i);
      children[i]->keys(buf, v);
      buf.pop_back();      
    }
  }
}

}

}
}
}