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

Tree::~Tree() noexcept {
  /*
  if (count != 0) {
    LOG(INFO) << "Tree::~Tree " << stats();
  }
  */
  clear();
}

struct Tree::Node {
  enum Type : unsigned char { N0, N4, N16, N48, N256 };
  Type type;

  unsigned char index;
  Node *parent;

  const Value * FOLLY_NULLABLE value;
  std::string prefix;

  explicit Node(Type ty) : type(ty) {}

  template<typename NodeT>
  static NodeT *alloc();
  template<typename NodeT>
  static NodeT *alloc(const Node& node);

  struct Child {
    const Node * FOLLY_NULLABLE node = nullptr;
    unsigned char byte;
  };

  void dealloc() {
    std::free(this);
  }

  void destroy();

  void seekTo(Iterator& iter, folly::ByteRange start) const;

  struct Insert {
    Node * FOLLY_NULLABLE * FOLLY_NULLABLE node = nullptr;
    union {
      folly::ByteRange::const_iterator keypos;
      const Value *fact;
    };
  };

  Insert insert(
    Node * FOLLY_NULLABLE &me,
    folly::ByteRange key,
    const Value *fact);

  const char *typeString() const;
  void validate() const;
  void validate(const Node *parent, unsigned int index, unsigned int byte) const;

  void keys(std::string& buf, std::vector<std::string>& v) const;

  void stats(Stats& s) const;

  void dump(std::ostream& s, int indent) const;

  template<typename F> auto dispatch(F&& f);
  template<typename F> auto dispatch(F&& f) const;
};

struct Tree::Node0 {
  Node node;

  Node0() : node(Node::N0) {}

  void destroy();

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;

  Node * FOLLY_NULLABLE * FOLLY_NULLABLE insert(
    Node * FOLLY_NULLABLE &me,
    unsigned char byte,
    folly::ByteRange key,
    const Value *f);

  void validate() const;
  void keys(std::string& buf, std::vector<std::string>& v) const;
  void keys(int idx, std::string& buf, std::vector<std::string>& v) const;

  void stats(Stats& s) const;

  void dump(std::ostream& s, int indent, const std::string& prefix) const;
};

struct Tree::Node4 {
  Node node;
  unsigned char bytes[4];
  Node * FOLLY_NULLABLE children[4];

  Node4() : node(Node::N4) {
    std::fill(children,children+4,nullptr);
  }

  void destroy();

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;

  Node * FOLLY_NULLABLE * FOLLY_NULLABLE insert(
    Node * FOLLY_NULLABLE &me,
    unsigned char byte,
    folly::ByteRange key,
    const Value *f);

  void validate() const;
  void keys(std::string& buf, std::vector<std::string>& v) const;
  void keys(int idx, std::string& buf, std::vector<std::string>& v) const;

  void stats(Stats& s) const;

  void dump(std::ostream& s, int indent, const std::string& prefix) const;
};

struct Tree::Node16 {
  Node node;
  unsigned char bytes[16];
  Node * FOLLY_NULLABLE children[16] = {};

  Node16() : node(Node::N16) {
    std::fill(children,children+16,nullptr);
  }

  void destroy();

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;

  Node * FOLLY_NULLABLE * FOLLY_NULLABLE insert(
    Node * FOLLY_NULLABLE &me,
    unsigned char byte,
    folly::ByteRange key,
    const Value *f);

  void validate() const;
  void keys(std::string& buf, std::vector<std::string>& v) const;
  void keys(int idx, std::string& buf, std::vector<std::string>& v) const;

  void stats(Stats& s) const;

  void dump(std::ostream& s, int indent, const std::string& prefix) const;
};

struct Tree::Node48 {
  Node node;
  unsigned char indices[256];
  unsigned char bytes[48];
  Node * FOLLY_NULLABLE children[48];

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;

  Node48() : node(Node::N48) {
    std::fill(indices,indices+256,0xff);
    std::fill(children,children+48,nullptr);
  }

  void destroy();

  Node * FOLLY_NULLABLE * FOLLY_NULLABLE insert(
    Node * FOLLY_NULLABLE &me,
    unsigned char byte,
    folly::ByteRange key,
    const Value *f);

  void validate() const;
  void keys(std::string& buf, std::vector<std::string>& v) const;
  void keys(int idx, std::string& buf, std::vector<std::string>& v) const;

  void stats(Stats& s) const;

  void dump(std::ostream& s, int indent, const std::string& prefix) const;
};

struct Tree::Node256 {
  Node node;
  Node * FOLLY_NULLABLE children[256];

  Node256() : node(Node::N256) {
    std::fill(children,children+256,nullptr);
  }

  void destroy();

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;

  Node * FOLLY_NULLABLE * FOLLY_NULLABLE insert(
    Node * FOLLY_NULLABLE &me,
    unsigned char byte,
    folly::ByteRange key,
    const Value *f);

  void validate() const;
  void keys(std::string& buf, std::vector<std::string>& v) const;
  void keys(int idx, std::string& buf, std::vector<std::string>& v) const;

  void stats(Stats& s) const;

  void dump(std::ostream& s, int indent, const std::string& prefix) const;
};

template<typename F>
auto Tree::Node::dispatch(F&& f) {
  switch(type) {
    case N0: return std::forward<F>(f)(reinterpret_cast<Node0*>(this));
    case N4: return std::forward<F>(f)(reinterpret_cast<Node4*>(this));
    case N16: return std::forward<F>(f)(reinterpret_cast<Node16*>(this));
    case N48: return std::forward<F>(f)(reinterpret_cast<Node48*>(this));
    case N256: return std::forward<F>(f)(reinterpret_cast<Node256*>(this));
    default: folly::assume_unreachable();
  }
}

template<typename F>
auto Tree::Node::dispatch(F&& f) const {
  switch(type) {
    case N0: return std::forward<F>(f)(reinterpret_cast<const Node0*>(this));
    case N4: return std::forward<F>(f)(reinterpret_cast<const Node4*>(this));
    case N16: return std::forward<F>(f)(reinterpret_cast<const Node16*>(this));
    case N48: return std::forward<F>(f)(reinterpret_cast<const Node48*>(this));
    case N256: return std::forward<F>(f)(reinterpret_cast<const Node256*>(this));
    default: folly::assume_unreachable();
  }
}

template<typename NodeT>
NodeT *Tree::Node::alloc() {
  auto p = static_cast<NodeT*>(std::malloc(sizeof(NodeT)));
  new(p) NodeT;
  return p;
}

template<typename NodeT>
NodeT *Tree::Node::alloc(const Node& node) {
  auto p = static_cast<NodeT*>(std::malloc(sizeof(NodeT)));
  new(p) NodeT;
  const auto type = p->node.type;
  p->node = node;
  p->node.type = type;
  if (p->node.value != nullptr) {
    p->node.value->node = &p->node;
  }
  return p;
}

void Tree::Node::destroy() {
  dispatch([](auto p) { p->destroy(); });
  std::free(this);
}

void Tree::Node0::destroy() {}

void Tree::Node4::destroy() {
  for (auto i = 0; i < 4 && children[i]; ++i) {
    children[i]->destroy();
  }
}

void Tree::Node16::destroy() {
  for (auto i = 0; i < 16 && children[i]; ++i) {
    children[i]->destroy();
  }
}

void Tree::Node48::destroy() {
  for (auto i = 0; i < 48 && children[i]; ++i) {
    children[i]->destroy();
  }
}

void Tree::Node256::destroy() {
  for (auto i = 0; i < 256; ++i) {
    if (children[i]) {
      children[i]->destroy();
    }
  }
}

Fact::Ref Tree::Value::get(
    FactIterator::Demand demand,
    std::vector<unsigned char>& buf) const {
  assert(node != nullptr);
  assert(node->value == this);
  const auto vsize = demand == FactIterator::Demand::KeyOnly ? 0 : value_size;
  buf.resize(key_size + vsize);
  auto pos = buf.begin() + key_size;
  if (vsize > 0) {
    std::copy(value().begin(), value().end(), pos);
  }
  auto current = node;
  while (current != nullptr) {
    assert(current->prefix.size() <= pos - buf.begin());
    pos -= current->prefix.size();
    std::copy (current->prefix.begin(), current->prefix.end(), pos);
    const auto index = current->index;
    current = current->parent;
    if (current != nullptr) {
      auto child =
        current->dispatch([&](const auto *p) { return p->at(index); });
      assert(child.node != nullptr);
      assert(pos > buf.begin());
      --pos;
      *pos = child.byte;
    }
  }
  return Fact::Ref{id, type, Fact::Clause{buf.data(), key_size, vsize}};
}

namespace {

std::pair<std::string::const_iterator, folly::ByteRange::iterator>
mismatch(const std::string& s, folly::ByteRange t) {
  const auto sp = reinterpret_cast<const unsigned char *>(s.data());
  const auto [p,k] = std::mismatch(sp, sp + s.size(), t.begin(), t.end());
  return {s.begin() + (p-sp), k};
}

std::pair<std::string::iterator, folly::ByteRange::iterator>
mismatch(std::string& s, folly::ByteRange t) {
  const auto sp = reinterpret_cast<unsigned char *>(s.data());
  const auto [p,k] = std::mismatch(sp, sp + s.size(), t.begin(), t.end());
  return {s.begin() + (p-sp), k};
}

std::pair<folly::ByteRange::iterator, folly::ByteRange::iterator>
mismatch(folly::ByteRange s, folly::ByteRange t) {
  const auto [p,k] = std::mismatch(s.begin(), s.end(), t.begin(), t.end());
  return {p, k};
}

}

Tree::Node::Insert Tree::Node::insert(
    Node * FOLLY_NULLABLE &me,
    folly::ByteRange key,
    const Value *fact) {
  const auto [p,k] = mismatch(prefix, key);
  Insert result;
  if (p < prefix.end()) {
    auto pre = alloc<Node4>();
    pre->node.prefix = {prefix.begin(), p};
    pre->node.parent = parent;
    pre->node.index = index;
    parent = &pre->node;
    const auto byte = static_cast<unsigned char>(*p);
    prefix = {p+1, prefix.end()};
    if (k == key.end()) {
      index = 0;
      pre->node.value = fact;
      fact->node = &pre->node;
      pre->bytes[0] = byte;
      pre->children[0] = this;
    } else {
      pre->node.value = nullptr;
      auto rest = alloc<Node0>();
      rest->node.prefix = {k+1, key.end()};
      rest->node.value = fact;
      fact->node = &rest->node;
      rest->node.parent = &pre->node;
      if (byte < *k) {
        index = 0;
        rest->node.index = 1;
      } else {
        index = 1;
        rest->node.index = 0;
      }
      pre->bytes[index] = byte;
      pre->children[index] = this;
      pre->bytes[rest->node.index] = *k;
      pre->children[rest->node.index] = &rest->node;
      assert(pre->bytes[0] < pre->bytes[1]);
    }
    me = &pre->node;
    result.node = nullptr;
    result.fact = nullptr;
  } else if (k < key.end()) {
    result.node = dispatch([&,k=k](auto node) {
      return node->insert(me, *k, {k+1,key.end()}, fact);
    });
    if (result.node != nullptr) {
      result.keypos = k+1;
    } else {
      result.fact = nullptr;;
    }
  } else {
    result.node = nullptr;
    if (value == nullptr) {
      value = fact;
      fact->node = this;
      result.fact = nullptr;
    } else {
      result.fact = value;
    }
  }
  return result;
}

Tree::Node * FOLLY_NULLABLE * FOLLY_NULLABLE Tree::Node0::insert(
    Tree::Node * FOLLY_NULLABLE &me,
    unsigned char byte,
    folly::ByteRange key,
    const Value *fact) {
  auto p = Node::alloc<Node4>(node);
  node.parent = &p->node;
  node.index = 0;
  node.prefix = binary::mkString(key);
  node.value = fact;
  fact->node = &node;
  p->bytes[0] = byte;
  p->children[0] = &node;
  me = &p->node;
  return nullptr;
}

Tree::Node * FOLLY_NULLABLE * FOLLY_NULLABLE Tree::Node4::insert(
    Tree::Node * FOLLY_NULLABLE &me,
    unsigned char byte,
    folly::ByteRange key,
    const Value *fact) {
  unsigned char i;
  for (i = 0; i < 4 && byte != bytes[i]; ++i) {}
  if (i < 4 && children[i]) {
    return &children[i];
  } else {
    auto p = Node::alloc<Node0>();
    p->node.prefix = binary::mkString(key);
    p->node.value = fact;
    fact->node = &p->node;
    for (i = 0; i < 4 && children[i]; ++i) {}
    if (i < 4) {
      while (i > 0 && bytes[i-1] > byte) {
        bytes[i] = bytes[i-1];
        children[i] = children[i-1];
        children[i]->index = i;
        --i;
      }
      p->node.parent = &node;
      p->node.index = i;
      bytes[i] = byte;
      children[i] = &p->node;
      auto b = bytes[0];
      for (i = 1; i < 4 && children[i]; ++i) {
        CHECK_LT(b, bytes[i]);
        b = bytes[i];
      }
    } else {
      auto c = Node::alloc<Node16>(node);
      for (i = 0; i < 4 && bytes[i] < byte; ++i) {
        c->bytes[i] = bytes[i];
        c->children[i] = children[i];
        c->children[i]->parent = &c->node;
      }
      p->node.parent = &c->node;
      p->node.index = i;
      c->bytes[i] = byte;
      c->children[i] = &p->node;
      while (i<4) {
        c->bytes[i+1] = bytes[i];
        c->children[i+1] = children[i];
        c->children[i+1]->index = i+1;
        c->children[i+1]->parent = &c->node;
        ++i;
      }
      std::free(this);
      me = &c->node;
    }
    return nullptr;
  }
}

Tree::Node * FOLLY_NULLABLE * FOLLY_NULLABLE Tree::Node16::insert(
    Tree::Node * FOLLY_NULLABLE &me,
    unsigned char byte,
    folly::ByteRange key,
    const Value *fact) {
  unsigned char i;
  for (i = 0; i < 16 && byte != bytes[i]; ++i) {}
  if (i < 16 && children[i]) {
    return &children[i];
  } else {
    auto p = Node::alloc<Node0>();
    p->node.prefix = binary::mkString(key);
    p->node.value = fact;
    fact->node = &p->node;
    for (i = 4; i < 16 && children[i]; ++i) {}
    if (i < 16) {
      while (i > 0 && bytes[i-1] > byte) {
        bytes[i] = bytes[i-1];
        children[i] = children[i-1];
        children[i]->index = i;
        --i;
      }
      p->node.parent = &node;
      p->node.index = i;
      bytes[i] = byte;
      children[i] = &p->node;
    } else {
      auto c = Node::alloc<Node48>(node);
      for (i = 0; i < 16 && bytes[i] < byte; ++i) {
        c->bytes[i] = bytes[i];
        c->indices[bytes[i]] = i;
        c->children[i] = children[i];
        c->children[i]->parent = &c->node;
      }
      p->node.parent = &c->node;
      p->node.index = i;
      c->bytes[i] = byte;
      c->indices[byte] = i;
      c->children[i] = &p->node;
      while (i < 16) {
        c->bytes[i+1] = bytes[i];
        c->indices[bytes[i]] = i+1;
        c->children[i+1] = children[i];
        c->children[i+1]->index = i+1;
        c->children[i+1]->parent = &c->node;
        ++i;
      }
      std::free(this);
      me = &c->node;
    }
    return nullptr;
  }
}

Tree::Node * FOLLY_NULLABLE * FOLLY_NULLABLE Tree::Node48::insert(
    Tree::Node * FOLLY_NULLABLE &me,
    unsigned char byte,
    folly::ByteRange key,
    const Value *fact) {
  if (indices[byte] != 0xff) {
    return &children[indices[byte]];
  } else {
    auto p = Node::alloc<Node0>();
    p->node.prefix = binary::mkString(key);
    p->node.value = fact;
    fact->node = &p->node;
    size_t i;
    for (i = 16; i < 48 && children[i]; ++i) {}
    if (i < 48) {
      while (i > 0 && bytes[i] > byte) {
        bytes[i] = bytes[i-1];
        indices[bytes[i]] = static_cast<unsigned char>(i);
        children[i] = children[i-1];
        children[i]->index = i;
        --i;        
      }
      p->node.parent = &node;
      p->node.index = i;
      bytes[i] = byte;
      indices[byte] = i;
      children[i] = &p->node;
    } else {
      auto c = Node::alloc<Node256>(node);
      for (i = 0; i < 256; ++i) {
        if (indices[i] != 0xFF) {
          c->children[i] = children[indices[i]];
          c->children[i]->index = i;
          c->children[i]->parent = &c->node;
        }
      }
      p->node.parent = &c->node;
      p->node.index = byte;
      c->children[byte] = &p->node;
      std::free(this);
      me = &c->node;
    }
    return nullptr;
  }
}

Tree::Node * FOLLY_NULLABLE * FOLLY_NULLABLE Tree::Node256::insert(
    Tree::Node * FOLLY_NULLABLE &me,
    unsigned char byte,
    folly::ByteRange key,
    const Value *fact) {
  if (children[byte]) {
    return &children[byte];
  } else {
    auto p = Node::alloc<Node0>();
    p->node.prefix = binary::mkString(key);
    p->node.value = fact;
    fact->node = &p->node;
    p->node.index = byte;
    p->node.parent = &node;
    children[byte] = &p->node;
    return nullptr;
  }
}

const Tree::Value * FOLLY_NULLABLE Tree::insert(folly::ByteRange key, const Value *val) {
  assert (key.size() == val->key_size);
  if (!root) {
    root = reinterpret_cast<Node*>(Node::alloc<Node0>());
    root->prefix = binary::mkString(key);
    root->value = val;
    val->node = root;
    root->parent = nullptr;
    root->index = 0;
    max_key_size = std::max(max_key_size, val->key_size);
    max_value_size = std::max(max_value_size, val->value_size);
    count = 1;
    keymem = key.size();
    return nullptr;
  } else {
    Node::Insert ins;
    ins.node = &root;
    ins.keypos = key.begin();
    while (ins.node != nullptr) {
      ins = (*ins.node)->insert(*ins.node, {ins.keypos, key.end()}, val);
    }
    if (ins.fact == nullptr) {
      max_key_size = std::max(max_key_size, val->key_size);
      max_value_size = std::max(max_value_size, val->value_size);
      ++count;
      keymem += key.size();
    }
    return ins.fact;
  }
}

void Tree::clear() noexcept {
  if (root != nullptr) {
    root->destroy();
    root = nullptr;
    max_key_size = 0;
    max_value_size = 0;
    count = 0;
    keymem = 0;
  }
}

Tree::Iterator::Iterator(
    const Node *node,
    uint32_t buf_size,
    folly::ByteRange start,
    size_t prefixlen)
  : node(node), prefixlen(prefixlen) {
  buf.resize(buf_size);
  assert (buf.size() >= start.size());
  std::copy(start.begin(), start.end(), buf.begin());
  key_size = start.size();
}

Id Tree::Iterator::id() const {
  assert(node != nullptr);
  assert(node->value != nullptr);
  return node->value->id;
}

Fact::Ref Tree::Iterator::get(Demand) {
  if (node) {
    assert(node->value != nullptr);
    assert(key_size == node->value->key_size);
    if (node->value->value_size != 0) {
      assert(key_size + node->value->value_size <= buf.size());
      std::copy(node->value->value().begin(), node->value->value().end(), buf.begin() + key_size);
    }
    return Fact::Ref{
      node->value->id,
      node->value->type,
      Fact::Clause{
        buf.data(),
        node->value->key_size,
        node->value->value_size
      }
    };
  } else {
    return Fact::Ref::invalid();
  }
}

const Tree::Value *Tree::Iterator::operator*() const {
  assert(node != nullptr);
  assert(node->value != nullptr);
  return node->value;
}

void Tree::Iterator::leftmost() {
  if (node != nullptr) {
    CHECK_LE(key_size + node->prefix.size(), buf.size())
      << "key_size: " << key_size;
    assert(key_size + node->prefix.size() <= buf.size());
    std::copy(
      node->prefix.begin(),
      node->prefix.end(),
      buf.begin() + key_size);
    key_size += node->prefix.size();
    if (!node->value) {
      next();
    }
  }
}


Tree::Iterator Tree::Iterator::leftmost(
      const Node * FOLLY_NULLABLE node,
      uint32_t buf_size,
      folly::ByteRange prefix) {
  Iterator iter(node, buf_size, prefix);
  iter.leftmost();
  /*
  if (node != nullptr) {
    assert (iter.key_size + node->prefix.size() <= iter.buf.size());
    std::copy(
      node->prefix.begin(),
      node->prefix.end(),
      iter.buf.begin() + iter.key_size);
    iter.key_size += node->prefix.size();
    if (!node->value) {
      iter.next();
    }
  }
  */
  return iter;
}

Tree::Iterator Tree::Iterator::right(
    const Node * FOLLY_NULLABLE node_,
    uint32_t buf_size_,
    folly::ByteRange prefix_) {
  if (node_ != nullptr) {
    Iterator iter(node_, buf_size_, prefix_);
    while (iter.node->parent != nullptr) {
      const int index = iter.node->index;
      iter.node = iter.node->parent;
      assert(iter.key_size > 0);
      --iter.key_size;
      const auto child =
        iter.node->dispatch([&](auto p) { return p->at(index+1); });
      if (child.node) {
        assert(iter.key_size + 1 <= iter.buf.size());
        iter.buf[iter.key_size] = child.byte;
        ++iter.key_size;
        iter.node = child.node;
        iter.leftmost();
        return iter;
        // return leftmost(child.node, std::move(prefix));
      } else {
        // assert(prefix.size() >= node->prefix.size());
        // prefix.resize(prefix.size() - node->prefix.size());
        assert(iter.key_size >= iter.node->prefix.size());
        iter.key_size -= iter.node->prefix.size();
      }
    }
  }
  return {};
}

void Tree::Iterator::next() {
  int index = 0;
  while (node != nullptr) {
    const auto child = node->dispatch([&](auto p) { return p->at(index); });
    if (child.node) {
      assert (key_size + child.node->prefix.size() + 1 <= buf.size());
      // buf.push_back(child.byte);
      buf[key_size] = child.byte;
      ++key_size;
      node = child.node;
      index = 0;
      // buf.append(node->prefix.begin(), node->prefix.end());
      std::copy(
        node->prefix.begin(),
        node->prefix.end(),
        buf.begin() + key_size);
      key_size += node->prefix.size();
      if (node->value) {
        return;
      }
    } else if (node->parent != nullptr
                && key_size >= prefixlen + node->prefix.size() + 1) {
      // buf.resize(buf.size() - node->prefix.size() - 1);
      assert(key_size >= node->prefix.size() + 1);
      key_size -= node->prefix.size() + 1;
      index = node->index + 1;
      node = node->parent;
      // loop
    } else {
      *this = {};
      return;
    }
  }
}

Tree::Node::Child Tree::Node0::at(int) const {
  return {};
}

Tree::Node::Child Tree::Node4::at(int index) const {
  if (index < 4 && children[index]) {
    return {children[index], bytes[index]};
  } else {
    return {};
  }
}

Tree::Node::Child Tree::Node16::at(int index) const {
  if (index < 16 && children[index]) {
    return {children[index], bytes[index]};
  } else {
    return {};
  }
}

Tree::Node::Child Tree::Node48::at(int index) const {
  if (index < 48 && children[index]) {
    return {children[index], bytes[index]};
  } else {
    return {};
  }
}

Tree::Node::Child Tree::Node256::at(int index) const {
  while (index < 256 && !children[index]) {
    ++index;
  }
  if (index < 256) {
    return {children[index], static_cast<unsigned char>(index)};
  } else {
    return {};
  }
}

Tree::Iterator Tree::begin() const {
  return Iterator::leftmost(root, max_key_size + max_value_size, {});
}

Tree::Node::Child Tree::Node0::find(unsigned char) const {
  return {};
}

Tree::Node::Child Tree::Node4::find(unsigned char byte) const {
  for (auto i = 0; i < 4 && children[i]; ++i) {
    if (bytes[i] == byte) {
      return {children[i], static_cast<unsigned char>(i)};
    }
  }
  return {};
}

Tree::Node::Child Tree::Node16::find(unsigned char byte) const {
  for (auto i = 0; i < 16 && children[i]; ++i) {
    if (bytes[i] == byte) {
      return {children[i], static_cast<unsigned char>(i)};
    }
  }
  return {};
}

Tree::Node::Child Tree::Node48::find(unsigned char byte) const {
  if (indices[byte] != 0xff) {
    return {children[indices[byte]], indices[byte]};
  } else {
    return {};
  }
}

Tree::Node::Child Tree::Node256::find(unsigned char byte) const {
  return {children[byte], byte};
}

Tree::Iterator Tree::find(folly::ByteRange key) const {
  if (root == nullptr) {
    return {};
  }
  auto keypos = key.begin();
  const Node *node = root;
  while (true) {
    const auto [p,k] = mismatch(node->prefix, {keypos, key.end()});
    if (p != node->prefix.end()) {
      return {};
    } else if (k == key.end()) {
      if (node->value) {
        return Iterator(node, max_key_size + max_value_size, key);
      } else {
        return {};
      }
    } else {
      const auto child = node->dispatch([b=*k](auto p) { return p->find(b); });
      if (child.node == nullptr) {
        return {};
      }
      keypos = k+1;
      node = child.node;
    }
  }
}

Tree::Node::Child Tree::Node0::lower_bound(unsigned char) const {
  return {};
}

Tree::Node::Child Tree::Node4::lower_bound(unsigned char byte) const {
  int i;
  for (i = 0; i < 4 && bytes[i] < byte && children[i]; ++i) {}
  if (i < 4) {
    return {children[i], bytes[i]};
  } else {
    return {};
  }
}

Tree::Node::Child Tree::Node16::lower_bound(unsigned char byte) const {
  int i;
  for (i = 0; i < 16 && bytes[i] < byte && children[i]; ++i) {}
  if (i < 16) {
    return {children[i], bytes[i]};
  } else {
    return {};
  }
}

Tree::Node::Child Tree::Node48::lower_bound(unsigned char byte) const {
  int i;
  for (i = 0; i < 48 && bytes[i] < byte && children[i]; ++i) {}
  if (i < 48) {
    return {children[i], bytes[i]};
  } else {
    return {};
  }
}

Tree::Node::Child Tree::Node256::lower_bound(unsigned char byte) const {
  for (int i = byte; i < 256; ++i) {
    if (children[i]) {
      return {children[i], static_cast<unsigned char>(i)};
    }
  }
  return {};
}

Tree::Iterator Tree::lower_bound(folly::ByteRange key) const {
  if (root == nullptr) {
    return {};
  }
  const Node * FOLLY_NULLABLE node = root;
  folly::ByteRange::const_iterator keypos = key.begin();
  while (true) {
    const auto [p,k] = mismatch(node->prefix, {keypos, key.end()});
    if (k == key.end() || (p != node->prefix.end() && *k < static_cast<unsigned char>(*p))) {
        return Iterator::leftmost(node, max_key_size + max_value_size, {key.begin(), keypos});
    } else if (p == node->prefix.end()) {
      const Node::Child child = node->dispatch([k=k](auto p) {
        return p->lower_bound(*k);
      });
      if (child.node) {
        if (*k == child.byte) {
          node = child.node;
          keypos = k+1;
          // loop
        } else {
          Iterator iter(
            child.node,
            max_key_size + max_value_size,
            // binary::mkString({key.begin(), k}) + char(child.byte));
            {key.begin(), k});
          assert(iter.key_size + 1 <= iter.buf.size());
          iter.buf[iter.key_size] = child.byte;
          ++iter.key_size;
          iter.leftmost();
          return iter;
        }
      } else {
        return Iterator::right(node, max_key_size + max_value_size, {key.begin(), keypos});
      }
    } else {
      return Iterator::right(node, max_key_size + max_value_size, {key.begin(), keypos});
    }
  }
}

Tree::Iterator Tree::lower_bound(
    folly::ByteRange key, size_t prefix_size) const {
  auto it = lower_bound(key);
  if (!it.done()) {
    const auto [p,k] = mismatch(it.getKey(), key);
    if (k - key.begin() < prefix_size) {
      it = {};
    } else {
      it.prefixlen = prefix_size;
    }
  }
  return it;
}

const char *Tree::Node::typeString() const {
  switch (type) {
    case N0: return "Node0";
    case N4: return "Node4";
    case N16: return "Node16";
    case N48: return "Node48";
    case N256: return "Node256";
    default: return "UKNOWN";
  }
}

void Tree::Node::validate() const {
  if (value != nullptr && value->node != this) {
    LOG(ERROR) << "Node::validate: wrong value->node pointer";
  }
  switch (type) {
    case N0:
    case N4:
    case N16:
    case N48:
    case N256:
      break;
    default:
      LOG(ERROR) << "Node::validate: invalid type " << static_cast<int>(type);
      return;
  }
  dispatch([](auto p) { p->validate(); });
}

void Tree::Node::validate(const Node *p, unsigned int i, unsigned int b) const {
  validate();
  if (parent != p) {
    LOG(ERROR) << typeString()
      << "::validate: invalid parent (expected " << p->typeString()
      << ", got " << parent->typeString() << ")";
  } else if (index != i) {
    LOG(ERROR) << typeString()
      << "::validate: invalid index (parent " << p->typeString()
      << ", expected " << int(i)
      << ", got " << int(index) << ")";
  }
}

void Tree::Node0::validate() const {
  if (!node.value) {
    LOG(ERROR) << "Node0::validate: no value";
  }
}

void Tree::Node4::validate() const {
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
    children[i]->validate(&node, i, bytes[i]);
  }
}

void Tree::Node16::validate() const {
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
    children[i]->validate(&node, i, bytes[i]);
  }
}

void Tree::Node48::validate() const {
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
    if (bytes[indices[i]] != i) {
        LOG(ERROR) << "Node48::validate: bytes out of sync";
    }
  }
  if (count < 17) {
    LOG(ERROR) << "Node48::validate: only " << count << "children";
  }
  for (auto i = 0; i < 256; ++i) {
    if (indices[i] != 0xff) {
      children[indices[i]]->validate(&node, indices[i], i);
    }
  }
}

void Tree::Node256::validate() const {
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
      children[i]->validate(&node, i, i);
    }
  }
}

void Tree::validate() const {
  if (root) {
    root->validate(nullptr, 0, 0);
  }
}

void Tree::Node::keys(std::string& buf, std::vector<std::string>& v) const {
  buf.append(prefix.begin(), prefix.end());
  if (value) {
    v.push_back(buf);
  }
  dispatch([&](auto p) { p->keys(buf,v); });
  buf.resize(buf.size() - prefix.size());
}

void Tree::Node0::keys(std::string& buf, std::vector<std::string>& v) const {}

void Tree::Node4::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 4 && children[i]; ++i) {
    buf.push_back(bytes[i]);
    children[i]->keys(buf, v);
    buf.pop_back();
  }
}

void Tree::Node16::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 16 && children[i]; ++i) {
    buf.push_back(bytes[i]);
    children[i]->keys(buf, v);
    buf.pop_back();
  }
}

void Tree::Node48::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 256; ++i) {
    if (indices[i] != 0xFF) {
      buf.push_back(i);
      children[indices[i]]->keys(buf, v);
      buf.pop_back();
    }
  }
}

void Tree::Node256::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 256; ++i) {
    if (children[i]) {
      buf.push_back(i);
      children[i]->keys(buf, v);
      buf.pop_back();      
    }
  }
}

std::vector<std::string> Tree::keys() const {
  std::vector<std::string> v;
  if (root) {
    std::string buf;
    root->keys(buf,v);
  }
  return v;
}

namespace {

struct spaces {
  int n;
  explicit spaces(int n) : n(n) {}
};

std::ostream& operator<<(std::ostream& s, spaces x) {
  return s << std::string(x.n, ' ');
}

}

void Tree::Node::dump(std::ostream& s, int indent) const {
  std::string str;
  if (value) {
    str = "*";
  }
  str += '[';
  str += binary::hex(prefix);
  str += ']';
  dispatch([&](auto p) { p->dump(s, indent, str); });
}

void Tree::Node0::dump(std::ostream& s, int indent, const std::string& prefix) const {
  s << spaces(indent) << "N0 " << prefix << std::endl;
}

void Tree::Node4::dump(std::ostream& s, int indent, const std::string& prefix) const {
  s << spaces(indent) << "N4 " << prefix << std::endl;
  for (auto i = 0; i < 4 && children[i]; ++i) {
    s << spaces(indent+2) << binary::hex(bytes[i]) << std::endl;
    children[i]->dump(s,indent+4);
  }
}

void Tree::Node16::dump(std::ostream& s, int indent, const std::string& prefix) const {
  s << spaces(indent) << "N16 " << prefix << std::endl;
  for (auto i = 0; i < 16 && children[i]; ++i) {
    s << spaces(indent+2) << binary::hex(bytes[i]) << std::endl;
    children[i]->dump(s,indent+4);
  }
}

void Tree::Node48::dump(std::ostream& s, int indent, const std::string& prefix) const {
  s << spaces(indent) << "N48 " << prefix << std::endl;
  for (auto i = 0; i < 256; ++i) {
    if (indices[i] != 0xff) {
      s << spaces(indent+2) << binary::hex(bytes[i]) << std::endl;
      children[indices[i]]->dump(s,indent+4);
    }
  }
}

void Tree::Node256::dump(std::ostream& s, int indent, const std::string& prefix) const {
  s << spaces(indent) << "N256 " << prefix << std::endl;
  for (auto i = 0; i < 256; ++i) {
    if (children[i]) {
      s << spaces(indent+2) << binary::hex(i) << std::endl;
      children[i]->dump(s,indent+4);
    }
  }
}

void Tree::dump(std::ostream& s) const {
  if (root != nullptr) {
    s << "TREE" << std::endl;
    root->dump(s, 2);
  } else {
    s << "TREE {}" << std::endl;
  }
}

void Tree::Node::stats(Stats& s) const {
  s.prefix_length += prefix.size();
  dispatch([&](auto p) { p->stats(s); });
}

void Tree::Node0::stats(Stats& s) const {
  ++s.node0;
  s.bytes += sizeof(this);
}

void Tree::Node4::stats(Stats& s) const {
  ++s.node4;
  s.bytes += sizeof(this);
  for (auto i = 0; i < 4 && children[i]; ++i) {
    ++s.node4_children;
    children[i]->stats(s);
  }
}

void Tree::Node16::stats(Stats& s) const {
  ++s.node16;
  s.bytes += sizeof(this);
  for (auto i = 0; i < 16 && children[i]; ++i) {
    ++s.node16_children;
    children[i]->stats(s);
  }
}

void Tree::Node48::stats(Stats& s) const {
  ++s.node48;
  s.bytes += sizeof(this);
  for (auto i = 0; i < 48 && children[i]; ++i) {
    ++s.node48_children;
    children[i]->stats(s);
  }
}

void Tree::Node256::stats(Stats& s) const {
  ++s.node256;
  s.bytes += sizeof(this);
  for (auto i = 0; i < 256; ++i) {
    if (children[i]) {
      ++s.node256_children;
      children[i]->stats(s);
    }
  }
}

Tree::Stats Tree::stats() const {
  Stats s;
  s.key_size = keymem;
  if (root) {
    root->stats(s);
  }
  return s;
}


}

}
}
}