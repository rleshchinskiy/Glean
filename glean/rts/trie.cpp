/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "glean/rts/trie.h"

#include <folly/memory/Arena.h>

namespace facebook {
namespace glean {
namespace rts {

namespace roart {

#if 0
struct Tree::Allocator {
  folly::SysArena arena;

  Allocator()
    : arena(
        folly::SysArena::kDefaultMinBlockSize,
        folly::SysArena::kNoSizeLimit,
        1)
    {
    }

  unsigned char *allocate(size_t size) {
    return reinterpret_cast<unsigned char *>(arena.allocate(size));
  }

  size_t totalSize() const {
    return arena.totalSize();
  }
};
#endif

struct Tree::Allocator {
  struct Page {
    Page *next;

    unsigned char *data() noexcept {
      return reinterpret_cast<unsigned char *>(this + 1);
    }

    static Page *alloc(size_t size) {
      return static_cast<Page *>(std::aligned_alloc(alignof(Page), size));
    }
  };

  Page *current = nullptr;
  size_t page_size = 4096;
  size_t used = page_size;
  size_t total_size = 0;

  Allocator() noexcept = default;
  ~Allocator() noexcept;

  Allocator(const Allocator&) = delete;
  Allocator(Allocator&&) = delete;
  Allocator& operator=(const Allocator&) = delete;
  Allocator& operator=(Allocator&&) = delete;

  size_t pageCapacity() const noexcept {
    return page_size - sizeof(Page);
  }

  unsigned char *allocate(size_t size) {
    if (page_size - used >= size) {
      const auto p = reinterpret_cast<unsigned char *>(current) + used;
      used += size;
      return p;
    } else {
      return newPage(size);
    }
  }

  unsigned char *newPage(size_t size);

  size_t totalSize() const {
    return total_size;
  }
};

Tree::Allocator::~Allocator() noexcept {
  auto page = current;
  while (page != nullptr) {
    auto next = page->next;
    std::free(page);
    page = next;
  }
}

unsigned char *Tree::Allocator::newPage(size_t size) {
  static constexpr size_t k = 16;
  if (size*k > pageCapacity()) {
    page_size = folly::nextPowTwo(sizeof(Page) + size*k);
  }
  const auto page =
    static_cast<Page*>(std::aligned_alloc(alignof(Page), page_size));
  page->next = current;
  current = page;
  used = sizeof(Page) + size;
  total_size += page_size;
  return page->data();
}


Tree::Tree() : allocator(new Allocator) {}

Tree::~Tree() noexcept {
  clear();
}

struct Tree::Node {
  enum Type : unsigned char { N0, N4, N16, N48, N256 };
  Type type;

  unsigned char index;
  Node *parent;

  const unsigned char *prefix;
  uint32_t prefix_size;

  explicit Node(Type ty) : type(ty) {}

  template<typename NodeT>
  static NodeT *alloc();
  template<typename NodeT>
  static NodeT *alloc(const Node& node);

  static Node0 *newNode0(
    Allocator& allocator,
    Fact::Clause clause,
    const Value *value);

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
      const unsigned char *keypos;
      const Value * FOLLY_NULLABLE fact;
    };

    static Insert inserted() {
      Insert result;
      result.node = nullptr;
      result.fact = nullptr;
      return result;
    }

    static Insert exists(const Value *fact) {
      Insert result;
      result.node = nullptr;
      result.fact = fact;
      return result;
    }

    static Insert cont(Node * FOLLY_NULLABLE *node, const unsigned char *keypos) {
      Insert result;
      result.node = node;
      result.keypos = keypos;
      return result;
    }
  };

  Insert insert(
    Allocator& alocator,
    Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
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
  const Value *value;

  Node0() : node(Node::N0) {}

  void destroy();

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;
  unsigned char byteAt(int index) const;

  Tree::Node::Insert insert(
    Allocator& allocator,
    Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
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
  unsigned char byteAt(int index) const;

  Tree::Node::Insert insert(
    Allocator& allocator,
    Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
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
  unsigned char byteAt(int index) const;

  Node::Insert insert(
    Allocator& alocator,
    Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
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
  unsigned char byteAt(int index) const;

  Node48() : node(Node::N48) {
    std::fill(indices,indices+256,0xff);
    std::fill(children,children+48,nullptr);
  }

  void destroy();

  Node::Insert insert(
    Allocator& allocator,
    Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
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
  unsigned char byteAt(int index) const;

  Node::Insert insert(
    Allocator& allocator,
    Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
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
  return p;
}

Tree::Node0 *Tree::Node::newNode0(
    Allocator& allocator,
    Fact::Clause clause,
    const Tree::Value *value) {
  auto node0 = alloc<Node0>();
  auto buf = static_cast<unsigned char *>(allocator.allocate(clause.size()));
  if (clause.size() != 0) {
    std::memcpy(buf, clause.data, clause.size());
  }
  node0->node.prefix = buf;
  node0->node.prefix_size = clause.key_size;
  node0->value = value;
  value->node0 = node0;
  return node0;
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

folly::ByteRange Tree::Value::value() const {
  return {node0->node.prefix + node0->node.prefix_size, value_size};
}

Fact::Ref Tree::Value::get(
    FactIterator::Demand demand,
    std::vector<unsigned char>& buf) const {
  assert(node0 != nullptr);
  assert(node0->value == this);
  const auto vsize = demand == FactIterator::Demand::KeyOnly ? 0 : value_size;
  buf.resize(key_size + vsize);
  auto pos = buf.data() + key_size;
  if (node0->node.prefix_size != 0 || vsize != 0) {
    assert(node0->node.prefix_size <= key_size);
    pos -= node0->node.prefix_size;
    std::memcpy(pos, node0->node.prefix, node0->node.prefix_size + vsize);
  }
  auto index = node0->node.index;
  auto current = node0->node.parent;
  while (current != nullptr) {
    assert(current->prefix_size + 1 <= pos - buf.data());
    --pos;
    *pos = current->dispatch([&](const auto *p) { return p->byteAt(index); });
    pos -= current->prefix_size;
    if (current->prefix_size != 0) {
      std::memcpy(pos, current->prefix, current->prefix_size);
    }
    index = current->index;
    current = current->parent;
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
    Allocator& allocator,
    Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
    const Value *fact) {
  const auto [p,k] = mismatch(
    folly::ByteRange{prefix, prefix+prefix_size},
    clause.key());
  Insert result;
  if (p < prefix + prefix_size) {
    if (k == clause.key().end()) {
      error("inserted key is prefix of existing key");
    }
    auto pre = alloc<Node4>();
    pre->node.prefix = prefix;
    pre->node.prefix_size = p - prefix;
    pre->node.parent = parent;
    pre->node.index = index;
    parent = &pre->node;
    const auto byte = static_cast<unsigned char>(*p);
    // prefix_size -= p - prefix + 1;
    prefix_size = (prefix + prefix_size) - (p+1);
    prefix = p+1;
    assert(clause.key_size >= (k+1) - clause.data);
    clause.key_size -= (k+1) - clause.data;
    clause.data = k+1;
    auto rest = newNode0(allocator, clause, fact);
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
    me = &pre->node;
    result.node = nullptr;
    result.fact = nullptr;
  } else {
    assert(clause.key_size >= k - clause.data);
    clause.key_size -= k - clause.data;
    clause.data = k;
    result = dispatch([&,k=k](auto node) {
      return node->insert(allocator, me, clause, fact);
    });
  }
  return result;
}

Tree::Node::Insert Tree::Node0::insert(
    Allocator& allocator,
    Tree::Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
    const Value *fact) {
  if (clause.key_size != 0) {
    error("existing key is prefix of inserted key");
  }
  return Node::Insert::exists(value);
}

Tree::Node::Insert Tree::Node4::insert(
    Allocator& allocator,
    Tree::Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
    const Value *fact) {
  if (clause.key_size == 0) {
    error("inserted key is prefix of existing key");
  }
  const auto byte = *clause.data;
  ++clause.data;
  --clause.key_size;
  unsigned char i;
  for (i = 0; i < 4 && byte != bytes[i]; ++i) {}
  if (i < 4 && children[i]) {
    return Node::Insert::cont(&children[i], clause.data);
  } else {
    auto p = Node::newNode0(allocator, clause, fact);
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
    return Node::Insert::inserted();
  }
}

Tree::Node::Insert Tree::Node16::insert(
    Allocator& allocator,
    Tree::Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
    const Value *fact) {
  if (clause.key_size == 0) {
    error("inserted key is prefix of existing key");
  }
  const auto byte = *clause.data;
  ++clause.data;
  --clause.key_size;
  unsigned char i;
  for (i = 0; i < 16 && byte != bytes[i]; ++i) {}
  if (i < 16 && children[i]) {
    return Node::Insert::cont(&children[i], clause.data);
  } else {
    auto p = Node::newNode0(allocator, clause, fact);
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
    return Node::Insert::inserted();
  }
}

Tree::Node::Insert Tree::Node48::insert(
    Allocator& allocator,
    Tree::Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
    const Value *fact) {
  if (clause.key_size == 0) {
    error("inserted key is prefix of existing key");
  }
  const auto byte = *clause.data;
  ++clause.data;
  --clause.key_size;
  if (indices[byte] != 0xff) {
    return Node::Insert::cont(&children[indices[byte]], clause.data);
  } else {
    auto p = Node::newNode0(allocator, clause, fact);
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
    return Node::Insert::inserted();
  }
}

Tree::Node::Insert Tree::Node256::insert(
    Allocator& allocator,
    Tree::Node * FOLLY_NULLABLE &me,
    Fact::Clause clause,
    const Value *fact) {
  if (clause.key_size == 0) {
    error("inserted key is prefix of existing key");
  }
  const auto byte = *clause.data;
  ++clause.data;
  --clause.key_size;
  if (children[byte]) {
    return Node::Insert::cont(&children[byte], clause.data);
  } else {
    auto p = Node::newNode0(allocator, clause, fact);
    p->node.index = byte;
    p->node.parent = &node;
    children[byte] = &p->node;
    return Node::Insert::inserted();
  }
}

const Tree::Value * FOLLY_NULLABLE Tree::insert(
    Fact::Clause clause,
    const Value *val) {
  assert (clause.key_size == val->key_size);
  if (!root) {
    root = &Node::newNode0(*allocator, clause, val)->node;
    root->parent = nullptr;
    root->index = 0;
    max_key_size = std::max(max_key_size, val->key_size);
    max_value_size = std::max(max_value_size, val->value_size);
    count = 1;
    keymem = clause.key_size;
    return nullptr;
  } else {
    Node::Insert ins;
    ins.node = &root;
    ins.keypos = clause.data;
    while (ins.node != nullptr) {
      assert (ins.keypos - clause.data <= clause.key_size);
      ins = (*ins.node)->insert(
        *allocator,
        *ins.node,
        Fact::Clause{
          ins.keypos,
          clause.key_size - static_cast<uint32_t>(ins.keypos - clause.data),
          clause.value_size},
        val);
    }
    if (ins.fact == nullptr) {
      max_key_size = std::max(max_key_size, val->key_size);
      max_value_size = std::max(max_value_size, val->value_size);
      ++count;
      keymem += clause.key_size;
    }
    return ins.fact;
  }
}

void Tree::clear() noexcept {
  if (root != nullptr) {
    root->destroy();
    root = nullptr;
    delete allocator;
    allocator = nullptr;
    max_key_size = 0;
    max_value_size = 0;
    count = 0;
    keymem = 0;
  }
}

void Tree::Buffer::enter(const Node *node) {
  assert(node != nullptr);
  assert(fits(node->prefix_size));
  if (node->prefix_size != 0) {
    std::memcpy(buf.get() + size, node->prefix, node->prefix_size);
    size += node->prefix_size;
  }
}

void Tree::Buffer::enter(unsigned char byte, const Node *node) {
  assert(node != nullptr);
  assert(fits(node->prefix_size + 1));
  buf[size] = byte;
  ++size;
  if (node->prefix_size != 0) {
    std::memcpy(buf.get() + size, node->prefix, node->prefix_size);
    size += node->prefix_size;
  }
}

void Tree::Buffer::leave(const Node *node) {
  assert(node != nullptr);
  assert(size >= node->prefix_size);
  size -= node->prefix_size;
  if (node->parent != nullptr) {
    assert(size > 0);
    --size;
  }
}

void Tree::Buffer::copyValue(const Node0 *node) {
  assert(fits(node->value->value_size));
  std::memcpy(
    buf.get() + size,
    reinterpret_cast<const Node *>(node)->prefix
      + reinterpret_cast<const Node *>(node)->prefix_size,
    node->value->value_size);
}

Tree::Iterator::Iterator(const Node0 *node, Buffer buffer, size_t prefixlen)
  : node(node), buffer(std::move(buffer)), prefixlen(prefixlen)
  {}

Id Tree::Iterator::id() const {
  assert(node != nullptr);
  assert(node->value != nullptr);
  return node->value->id;
}

Fact::Ref Tree::Iterator::get(Demand demand) {
  if (node) {
    assert(node->value != nullptr);
    assert(buffer.size == node->value->key_size);
    const auto vsize = demand == Demand::KeyOnly ? 0 : node->value->value_size;
    if (vsize != 0) {
      buffer.copyValue(node);
    }
    return Fact::Ref{
      node->value->id,
      node->value->type,
      Fact::Clause{
        buffer.data(),
        node->value->key_size,
        vsize
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

Tree::Cursor::Cursor(
    const Node * FOLLY_NULLABLE node,
    Buffer buffer,
    size_t prefixlen)
  : node(node), buffer(std::move(buffer)), prefixlen(prefixlen)
  {}


Tree::Iterator Tree::Cursor::leftmost() && {
  assert(node != nullptr);
  while (node->type != Node::Type::N0) {
    const auto child = node->dispatch([&](auto p) { return p->at(0); });
    assert(child.node != nullptr);
    buffer.enter(child.byte, child.node);
    node = child.node;
  }
  return Iterator(
    reinterpret_cast<const Node0 *>(node),
    std::move(buffer),
    prefixlen);
}

Tree::Cursor& Tree::Cursor::down(unsigned char byte, const Node *child) & {
  assert(child != nullptr);
  assert(child->parent == node);
  buffer.enter(byte, child);
  node = child;
  return *this;
}

Tree::Cursor&& Tree::Cursor::down(unsigned char byte, const Node *child) && {
  return std::move(down(byte, child));
}

unsigned char Tree::Cursor::up() {
  assert(node != nullptr);
  buffer.leave(node);
  const auto index = node->index;
  node = buffer.size >= prefixlen ? node->parent : nullptr;
  return index;
}

Tree::Iterator Tree::Cursor::next() && {
  assert(node != nullptr);
  for (auto index = up() + 1; node != nullptr; index = up() + 1) {
    const auto child = node->dispatch([&](auto p) { return p->at(index); });
    if (child.node) {
      return std::move(down(child.byte, child.node)).leftmost();
    }
  }
  return {};
}

void Tree::Iterator::next() {
  *this = Cursor(&node->node, std::move(buffer), prefixlen).next();
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

unsigned char Tree::Node0::byteAt(int) const {
  assert(false);
  return 0;
}

unsigned char Tree::Node4::byteAt(int index) const {
  assert(index < 4 && children[index]);
  return bytes[index];
}

unsigned char Tree::Node16::byteAt(int index) const {
  assert(index < 16 && children[index]);
  return bytes[index];
}

unsigned char Tree::Node48::byteAt(int index) const {
  assert(index < 48 && children[index]);
  return bytes[index];
}

unsigned char Tree::Node256::byteAt(int index) const {
  assert(index < 256 && children[index]);
  return static_cast<unsigned char>(index);
}

Tree::Cursor Tree::cursorAt(folly::ByteRange prefix, const Node *node) const {
  Cursor cursor(node, buffer(prefix));
  cursor.buffer.enter(node);
  return cursor;
}

Tree::Iterator Tree::begin() const {
  if (root != nullptr) {
    return cursorAt({}, root).leftmost();
  } else {
    return {};
  }
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
  while (node->type != Node::Type::N0) {
    const auto [p,k] = mismatch(folly::ByteRange{node->prefix, node->prefix_size}, {keypos, key.end()});
    if (p != node->prefix + node->prefix_size || k == key.end()) {
      return {};
    } else {
      const auto child = node->dispatch([b=*k](auto p) { return p->find(b); });
      if (child.node == nullptr) {
        return {};
      }
      keypos = k+1;
      node = child.node;
    }
  }
  return
    folly::ByteRange(keypos, key.end()) == folly::ByteRange(node->prefix, node->prefix_size)
    ? Iterator(reinterpret_cast<const Node0 *>(node), buffer(key))
    : Iterator();
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
  const Node * node = root;
  auto keypos = key.begin();
  while (true) {
    const auto [p,k] = mismatch(folly::ByteRange{node->prefix, node->prefix_size}, {keypos, key.end()});
    if (k == key.end() || (p != node->prefix + node->prefix_size && *k < *p)) {
        return cursorAt({key.begin(), keypos}, node).leftmost();
    } else if (p == node->prefix + node->prefix_size) {
      const auto child = node->dispatch([k=k](auto p) {
        return p->lower_bound(*k);
      });
      if (child.node) {
        if (*k == child.byte) {
          node = child.node;
          keypos = k+1;
          // loop
        } else {
          // *k < child.byte
          return Cursor(node, buffer({key.begin(), k}))
            .down(child.byte, child.node)
            .leftmost();
        }
      } else {
        return Cursor(node, buffer({key.begin(), k})).next();
      }
    } else { // *k > *p
        return cursorAt({key.begin(), keypos}, node).next();
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
  if (value->node0 != this) {
    LOG(ERROR) << "Node::validate: wrong value->node pointer";
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
  buf.append(reinterpret_cast<const char *>(prefix), prefix_size);
  dispatch([&](auto p) { p->keys(buf,v); });
  buf.resize(buf.size() - prefix_size);
}

void Tree::Node0::keys(std::string& buf, std::vector<std::string>& v) const {
  v.push_back(buf);
}

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
  if (type == Type::N0) {
    str = "*";
  }
  str += '[';
  str += binary::hex(folly::ByteRange{prefix, prefix_size});
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
  s.prefix_length += prefix_size;
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
  s.arena_size = allocator->totalSize();
  if (root) {
    root->stats(s);
  }
  return s;
}


}

}
}
}