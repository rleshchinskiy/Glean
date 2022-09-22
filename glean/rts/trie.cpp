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

namespace {

struct Empty {};

template<typename T, typename U = Empty>
struct NodeAllocator : private U {
  static_assert(std::is_trivially_destructible_v<T>);

  struct Page;
  union alignas(void *) alignas(T) Slot {
    Slot *freelink;
    unsigned char bytes[sizeof(T)];
  };
  struct alignas(T) Header : U {
    Page *next;

    explicit Header(const U& u) : U(u) {}
  };

  static constexpr size_t PAGE_SIZE =
    std::max(size_t(4096), folly::nextPowTwo(sizeof(T)*8 + sizeof(Header)));
  static constexpr size_t CAPACITY =
    (PAGE_SIZE - sizeof(Header)) / sizeof(T);

  struct alignas(PAGE_SIZE) Page {
    Header header;
    Slot slots[CAPACITY];

    explicit Page(const U& u) : header(u) {}
  };

  Page *current = nullptr;
  Slot *freelink = nullptr;
  size_t used = CAPACITY;
  size_t count = 0;
  size_t total_size = 0;

  NodeAllocator() = default;
  explicit NodeAllocator(const U& u) : U(u) {}
  ~NodeAllocator() noexcept;

  NodeAllocator(const NodeAllocator&) = delete;
  NodeAllocator(NodeAllocator&&) = delete;
  NodeAllocator& operator=(const NodeAllocator&) = delete;
  NodeAllocator& operator=(NodeAllocator&&) = delete;

  T *allocate() {
    Slot *slot;
    if (used < CAPACITY) {
      slot = current->slots + used;
      ++used;
    } else if (freelink != nullptr) {
      slot = freelink;
      freelink = slot->freelink;
    } else {
      slot = newPage();
    }
    ++count;
    return new(slot) T();
  }

  Slot *newPage();

  void free(T *p) {
    Slot *slot = reinterpret_cast<Slot *>(p);
    slot->freelink = freelink;
    freelink = slot;
    --count;
  }

  static const U& data(const T *p) {
    const Page *page =
      reinterpret_cast<const Page *>(
        reinterpret_cast<uintptr_t>(p) & ~(uintptr_t(PAGE_SIZE) - 1));
    return page->header;
  }

  size_t totalSize() const {
    return total_size;
  }

  size_t wasted() const {
    return total_size - count * sizeof(T);
  }

  size_t usedNodes() const {
    return count;
  }
};

template<typename T, typename U>
NodeAllocator<T,U>::~NodeAllocator() noexcept {
  auto page = current;
  while (current != nullptr) {
    const auto next = current->header.next;
    delete current;
    current = next;
  }
}

template<typename T, typename U>
typename NodeAllocator<T,U>::Slot *NodeAllocator<T,U>::newPage() {
  auto page = new Page(*this);
  page->header.next = current;
  current = page;
  used = 1;
  total_size += sizeof(Page);
  return page->slots;
}


struct ByteAllocator {
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
  size_t total_used = 0;

  ByteAllocator() noexcept = default;
  ~ByteAllocator() noexcept;

  ByteAllocator(const ByteAllocator&) = delete;
  ByteAllocator(ByteAllocator&&) = delete;
  ByteAllocator& operator=(const ByteAllocator&) = delete;
  ByteAllocator& operator=(ByteAllocator&&) = delete;

  size_t pageCapacity() const noexcept {
    return page_size - sizeof(Page);
  }

  unsigned char *allocate(size_t size) {
    total_used += size;
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

ByteAllocator::~ByteAllocator() noexcept {
  auto page = current;
  while (page != nullptr) {
    auto next = page->next;
    std::free(page);
    page = next;
  }
}

unsigned char *ByteAllocator::newPage(size_t size) {
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

}

struct Tree::Node {
  enum Type : unsigned char { N0, N4, N16, N48, N256 };

  template<typename T>
  struct TypedPtr {
    uintptr_t ptr;

    constexpr TypedPtr() : ptr(0) {}
    constexpr TypedPtr(T *p, Type t)
      : ptr(reinterpret_cast<uintptr_t>(p) | t)
      {}
    template<typename U, typename = std::enable_if_t<std::is_same_v<T, const U>>>
    constexpr TypedPtr(const TypedPtr<U>& other)
      : ptr(other.ptr)
      {}

    explicit operator bool() const {
      return ptr != 0;
    }

    T *pointer() const {
      return reinterpret_cast<T *>(ptr & ~uintptr_t(7));
    }

    Type type() const {
      return static_cast<Type>(ptr & 7);
    }

    T *operator->() const {
      return pointer();
    }

    T& operator*() const {
      return *pointer();
    }

    template<typename U>
    bool operator==(TypedPtr<U> other) const {
      return ptr == other.ptr;
    }

    template<typename U>
    bool operator!=(TypedPtr<U> other) const {
      return ptr != other.ptr;
    }

    static TypedPtr from_uintptr_t(uintptr_t p) {
      TypedPtr ptr;
      ptr.ptr = p;
      return ptr;
    }

    uintptr_t to_uintptr_t() const {
      return ptr;
    }

    const char *typeString() const {
      switch (type()) {
        case N0: return "Node0";
        case N4: return "Node4";
        case N16: return "Node16";
        case N48: return "Node48";
        case N256: return "Node256";
        default: return "UKNOWN";
      }
    }
  };

  using Ptr = TypedPtr<Node>;
  using ConstPtr = TypedPtr<const Node>;

  static const Ptr null;

  template<typename T>
  static constexpr Ptr ptr(T *p) {
    static_assert(std::is_standard_layout_v<T>,
      "Node type isn't standard layout");
    static_assert(offsetof(T, node) == 0,
      "Node type doesn't start with Node");

    return Ptr(reinterpret_cast<Node *>(p), T::type);
  }

  template<typename T>
  static constexpr ConstPtr ptr(const T *p) {
    static_assert(std::is_standard_layout_v<T>,
      "Node type isn't standard layout");
    static_assert(offsetof(T, node) == 0,
      "Node type doesn't start with Node");

    return ConstPtr(reinterpret_cast<const Node *>(p), T::type);
  }

  unsigned char index;
  Ptr parent;

  const unsigned char *prefix;
  uint32_t prefix_size;

  // explicit Node(Type ty) : type(ty) {}

  /*
  template<typename NodeT>
  static NodeT *alloc();
  */
  template<typename NodeT>
  static NodeT *cloneAs(Allocator& allocator, const Node& node);

  static Node0 *newNode0(Allocator& allocator, Fact::Clause clause);

  struct Child {
    // const Node * FOLLY_NULLABLE node = nullptr;
    ConstPtr node;
    unsigned char byte;
  };

  void seekTo(Iterator& iter, folly::ByteRange start) const;

  struct Insert {
    Ptr * FOLLY_NULLABLE node;
    union {
      const unsigned char *keypos;
      struct {
        Node0 *leaf;
        bool fresh;
      };
    };

    static Insert inserted(Node0 *leaf) {
      Insert result;
      result.node = nullptr;
      result.leaf = leaf;
      result.fresh = true;
      return result;
    }

    static Insert exists(Node0 *leaf) {
      Insert result;
      result.node = nullptr;
      result.leaf = leaf;
      result.fresh = false;
      return result;
    }

    static Insert cont(Ptr *node, const unsigned char *keypos) {
      Insert result;
      result.node = node;
      result.keypos = keypos;
      return result;
    }
  };

  Insert insert(
    Allocator& alocator,
    Ptr& me,
    Fact::Clause Clause);

  static void validate(ConstPtr node);
  static void validate(ConstPtr node, ConstPtr parent, unsigned int index, unsigned int byte);

  static void keys(ConstPtr node, std::string& buf, std::vector<std::string>& v);

  static void stats(ConstPtr node, Stats& s);

  static void dump(ConstPtr node, std::ostream& s, int indent);

  template<typename F> static auto dispatch(Ptr p, F&& f);
  template<typename F> static auto dispatch(ConstPtr p, F&& f);
};

const Tree::Node::Ptr Tree::Node::null;

struct Tree::Node0 {
  Node node;
  Id id;
  uint32_t key_size;
  uint32_t value_size;

  static constexpr Node::Type type = Node::Type::N0;

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;
  unsigned char byteAt(int index) const;

  Tree::Node::Insert insert(
    Allocator& allocator,
    Node::Ptr& me,
    Fact::Clause Clause);

  void validate() const;
  void keys(std::string& buf, std::vector<std::string>& v) const;
  void keys(int idx, std::string& buf, std::vector<std::string>& v) const;

  void stats(Stats& s) const;

  void dump(std::ostream& s, int indent, const std::string& prefix) const;
};

struct Tree::Node4 {
  Node node;
  unsigned char bytes[4];
  Node::Ptr children[4];

  static constexpr Node::Type type = Node::Type::N4;

  Node4() {
    std::fill(children, children+4, Node::null);
  }

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;
  unsigned char byteAt(int index) const;

  Tree::Node::Insert insert(
    Allocator& allocator,
    Node::Ptr& me,
    Fact::Clause Clause);

  void validate() const;
  void keys(std::string& buf, std::vector<std::string>& v) const;
  void keys(int idx, std::string& buf, std::vector<std::string>& v) const;

  void stats(Stats& s) const;

  void dump(std::ostream& s, int indent, const std::string& prefix) const;
};

struct Tree::Node16 {
  Node node;
  unsigned char bytes[16];
  Node::Ptr children[16];

  static constexpr Node::Type type = Node::Type::N16;

  Node16() {
    std::fill(children, children+16, Node::null);
  }

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;
  unsigned char byteAt(int index) const;

  Node::Insert insert(
    Allocator& alocator,
    Node::Ptr& me,
    Fact::Clause Clause);

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
  Node::Ptr children[48];

  static constexpr Node::Type type = Node::Type::N48;

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;
  unsigned char byteAt(int index) const;

  Node48() {
    std::fill(indices, indices+256, 0xff);
    std::fill(children, children+48, Node::null);
  }

  Node::Insert insert(
    Allocator& allocator,
    Node::Ptr& me,
    Fact::Clause Clause);

  void validate() const;
  void keys(std::string& buf, std::vector<std::string>& v) const;
  void keys(int idx, std::string& buf, std::vector<std::string>& v) const;

  void stats(Stats& s) const;

  void dump(std::ostream& s, int indent, const std::string& prefix) const;
};

struct Tree::Node256 {
  Node node;
  Node::Ptr children[256];

  static constexpr Node::Type type = Node::Type::N256;

  Node256() {
    std::fill(children, children+256, Node::null);
  }

  Node::Child at(int index) const;
  Node::Child find(unsigned char byte) const;
  Node::Child lower_bound(unsigned char byte) const;
  unsigned char byteAt(int index) const;

  Node::Insert insert(
    Allocator& allocator,
    Node::Ptr& me,
    Fact::Clause Clause);

  void validate() const;
  void keys(std::string& buf, std::vector<std::string>& v) const;
  void keys(int idx, std::string& buf, std::vector<std::string>& v) const;

  void stats(Stats& s) const;

  void dump(std::ostream& s, int indent, const std::string& prefix) const;
};


struct Tree::Allocator {
  ByteAllocator byteAllocator;

  struct WithPid {
    Pid pid;
  };

  NodeAllocator<Node0, WithPid> leafAllocator;
  NodeAllocator<Node4> node4Allocator;
  NodeAllocator<Node16> node16Allocator;
  NodeAllocator<Node48> node48Allocator;
  NodeAllocator<Node256> node256Allocator;

  explicit Allocator(Pid pid)
    : leafAllocator(WithPid{pid}) {}

  NodeAllocator<Node0, WithPid>& nodeAllocator(Node0*) {
    return leafAllocator;
  }

  NodeAllocator<Node4>& nodeAllocator(Node4*) {
    return node4Allocator;
  }

  NodeAllocator<Node16>& nodeAllocator(Node16*) {
    return node16Allocator;
  }

  NodeAllocator<Node48>& nodeAllocator(Node48*) {
    return node48Allocator;
  }

  NodeAllocator<Node256>& nodeAllocator(Node256*) {
    return node256Allocator;
  }

  unsigned char *allocate(size_t size) {
    return byteAllocator.allocate(size);
  }

  template<typename T>
  T *alloc() {
    return nodeAllocator(static_cast<T*>(nullptr)).allocate();
  }

  template<typename T>
  void free(T *x) {
    return nodeAllocator(x).free(x);
  }

  static Pid leafPid(const Node0 *node) {
    return NodeAllocator<Node0, WithPid>::data(node).pid;
  }

  size_t totalSize() const {
    return byteAllocator.totalSize()
      + leafAllocator.totalSize()
      + node4Allocator.totalSize()
      + node16Allocator.totalSize()
      + node48Allocator.totalSize()
      + node256Allocator.totalSize();
  }

  size_t wasted() const {
    return leafAllocator.wasted()
      + node4Allocator.wasted()
      + node16Allocator.wasted()
      + node48Allocator.wasted()
      + node256Allocator.wasted();
  }
};

struct Tree::Impl {
  Node::Ptr root;
  // Node * FOLLY_NULLABLE root = nullptr;
  Allocator allocator;
  uint32_t max_key_size = 0;
  uint32_t max_value_size = 0;
  size_t count = 0;
  size_t keymem = 0;

  explicit Impl(Pid pid) : allocator(pid) {}

  Buffer buffer() const {
    return Buffer(max_key_size + max_value_size);
  }

  Buffer buffer(folly::ByteRange prefix) const {
    return Buffer(max_key_size + max_value_size, prefix);
  }
};

Tree::Tree(Pid pid) : impl(new Impl(pid)) {}

Tree::~Tree() noexcept {
  delete impl;
}

template<typename F>
auto Tree::Node::dispatch(Ptr p, F&& f) {
  const auto node = p.pointer();
  switch(p.type()) {
    case N0: return std::forward<F>(f)(reinterpret_cast<Node0*>(node));
    case N4: return std::forward<F>(f)(reinterpret_cast<Node4*>(node));
    case N16: return std::forward<F>(f)(reinterpret_cast<Node16*>(node));
    case N48: return std::forward<F>(f)(reinterpret_cast<Node48*>(node));
    case N256: return std::forward<F>(f)(reinterpret_cast<Node256*>(node));
    default: folly::assume_unreachable();
  }
}

template<typename F>
auto Tree::Node::dispatch(ConstPtr p, F&& f) {
  const auto node = p.pointer();
  switch(p.type()) {
    case N0: return std::forward<F>(f)(reinterpret_cast<const Node0*>(node));
    case N4: return std::forward<F>(f)(reinterpret_cast<const Node4*>(node));
    case N16: return std::forward<F>(f)(reinterpret_cast<const Node16*>(node));
    case N48: return std::forward<F>(f)(reinterpret_cast<const Node48*>(node));
    case N256: return std::forward<F>(f)(reinterpret_cast<const Node256*>(node));
    default: folly::assume_unreachable();
  }
}

/*
template<typename NodeT>
NodeT *Tree::Node::alloc() {
  auto p = static_cast<NodeT*>(std::malloc(sizeof(NodeT)));
  new(p) NodeT;
  return p;
}
*/

template<typename NodeT>
NodeT *Tree::Node::cloneAs(Allocator& allocator, const Node& node) {
  auto clone = allocator.alloc<NodeT>();
  clone->node = node;
  return clone;
}

Tree::Node0 *Tree::Node::newNode0(Allocator& allocator, Fact::Clause clause) {
  auto node0 = allocator.alloc<Node0>();
  auto buf = static_cast<unsigned char *>(
    allocator.allocate(clause.size()));
  if (clause.size() != 0) {
    std::memcpy(buf, clause.data, clause.size());
  }
  node0->node.prefix = buf;
  node0->node.prefix_size = clause.key_size;
  return node0;
}

folly::ByteRange Tree::value(const Node0 *leaf) {
  return {leaf->node.prefix + leaf->node.prefix_size, leaf->value_size};
}

Tree::FactInfo Tree::info(const Tree::Node0 *leaf) {
  return {Allocator::leafPid(leaf), leaf->key_size, leaf->value_size};
}

Pid Tree::type(const Tree::Node0 *leaf) {
  return Allocator::leafPid(leaf);
}

Fact::Ref Tree::get(
    const Node0 *leaf,
    FactIterator::Demand demand,
    std::vector<unsigned char>& buf) {
  assert(leaf != nullptr);
  const auto vsize = demand == FactIterator::Demand::KeyOnly ? 0 : leaf->value_size;
  buf.resize(leaf->key_size + vsize);
  assert(leaf->key_size >= leaf->node.prefix_size);
  auto pos = buf.data() + leaf->key_size - leaf->node.prefix_size;
  std::memcpy(pos, leaf->node.prefix, leaf->node.prefix_size + vsize);
  auto index = leaf->node.index;
  auto current = leaf->node.parent;
  while (current != Node::null) {
    assert(current->prefix_size + 1 <= pos - buf.data());
    --pos;
    *pos = Node::dispatch(current, [&](const auto *p) { return p->byteAt(index); });
    pos -= current->prefix_size;
    if (current->prefix_size != 0) {
      std::memcpy(pos, current->prefix, current->prefix_size);
    }
    index = current->index;
    current = current->parent;
  }
  return Fact::Ref{
    leaf->id,
    Allocator::leafPid(leaf),
    Fact::Clause{buf.data(), leaf->key_size, vsize}};
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
    Ptr& me,
    Fact::Clause clause) {
  const auto [p,k] = mismatch(
    folly::ByteRange{prefix, prefix+prefix_size},
    clause.key());
  if (p < prefix + prefix_size) {
    if (k == clause.key().end()) {
      error("inserted key is prefix of existing key");
    }
    auto pre = allocator.alloc<Node4>();
    pre->node.prefix = prefix;
    pre->node.prefix_size = p - prefix;
    pre->node.parent = parent;
    pre->node.index = index;
    parent = Node::ptr(pre);
    const auto byte = static_cast<unsigned char>(*p);
    // prefix_size -= p - prefix + 1;
    prefix_size = (prefix + prefix_size) - (p+1);
    prefix = p+1;
    assert(clause.key_size >= (k+1) - clause.data);
    clause.key_size -= (k+1) - clause.data;
    clause.data = k+1;
    auto leaf = newNode0(allocator, clause);
    leaf->node.parent = Node::ptr(pre);
    if (byte < *k) {
      index = 0;
      leaf->node.index = 1;
    } else {
      index = 1;
      leaf->node.index = 0;
    }
    pre->bytes[index] = byte;
    pre->children[index] = me;
    pre->bytes[leaf->node.index] = *k;
    pre->children[leaf->node.index] = Node::ptr(leaf);
    assert(pre->bytes[0] < pre->bytes[1]);
    me = ptr(pre);
    return Insert::inserted(leaf);
  } else {
    assert(clause.key_size >= k - clause.data);
    clause.key_size -= k - clause.data;
    clause.data = k;
    return dispatch(me, [&,k=k](auto node) {
      return node->insert(allocator, me, clause);
    });
  }
}

Tree::Node::Insert Tree::Node0::insert(
    Allocator& allocator,
    Node::Ptr& me,
    Fact::Clause clause) {
  if (clause.key_size != 0) {
    error("existing key is prefix of inserted key");
  }
  return Node::Insert::exists(this);
}

Tree::Node::Insert Tree::Node4::insert(
    Allocator& allocator,
    Node::Ptr& me,
    Fact::Clause clause) {
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
    auto leaf = Node::newNode0(allocator, clause);
    for (i = 0; i < 4 && children[i]; ++i) {}
    if (i < 4) {
      while (i > 0 && bytes[i-1] > byte) {
        bytes[i] = bytes[i-1];
        children[i] = children[i-1];
        children[i]->index = i;
        --i;
      }
      leaf->node.parent = Node::ptr(this);
      leaf->node.index = i;
      bytes[i] = byte;
      children[i] = Node::ptr(leaf);
      auto b = bytes[0];
      for (i = 1; i < 4 && children[i]; ++i) {
        CHECK_LT(b, bytes[i]);
        b = bytes[i];
      }
    } else {
      auto c = Node::cloneAs<Node16>(allocator, node);
      for (i = 0; i < 4 && bytes[i] < byte; ++i) {
        c->bytes[i] = bytes[i];
        c->children[i] = children[i];
        c->children[i]->parent = Node::ptr(c);
      }
      leaf->node.parent = Node::ptr(c);
      leaf->node.index = i;
      c->bytes[i] = byte;
      c->children[i] = Node::ptr(leaf);
      while (i<4) {
        c->bytes[i+1] = bytes[i];
        c->children[i+1] = children[i];
        c->children[i+1]->index = i+1;
        c->children[i+1]->parent = Node::ptr(c);
        ++i;
      }
      allocator.free(this);
      me = Node::ptr(c);
    }
    return Node::Insert::inserted(leaf);
  }
}

Tree::Node::Insert Tree::Node16::insert(
    Allocator& allocator,
    Node::Ptr& me,
    Fact::Clause clause) {
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
    auto leaf = Node::newNode0(allocator, clause);
    for (i = 4; i < 16 && children[i]; ++i) {}
    if (i < 16) {
      while (i > 0 && bytes[i-1] > byte) {
        bytes[i] = bytes[i-1];
        children[i] = children[i-1];
        children[i]->index = i;
        --i;
      }
      leaf->node.parent = Node::ptr(this);
      leaf->node.index = i;
      bytes[i] = byte;
      children[i] = Node::ptr(leaf);
    } else {
      auto c = Node::cloneAs<Node48>(allocator, node);
      for (i = 0; i < 16 && bytes[i] < byte; ++i) {
        c->bytes[i] = bytes[i];
        c->indices[bytes[i]] = i;
        c->children[i] = children[i];
        c->children[i]->parent = Node::ptr(c);
      }
      leaf->node.parent = Node::ptr(c);
      leaf->node.index = i;
      c->bytes[i] = byte;
      c->indices[byte] = i;
      c->children[i] = Node::ptr(leaf);
      while (i < 16) {
        c->bytes[i+1] = bytes[i];
        c->indices[bytes[i]] = i+1;
        c->children[i+1] = children[i];
        c->children[i+1]->index = i+1;
        c->children[i+1]->parent = Node::ptr(c);
        ++i;
      }
      allocator.free(this);
      me = Node::ptr(c);
    }
    return Node::Insert::inserted(leaf);
  }
}

Tree::Node::Insert Tree::Node48::insert(
    Allocator& allocator,
    Node::Ptr& me,
    Fact::Clause clause) {
  if (clause.key_size == 0) {
    error("inserted key is prefix of existing key");
  }
  const auto byte = *clause.data;
  ++clause.data;
  --clause.key_size;
  if (indices[byte] != 0xff) {
    return Node::Insert::cont(&children[indices[byte]], clause.data);
  } else {
    auto leaf = Node::newNode0(allocator, clause);
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
      leaf->node.parent = Node::ptr(this);
      leaf->node.index = i;
      bytes[i] = byte;
      indices[byte] = i;
      children[i] = Node::ptr(leaf);
    } else {
      auto c = Node::cloneAs<Node256>(allocator, node);
      for (i = 0; i < 256; ++i) {
        if (indices[i] != 0xFF) {
          c->children[i] = children[indices[i]];
          c->children[i]->index = i;
          c->children[i]->parent = Node::ptr(c);
        }
      }
      leaf->node.parent = Node::ptr(c);
      leaf->node.index = byte;
      c->children[byte] = Node::ptr(leaf);
      allocator.free(this);
      me = Node::ptr(c);
    }
    return Node::Insert::inserted(leaf);
  }
}

Tree::Node::Insert Tree::Node256::insert(
    Allocator& allocator,
    Node::Ptr& me,
    Fact::Clause clause) {
  if (clause.key_size == 0) {
    error("inserted key is prefix of existing key");
  }
  const auto byte = *clause.data;
  ++clause.data;
  --clause.key_size;
  if (children[byte]) {
    return Node::Insert::cont(&children[byte], clause.data);
  } else {
    auto leaf = Node::newNode0(allocator, clause);
    leaf->node.index = byte;
    leaf->node.parent = Node::ptr(this);
    children[byte] = Node::ptr(leaf);
    return Node::Insert::inserted(leaf);
  }
}

std::pair<const Tree::Node0 * FOLLY_NULLABLE, bool> Tree::insert(
    Id id, Fact::Clause clause) {
  Node::Insert ins;
  if (impl->root == Node::null) {
    const auto leaf = Node::newNode0(impl->allocator, clause);
    leaf->node.parent = Node::null;
    leaf->node.index = 0;
    impl->root = Node::ptr(leaf);
    ins = Node::Insert::inserted(leaf);
  } else {
    ins.node = &impl->root;
    ins.keypos = clause.data;
    while (ins.node != nullptr) {
      assert (ins.keypos - clause.data <= clause.key_size);
      ins = (*ins.node)->insert(
        impl->allocator,
        *ins.node,
        Fact::Clause{
          ins.keypos,
          clause.key_size - static_cast<uint32_t>(ins.keypos - clause.data),
          clause.value_size});
    }
  }
  if (ins.fresh) {
    ins.leaf->id = id;
    ins.leaf->key_size = clause.key_size;
    ins.leaf->value_size = clause.value_size;
    impl->max_key_size = std::max(impl->max_key_size, clause.key_size);
    impl->max_value_size = std::max(impl->max_value_size, clause.value_size);
    ++impl->count;
    impl->keymem += clause.key_size;
  }
  return {ins.leaf, ins.fresh};
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
  if (node->parent != Node::null) {
    assert(size > 0);
    --size;
  }
}

void Tree::Buffer::copyValue(const Node0 *node) {
  assert(fits(node->value_size));
  std::memcpy(
    buf.get() + size,
    reinterpret_cast<const Node *>(node)->prefix
      + reinterpret_cast<const Node *>(node)->prefix_size,
    node->value_size);
}

Tree::Iterator::Iterator(const Node0 *node, Buffer buffer, size_t prefixlen)
  : node(node), buffer(std::move(buffer)), prefixlen(prefixlen)
  {}

Id Tree::Iterator::id() const {
  assert(node != nullptr);
  return node->id;
}

Fact::Ref Tree::Iterator::get(Demand demand) {
  if (node) {
    assert(buffer.size == node->key_size);
    const auto vsize = demand == Demand::KeyOnly ? 0 : node->value_size;
    if (vsize != 0) {
      buffer.copyValue(node);
    }
    return Fact::Ref{
      node->id,
      Allocator::leafPid(node),
      Fact::Clause{
        buffer.data(),
        node->key_size,
        vsize
      }
    };
  } else {
    return Fact::Ref::invalid();
  }
}

struct Tree::Cursor {
  Node::ConstPtr node = Node::null;
  Buffer buffer;
  size_t prefixlen = 0;

  Cursor() = default;
  Cursor(
    Node::ConstPtr node,
    Buffer buffer,
    size_t prefixlen = 0);

  enum EnterNode { Enter };

  Cursor(
    EnterNode,
    Node::ConstPtr node,
    Buffer buffer,
    size_t prefixlen = 0);

  Cursor& down(unsigned char byte, Node::ConstPtr child) &;
  Cursor&& down(unsigned char byte,  Node::ConstPtr child) &&;
  Iterator leftmost() &&;
  Iterator next() &&;

  unsigned char up();
};

Tree::Cursor::Cursor(
    Node::ConstPtr node,
    Buffer buffer,
    size_t prefixlen)
  : node(node), buffer(std::move(buffer)), prefixlen(prefixlen)
  {}

Tree::Cursor::Cursor(
    EnterNode,
    Node::ConstPtr node,
    Buffer buf,
    size_t prefixlen)
  : Cursor(node, std::move(buf), prefixlen) {
  buffer.enter(node.pointer());
}

Tree::Iterator Tree::Cursor::leftmost() && {
  assert(node != Node::null);
  while (node.type() != Node::Type::N0) {
    const auto child = Node::dispatch(node, [&](auto p) { return p->at(0); });
    assert(child.node != Node::null);
    buffer.enter(child.byte, child.node.pointer());
    node = child.node;
  }
  return Iterator(
    reinterpret_cast<const Node0 *>(node.pointer()),
    std::move(buffer),
    prefixlen);
}

Tree::Cursor& Tree::Cursor::down(unsigned char byte, Node::ConstPtr child) & {
  assert(child != Node::null);
  assert(child->parent == node);
  buffer.enter(byte, child.pointer());
  node = child;
  return *this;
}

Tree::Cursor&& Tree::Cursor::down(unsigned char byte, Node::ConstPtr child) && {
  return std::move(down(byte, child));
}

unsigned char Tree::Cursor::up() {
  assert(node != Node::null);
  buffer.leave(node.pointer());
  const auto index = node->index;
  node = buffer.size >= prefixlen ? node->parent : Node::null;
  return index;
}

Tree::Iterator Tree::Cursor::next() && {
  assert(node != Node::null);
  for (auto index = up() + 1; node != Node::null; index = up() + 1) {
    const auto child = Node::dispatch(node, [&](auto p) { return p->at(index); });
    if (child.node) {
      return std::move(down(child.byte, child.node)).leftmost();
    }
  }
  return {};
}

void Tree::Iterator::next() {
  *this = Cursor(Node::ptr(node), std::move(buffer), prefixlen).next();
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

/*
Tree::Cursor Tree::cursorAt(folly::ByteRange prefix, const Node *node) const {
  Cursor cursor(node, buffer(prefix));
  cursor.buffer.enter(node);
  return cursor;
}
*/

bool Tree::empty() const noexcept {
  return impl->root == Node::null;
}

size_t Tree::size() const noexcept {
  return impl->count;
}

Tree::Iterator Tree::begin() const {
  if (impl->root != Node::null) {
    return Cursor(Cursor::Enter, impl->root, impl->buffer()).leftmost();
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
  if (impl->root == Node::null) {
    return {};
  }
  auto keypos = key.begin();
  Node::ConstPtr node = impl->root;
  while (node.type() != Node::Type::N0) {
    const auto [p,k] = mismatch(folly::ByteRange{node->prefix, node->prefix_size}, {keypos, key.end()});
    if (p != node->prefix + node->prefix_size || k == key.end()) {
      return {};
    } else {
      const auto child = Node::dispatch(node, [b=*k](auto p) { return p->find(b); });
      if (child.node == Node::null) {
        return {};
      }
      keypos = k+1;
      node = child.node;
    }
  }
  return
    folly::ByteRange(keypos, key.end()) == folly::ByteRange(node->prefix, node->prefix_size)
    ? Iterator(reinterpret_cast<const Node0 *>(node.pointer()), impl->buffer(key))
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
  if (impl->root == Node::null) {
    return {};
  }
  Node::ConstPtr node = impl->root;
  auto keypos = key.begin();
  while (true) {
    const auto [p,k] = mismatch(folly::ByteRange{node->prefix, node->prefix_size}, {keypos, key.end()});
    if (k == key.end() || (p != node->prefix + node->prefix_size && *k < *p)) {
      return Cursor(Cursor::Enter, node, impl->buffer({key.begin(), keypos})).leftmost();
    } else if (p == node->prefix + node->prefix_size) {
      const auto child = Node::dispatch(node, [k=k](auto p) {
        return p->lower_bound(*k);
      });
      if (child.node) {
        if (*k == child.byte) {
          node = child.node;
          keypos = k+1;
          // loop
        } else {
          // *k < child.byte
          return Cursor(node, impl->buffer({key.begin(), k}))
            .down(child.byte, child.node)
            .leftmost();
        }
      } else {
        return Cursor(node, impl->buffer({key.begin(), k})).next();
      }
    } else { // *k > *p
        return Cursor(Cursor::Enter, node, impl->buffer({key.begin(), keypos})).next();
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

void Tree::Node::validate(ConstPtr node) {
  switch (node.type()) {
    case N0:
    case N4:
    case N16:
    case N48:
    case N256:
      break;
    default:
      LOG(ERROR) << "Node::validate: invalid type " << static_cast<int>(node.type());
      return;
  }
  dispatch(node, [](auto p) { p->validate(); });
}

void Tree::Node::validate(ConstPtr node, ConstPtr p, unsigned int i, unsigned int b) {
  validate(node);
  if (node->parent != p) {
    LOG(ERROR) << node.typeString()
      << "::validate: invalid parent (expected " << p.typeString()
      << ", got " << node->parent.typeString() << ")";
  } else if (node->index != i) {
    LOG(ERROR) << node.typeString()
      << "::validate: invalid index (parent " << p.typeString()
      << ", expected " << int(i)
      << ", got " << int(node->index) << ")";
  }
}

void Tree::Node0::validate() const {}

void Tree::Node4::validate() const {
  if (children[0] == Node::null) {
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
    Node::validate(children[i], Node::ptr(this), i, bytes[i]);
  }
}

void Tree::Node16::validate() const {
  if (children[0] == Node::null) {
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
    Node::validate(children[i], Node::ptr(this), i, bytes[i]);
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
      Node::validate(children[indices[i]], Node::ptr(this), indices[i], i);
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
      Node::validate(children[i], Node::ptr(this), i, i);
    }
  }
}

void Tree::validate() const {
  if (impl->root != Node::null) {
    Node::validate(impl->root, Node::null, 0, 0);
  }
}

void Tree::Node::keys(ConstPtr node, std::string& buf, std::vector<std::string>& v) {
  buf.append(reinterpret_cast<const char *>(node->prefix), node->prefix_size);
  dispatch(node, [&](auto p) { p->keys(buf,v); });
  buf.resize(buf.size() - node->prefix_size);
}

void Tree::Node0::keys(std::string& buf, std::vector<std::string>& v) const {
  v.push_back(buf);
}

void Tree::Node4::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 4 && children[i]; ++i) {
    buf.push_back(bytes[i]);
    Node::keys(children[i], buf, v);
    buf.pop_back();
  }
}

void Tree::Node16::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 16 && children[i]; ++i) {
    buf.push_back(bytes[i]);
    Node::keys(children[i], buf, v);
    buf.pop_back();
  }
}

void Tree::Node48::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 256; ++i) {
    if (indices[i] != 0xFF) {
      buf.push_back(i);
      Node::keys(children[i], buf, v);
      buf.pop_back();
    }
  }
}

void Tree::Node256::keys(std::string& buf, std::vector<std::string>& v) const {
  for (auto i = 0; i < 256; ++i) {
    if (children[i]) {
      buf.push_back(i);
      Node::keys(children[i], buf, v);
      buf.pop_back();      
    }
  }
}

std::vector<std::string> Tree::keys() const {
  std::vector<std::string> v;
  if (impl->root != Node::null) {
    std::string buf;
    Node::keys(impl->root, buf, v);
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

void Tree::Node::dump(ConstPtr node, std::ostream& s, int indent) {
  std::string str;
  if (node.type() == Type::N0) {
    str = "*";
  }
  str += '[';
  str += binary::hex(folly::ByteRange{node->prefix, node->prefix_size});
  str += ']';
  dispatch(node, [&](auto p) { p->dump(s, indent, str); });
}

void Tree::Node0::dump(std::ostream& s, int indent, const std::string& prefix) const {
  s << spaces(indent) << "N0 " << prefix << std::endl;
}

void Tree::Node4::dump(std::ostream& s, int indent, const std::string& prefix) const {
  s << spaces(indent) << "N4 " << prefix << std::endl;
  for (auto i = 0; i < 4 && children[i]; ++i) {
    s << spaces(indent+2) << binary::hex(bytes[i]) << std::endl;
    Node::dump(children[i], s, indent+4);
  }
}

void Tree::Node16::dump(std::ostream& s, int indent, const std::string& prefix) const {
  s << spaces(indent) << "N16 " << prefix << std::endl;
  for (auto i = 0; i < 16 && children[i]; ++i) {
    s << spaces(indent+2) << binary::hex(bytes[i]) << std::endl;
    Node::dump(children[i], s, indent+4);
  }
}

void Tree::Node48::dump(std::ostream& s, int indent, const std::string& prefix) const {
  s << spaces(indent) << "N48 " << prefix << std::endl;
  for (auto i = 0; i < 256; ++i) {
    if (indices[i] != 0xff) {
      s << spaces(indent+2) << binary::hex(bytes[i]) << std::endl;
      Node::dump(children[indices[i]], s, indent+4);
    }
  }
}

void Tree::Node256::dump(std::ostream& s, int indent, const std::string& prefix) const {
  s << spaces(indent) << "N256 " << prefix << std::endl;
  for (auto i = 0; i < 256; ++i) {
    if (children[i]) {
      s << spaces(indent+2) << binary::hex(i) << std::endl;
      Node::dump(children[i], s, indent+4);
    }
  }
}

void Tree::dump(std::ostream& s) const {
  if (impl->root != Node::null) {
    s << "TREE" << std::endl;
    Node::dump(impl->root, s, 2);
  } else {
    s << "TREE {}" << std::endl;
  }
}

void Tree::Node::stats(ConstPtr node, Stats& s) {
  dispatch(node, [&](auto p) { p->stats(s); });
}

void Tree::Node0::stats(Stats& s) const {}

void Tree::Node4::stats(Stats& s) const {
  for (auto i = 0; i < 4 && children[i]; ++i) {
    ++s.node4.children;
    Node::stats(children[i], s);
  }
}

void Tree::Node16::stats(Stats& s) const {
  for (auto i = 0; i < 16 && children[i]; ++i) {
    ++s.node16.children;
    Node::stats(children[i], s);
  }
}

void Tree::Node48::stats(Stats& s) const {
  for (auto i = 0; i < 48 && children[i]; ++i) {
    ++s.node48.children;
    Node::stats(children[i], s);
  }
}

void Tree::Node256::stats(Stats& s) const {
  for (auto i = 0; i < 256; ++i) {
    if (children[i]) {
      ++s.node256.children;
      Node::stats(children[i], s);
    }
  }
}

Tree::Stats Tree::stats() const {
  Stats s;
  s.bytes = impl->allocator.totalSize() + sizeof(Impl);
  s.node0.count = impl->allocator.leafAllocator.count;
  s.node0.bytes = impl->allocator.leafAllocator.total_size;
  s.node4.count = impl->allocator.node4Allocator.count;
  s.node4.bytes = impl->allocator.node4Allocator.total_size;
  s.node16.count = impl->allocator.node16Allocator.count;
  s.node16.bytes = impl->allocator.node16Allocator.total_size;
  s.node48.count = impl->allocator.node48Allocator.count;
  s.node48.bytes = impl->allocator.node48Allocator.total_size;
  s.node256.count = impl->allocator.node256Allocator.count;
  s.node256.bytes = impl->allocator.node256Allocator.total_size;
  s.data_size = impl->allocator.byteAllocator.totalSize();
  s.data_used = impl->allocator.byteAllocator.total_used;
  s.wasted = impl->allocator.wasted();
  s.key_size = impl->keymem;
  if (impl->root != Node::null) {
    Node::stats(impl->root, s);
  }
  return s;
}


}

}
}
}