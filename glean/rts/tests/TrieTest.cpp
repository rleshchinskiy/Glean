/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "glean/rts/fact.h"
#include "glean/rts/trie.h"

#include <algorithm>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <string>

namespace facebook {
namespace glean {
namespace rts{

namespace {

Id nextId = Id::fromWord(1);

struct kv {
  kv(const char *s)
    : data(s), key_size(data.size()), value_size(0) {}
  kv(std::string k)
    : data(std::move(k)), key_size(data.size()), value_size(0) {}
  kv(std::string k, std::string v)
    : data(k+v), key_size(k.size()), value_size(v.size()) {}

  folly::ByteRange key() const {
    return {reinterpret_cast<const unsigned char *>(data.data()), key_size};
  }

  folly::ByteRange value() const {
    return {reinterpret_cast<const unsigned char *>(data.data()+key_size), value_size};
  }
  
  std::string data;
  uint32_t key_size;
  uint32_t value_size;
};

bool operator==(const kv& x, const kv& y) {
  return x.data == y.data && x.key_size == y.key_size && x.value_size == y.value_size;
}

bool operator!=(const kv& x, const kv& y) {
  return !(x == y);
}

bool operator<(const kv& x, const kv& y) {
  return x.key() < y.key() || (x.key() == y.key() && x.value() < y.value());
}

bool operator<=(const kv& x, const kv& y) {
  return x.key() < y.key() || (x.key() == y.key() && x.value() <= y.value());
}

bool operator>(const kv& x, const kv& y) {
  return y < x;
}

bool operator>=(const kv& x, const kv& y) {
  return y <= x;
}

static constexpr Pid pid = Pid::fromWord(123);

Fact::unique_ptr mkfact(const kv& fact) {
  return Fact::create(Fact::Ref{
    nextId++,
    pid,
    Fact::Clause{
      reinterpret_cast<const unsigned char *>(fact.data.data()),
      fact.key_size,
      fact.value_size
    }
  });
}

struct Entry {
  Fact::unique_ptr fact;
  const roart::Tree::Node0 *leaf;
};

struct Storage {
  roart::Tree tree{pid};
  std::vector<Entry> facts;
};




struct Keys {
  std::vector<std::string> keys;

  auto begin() const { return keys.begin(); }
  auto begin() { return keys.begin(); }
  auto end() const { return keys.end(); }
  auto end() { return keys.end(); }
};

bool operator==(const Keys& x, const Keys& y) {
  return x.keys == y.keys;
}

bool operator!=(const Keys& x, const Keys& y) {
  return x.keys == y.keys;
}

std::ostream& operator<<(std::ostream& o, const Keys& keys) {
  for (const auto& key : keys) {
    o << '"' << key << "\"\n";
  }
  return o;
}

struct KVs {
  std::vector<kv> kvs;

  KVs() {}
  KVs(std::initializer_list<const char *> keys) {
    for (auto key : keys) {
      kvs.push_back(key);
    }
  }

  KVs(const std::vector<std::string>& keys) {
    for (const auto& key : keys) {
      kvs.push_back(key);
    }    
  }

  /*
  template<typename... Args>
  KVs(Args&&... args) {
    (pushArg(std::forward<Args>(args)),...);
  }

  void pushArg(const char *key) {
    kvs.push_back(key);
  }

  void pushArg(const std::string& key) {
    kvs.push_back(key);
  }
  */

  Keys keys() const {
    Keys keys;
    for (const auto& kv : kvs) {
      keys.keys.push_back(binary::mkString(kv.key()));
    }
    return keys;
  }

  void prep() {
    std::sort(kvs.begin(), kvs.end());
    kvs.erase(std::unique(kvs.begin(), kvs.end()), kvs.end());
  }
};

Storage mkTree(const KVs& facts) {
  Storage storage;
  for (const auto& kv : facts.kvs) {
    auto fact = mkfact(kv);
    const auto r = storage.tree.insert(fact->id(), fact->clause());
    if (r.second) {
      storage.facts.push_back(Entry{std::move(fact), r.first});
    }
  }
  return storage;
}

folly::ByteRange key(const char *s) {
  const auto p = reinterpret_cast<const unsigned char *>(s);
  return {p, p + std::strlen(s)};
}

void buildAndCheck(KVs kvs) {
  auto [tree, facts] = mkTree(kvs);

  LOG(INFO) << tree.stats();
  tree.validate();

  kvs.prep();
  const Keys keys{kvs.keys()};

  auto treeKeys = tree.keys();
 
  EXPECT_EQ(keys, Keys{treeKeys});

  treeKeys.clear();

  for (auto i = tree.begin(); !i.done(); i.next()) {
    assert(i.get());
    EXPECT_EQ(i.getKey(), i.get().key()) << " k=" << treeKeys.size();
    assert(i.getKey() == i.get().key());
    treeKeys.push_back(binary::mkString(i.getKey()));
  }

  EXPECT_EQ(keys, Keys{treeKeys});

  EXPECT_EQ(tree.size(), kvs.kvs.size());

  for (const auto& entry : facts) {
    const auto r = tree.insert(entry.fact->id(), entry.fact->clause());
    EXPECT_FALSE(r.second);
    auto it = tree.find(entry.fact->key());
    EXPECT_FALSE(it.done());
    EXPECT_EQ(it.get().key(), entry.fact->key());
    EXPECT_EQ(it.get().value(), entry.fact->value());
    it = tree.lower_bound(entry.fact->key());
    EXPECT_FALSE(it.done());
    EXPECT_EQ(it.get().key(), entry.fact->key());
    EXPECT_EQ(it.get().value(), entry.fact->value());
  }

  // treeKeys.clear();

  auto n = 0;
  for (auto i = tree.begin(); !i.done(); i.next()) {
    EXPECT_LE(n, kvs.kvs.size());
    EXPECT_EQ(i.getKey(), kvs.kvs[n].key()) << " at " << n;
    ++n;
    // treeKeys.push_back(binary::mkString(i.getKey()));
  }

  // EXPECT_EQ(keys, Keys{treeKeys});
}

std::vector<std::string> permutes() {
  std::vector<std::string> bits{
    "abcde", "fgh", "fij", "iiii", "iik",
    "aa", "bb", "cc", "dd", "ee", "x", "y", "z"
  };

  std::vector<std::string> results;
  for (const auto& s1 : bits) {
    for (const auto& s2 : bits) {
      for (const auto& s3 : bits) {
        for (const auto& s4 : bits) {
          results.push_back(s1+s2+s3+s4);
        }
      }
    }
  }
  return results;
}

std::vector<std::string> x3() {
  std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  std::vector<std::string> v;

  for (auto c1 : chars) {
    for (auto c2 : chars) {
      for (auto c3 : chars) {
        std::string s;
        s += c1;
        s += c2;
        s += c3;
        v.push_back(std::move(s));
      } 
    }
  }

  return v;
}

void lowerBoundTest() {

}

}

TEST(TrieTest, keys) {
  LOG(INFO) << "*** 1";
  buildAndCheck({});
  LOG(INFO) << "*** 2";
  buildAndCheck({"a"});
  LOG(INFO) << "*** 3";
  buildAndCheck({"a","b"});
  LOG(INFO) << "*** 4";
  buildAndCheck({"ab","acc","adbx","adcde","cde"});
  LOG(INFO) << "*** 5";
  buildAndCheck(permutes());
  LOG(INFO) << "*** 5";
  buildAndCheck(x3());
}

TEST(TrieTest, lowerBound) {
  std::vector<std::string> keys{
    "abcda", "abcdefg", "bcdea", "bcdefg", "bcdehij", "bcdehil", "bcdehim", "cde", "cdfgh", "cdxyz"
  };
  auto [tree, facts] = mkTree(keys);

  EXPECT_EQ(Keys{tree.keys()}, Keys{keys});

  EXPECT_EQ(tree.lower_bound(key("aaa")).getKey(), key("abcda"));
  EXPECT_EQ(tree.lower_bound(key("abcd")).getKey(), key("abcda"));
  EXPECT_EQ(tree.lower_bound(key("abcda")).getKey(), key("abcda"));
  EXPECT_EQ(tree.lower_bound(key("abcdb")).getKey(), key("abcdefg"));
  EXPECT_EQ(tree.lower_bound(key("abcde")).getKey(), key("abcdefg"));
  EXPECT_EQ(tree.lower_bound(key("abcdef")).getKey(), key("abcdefg"));
  EXPECT_EQ(tree.lower_bound(key("abcdefg")).getKey(), key("abcdefg"));
  EXPECT_EQ(tree.lower_bound(key("abcdefh")).getKey(), key("bcdea"));

  EXPECT_EQ(tree.lower_bound(key("bcde")).getKey(), key("bcdea"));
  EXPECT_EQ(tree.lower_bound(key("bcdea")).getKey(), key("bcdea"));
  EXPECT_EQ(tree.lower_bound(key("bcdeb")).getKey(), key("bcdefg"));
  EXPECT_EQ(tree.lower_bound(key("bcdef")).getKey(), key("bcdefg"));
  EXPECT_EQ(tree.lower_bound(key("bcdefh")).getKey(), key("bcdehij"));
  EXPECT_EQ(tree.lower_bound(key("bcdeh")).getKey(), key("bcdehij"));
  EXPECT_EQ(tree.lower_bound(key("bcdehij")).getKey(), key("bcdehij"));
  EXPECT_EQ(tree.lower_bound(key("bcdehik")).getKey(), key("bcdehil"));
  EXPECT_EQ(tree.lower_bound(key("bcdehil")).getKey(), key("bcdehil"));
  EXPECT_EQ(tree.lower_bound(key("bcdehim")).getKey(), key("bcdehim"));

  EXPECT_EQ(tree.lower_bound(key("bcdehin")).getKey(), key("cde"));
  EXPECT_EQ(tree.lower_bound(key("cdef")).getKey(), key("cdfgh"));
  EXPECT_TRUE(tree.lower_bound(key("cdxz")).done());
}

TEST(TrieTest, lowerBound2) {
  std::string z;
  z += '\0';
  z += "bcdefgh";
  auto [tree, fact] = mkTree(KVs({z ,"bcdefgh"}));

  EXPECT_FALSE(tree.lower_bound(binary::byteRange(z)).done());
  EXPECT_FALSE(tree.lower_bound(key("bcdefgh")).done());
}


TEST(TrieTest, prefixIter) {
  std::vector<std::string> keys{
    "abcda", "abcdefg", "bcdea", "bcdefg", "bcdehij", "bcdehil", "bcdehim", "cde", "cdfgh", "cdxyz"
  };
  auto [tree, facts] = mkTree(keys);

  EXPECT_EQ(Keys{tree.keys()}, Keys{keys});

  auto it = tree.find(key("bcdefg"));
  EXPECT_FALSE(it.done());
  it.prefixlen = 4;
  EXPECT_EQ(it.getKey(), key("bcdefg"));
  it.next();
  EXPECT_EQ(it.getKey(), key("bcdehij"));
  it.next();
  EXPECT_EQ(it.getKey(), key("bcdehil"));
  it.next();
  EXPECT_EQ(it.getKey(), key("bcdehim"));
  it.next();
  EXPECT_TRUE(it.done());
}

}
}
}
