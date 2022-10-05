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

Fact::unique_ptr mkfact(const std::string& key) {
  return Fact::create(Fact::Ref{
    nextId++,
    Pid::fromWord(123),
    Fact::Clause{
      reinterpret_cast<const unsigned char *>(key.data()),
      static_cast<uint32_t>(key.size()),
      0
    }
  });
}

struct Entry {
  Fact::unique_ptr fact;
  roart::Tree::Value::unique_ptr value;
};

struct Storage {
  roart::Tree tree;
  std::vector<Entry> facts;
};

Storage mkTree(const std::vector<std::string>& keys) {
  Storage storage;
  for (const auto& key : keys) {
    auto fact = mkfact(key);
    CHECK_EQ(binary::mkString(fact->ref().key()), key);
    auto value = roart::Tree::Value::alloc(fact->ref());
    const auto old = storage.tree.insert(binary::byteRange(key), value.get());
    if (old == nullptr) {
      storage.facts.push_back(Entry{std::move(fact), std::move(value)});
    }
  }
  return storage;
}

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

folly::ByteRange key(const char *s) {
  const auto p = reinterpret_cast<const unsigned char *>(s);
  return {p, p + std::strlen(s)};
}

void buildAndCheck(Keys keys) {
  auto [tree, facts] = mkTree(keys.keys);

  LOG(INFO) << tree.stats();
  tree.validate();

  std::sort(keys.begin(), keys.end());
  keys.keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

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

  EXPECT_EQ(tree.size(), keys.keys.size());

  for (const auto& entry : facts) {
    const auto b = tree.insert(entry.fact->key(), entry.value.get());
    EXPECT_NE(b, nullptr);
    auto it = tree.find(entry.fact->key());
    EXPECT_FALSE(it.done());
    EXPECT_EQ(it.get().key(), entry.fact->key());
    it = tree.lower_bound(entry.fact->key());
    EXPECT_FALSE(it.done());
    EXPECT_EQ(it.get().key(), entry.fact->key());
  }

  treeKeys.clear();

  for (auto i = tree.begin(); !i.done(); i.next()) {
    treeKeys.push_back(binary::mkString(i.getKey()));
  }

  EXPECT_EQ(keys, Keys{treeKeys});
}

std::vector<std::string> permutes() {
  std::vector<std::string> bits{
    "abcde", "fgh", "fij", "iiii", "iix",
    "a", "b", "c", "d", "e", ""
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
  buildAndCheck(Keys{});
  LOG(INFO) << "*** 2";
  buildAndCheck(Keys{{"a"}});
  LOG(INFO) << "*** 3";
  buildAndCheck(Keys{{"a","b"}});
  LOG(INFO) << "*** 4";
  buildAndCheck(Keys{{"a","abc","adbx","adcde","cde"}});
  LOG(INFO) << "*** 5";
  buildAndCheck(Keys{permutes()});
  LOG(INFO) << "*** 5";
  buildAndCheck(Keys{x3()});
}

TEST(TrieTest, lowerBound) {
  std::vector<std::string> keys{
    "abcd", "abcdefg", "bcde", "bcdefg", "bcdehij", "bcdehil", "bcdehim", "cde", "cdfgh", "cdxyz"
  };
  auto [tree, facts] = mkTree(keys);

  EXPECT_EQ(Keys{tree.keys()}, Keys{keys});

  EXPECT_EQ(tree.lower_bound(key("aaa")).getKey(), key("abcd"));
  EXPECT_EQ(tree.lower_bound(key("abcd")).getKey(), key("abcd"));
  EXPECT_EQ(tree.lower_bound(key("abcde")).getKey(), key("abcdefg"));
  EXPECT_EQ(tree.lower_bound(key("abcdef")).getKey(), key("abcdefg"));
  EXPECT_EQ(tree.lower_bound(key("abcdefg")).getKey(), key("abcdefg"));
  EXPECT_EQ(tree.lower_bound(key("abcdefh")).getKey(), key("bcde"));

  EXPECT_EQ(tree.lower_bound(key("bcde")).getKey(), key("bcde"));
  EXPECT_EQ(tree.lower_bound(key("bcdea")).getKey(), key("bcdefg"));
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
  auto [tree, fact] = mkTree({z ,"bcdefgh"});

  EXPECT_FALSE(tree.lower_bound(binary::byteRange(z)).done());
  EXPECT_FALSE(tree.lower_bound(key("bcdefgh")).done());
}


TEST(TrieTest, prefixIter) {
  std::vector<std::string> keys{
    "abcd", "abcdefg", "bcde", "bcdefg", "bcdehij", "bcdehil", "bcdehim", "cde", "cdfgh", "cdxyz"
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
