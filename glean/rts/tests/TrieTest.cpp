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

struct Storage {
  roart::Tree tree;
  std::vector<Fact::unique_ptr> facts;
};

std::pair<roart::Tree, std::vector<Fact::unique_ptr>> mkTree(
    const std::vector<std::string>& keys) {
  roart::Tree tree;
  std::vector<Fact::unique_ptr> facts;
  for (const auto& key : keys) {
    auto fact = mkfact(key);
    CHECK_EQ(binary::mkString(fact->ref().key()), key);
    const auto b = tree.insert(binary::byteRange(key), fact.get());
    if (b) {
      facts.emplace_back(std::move(fact));
    }
  }
  return {std::move(tree), std::move(facts)};
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
    EXPECT_EQ(i.getKey(), binary::mkString(i.get().key()));
    treeKeys.push_back(i.getKey());
  }

  EXPECT_EQ(keys, Keys{treeKeys});

  EXPECT_EQ(tree.size(), keys.keys.size());

  for (const auto& fact : facts) {
    const auto b = tree.insert(fact->key(), fact.get());
    EXPECT_FALSE(b);
    auto it = tree.find(fact->key());
    EXPECT_FALSE(it.done());
    EXPECT_EQ(it.get().key(), fact->key());
    it = tree.lower_bound(fact->key());
    EXPECT_FALSE(it.done());
    EXPECT_EQ(it.get().key(), fact->key());
  }

  treeKeys.clear();

  for (auto i = tree.begin(); !i.done(); i.next()) {
    treeKeys.push_back(i.getKey());
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

  EXPECT_EQ(tree.lower_bound(key("aaa")).getKey(), "abcd");
  EXPECT_EQ(tree.lower_bound(key("abcd")).getKey(), "abcd");
  EXPECT_EQ(tree.lower_bound(key("abcde")).getKey(), "abcdefg");
  EXPECT_EQ(tree.lower_bound(key("abcdef")).getKey(), "abcdefg");
  EXPECT_EQ(tree.lower_bound(key("abcdefg")).getKey(), "abcdefg");
  EXPECT_EQ(tree.lower_bound(key("abcdefh")).getKey(), "bcde");

  EXPECT_EQ(tree.lower_bound(key("bcde")).getKey(), "bcde");
  EXPECT_EQ(tree.lower_bound(key("bcdea")).getKey(), "bcdefg");
  EXPECT_EQ(tree.lower_bound(key("bcdef")).getKey(), "bcdefg");
  EXPECT_EQ(tree.lower_bound(key("bcdefh")).getKey(), "bcdehij");
  EXPECT_EQ(tree.lower_bound(key("bcdeh")).getKey(), "bcdehij");
  EXPECT_EQ(tree.lower_bound(key("bcdehij")).getKey(), "bcdehij");
  EXPECT_EQ(tree.lower_bound(key("bcdehik")).getKey(), "bcdehil");
  EXPECT_EQ(tree.lower_bound(key("bcdehil")).getKey(), "bcdehil");
  EXPECT_EQ(tree.lower_bound(key("bcdehim")).getKey(), "bcdehim");

  EXPECT_EQ(tree.lower_bound(key("bcdehin")).getKey(),"cde");
  EXPECT_EQ(tree.lower_bound(key("cdef")).getKey(),"cdfgh");
  EXPECT_TRUE(tree.lower_bound(key("cdxz")).done());
}

TEST(TrieTest, lowerBound2) {
  std::string z;
  z += '\0';
  z += "bcdefgh";
  auto [tree, fact] = mkTree({z ,"bcdefgh"});

  std::ostringstream os;
  tree.dump(os);
  LOG(INFO) << os.str();

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
  EXPECT_EQ(it.getKey(), "bcdefg");
  it.next();
  EXPECT_EQ(it.getKey(), "bcdehij");
  it.next();
  EXPECT_EQ(it.getKey(), "bcdehil");
  it.next();
  EXPECT_EQ(it.getKey(), "bcdehim");
  it.next();
  EXPECT_TRUE(it.done());
}

}
}
}
