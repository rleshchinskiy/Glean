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

void buildAndCheck(Keys keys) {
  std::vector<Fact::unique_ptr> facts;
  roart::Tree tree;
  // tree.value = nullptr;
  // tree.parent = nullptr;

  for (const auto& key : keys) {
    auto fact = mkfact(key);
    tree.insert(binary::byteRange(key), fact.get());
    facts.emplace_back(std::move(fact));
  }

  LOG(INFO) << tree.stats();
  // LOG(INFO) << tree;
  // std::cerr << tree;
  tree.validate();

  std::sort(keys.begin(), keys.end());
  keys.keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

  /*
  std::string buf;
  std::vector<std::string> treeKeys;
  tree.keys(buf, treeKeys);
  */

  auto treeKeys = tree.keys();
 
  CHECK_EQ(keys, Keys{treeKeys});

  treeKeys.clear();

  for (auto i = tree.begin(); !i.done(); i.next()) {
    treeKeys.push_back(i.getKey());
  }

  CHECK_EQ(keys, Keys{treeKeys});
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

std::ostream& operator<<(std::ostream& o, const Keys& keys) {
  for (const auto& key : keys) {
    o << '"' << key << "\"\n";
  }
  return o;
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


}
}
}
