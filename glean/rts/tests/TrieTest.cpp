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
  for(const auto& key : keys) {
  }
  roart::Tree tree;
  tree.value = nullptr;
  tree.parent = nullptr;

  for (const auto& key : keys) {
    auto fact = mkfact(key);
    tree.insert(binary::byteRange(key), fact.get());
    facts.emplace_back(std::move(fact));
  }

  tree.validate();

  std::sort(keys.begin(), keys.end());
  keys.keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

  std::string buf;
  std::vector<std::string> treeKeys;
  tree.keys(buf, treeKeys);

  CHECK_EQ(keys, Keys{treeKeys});
}

std::vector<std::string> permutes() {
  std::vector<std::string> bits{
    "abcde", "fgh", "fij", "iiii",
    "a", "b", "c", "d", "e", ""
  };

  std::vector<std::string> results;
  for (const auto& s1 : bits) {
    for (const auto& s2 : bits) {
      /* for (const auto& s3 : bits) {
        for (const auto& s4 : bits) {
          results.push_back(s1+s2+s3+s4);
        }
      } */
      results.push_back(s1+s2);
    }
  }
  return results;
}

std::ostream& operator<<(std::ostream& o, const Keys& keys) {
  for (const auto& key : keys) {
    o << '"' << key << "\"\n";
  }
  return o;
}

}

TEST(TrieTest, keys) {
  buildAndCheck(Keys{{"a","abc","adbx","adcde","cde"}});
  buildAndCheck(Keys{permutes()});
}


}
}
}
