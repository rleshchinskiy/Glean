/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <random>

#include "glean/rts/random.h"

namespace facebook {
namespace glean {
namespace rts {

namespace {

struct RNG {
  std::mt19937_64 mt;

  explicit RNG(folly::Optional<uint32_t> seed)
    : mt(seed.has_value() ? seed.value() : std::random_device{}()) {}

  uint64_t number(uint64_t min, uint64_t max) {
    return max > min ? (mt() - min) % max : min;
  }

  bool freq(double d) {
    const uint64_t k = 1000000Ul;
    return d < k ? false : number(0, k) < static_cast<uint64_t>(d*k);
  }
};

}

FactSet randomFactSet(
    Id firstId,
    folly::Optional<uint32_t> seed,
    folly::F14FastMap<Pid, RandomParams> params) {

  struct State {
    Id firstId;
    folly::F14FastMap<Pid, RandomParams> params;
    RNG rng;

    RandomParams *current;

    explicit State(
        Id firstId,
        folly::Optional<uint32_t> seed,
        folly::F14FastMap<Pid, RandomParams> params)
      : firstId(firstId)
      , params(std::move(params))
      , rng(seed)
      {}

    void genString(binary::Output *out) {
      const auto len = rng.number(0,253);
      unsigned char buf[len+2];
      for (auto i = 0; i < len; ++i) {
        buf[i] = static_cast<unsigned char>(rng.number(32,127));
      }
      buf[len] = 0;
      buf[len+1] = 0;
      out->bytes(buf, len+2);
    }

    uint64_t genNat(uint64_t upto) {
      return rng.number(0, upto-1);
    }

    uint64_t genSize() {
      return rng.number(0, current->sizeCap == 0 ? 71 : current->sizeCap-1);
    }


    Id genFact(Pid pid) {
      return Id::fromWord(
        rng.number(firstId.toWord(), firstId.toWord() + 12345678Ul));
    }

    FactSet generate() {
      const auto context_ = syscalls<
        &State::genFact,
        &State::genSize,
        &State::genNat,
        &State::genString>(*this);

      FactSet facts{firstId != Id::invalid() ? firstId : Id::fromWord(1)};

      std::vector<decltype(params)::iterator> remaining;
      for (auto i = params.begin(); i != params.end(); ++i) {
        if (i->second.wanted > 0) {
          remaining.push_back(i);
        }
      }
      
      while (!remaining.empty()) {
        const auto i = rng.number(0, remaining.size() - 1);
        current = &(remaining[i]->second);

        binary::Output output;
        size_t key_size;

        Subroutine::Activation::with(
          *(remaining[i]->second.gen),
          this,
          [&](Subroutine::Activation& activation) {
            activation.start();
            std::copy(
              context_.handlers_begin(),
              context_.handlers_end(),
              activation.args());
            activation.execute();
            output = std::move(activation.output(0));
            key_size = activation.results()[0];
        });

        const auto expected = facts.firstFreeId();
        const auto real = facts.define(
          remaining[i]->first,
          Fact::Clause::from(output.bytes(), key_size));
        if (real == expected) {
          --remaining[i]->second.wanted;
          if (remaining[i]->second.wanted == 0) {
            remaining.erase(remaining.begin() + i);
          }
        }
      }

      return facts;
    }
  };

  return State(firstId, seed, std::move(params)).generate();
}

FactSet copyFactSetWithRandomRepeats(
    folly::Optional<uint32_t> seed,
    double repeatFreq,
    const FactSet& facts) {
  RNG rng(seed);
  FactSet copy(facts.startingId());
  std::vector<const Fact *> copied;
  copied.reserve(facts.size());

  auto i = facts.begin();
  while (i != facts.end()) {
    const auto& fact =
      rng.freq(repeatFreq) && !copied.empty()
        ? *copied[rng.number(0,copied.size())]
        : *i++;
    const auto id = copy.define(fact.type(), fact.clause());
    CHECK_EQ(id.toWord(), fact.id().toWord());
  }

  return copy;
}

}
}
}