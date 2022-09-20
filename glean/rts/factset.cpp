/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "glean/rts/factset.h"

namespace facebook {
namespace glean {
namespace rts {

#if 0
struct FactSet::Index {
  /// Type of index maps
#if USE_ROART
  using map_t = roart::Tree;
#else
  using map_t = std::map<folly::ByteRange, const Fact *>;
#endif

  /// Type of index maps with synchronised access
  using entry_t = folly::Synchronized<map_t>;

  entry_t& operator[](Pid pid) {
    auto ulock = index.ulock();
    if (const auto p = ulock->lookup(pid)) {
      return **p;
    } else {
      auto wlock = ulock.moveFromUpgradeToWrite();
      auto q = new entry_t;
      (*wlock)[pid].reset(q);
      return *q;
    }
  }

  folly::Synchronized<DenseMap<Pid, std::unique_ptr<entry_t>>> index;
};
#endif

FactSet::FactSet(Id start)
  : facts(start)
  {}
FactSet::FactSet(FactSet&&) = default;
FactSet& FactSet::operator=(FactSet&&) = default;
FactSet::~FactSet() noexcept = default;

size_t FactSet::allocatedMemory() const noexcept {
  size_t mem = facts.allocatedMemory() + keys.allocatedMemory();
  roart::Tree::Stats stats;
  for (const auto& [_,tree] : keys) {
    stats += tree.stats();
  }
  LOG(INFO)
    << "dsize " << stats.data_size
    << " dused " << stats.data_used
    << " wasted " << stats.wasted
    << " leaf " << stats.node0.count << " (" << stats.node0.bytes << ")"
    << " n4 " << stats.node4.count << " (" << stats.node4.bytes << ")"
    << " n16 " << stats.node16.count << " (" << stats.node16.bytes << ")"
    << " n48 " << stats.node48.count << " (" << stats.node48.bytes << ")"
    << " n256 " << stats.node256.count << " (" << stats.node256.bytes << ")"
    ;

  mem += stats.bytes;
  return mem;
}

size_t FactSet::Facts::allocatedMemory() const noexcept {
  return facts.capacity() * sizeof(facts[0]);
}

struct FactSet::CachedPredicateStats {
  struct Data {
    /// Cached stats
    PredicateStats stats;

    /// Index in 'facts' up to which we've computed the stats so far. Since
    /// FactSet is append-only, 'stats' are out of date if we have more than
    /// 'upto' facts and we only need to add the new facts to be up to date
    /// again
    size_t upto = 0;
  };
  folly::Synchronized<Data> data;
};

PredicateStats FactSet::predicateStats() const {
  auto ulock = predicate_stats.value().data.ulock();
  if (ulock->upto < facts.size()) {
    auto wlock = ulock.moveFromUpgradeToWrite();
    wlock->stats.reserve(keys.low_bound(), keys.high_bound());
    for (const auto leaf
          : folly::range(facts.begin() + wlock->upto, facts.end())) {
      auto info = roart::Tree::info(leaf);
      wlock->stats[info.type]
        += MemoryStats::one(info.key_size + info.value_size);
    }
    wlock->upto = facts.size();
    return wlock->stats;
  } else {
    return ulock->stats;
  }
}

Id FactSet::idByKey(Pid type, folly::ByteRange key) {
  if (const auto p = keys.lookup(type)) {
    const auto i = p->find(key);
    if (i != p->end()) {
      return i.id();
    }
  }
  return Id::invalid();
}

Pid FactSet::typeById(Id id) {
  if (id >= facts.startingId()) {
    const auto i = distance(facts.startingId(), id);
    if (i < facts.size()) {
      return roart::Tree::type(facts[i]);
    }
  }
  return Pid::invalid();
}

bool FactSet::factById(Id id, std::function<void(Pid, Fact::Clause)> f) {
  if (id >= facts.startingId()) {
    const auto i = distance(facts.startingId(), id);
    if (i < facts.size()) {
      std::vector<unsigned char> buf;
      const auto ref = roart::Tree::get(facts[i], FactIterator::Demand::KeyValue, buf);
      f(ref.type, ref.clause);
      return true;
    }
  }
  return false;
}

Interval FactSet::count(Pid pid) const {
  const auto p = keys.lookup(pid);
  return p ? p->size() : 0;
}

std::unique_ptr<FactIterator> FactSet::enumerate(Id from, Id upto) {
  struct Iterator final : FactIterator {
    using iter_t = Facts::const_iterator;
    iter_t pos;
    const iter_t end;
    std::vector<unsigned char> buf;

    Iterator(iter_t p, iter_t e)
      : pos(p), end(e) {}

    void next() override {
      assert(pos != end);
      ++pos;
    }

    Fact::Ref get(Demand demand) override {
      return pos != end
        ? roart::Tree::get(*pos, demand, buf)
        : Fact::Ref::invalid();
    }

    std::optional<Id> lower_bound() override { return std::nullopt; }
    std::optional<Id> upper_bound() override { return std::nullopt; }
  };

  return std::make_unique<Iterator>(
    facts.lower_bound(from),
    upto ? facts.lower_bound(upto) : facts.end());
}

std::unique_ptr<FactIterator> FactSet::enumerateBack(Id from, Id downto) {
  struct BackIterator final : FactIterator {
    using iter_t = Facts::const_iterator;
    iter_t pos;
    const iter_t end;
    std::vector<unsigned char> buf;

    BackIterator(iter_t p, iter_t e)
      : pos(p), end(e) {}

    void next() override {
      assert(pos != end);
      --pos;
    }

    Fact::Ref get(Demand demand) override {
      if (pos != end) {
        auto i = pos;
        --i;
        return roart::Tree::get(*i, demand, buf);
      } else {
        return Fact::Ref::invalid();
      }
    }

    std::optional<Id> lower_bound() override { return std::nullopt; }
    std::optional<Id> upper_bound() override { return std::nullopt; }
  };

  return std::make_unique<BackIterator>(
    from ? facts.lower_bound(from) : facts.end(),
    facts.lower_bound(downto));
}

std::unique_ptr<FactIterator> FactSet::seek(
    Pid type,
    folly::ByteRange start,
    size_t prefix_size) {
  if (const auto tree = keys.lookup(type)) {
    auto it = tree->lower_bound(start, prefix_size);
    return std::make_unique<roart::Tree::Iterator>(std::move(it));
  } else {
    return std::make_unique<EmptyIterator>();
  }
#if !USE_ROART
  struct SeekIterator : FactIterator {
    explicit SeekIterator(Index::map_t::iterator b, Index::map_t::iterator e)
      : current(b)
      , end(e)
    {}

    void next() override {
      assert(current != end);
      ++current;
    }

    Fact::Ref get(Demand) override {
      return current != end ? current->second->ref() : Fact::Ref::invalid();
    }

    std::optional<Id> lower_bound() override { return std::nullopt; }
    std::optional<Id> upper_bound() override { return std::nullopt; }

    Index::map_t::const_iterator current;
    const Index::map_t::const_iterator end;
  };
#endif

#if 0
  assert(prefix_size <= start.size());

  if (const auto p = keys.lookup(type)) {
    auto& entry = index.value()[type];
    // Check if the entry is up to date (i.e., has the same number of items as
    // the key hashmap). If it doesn't, fill it.
    if (!entry.withRLock([&](auto& map) { return map.size() == p->size(); })) {
      entry.withWLock([&](auto& map) {
        if (map.size() != p->size()) {
          map.clear();
          for (const Fact *fact : *p) {
#if USE_ROART
            map.insert(fact->key(), fact);
#else
            map.insert({fact->key(), fact});
#endif
          }
#if USE_ROART
          map.validate();
#endif
        }
      });
    }
    // The map for a pid is only created once so this is safe. We are *not*
    // thread safe with respect to concurrent modifications of the FactSet
    // as per spec.
    auto& map = entry.unsafeGetUnlocked();

#if USE_ROART
    auto it = map.lower_bound(start, prefix_size);
    return std::make_unique<roart::Tree::Iterator>(std::move(it));
#else
    const auto next =
      binary::lexicographicallyNext({start.data(), prefix_size});
    return std::make_unique<SeekIterator>(
      map.lower_bound(start),
      next.empty()
        ? map.end()
        : map.lower_bound(binary::byteRange(next)));
#endif
  } else {
    return std::make_unique<EmptyIterator>();
  }
  return std::make_unique<EmptyIterator>();
#endif
}

std::unique_ptr<FactIterator> FactSet::seekWithinSection(
    Pid type,
    folly::ByteRange start,
    size_t prefix_size,
    Id from,
    Id to ) {
  if (from <= startingId() && firstFreeId() <= to) {
    return seek(type, start, prefix_size);
  }

  // We have no use case for actually performing a bounded
  // seek of a FactSet, therefore we would rather know if
  // anything tries to trigger it.
  error("FactSet::seekWithinSection: bounds too narrow");
}

Id FactSet::define(Pid type, Fact::Clause clause, Id) {
  if (clause.key_size > Fact::MAX_KEY_SIZE) {
    error("key too large: {}", clause.key_size);
  }
  const auto next_id = firstFreeId();
  // auto fact = facts.alloc(next_id, type, clause);
  auto& key_map = keys.at(type, type);
  // const auto r = key_map.insert(fact.get());
  // if (r.second) {
  const auto r = key_map.insert(next_id, clause);
  if (r.second) {
    facts.add(clause, r.first);
    return next_id;
  } else {
    return
      // fact->value() == (*r.first)->value() ? (*r.first)->id() : Id::invalid();
      clause.value() == roart::Tree::value(r.first) ? next_id : Id::invalid();
  }
}

thrift::Batch FactSet::serialize() const {
  std::vector<unsigned char> buf;
  binary::Output output;
  for (const auto fact : facts) {
    roart::Tree::get(fact, FactIterator::Demand::KeyValue, buf).serialize(output);
  }

  thrift::Batch batch;
  batch.firstId() = startingId().toThrift();
  batch.count() = size();
  batch.facts() = output.moveToFbString();

  return batch;
}

///
// Serialize facts in the order given by the input range. Preconditions:
//
// * The order must mention only fact IDs in this set.
// * The facts in the set cannot refer to each other (because then we
//   would need to substitute in addition to reordering)
//
// The ordering can omit facts or mention facts multiple times,
// although I'm not sure why you would want to do that.
//
thrift::Batch
FactSet::serializeReorder(folly::Range<const uint64_t *> order) const {
  std::vector<unsigned char> buf;
  binary::Output output;
  for (auto i : order) {
    assert(i >= startingId().toWord() &&
           i - startingId().toWord() < facts.size());
    roart::Tree::get(facts[i - startingId().toWord()], FactIterator::Demand::KeyValue, buf).serialize(output);
  }

  thrift::Batch batch;
  batch.firstId() = startingId().toThrift();
  batch.count() = size();
  batch.facts() = output.moveToFbString();

  return batch;
}

namespace {

template<typename Context>
std::pair<binary::Output, size_t> substituteFact(
    const Inventory& inventory,
    const Predicate::Rename<Context>& substitute,
    Fact::Ref fact) {
  auto predicate = inventory.lookupPredicate(fact.type);
  CHECK_NOTNULL(predicate);
  binary::Output clause;
  uint64_t key_size;
  predicate->substitute(substitute, fact.clause, clause, key_size);
  return {std::move(clause), key_size};
}

}

FactSet FactSet::rebase(
    const Inventory& inventory,
    const Substitution& subst,
    Store& global) const {
  const auto new_start = subst.firstFreeId();
  const auto offset = distance(subst.finish(), new_start);
  const auto substitute = syscall([&subst, offset](Id id, Pid) {
    return id < subst.finish() ? subst.subst(id) : id + offset;
  });

  std::vector<unsigned char> buf;
  const auto split = facts.lower_bound(subst.finish());

  for (const auto fact : folly::range(facts.begin(), split)) {
    const auto ref = roart::Tree::get(fact, FactIterator::Demand::KeyValue, buf);
    auto r = substituteFact(inventory, substitute, ref);
    global.insert({
      subst.subst(ref.id),
      ref.type,
      Fact::Clause::from(r.first.bytes(), r.second)
    });
  }

  FactSet local(new_start);
  auto expected = new_start;
  for (const auto& fact : folly::range(split, facts.end())) {
    const auto ref = roart::Tree::get(fact, FactIterator::Demand::KeyValue, buf);
    auto r = substituteFact(inventory, substitute, ref);
    const auto id =
      local.define(ref.type, Fact::Clause::from(r.first.bytes(), r.second));
    CHECK(id == expected);
    ++expected;
  }

  return local;
}

void FactSet::append(FactSet other) {
  // assert(appendable(other));

  facts.append(std::move(other.facts));

  keys.merge(std::move(other.keys), [](roart::Tree& left, const roart::Tree& right) {
    // left.insert(right.begin(), right.end());
    for (auto i = left.begin(); !i.done(); i.next()) {
      const auto ref = i.get();
      const auto r = left.insert(ref.id, ref.clause);
      assert(r.second);
    }
  });
}

bool FactSet::appendable(const FactSet& other) const {
  return false;

#if 0
  if (empty() || other.empty()) {
    return true;
  }

  if (firstFreeId() != other.startingId()) {
    return false;
  }

  for (const auto& k : other.keys) {
    if (const auto *p = keys.lookup(k.first)) {
      for (auto i = k.second.begin(); i != k.second.end(); ++i) {
        // if (p->contains((*i)->key())) {
        if (p->find((*i)->key()) != p->end()) {
          return false;
        }
      }
    }
  }

  return true;
#endif
}

FactSet FactSet::cloneContiguous(Lookup& lookup) {
  auto iter = lookup.enumerate();
  auto fact = iter->get();
  if (!fact) {
    return FactSet(lookup.firstFreeId());
  }

  FactSet facts(fact.id);
  auto expected = fact.id;

  while (fact && fact.id == expected) {
    const auto k = facts.define(fact.type, fact.clause);
    assert(k == expected);
    ++expected;
    iter->next();
    fact = iter->get();
  }

  return facts;
}


bool FactSet::sanityCheck() const {
  // TODO: implement
  return true;
}

}
}
}
