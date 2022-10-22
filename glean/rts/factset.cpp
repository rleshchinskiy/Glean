/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "glean/rts/factset.h"
#include <folly/system/MemoryMapping.h>
#include <sys/mman.h>

namespace facebook {
namespace glean {
namespace rts {

struct FactSet::Index {
  /// Type of index maps
  using map_t = std::map<folly::ByteRange, CompressedFactPtr>;

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

struct FactSet::Facts::Arena::Impl {
  struct Mapping {
    void *start;
    size_t length;
  };
  std::vector<Mapping> pages;

  ~Impl() noexcept {
    for (const auto& p : pages) {
      munmap(p.start, p.length);
    }
  }
};

void FactSet::Facts::Arena::destroy(Impl *impl) {
  delete impl;
}

FactSet::Facts::Arena::Arena() noexcept {}

FactSet::Facts::Arena::Arena(Arena&& other) noexcept
  : impl(std::move(other.impl))
  , buf(other.buf)
  , avail(other.avail)
  , total(other.total)
  , used(other.used)
{
  other.buf = nullptr;
  other.avail = 0;
  other.total = 0;
  other.used = 0;
}

FactSet::Facts::Arena& FactSet::Facts::Arena::operator=(Arena&& other) noexcept {
  clear();
  std::swap(impl,other.impl);
  std::swap(buf,other.buf);
  std::swap(avail,other.avail);
  std::swap(total,other.total);
  std::swap(used,other.used);
  return *this;
}

FactSet::Facts::Arena::~Arena() noexcept {
  clear();
}

void FactSet::Facts::Arena::clear() noexcept {
  impl.reset();
  buf = nullptr;
  avail = 0;
  total = 0;
  used = 0;
}

void FactSet::Facts::Arena::allocNewPage(size_t n) {
  if (!impl) {
    impl.reset(new Impl());
  }
  const auto size = (n / DEFAULT_PAGE_SIZE + 1) * DEFAULT_PAGE_SIZE;
  if (!impl->pages.empty()) {
    const auto new_length = impl->pages.back().length + size;
    const auto r = mremap(
      impl->pages.back().start,
      impl->pages.back().length,
      new_length,
      0);
    if (r != MAP_FAILED) {
      LOG(INFO) << "remapped " << impl->pages.back().length
        << " to " << new_length;
      impl->pages.back().length = new_length;
      avail += size;
      total += size;
      return;
    } /* else {
      LOG(INFO) << "couldn't remap " << impl->pages.back().length
        << " to " << new_length << ": " << strerror(errno);
    } */
  }
  const auto r = mmap(
    nullptr,
    size,
    PROT_READ | PROT_WRITE,
    MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
    -1,
    0);
  if (r == MAP_FAILED) {
    error("Couldn't mmap {} bytes: {}", size, strerror(errno));
  }
  impl->pages.push_back({r, size});
  buf = static_cast<unsigned char *>(r);
  avail = size;
  total += size;
}

size_t FactSet::Facts::Arena::totalSize() const noexcept {
  return total + sizeof(Impl) + impl->pages.capacity() * sizeof(impl->pages[0]);
}


void FactSet::Facts::Arena::unalloc(const void *upto) noexcept {
  assert(impl);
  assert(!impl->pages.empty());
  assert(upto >= impl->pages.back().start);
  assert(upto <= buf);
  const auto diff = buf - static_cast<const unsigned char *>(upto);
  avail += diff;
  buf -= diff;
  assert(used >= diff);
  used -= diff;
}

FactSet::Facts::Token FactSet::Facts::alloc(Id id, Pid type, Fact::Clause clause) {
  assert(id == firstFreeId());
  return CompressedFactPtr::compress(
    Fact::Ref{id,type,clause},
    [&](size_t n) {
      return arena.alloc(n);
    });
}

void FactSet::Facts::commit(Token tok) {
  assert(tok.first.id() == firstFreeId());
  facts.push_back(tok.first);
  arena.unalloc(tok.second);
}

void FactSet::Facts::revert(Token tok) {
  assert(tok.first.id() == firstFreeId());
  arena.unalloc(tok.first.ptr);
}


FactSet::FactSet(Id start) : facts(start) {}
FactSet::FactSet(FactSet&&) noexcept = default;
FactSet& FactSet::operator=(FactSet&&) = default;
FactSet::~FactSet() noexcept = default;

size_t FactSet::allocatedMemory() const noexcept {
  size_t n = facts.allocatedMemory() + keys.allocatedMemory();
  for (const auto& k : keys) {
    n += k.second.getAllocatedMemorySize();
  }
  return n;
}

size_t FactSet::Facts::allocatedMemory() const noexcept {
  LOG(INFO) << "used " << arena.usedSize() << " total " << arena.totalSize();
  return facts.capacity() * sizeof(facts[0]) + arena.totalSize();
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
    for (const auto& fact
          : folly::range(facts.iter(wlock->upto), facts.end())) {
      const auto ref = fact.ref();
      wlock->stats[ref.type] += MemoryStats::one(ref.clause.size());
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
      return i->id();
    }
  }
  return Id::invalid();
}

Pid FactSet::typeById(Id id) {
  if (id >= facts.startingId()) {
    const auto i = distance(facts.startingId(), id);
    if (i < facts.size()) {
      return facts[i].type();
    }
  }
  return Pid::invalid();
}

bool FactSet::factById(Id id, std::function<void(Pid, Fact::Clause)> f) {
  if (id >= facts.startingId()) {
    const auto i = distance(facts.startingId(), id);
    if (i < facts.size()) {
      const auto ref = facts[i].ref();
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
    using iter_t = FactSet::const_iterator;
    iter_t pos;
    const iter_t end;

    Iterator(iter_t p, iter_t e)
      : pos(p), end(e) {}

    void next() override {
      assert(pos != end);
      ++pos;
    }

    Fact::Ref get(Demand) override {
      return pos != end ? pos->ref() : Fact::Ref::invalid();
    }

    std::optional<Id> lower_bound() override { return std::nullopt; }
    std::optional<Id> upper_bound() override { return std::nullopt; }
  };

  return std::make_unique<Iterator>(
    lower_bound(from),
    upto ? lower_bound(upto) : end());
}

std::unique_ptr<FactIterator> FactSet::enumerateBack(Id from, Id downto) {
  struct BackIterator final : FactIterator {
    using iter_t = FactSet::const_iterator;
    iter_t pos;
    const iter_t end;

    BackIterator(iter_t p, iter_t e)
      : pos(p), end(e) {}

    void next() override {
      assert(pos != end);
      --pos;
    }

    Fact::Ref get(Demand) override {
      if (pos != end) {
        auto i = pos;
        --i;
        return i->ref();
      } else {
        return Fact::Ref::invalid();
      }
    }

    std::optional<Id> lower_bound() override { return std::nullopt; }
    std::optional<Id> upper_bound() override { return std::nullopt; }
  };

  return std::make_unique<BackIterator>(
    from ? lower_bound(from) : end(),
    lower_bound(downto));
}

std::unique_ptr<FactIterator> FactSet::seek(
    Pid type,
    folly::ByteRange start,
    size_t prefix_size) {
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
      return current != end ? current->second.ref() : Fact::Ref::invalid();
    }

    std::optional<Id> lower_bound() override { return std::nullopt; }
    std::optional<Id> upper_bound() override { return std::nullopt; }

    Index::map_t::const_iterator current;
    const Index::map_t::const_iterator end;
  };

  assert(prefix_size <= start.size());

  if (const auto p = keys.lookup(type)) {
    auto& entry = index.value()[type];
    // Check if the entry is up to date (i.e., has the same number of items as
    // the key hashmap). If it doesn't, fill it.
    if (!entry.withRLock([&](auto& map) { return map.size() == p->size(); })) {
      entry.withWLock([&](auto& map) {
        if (map.size() != p->size()) {
          map.clear();
          for (const auto fact : *p) {
            map.insert({fact.ref().key(), fact});
          }
        }
      });
    }
    // The map for a pid is only created once so this is safe. We are *not*
    // thread safe with respect to concurrent modifications of the FactSet
    // as per spec.
    auto& map = entry.unsafeGetUnlocked();

    const auto next =
      binary::lexicographicallyNext({start.data(), prefix_size});
    return std::make_unique<SeekIterator>(
      map.lower_bound(start),
      next.empty()
        ? map.end()
        : map.lower_bound(binary::byteRange(next)));
  } else {
    return std::make_unique<EmptyIterator>();
  }
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
  auto tok = facts.alloc(next_id, type, clause);
  auto& key_map = keys[type];
  const auto r = key_map.insert(tok.first);
  if (r.second) {
    facts.commit(tok);
    return next_id;
  } else {
    facts.revert(tok);
    const auto ref = r.first->ref();
    return clause.value() == ref.value() ? ref.id : Id::invalid();
  }
}

thrift::Batch FactSet::serialize() const {
  binary::Output output;
  for (auto fact : *this) {
    fact.ref().serialize(output);
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
  binary::Output output;
  for (auto i : order) {
    assert(i >= startingId().toWord() &&
           i - startingId().toWord() < facts.size());
    facts[i - startingId().toWord()].ref().serialize(output);
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

  const auto split = lower_bound(subst.finish());

  for (auto fact : folly::range(begin(), split)) {
    const auto ref = fact.ref();
    auto r = substituteFact(inventory, substitute, ref);
    global.insert({
      subst.subst(ref.id),
      ref.type,
      Fact::Clause::from(r.first.bytes(), r.second)
    });
  }

  FactSet local(new_start);
  auto expected = new_start;
  for (auto fact : folly::range(split, end())) {
    const auto ref = fact.ref();
    auto r = substituteFact(inventory, substitute, ref);
    const auto id =
      local.define(ref.type, Fact::Clause::from(r.first.bytes(), r.second));
    CHECK(id == expected);
    ++expected;
  }

  return local;
}

void FactSet::append(FactSet other) {
  for (const auto fact : other) {
    const auto ref = fact.ref();
    const auto id = define(ref.type, ref.clause);
    assert(id == ref.id);
  }
  /*
  assert(appendable(other));

  facts.append(std::move(other.facts));

  keys.merge(std::move(other.keys), [](auto& left, const auto& right) {
    left.insert(right.begin(), right.end());
  });
  */
}

bool FactSet::appendable(const FactSet& other) const {
  if (empty() || other.empty()) {
    return true;
  }

  if (firstFreeId() != other.startingId()) {
    return false;
  }

  for (const auto& k : other.keys) {
    if (const auto *p = keys.lookup(k.first)) {
      for (auto i = k.second.begin(); i != k.second.end(); ++i) {
        if (p->contains(i->ref().key())) {
          return false;
        }
      }
    }
  }

  return true;
}

bool FactSet::sanityCheck() const {
  // TODO: implement
  return true;
}


}
}
}
