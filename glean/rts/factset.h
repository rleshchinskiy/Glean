/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "glean/rts/define.h"
#include "glean/rts/densemap.h"
#include "glean/rts/fact.h"
#include "glean/rts/inventory.h"
#include "glean/rts/ondemand.h"
#include "glean/rts/stats.h"
#include "glean/rts/store.h"
#include "glean/rts/substitution.h"

#include <atomic>
#include <boost/intrusive/list.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <folly/container/F14Set.h>
#include <folly/memory/Arena.h>
#include <folly/Synchronized.h>

namespace facebook {
namespace glean {
namespace rts {

// We used to have maps Id -> Fact* and pair<Id,ByteRange> -> Fact* here but
// this used quite a bit of memory (almost exactly as much as the facts
// themselves). Sets with folly's heterogenous key comparisons should use a
// lot less memory per fact.

struct FactById {
  using value_type = Id;

  static value_type get(const Fact *fact) {
    return fact->id();
  }

  static value_type get(const Fact::unique_ptr& p) {
    return get(p.get());
  }

  static value_type get(const value_type& x) {
    return x;
  }
};

struct FactByKey {
  using value_type = std::pair<Pid, folly::ByteRange>;

  static value_type get(const Fact *fact) {
    return {fact->type(), fact->key()};
  }

  static value_type get(const Fact::unique_ptr& p) {
    return get(p.get());
  }

  static value_type get(const value_type& x) {
    return x;
  }
};

struct FactByKeyOnly {
  using value_type = folly::ByteRange;

  static value_type get(const Fact *fact) {
    return fact->key();
  }

  static value_type get(const Fact::unique_ptr& p) {
    return get(p.get());
  }

  static value_type get(const value_type& x) {
    return x;
  }

  static value_type get(const Fact::Ref *ref) {
    return ref->key();
  }
};

template<typename By> struct EqualBy {
  template<typename T, typename U>
  bool operator()(const T& x, const U& y) const {
    return By::get(x) == By::get(y);
  }
};

template<typename By> struct HashBy {
  template<typename T>
  uint64_t operator()(const T& x) const {
    return folly::Hash()(By::get(x));
  }
};

template<typename T, typename By> using FastSetBy =
  folly::F14FastSet<
    T,
    folly::transparent<HashBy<By>>,
    folly::transparent<EqualBy<By>>>;

/**
 * A set of facts which can be looked up by id or by key.
 *
 * Iteration through the set happens in the order in which facts have been
 * first inserted.
 *
 */
class FactSet final : public Define {
private:
  class Facts final {
  private:
    Id starting_id;

    static constexpr size_t ENTRIES_PER_PAGE = 256;
    static constexpr size_t PAGE_SIZE = ENTRIES_PER_PAGE * sizeof(Fact::Ref);

    struct Page {
      Fact::Ref entries[ENTRIES_PER_PAGE];
    };

    std::vector<std::unique_ptr<Page>> pages;
    size_t used = ENTRIES_PER_PAGE;
    std::unique_ptr<folly::SysArena> arena;
    size_t fact_memory = 0;

  public:
    explicit Facts(Id start) noexcept
      : starting_id(start)
      , arena(new folly::SysArena(folly::SysArena::kDefaultMinBlockSize, folly::SysArena::kNoSizeLimit, 1))
      {}

    Facts(Facts&& other) noexcept = default;
    Facts& operator=(Facts&& other) noexcept = default;

    bool empty() const {
      return pages.empty();
    }

    size_t size() const {
      return pages.size() * ENTRIES_PER_PAGE - (ENTRIES_PER_PAGE - used);
    }

    Id startingId() const {
      return starting_id;
    }

    struct const_iterator : boost::iterator_facade<const_iterator, const Fact::Ref, boost::bidirectional_traversal_tag> {
      std::vector<std::unique_ptr<Page>>::const_iterator page_iter;
      const Fact::Ref *ref_iter;

      const_iterator(
        std::vector<std::unique_ptr<Page>>::const_iterator p,
        const Fact::Ref *r) : page_iter(p), ref_iter(r) {}

      const Fact::Ref& dereference() const {
        return *ref_iter;
      }

      void increment() {
        ++ref_iter;
        // if ((reinterpret_cast<uintptr_t>(ref_iter) & (PAGE_SIZE-1)) == 0) {
        if (ref_iter == (*page_iter)->entries + ENTRIES_PER_PAGE) {
          ++page_iter;
          ref_iter = (*page_iter)->entries;
        }
      }

      void decrement() {
        // if ((reinterpret_cast<uintptr_t>(ref_iter) & (PAGE_SIZE-1)) == 0) {
        if (ref_iter == (*page_iter)->entries) {
          --page_iter;
          ref_iter = (*page_iter)->entries + ENTRIES_PER_PAGE - 1;
        } else {
          --ref_iter;
        }
      }

      bool equal(const const_iterator& other) const {
        return ref_iter == other.ref_iter;
      }      
    };

    const_iterator begin() const {
      return const_iterator(
        pages.begin(),
        pages.empty()
          ? nullptr
          : pages.front()->entries);
    }

    const_iterator end() const {
      return pages.empty()
        ? const_iterator(pages.end(), nullptr)
        : const_iterator(
            pages.end()-1,
            pages.back()->entries + used);
    }

    const_iterator iter(size_t i) const {
      const auto page = pages.begin() + i / ENTRIES_PER_PAGE;
      return const_iterator(
        page,
        pages.empty()
          ? nullptr
          : (*page)->entries + (i % ENTRIES_PER_PAGE));
    }

    /*
    struct deref {
      Fact::Ref operator()(const Fact::unique_ptr& p) const {
        return p->ref();
      }
    };

    using const_iterator =
      boost::transform_iterator<
        deref,
        std::vector<Fact::unique_ptr>::const_iterator>;

    const_iterator begin() const {
      return boost::make_transform_iterator(facts.begin(), deref());
    }

    const_iterator end() const {
      return boost::make_transform_iterator(facts.end(), deref());
    }
    */

    Fact::Ref operator[](size_t i) const {
      assert(i < size());
      const auto page = i / ENTRIES_PER_PAGE;
      const auto idx = i % ENTRIES_PER_PAGE;
      return pages[page]->entries[idx];
    }

    void clear() {
      *this = Facts(starting_id);
    }

    using Token = Fact::Ref *;

    Token alloc(Id id, Pid type, Fact::Clause clause);
    void commit(Token token);
    // void append(Facts other);

    /// Return the number of bytes occupied by facts.
    size_t factMemory() const noexcept {
      return fact_memory;
    }

    size_t allocatedMemory() const noexcept;
  };

public:
  explicit FactSet(Id start);
  FactSet(FactSet&&) noexcept;
  FactSet& operator=(FactSet&&);
  ~FactSet() noexcept;

  FactSet(const FactSet&) = delete;
  FactSet& operator=(const FactSet&) = delete;


  size_t size() const noexcept {
    return facts.size();
  }

  bool empty() const noexcept {
    return facts.empty();
  }

  using const_iterator = Facts::const_iterator;

  const_iterator begin() const {
    return facts.begin();
  }

  const_iterator end() const {
    return facts.end();
  }

  /// Return iterator to the first fact with an id that's not less than the
  /// given id (or end() if no such fact exists).
  const_iterator lower_bound(Id id) const {

    return facts.iter
      (id <= facts.startingId()
        ? 0
        : std::min(distance(facts.startingId(), id), facts.size()));
  }

  const_iterator upper_bound(Id id) const {
    return facts.iter
      (id < facts.startingId()
        ? 0
        : std::min(distance(facts.startingId(), id)+1, facts.size()));
  }

  /// Return the number of bytes occupied by facts.
  size_t factMemory() const noexcept {
    return facts.factMemory();
  }

  size_t allocatedMemory() const noexcept;

  PredicateStats predicateStats() const;

 // Lookup implementation

  Id idByKey(Pid type, folly::ByteRange key) override;

  Pid typeById(Id id) override;

  bool factById(Id id, std::function<void(Pid, Fact::Clause)> f) override;

  Id startingId() const override {
    return facts.startingId();
  }

  Id firstFreeId() const override {
    return facts.startingId() + facts.size();
  }

  Interval count(Pid pid) const override;

  std::unique_ptr<FactIterator> enumerate(
    Id from = Id::invalid(),
    Id upto = Id::invalid()) override;
  std::unique_ptr<FactIterator> enumerateBack(
    Id from = Id::invalid(),
    Id downto = Id::invalid()) override;

  /// Prefix seeks. This function can be called from multiple threads but prefix
  /// seeks can *not* be interleaved with modifying the FactSet.
  ///
  /// WARNING: This is currently not intended for production use as it is very
  /// slow. The first call for each predicate will be especially slow as it will
  /// need to create an index.
  std::unique_ptr<FactIterator> seek(
    Pid type,
    folly::ByteRange start,
    size_t prefix_size) override;

  std::unique_ptr<FactIterator> seekWithinSection(
    Pid type,
    folly::ByteRange start,
    size_t prefix_size,
    Id from,
    Id to) override;

  // Define implementation

  Id define(Pid type, Fact::Clause, Id max_ref = Id::invalid()) override;

  thrift::Batch serialize() const;

  thrift::Batch serializeReorder(folly::Range<const uint64_t*> order) const;

  // Substitute all facts in the set and split it into a global and a local part
  // based on the substitution. Facts with Ids that are in the range of the
  // substitution go into the global part - they are moved from the set
  // to the global Store. Facts that are beyond that range (i.e., those with
  // id >= subst.finish()) are assigned new Ids which don't clash with the
  // domain of the substitution and are added to the local part which is
  // returned by the function.
  FactSet rebase(
    const Inventory& inventory,
    const Substitution& subst,
    Store& global) const;

  /// Append a set of facts. This operation is only well defined under the
  /// following conditions.
  ///
  ///   * other.startingId() == this->firstFreeId() unless one of the FactSets
  ///     is empty
  ///   * the fact sets are disjoint, i.e., there are no facts that exist in
  ///     both sets
  ///
  void append(FactSet other);

  /// Checks if appending a particular fact set would be well defined.
  bool appendable(const FactSet& other) const;

  bool sanityCheck() const;

private:
  Facts facts;
  DenseMap<Pid, FastSetBy<const Fact::Ref *, FactByKeyOnly>> keys;

  /// Cached predicate stats. We create these on-demand rather than maintain
  /// them throughout because most FactSets don't need them.
  struct CachedPredicateStats;
  mutable OnDemand<CachedPredicateStats> predicate_stats;

  /// Index for prefix seeks. It is lazily initialised and slow as we typically
  /// don't do seeks on FactSets.
  struct Index;
  OnDemand<Index> index;
};

}
}
}
