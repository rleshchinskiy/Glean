/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "glean/rts/batch.h"
#include "glean/rts/binary.h"

namespace facebook {
namespace glean {
namespace rts {

Batch Batch::serialize(
    std::unique_ptr<FactIterator> iter,
    size_t maxResults,
    size_t maxBytes) {
  Batch batch;

  Fact::Ref fact = iter->get();

  if (fact) {
    batch.firstId = fact.id;
    do {
      fact.serialize(batch.facts);
      ++batch.count;
      iter->next();
      fact = iter->get();
    } while (fact
        && batch.count < maxResults
        && batch.facts.size() < maxBytes
        && fact.id == batch.firstId + batch.count);

    if (fact && batch.count < maxResults && batch.facts.size() < maxBytes) {
      batch.ids.reserve(batch.count+1);
      for (size_t i = 0; i < batch.count; ++i) {
        batch.ids.push_back((batch.firstId + i).toThrift());
      }

      do {
        fact.serialize(batch.facts);
        batch.ids.push_back(fact.id.toThrift());
        ++batch.count;
        iter->next();
        fact = iter->get();
      } while(fact
          && batch.count < maxResults
          && batch.facts.size() < maxBytes);
    }
  }

  return batch;
}

thrift::Batch Batch::toThrift() && {
  thrift::Batch batch;
  batch.firstId() = firstId.toThrift();
  batch.count() = count;
  batch.facts() = facts.moveToFbString();
  if (!ids.empty()) {
    batch.ids() = std::move(ids);
  }
  return batch;
}

}
}
}
