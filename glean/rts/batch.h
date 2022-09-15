/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <limits>

#include "glean/rts/lookup.h"

namespace facebook {
namespace glean {
namespace rts {

struct Batch {
  Id firstId = Id::invalid();
  size_t count = 0;
  binary::Output facts;
  std::vector<thrift::Id> ids;

  static Batch serialize(
    std::unique_ptr<FactIterator> iter,
    size_t maxResults = std::numeric_limits<size_t>::max(),
    size_t maxBytes = std::numeric_limits<size_t>::max());

  thrift::Batch toThrift() &&;
};

}
}
}
