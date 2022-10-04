/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "glean/rts/factset.h"

namespace facebook {
namespace glean {
namespace rts {

struct RandomParams {
  size_t wanted;
  size_t sizeCap;
  std::shared_ptr<Subroutine> gen;
};

FactSet randomFactSet(
  Id firstId,
  folly::Optional<uint32_t> seed,
  folly::F14FastMap<Pid, RandomParams> params);

FactSet copyFactSetWithRandomRepeats(
  folly::Optional<uint32_t> seed,
  double repeatFreq,
  const FactSet& other);

}
}
}
