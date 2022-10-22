/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "glean/rts/nat.h"

namespace facebook {
namespace glean {
namespace rts {

const unsigned char* FOLLY_NULLABLE
skipUntrustedNat(const unsigned char* p, const unsigned char* e) {
  if (LIKELY(p != e)) {
    auto size = natSize(*p);
    if (LIKELY(
            e - p >= size &&
            (size < 9 ||
             folly::Endian::swap(*reinterpret_cast<const uint64_t*>(p + 1)) <=
                 0xFEFDFBF7EFDFBF7FUl))) {
      return p + size;
    }
  }
  return nullptr;
}

namespace {
template <int n>
uint64_t load(unsigned char b0, const unsigned char* p);

template <>
inline uint64_t load<2>(unsigned char b0, const unsigned char* p) {
  return (uint64_t(b0 & 0x3F) << 8) + p[1] + 0x80;
}

template <>
inline uint64_t load<3>(unsigned char b0, const unsigned char* p) {
  return (uint64_t(b0 & 0x1F) << 16) +
      folly::Endian::big(folly::loadUnaligned<uint16_t>(p + 1)) + 0x4080;
}

template <>
inline uint64_t load<4>(unsigned char b0, const unsigned char* p) {
  return (folly::Endian::big(folly::loadUnaligned<uint32_t>(p)) & 0x0FFFFFFF) +
      0x204080Ul;
}

template <>
inline uint64_t load<5>(unsigned char b0, const unsigned char* p) {
  return (uint64_t(b0 & 0x7) << 32) +
      folly::Endian::big(folly::loadUnaligned<uint32_t>(p + 1)) + 0x10204080Ul;
}

template <>
inline uint64_t load<6>(unsigned char b0, const unsigned char* p) {
  return (uint64_t(b0 & 0x3) << 40) + (uint64_t(p[1]) << 32) +
      folly::Endian::big(folly::loadUnaligned<uint32_t>(p + 2)) +
      0x0810204080Ul;
}

template <>
inline uint64_t load<7>(unsigned char b0, const unsigned char* p) {
  return (uint64_t(b0 & 0x1) << 48) +
      (uint64_t(folly::Endian::big(folly::loadUnaligned<uint16_t>(p + 1)))
       << 32) +
      (folly::Endian::big(folly::loadUnaligned<uint32_t>(p + 3))) +
      0x040810204080Ul;
}

template <>
inline uint64_t load<8>(unsigned char b0, const unsigned char* p) {
  return (folly::Endian::big(folly::loadUnaligned<uint64_t>(p)) &
          0xFFFFFFFFFFFFFF) +
      0x02040810204080Ul;
}

template <>
inline uint64_t load<9>(unsigned char b0, const unsigned char* p) {
  return folly::Endian::big(folly::loadUnaligned<uint64_t>(p + 1)) +
      0x0102040810204080Ul;
}

template <int n>
inline std::pair<uint64_t, const unsigned char*> decode(
    unsigned char b0,
    const unsigned char* p) {
  return {load<n>(b0, p), p + n};
}

template <int n>
inline std::pair<uint64_t, const unsigned char*>
decodeCheck(unsigned char b0, const unsigned char* p, const unsigned char* e) {
  if (LIKELY(e - p >= n)) {
    auto r = decode<n>(b0, p);
    // We've already added 0x0102040810204080 to the number so invalid bit
    // patterns will have overflown.
    if (n != 9 || LIKELY(r.first >= 0x0102040810204080Ul)) {
      return r;
    }
  }
  return {0, nullptr};
}

} // namespace

// We use computed gotos, keyed on the first byte. This seems to be massively
// faster than anything else.
#define DECODER_LABELS(decoder)                                         \
  static const void* decoder[256] = {                                   \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, &&decode1, \
      &&decode1, &&decode1,                                             \
                                                                        \
      &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, \
      &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, \
      &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, \
      &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, \
      &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, \
      &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, \
      &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, \
      &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, \
      &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, \
      &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, &&decode2, \
      &&decode2, &&decode2, &&decode2, &&decode2,                       \
                                                                        \
      &&decode3, &&decode3, &&decode3, &&decode3, &&decode3, &&decode3, \
      &&decode3, &&decode3, &&decode3, &&decode3, &&decode3, &&decode3, \
      &&decode3, &&decode3, &&decode3, &&decode3, &&decode3, &&decode3, \
      &&decode3, &&decode3, &&decode3, &&decode3, &&decode3, &&decode3, \
      &&decode3, &&decode3, &&decode3, &&decode3, &&decode3, &&decode3, \
      &&decode3, &&decode3,                                             \
                                                                        \
      &&decode4, &&decode4, &&decode4, &&decode4, &&decode4, &&decode4, \
      &&decode4, &&decode4, &&decode4, &&decode4, &&decode4, &&decode4, \
      &&decode4, &&decode4, &&decode4, &&decode4,                       \
                                                                        \
      &&decode5, &&decode5, &&decode5, &&decode5, &&decode5, &&decode5, \
      &&decode5, &&decode5,                                             \
                                                                        \
      &&decode6, &&decode6, &&decode6, &&decode6, &&decode7, &&decode7, \
      &&decode8, &&decode9,                                             \
  }

std::pair<uint64_t, const unsigned char*> loadTrustedNat(
    const unsigned char* p) {
  DECODER_LABELS(decoder);
  const unsigned char b0 = *p;
  goto* decoder[b0];

decode1:
  return {uint64_t(b0), p + 1};

decode2:
  return decode<2>(b0, p);

decode3:
  return decode<3>(b0, p);

decode4:
  return decode<4>(b0, p);

decode5:
  return decode<5>(b0, p);

decode6:
  return decode<6>(b0, p);

decode7:
  return decode<7>(b0, p);

decode8:
  return decode<8>(b0, p);

decode9:
  return decode<9>(b0, p);
}

std::pair<uint64_t, const unsigned char * FOLLY_NULLABLE> loadUntrustedNat(
    const unsigned char* p,
    const unsigned char* e) {
  DECODER_LABELS(decoder);
  if (UNLIKELY(p >= e)) {
    return {0, nullptr};
  }
  const unsigned char b0 = *p;
  goto* decoder[b0];

decode1:
  return {uint64_t(b0), p + 1};

decode2:
  return decodeCheck<2>(b0, p, e);

decode3:
  return decodeCheck<3>(b0, p, e);

decode4:
  return decodeCheck<4>(b0, p, e);

decode5:
  return decodeCheck<5>(b0, p, e);

decode6:
  return decodeCheck<6>(b0, p, e);

decode7:
  return decodeCheck<7>(b0, p, e);

decode8:
  return decodeCheck<8>(b0, p, e);

decode9:
  return decodeCheck<9>(b0, p, e);
}

size_t storeNat(unsigned char* out, uint64_t val) {
  // There is probably ample room for improvement here.
  if (val < 0x80) {
    *out = static_cast<unsigned char>(val);
    return 1;
  } else if (val < 0x4080) {
    folly::storeUnaligned<uint16_t>(
        out, folly::Endian::big(static_cast<uint16_t>(val - 0x80)) | 0x80);
    return 2;
  } else if (val < 0x204080) {
    val -= 0x4080;
    out[0] = 0xC0 | static_cast<unsigned char>(val >> 16);
    folly::storeUnaligned<uint16_t>(
        out + 1, folly::Endian::big(static_cast<uint16_t>(val)));
    return 3;
  } else if (val < 0x10204080) {
    val -= 0x204080;
    folly::storeUnaligned<uint32_t>(
        out, folly::Endian::big(static_cast<uint32_t>(val | 0xE0000000)));
    return 4;
  } else if (val < 0x0810204080) {
    val -= 0x10204080;
    out[0] = 0xF0 | static_cast<unsigned char>(val >> 32);
    folly::storeUnaligned<uint32_t>(
        out + 1, folly::Endian::big(static_cast<uint32_t>(val)));
    return 5;
  } else if (val < 0x040810204080) {
    val -= 0x0810204080;
    folly::storeUnaligned<uint16_t>(
        out, folly::Endian::big(static_cast<uint16_t>((val >> 32) | 0xF800)));
    folly::storeUnaligned<uint32_t>(
        out + 2, folly::Endian::big(static_cast<uint32_t>(val)));
    return 6;
  } else if (val < 0x02040810204080) {
    val -= 0x040810204080;
    out[0] = 0xFC | static_cast<unsigned char>(val >> 48);
    folly::storeUnaligned<uint16_t>(
        out + 1, folly::Endian::big(static_cast<uint16_t>(val >> 32)));
    folly::storeUnaligned<uint32_t>(
        out + 3, folly::Endian::big(static_cast<uint32_t>(val)));
    return 7;
  } else if (val < 0x0102040810204080) {
    val -= 0x02040810204080;
    folly::storeUnaligned<uint64_t>(
        out, folly::Endian::big(val | 0xFE00000000000000Ul));
    return 8;
  } else {
    val -= 0x0102040810204080;
    out[0] = 0xFF;
    folly::storeUnaligned<uint64_t>(out + 1, folly::Endian::big(val));
    return 9;
  }
}

#if 0
size_t storeNat_new_32(unsigned char *out, uint32_t val) {
  const auto n = uint64_t(val) - 0x0102040810204080;
  const auto w = n ^ ~0x0102040810204080;
  const auto b0 = __builtin_clzl(w);
  const auto i = b0/8;
  const auto k = i*8;
  const auto m = n << k;
  const auto r = m & ~(1ul << (i+56));
  const auto size = 8-i;
  folly::storeUnaligned(out, folly::Endian::big(r));
  return size;
}
#endif

size_t storeNat_new(unsigned char *out, uint64_t val) {
  if (val < 0x80) {
    *out = static_cast<unsigned char>(val);
    return 1;
  }
  const auto n = val - 0x0102040810204080;
  if (FOLLY_LIKELY(val < 0x0102040810204080)) {
    const auto w = n ^ ~0x0102040810204080;
  // if (((val | w) & 0x8000000000000000Ul) == 0) {
    const auto b0 = __builtin_clzl(w);
    const auto i = b0/8;
    const auto k = i*8;
    const auto m = n << k;
    const auto r = m & ~(1ul << (i+56));
    const auto size = 8-i;
    folly::storeUnaligned(out, folly::Endian::big(r));
    return size;
  } else {
    *out++ = 0xff;
    folly::storeUnaligned(out, folly::Endian::big(n));
    return 9;
  }
}

} // namespace rts
} // namespace glean
} // namespace facebook
