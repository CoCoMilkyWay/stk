#pragma once

#include "../../../package/zstd-1.5.7/lib/zstd.h"
#include <cassert>
#include <cstddef>
#include <cstring>

// ============================================================================
// ZSTD COMPRESSION HELPER
// ============================================================================
// 特征张量 (fp16) 的压缩/解压. COMPRESSION_LEVEL == 0 时整条链退化为直读直写
// (FeatureStore / FeatureRead 各自 if constexpr 免掉中转拷贝, 这里只剩解压兜底).
// ============================================================================

class ZstdHelper {
public:
  // Compression level: 0=no compression, 1=fastest, 22=best compression
  static constexpr int COMPRESSION_LEVEL = 0;

  // Compress to pre-allocated buffer (returns actual size)
  static size_t compress_to_buffer(const void *src, size_t src_size, void *dst, size_t dst_capacity) {
    if constexpr (COMPRESSION_LEVEL == 0) {
      assert(dst_capacity >= src_size);
      std::memcpy(dst, src, src_size);
      return src_size;
    } else {
      size_t compressed_size = ZSTD_compress(dst, dst_capacity, src, src_size, COMPRESSION_LEVEL);
      assert(!ZSTD_isError(compressed_size));
      return compressed_size;
    }
  }

  // Get upper bound for compressed size
  static size_t compress_bound(size_t size) {
    if constexpr (COMPRESSION_LEVEL == 0) {
      return size;
    } else {
      return ZSTD_compressBound(size);
    }
  }

  // Decompress to pre-allocated buffer (returns decompressed size)
  static size_t decompress(const void *src, size_t src_size, void *dst, size_t dst_capacity) {
    if constexpr (COMPRESSION_LEVEL == 0) {
      assert(dst_capacity >= src_size);
      std::memcpy(dst, src, src_size);
      return src_size;
    } else {
      size_t decompressed_size = ZSTD_decompress(dst, dst_capacity, src, src_size);
      assert(!ZSTD_isError(decompressed_size));
      return decompressed_size;
    }
  }
};
