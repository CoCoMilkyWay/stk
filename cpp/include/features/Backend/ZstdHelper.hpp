#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>
#include "../../../package/zstd-1.5.7/lib/zstd.h"

// ============================================================================
// ZSTD COMPRESSION HELPER
// ============================================================================
// Lightweight wrapper for zstd compression/decompression
// Optimized for feature tensor data (float16 arrays)
// ============================================================================

class ZstdHelper {
public:
  // Compression level: 0=no compression, 1=fastest, 22=best compression
  static constexpr int COMPRESSION_LEVEL = 0;

  // ========================================================================
  // Compression
  // ========================================================================

  // Compress data to vector (allocates)
  static std::vector<uint8_t> compress(const void *data, size_t size) {
    if constexpr (COMPRESSION_LEVEL == 0) {
      // Skip compression, return raw copy
      const uint8_t *src = static_cast<const uint8_t *>(data);
      return std::vector<uint8_t>(src, src + size);
    } else {
      size_t bound = ZSTD_compressBound(size);
      std::vector<uint8_t> compressed(bound);

      size_t compressed_size = ZSTD_compress(compressed.data(), bound, data, size, COMPRESSION_LEVEL);
      assert(!ZSTD_isError(compressed_size));

      compressed.resize(compressed_size);
      return compressed;
    }
  }

  // Compress to pre-allocated buffer (returns actual size)
  static size_t compress_to_buffer(const void *src, size_t src_size, void *dst, size_t dst_capacity) {
    if constexpr (COMPRESSION_LEVEL == 0) {
      // Skip compression, copy raw data
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
      return size;  // No compression, size stays the same
    } else {
      return ZSTD_compressBound(size);
    }
  }

  // ========================================================================
  // Decompression
  // ========================================================================

  // Decompress to pre-allocated buffer (returns decompressed size)
  static size_t decompress(const void *src, size_t src_size, void *dst, size_t dst_capacity) {
    if constexpr (COMPRESSION_LEVEL == 0) {
      // Skip decompression, copy raw data
      assert(dst_capacity >= src_size);
      std::memcpy(dst, src, src_size);
      return src_size;
    } else {
      size_t decompressed_size = ZSTD_decompress(dst, dst_capacity, src, src_size);
      assert(!ZSTD_isError(decompressed_size));
      return decompressed_size;
    }
  }

  // Decompress to vector (allocates based on frame header or provided size)
  static std::vector<uint8_t> decompress_alloc(const void *src, size_t src_size, size_t decompressed_size = 0) {
    if constexpr (COMPRESSION_LEVEL == 0) {
      // Skip decompression, return raw copy
      const uint8_t *src_ptr = static_cast<const uint8_t *>(src);
      return std::vector<uint8_t>(src_ptr, src_ptr + src_size);
    } else {
      if (decompressed_size == 0) {
        unsigned long long size = ZSTD_getFrameContentSize(src, src_size);
        assert(size != ZSTD_CONTENTSIZE_UNKNOWN);
        assert(size != ZSTD_CONTENTSIZE_ERROR);
        decompressed_size = static_cast<size_t>(size);
      }

      std::vector<uint8_t> decompressed(decompressed_size);
      size_t result = ZSTD_decompress(decompressed.data(), decompressed_size, src, src_size);
      assert(!ZSTD_isError(result));
      assert(result == decompressed_size);

      return decompressed;
    }
  }

  // Get decompressed size from frame header
  static size_t get_decompressed_size(const void *src, size_t src_size) {
    if constexpr (COMPRESSION_LEVEL == 0) {
      return src_size;  // No compression, size is unchanged
    } else {
      unsigned long long size = ZSTD_getFrameContentSize(src, src_size);
      assert(size != ZSTD_CONTENTSIZE_UNKNOWN);
      assert(size != ZSTD_CONTENTSIZE_ERROR);
      return static_cast<size_t>(size);
    }
  }

  // ========================================================================
  // Error Handling
  // ========================================================================

  static bool is_error(size_t code) {
    return ZSTD_isError(code);
  }

  static const char *get_error_name(size_t code) {
    return ZSTD_getErrorName(code);
  }
};

