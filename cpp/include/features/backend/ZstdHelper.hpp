#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
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
  // Compression level (1=fastest, 22=best compression)
  static constexpr int DEFAULT_LEVEL = 0;

  // ========================================================================
  // Compression
  // ========================================================================

  // Compress data to vector (allocates)
  static std::vector<uint8_t> compress(const void *data, size_t size, int level = DEFAULT_LEVEL) {
    size_t bound = ZSTD_compressBound(size);
    std::vector<uint8_t> compressed(bound);

    size_t compressed_size = ZSTD_compress(compressed.data(), bound, data, size, level);
    assert(!ZSTD_isError(compressed_size));

    compressed.resize(compressed_size);
    return compressed;
  }

  // Compress to pre-allocated buffer (returns actual size)
  static size_t compress_to_buffer(const void *src, size_t src_size, void *dst, size_t dst_capacity, int level = DEFAULT_LEVEL) {
    size_t compressed_size = ZSTD_compress(dst, dst_capacity, src, src_size, level);
    assert(!ZSTD_isError(compressed_size));
    return compressed_size;
  }

  // Get upper bound for compressed size
  static size_t compress_bound(size_t size) {
    return ZSTD_compressBound(size);
  }

  // ========================================================================
  // Decompression
  // ========================================================================

  // Decompress to pre-allocated buffer (returns decompressed size)
  static size_t decompress(const void *src, size_t src_size, void *dst, size_t dst_capacity) {
    size_t decompressed_size = ZSTD_decompress(dst, dst_capacity, src, src_size);
    assert(!ZSTD_isError(decompressed_size));
    return decompressed_size;
  }

  // Decompress to vector (allocates based on frame header)
  static std::vector<uint8_t> decompress_alloc(const void *src, size_t src_size) {
    unsigned long long decompressed_size = ZSTD_getFrameContentSize(src, src_size);
    assert(decompressed_size != ZSTD_CONTENTSIZE_UNKNOWN);
    assert(decompressed_size != ZSTD_CONTENTSIZE_ERROR);

    std::vector<uint8_t> decompressed(decompressed_size);
    size_t result = ZSTD_decompress(decompressed.data(), decompressed_size, src, src_size);
    assert(!ZSTD_isError(result));
    assert(result == decompressed_size);

    return decompressed;
  }

  // Get decompressed size from frame header
  static size_t get_decompressed_size(const void *src, size_t src_size) {
    unsigned long long size = ZSTD_getFrameContentSize(src, src_size);
    assert(size != ZSTD_CONTENTSIZE_UNKNOWN);
    assert(size != ZSTD_CONTENTSIZE_ERROR);
    return static_cast<size_t>(size);
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

