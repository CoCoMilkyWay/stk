#pragma once

#include "../../../package/zstd-1.5.7/lib/zstd.h"
#include <cassert>
#include <cstddef>

// ============================================================================
// ZSTD CODEC — 特征张量落盘编码之二: 通用熵压缩 (与 SparseCodec 同 API 对仗,
// bound / encode / decode; 选型见 FeatureStoreConfig.hpp 的 FeatureCodec)
// ============================================================================
// 实测 (5897 资产, 2023/01/03, xor_delta 层差分后): 全天 945 MB → ~272 MB (29%).
// 比 SparseCodec 压得狠 (零之外还吃非零值的重复与熵), 但 level 1 单核仅
// ~185 MB/s, 顶不住 ~6.4 s/天 的落盘节奏 —— 单 IO worker 时慎选.
// level 1↔3 压缩率差距 <1%, 吞吐优先取 1.
// ============================================================================

class ZstdCodec {
public:
  static constexpr int LEVEL = 1; // 1=fastest .. 22=best

  // 编码输出上界
  static size_t bound(size_t raw_bytes) { return ZSTD_compressBound(raw_bytes); }

  // 返回实际载荷字节数
  static size_t encode(const void *src, size_t raw_bytes, void *dst, size_t dst_capacity) {
    const size_t payload = ZSTD_compress(dst, dst_capacity, src, raw_bytes, LEVEL);
    assert(!ZSTD_isError(payload));
    return payload;
  }

  static void decode(const void *src, size_t payload_bytes, void *dst, size_t raw_bytes) {
    [[maybe_unused]] const size_t n = ZSTD_decompress(dst, raw_bytes, src, payload_bytes);
    assert(!ZSTD_isError(n) && n == raw_bytes &&
           "解压尺寸与形状不符: 旧格式文件 / 编码选型与文件不符 / 文件损坏, 需重算");
  }
};
