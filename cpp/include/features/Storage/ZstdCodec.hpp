#pragma once

#include "../../../package/zstd-1.5.7/lib/zstd.h"
#include <cassert>
#include <cstddef>
#include <cstdint>

// ============================================================================
// ZSTD CODEC — 特征列落盘编码之二: 通用熵压缩 (与 SparseCodec 同 API 对仗,
// bound / encode / decode; 选型见 FeatureDay.hpp 的 FeatureCodec)
// ============================================================================
// 单位 = 一列 [T][A]; delta = 沿 T 轴 XOR 差分 (语义同 SparseCodec), 这里是压缩前 /
// 解压后各一遍就地差分 (zstd 不能融合).
// 实测 (5897 资产, 2023/01/03, L_1min 差分后): 全天 945 MB → ~272 MB (29%).
// 比 SparseCodec 压得狠 (零之外还吃非零值的重复与熵), 但 level 1 单核仅
// ~185 MB/s, 顶不住 ~6.4 s/天 的落盘节奏 —— 单 IO worker 时慎选.
// level 1↔3 压缩率差距 <1%, 吞吐优先取 1.
// ============================================================================

class ZstdCodec {
public:
  static constexpr int LEVEL = 1; // 1=fastest .. 22=best

  // 编码输出上界
  static size_t bound(size_t words) { return ZSTD_compressBound(words * sizeof(uint16_t)); }

  // delta 时 col 被就地差分 (调用方的抽列缓冲). 返回实际载荷字节数
  static size_t encode(void *col, size_t T, size_t A, bool delta, void *dst, size_t dst_capacity) {
    uint16_t *u = static_cast<uint16_t *>(col);
    if (delta)
      for (size_t k = T * A; k-- > A;)
        u[k] ^= u[k - A];
    const size_t payload = ZSTD_compress(dst, dst_capacity, col, T * A * sizeof(uint16_t), LEVEL);
    assert(!ZSTD_isError(payload));
    return payload;
  }

  static void decode(const void *src, size_t payload_bytes, void *col, size_t T, size_t A, bool delta) {
    [[maybe_unused]] const size_t n = ZSTD_decompress(col, T * A * sizeof(uint16_t), src, payload_bytes);
    assert(!ZSTD_isError(n) && n == T * A * sizeof(uint16_t) &&
           "解压尺寸与形状不符: 旧格式文件 / 编码选型与文件不符 / 文件损坏, 需重算");
    uint16_t *u = static_cast<uint16_t *>(col);
    if (delta)
      for (size_t k = A; k < T * A; ++k)
        u[k] ^= u[k - A];
  }
};
