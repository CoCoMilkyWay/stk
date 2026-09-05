#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>

// ============================================================================
// SPARSE CODEC — 特征张量落盘编码之一: 位图 + 非零字面 (手搓, 无第三方依赖,
// 与 ZstdCodec 同 API 对仗, bound / encode / decode; 选型见 FeatureStoreConfig.hpp)
// ============================================================================
// 数据特性: L0 事件稀疏 / L1·DEPTH 经 T 轴 XOR 差分 (见 FeatureStoreConfig.hpp)
// 后, 零字 (uint16) 占比 ~60%, 但沿 A 轴逐字散布不成游程 —— RLE 吃不到 (实测
// 仅 ~82%), 位图刚好: 每 64 字 1 个 u64 掩码 (1/16 开销) + 非零字面原样.
//
// 实测 (5897 资产, 2023/01/03): 全天 945 MB → ~422 MB (45%). 压缩率不及
// ZstdCodec (29%), 但单遍分支免除扫描是内存带宽级吞吐, IO worker 单核绰绰有余.
//
// 格式: [u64 位图 × ceil(words/64)][非零 uint16 字面 ...], 位图 LSB 优先.
// 解码端 raw_size 由形状推出 (T×F×A, 文件头校验), 载荷长度与位图 popcount
// 必须自洽 —— 旧格式文件 (裸写/zstd) 在此立刻断言失败, 不会静默出错.
// ============================================================================

class SparseCodec {
public:
  // 编码输出上界: 位图 + 全字面 (无零时)
  static constexpr size_t bound(size_t raw_bytes) {
    const size_t words = raw_bytes / sizeof(uint16_t);
    return ((words + 63) / 64) * sizeof(uint64_t) + raw_bytes;
  }

  // 返回实际载荷字节数
  static size_t encode(const void *src, size_t raw_bytes, void *dst, size_t dst_capacity) {
    assert(raw_bytes % sizeof(uint16_t) == 0);
    assert(dst_capacity >= bound(raw_bytes));
    const uint16_t *s = static_cast<const uint16_t *>(src);
    const size_t words = raw_bytes / sizeof(uint16_t);
    const size_t nblk = (words + 63) / 64;
    uint64_t *bm = static_cast<uint64_t *>(dst);
    uint16_t *lit = reinterpret_cast<uint16_t *>(bm + nblk);
    for (size_t b = 0; b < nblk; ++b) {
      const size_t base = b * 64;
      const size_t n = words - base < 64 ? words - base : 64;
      uint64_t m = 0;
      for (size_t i = 0; i < n; ++i) {
        const uint16_t v = s[base + i];
        m |= static_cast<uint64_t>(v != 0) << i;
        *lit = v; // 分支免除: 无条件写, 非零才推进
        lit += v != 0;
      }
      bm[b] = m;
    }
    return static_cast<size_t>(reinterpret_cast<uint8_t *>(lit) - static_cast<uint8_t *>(dst));
  }

  static void decode(const void *src, size_t payload_bytes, void *dst, size_t raw_bytes) {
    assert(raw_bytes % sizeof(uint16_t) == 0);
    const size_t words = raw_bytes / sizeof(uint16_t);
    const size_t nblk = (words + 63) / 64;
    const uint64_t *bm = static_cast<const uint64_t *>(src);
    const uint16_t *lit = reinterpret_cast<const uint16_t *>(bm + nblk);
    uint16_t *d = static_cast<uint16_t *>(dst);
    std::memset(d, 0, raw_bytes);
    for (size_t b = 0; b < nblk; ++b)
      for (uint64_t m = bm[b]; m; m &= m - 1) // 只走置位, ctz 跳零
        d[b * 64 + static_cast<size_t>(__builtin_ctzll(m))] = *lit++;
    assert(reinterpret_cast<const uint8_t *>(lit) - static_cast<const uint8_t *>(src) ==
               static_cast<ptrdiff_t>(payload_bytes) &&
           "载荷长度与位图不自洽: 旧格式特征文件 (裸写/zstd) 或文件损坏, 需重算");
  }
};
