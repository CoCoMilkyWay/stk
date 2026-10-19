#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>

// ============================================================================
// SPARSE CODEC — 特征列落盘编码之一: 位图 + 非零字面 (手搓, 无第三方依赖,
// 与 ZstdCodec 同 API 对仗, bound / encode / decode; 选型见 FeatureDay.hpp)
// ============================================================================
// 单位 = 一列 [T][A] (fp16 按位当 uint16). delta = 沿 T 轴 XOR 差分 (字 k 存
// col[k] ^ col[k − A], 首行原样; 无损): 稠密慢变列 (L_1min / 整日常量) 差分后大片零,
// 事件稀疏列本身就是大片零, 差分反而打散 —— 由写端逐列按数据定 (FeatureDay::write).
// 零字沿 A 轴逐字散布不成游程 —— RLE 吃不到 (实测仅 ~82%), 位图刚好:
// 每 64 字 1 个 u64 掩码 (1/16 开销) + 非零字面原样.
//
// 实测 (5897 资产, 2023/01/03): 全天 945 MB → ~422 MB (45%). 压缩率不及
// ZstdCodec (29%), 但单遍分支免除扫描是内存带宽级吞吐, IO worker 单核绰绰有余.
//
// 格式: [u64 位图 × ceil(words/64)][非零 uint16 字面 ...][u16 尾填充 0], 位图 LSB 优先.
// 尾填充字让解码无分支取字面 (多读一字不越界). 解码端字数由形状推出 (T×A, 文件头校验),
// 载荷长度与位图 popcount 必须自洽 —— 旧格式文件在此立刻断言失败, 不会静默出错.
// 差分还原融合进解码同一遍: 字 k 只依赖 k − A, 升序写时它已是终值.
// ============================================================================

class SparseCodec {
public:
  // 编码输出上界: 位图 + 全字面 (无零时) + 尾填充字
  static constexpr size_t bound(size_t words) {
    return ((words + 63) / 64) * sizeof(uint64_t) + (words + 1) * sizeof(uint16_t);
  }

  // col 不改写 (与 ZstdCodec 同签名). 返回实际载荷字节数
  static size_t encode(void *col, size_t T, size_t A, bool delta, void *dst, size_t dst_capacity) {
    assert(dst_capacity >= bound(T * A));
    return delta ? encode_<true>(static_cast<const uint16_t *>(col), T * A, A, dst)
                 : encode_<false>(static_cast<const uint16_t *>(col), T * A, A, dst);
  }

  static void decode(const void *src, size_t payload_bytes, void *col, size_t T, size_t A, bool delta) {
    const uint16_t *end = delta ? decode_<true>(src, static_cast<uint16_t *>(col), T * A, A)
                                : decode_<false>(src, static_cast<uint16_t *>(col), T * A, A);
    assert(reinterpret_cast<const uint8_t *>(end + 1) - static_cast<const uint8_t *>(src) ==
               static_cast<ptrdiff_t>(payload_bytes) &&
           "载荷长度与位图不自洽: 旧格式特征文件或文件损坏, 需重算");
    (void)end;
    (void)payload_bytes;
  }

private:
  template <bool kDelta>
  static size_t encode_(const uint16_t *s, size_t words, size_t A, void *dst) {
    const size_t nblk = (words + 63) / 64;
    uint64_t *bm = static_cast<uint64_t *>(dst);
    uint16_t *lit = reinterpret_cast<uint16_t *>(bm + nblk);
    for (size_t b = 0; b < nblk; ++b) {
      const size_t base = b * 64;
      const size_t n = words - base < 64 ? words - base : 64;
      uint64_t m = 0;
      for (size_t i = 0; i < n; ++i) {
        const size_t k = base + i;
        const uint16_t v = kDelta && k >= A ? static_cast<uint16_t>(s[k] ^ s[k - A]) : s[k];
        m |= static_cast<uint64_t>(v != 0) << i;
        *lit = v; // 分支免除: 无条件写, 非零才推进
        lit += v != 0;
      }
      bm[b] = m;
    }
    *lit++ = 0; // 尾填充字
    return static_cast<size_t>(reinterpret_cast<uint8_t *>(lit) - static_cast<uint8_t *>(dst));
  }

  // 返回字面末尾 (= 尾填充字位置)
  template <bool kDelta>
  static const uint16_t *decode_(const void *src, uint16_t *d, size_t words, size_t A) {
    const size_t nblk = (words + 63) / 64;
    const uint64_t *bm = static_cast<const uint64_t *>(src);
    const uint16_t *lit = reinterpret_cast<const uint16_t *>(bm + nblk);
    for (size_t b = 0; b < nblk; ++b) {
      const size_t base = b * 64;
      const size_t n = words - base < 64 ? words - base : 64;
      uint64_t m = bm[b];
      if constexpr (!kDelta) {
        // 稀疏列: 块清零 (在缓存里) 后只走置位, ctz 跳零
        std::memset(d + base, 0, n * sizeof(uint16_t));
        for (; m; m &= m - 1)
          d[base + static_cast<size_t>(__builtin_ctzll(m))] = *lit++;
      } else {
        for (size_t i = 0; i < n; ++i) {
          const size_t k = base + i;
          const uint16_t bit = static_cast<uint16_t>((m >> i) & 1u);
          const uint16_t v = static_cast<uint16_t>(*lit & static_cast<uint16_t>(0u - bit));
          lit += bit;
          d[k] = k >= A ? static_cast<uint16_t>(v ^ d[k - A]) : v;
        }
      }
    }
    return lit;
  }
};
