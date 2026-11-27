#pragma once

// CPU 后端的行内排序核 (CS 序统计族与 Stat 秩同一份; 只在 CPU 后端内共享, 不跨后端):
//   稳定 LSD 基数排序 k[0, n) 按位段 [lo, lo + bits) (8 位一趟, 各趟直方图一遍扫齐; 全同字节的趟跳过); tmp = n 个 u64 暂存.
//   稳定 ⇒ 位段相等者保持入序: 调用方按资产升序装键 (低位 = 资产 / 在池位), 结果与整键 std::sort 逐位同序.
//   一行几百个键时比比较排序省一个量级 (无分支误预测, 每趟两遍线性扫)
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <utility>

namespace factor::cpu {

inline void radix_sort(uint64_t *k, uint64_t *tmp, int n, int lo, int bits) {
  constexpr int kMaxPass = 8;
  const int P = (bits + 7) / 8;
  assert(lo >= 0 && bits >= 1 && lo + bits <= 64 && P <= kMaxPass);
  if (n <= 1)
    return;
  int cnt[kMaxPass][256];
  std::fill_n(&cnt[0][0], P * 256, 0);
  for (int i = 0; i < n; ++i)
    for (int p = 0; p < P; ++p)
      ++cnt[p][(k[i] >> (lo + 8 * p)) & 0xffu];
  uint64_t *src = k, *dst = tmp;
  for (int p = 0; p < P; ++p) {
    const int sh = lo + 8 * p;
    int *c = cnt[p];
    if (c[(k[0] >> sh) & 0xffu] == n) // 本趟字节全同: 不动
      continue;
    for (int b = 0, off = 0; b < 256; ++b) {
      const int x = c[b];
      c[b] = off, off += x;
    }
    for (int i = 0; i < n; ++i)
      dst[c[(src[i] >> sh) & 0xffu]++] = src[i];
    std::swap(src, dst);
  }
  if (src != k)
    std::copy_n(src, n, k);
}

} // namespace factor::cpu
