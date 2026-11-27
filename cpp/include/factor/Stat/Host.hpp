#pragma once

// =============================================================================
// Stat 行核的宿主版 (CPU; 契约 factor/Stat/Contract.hpp): 一行的 rank / 一 (t, hold) 的 Row
// =============================================================================
//   Stat/Cpu.hpp 用, 与 Stat/Dev.cuh 对仗 (同一组行核同名同参序, 一边宿主一边设备). 单线程, 无流程 / 无分配.
//     KeyX / KeyY 取键器 (行视图, 与 Dev.cuh 同名同形): 因子一行 (只看池内 g ∧ m 的格) / 做多收益一行 (fp16 位) → 保序键 / kNoKey
//     rank_row    精确并列均秩: key(a) → LSD 基数排序 (n ≤ 1024 用 8 位 × 4 趟, 否则 11 位 × 3 趟) → 等值段均秩 → out
//     rank_x_row  因子一行: CS rank_row(KeyX) / TS 直接量化 —— 口径分叉只在这里; 方向在此一次定 (契约【方向 Dir】)
//     row_stat    一 (t, hold) 完整行 = 换手侧 lag_row (t−h → t) + 标签侧 label_row (t → t+h, 开仓行才算):
//                 整数五和 / 分组和 / 尾部和一遍 A 轴 (仓位感知 Pos.hpp cell: 每格一次链回走同出组与各级尾部), 收尾 finish_row (绝对口径)
//   换手 (账集) 与池基准 pool_mean 在 Stat/Book.hpp (只有宿主版).
//   【标签】fp16 位 → float 用 _Float16 (clang, F16C), 精确转换, 与 GPU __half2float 逐位一致.
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Pos.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <vector>

namespace factor::cpu::stat {

using factor::stat::Frame;
using factor::stat::kEnds;
using factor::stat::kGroups;
using factor::stat::kMaxA;
using factor::stat::kNoKey;
using factor::stat::kRankNone;
using factor::stat::kSums;
using factor::stat::kTails;
using factor::stat::Label;
using factor::stat::Row;

inline float h2f(uint16_t b) {
  _Float16 h;
  std::memcpy(&h, &b, sizeof(h));
  return static_cast<float>(h);
}
struct H2F {
  float operator()(uint16_t b) const { return h2f(b); }
};

// ---- 取键器: 格 a (指针已偏到行首) → 保序键 / kNoKey ----
struct KeyX { // 因子一行, 只看池内 g ∧ m 的格 (契约【截面池 g】); g = m 即不看池
  const float *v;
  const uint8_t *m, *g;
  bool on(int a) const { return (m[a] & g[a]) != 0; }
  unsigned key(int a) const { return on(a) ? factor::stat::ord(v[a]) : kNoKey; }
};
struct KeyY { // 做多收益一行 (fp16 位)
  const uint16_t *v;
  const uint8_t *m;
  unsigned key(int a) const { return m[a] ? factor::stat::ord(h2f(v[a])) : kNoKey; }
};

// 每线程排序暂存 (2 × A × 8B + 桶计数), 跨行 / 跨因子复用
inline constexpr int kRadixMaxBits = 11;
struct Scratch {
  std::vector<uint64_t> a, b; // (key << 16) | idx
  int cnt[1 << kRadixMaxBits];
  void reserve(int A) {
    assert(A >= 1 && A <= kMaxA);
    a.resize(static_cast<size_t>(A));
    b.resize(static_cast<size_t>(A));
  }
};

// 一行的精确并列均秩: key.key(a) 给保序键或 kNoKey; out[a] = r16 / kRankNone
template <class Key>
inline void rank_row(const Key &key, int A, uint16_t *out, Scratch &sc) {
  assert(sc.a.size() >= static_cast<size_t>(A));
  int n = 0;
  for (int a = 0; a < A; ++a) {
    const unsigned k = key.key(a);
    sc.a[static_cast<size_t>(n)] = (static_cast<uint64_t>(k) << 16) | static_cast<uint64_t>(a);
    n += k != kNoKey;
  }
  std::fill(out, out + A, kRankNone);
  if (n < 2)
    return;
  // LSD 基数排序: 键在位 16..47 (32 位), 稳定. 小 n 时桶清零 / 前缀和是固定开销, 用窄桶多趟
  const int bits = n <= 1024 ? 8 : kRadixMaxBits, passes = (32 + bits - 1) / bits, radix = 1 << bits, mask = radix - 1;
  uint64_t *src = sc.a.data(), *dst = sc.b.data();
  for (int p = 0; p < passes; ++p) {
    const int shift = 16 + bits * p;
    std::fill(sc.cnt, sc.cnt + radix, 0);
    for (int i = 0; i < n; ++i)
      ++sc.cnt[(src[i] >> shift) & mask];
    int run = 0;
    for (int d = 0; d < radix; ++d) {
      const int c = sc.cnt[d];
      sc.cnt[d] = run;
      run += c;
    }
    for (int i = 0; i < n; ++i)
      dst[sc.cnt[(src[i] >> shift) & mask]++] = src[i];
    std::swap(src, dst);
  }
  const auto key_at = [&](int i) { return static_cast<unsigned>(src[i] >> 16); };
  if (!(key_at(n - 1) > key_at(0))) // 全并列 = spread 为假
    return;
  for (int i = 0; i < n;) {
    int j = i + 1;
    while (j < n && key_at(j) == key_at(i))
      ++j;
    const uint16_t r = factor::stat::r16_of(i, j - i, n);
    for (int p = i; p < j; ++p)
      out[src[p] & 0xFFFFu] = r;
    i = j;
  }
}

// 因子一行 (口径 f, 方向 d) → 已定向的 r16
inline void rank_x_row(const KeyX &key, Frame f, int d, int A, uint16_t *out, Scratch &sc) {
  if (f == Frame::CS)
    rank_row(key, A, out, sc);
  else
    for (int a = 0; a < A; ++a)
      out[a] = key.on(a) ? factor::stat::r16_quant(key.v[a]) : kRankNone;
  if (d != factor::stat::kFwd)
    for (int a = 0; a < A; ++a)
      out[a] = factor::stat::oriented(out[a], d);
}

// 换手侧 (t−h → t): rx = 本行 r16, rlag = t−h 行 r16 → r.rac / r.ok_rac. 也是两候选同行截面相关的核 (rlag 换成另一候选)
inline void lag_row(const uint16_t *rx, const uint16_t *rlag, int A, Row &r) {
  unsigned long long s[kSums] = {};
  for (int a = 0; a < A; ++a)
    factor::stat::pair_add(s, rx[a], rlag[a], rx[a] != kRankNone && rlag[a] != kRankNone);
  bool ok = false;
  r.rac = factor::stat::pearson_int(s, ok);
  r.ok_rac = ok ? 1u : 0u;
}

// 真持有格的回调 (宿主路径的可选出口, 观察工具要两端组的格时给): cell(t, a, g, p, y) = 行 t 格 a 在组 g 真持有, 仓位 p, 两腿 y; on = false 整段编译期消失
struct NoCells {
  static constexpr bool on = false;
  void cell(int, int, int, const factor::stat::Pos &, const float *) const {}
};

// 标签侧 (t → t+h): rx = 本行 r16 (已定向), lb / eo 已偏到行首 (交接链回看 / 前看同块其他行; 本行池基准 mk / 满仓 cap 按 c.t 取), c = 本行交接链
//   → r 的 ric / mkt / cap / grp / cnt / tail / ok. ric 在 J 上 (与可成交无关); 组与尾部只收真持有的格 (一格一次 cell 出层级), 收益按续持取名义值
template <class Cells = NoCells>
inline void label_row(const uint16_t *rx, const Label &lb, const uint8_t *eo, const factor::stat::Chain &c, Row &r, const Cells &cs = {}) {
  unsigned long long s[kSums] = {};
  double gs[kGroups] = {}, ts[kEnds][kTails] = {};
  int gc[kGroups] = {}, tc[kEnds][kTails] = {};
  for (int a = 0; a < c.A; ++a) {
    const bool ok = rx[a] != kRankNone && lb.ry[a] != kRankNone;
    factor::stat::pair_add(s, rx[a], lb.ry[a], ok);
    if (!ok)
      continue;
    const int g = factor::stat::grp_of(rx[a]);
    float y[2];
    const factor::stat::Pos p = factor::stat::cell(c, lb, eo, a, factor::stat::InRank{rx + a, g}, H2F{}, y);
    if (p.held < 0)
      continue;
    if constexpr (Cells::on)
      cs.cell(c.t, a, g, p, y);
    gs[g] += factor::stat::ret_at(p, 0, y);
    ++gc[g];
    const int e = g == factor::stat::end_group(factor::stat::kTop) ? factor::stat::kTop : g == factor::stat::end_group(factor::stat::kBot) ? factor::stat::kBot
                                                                                                                                           : -1;
    if (e < 0)
      continue;
    for (int j = 0; j < kTails && j < p.held; ++j) {
      ts[e][j] += factor::stat::ret_at(p, 1 + j, y);
      ++tc[e][j];
    }
  }
  factor::stat::finish_row(s, lb.mk[c.t], gs, gc, ts, tc, lb.cap[c.t], r);
}

// 一 (t, hold) 完整行: rlag = t−h 行 r16 (没有 → nullptr, 无 rAC); lb / eo 已偏到行首; c = 本行交接链; opens = 开仓行 (Hold::opens)
template <class Cells = NoCells>
inline Row row_stat(const uint16_t *rx, const uint16_t *rlag, const Label &lb, const uint8_t *eo, const factor::stat::Chain &c, bool opens, const Cells &cs = {}) {
  Row r;
  if (rlag)
    lag_row(rx, rlag, c.A, r);
  if (opens)
    label_row(rx, lb, eo, c, r, cs);
  return r;
}

} // namespace factor::cpu::stat
