#pragma once

// =============================================================================
// Stat 行核的宿主版 (CPU; 契约 factor/Stat/Contract.hpp): 一行的 rank / 一 (t, hold) 的 Row
// =============================================================================
//   Stat/Cpu.hpp 与 Search/Cpu.hpp 两个 CPU 算子共用, 与 Stat/Dev.cuh 对仗 (同一组行核, 一边宿主一边设备). 单线程, 无流程 / 无分配.
//     rank_row    精确并列均秩: 有效格 → 保序键 → LSD 基数排序 (位宽按 n 自适应: n ≤ 1024 用 8 位 × 4 趟, 否则 11 位 × 3 趟;
//                 任何正确的稳定排序给出同一 r16) → 等值段均秩 → out. n < 2 或全并列 → 整行 kRankNone
//     rank_x_row  因子一行 (只看池内 g ∧ xm 的格): CS 排序 / TS 直接量化 —— 口径分叉只在这里
//     rank_y_row  做多标签一行 (fp16 位) → ry. 与因子无关, 常驻期算一次
//     row_stat    一 (t, hold) 完整行 = rac_row (给了 lag 行才算) + label_row (非段末尾部才算); 公式收尾走契约的 finish_row.
//                 label_row 一遍 A 轴: 整数五和 / 标签浮点和 / 分组和, branchless select
//     cost_row    一 (t, hold) 行的换手成本 (契约 cost_cell 逐格) → CostAcc. Search 打分 / Inspect 期限结构净线共用, 不进 Row
//   【标签】fp16 位 → float 用 _Float16 (clang, -march=native 有 F16C), 精确转换, 与 GPU __half2float 逐位一致.
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Stat/Contract.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <vector>

namespace factor::cpu::stat {

using factor::stat::Cost;
using factor::stat::CostAcc;
using factor::stat::Frame;
using factor::stat::kGroups;
using factor::stat::kMaxA;
using factor::stat::kNoKey;
using factor::stat::kRankNone;
using factor::stat::Label;
using factor::stat::Row;
using factor::stat::Side;

inline float h2f(uint16_t b) {
  _Float16 h;
  std::memcpy(&h, &b, sizeof(h));
  return static_cast<float>(h);
}

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

// 一行的精确并列均秩: key(a) 给保序键或 kNoKey; out[a] = r16 / kRankNone
template <class Key>
inline void rank_row(const Key &key, int A, uint16_t *out, Scratch &sc) {
  assert(sc.a.size() >= static_cast<size_t>(A));
  int n = 0;
  for (int a = 0; a < A; ++a) {
    const unsigned k = key(a);
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

// 因子一行 (口径 f, 只看池内 g ∧ xm 的格; 契约【截面池 g】) → r16
inline void rank_x_row(const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int A, uint16_t *out, Scratch &sc) {
  if (f == Frame::CS) {
    rank_row([&](int a) { return (xm[a] & g[a]) ? factor::stat::ord(xv[a]) : kNoKey; }, A, out, sc);
    return;
  }
  for (int a = 0; a < A; ++a)
    out[a] = (xm[a] & g[a]) ? factor::stat::r16_quant(xv[a]) : kRankNone;
}

// 做多标签一行 (fp16 位 + 有效位) → r16
inline void rank_y_row(const uint16_t *lv, const uint8_t *m, int A, uint16_t *out, Scratch &sc) {
  rank_row([&](int a) { return m[a] ? factor::stat::ord(h2f(lv[a])) : kNoKey; }, A, out, sc);
}

// rAC: rx = 本行 r16, rlag = t−h 行 r16 → r.rac / r.ok_rac
inline void rac_row(const uint16_t *rx, const uint16_t *rlag, int A, Row &r) {
  using ull = unsigned long long;
  ull n = 0, sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
  for (int a = 0; a < A; ++a) {
    const ull x = rx[a], y = rlag[a];
    const bool ok = rx[a] != kRankNone && rlag[a] != kRankNone;
    n += ok;
    sx += ok ? x : 0;
    sy += ok ? y : 0;
    sxx += ok ? x * x : 0;
    syy += ok ? y * y : 0;
    sxy += ok ? x * y : 0;
  }
  bool ok = false;
  r.rac = factor::stat::pearson_int(n, sx, sy, sxx, syy, sxy, ok);
  r.ok_rac = ok ? 1u : 0u;
}

// 换手成本 (契约【换手成本】): rx = 本行 r16, rlag = t−h 行 r16 (必有), c 已偏到行首 → 攒进 acc (顶 / 底组计数, 新进者累建仓冲击)
inline void cost_row(const uint16_t *rx, const uint16_t *rlag, const Cost &c, int A, CostAcc &acc) {
  for (int a = 0; a < A; ++a) {
    bool ent = false;
    const int leg = factor::stat::cost_cell(rx[a], rlag[a], c.m[a] != 0, ent);
    if (leg > 0) {
      ++acc.n_hi;
      if (ent)
        ++acc.e_hi, acc.buy_hi += static_cast<double>(h2f(c.buy[a]));
    } else if (leg < 0) {
      ++acc.n_lo;
      if (ent)
        ++acc.e_lo, acc.sell_lo += static_cast<double>(h2f(c.sell[a]));
    }
  }
}

// 标签侧: rx = 本行 r16, lb 已偏到行首 → r 的 ric / mkt / grp / cnt / ls / ok. 调用方已排除段末尾部
inline void label_row(Frame f, const uint16_t *rx, const Label &lb, int A, Row &r) {
  using ull = unsigned long long;
  ull n = 0, sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
  double smk = 0.0, ssv = 0.0, gs[kGroups] = {};
  int gc[kGroups] = {};
  double sv_bot = 0.0; // 底组的 Σ sv (lv 侧与计数就是 gs / gc)
  for (int a = 0; a < A; ++a) {
    const ull x = rx[a], y = lb.ry[a];
    const bool ok = rx[a] != kRankNone && lb.ry[a] != kRankNone;
    n += ok;
    sx += ok ? x : 0;
    sy += ok ? y : 0;
    sxx += ok ? x * x : 0;
    syy += ok ? y * y : 0;
    sxy += ok ? x * y : 0;
    const float yl = h2f(lb.lv[a]), ys = h2f(lb.sv[a]);
    smk += ok ? yl : 0.f;
    ssv += ok ? ys : 0.f;
    const int g = ok ? factor::stat::grp_of(rx[a]) : 0;
    gs[g] += ok ? yl : 0.f;
    gc[g] += ok;
    sv_bot += (ok && g == 0) ? ys : 0.f;
  }
  constexpr int kt = kGroups - 1;
  const Side top{static_cast<double>(gc[kt]), gs[kt], 0.0}, bot{static_cast<double>(gc[0]), gs[0], sv_bot};
  factor::stat::finish_row(f, n, sx, sy, sxx, syy, sxy, smk, ssv, gs, gc, top, bot, r);
}

// 一 (t, hold) 完整行: rlag = t−h 行 r16 (没有 → nullptr, 无 rAC); lb 已偏到行首; tail = 段末尾部 (tail_masked)
inline Row row_stat(Frame f, const uint16_t *rx, const uint16_t *rlag, const Label &lb, int A, bool tail) {
  Row r;
  if (rlag)
    rac_row(rx, rlag, A, r);
  if (!tail)
    label_row(f, rx, lb, A, r);
  return r;
}

} // namespace factor::cpu::stat
