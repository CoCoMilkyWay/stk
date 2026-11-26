#pragma once

// =============================================================================
// Stat 行核的宿主版 (CPU; 契约 factor/Stat/Contract.hpp): 一行的 rank / 一 (t, hold) 的 Row / 一行一持有期的换手
// =============================================================================
//   Stat/Cpu.hpp 用, 与 Stat/Dev.cuh 对仗 (同一组行核同名同参序, 一边宿主一边设备). 单线程, 无流程 / 无分配.
//     KeyX / KeyY 取键器 (行视图, 与 Dev.cuh 同名同形): 因子一行 (只看池内 g ∧ m 的格) / 做多收益一行 (fp16 位) → 保序键 / kNoKey
//     rank_row    精确并列均秩: key(a) → LSD 基数排序 (n ≤ 1024 用 8 位 × 4 趟, 否则 11 位 × 3 趟) → 等值段均秩 → out
//     rank_x_row  因子一行: CS rank_row(KeyX) / TS 直接量化 —— 口径分叉只在这里; 方向在此一次定 (契约【方向 Dir】)
//     row_stat    一 (t, hold) 完整行 = 换手侧 lag_row (t−h → t) + 标签侧 label_row (t → t+h, 开仓行才算):
//                 整数五和 / 基准浮点和 / 分组和 / 尾部和一遍 A 轴 (仓位感知 Pos.hpp: 每格一次链回走同出组与各级尾部), 收尾 finish_row
//     turn_row    一行一持有期的换手 (契约 Turn.hpp): 本批 ⇄ 前一批两列格位 → 每本每金额段两侧流水 + 本批收益
//     factor_slots 因子账集 (端 × 层级) 一行的两列格位 (InRank 一次回走)
//   【标签】fp16 位 → float 用 _Float16 (clang, F16C), 精确转换, 与 GPU __half2float 逐位一致.
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Pos.hpp"
#include "factor/Stat/Turn.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

namespace factor::cpu::stat {

using factor::stat::Cost;
using factor::stat::Frame;
using factor::stat::kEnds;
using factor::stat::kGroups;
using factor::stat::kLevels;
using factor::stat::kMaxA;
using factor::stat::kNoKey;
using factor::stat::kRankNone;
using factor::stat::kSides;
using factor::stat::kSums;
using factor::stat::kTails;
using factor::stat::Label;
using factor::stat::Row;
using factor::stat::Slot;
using factor::stat::Turn;

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

// 标签侧 (t → t+h): rx = 本行 r16 (已定向), lb 已偏到行首 (交接链回看 / 前看同块其他行), c = 本行交接链, cap = 满仓
//   → r 的 ric / mkt / grp / cnt / tail / ok. ric / mkt 在 J 上 (与可成交无关); 组与尾部只收真持有的格 (一次 walk 出层级), 收益按续持取名义值
inline void label_row(const uint16_t *rx, const Label &lb, const factor::stat::Chain &c, double cap, Row &r) {
  unsigned long long s[kSums] = {};
  double sm = 0.0, gs[kGroups] = {}, ts[kEnds][kTails] = {};
  int gc[kGroups] = {}, tc[kEnds][kTails] = {};
  for (int a = 0; a < c.A; ++a) {
    const bool ok = rx[a] != kRankNone && lb.ry[a] != kRankNone;
    factor::stat::pair_add(s, rx[a], lb.ry[a], ok);
    float y[2];
    factor::stat::label_cell(lb, a, H2F{}, y);
    sm += ok ? y[0] : 0.f;
    if (!ok)
      continue;
    const int g = factor::stat::grp_of(rx[a]);
    const factor::stat::Pos p = factor::stat::walk(c, lb.m + a, lb.eo + a, factor::stat::InRank{rx + a, g});
    if (p.held < 0)
      continue;
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
  factor::stat::finish_row(s, sm, gs, gc, ts, tc, cap, r);
}

// 一 (t, hold) 完整行: rlag = t−h 行 r16 (没有 → nullptr, 无 rAC); lb 已偏到行首; c = 本行交接链; opens = 开仓行 (Hold::opens)
inline Row row_stat(const uint16_t *rx, const uint16_t *rlag, const Label &lb, const factor::stat::Chain &c, double cap, bool opens) {
  Row r;
  if (rlag)
    lag_row(rx, rlag, c.A, r);
  if (opens)
    label_row(rx, lb, c, cap, r);
  return r;
}

// ---- 换手 (契约 Turn.hpp) ----

// 每线程的账集工作区 (本数 B 定形, 跨行复用)
struct TurnWork {
  std::vector<double> W, PW, V, PV; // [B] 本批 / 前一批的目标权和与资金 (1 + Σ w̄ r)
  std::vector<Slot> cur, pre;       // 一行的两列格位
};

// 一行一持有期: 两列格位 cur / pre (各按 (a, b) 升序; pre 只交接时非空) → ret[b] += 本批收益, out[k · B + b] += 两侧流水.
//   settle = 本批平仓不与下一批交接; eo / c[k] 已偏到行首; cap[b] = 本 b 满仓权
inline void turn_row(const std::vector<Slot> &cur, const std::vector<Slot> &pre, bool settle, const uint8_t *eo, const Cost *c, int C, std::span<const double> cap,
                     TurnWork &w, double *ret, Turn *out) {
  const size_t B = cap.size();
  w.W.assign(B, 0.0), w.PW.assign(B, 0.0), w.V.assign(B, 1.0), w.PV.assign(B, 1.0);
  for (const Slot &s : cur)
    w.W[s.b] += s.w;
  for (const Slot &s : pre)
    w.PW[s.b] += s.w;
  const auto wt = [&](const Slot &s, const std::vector<double> &W) { return s.w / factor::stat::book_den(W[s.b], cap[s.b]); };
  for (const Slot &s : cur) {
    const double x = wt(s, w.W) * s.r;
    w.V[s.b] += x, ret[s.b] += x;
  }
  for (const Slot &s : pre)
    w.PV[s.b] += wt(s, w.PW) * s.r;
  const auto key = [](const Slot &s) { return static_cast<uint64_t>(s.a) << 32 | s.b; };
  for (size_t i = 0, j = 0; i < cur.size() || j < pre.size();) {
    const bool has_c = i < cur.size() && (j == pre.size() || key(cur[i]) <= key(pre[j]));
    const bool has_p = j < pre.size() && (i == cur.size() || key(pre[j]) <= key(cur[i]));
    const Slot &s = has_c ? cur[i] : pre[j];
    const double x = has_c ? wt(s, w.W) : 0.0, px = has_p ? wt(pre[j], w.PW) * (1.0 + pre[j].r) / w.PV[s.b] : 0.0;
    double d = x - px;
    if (d > 0.0 && !(eo[s.a] & factor::stat::kEoBuy))
      d = 0.0;
    const double u[kSides] = {d > 0.0 ? d : 0.0, (d < 0.0 ? -d : 0.0) + (settle && has_c ? x * (1.0 + s.r) / w.V[s.b] : 0.0)};
    for (int sd = 0; sd < kSides; ++sd) {
      if (u[sd] == 0.0)
        continue;
      for (int k = 0; k < C; ++k) {
        Turn &o = out[static_cast<size_t>(k) * B + s.b];
        o.vol[sd] += u[sd];
        if (c[k].flat())
          o.fix[sd] += u[sd];
        else
          o.imp[sd] += u[sd] * static_cast<double>(std::min(h2f(c[k].imp[sd][s.a]), c[k].top));
      }
    }
    i += has_c, j += has_p;
  }
}

// 因子账集 (b = e · kLevels + l) 一行的两列格位: rx (已定向) / lb 已偏到行首; c = 本行交接链; net = 与前一批交接
//   本批: 端组真持有的格进其各级 (l ≤ held), r = 该级续持口径收益; 前一批 (net): 行 t − h 端组真持有的格, r = 到交接点的名义收益
inline void factor_slots(const uint16_t *rx, const Label &lb, const factor::stat::Chain &c, bool net, TurnWork &w) {
  w.cur.clear(), w.pre.clear();
  const ptrdiff_t back = -static_cast<ptrdiff_t>(c.hd.h) * c.A;
  const factor::stat::Chain pc = c.back();
  for (int a = 0; a < c.A; ++a) {
    for (int e = 0; e < kEnds; ++e) {
      const int g = factor::stat::end_group(e);
      float y[2];
      if (rx[a] != kRankNone && factor::stat::grp_of(rx[a]) == g && lb.m[a]) {
        const factor::stat::Pos p = factor::stat::walk(c, lb.m + a, lb.eo + a, factor::stat::InRank{rx + a, g});
        factor::stat::label_cell(lb, a, H2F{}, y);
        for (int l = 0; l <= p.held; ++l)
          w.cur.push_back({static_cast<uint32_t>(a), static_cast<uint32_t>(e * kLevels + l), 1.f, factor::stat::ret_at(p, l, y)});
      }
      const ptrdiff_t q = back + a;
      if (net && rx[q] != kRankNone && factor::stat::grp_of(rx[q]) == g && lb.m[q]) {
        const factor::stat::Pos p = factor::stat::walk(pc, lb.m + q, lb.eo + q, factor::stat::InRank{rx + q, g});
        factor::stat::label_cell(lb, q, H2F{}, y);
        const float r = factor::stat::hold_ret(true, lb.eo[a], y);
        for (int l = 0; l <= p.held; ++l)
          w.pre.push_back({static_cast<uint32_t>(a), static_cast<uint32_t>(e * kLevels + l), 1.f, r});
      }
    }
  }
}

} // namespace factor::cpu::stat
