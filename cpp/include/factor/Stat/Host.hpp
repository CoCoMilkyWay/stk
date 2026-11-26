#pragma once

// =============================================================================
// Stat 行核的宿主版 (CPU; 契约 factor/Stat/Contract.hpp): 一行的 rank / 一 (t, hold) 的 Row / 一行的换手成本
// =============================================================================
//   Stat/Cpu.hpp 用, 与 Stat/Dev.cuh 对仗 (同一组行核同名同参序, 一边宿主一边设备). 单线程, 无流程 / 无分配.
//     KeyX / KeyY 取键器 (行视图, 与 Dev.cuh 同名同形): 因子一行 (只看池内 g ∧ m 的格) / 做多收益一行 (fp16 位) → 保序键 / kNoKey
//     rank_row    精确并列均秩: key(a) → LSD 基数排序 (n ≤ 1024 用 8 位 × 4 趟, 否则 11 位 × 3 趟) → 等值段均秩 → out
//     rank_x_row  因子一行: CS rank_row(KeyX) / TS 直接量化 —— 口径分叉只在这里; 方向在此一次定 (契约【方向 Dir】)
//     row_stat    一 (t, hold) 完整行 = 换手侧 lag_row (t−h → t) + 标签侧 label_row (t → t+h, 非段末尾部才算):
//                 整数五和 / 基准浮点和 / 分组和 / 尾部和一遍 A 轴 (契约【仓位感知】【尾部】), 收尾 finish_row
//     cost_row    一行 t 的换手成本 (前一批随价漂移到交接点, 契约【换手成本】), 每持有期两遍 A 轴 × C ≥ 1 个金额段 → CostAcc[C][H]
//   【标签】fp16 位 → float 用 _Float16 (clang, F16C), 精确转换, 与 GPU __half2float 逐位一致.
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
using factor::stat::CostBook;
using factor::stat::Frame;
using factor::stat::kEcMid;
using factor::stat::kEnds;
using factor::stat::kGroups;
using factor::stat::kMaxA;
using factor::stat::kNoKey;
using factor::stat::kRankNone;
using factor::stat::kSides;
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

// 标签侧 (t → t+h): rx = 本行 r16 (已定向), lb 已偏到行首 (交接链回看 / 前看同块其他行), c = 本行交接链, cap = 满仓
//   → r 的 ric / mkt / grp / cnt / tail / ok. ric / mkt 在 J 上 (与可成交无关); 组与尾部只收真持有的格, 收益按续持取名义值. 调用方已排除段末尾部
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
    const factor::stat::Cell<uint16_t> cl{c, rx + a, lb.m + a, lb.eo + a, y};
    const auto in = [g](uint16_t x) { return x != kRankNone && factor::stat::grp_of(x) == g; };
    if (cl.held(in)) {
      gs[g] += cl.ret(cl.continues(in));
      ++gc[g];
    }
    for (int e = 0; e < kEnds; ++e) {
      if (g != factor::stat::end_group(e))
        continue;
      for (int j = 0; j < kTails; ++j) {
        if (!factor::stat::in_tail(e, rx[a], j))
          continue;
        const auto it = [e, j](uint16_t x) { return x != kRankNone && factor::stat::in_tail(e, x, j); };
        if (cl.held(it)) {
          ts[e][j] += cl.ret(cl.continues(it));
          ++tc[e][j];
        }
      }
    }
  }
  factor::stat::finish_row(s, sm, gs, gc, ts, tc, cap, r);
}

// 一 (t, hold) 完整行: rlag = t−h 行 r16 (没有 → nullptr, 无 rAC); lb 已偏到行首; c = 本行交接链; tail = 段末尾部 (tail_masked)
inline Row row_stat(const uint16_t *rx, const uint16_t *rlag, const Label &lb, const factor::stat::Chain &c, double cap, bool tail) {
  Row r;
  if (rlag)
    lag_row(rx, rlag, c.A, r);
  if (!tail)
    label_row(rx, lb, c, cap, r);
  return r;
}

// 一行一个持有期的换手输入 (契约【换手成本】; 指针已偏到行首): lb = 本行标签 (lb.m = nullptr → 本行尾部不开仓, 不攒);
// c = 本行的交接链 (c.nets(c.t) = 与行 t−h 的批交接, 只交易差额; 否则本批整建); settle = 本批平仓不与下一批交接 (建仓时一并记平仓量)
struct CostHold {
  Label lb;
  factor::stat::Chain c;
  bool settle = false;
};

// 换手成本: ec = 本行端码 (块形平面偏到行首, 交接链回看前行); ch[i] = 持有期 i 的输入; c[k] 已偏到行首; cap = 一端满仓 → acc[k·H + i].
//   端成员 = 端码 ∧ m ∧ 真持有 (与 Row 的端组同一集合); 两批各端只数 → 目标权 1 / max(只数, cap);
//   前一批的权随价漂移到交接点: w̃ = w (1 + r) / V, r = 名义收益 (续持口径), V = 1 + Σ w r; 逐格 Δ = 本批权 − w̃:
//   Δ⁺ 增仓 (本格不可买 → 加不了仓, 记 0), Δ⁻ 减仓; 不交接的批建仓时一并记整批平仓 w (1 + lv) / V (settle).
//   每侧量 × 本格该侧冲击 (盘口段) 或记进固定量 (固定段); 税佣在 finish 端按 Fee 乘量
inline void cost_row(const uint8_t *ec, const CostHold *ch, int H, const Cost *c, int C, int A, double cap, CostAcc *acc) {
  assert(C >= 1 && C <= factor::stat::kAmtCount);
  for (int i = 0; i < H; ++i) {
    const CostHold &q = ch[i];
    if (!q.lb.m)
      continue;
    const bool net = q.c.nets(q.c.t);
    factor::stat::Chain pc = q.c; // 前一批 (行 t − h) 的链
    pc.t -= q.c.h;
    const ptrdiff_t back = -static_cast<ptrdiff_t>(q.c.h) * A;
    // 偏 o 行的格 a 在链 cn 上的端 (−1 = 不是真持有的端成员)
    const auto end_at = [&](const factor::stat::Chain &cn, ptrdiff_t o, int a) -> int8_t {
      const ptrdiff_t j = o + a;
      const uint8_t x = ec[j];
      if (x >= kEcMid || !q.lb.m[j])
        return -1;
      return factor::stat::held(cn, ec + j, q.lb.m + j, q.lb.eo + j, [x](uint8_t v) { return v == x; }) ? static_cast<int8_t>(x) : -1;
    };
    static thread_local int8_t en[kMaxA], pen[kMaxA];
    static thread_local float rn[kMaxA], pr[kMaxA]; // 本批平仓收益 (settle) / 前一批到交接点的名义收益
    int n[kEnds] = {}, pn[kEnds] = {};
    for (int a = 0; a < A; ++a) {
      en[a] = end_at(q.c, 0, a);
      pen[a] = net ? end_at(pc, back, a) : -1;
      if (en[a] >= 0) {
        ++n[en[a]];
        rn[a] = h2f(q.lb.lv[a]);
      }
      if (pen[a] >= 0) {
        ++pn[pen[a]];
        float y[2];
        factor::stat::label_cell(q.lb, back + a, H2F{}, y);
        pr[a] = factor::stat::hold_ret(true, q.lb.eo[a], y);
      }
    }
    double w[kEnds], pw[kEnds], v[kEnds], pv[kEnds];
    for (int e = 0; e < kEnds; ++e) {
      w[e] = 1.0 / factor::stat::book_den(n[e], cap);
      pw[e] = 1.0 / factor::stat::book_den(pn[e], cap);
      v[e] = pv[e] = 1.0;
    }
    for (int a = 0; a < A; ++a) {
      if (q.settle && en[a] >= 0)
        v[en[a]] += w[en[a]] * rn[a];
      if (pen[a] >= 0)
        pv[pen[a]] += pw[pen[a]] * pr[a];
    }
    double imp[factor::stat::kAmtCount][kEnds][kSides] = {}, vol[kEnds][kSides] = {};
    for (int a = 0; a < A; ++a) {
      const int x = en[a], px = pen[a];
      if (x < 0 && px < 0)
        continue;
      for (int e = 0; e < kEnds; ++e) {
        double d = (x == e ? w[e] : 0.0) - (px == e ? pw[e] * (1.0 + pr[a]) / pv[e] : 0.0);
        if (d > 0.0 && !(q.lb.eo[a] & factor::stat::kEoBuy))
          d = 0.0;
        // 本格两侧成交量: 买 = Δ⁺; 卖 = Δ⁻ + 本批整批平仓 (不交接时建仓即记, 随价漂移)
        const double u[kSides] = {d > 0.0 ? d : 0.0, (d < 0.0 ? -d : 0.0) + (q.settle && x == e ? w[e] * (1.0 + rn[a]) / v[e] : 0.0)};
        for (int s = 0; s < kSides; ++s) {
          if (u[s] == 0.0)
            continue;
          vol[e][s] += u[s];
          for (int k = 0; k < C; ++k)
            if (!c[k].flat())
              imp[k][e][s] += u[s] * static_cast<double>(std::min(h2f(c[k].imp[s][a]), c[k].top));
        }
      }
    }
    for (int k = 0; k < C; ++k) {
      CostAcc &o = acc[k * H + i];
      ++o.rows;
      for (int e = 0; e < kEnds; ++e) {
        CostBook &b = o.book[e];
        for (int s = 0; s < kSides; ++s) {
          b.vol[s] += vol[e][s];
          b.imp[s] += imp[k][e][s];
          b.fix[s] += c[k].flat() ? vol[e][s] : 0.0;
        }
        b.w += n[e] * w[e], b.n += n[e];
      }
    }
  }
}

} // namespace factor::cpu::stat
