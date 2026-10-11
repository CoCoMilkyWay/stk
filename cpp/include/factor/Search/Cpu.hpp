#pragma once

// =============================================================================
// Search 评估算子的 CPU 后端 (factor::cpu::search; 契约 factor/Search/Contract.hpp + Stat/Contract.hpp)
// =============================================================================
//   定位: 无 GPU 时的参数搜索路径. 独立实现, 不 include Stat/Cpu.hpp / Gpu.cuh; 与 Stat CPU / Search GPU 对拍 (Search/Check.hpp).
//
//   【并行】候选级: 一个候选 = 一个线程串行跑完 (eval_one), 候选间由 Exec::tasks 抢单 (run); 算子内不起线程, 每线程一个 Worker.
//   【行序流水】候选平面逐行 t 升序走一遍 (prep_row), 每行一次性产出"按秩排好的资产序" perm[n] + 每位置的 r16 xr[n] + 组边界 gb[K+1]
//     (CS: 基数排序后 r16 单调 ⇒ 组在 perm 上连续; TS: 按组计数排序), 再按 perm 做 H 个持有期:
//       组和 = 连续区间顺序累加, 组累加器全在寄存器, 没有 20 路散射 (Stat 的 gs[g] += 是它的热点);
//       IC / 超额 / 顶底 / rank-AC 全是 perm 上的 gather (ry / lv / sv / lag 行一行 ≤ 10 KB, 在 L1);
//       成本只走顶组 / 底组两个区间 (A 的 1/10).
//     rank-AC 的 lag 行取自环形缓冲 ring[(t − h) mod R] (R = max_lag + 1 行 r16 按资产下标), 整块不留 [T][A] 的 r16.
//     每行收尾 finish_row → Row (栈上) → Accum::add: 公式与 Stat 同一份 (Stat/Contract.hpp).
//   【与 Stat CPU 的差别】整数派生量 (ic / ac / cnt) 逐位一致; 浮点和 (grp / ls / mkt) 只差求和序 (perm 序 vs 资产序) → 对拍容差.
//   【标签】ry (做多标签逐行 r16) 块内一次 prep_label, 候选共用; fp16 → float 用 _Float16 (精确).
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Search/Contract.hpp"
#include "factor/Stat/Contract.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <vector>

namespace factor::cpu::search {

using factor::search::CostAcc;
using factor::search::Result;
using factor::stat::Frame;
using factor::stat::Holds;
using factor::stat::kGroups;
using factor::stat::kMaxA;
using factor::stat::kMaxHold;
using factor::stat::kNoKey;
using factor::stat::kRankNone;
using factor::stat::Row;

// 一组标签 (宿主指针, 全部 [T][A], 块形); ry 由 prep_label 填
struct Label {
  const uint16_t *lv = nullptr;
  const uint16_t *sv = nullptr;
  const uint8_t *m = nullptr;
  const uint16_t *ry = nullptr;
};
// 单一金额档的建仓冲击 (fp16 位 + 有效位), [T][A]
struct Cost {
  const uint16_t *buy = nullptr;
  const uint16_t *sell = nullptr;
  const uint8_t *m = nullptr;
};

inline constexpr int kRadixMaxBits = 11;

// 一个线程的全部暂存: 排序缓冲 + 行序 (perm / xr / gb) + lag 环. ensure 幂等 → 稳态零分配
struct Worker {
  std::vector<uint64_t> sa, sb; // (key << 16) | a
  int cnt[1 << kRadixMaxBits];
  std::vector<uint16_t> perm, xr; // 当前行: 按秩排好的资产下标 / 对应 r16 (前 n 个有效)
  int gb[kGroups + 1] = {};       // 当前行: 组 k = perm[gb[k], gb[k+1])
  int n = 0;                      // 当前行有效数 (CS 整行无效 → 0)
  std::vector<uint16_t> ring;     // [R][A] r16 按资产下标 (lag 行)
  int A = 0, R = 0;
  void ensure(int A_, int R_) {
    assert(A_ >= 1 && A_ <= kMaxA && R_ >= 1);
    if (A != A_) {
      sa.resize(static_cast<size_t>(A_)), sb.resize(static_cast<size_t>(A_));
      perm.resize(static_cast<size_t>(A_)), xr.resize(static_cast<size_t>(A_));
    }
    if (A != A_ || R != R_)
      ring.resize(static_cast<size_t>(R_) * A_);
    A = A_, R = R_;
  }
  uint16_t *row(int t) { return ring.data() + static_cast<size_t>(t % R) * A; }
};
// 每线程一个 Worker: R = max_lag + 1 行
inline void ensure_workers(std::vector<Worker> &ws, int threads, int A, const Holds &hd) {
  assert(threads >= 1);
  if (ws.size() < static_cast<size_t>(threads))
    ws.resize(static_cast<size_t>(threads));
  for (Worker &w : ws)
    w.ensure(A, factor::search::max_lag(hd) + 1);
}

namespace detail {

inline float h2f(uint16_t b) {
  _Float16 h;
  std::memcpy(&h, &b, sizeof(h));
  return static_cast<float>(h);
}

// 精确并列均秩一行: key(a) 给保序键或 kNoKey → out[a] = r16 / kRankNone, 同时留下 perm / xr / gb (n 个有效).
// n < 2 或全并列 → 整行 kRankNone, n = 0. LSD 基数排序 (位宽按 n 自适应: n ≤ 1024 用 8 位 × 4 趟, 否则 11 位 × 3 趟)
template <class Key>
inline void rank_row(Worker &w, const Key &key, int A, uint16_t *out) {
  assert(w.A == A);
  int n = 0;
  for (int a = 0; a < A; ++a) {
    const unsigned k = key(a);
    w.sa[static_cast<size_t>(n)] = (static_cast<uint64_t>(k) << 16) | static_cast<uint64_t>(a);
    n += k != kNoKey;
  }
  std::fill(out, out + A, kRankNone);
  std::fill(w.gb, w.gb + kGroups + 1, 0);
  w.n = 0;
  if (n < 2)
    return;
  const int bits = n <= 1024 ? 8 : kRadixMaxBits, passes = (32 + bits - 1) / bits, radix = 1 << bits, mask = radix - 1;
  uint64_t *src = w.sa.data(), *dst = w.sb.data();
  for (int p = 0; p < passes; ++p) {
    const int shift = 16 + bits * p;
    std::fill(w.cnt, w.cnt + radix, 0);
    for (int i = 0; i < n; ++i)
      ++w.cnt[(src[i] >> shift) & mask];
    int run = 0;
    for (int d = 0; d < radix; ++d) {
      const int c = w.cnt[d];
      w.cnt[d] = run;
      run += c;
    }
    for (int i = 0; i < n; ++i)
      dst[w.cnt[(src[i] >> shift) & mask]++] = src[i];
    std::swap(src, dst);
  }
  const auto key_at = [&](int i) { return static_cast<unsigned>(src[i] >> 16); };
  if (!(key_at(n - 1) > key_at(0))) // 全并列 = spread 为假
    return;
  w.n = n;
  int k = 0; // r16 沿 i 单调 ⇒ 组号单调: 组 k 的起点 = 首个 grp_of ≥ k 的位置
  for (int i = 0; i < n;) {
    int j = i + 1;
    while (j < n && key_at(j) == key_at(i))
      ++j;
    const uint16_t r = factor::stat::r16_of(i, j - i, n);
    const int gk = factor::stat::grp_of(r);
    while (k < gk)
      w.gb[++k] = i;
    for (int p = i; p < j; ++p) {
      const int a = static_cast<int>(src[p] & 0xFFFFu);
      out[a] = r;
      w.perm[static_cast<size_t>(p)] = static_cast<uint16_t>(a);
      w.xr[static_cast<size_t>(p)] = r;
    }
    i = j;
  }
  while (k < kGroups)
    w.gb[++k] = n;
}

// 一行的准备: 口径 f (只看池内 g ∧ xm) → rx[a] (r16 按资产, 进 ring) + perm / xr / gb
inline void prep_row(Worker &w, const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int A, uint16_t *rx) {
  if (f == Frame::CS) {
    rank_row(w, [&](int a) { return (xm[a] & g[a]) ? factor::stat::ord(xv[a]) : kNoKey; }, A, rx);
    return;
  }
  // TS: 逐格量化, 再按组计数排序 (稳定, 按资产序) → 组在 perm 上连续
  int cnt[kGroups] = {};
  for (int a = 0; a < A; ++a) {
    const bool on = (xm[a] & g[a]) != 0;
    const uint16_t r = on ? factor::stat::r16_quant(xv[a]) : kRankNone;
    rx[a] = r;
    cnt[on ? factor::stat::grp_of(r) : 0] += on;
  }
  int pos[kGroups];
  w.gb[0] = 0;
  for (int k = 0; k < kGroups; ++k) {
    pos[k] = w.gb[k];
    w.gb[k + 1] = w.gb[k] + cnt[k];
  }
  w.n = w.gb[kGroups];
  for (int a = 0; a < A; ++a) {
    const uint16_t r = rx[a];
    if (r == kRankNone)
      continue;
    const int p = pos[factor::stat::grp_of(r)]++;
    w.perm[static_cast<size_t>(p)] = static_cast<uint16_t>(a);
    w.xr[static_cast<size_t>(p)] = r;
  }
}

// rank-AC: 当前行 (perm / xr) × lag 行 rlag (按资产) → r.ac / r.ok_ac
inline void ac_sums(const Worker &w, const uint16_t *rlag, Row &r) {
  using ull = unsigned long long;
  ull n = 0, sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
  for (int p = 0; p < w.n; ++p) {
    const ull x = w.xr[static_cast<size_t>(p)], y = rlag[w.perm[static_cast<size_t>(p)]];
    const bool ok = y != kRankNone;
    n += ok;
    sx += ok ? x : 0;
    sy += ok ? y : 0;
    sxx += ok ? x * x : 0;
    syy += ok ? y * y : 0;
    sxy += ok ? x * y : 0;
  }
  bool ok = false;
  r.ac = factor::stat::pearson_int(n, sx, sy, sxx, syy, sxy, ok);
  r.ok_ac = ok ? 1u : 0u;
}

// 标签侧: 当前行 × 标签行 t → r 的 ic / mkt / grp / cnt / ls / ok (调用方已排除段末尾部). 组和 = perm 上连续区间
inline void label_sums(const Worker &w, Frame f, int t, int A, const Label &L, Row &r) {
  using ull = unsigned long long;
  const size_t base = static_cast<size_t>(t) * A;
  const uint16_t *ry = L.ry + base, *lv = L.lv + base, *sv = L.sv + base;
  ull n = 0, sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
  double smk = 0.0, ssv = 0.0, gs[kGroups], sv_bot = 0.0;
  int gc[kGroups];
  for (int k = 0; k < kGroups; ++k) {
    double gsk = 0.0;
    int gck = 0;
    const bool bot = k == 0;
    for (int p = w.gb[k]; p < w.gb[k + 1]; ++p) {
      const int a = w.perm[static_cast<size_t>(p)];
      const ull x = w.xr[static_cast<size_t>(p)], y = ry[a];
      const bool ok = y != kRankNone;
      n += ok;
      sx += ok ? x : 0;
      sy += ok ? y : 0;
      sxx += ok ? x * x : 0;
      syy += ok ? y * y : 0;
      sxy += ok ? x * y : 0;
      const float yl = h2f(lv[a]), ys = h2f(sv[a]);
      smk += ok ? yl : 0.f;
      ssv += ok ? ys : 0.f;
      gsk += ok ? yl : 0.f;
      gck += ok;
      sv_bot += (ok & bot) ? ys : 0.f;
    }
    gs[k] = gsk, gc[k] = gck;
  }
  using factor::stat::Side;
  constexpr int kt = kGroups - 1;
  const Side top{static_cast<double>(gc[kt]), gs[kt], 0.0}, bot{static_cast<double>(gc[0]), gs[0], sv_bot};
  factor::stat::finish_row(f, n, sx, sy, sxx, syy, sxy, smk, ssv, gs, gc, top, bot, r);
}

// 成本两格: 顶组买入 / 底组卖出 (rx 有效 ∧ 成本有效; 不看 ok / 尾部)
inline void cost_sums(const Worker &w, int t, int A, const Cost &c, CostAcc &acc) {
  const size_t base = static_cast<size_t>(t) * A;
  const uint8_t *m = c.m + base;
  const uint16_t *buy = c.buy + base, *sell = c.sell + base;
  for (int p = w.gb[kGroups - 1]; p < w.gb[kGroups]; ++p) {
    const int a = w.perm[static_cast<size_t>(p)];
    acc.buy_hi += m[a] ? static_cast<double>(h2f(buy[a])) : 0.0;
    acc.n_hi += m[a] != 0;
  }
  for (int p = w.gb[0]; p < w.gb[1]; ++p) {
    const int a = w.perm[static_cast<size_t>(p)];
    acc.sell_lo += m[a] ? static_cast<double>(h2f(sell[a])) : 0.0;
    acc.n_lo += m[a] != 0;
  }
}

} // namespace detail

// -----------------------------------------------------------------------------
// 流程
// -----------------------------------------------------------------------------

// 标签预处理: 做多标签 (fp16 位 + 有效位) 行 [0,T) → ry (r16). 块内一次, 候选共用 (Exec 切行, 每 tid 一个 Worker)
template <class Exec>
inline void prep_label(Exec &ex, const uint16_t *lv, const uint8_t *m, int T, int A, uint16_t *ry, std::vector<Worker> &ws) {
  assert(ws.size() >= static_cast<size_t>(ex.threads()) && T >= 1);
  ex.rows(T, [&](int t0, int t1, int tid) {
    Worker &w = ws[static_cast<size_t>(tid)];
    for (int t = t0; t < t1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      detail::rank_row(w, [&](int a) { return m[base + a] ? factor::stat::ord(detail::h2f(lv[base + a])) : kNoKey; }, A, ry + base);
    }
  });
}

// 一个候选的完整评估 (单线程): 块行 [0,T), 统计 [t_lo,T), 绝对首行 t0_abs (契约【分块流式】) → out (覆盖)
inline void eval_one(Worker &w, const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int T, int A, int t0_abs, int t_lo,
                     const Holds &hd, const Label *lab, const Cost &cost, Result &out) {
  factor::stat::assert_holds(hd);
  assert(xv && xm && g && lab && cost.buy && cost.sell && cost.m);
  assert(T >= 1 && w.A == A && w.R > factor::search::max_lag(hd) && "Worker 未 ensure 到位");
  assert(t0_abs >= 0 && t_lo >= 0 && t_lo < T);
  for (int i = 0; i < hd.n; ++i)
    assert(lab[i].lv && lab[i].sv && lab[i].m && lab[i].ry && "标签未预处理 (prep_label)");
  out = Result{};
  for (int t = 0; t < T; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    detail::prep_row(w, xv + base, xm + base, g + base, f, A, w.row(t));
    if (t < t_lo)
      continue;
    for (int i = 0; i < hd.n; ++i) {
      const int hold = hd.h[i], h = factor::stat::hold_minutes(hold);
      Row r;
      if (t >= h)
        detail::ac_sums(w, w.row(t - h), r);
      if (!factor::stat::tail_masked(t0_abs + t, hold))
        detail::label_sums(w, f, t, A, lab[i], r);
      out.acc[i].add(r);
    }
    detail::cost_sums(w, t, A, cost, out.cost);
  }
}

// 一批候选: xv[c] / xm[c] 各一张 [T][A], 候选间 Exec::tasks 抢单 → out[c]
template <class Exec>
inline void run(Exec &ex, int C, const float *const *xv, const uint8_t *const *xm, const uint8_t *g, Frame f, int T, int A, int t0_abs,
                int t_lo, const Holds &hd, const Label *lab, const Cost &cost, std::vector<Worker> &ws, Result *out) {
  assert(C >= 1 && out && ws.size() >= static_cast<size_t>(ex.threads()));
  ex.tasks(C, [&](int c, int tid) { eval_one(ws[static_cast<size_t>(tid)], xv[c], xm[c], g, f, T, A, t0_abs, t_lo, hd, lab, cost, out[c]); });
}

} // namespace factor::cpu::search
