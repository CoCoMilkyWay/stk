#pragma once

// =============================================================================
// Search 评估算子对拍的公共件: 候选造数 / 参考 (Stat CPU) / 比较 / CPU 驱动 / 分块切法
// =============================================================================
//   口径: Search 的语义 = Stat (契约 Search/Contract.hpp), 所以参考不是另一份 Search 实现, 而是 **Stat CPU** 对同一张平面算出的 HoldStat
//   (Stat 自身已 cpu ↔ gpu 对拍过) + 朴素逐行算的每持有期换手成本 (整张量 rank 行, t ≥ h 逐格比 t−h 组别). Search CPU / Search GPU 各自与它比
//   (二级 compare_stat 容差 + 成本: 四个计数逐位, 两个冲击均值容差), 再比 分块 (两块带 carry, merge) ≡ 整块.
//   造数沿用 Stat/Check.hpp 的 make (x / g / 标签), 候选 = 从 x 派生的 kCands 张平面 (CS {x, −x, x²}; TS {x, 1−x, x²}: 都在 [0,1]),
//   成本 = POS 造数 × 1e-3 (买) / × 1.5e-3 (卖) 经 fp16, 掩码取造数的空洞.
//   本头只依赖 Contract / Check / Exec / Stat/{Contract,Check,Cpu} / Search/{Contract,Cpu}; 消费者 TU 编进 -fno-fast-math.
// =============================================================================

#include "factor/Check.hpp"
#include "factor/Exec.hpp"
#include "factor/Search/Contract.hpp"
#include "factor/Search/Cpu.hpp"
#include "factor/Stat/Check.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp"

#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>
#include <vector>

namespace factor::search::check {

using factor::check::Diff;
using factor::check::Gen;
using factor::check::Plane;
using factor::check::Profile;
using factor::check::Tol;
using factor::stat::Frame;
using factor::stat::HoldStat;

inline constexpr int kCands = 3;

struct Data {
  factor::stat::check::Data base;  // x / g / 标签 (Stat 造数同口径; base.x 即候选 0)
  Plane cand[kCands];              // 候选平面
  std::vector<uint16_t> buy, sell; // 成本 fp16 位
  std::vector<uint8_t> cm;         // 成本有效位
};

inline void make(Data &d, Frame f, Profile pr, int T, int A, std::mt19937 &rng) {
  factor::stat::check::make(d.base, f, pr, T, A, rng);
  const size_t n = static_cast<size_t>(T) * A;
  d.cand[0] = d.base.x;
  d.cand[1] = d.base.x;
  d.cand[2] = d.base.x;
  for (size_t i = 0; i < n; ++i) {
    const float x = d.base.x.v[i];
    const bool m = d.base.x.m[i] != 0;
    d.cand[1].v[i] = m ? (f == Frame::CS ? -x : 1.f - x) : 0.f;
    d.cand[2].v[i] = m ? x * x : 0.f;
  }
  Plane c;
  factor::check::fill(c, Gen::POS, pr, T, A, rng);
  d.buy.resize(n), d.sell.resize(n), d.cm.resize(n);
  for (size_t i = 0; i < n; ++i) {
    d.buy[i] = factor::stat::check::f2h(c.v[i] * 1e-3f);
    d.sell[i] = factor::stat::check::f2h(c.v[i] * 1.5e-3f);
    d.cm[i] = c.m[i];
  }
}

// 分块切法: 块 2 从 s 起, 统计从 t_lo 起 (= 块 1 的终点 T1); carry = min(max_lag, T1)
struct Split {
  int T1 = 0, s = 0, t_lo = 0;
};
inline Split split_of(int T, const factor::stat::Holds &hd) {
  Split sp;
  sp.T1 = T / 2 < 1 ? 1 : T / 2;
  const int carry = max_lag(hd) < sp.T1 ? max_lag(hd) : sp.T1;
  sp.s = sp.T1 - carry;
  sp.t_lo = carry;
  return sp;
}

// ---- 参考: 候选 c 走 Stat CPU (HoldStat[H]) + 每持有期换手成本朴素逐行 (整张量 rank_x_rows, t ≥ h 逐格比 t−h 组别) ----
struct Ref {
  std::vector<HoldStat> stat; // [H]
  std::vector<CostAcc> cost;  // [H]
};
inline void reference(const Data &d, int c, int threads, Ref &r) {
  factor::stat::check::Data b = d.base;
  b.x = d.cand[c];
  factor::stat::check::Result res;
  factor::stat::check::run_cpu(b, res, threads);
  r.stat = res.stat;
  const int T = d.base.T, A = d.base.A, H = d.base.hd.n;
  const size_t n = static_cast<size_t>(T) * A;
  std::vector<uint16_t> ws(n);
  factor::cpu::stat::Scratch sc;
  sc.reserve(A);
  factor::cpu::stat::rank_x_rows(b.x.v.data(), b.x.m.data(), d.base.g.m.data(), d.base.frame, 0, T, A, ws.data(), sc);
  constexpr int kt = factor::stat::kGroups - 1;
  r.cost.assign(static_cast<size_t>(H), CostAcc{});
  for (int i = 0; i < H; ++i) {
    const int h = factor::stat::hold_minutes(d.base.hd.h[i]);
    CostAcc &ca = r.cost[static_cast<size_t>(i)];
    for (int t = h; t < T; ++t)
      for (int a = 0; a < A; ++a) {
        const size_t p = static_cast<size_t>(t) * A + a, q = p - static_cast<size_t>(h) * A;
        if (ws[p] == factor::stat::kRankNone || !d.cm[p])
          continue;
        const int g = factor::stat::grp_of(ws[p]), gl = ws[q] != factor::stat::kRankNone ? factor::stat::grp_of(ws[q]) : -1;
        if (g == kt) {
          ++ca.n_hi;
          if (gl != kt)
            ++ca.e_hi, ca.buy_hi += static_cast<double>(factor::cpu::stat::h2f(d.buy[p]));
        } else if (g == 0) {
          ++ca.n_lo;
          if (gl != 0)
            ++ca.e_lo, ca.sell_lo += static_cast<double>(factor::cpu::stat::h2f(d.sell[p]));
        }
      }
  }
}
// Search 的 Result → 同形 Ref (finish 各持有期), 给 分块 ≡ 整块 用
inline void to_ref(const Result &res, const factor::stat::Holds &hd, Ref &r) {
  r.stat.resize(static_cast<size_t>(hd.n));
  r.cost.resize(static_cast<size_t>(hd.n));
  for (int i = 0; i < hd.n; ++i) {
    r.stat[static_cast<size_t>(i)] = res.acc[i].finish(hd.h[i]);
    r.cost[static_cast<size_t>(i)] = res.cost[i];
  }
}

// ---- 比较: 二级逐字段 (compare_stat) + 每持有期成本 (四个计数逐位, 两个新进者冲击均值容差; 位置 hd.n + i) ----
inline Diff compare(const Ref &ref, const Result &got, const factor::stat::Holds &hd, Tol tol) {
  Ref g;
  to_ref(got, hd, g);
  Diff d = factor::stat::check::compare_stat(ref.stat.data(), g.stat.data(), hd.n, tol);
  for (int i = 0; i < hd.n; ++i) {
    const CostAcc &a = ref.cost[static_cast<size_t>(i)], &b = g.cost[static_cast<size_t>(i)];
    if (a.n_hi != b.n_hi || a.n_lo != b.n_lo || a.e_hi != b.e_hi || a.e_lo != b.e_lo) {
      if (d.mask_bad++ == 0)
        d.worst_at = hd.n + i;
      return d;
    }
    factor::stat::check::detail::cmp1(d, a.buy_mean(), b.buy_mean(), tol, hd.n + i);
    factor::stat::check::detail::cmp1(d, a.sell_mean(), b.sell_mean(), tol, hd.n + i);
  }
  return d;
}

// ---- CPU 驱动: 块 = 行 [row0, row0 + rows), 统计从 t_lo 起 (t0_abs = row0) → out[kCands] ----
//   ry 整张量一次预处理 (按行独立, 块取子段指针即可); 候选内 ForkJoin(threads) 切行.
//   prep_ms / eval_ms 非空 → 回填 预处理 (标签 rank) / 评估 (kCands 张候选) 的 wall 耗时 (与 Stat/Check.hpp run_cpu, gpu::run_search 同口径)
inline void run_cpu(const Data &d, int threads, int row0, int rows, int t_lo, Result *out, double *prep_ms = nullptr,
                    double *eval_ms = nullptr) {
  using Clock = std::chrono::steady_clock;
  const int T = d.base.T, A = d.base.A, H = d.base.hd.n;
  const size_t n = static_cast<size_t>(T) * A;
  assert(row0 >= 0 && rows >= 1 && row0 + rows <= T);
  factor::exec::ForkJoin ex(threads);
  factor::cpu::search::Workspace w;
  w.ensure(T, A, H, ex.threads()); // 整张量形状: ry 预处理用; 块评估前再 ensure 成块形状
  std::vector<std::vector<uint16_t>> ry(static_cast<size_t>(H), std::vector<uint16_t>(n));
  std::vector<factor::stat::Label> lab(static_cast<size_t>(H));
  const size_t off = static_cast<size_t>(row0) * A;
  Clock::time_point t0 = Clock::now();
  for (int i = 0; i < H; ++i) {
    const factor::stat::check::LabelHost &L = d.base.lab[static_cast<size_t>(i)];
    factor::cpu::stat::prep_label(ex, L.lv.data(), L.m.data(), T, A, ry[static_cast<size_t>(i)].data(), w.st);
    lab[static_cast<size_t>(i)] = {L.lv.data() + off, L.sv.data() + off, L.m.data() + off, ry[static_cast<size_t>(i)].data() + off};
  }
  if (prep_ms)
    *prep_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
  const Cost cost{d.buy.data() + off, d.sell.data() + off, d.cm.data() + off};
  const float *xv[kCands];
  const uint8_t *xm[kCands];
  for (int c = 0; c < kCands; ++c)
    xv[c] = d.cand[c].v.data() + off, xm[c] = d.cand[c].m.data() + off;
  w.ensure(rows, A, H, ex.threads());
  t0 = Clock::now();
  factor::cpu::search::run(ex, kCands, xv, xm, d.base.g.m.data() + off, d.base.frame, rows, A, row0, t_lo, d.base.hd, lab.data(), cost, w, out);
  if (eval_ms)
    *eval_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

} // namespace factor::search::check
