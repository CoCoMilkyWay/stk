#pragma once

// =============================================================================
// Search 评估算子对拍的公共件: 候选造数 / 参考 (Stat CPU) / 比较 / CPU 驱动 / 分块切法
// =============================================================================
//   口径: Search 的语义 = Stat (契约 Search/Contract.hpp), 所以参考不是另一份 Search 实现, 而是 **Stat CPU** 对同一张平面算出的 HoldStat
//   (Stat 自身已 cpu ↔ gpu 对拍过). Search CPU / Search GPU 各自与它比 (二级 compare_stat 容差), 再比 分块 (两块带 carry, merge) ≡ 整块.
//   造数沿用 Stat/Check.hpp 的 make (x / g / 标签), 候选 = 从 x 派生的 kCands 张平面 (CS {x, −x, x²}; TS {x, 1−x, x²}: 都在 [0,1]).
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
  factor::stat::check::Data base; // x / g / 标签 (Stat 造数同口径; base.x 即候选 0)
  Plane cand[kCands];             // 候选平面
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

// ---- 参考: 候选 c 走 Stat CPU (HoldStat[H]) ----
struct Ref {
  std::vector<HoldStat> stat; // [H]
};
inline void reference(const Data &d, int c, int threads, Ref &r) {
  factor::stat::check::Data b = d.base;
  b.x = d.cand[c];
  factor::stat::check::Result res;
  factor::stat::check::run_cpu(b, res, threads);
  r.stat = res.stat;
}
// Search 的 Result → 同形 Ref (finish 各持有期), 给 分块 ≡ 整块 用
inline void to_ref(const Result &res, const factor::stat::Holds &hd, Ref &r) {
  r.stat.resize(static_cast<size_t>(hd.n));
  for (int i = 0; i < hd.n; ++i)
    r.stat[static_cast<size_t>(i)] = res.acc[i].finish(hd.h[i]);
}

// ---- 比较: 二级逐字段 (compare_stat) ----
inline Diff compare(const Ref &ref, const Result &got, const factor::stat::Holds &hd, Tol tol) {
  Ref g;
  to_ref(got, hd, g);
  return factor::stat::check::compare_stat(ref.stat.data(), g.stat.data(), hd.n, tol);
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
  factor::cpu::stat::Workspace w;
  w.ensure(A, H, 0, ex.threads());
  std::vector<std::vector<uint16_t>> ry(static_cast<size_t>(H), std::vector<uint16_t>(n));
  std::vector<uint16_t> rx(static_cast<size_t>(rows) * A); // 块形
  std::vector<factor::stat::Label> lab(static_cast<size_t>(H));
  const size_t off = static_cast<size_t>(row0) * A;
  Clock::time_point t0 = Clock::now();
  for (int i = 0; i < H; ++i) {
    const factor::stat::check::LabelHost &L = d.base.lab[static_cast<size_t>(i)];
    factor::cpu::stat::prep_label(ex, L.v[factor::stat::kLong].data(), L.m.data(), T, A, ry[static_cast<size_t>(i)].data(), w);
    lab[static_cast<size_t>(i)] = factor::stat::check::label_of(L, ry[static_cast<size_t>(i)].data(), off);
  }
  if (prep_ms)
    *prep_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
  const float *xv[kCands];
  const uint8_t *xm[kCands];
  for (int c = 0; c < kCands; ++c)
    xv[c] = d.cand[c].v.data() + off, xm[c] = d.cand[c].m.data() + off;
  t0 = Clock::now();
  factor::cpu::search::run(ex, kCands, xv, xm, d.base.g.m.data() + off, d.base.frame, rows, A, row0, t_lo, d.base.hd, lab.data(), rx.data(), w,
                           out);
  if (eval_ms)
    *eval_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

} // namespace factor::search::check
