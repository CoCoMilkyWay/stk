#pragma once

// =============================================================================
// Stat 评估算子对拍的公共件: 造数 / 容差 / 比较 / 两后端驱动 (单候选 run_cpu / run_gpu, 批 run_batch_cpu / run_batch_gpu)
// =============================================================================
//   两个消费者共用 (口径必须一致): src/factor/op_check.cpp 与 GUI OperatorsService (都链 GpuRun 的实现或桩).
//   无 golden: cpu ↔ gpu 两方 match 即过. 一级 Row 逐 (h, t) 比 (ok / ok_rac / 计数逐位相等, 有效字段容差内),
//   二级逐日 Sum 逐 (h, 日) 比, 三级 HoldStat 逐字段比 (n / n_rac 逐位相等; 两后端都由各自的逐日 Sum 按日序 stat_of).
//   另比 账集 ↔ 行核 (compare_book): 因子两端 × 层级的账集批收益 (Book.hpp FactorBooks) = CPU Row 的端组 / 尾部 (绝对, ok 行).
//   批: 候选 = 从 x 派生的 kCands 张平面 (CS {x, −x, x²}; TS {x, 1−x, x²}); 参考 = 单候选 CPU 逐张评; 另比 分块 (两块带前后带, 逐日拼回) ≡ 整块.
//   造数沿用 factor/Check.hpp 的 Profile, 两口径各一份 (Data::m.f):
//     x     CS: NORM (按 profile); TS: 同一 NORM 经 Φ 映到 (0,1) 当分位 (TsRankRoll 的输出形状)
//     标签  每持有期 lv = 0.3·z + NORM 噪声 (IC ≈ 0.3); 偏移型另有 nv = 0.3·z + 0.7·噪声; 掩码取噪声平面的空洞; 值经 fp16 往返;
//           可成交码 eo 各持有期共用, 10% 单向 (仓位感知的交接链有东西可比); 逐行池基准 / 满仓与装载同一式 (Book.hpp pool_mean)
//   消费者 TU 编进 -fno-fast-math.
// =============================================================================

#include "factor/Check.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Stat/Book.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp"
#include "factor/Stat/Fold.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

namespace factor::stat::check {

using factor::check::Diff;
using factor::check::Gen;
using factor::check::kTinyScale;
using factor::check::Plane;
using factor::check::Profile;
using factor::check::Tol;

inline uint16_t f2h(float f) {
  const _Float16 h = static_cast<_Float16>(f);
  uint16_t b;
  std::memcpy(&b, &h, sizeof(b));
  return b;
}

inline constexpr int kCands = 3;

struct Data {
  Mode m; // 评估口径
  Plane x;
  Plane cand[kCands];           // 批的候选平面 (cand[0] = x)
  Plane g;                      // 截面池掩码 (只用 m; factor::check::fill_gate)
  std::vector<uint8_t> eo;      // 建仓可成交码 (各持有期共用)
  std::vector<LabelPlanes> lab; // 按 hd 序 (逐行 mk / cap 与 GUI 装载同一式 pool_mean)
  Holds hd;                     // 本层合法的全部持有期 (holds_on)
  int T = 0, A = 0, seg = 0;    // seg = 段长; T = 整段数
  Span span() const { return {T, A, geo_of(seg)}; }
};
// 第 i 组标签自第 t0 行起 (ry 为空 = 未预处理)
inline Label label_at(const Data &d, int i, const uint16_t *ry, size_t t0 = 0) { return label_of(d.lab[static_cast<size_t>(i)], ry, t0); }
// d 的全部标签 + 各组预处理 ry → 契约 Labels (自第 t0 行起; ry 为空 = 未预处理)
inline Labels labels_of(const Data &d, const std::vector<std::vector<uint16_t>> &ry, size_t t0 = 0) {
  assert(ry.empty() || ry.size() == static_cast<size_t>(d.hd.n));
  Labels L;
  L.hd = d.hd;
  L.eo = d.eo.data() + t0 * static_cast<size_t>(d.A);
  for (int i = 0; i < d.hd.n; ++i)
    L.l[i] = label_at(d, i, ry.empty() ? nullptr : ry[static_cast<size_t>(i)].data(), t0);
  return L;
}

inline void make(Data &d, const Mode &mode, Profile pr, int T, int A, int seg, std::mt19937 &rng) {
  assert(T % seg == 0 && T >= 2 * seg && "造数按整日, 至少两日 (分块切在日界)");
  const Frame f = mode.f;
  d.m = mode;
  d.T = T, d.A = A, d.seg = seg;
  d.hd = holds_on(seg);
  assert_holds(d.hd, seg);
  Plane z;
  factor::check::fill(z, Gen::NORM, pr, T, A, seg, rng);
  factor::check::fill_gate(d.g, pr, T, A, seg, rng);
  const size_t n = static_cast<size_t>(T) * A;
  d.x = z;
  if (f == Frame::TS) // 标准正态 → 分位 Φ(z) ∈ (0,1); 无效格值本就是 0
    for (size_t i = 0; i < n; ++i)
      d.x.v[i] = d.x.m[i] ? static_cast<float>(0.5 * (1.0 + std::erf(static_cast<double>(z.v[i]) / std::sqrt(2.0)))) : 0.f;
  for (int c = 0; c < kCands; ++c)
    d.cand[c] = d.x;
  for (size_t i = 0; i < n; ++i) {
    const float x = d.x.v[i];
    const bool m = d.x.m[i] != 0;
    d.cand[1].v[i] = m ? (f == Frame::CS ? -x : 1.f - x) : 0.f;
    d.cand[2].v[i] = m ? x * x : 0.f;
  }
  d.eo.resize(n);
  std::uniform_int_distribution<int> pct(0, 99);
  for (size_t i = 0; i < n; ++i) { // 90% 两向, 5% 只可买 (封跌停), 5% 只可卖 (封涨停)
    const int p = pct(rng);
    d.eo[i] = p < 90 ? (kEoBuy | kEoSell) : p < 95 ? kEoBuy
                                                   : kEoSell;
  }
  d.lab.assign(static_cast<size_t>(d.hd.n), LabelPlanes{});
  for (int h = 0; h < d.hd.n; ++h) {
    LabelPlanes &L = d.lab[static_cast<size_t>(h)];
    Plane e;
    factor::check::fill(e, Gen::NORM, pr, T, A, seg, rng);
    const bool nom = hold_offset(d.hd.k[h]);
    L.assign(static_cast<size_t>(T), static_cast<size_t>(A), nom);
    for (size_t i = 0; i < n; ++i) {
      L.lv[i] = f2h(0.3f * z.v[i] + e.v[i]);
      if (nom)
        L.nv[i] = f2h(0.3f * z.v[i] + 0.7f * e.v[i]);
      L.m[i] = e.m[i];
    }
    misc::exec::Serial ex;
    pool_mean(ex, L, d.eo.data(), hold_on(d.hd.k[h], geo_of(seg)), 0, T);
  }
}

// ---- 容差: 整数派生量 (ric / rac) 两后端本应逐位一致, 浮点和 (grp / tail / mkt) 只差求和序 ----
inline Tol tol_of(Profile pr) {
  Tol t{1e-5, 1e-4};
  if (pr == Profile::TINY)
    t.atol *= kTinyScale;
  return t;
}

namespace detail {
inline void cmp1(Diff &d, double a, double b, Tol tol, int at) {
  ++d.compared;
  const double e = std::fabs(a - b);
  if (e > d.worst)
    d.worst = e;
  if (e > tol.atol + tol.rtol * std::fmax(std::fabs(a), std::fabs(b)))
    if (d.val_bad++ == 0 && d.worst_at < 0)
      d.worst_at = at;
}
inline void bad(Diff &d, int at) {
  if (d.mask_bad++ == 0)
    d.worst_at = at;
}
} // namespace detail

// 一级: rows[H][T] 逐行
inline Diff compare_rows(const Row *ref, const Row *got, size_t n, Tol tol) {
  Diff d;
  for (size_t i = 0; i < n; ++i) {
    const Row &a = ref[i], &b = got[i];
    const int at = static_cast<int>(i);
    if ((a.ok != 0) != (b.ok != 0) || (a.ok_rac != 0) != (b.ok_rac != 0)) {
      detail::bad(d, at);
      continue;
    }
    if (a.ok) {
      bool cnt_ok = true; // 计数是整数, 两后端必须逐位一致
      for (int k = 0; k < kGroups; ++k)
        cnt_ok = cnt_ok && a.cnt[k] == b.cnt[k];
      for (int e = 0; e < kEnds; ++e)
        for (int j = 0; j < kTails; ++j)
          cnt_ok = cnt_ok && a.tail_n[e][j] == b.tail_n[e][j];
      if (!cnt_ok) {
        detail::bad(d, at);
        continue;
      }
      detail::cmp1(d, a.ric, b.ric, tol, at);
      detail::cmp1(d, a.mkt, b.mkt, tol, at);
      detail::cmp1(d, a.cap, b.cap, tol, at);
      for (int k = 0; k < kGroups; ++k)
        detail::cmp1(d, a.grp[k], b.grp[k], tol, at);
      for (int e = 0; e < kEnds; ++e)
        for (int j = 0; j < kTails; ++j)
          detail::cmp1(d, a.tail[e][j], b.tail[e][j], tol, at);
    }
    if (a.ok_rac)
      detail::cmp1(d, a.rac, b.rac, tol, at);
  }
  return d;
}

// 二级: 逐 (h, 日) 的 Sum (计数逐位相等, 和在容差内)
inline Diff compare_days(const Sum *ref, const Sum *got, size_t n, Tol tol) {
  Diff d;
  for (size_t i = 0; i < n; ++i) {
    const int at = static_cast<int>(i);
    const Sum::Head &a = ref[i].h, &b = got[i].h;
    bool cnt_ok = a.rows == b.rows && a.n_trig == b.n_trig && a.ric.n == b.ric.n && a.rac.n == b.rac.n && a.ric.npos == b.ric.npos;
    for (int k = 0; k < kGroups; ++k)
      cnt_ok = cnt_ok && ref[i].cnt[k] == got[i].cnt[k];
    for (int e = 0; e < kEnds; ++e)
      for (int j = 0; j < kTails; ++j)
        cnt_ok = cnt_ok && a.tail_n[e][j] == b.tail_n[e][j];
    if (!cnt_ok) {
      detail::bad(d, at);
      continue;
    }
    for (const auto m : {&MomAcc::s1, &MomAcc::s2})
      detail::cmp1(d, a.ric.*m, b.ric.*m, tol, at), detail::cmp1(d, a.rac.*m, b.rac.*m, tol, at), detail::cmp1(d, a.ls.*m, b.ls.*m, tol, at);
    detail::cmp1(d, a.smk, b.smk, tol, at);
    detail::cmp1(d, a.slm, b.slm, tol, at);
    detail::cmp1(d, a.cap, b.cap, tol, at);
    for (int e = 0; e < kEnds; ++e)
      for (int j = 0; j < kTails; ++j)
        detail::cmp1(d, a.tail[e][j], b.tail[e][j], tol, at);
    for (int k = 0; k < kGroups; ++k)
      detail::cmp1(d, ref[i].grp[k], got[i].grp[k], tol, at);
  }
  return d;
}

// 三级: 每持有期一份
inline Diff compare_stat(const HoldStat *ref, const HoldStat *got, int H, Tol tol) {
  Diff d;
  for (int i = 0; i < H; ++i) {
    const HoldStat &a = ref[i], &b = got[i];
    if (a.hold != b.hold || a.n != b.n || a.n_rac != b.n_rac || a.n_all != b.n_all || a.n_trig != b.n_trig) {
      detail::bad(d, i);
      continue;
    }
    const auto cmp_mom = [&](const Moments &x, const Moments &y) {
      const float fx[] = {x.mean, x.sd, x.ir, x.t, x.pos, x.skew, x.kurt}, fy[] = {y.mean, y.sd, y.ir, y.t, y.pos, y.skew, y.kurt};
      for (size_t j = 0; j < sizeof(fx) / sizeof(fx[0]); ++j)
        detail::cmp1(d, fx[j], fy[j], tol, i);
    };
    cmp_mom(a.ric, b.ric);
    cmp_mom(a.rac, b.rac);
    cmp_mom(a.ls, b.ls);
    const float fa[] = {a.beta, a.mono, a.end_n, a.cap, a.ann, a.sharpe}, fb[] = {b.beta, b.mono, b.end_n, b.cap, b.ann, b.sharpe};
    for (size_t j = 0; j < sizeof(fa) / sizeof(fa[0]); ++j)
      detail::cmp1(d, fa[j], fb[j], tol, i);
    for (int k = 0; k < kGroups; ++k)
      detail::cmp1(d, a.grp[k], b.grp[k], tol, i);
  }
  return d;
}
inline void merge_into(Diff &a, const Diff &b) {
  if (a.mask_bad == 0 && a.val_bad == 0 && (b.mask_bad || b.val_bad))
    a.worst_at = b.worst_at;
  a.mask_bad += b.mask_bad, a.val_bad += b.val_bad, a.compared += b.compared;
  a.worst = std::fmax(a.worst, b.worst);
}

// ---- 单候选结果 (两后端同形) ----
struct Result {
  std::vector<Row> rows;               // [H][T]
  std::vector<uint16_t> rx;            // [T][A] 已定向的 r16 (只 CPU; 账集对拍用)
  std::vector<Sum> day;                // [H][D]
  std::vector<HoldStat> stat;          // [H]
  double prep_ms = 0.0, eval_ms = 0.0; // 预处理 (标签 rank, 常驻期一次) / 评估 (每因子一次)
};

// 逐日 day[H][D] → 各持有期的三级 (Fold stat_of, 两后端同一份)
inline std::vector<HoldStat> stats_of(const Data &d, const Sum *day, int D) {
  std::vector<HoldStat> st(static_cast<size_t>(d.hd.n));
  for (int i = 0; i < d.hd.n; ++i)
    st[static_cast<size_t>(i)] = stat_of({day + static_cast<size_t>(i) * D, static_cast<size_t>(D)}, d.hd.k[i], d.seg);
  return st;
}

// 账集 ↔ 行核 (CPU 单候选 r): 因子两端 × 层级 (FactorBooks) 逐开仓行的批收益 = Row 的端组 grp / 尾部 tail (绝对; 只比 ok 行)
inline Diff compare_book(const Data &d, const Result &r, Tol tol) {
  assert(r.rx.size() == d.span().n() && "账集对拍要 CPU 的已定向 rx");
  const Span s = d.span();
  misc::exec::Serial ex;
  Diff out;
  for (int i = 0; i < d.hd.n; ++i) {
    const Label lb = label_at(d, i, nullptr);
    book_rows(ex, s, hold_on(d.hd.k[i], s.g), lb, d.eo.data(), FactorBooks{r.rx.data(), d.A, lb.cap}, Cost{}, [&](int, int t, const double *ret, const TurnFlow *) {
      const int at = i * d.T + t;
      const Row &w = r.rows[static_cast<size_t>(at)];
      if (!w.ok)
        return;
      for (int e = 0; e < kEnds; ++e) {
        detail::cmp1(out, ret[e * kLevels], w.grp[end_group(e)], tol, at);
        for (int j = 0; j < kTails; ++j)
          detail::cmp1(out, ret[e * kLevels + 1 + j], w.tail[e][j], tol, at);
      }
    });
  }
  return out;
}

// CPU 驱动 (ForkJoin(threads) 切日): 预处理 + 评估 (含三级) 各计时 (wall)
inline void run_cpu(const Data &d, Result &r, int threads) {
  using Clock = std::chrono::steady_clock;
  const Span s = d.span();
  const int H = d.hd.n;
  misc::exec::ForkJoin ex(threads);
  factor::cpu::stat::Workspace w;
  w.ensure(d.A, ex.threads());
  std::vector<std::vector<uint16_t>> ry(static_cast<size_t>(H), std::vector<uint16_t>(s.n()));
  r.rx.assign(s.n(), 0);
  Clock::time_point t0 = Clock::now();
  for (int i = 0; i < H; ++i)
    factor::cpu::stat::prep_label(ex, label_at(d, i, nullptr), s, ry[static_cast<size_t>(i)].data(), w);
  r.prep_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
  r.rows.assign(static_cast<size_t>(H) * d.T, Row{});
  r.day.assign(static_cast<size_t>(H) * s.days(), Sum{});
  t0 = Clock::now();
  factor::cpu::stat::run(ex, factor::stat::Plane{d.x.v.data(), d.x.m.data()}, d.g.m.data(), d.m, s, labels_of(d, ry), r.rx.data(), w, r.day.data(),
                         factor::cpu::stat::LandRows{r.rows.data(), d.T});
  r.stat = stats_of(d, r.day.data(), s.days());
  r.eval_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// GPU 驱动 (一次性会话; 须 gpu::available()): rows / day 拷回对拍, 宿主 stat_of; 计时为纯 kernel (cudaEvent)
inline void run_gpu(const Data &d, Result &r) {
  const int H = d.hd.n;
  r.rows.assign(static_cast<size_t>(H) * d.T, Row{});
  r.day.assign(static_cast<size_t>(H) * d.span().days(), Sum{});
  const float *xv = d.x.v.data();
  const uint8_t *xm = d.x.m.data();
  factor::gpu::run_stat(1, &xv, &xm, d.g.m.data(), d.m, d.span(), labels_of(d, {}), r.day.data(), r.rows.data(), &r.prep_ms, &r.eval_ms);
  r.stat = stats_of(d, r.day.data(), d.span().days());
}

// ---- 批: 一块 = 行 [row0, row0 + rows), 统计 [t_lo, rows − t_back) (全按整日) ----
struct Block {
  int row0 = 0, rows = 0, t_lo = 0, t_back = 0;
  std::vector<Sum> day; // [kCands][H][D]
  Span span(const Data &d) const { return {rows, d.A, geo_of(d.seg), t_lo, t_back}; }
};
// 分块切法 (造数无因子回看): 块 1 = 前一半日 + 后带 (hold_reach 取整日), 块 2 = 后一半日 + 前带 (hold_reach + kChainDays 日)
inline void split_of(const Data &d, Block &full, Block &c1, Block &c2) {
  const int S = d.seg, days = d.T / S, d1 = days / 2;
  const int reach = (hold_reach(d.hd, S) + S - 1) / S, back = std::min(reach, days - d1), front = std::min(reach + kChainDays, d1);
  full = {0, d.T, 0, 0, {}};
  c1 = {0, (d1 + back) * S, 0, back * S, {}};
  c2 = {(d1 - front) * S, (days - d1 + front) * S, front * S, 0, {}};
}

// eval_ms 非空 → 回填评估耗时 (wall; 不含标签 prep 与工作区分配, 与 run_cpu 的 eval_ms 同口径)
inline void run_batch_cpu(const Data &d, int threads, Block &b, double *eval_ms = nullptr) {
  const int H = d.hd.n;
  const Span all = d.span(), s = b.span(d);
  misc::exec::ForkJoin ex(threads);
  factor::cpu::stat::Workspace w;
  w.ensure(d.A, ex.threads());
  std::vector<std::vector<uint16_t>> ry(static_cast<size_t>(H), std::vector<uint16_t>(all.n()));
  for (int i = 0; i < H; ++i)
    factor::cpu::stat::prep_label(ex, label_at(d, i, nullptr), all, ry[static_cast<size_t>(i)].data(), w);
  const size_t off = static_cast<size_t>(b.row0) * d.A;
  Batch B;
  B.C = kCands;
  for (int c = 0; c < kCands; ++c)
    B.x[c] = {d.cand[c].v.data() + off, d.cand[c].m.data() + off};
  std::vector<uint16_t> rx(s.n());
  b.day.assign(static_cast<size_t>(kCands) * H * s.days(), Sum{});
  const auto t0 = std::chrono::steady_clock::now();
  factor::cpu::stat::run_batch(ex, B, d.g.m.data() + off, d.m, s, labels_of(d, ry, static_cast<size_t>(b.row0)), rx.data(), w, b.day.data());
  if (eval_ms)
    *eval_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
}

// eval_ms 非空 → 回填纯 kernel 耗时 (cudaEvent; 不含会话开关 / 上传 / 标签 prep, 与 run_gpu 同口径)
inline void run_batch_gpu(const Data &d, Block &b, double *eval_ms = nullptr) {
  const int H = d.hd.n;
  const Span s = b.span(d);
  const size_t off = static_cast<size_t>(b.row0) * d.A;
  const float *xv[kCands];
  const uint8_t *xm[kCands];
  for (int c = 0; c < kCands; ++c)
    xv[c] = d.cand[c].v.data() + off, xm[c] = d.cand[c].m.data() + off;
  b.day.assign(static_cast<size_t>(kCands) * H * s.days(), Sum{});
  factor::gpu::run_stat(kCands, xv, xm, d.g.m.data() + off, d.m, s, labels_of(d, {}, static_cast<size_t>(b.row0)), b.day.data(), nullptr, nullptr, eval_ms);
}

// 批的参考: 候选 c 走单候选 CPU (整张量)
inline void reference(const Data &d, int c, int threads, Result &r) {
  Data b = d;
  b.x = d.cand[c];
  run_cpu(b, r, threads);
}

// 批一块 vs 参考: 三级 (块即整张量时) + 二级按绝对日对回参考
inline Diff compare_block(const Data &d, const Result *ref, const Block &b, Tol tol, bool whole) {
  const int H = d.hd.n, S = d.seg, D = b.span(d).days(), D_all = d.T / S, d0 = (b.row0 + b.t_lo) / S;
  Diff out;
  for (int c = 0; c < kCands; ++c) {
    if (whole)
      merge_into(out, compare_stat(ref[c].stat.data(), stats_of(d, b.day.data() + static_cast<size_t>(c) * H * D, D).data(), H, tol));
    for (int i = 0; i < H; ++i)
      merge_into(out, compare_days(ref[c].day.data() + static_cast<size_t>(i) * D_all + d0, b.day.data() + (static_cast<size_t>(c) * H + i) * D, static_cast<size_t>(D), tol));
  }
  return out;
}
// 两块逐日拼回 vs 整块 (三级)
inline Diff compare_merge(const Data &d, const Block &full, const Block &c1, const Block &c2, Tol tol) {
  const int H = d.hd.n, D = full.span(d).days(), D1 = c1.span(d).days(), D2 = c2.span(d).days();
  assert(D1 + D2 == D);
  Diff out;
  for (int c = 0; c < kCands; ++c) {
    std::vector<Sum> cat(static_cast<size_t>(H) * D);
    for (int i = 0; i < H; ++i) {
      const auto at = [&](const Block &b, int Db) { return b.day.begin() + static_cast<std::ptrdiff_t>((static_cast<size_t>(c) * H + i) * Db); };
      std::copy(at(c1, D1), at(c1, D1) + D1, cat.begin() + static_cast<std::ptrdiff_t>(static_cast<size_t>(i) * D));
      std::copy(at(c2, D2), at(c2, D2) + D2, cat.begin() + static_cast<std::ptrdiff_t>(static_cast<size_t>(i) * D + D1));
    }
    merge_into(out, compare_stat(stats_of(d, full.day.data() + static_cast<size_t>(c) * H * D, D).data(), stats_of(d, cat.data(), D).data(), H, tol));
  }
  return out;
}

} // namespace factor::stat::check
