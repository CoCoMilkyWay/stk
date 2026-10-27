#pragma once

// =============================================================================
// Stat 评估算子对拍的公共件: 造数 / 容差 / 比较 / 两后端驱动 (run_cpu / run_gpu; GPU 经 GpuRun.hpp)
// =============================================================================
//   两个消费者共用 (口径必须一致): src/factor/op_check.cpp 与 GUI OperatorsService (都链 GpuRun 的实现或桩).
//   无 golden: cpu ↔ gpu 两方 match 即过. 一级 Row 逐 (h, t) 比 (ok / ok_rac 逐位相等, 有效字段容差内),
//   二级 HoldStat 逐字段比 (n / n_rac 逐位相等).
//   造数沿用 factor/Check.hpp 的 Profile (HOLES 打整行缺失 / CONSTCOL 打全并列 / TINY 打量级), 两口径各一份 (Data::frame):
//     x     CS: NORM (按 profile); TS: 同一 NORM 经 Φ 映到 (0,1) 当分位 (TsRankRoll 的输出形状)
//     标签  每持有期 long = 0.3·z + NORM 噪声 (z = 造数用的 NORM 平面; IC ≈ 0.3, 二级统计有东西可比); short = −(0.3·z + 0.7·噪声);
//           掩码取噪声平面的空洞; 值经 fp16 往返 (与常驻格式同); 可成交码 eo 各持有期共用, 10% 单向 (仓位感知的交接链有东西可比)
//   本头只依赖 Contract / Check / Exec / GpuRun / Stat/Contract / Stat/Cpu; 消费者 TU 编进 -fno-fast-math.
// =============================================================================

#include "factor/Check.hpp"
#include "factor/GpuRun.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp"
#include "misc/exec.hpp"

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

// 对拍用的持有期键: 与 features 层 LabelGroups 的 LABEL_GROUPS 同值 (键定义见 Stat/Contract.hpp), 但这里不 include 它 (factor/ 不依赖 features/)
inline constexpr int kHolds[] = {1, 5, 15, 30, 60, factor::stat::kHoldClose, factor::stat::hold_open(1), factor::stat::hold_close(1), factor::stat::hold_close(2),
                                 factor::stat::hold_close(3)};
inline constexpr int kNumHolds = static_cast<int>(sizeof(kHolds) / sizeof(kHolds[0]));

inline uint16_t f2h(float f) {
  const _Float16 h = static_cast<_Float16>(f);
  uint16_t b;
  std::memcpy(&b, &h, sizeof(b));
  return b;
}

// 一组宿主标签 (两腿 fp16 位 + 掩码); ry 由各后端自己预处理, 不在此
struct LabelHost {
  std::vector<uint16_t> v[kLegs];
  std::vector<uint8_t> m;
};
// 宿主标签 + 可成交码 eo + 预处理 ry → 契约 Label (偏 off 格; ry 为空 = 未预处理)
inline Label label_of(const LabelHost &L, const std::vector<uint8_t> &eo, const uint16_t *ry, size_t off = 0) {
  return {{L.v[kLong].data() + off, L.v[kShort].data() + off}, L.m.data() + off, eo.data() + off, ry ? ry + off : nullptr};
}

struct Data {
  Frame frame = Frame::CS;
  Plane x;
  Plane g;                    // 截面池掩码 (只用 m; factor::check::fill_gate)
  std::vector<uint8_t> eo;    // 建仓可成交码 (各持有期共用): 多数两向, 少量单向 (封板, 走交接链)
  std::vector<LabelHost> lab; // 按 hd 序
  Holds hd;                   // kHolds 里本层合法的 (hold_ok)
  int T = 0, A = 0, seg = 0;  // seg = 段长 (Contract【层几何】)
  double cap = 0.0;           // 满仓 = 池内平均每行格数 / kGroups (与 GUI YPlanes::n_full 同式)
  Span span() const { return {T, A, geo_of(seg)}; }
};
// d 的全部标签 + 各组预处理 ry → 契约 Labels (偏 off 格; ry 为空 = 未预处理)
inline Labels labels_of(const Data &d, const std::vector<std::vector<uint16_t>> &ry, size_t off = 0) {
  assert(ry.empty() || ry.size() == static_cast<size_t>(d.hd.n));
  Labels L;
  L.hd = d.hd;
  L.cap = d.cap;
  for (int i = 0; i < d.hd.n; ++i)
    L.l[i] = label_of(d.lab[static_cast<size_t>(i)], d.eo, ry.empty() ? nullptr : ry[static_cast<size_t>(i)].data(), off);
  return L;
}

inline void make(Data &d, Frame f, Profile pr, int T, int A, int seg, std::mt19937 &rng) {
  d.frame = f;
  d.T = T, d.A = A, d.seg = seg;
  d.hd.n = 0;
  for (int h : kHolds)
    if (hold_ok(h, geo_of(seg)))
      d.hd.k[d.hd.n++] = h;
  assert_holds(d.hd, seg);
  Plane z;
  factor::check::fill(z, Gen::NORM, pr, T, A, seg, rng);
  factor::check::fill_gate(d.g, pr, T, A, seg, rng);
  const size_t n = static_cast<size_t>(T) * A;
  long long on = 0;
  for (size_t i = 0; i < n; ++i)
    on += d.g.m[i] != 0;
  d.cap = static_cast<double>(on) / T / kGroups;
  assert(d.cap > 0.0 && "截面池全空");
  d.x = z;
  if (f == Frame::TS) // 标准正态 → 分位 Φ(z) ∈ (0,1); 无效格值本就是 0
    for (size_t i = 0; i < n; ++i)
      d.x.v[i] = d.x.m[i] ? static_cast<float>(0.5 * (1.0 + std::erf(static_cast<double>(z.v[i]) / std::sqrt(2.0)))) : 0.f;
  d.eo.resize(n);
  std::uniform_int_distribution<int> pct(0, 99);
  for (size_t i = 0; i < n; ++i) { // 90% 两向, 5% 只可买 (封跌停), 5% 只可卖 (封涨停)
    const int p = pct(rng);
    d.eo[i] = p < 90 ? (kEoBuy | kEoSell) : p < 95 ? kEoBuy
                                                   : kEoSell;
  }
  d.lab.assign(static_cast<size_t>(d.hd.n), LabelHost{});
  for (LabelHost &L : d.lab) {
    Plane e;
    factor::check::fill(e, Gen::NORM, pr, T, A, seg, rng);
    L.v[kLong].resize(n), L.v[kShort].resize(n), L.m.resize(n);
    for (size_t i = 0; i < n; ++i) {
      const float yl = 0.3f * z.v[i] + e.v[i];
      const float ys = -(0.3f * z.v[i] + 0.7f * e.v[i]);
      L.v[kLong][i] = f2h(yl);
      L.v[kShort][i] = f2h(ys);
      L.m[i] = e.m[i];
    }
  }
}

// ---- 容差: 整数派生量 (ric / rac) 两后端本应逐位一致, 浮点和 (grp / ls / mkt[腿]) 只差求和序 ----
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
} // namespace detail

// 一级: rows[H][T] 逐行
inline Diff compare_rows(const Row *ref, const Row *got, size_t n, Tol tol) {
  Diff d;
  for (size_t i = 0; i < n; ++i) {
    const Row &a = ref[i], &b = got[i];
    if ((a.ok != 0) != (b.ok != 0) || (a.ok_rac != 0) != (b.ok_rac != 0)) {
      if (d.mask_bad++ == 0)
        d.worst_at = static_cast<int>(i);
      continue;
    }
    const int at = static_cast<int>(i);
    if (a.ok) {
      bool cnt_ok = true;
      for (int k = 0; k < kGroups; ++k)
        cnt_ok = cnt_ok && a.cnt[k] == b.cnt[k];
      if (!cnt_ok) { // 组计数是整数, 两后端必须逐位一致
        if (d.mask_bad++ == 0)
          d.worst_at = at;
        continue;
      }
      detail::cmp1(d, a.ric, b.ric, tol, at);
      for (int l = 0; l < kLegs; ++l)
        detail::cmp1(d, a.mkt[l], b.mkt[l], tol, at);
      detail::cmp1(d, a.ls, b.ls, tol, at);
      for (int k = 0; k < kGroups; ++k)
        detail::cmp1(d, a.grp[k], b.grp[k], tol, at);
    }
    if (a.ok_rac)
      detail::cmp1(d, a.rac, b.rac, tol, at);
  }
  return d;
}

// 二级: 每持有期一份
inline Diff compare_stat(const HoldStat *ref, const HoldStat *got, int H, Tol tol) {
  Diff d;
  for (int i = 0; i < H; ++i) {
    const HoldStat &a = ref[i], &b = got[i];
    if (a.hold != b.hold || a.n != b.n || a.n_rac != b.n_rac || a.n_all != b.n_all || a.n_trig != b.n_trig) {
      if (d.mask_bad++ == 0)
        d.worst_at = i;
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
    const float fa[] = {a.beta, a.mono, a.leg_n};
    const float fb[] = {b.beta, b.mono, b.leg_n};
    for (size_t j = 0; j < sizeof(fa) / sizeof(fa[0]); ++j)
      detail::cmp1(d, fa[j], fb[j], tol, i);
    for (int k = 0; k < kGroups; ++k)
      detail::cmp1(d, a.grp[k], b.grp[k], tol, i);
  }
  return d;
}

// ---- 结果容器 (两后端同形) ----
struct Result {
  std::vector<Row> rows;               // [H][T]
  std::vector<HoldStat> stat;          // [H]
  double prep_ms = 0.0, eval_ms = 0.0; // 预处理 (标签 rank, 常驻期一次) / 评估 (每因子一次)
};

// CPU 驱动 (ForkJoin(threads) 切行): 预处理 + 评估 (含二级) 各计时 (wall)
inline void run_cpu(const Data &d, Result &r, int threads) {
  using Clock = std::chrono::steady_clock;
  const Span s = d.span();
  const int H = d.hd.n;
  misc::exec::ForkJoin ex(threads);
  factor::cpu::stat::Workspace w;
  w.ensure(d.A, H, 0, ex.threads());
  std::vector<std::vector<uint16_t>> ry(static_cast<size_t>(H), std::vector<uint16_t>(s.n()));
  std::vector<uint16_t> rx(s.n());
  std::vector<Accum> acc(static_cast<size_t>(H));
  Clock::time_point t0 = Clock::now();
  for (int i = 0; i < H; ++i)
    factor::cpu::stat::prep_label(ex, label_of(d.lab[static_cast<size_t>(i)], d.eo, nullptr), s, ry[static_cast<size_t>(i)].data(), w);
  r.prep_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
  r.rows.assign(static_cast<size_t>(H) * d.T, Row{});
  r.stat.resize(static_cast<size_t>(H));
  t0 = Clock::now();
  factor::cpu::stat::run(ex, factor::stat::Plane{d.x.v.data(), d.x.m.data()}, d.g.m.data(), d.frame, s, labels_of(d, ry), rx.data(), w, acc.data(),
                         factor::cpu::stat::LandRows{r.rows.data(), d.T});
  finish_all(acc.data(), d.hd, d.seg, r.stat.data());
  r.eval_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// GPU 驱动 (一次性会话; 须 gpu::available()): rows 拷回对拍一级, acc 拷回宿主 finish_all; 计时为纯 kernel (cudaEvent)
inline void run_gpu(const Data &d, Result &r) {
  const int H = d.hd.n;
  std::vector<Accum> acc(static_cast<size_t>(H));
  r.rows.assign(static_cast<size_t>(H) * d.T, Row{});
  r.stat.resize(static_cast<size_t>(H));
  factor::gpu::run_stat(factor::stat::Plane{d.x.v.data(), d.x.m.data()}, d.g.m.data(), d.frame, d.T, d.A, d.seg, labels_of(d, {}), acc.data(),
                        r.rows.data(), &r.prep_ms, &r.eval_ms);
  finish_all(acc.data(), d.hd, d.seg, r.stat.data());
}

} // namespace factor::stat::check
