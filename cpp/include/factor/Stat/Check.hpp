#pragma once

// =============================================================================
// Stat 评估算子对拍的公共件: 造数 / 容差 / 比较 / CPU 驱动 (GPU 经 GpuRun.hpp)
// =============================================================================
//   两个消费者共用 (口径必须一致): src/factor/op_check.cpp 与 GUI OperatorsService.
//   无 golden: cpu ↔ gpu 两方 match 即过. 一级 Row 逐 (h, t) 比 (ok / ok_ac 逐位相等, 有效字段容差内),
//   二级 HoldStat 逐字段比 (n / n_ac 逐位相等).
//   造数沿用 factor/Check.hpp 的 Profile (HOLES 打整行缺失 / CONSTCOL 打全并列 / TINY 打量级), 两口径各一份 (Data::frame):
//     x     CS: NORM (按 profile); TS: 同一 NORM 经 Φ 映到 (0,1) 当分位 (TsRankRoll 的输出形状)
//     标签  每持有期 long = 0.3·z + NORM 噪声 (z = 造数用的 NORM 平面; IC ≈ 0.3, 二级统计有东西可比); short = −(0.3·z + 0.7·噪声);
//           掩码取噪声平面的空洞; 值经 fp16 往返 (与常驻格式同)
//   本头只依赖 Contract / Check / Stat/Contract / Stat/Cpu; 消费者 TU 编进 -fno-fast-math.
// =============================================================================

#include "factor/Check.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>
#include <thread>
#include <vector>

namespace factor::stat::check {

using factor::check::Diff;
using factor::check::Gen;
using factor::check::kTinyScale;
using factor::check::Plane;
using factor::check::Profile;
using factor::check::Tol;

// 对拍用的持有期键: 与 features 层 LabelReturn 的 LABEL_GROUPS 同值 (键定义见 Stat/Contract.hpp), 但这里不 include 它 (factor/ 不依赖 features/)
inline constexpr int kHolds[] = {1, 5, 15, 30, 60, factor::stat::kHoldClose, factor::stat::hold_open(1), factor::stat::hold_open(3), factor::stat::hold_open(5)};
inline constexpr int kNumHolds = static_cast<int>(sizeof(kHolds) / sizeof(kHolds[0]));

inline uint16_t f2h(float f) {
  const _Float16 h = static_cast<_Float16>(f);
  uint16_t b;
  std::memcpy(&b, &h, sizeof(b));
  return b;
}

// 一组宿主标签 (fp16 位 + 掩码); ry 由各后端自己预处理, 不在此
struct LabelHost {
  std::vector<uint16_t> lv, sv;
  std::vector<uint8_t> m;
};

struct Data {
  Frame frame = Frame::CS;
  Plane x;
  std::vector<LabelHost> lab; // 按 hd 序
  Holds hd;
  int T = 0, A = 0;
};

inline void make(Data &d, Frame f, Profile pr, int T, int A, std::mt19937 &rng) {
  d.frame = f;
  d.T = T, d.A = A;
  d.hd.n = kNumHolds;
  for (int i = 0; i < kNumHolds; ++i)
    d.hd.h[i] = kHolds[i];
  Plane z;
  factor::check::fill(z, Gen::NORM, pr, T, A, rng);
  const size_t n = static_cast<size_t>(T) * A;
  d.x = z;
  if (f == Frame::TS) // 标准正态 → 分位 Φ(z) ∈ (0,1); 无效格值本就是 0
    for (size_t i = 0; i < n; ++i)
      d.x.v[i] = d.x.m[i] ? static_cast<float>(0.5 * (1.0 + std::erf(static_cast<double>(z.v[i]) / std::sqrt(2.0)))) : 0.f;
  d.lab.assign(static_cast<size_t>(kNumHolds), LabelHost{});
  for (LabelHost &L : d.lab) {
    Plane e;
    factor::check::fill(e, Gen::NORM, pr, T, A, rng);
    L.lv.resize(n), L.sv.resize(n), L.m.resize(n);
    for (size_t i = 0; i < n; ++i) {
      const float yl = 0.3f * z.v[i] + e.v[i];
      const float ys = -(0.3f * z.v[i] + 0.7f * e.v[i]);
      L.lv[i] = f2h(yl);
      L.sv[i] = f2h(ys);
      L.m[i] = e.m[i];
    }
  }
}

// ---- 容差: 整数派生量 (ic / ac) 两后端本应逐位一致, 浮点和 (grp / ls / mkt) 只差求和序 ----
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
    if ((a.ok != 0) != (b.ok != 0) || (a.ok_ac != 0) != (b.ok_ac != 0)) {
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
      detail::cmp1(d, a.ic, b.ic, tol, at);
      detail::cmp1(d, a.mkt, b.mkt, tol, at);
      detail::cmp1(d, a.ls, b.ls, tol, at);
      for (int k = 0; k < kGroups; ++k)
        detail::cmp1(d, a.grp[k], b.grp[k], tol, at);
    }
    if (a.ok_ac)
      detail::cmp1(d, a.ac, b.ac, tol, at);
  }
  return d;
}

// 二级: 每持有期一份
inline Diff compare_stat(const HoldStat *ref, const HoldStat *got, int H, Tol tol) {
  Diff d;
  for (int i = 0; i < H; ++i) {
    const HoldStat &a = ref[i], &b = got[i];
    if (a.hold != b.hold || a.n != b.n || a.n_ac != b.n_ac) {
      if (d.mask_bad++ == 0)
        d.worst_at = i;
      continue;
    }
    const float fa[] = {a.ic_mean, a.ic_std, a.icir, a.ic_t, a.ic_pos, a.ic_skew, a.ic_kurt, a.ls_mean, a.ls_t, a.ls_pos, a.sharpe, a.beta, a.mono, a.rank_ac};
    const float fb[] = {b.ic_mean, b.ic_std, b.icir, b.ic_t, b.ic_pos, b.ic_skew, b.ic_kurt, b.ls_mean, b.ls_t, b.ls_pos, b.sharpe, b.beta, b.mono, b.rank_ac};
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

inline void summarize_all(Result &r, const Data &d) {
  r.stat.resize(static_cast<size_t>(d.hd.n));
  for (int i = 0; i < d.hd.n; ++i)
    r.stat[static_cast<size_t>(i)] = summarize(r.rows.data() + static_cast<size_t>(i) * d.T, d.T, d.hd.h[i]);
}

inline int cpu_threads() { return static_cast<int>(std::max<unsigned>(1, std::thread::hardware_concurrency())); }

// CPU 驱动: 预处理 + 评估各计时 (wall), 再做二级汇总
inline void run_cpu(const Data &d, Result &r, int threads) {
  using Clock = std::chrono::steady_clock;
  const size_t n = static_cast<size_t>(d.T) * d.A;
  const int H = d.hd.n;
  std::vector<std::vector<uint16_t>> ry(static_cast<size_t>(H), std::vector<uint16_t>(n));
  std::vector<factor::cpu::stat::Label> lab(static_cast<size_t>(H));
  Clock::time_point t0 = Clock::now();
  for (int i = 0; i < H; ++i) {
    const LabelHost &L = d.lab[static_cast<size_t>(i)];
    factor::cpu::stat::prep_label(L.lv.data(), L.m.data(), d.T, d.A, ry[static_cast<size_t>(i)].data(), threads);
    lab[static_cast<size_t>(i)] = {L.lv.data(), L.sv.data(), L.m.data(), ry[static_cast<size_t>(i)].data()};
  }
  r.prep_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
  std::vector<uint16_t> ws(n);
  r.rows.assign(static_cast<size_t>(H) * d.T, Row{});
  t0 = Clock::now();
  factor::cpu::stat::eval(d.x.v.data(), d.x.m.data(), d.frame, d.T, d.A, d.hd, lab.data(), ws.data(), r.rows.data(), threads);
  r.eval_ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
  summarize_all(r, d);
}

} // namespace factor::stat::check
