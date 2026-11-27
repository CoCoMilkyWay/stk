#pragma once

// =============================================================================
// Fold — Stat 的二级 / 三级 (契约 factor/Stat/Contract.hpp): Day (逐日) / Accum → HoldStat (每持有期) + 派生量 (毛 ⇄ 净)
// =============================================================================
//   【二级 Day】逐统计日 (建仓日): ok 行数 / Σ rIC / Σ ls_row / 各组 Σ grp (行序累加, 两后端逐位同); 块内统计行按整日对齐.
//     日净值 LS 的日收益 (钱按平仓日, define/Label.hpp 协议表 2【记日】) = 建仓日 i − hold_days 的 Σ ls_row / h̄ (每批资金 1/h̄, 不 ok 行记 0)
//     → Daily (define/Annual.hpp): 复利年化 ann / Sharpe (全仓同一口径)
//   【三级 HoldStat (每 hold)】只用 ok 行 (n) / ok_rac 行 (n_rac); n_all = 攒过的全部行:
//     ric / rac / ls  逐行量沿 t 的矩 (Moments, MomAcc): mean / sd (ddof=1) / ir / pos / skew / kurt; t = ir · √(n/h) (重叠折算)
//     每期真实收益 (派生): 不 ok 的行记 0 补满 n_all 行 —— ls_period = Σls / n_all; ann / sharpe = 日净值 LS 的复利年化 / Sharpe
//     beta = ls 对 mkt 的 OLS 斜率; grp[k] = Σ grp / n_all; mono = Spearman(组号, grp[k]); n_trig / end_n 诊断.
//     n < 3 → 标签侧全 0; n_rac < 3 → rac 全 0. Accum = Head + Grp[kGroups] 可合并 (GPU 一 warp 一候选: lane 0 攒 Head, lane k 攒 Grp[k]).
//   Row / HoldStat 全是毛口径; 净多空 / 换手 (账集流水的派生) 在 Stat/Book.hpp. 主机 / 设备两用 (Gpu.cuh 引入)
// =============================================================================

#include "define/Annual.hpp"
#include "factor/Stat/Contract.hpp"

#include <cassert>
#include <cmath>
#include <limits>
#include <span>
#include <utility>

namespace factor::stat {

// ---- 二级输出 (逐日): 一个统计日的 ok 行数 / Σ rIC / Σ 多空 / 各组 Σ 持仓收益 (窗 mono / 分年 mono 的原料) ----
struct Day {
  int n = 0;
  double ric = 0.0, ls = 0.0;
  double grp[kGroups] = {};
  FACTOR_HD void add_head(const Row &r) { // 标量三项 (GPU 批核 lane 0 攒; 组和 lane q 攒 grp[q])
    if (r.ok)
      ++n, ric += r.ric, ls += ls_row(r);
  }
  FACTOR_HD void add(const Row &r) {
    add_head(r);
    if (r.ok)
      for (int k = 0; k < kGroups; ++k)
        grp[k] += r.grp[k];
  }
  void merge(const Day &o) {
    n += o.n, ric += o.ric, ls += o.ls;
    for (int k = 0; k < kGroups; ++k)
      grp[k] += o.grp[k];
  }
  Day operator-(const Day &o) const { // 前缀相减 = 窗
    Day w = *this;
    w.n -= o.n, w.ric -= o.ric, w.ls -= o.ls;
    for (int k = 0; k < kGroups; ++k)
      w.grp[k] -= o.grp[k];
    return w;
  }
  Day reversed() const { // 改读反向: rIC / 多空变号, 组倒序
    Day w = *this;
    w.ric = -ric, w.ls = -ls;
    for (int k = 0; k < kGroups; ++k)
      w.grp[k] = grp[kGroups - 1 - k];
    return w;
  }
};
// 日净值 LS 的逐日收益 (钱按平仓日 Hold::money_day): 第 i 日 = 建仓日 i − lag 的 Σ ls_row / h̄ (序列首 lag 日为 0; 平仓越过序列末的不计)
struct LsDays {
  std::span<const Day> d;
  size_t lag = 0;
  double span = 1.0;
  LsDays(std::span<const Day> d_, HoldId hold, int seg) : d(d_) {
    const Hold hd = hold_on(hold, geo_of(seg));
    lag = static_cast<size_t>(hd.money_day(0)), span = hd.span();
  }
  double operator[](size_t i) const { return i >= lag ? d[i - lag].ls / span : 0.0; }
};
// 逐日 d → 日净值 LS 的 Daily
inline Daily ls_daily(std::span<const Day> d, HoldId hold, int seg) {
  const LsDays ls(d, hold, seg);
  Daily dl;
  for (size_t i = 0; i < d.size(); ++i)
    dl.add(ls[i]);
  return dl;
}

// ---- 三级输出: 每持有期一份 ----
// 口径戳: 字段不变但口径变时 +1, 落盘 stat 与之不符读取即作废 (下次 Run 重算)
inline constexpr int kStatSemver = 9;
struct Moments {
  float mean = 0.f, sd = 0.f, ir = 0.f, t = 0.f, pos = 0.f, skew = 0.f, kurt = 0.f;
};
struct HoldStat {
  int seg = 0; // 因子所在层的每段行数 (finish 填; 不落文件, 载入时由因子目录的层给): 年化 / 换手折算用
  HoldId hold = kHoldM1;
  int n = 0, n_rac = 0;
  int n_all = 0, n_trig = 0; // 攒过的全部行 (真实收益的分母) / 触发行 (ok 且两端至少一端非空)
  Moments ric, rac, ls;      // 逐行量 (ric / ls: ok 行; rac: ok_rac 行)
  float beta = 0.f, mono = 0.f;
  float end_n = 0.f;             // 平均每触发行一端成员数 (util_of 的分子)
  float ann = 0.f, sharpe = 0.f; // 日净值 LS 的复利年化 / Sharpe (Daily)
  float grp[kGroups] = {};       // 组每行持仓收益的均值 (Σ / n_all)
};
// 每年满仓轮数 = kDaysPerYear · S / h̄: 金额口径的年化 (均值超额 / 均摊成本 / 换手 = 满仓一年) 走这一个
inline double span_per_year(HoldId h, int seg) { return static_cast<double>(kDaysPerYear) * seg / hold_on(h, geo_of(seg)).span(); }
inline double span_per_year(const HoldStat &s) { return span_per_year(s.hold, s.seg); }
// LS 每期真实收益 / 日净值复利年化 / Sharpe (派生量)
inline double ls_period(const HoldStat &s) { return s.n_all > 0 ? static_cast<double>(s.ls.mean) * s.n / s.n_all : 0.0; }
inline double ls_annual(const HoldStat &s) { return s.ann; }
inline double sharpe_of(const HoldStat &s) { return s.sharpe; }
// 端 e 的每期真实收益 (超额; 与 ls_period 同分母)
inline double end_period(const HoldStat &s, int e) { return s.grp[end_group(e)]; }
// 触发率 = 触发行 / ok 行 (诊断)
inline double trigger_of(const HoldStat &s) { return s.n > 0 ? static_cast<double>(s.n_trig) / s.n : 0.0; }
// 两端利用率 = 平均每触发行一端成员数 / 满仓 cap, 不封顶 (诊断)
inline double util_of(const HoldStat &s, double cap) {
  assert(cap > 0.0);
  return static_cast<double>(s.end_n) / cap;
}
// ---- 三级汇总 (主机, double; 两后端共用) ----

// 组号 0..K−1 与 g[k] 的 Spearman (g 并列均秩); g 全并列 → 0
inline double mono_of(const double *g, int K) {
  double rk[kGroups];
  for (int i = 0; i < K; ++i) {
    int less = 0, eq = 0;
    for (int j = 0; j < K; ++j) {
      less += g[j] < g[i];
      eq += g[j] == g[i];
    }
    rk[i] = less + (eq + 1) * 0.5;
  }
  const double mu = (K + 1) * 0.5;
  double cxy = 0.0, vx = 0.0, vy = 0.0;
  for (int i = 0; i < K; ++i) {
    const double dx = (i + 1) - mu, dy = rk[i] - mu;
    cxy += dx * dy;
    vx += dx * dx;
    vy += dy * dy;
  }
  return vy > 0.0 ? cxy / std::sqrt(vx * vy) : 0.0;
}

// 宿主侧的全量行和 = Day + 行数 / rIC 二阶与正号数 / 组真持有只数 / 两端尾部 (分段观察: 全期 / 月 / 时段 / 条件同一份; 可合并)
//   每期量与三级同分母: 组 / 尾部 = Σ / 全部行 (不 ok 行记 0), 只数 = Σ / ok 行
struct Sums : Day {
  int rows = 0, ric_pos = 0;
  double ric2 = 0.0;
  double cnt[kGroups] = {}, tail[kEnds][kTails] = {}, tail_n[kEnds][kTails] = {};
  void add(const Row &r) {
    ++rows;
    Day::add(r);
    if (!r.ok)
      return;
    ric2 += static_cast<double>(r.ric) * r.ric, ric_pos += r.ric > 0.f;
    for (int k = 0; k < kGroups; ++k)
      cnt[k] += r.cnt[k];
    for (int e = 0; e < kEnds; ++e)
      for (int j = 0; j < kTails; ++j)
        tail[e][j] += r.tail[e][j], tail_n[e][j] += r.tail_n[e][j];
  }
  void merge(const Sums &o) {
    rows += o.rows, ric_pos += o.ric_pos, ric2 += o.ric2;
    Day::merge(o);
    for (int k = 0; k < kGroups; ++k)
      cnt[k] += o.cnt[k];
    for (int e = 0; e < kEnds; ++e)
      for (int j = 0; j < kTails; ++j)
        tail[e][j] += o.tail[e][j], tail_n[e][j] += o.tail_n[e][j];
  }
  static constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
  double grp_of(int k) const { return n ? grp[k] / rows : kNaN; }
  double tail_of(int e, int j) const { return rows ? tail[e][j] / rows : kNaN; }
  double held(int k) const { return n ? cnt[k] / n : kNaN; }
  double tail_held(int e, int j) const { return n ? tail_n[e][j] / n : kNaN; }
  double ls_of() const { return (grp_of(end_group(kTop)) - grp_of(end_group(kBot))) / 2.0; }
  double mono() const {
    if (!n)
      return kNaN;
    double g[kGroups];
    for (int k = 0; k < kGroups; ++k)
      g[k] = grp_of(k);
    return mono_of(g, kGroups);
  }
  double ric_mean() const { return n ? ric / n : kNaN; }
  double ric_t() const { // mean / sd (ddof 1) · √n
    if (n < 2)
      return kNaN;
    const double m = ric / n, v = (ric2 - n * m * m) / (n - 1);
    return v > 0.0 ? m / std::sqrt(v) * std::sqrt(static_cast<double>(n)) : kNaN;
  }
  double ric_up() const { return n ? static_cast<double>(ric_pos) / n : kNaN; }
};

// 一个逐行量 (rIC / rAC / LS 同一份) 的幂和 → Moments; n < 3 → 全 0
struct MomAcc {
  int n = 0, npos = 0, nneg = 0;
  double s1 = 0.0, s2 = 0.0, s3 = 0.0, s4 = 0.0;
  FACTOR_HD void add(float v) {
    ++n;
    npos += v > 0.f, nneg += v < 0.f;
    const double x = v, x2 = x * x;
    s1 += x, s2 += x2, s3 += x2 * x, s4 += x2 * x2;
  }
  FACTOR_HD void merge(const MomAcc &o) {
    n += o.n, npos += o.npos, nneg += o.nneg;
    s1 += o.s1, s2 += o.s2, s3 += o.s3, s4 += o.s4;
  }
  // 逐行量取负后的幂和 (奇次变号, 正负计数互换)
  MomAcc negated() const {
    MomAcc m = *this;
    m.npos = nneg, m.nneg = npos;
    m.s1 = -s1, m.s3 = -s3;
    return m;
  }
  // h = 持有期折成的行数 (Hold::h; t 值的重叠折算 n/h)
  Moments finish(int h) const {
    Moments m;
    if (n < 3)
      return m;
    const double nn = n, mu = s1 / nn;
    const double m2 = std::fmax(s2 - nn * mu * mu, 0.0);
    const double m3 = s3 - 3.0 * mu * s2 + 2.0 * nn * mu * mu * mu;
    const double m4 = std::fmax(s4 - 4.0 * mu * s3 + 6.0 * mu * mu * s2 - 3.0 * nn * mu * mu * mu * mu, 0.0);
    const double sd = std::sqrt(m2 / (nn - 1));
    m.mean = static_cast<float>(mu);
    m.sd = static_cast<float>(sd);
    m.pos = static_cast<float>(npos / nn);
    if (sd > 0.0) {
      m.ir = static_cast<float>(mu / sd);
      m.t = static_cast<float>(mu / sd * std::sqrt(nn / h));
      const double v = m2 / nn;
      m.skew = static_cast<float>((m3 / nn) / (v * std::sqrt(v)));
      m.kurt = static_cast<float>((m4 / nn) / (v * v) - 3.0);
    }
    return m;
  }
};

struct Accum {
  // 标量头: 逐行量的矩 + beta 的幂和 + 行计数 (GPU 上 lane 0 攒)
  struct Head {
    MomAcc ric, rac, ls;
    double smk = 0.0, smk2 = 0.0, slm = 0.0; // mkt 幂和 + ls·mkt 交叉 (beta)
    int n_all = 0, n_trig = 0;
    FACTOR_HD void add(const Row &w) {
      ++n_all;
      if (w.ok) {
        ric.add(w.ric);
        const double mk = w.mkt;
        smk += mk, smk2 += mk * mk;
        n_trig += w.cnt[end_group(kTop)] + w.cnt[end_group(kBot)] > 0;
        const float v = ls_row(w);
        ls.add(v);
        slm += static_cast<double>(v) * mk;
      }
      if (w.ok_rac)
        rac.add(w.rac);
    }
    FACTOR_HD void merge(const Head &o) {
      ric.merge(o.ric), rac.merge(o.rac), ls.merge(o.ls);
      smk += o.smk, smk2 += o.smk2, slm += o.slm;
      n_all += o.n_all, n_trig += o.n_trig;
    }
  };
  // 一个组: 组持仓收益 / 组样本数 沿 t 的和 (GPU 上 lane k 攒组 k; 只收 ok 行)
  struct Grp {
    double s = 0.0;
    long long c = 0;
    FACTOR_HD void add(float g, unsigned n) { s += g, c += n; }
    FACTOR_HD void merge(const Grp &o) { s += o.s, c += o.c; }
  };
  Head head;
  Grp grp[kGroups];

  FACTOR_HD void add(const Row &w) {
    head.add(w);
    if (w.ok)
      for (int k = 0; k < kGroups; ++k)
        grp[k].add(w.grp[k], w.cnt[k]);
  }
  FACTOR_HD void merge(const Accum &o) {
    head.merge(o.head);
    for (int k = 0; k < kGroups; ++k)
      grp[k].merge(o.grp[k]);
  }
  // seg = 因子所在层的每段行数 (折算 / 年化); dl = 日净值 LS (空 = ann / sharpe 记 0); n < 3 → 标签侧全 0, rac 照算
  HoldStat finish(HoldId hold, int seg, const Daily &dl = {}) const {
    const int h = hold_on(hold, geo_of(seg)).h;
    assert(h >= 1);
    HoldStat s;
    s.seg = seg;
    s.hold = hold;
    s.n = head.ric.n;
    s.n_rac = head.rac.n;
    s.n_all = head.n_all;
    s.n_trig = head.n_trig;
    s.ric = head.ric.finish(h);
    s.rac = head.rac.finish(h);
    s.ls = head.ls.finish(h);
    if (s.n < 3)
      return s;
    s.ann = static_cast<float>(dl.ann());
    s.sharpe = static_cast<float>(dl.sharpe());
    double g[kGroups];
    for (int k = 0; k < kGroups; ++k)
      g[k] = grp[k].s / static_cast<double>(s.n_all);
    const double nn = s.n, mmk = head.smk / nn;
    const double vmk = std::fmax(head.smk2 - nn * mmk * mmk, 0.0);
    if (vmk > 0.0)
      s.beta = static_cast<float>((head.slm - head.ls.s1 * mmk) / vmk);
    for (int k = 0; k < kGroups; ++k)
      s.grp[k] = static_cast<float>(g[k]);
    s.mono = static_cast<float>(mono_of(g, kGroups));
    const long long ne = grp[end_group(kTop)].c + grp[end_group(kBot)].c;
    s.end_n = s.n_trig > 0 ? static_cast<float>(static_cast<double>(ne) / (2.0 * s.n_trig)) : 0.f;
    return s;
  }
};
// 正向攒的累加器改读成方向 d (Search 定向用): 反向 = rIC / LS 幂和取负, 组倒序
inline Accum oriented(Accum a, int d) {
  if (d == kFwd)
    return a;
  Accum::Head &h = a.head;
  h.ric = h.ric.negated();
  h.ls = h.ls.negated();
  h.slm = -h.slm;
  for (int k = 0; k < kGroups / 2; ++k)
    std::swap(a.grp[k], a.grp[kGroups - 1 - k]);
  return a;
}

// 流程出口 Accum[hd.n] (+ 日净值 Daily[hd.n], 可空) → HoldStat[hd.n]
inline void finish_all(const Accum *acc, const Daily *dl, const Holds &hd, int seg, HoldStat *out) {
  assert(acc && out);
  for (int i = 0; i < hd.n; ++i)
    out[i] = acc[i].finish(hd.k[i], seg, dl ? dl[i] : Daily{});
}

} // namespace factor::stat
