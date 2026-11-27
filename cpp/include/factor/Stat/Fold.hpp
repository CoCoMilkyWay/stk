#pragma once

// =============================================================================
// Fold — Stat 的二级 / 三级 (契约 factor/Stat/Contract.hpp): 唯一累加单元 Sum → HoldStat + 净值记日 Realize + 派生量
// =============================================================================
//   【二级 Sum】一份 Row 的可合并和 (行序累加, 两后端逐位同; 逐统计日一份 = 建仓日 Hold::stat_day): 任一日段 = merge / 前缀相减,
//     全期 / 分年 / 滚动窗 / 观察分段同一 finish. 头 Head (GPU 核 lane 0 攒) + 组 grp / cnt (lane q 攒组 q). 收益口径在折进时定 (add(r, ret), Row 恒绝对).
//   【净值 Realize】协议表 2【净值】【记日】的唯一实现: 行 u 这一批的量 → 期 (实现行 / stride), 每批资金 1/h̄;
//     实现行 = 平仓行 Hold::exit (收益, 钱按平仓日) | 建仓行 (成本, 即付). 日净值 LS / 账线 / 选池账 / 净值探针同一份.
//     一期只收一个交易日开的批 (开仓行恒有 平仓日 = 建仓日 + hold_days) ⇒ 按日切线程直落期线无竞争; 按日的线直接平移 (ls_days)
//   【期线】LedgerLines [线][期] (钱的唯一载体) ← 写端 Lines (每线一个 Realize 记日); 一组账线 BookLines = 收益 + 流水各字段, 净 = 毛 − Fee::cost
//   【三级 HoldStat (每 hold)】只用 ok 行 (n) / ok_rac 行 (n_rac); n_all = 攒过的全部行:
//     ric / rac / ls  逐行量沿 t 的矩 (MomAcc → Moments): mean / sd (ddof=1) / ir / pos / skew / kurt; t = ir · √(n/h) (重叠折算)
//     每期真实收益 (派生): 不 ok 的行记 0 补满 n_all 行 —— ls_period = Σls / n_all; ann / sharpe = 日净值 LS (Realize) 的复利年化 / Sharpe
//     beta = ls 对 mkt 的 OLS 斜率; grp[k] = Σ grp / n_all; mono = Spearman(组号, grp[k]); n_trig / end_n 诊断.
//     n < 3 → 标签侧全 0; n_rac < 3 → rac 全 0.
//   Row / HoldStat 全是毛口径; 净多空 / 换手 (账集流水的派生) 在 Stat/Book.hpp. 主机 / 设备两用 (Gpu.cuh 引入)
// =============================================================================

#include "define/Annual.hpp"
#include "define/Fee.hpp"
#include "factor/Stat/Contract.hpp"

#include <cassert>
#include <cmath>
#include <limits>
#include <span>
#include <utility>
#include <vector>

namespace factor::stat {

// 一个逐行量 (rIC / rAC / LS 同一份) 的幂和 → Moments; n < 3 → 全 0
struct Moments {
  float mean = 0.f, sd = 0.f, ir = 0.f, t = 0.f, pos = 0.f, skew = 0.f, kurt = 0.f;
};
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
  MomAcc operator-(const MomAcc &o) const { return {n - o.n, npos - o.npos, nneg - o.nneg, s1 - o.s1, s2 - o.s2, s3 - o.s3, s4 - o.s4}; }
  // 逐行量取负后的幂和 (奇次变号, 正负计数互换)
  MomAcc negated() const { return {n, nneg, npos, -s1, s2, -s3, s4}; }
  double mean() const { return n ? s1 / n : 0.0; }
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

// ---- 三级输出: 每持有期一份 ----
// 口径戳: 字段不变但口径变时 +1, 落盘 stat 与之不符读取即作废 (下次 Run 重算)
inline constexpr int kStatSemver = 12;
struct HoldStat {
  int seg = 0; // 因子所在层的每段行数 (finish 填; 不落文件, 载入时由因子目录的层给): 年化 / 换手折算用
  HoldId hold = kHoldM1;
  int n = 0, n_rac = 0;
  int n_all = 0, n_trig = 0; // 攒过的全部行 (真实收益的分母) / 触发行 (ok 且两端至少一端非空)
  Moments ric, rac, ls;      // 逐行量 (ric / ls: ok 行; rac: ok_rac 行)
  float beta = 0.f, mono = 0.f;
  float end_n = 0.f;                                     // 平均每触发行一端成员数 (util_of 的分子)
  float cap = 0.f;                                       // ok 行的平均满仓只数 (契约【持仓口径】; util_of 的分母)
  float ann = 0.f, sharpe = 0.f;                         // 日净值 LS 的复利年化 / Sharpe (Daily)
  float grp[kGroups] = {};                               // 组每行持仓收益的均值 (Σ / n_all)
  Hold hd() const { return hold_on(hold, geo_of(seg)); } // 持有期落在因子层上的几何
};

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

// ---- 可合并字段的逐字段运算 (Sum 字段表 SUM_HEAD_FIELDS / SUM_BODY_FIELDS 逐项展开; 数组逐元素递归) ----
FACTOR_HD inline void acc(int &a, int b) { a += b; }
FACTOR_HD inline void acc(double &a, double b) { a += b; }
FACTOR_HD inline void acc(MomAcc &a, const MomAcc &b) { a.merge(b); }
template <class T, size_t N>
FACTOR_HD inline void acc(T (&a)[N], const T (&b)[N]) {
  for (size_t i = 0; i < N; ++i)
    acc(a[i], b[i]);
}
inline void dif(int &a, int b) { a -= b; }
inline void dif(double &a, double b) { a -= b; }
inline void dif(MomAcc &a, const MomAcc &b) { a = a - b; }
template <class T, size_t N>
inline void dif(T (&a)[N], const T (&b)[N]) {
  for (size_t i = 0; i < N; ++i)
    dif(a[i], b[i]);
}

// ---- 二级 Sum: Row 的可合并和 (唯一累加单元); 字段表一份, merge / 前缀差 / 对拍 (Check.hpp) 逐项展开 ----
#define SUM_HEAD_FIELDS(X) X(rows) X(n_trig) X(ric) X(rac) X(ls) X(smk) X(smk2) X(slm) X(cap) X(tail) X(tail_n)
#define SUM_BODY_FIELDS(X) X(grp) X(cnt)
struct Sum {
  struct Head {               // 标量头 (GPU 批核 lane 0 攒)
    int rows = 0, n_trig = 0; // 攒过的全部行 / 触发行
    MomAcc ric, rac, ls;
    double smk = 0.0, smk2 = 0.0, slm = 0.0, cap = 0.0;          // mkt 幂和 + ls·mkt 交叉 (beta) / 满仓和
    double tail[kEnds][kTails] = {}, tail_n[kEnds][kTails] = {}; // 两端尾部的持仓收益和 / 真持有只数和
    FACTOR_HD void add(const Row &r, Ret ret) {
      ++rows;
      if (r.ok) {
        ric.add(r.ric);
        const float v = static_cast<float>(ls_row(r, ret));
        const double mk = r.mkt;
        ls.add(v);
        smk += mk, smk2 += mk * mk, slm += static_cast<double>(v) * mk, cap += r.cap;
        n_trig += r.cnt[end_group(kTop)] + r.cnt[end_group(kBot)] > 0;
        for (int e = 0; e < kEnds; ++e)
          for (int j = 0; j < kTails; ++j)
            tail[e][j] += static_cast<float>(tail_as(r, e, j, ret)), tail_n[e][j] += r.tail_n[e][j];
      }
      if (r.ok_rac)
        rac.add(r.rac);
    }
    FACTOR_HD void merge(const Head &o) {
#define SUM_ACC(f) acc(f, o.f);
      SUM_HEAD_FIELDS(SUM_ACC)
#undef SUM_ACC
    }
  };
  Head h;
  double grp[kGroups] = {}; // 组持仓收益和 (ok 行; GPU 批核 lane q 攒组 q)
  double cnt[kGroups] = {}; // 组真持有只数和 (ok 行)

  FACTOR_HD void add_grp(int k, float g, unsigned c) { grp[k] += g, cnt[k] += c; }
  // 行 r 按收益口径 ret 折进 (组 / 尾部 / 多空经 grp_as / tail_as / ls_row 改读; GPU 核 lane q 同式取组 q)
  FACTOR_HD void add(const Row &r, Ret ret) {
    h.add(r, ret);
    if (r.ok)
      for (int k = 0; k < kGroups; ++k)
        add_grp(k, static_cast<float>(grp_as(r, k, ret)), r.cnt[k]);
  }
  FACTOR_HD void merge(const Sum &o) {
    h.merge(o.h);
#define SUM_ACC(f) acc(f, o.f);
    SUM_BODY_FIELDS(SUM_ACC)
#undef SUM_ACC
  }
  Sum operator-(const Sum &o) const { // 前缀相减 = 窗
    Sum w = *this;
#define SUM_DIF_H(f) dif(w.h.f, o.h.f);
#define SUM_DIF(f) dif(w.f, o.f);
    SUM_HEAD_FIELDS(SUM_DIF_H)
    SUM_BODY_FIELDS(SUM_DIF)
#undef SUM_DIF_H
#undef SUM_DIF
    return w;
  }
  // 改读反向 (契约【方向 Dir】): rIC / 多空变号, 组倒序, 两端互换
  Sum reversed() const {
    Sum w = *this;
    w.h.ric = h.ric.negated(), w.h.ls = h.ls.negated(), w.h.slm = -h.slm;
    for (int j = 0; j < kTails; ++j)
      std::swap(w.h.tail[kTop][j], w.h.tail[kBot][j]), std::swap(w.h.tail_n[kTop][j], w.h.tail_n[kBot][j]);
    for (int k = 0; k < kGroups; ++k)
      w.grp[k] = grp[kGroups - 1 - k], w.cnt[k] = cnt[kGroups - 1 - k];
    return w;
  }

  // 读法 (每期量与三级同分母: 组 / 尾部 = Σ / 全部行 (不 ok 行记 0), 只数 = Σ / ok 行; 无 ok 行 = NaN)
  static constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
  int ok() const { return h.ric.n; }
  double grp_of(int k) const { return ok() ? grp[k] / h.rows : kNaN; }
  double tail_of(int e, int j) const { return h.rows ? h.tail[e][j] / h.rows : kNaN; }
  double held(int k) const { return ok() ? cnt[k] / ok() : kNaN; }
  double tail_held(int e, int j) const { return ok() ? h.tail_n[e][j] / ok() : kNaN; }
  double ls_of() const { return (grp_of(end_group(kTop)) - grp_of(end_group(kBot))) / 2.0; }
  double mono() const { return ok() ? mono_of(grp, kGroups) : kNaN; }
  double ric_mean() const { return ok() ? h.ric.mean() : kNaN; }
  double ric_t() const { // mean / sd (ddof 1) · √n
    const int n = ok();
    if (n < 2)
      return kNaN;
    const double m = h.ric.s1 / n, v = (h.ric.s2 - n * m * m) / (n - 1);
    return v > 0.0 ? m / std::sqrt(v) * std::sqrt(static_cast<double>(n)) : kNaN;
  }
  double ric_up() const { return ok() ? static_cast<double>(h.ric.npos) / ok() : kNaN; }

  // → 三级 (seg = 因子所在层的每段行数, 折算 / 年化; dl = 日净值 LS)
  HoldStat finish(HoldId hold, int seg, const Daily &dl) const {
    const int hr = hold_on(hold, geo_of(seg)).h;
    assert(hr >= 1);
    HoldStat s;
    s.seg = seg;
    s.hold = hold;
    s.n = h.ric.n;
    s.n_rac = h.rac.n;
    s.n_all = h.rows;
    s.n_trig = h.n_trig;
    s.ric = h.ric.finish(hr);
    s.rac = h.rac.finish(hr);
    s.ls = h.ls.finish(hr);
    if (s.n < 3)
      return s;
    s.ann = static_cast<float>(dl.ann());
    s.sharpe = static_cast<float>(dl.sharpe());
    double g[kGroups];
    for (int k = 0; k < kGroups; ++k)
      g[k] = grp[k] / static_cast<double>(s.n_all);
    const double nn = s.n, mmk = h.smk / nn;
    s.cap = static_cast<float>(h.cap / nn);
    const double vmk = std::fmax(h.smk2 - nn * mmk * mmk, 0.0);
    if (vmk > 0.0)
      s.beta = static_cast<float>((h.slm - h.ls.s1 * mmk) / vmk);
    for (int k = 0; k < kGroups; ++k)
      s.grp[k] = static_cast<float>(g[k]);
    s.mono = static_cast<float>(mono_of(g, kGroups));
    const double ne = cnt[end_group(kTop)] + cnt[end_group(kBot)];
    s.end_n = s.n_trig > 0 ? static_cast<float>(ne / (2.0 * s.n_trig)) : 0.f;
    return s;
  }
};
// 逐日 d 改读成方向 sign (±1; 正向原样)
inline std::vector<Sum> oriented(std::span<const Sum> d, int sign) {
  assert(sign == 1 || sign == -1);
  std::vector<Sum> o(d.begin(), d.end());
  if (sign < 0)
    for (Sum &x : o)
      x = x.reversed();
  return o;
}

// ---- 净值记日 (协议表 2【净值】【记日】唯一实现) ----
// 行 u (评估期行) 这一批的量 x → 期 (实现行 / stride) 累加 x / h̄; 实现行 = 平仓行 Hold::exit (exit, 收益) ⇄ 建仓行 (成本, 即付); 越过末期的不计
struct Realize {
  Hold hd;
  double span = 1.0; // h̄
  int stride = 1, T = 0;
  Realize() = default;
  Realize(HoldId h, int seg, int stride_, int T_) : hd(hold_on(h, geo_of(seg))), span(hd.span()), stride(stride_), T(T_) {
    assert(stride >= 1 && T % stride == 0 && T % seg == 0);
  }
  int periods() const { return T / stride; }
  // 行 u 的实现期 (越过末期 → −1)
  int period(int u, bool exit) const {
    const int v = exit ? hd.exit(u) : u;
    return v < T ? v / stride : -1;
  }
  template <class D>
  void add(int u, double x, bool exit, D *dst) const {
    if (const int p = period(u, exit); p >= 0)
      dst[p] += static_cast<D>(x / span);
  }
};
// ---- 期线 (钱的唯一载体: 因子组合账 / 篮子账 / 个股账 / Beta 选池账 / 净值探针 / 因子账集流水同一份) ----
// 期收益 / 序列矩阵 [n][P], 线 i 点 p
struct LedgerLines {
  int n = 0, P = 0;
  std::vector<float> v;
  void reset(int n_, int P_, float fill = 0.f) {
    n = n_, P = P_;
    v.assign(static_cast<size_t>(n) * static_cast<size_t>(P), fill);
  }
  float *at(int i) { return v.data() + static_cast<size_t>(i) * static_cast<size_t>(P); }
  const float *at(int i) const { return v.data() + static_cast<size_t>(i) * static_cast<size_t>(P); }
  LedgerLines slice(int i0, int m) const {
    assert(i0 >= 0 && m >= 0 && i0 + m <= n);
    LedgerLines o;
    o.n = m, o.P = P;
    o.v.assign(at(i0), at(i0) + static_cast<size_t>(m) * static_cast<size_t>(P));
    return o;
  }
};

// 期线的写端: 线 i 的记日 = 持有期 rz[r] (Realize, 协议表 2【净值】【记日】唯一实现) × 记在平仓行 (收益) ⇄ 建仓行 (流水 / 成本, 即付);
//   add(i, u, x) 把评估期行 u 这一批的量直落期线 (一期只收一日开的批: 按日切线程无竞争)
struct Lines {
  struct Line {
    uint8_t r = 0;     // rz 下标
    bool exit = false; // 记在平仓行
  };
  std::vector<Realize> rz; // [持有期]
  std::vector<Line> ln;    // [线]
  LedgerLines out;         // [线][期]
  // n 条线, 线 i 的持有期 hold_of(i) / 记在平仓行 exit_of(i); seg = 层每日行数, stride = 每几行并一期, T = 评估期行数
  template <class HoldOf, class ExitOf>
  void reset(int n, int seg, int stride, int T, HoldOf &&hold_of, ExitOf &&exit_of) {
    assert(n >= 1);
    rz.clear(), ln.assign(static_cast<size_t>(n), {});
    for (int i = 0; i < n; ++i) {
      const HoldId h = hold_of(i);
      size_t r = 0;
      while (r < rz.size() && rz[r].hd.id != h)
        ++r;
      if (r == rz.size())
        rz.emplace_back(h, seg, stride, T);
      ln[static_cast<size_t>(i)] = {static_cast<uint8_t>(r), exit_of(i)};
    }
    out.reset(n, rz[0].periods());
  }
  void add(int i, int u, double x) {
    const Line &l = ln[static_cast<size_t>(i)];
    rz[l.r].add(u, x, l.exit, out.at(i));
  }
};

// 一组账线 (每本一条): 收益线 ret (平仓行实现) | 流水 TurnFlow 各字段线 flow (define/Fee.hpp 字段表, 建仓行即付; 没装的字段空 n = 0);
//   净 = 毛 − Fee::cost(该期流水, 口径段) (费率现乘, 唯一一式)
struct BookLines {
  LedgerLines ret;
  LedgerLines flow[kFlowFields];
  // 由 Lines 的 [字段 · B + 本] 排布取出 (字段 0 = 收益, 1 + f = 流水字段 f < nf)
  void take(const LedgerLines &all, int B, int nf) {
    ret = all.slice(0, B);
    for (int f = 0; f < nf; ++f)
      flow[f] = all.slice((1 + f) * B, B);
  }
  TurnFlow flow_at(int i, int p) const {
    TurnFlow o;
    for (int f = 0; f < kFlowFields; ++f)
      o[f] = flow[f].n ? flow[f].at(i)[p] : 0.0;
    return o;
  }
  double net(int i, int p, const Fee &fee, AmtId a) const { return ret.at(i)[p] - fee.cost(flow_at(i, p), a); }
  size_t bytes() const {
    size_t n = ret.v.size();
    for (const LedgerLines &c : flow)
      n += c.v.size();
    return n * sizeof(float);
  }
};

// 逐日 d → 日净值 LS 的逐日收益: 建仓日 i 的 Σ ls_row / h̄ 记到平仓日 i + hold_days (Hold::money_lag; 序列首几日为 0, 越过末日的不计)
inline std::vector<double> ls_days(std::span<const Sum> d, HoldId hold, int seg) {
  const Hold hd = hold_on(hold, geo_of(seg));
  const size_t D = d.size(), lag = static_cast<size_t>(hd.money_lag());
  const double span = hd.span();
  std::vector<double> o(D, 0.0);
  for (size_t i = 0; i + lag < D; ++i)
    o[i + lag] = d[i].h.ls.s1 / span;
  return o;
}
inline Daily ls_daily(std::span<const Sum> d, HoldId hold, int seg) {
  Daily dl;
  for (const double r : ls_days(d, hold, seg))
    dl.add(r);
  return dl;
}
// 逐日 d (一个日段) → 三级 HoldStat: 日段之和 finish + 段内日净值 LS
inline HoldStat stat_of(std::span<const Sum> d, HoldId hold, int seg) {
  Sum s;
  for (const Sum &x : d)
    s.merge(x);
  return s.finish(hold, seg, ls_daily(d, hold, seg));
}

// ---- 派生量 ----
// 每年满仓轮数 = kDaysPerYear · S / h̄: 金额口径的年化 (均值超额 / 均摊成本 / 换手 = 满仓一年) 走这一个
inline double span_per_year(HoldId h, int seg) { return static_cast<double>(kDaysPerYear) * seg / hold_on(h, geo_of(seg)).span(); }
inline double span_per_year(const HoldStat &s) { return static_cast<double>(kDaysPerYear) * s.seg / s.hd().span(); }
// LS 每期真实收益
inline double ls_period(const HoldStat &s) { return s.n_all > 0 ? static_cast<double>(s.ls.mean) * s.n / s.n_all : 0.0; }
// 端 e 的每期真实收益 (与 ls_period 同分母)
inline double end_period(const HoldStat &s, int e) { return s.grp[end_group(e)]; }
// 触发率 = 触发行 / ok 行 (诊断)
inline double trigger_of(const HoldStat &s) { return s.n > 0 ? static_cast<double>(s.n_trig) / s.n : 0.0; }
// 两端利用率 = 平均每触发行一端成员数 / 平均满仓, 不封顶 (诊断)
inline double util_of(const HoldStat &s) { return s.cap > 0.f ? static_cast<double>(s.end_n) / s.cap : 0.0; }

} // namespace factor::stat
