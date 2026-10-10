#pragma once

// =============================================================================
// TS 流式实现 (实盘: 每分钟一次 push, O(1) 或 O(d) 状态; 语义契约见 factor/Contract.hpp)
// =============================================================================
//   结构 = 两节, 对应 OpTable 的 T 窗列 (每个 struct 带 static constexpr T kT, op_check 对表):
//     节一  POINT             无状态纯函数   struct Op : Point { static Val apply(Val x[, Val y[, Val z]], const Param &); }
//                             TodMask 元数 0: apply(int t_seg, const Param &)
//     节二  EXPAND/ROLL/EXPO  有状态          struct Op { explicit Op(const Param &); Val push(Val x[, Val y]); }
//           EXPAND 推满 kSegLen 自动归零 (也可 reset() 手动); ROLL / EXPO 没有 reset —— 契约说它们跨段不重置.
//
//   节二只写一份公式: **统计核 core::* × 窗适配 Expand/Roll/Ema**.
//   命名 Ts<核><窗>: *Cum 与 *Roll 就是同一个核挂在两个窗上 (见文件末尾的实例化清单),
//   所以不存在"同一统计量两处实现会漂移"的问题.
//
//   核接口 (窗适配器驱动):
//     void reset(const Param &);        窗重置时调, 参数在此落到核里
//     void add(int i, Val x, Val y);    喂入一个样本, i = 样本在窗/段内的位置 (0 = 最旧)
//     Val  get(const Ctx &) const;      取当前输出
//   ROLL 每步 **reset + 整窗重放** (未满窗时重放已有前缀, 位置锚定最新格 = d−1): d ≤ 240 且每分钟只走一次,
//   代价可忽略, 换来零递推漂移 (Welford 在滑出样本时做减法会在长序列上失稳, 不值得).
//   退化按契约第 6 条给中性值 (全并列用核内追踪的 lo/hi 精确判, 相消用 den_ok); 掩码只随输入格 (第 7 条).
//
//   【precise-math】依赖受控浮点, 编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/StreamHist.hpp"

#include <algorithm>
#include <cmath>
#include <vector>

namespace factor::ts {

// =========================== 节一: POINT (无状态) ===========================

struct Point {
  static constexpr T kT = T::POINT;
};

struct TsAbs : Point {
  static Val apply(Val x, const Param &) { return mk(std::fabs(x.v), x.m); }
};
struct TsSign : Point {
  static Val apply(Val x, const Param &) { return mk(x.v > 0.f ? 1.f : (x.v < 0.f ? -1.f : 0.f), x.m); }
};
struct TsLog : Point { // 保号 log1p: 在 0 附近线性, 尾部对数, 不需要输入为正
  static Val apply(Val x, const Param &) { return mk(std::copysign(std::log1p(std::fabs(x.v)), x.v), x.m); }
};
struct TsSqrt : Point {
  static Val apply(Val x, const Param &) { return mk(std::copysign(std::sqrt(std::fabs(x.v)), x.v), x.m); }
};
struct TsRelu : Point {
  static Val apply(Val x, const Param &) { return mk(std::fmax(0.f, x.v), x.m); }
};
struct TsRecip : Point { // x = 0 → 0; 溢出由 mk 转无效
  static Val apply(Val x, const Param &) { return mk(x.v != 0.f ? 1.f / x.v : 0.f, x.m); }
};
struct TsClip : Point {
  static Val apply(Val x, const Param &p) {
    assert(p.k >= 0.f);
    return mk(std::clamp(x.v, -p.k, p.k), x.m);
  }
};
struct TsGt : Point { // 指示: 严格大于. 套 Sum 窗 = 计数, 套 Mean 窗 = 占比
  static Val apply(Val x, const Param &p) { return mk(x.v > p.k ? 1.f : 0.f, x.m); }
};
struct TsTodMask : Point { // 元数 0: 只看段内位置, 恒有效
  static Val apply(int t_seg, const Param &p) {
    return mk((t_seg >= static_cast<int>(p.k) && t_seg < static_cast<int>(p.k2)) ? 1.f : 0.f, true);
  }
};

struct TsAdd : Point {
  static Val apply(Val x, Val y, const Param &) { return mk(x.v + y.v, x.m && y.m); }
};
struct TsSub : Point {
  static Val apply(Val x, Val y, const Param &) { return mk(x.v - y.v, x.m && y.m); }
};
struct TsMul : Point {
  static Val apply(Val x, Val y, const Param &) { return mk(x.v * y.v, x.m && y.m); }
};
struct TsDiv : Point { // y = 0 → 0; 溢出由 mk 转无效
  static Val apply(Val x, Val y, const Param &) { return mk(y.v != 0.f ? x.v / y.v : 0.f, x.m && y.m); }
};
struct TsMax : Point {
  static Val apply(Val x, Val y, const Param &) { return mk(std::fmax(x.v, y.v), x.m && y.m); }
};
struct TsMin : Point {
  static Val apply(Val x, Val y, const Param &) { return mk(std::fmin(x.v, y.v), x.m && y.m); }
};
struct TsImb : Point { // 和在两侧量级下相消 (双边皆 0 / x ≈ −y) → 0 (无失衡)
  static Val apply(Val x, Val y, const Param &) {
    const bool ok = den_ok(static_cast<double>(x.v) + y.v, std::fabs(x.v) + std::fabs(y.v));
    return mk(ok ? (x.v - y.v) / (x.v + y.v) : 0.f, x.m && y.m);
  }
};
struct TsShare : Point { // 同上 → 0.5 (对半)
  static Val apply(Val x, Val y, const Param &) {
    const bool ok = den_ok(static_cast<double>(x.v) + y.v, std::fabs(x.v) + std::fabs(y.v));
    return mk(ok ? x.v / (x.v + y.v) : 0.5f, x.m && y.m);
  }
};
struct TsLogRatio : Point { // ln(clamp(x/y, ε, 1/ε)): 非正侧饱和到 ∓kLogCap (见 log_ratio)
  static Val apply(Val x, Val y, const Param &) { return mk(log_ratio(x.v, y.v), x.m && y.m); }
};
struct TsMask : Point { // y 当掩码用, 不取值; y ≤ 0 → 无效 (不是补 0), 让下游窗只吃子集
  static Val apply(Val x, Val y, const Param &) { return mk(x.v, x.m && y.m && y.v > 0.f); }
};

struct TsWhere : Point { // x 只当条件用, 不取值; 未被选中的那支不要求有效
  static Val apply(Val x, Val y, Val z, const Param &) {
    const bool pick_y = x.v > 0.f;
    return mk(pick_y ? y.v : z.v, x.m && (pick_y ? y.m : z.m));
  }
};

// =========================== 节二: 有窗 ===========================

// 取值上下文: 窗适配器在 get 时提供 (核自己不知道挂在哪个窗上)
struct Ctx {
  int d = 0; // 窗跨度: ROLL = 窗长, EXPAND = 段内已推进期数. 位置一律报"距今期数" d−1−i
  Val x, y;  // 当前点 (相对型核用)
  Param p;
};

namespace core {

// ---- 一元累加器: 一阶 (Sum/Mean 不该为了取个和去跑四阶 Welford) ----
struct SumAcc {
  double s = 0;
  int n = 0;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val) {
    if (!x.m)
      return;
    s += x.v, ++n;
  }
};

struct Sum : SumAcc { // 空和 = 0
  Val get(const Ctx &) const { return mk(s, true); }
};
struct Mean : SumAcc {
  Val get(const Ctx &) const { return mk(n >= 1 ? s / n : 0.0, true); }
};

// ---- 一元累加器: 四阶 (Welford / Terriberry 递推; lo/hi 只给全并列判据) ----
struct MomAcc {
  double n = 0, mean = 0, m2 = 0, m3 = 0, m4 = 0;
  float lo = 0, hi = 0;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val) {
    if (!x.m)
      return;
    const double v = x.v, n1 = n;
    lo = n1 == 0 ? x.v : std::fmin(lo, x.v);
    hi = n1 == 0 ? x.v : std::fmax(hi, x.v);
    n += 1;
    const double delta = v - mean, dn = delta / n, dn2 = dn * dn, term = delta * dn * n1;
    mean += dn;
    m4 += term * dn2 * (n * n - 3 * n + 3) + 6 * dn2 * m2 - 4 * dn * m3;
    m3 += term * dn * (n - 2) - 3 * dn * m2;
    m2 += term;
  }
  bool disp() const { return spread(lo, hi); }
  bool ok(int need) const { return n >= need && disp(); } // 退化 (样本不足 / 全并列) → 中性值 0
};
struct Var : MomAcc {
  Val get(const Ctx &) const { return mk(ok(2) ? m2 / (n - 1) : 0.0, true); }
};
struct Std : MomAcc {
  Val get(const Ctx &) const { return mk(ok(2) ? std::sqrt(m2 / (n - 1)) : 0.0, true); }
};
struct Skew : MomAcc { // 总体矩: m3/n / (m2/n)^1.5 = √n·m3/m2^1.5
  Val get(const Ctx &) const { return mk(ok(3) ? std::sqrt(n) * m3 / std::pow(m2, 1.5) : 0.0, true); }
};
struct Kurt : MomAcc { // 超额峰度
  Val get(const Ctx &) const { return mk(ok(4) ? n * m4 / (m2 * m2) - 3.0 : 0.0, true); }
};
struct Z : MomAcc { // 相对型: 描述当前点; σ 退化 → 0 (x = μ)
  Val get(const Ctx &c) const {
    return mk(ok(2) ? (c.x.v - mean) / std::sqrt(m2 / (n - 1)) : 0.0, c.x.m);
  }
};

// ---- 极值 / 极值位置 (严格比较 → 并列保留最旧) ----
template <bool IsMax, bool IsArg>
struct Extreme {
  double best = 0;
  int bi = 0, n = 0;
  void reset(const Param &) { *this = {}; }
  void add(int i, Val x, Val) {
    if (!x.m)
      return;
    if (n == 0 || (IsMax ? x.v > best : x.v < best)) {
      best = x.v;
      bi = i;
    }
    ++n;
  }
  Val get(const Ctx &c) const {
    const double v = IsArg ? c.d - 1 - bi : best; // Arg: 距今期数 (两个窗同口径)
    return mk(n >= 1 ? v : 0.0, true);
  }
};

// ---- 集中度 / 分布形状 ----
struct Hhi { // Σx²/(Σx)²: 值全集中在一点 → 1, 均匀摊在 n 点 → 1/n
  double s = 0, sx2 = 0;
  int n = 0;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val) {
    if (!x.m)
      return;
    s += x.v, sx2 += static_cast<double>(x.v) * x.v, ++n;
  }
  // Σx 相消 (非负域下即全 0) → 1/n: 全体相等时 Σx²/(Σx)² = 1/n, 取这个极限; 空窗 → 0
  Val get(const Ctx &) const { return mk(n < 1 ? 0.0 : (den_ok(s * s, n * sx2) ? sx2 / (s * s) : 1.0 / n), true); }
};

struct Entropy { // 只对正值定义: ln S − Σ x ln x / S; 无正样本 → 0 (单样本熵 = 0 的极限)
  double s = 0, sxlnx = 0;
  int npos = 0;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val) {
    if (!x.m || x.v <= 0.f)
      return;
    s += x.v, sxlnx += static_cast<double>(x.v) * std::log(x.v), ++npos;
  }
  Val get(const Ctx &) const { return mk(npos >= 1 ? std::log(s) - sxlnx / s : 0.0, true); }
};

struct Product { // Π(1+x) − 1 精确: 对数域累 ln|1+x|, 零因子 (x = −1) 计数, 负因子 (x < −1) 计奇偶; 空窗 = 空积 − 1 = 0
  double sl = 0;
  int zero = 0, neg = 0;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val) {
    if (!x.m)
      return;
    if (x.v > -1.f)
      sl += std::log1p(x.v);
    else if (x.v < -1.f)
      sl += std::log(-1.0 - x.v), ++neg;
    else
      ++zero;
  }
  Val get(const Ctx &) const { return mk(zero > 0 ? -1.0 : (neg & 1 ? -std::exp(sl) - 1.0 : std::expm1(sl)), true); }
};

struct Wma { // 线性权 w = i+1 (最旧 1 … 最新 d), 只在有效点上累权; n ≥ 1 ⇒ Σw ≥ 1; 空窗 → 0
  double sw = 0, swx = 0;
  int n = 0;
  void reset(const Param &) { *this = {}; }
  void add(int i, Val x, Val) {
    if (!x.m)
      return;
    const double w = i + 1;
    sw += w, swx += w * x.v, ++n;
  }
  Val get(const Ctx &) const { return mk(n >= 1 ? swx / sw : 0.0, true); }
};

struct Slope { // x 对窗内下标 i 的 OLS 斜率; 下标两两不同 ⇒ n ≥ 2 时 Σ(i−ī)² ≥ 1/2; n < 2 → 0 (无趋势)
  double si = 0, sii = 0, sx = 0, six = 0;
  int n = 0;
  void reset(const Param &) { *this = {}; }
  void add(int i, Val x, Val) {
    if (!x.m)
      return;
    const double t = i;
    si += t, sii += t * t, sx += x.v, six += t * x.v, ++n;
  }
  Val get(const Ctx &) const { return mk(n >= 2 ? (six - si * sx / n) / (sii - si * si / n) : 0.0, true); }
};

struct Oldest { // 窗最旧一格 (TsDelayRoll: 窗长 d+1 → 最旧格即 x_{t−d}; 未满窗时即 x_0, 滞后夹到序列头)
  Val v0;
  bool has = false;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val) {
    v0 = has ? v0 : x;
    has = true;
  }
  Val get(const Ctx &) const { return mk(v0.v, v0.m); }
};

struct Diff : Oldest { // 最新 − 最旧
  Val get(const Ctx &c) const { return mk(static_cast<double>(c.x.v) - v0.v, v0.m && c.x.m); }
};

// ---- 序统计族: 窗/段内样本缓存 + 分桶 (规则见 stream::Hist) ----
using stream::Hist;

// 样本缓存基类: 窗/段内所有有效值
struct SampleBase {
  std::vector<float> s;
  Param p;
  void reset(const Param &pp) { s.clear(), p = pp; }
  void add(int, Val x, Val) {
    if (x.m)
      s.push_back(x.v);
  }
};

struct Rank : SampleBase { // 相对型: x_t 有效 ⇒ 样本非空; 全并列 → 0.5 (Hist 内)
  Val get(const Ctx &c) const {
    Hist h;
    h.build(s);
    return mk(h.rank(c.x.v), c.x.m);
  }
};
struct Quantile : SampleBase { // k 分位 (k = 0.5 即中位); 空窗 → 0
  Val get(const Ctx &) const {
    Hist h;
    h.build(s);
    return mk(s.empty() ? 0.f : h.quantile(p.k), true);
  }
};

// ---- 二元累加器 (Welford 协方差; 双有效样本上的 x/y 极值给全并列判据) ----
struct CoAcc {
  double n = 0, mx = 0, my = 0, cxx = 0, cyy = 0, cxy = 0;
  float lox = 0, hix = 0, loy = 0, hiy = 0;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val y) {
    if (!x.m || !y.m)
      return;
    const bool first = n == 0;
    lox = first ? x.v : std::fmin(lox, x.v), hix = first ? x.v : std::fmax(hix, x.v);
    loy = first ? y.v : std::fmin(loy, y.v), hiy = first ? y.v : std::fmax(hiy, y.v);
    n += 1;
    const double dx = x.v - mx, dy = y.v - my;
    mx += dx / n, my += dy / n;
    cxx += dx * (x.v - mx), cyy += dy * (y.v - my), cxy += dx * (y.v - my);
  }
  bool dx_ok() const { return spread(lox, hix); }
  bool dy_ok() const { return spread(loy, hiy); }
};

struct Cov : CoAcc { // n < 2 → 0
  Val get(const Ctx &) const { return mk(n >= 2 ? cxy / (n - 1) : 0.0, true); }
};
struct Corr : CoAcc { // 任一侧无离散度 → 0 (无关)
  Val get(const Ctx &) const { return mk(n >= 2 && dx_ok() && dy_ok() ? cxy / std::sqrt(cxx * cyy) : 0.0, true); }
};
struct Beta : CoAcc { // x 对 y 的 OLS 斜率; y 无离散度 → 0
  Val get(const Ctx &) const { return mk(n >= 2 && dy_ok() ? cxy / cyy : 0.0, true); }
};
struct Resid : CoAcc { // 相对型: 当前点对回归线的偏离; y 无离散度 → β = 0, 即 x − μ^x (当前点有效 ⇒ n ≥ 1)
  Val get(const Ctx &c) const {
    const double b = n >= 2 && dy_ok() ? cxy / cyy : 0.0;
    return mk((c.x.v - mx) - b * (c.y.v - my), c.x.m && c.y.m);
  }
};

struct WMean { // y 为权; Σy 相消 → 等权均值 Σx/n (权全相等的极限); 空窗 → 0
  double sx = 0, sy = 0, syx = 0, say = 0;
  int n = 0;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val y) {
    if (!x.m || !y.m)
      return;
    sx += x.v, sy += y.v, syx += static_cast<double>(y.v) * x.v, say += std::fabs(y.v), ++n;
  }
  Val get(const Ctx &) const { return mk(n < 1 ? 0.0 : (den_ok(sy, say) ? syx / sy : sx / n), true); }
};

} // namespace core

// ---- 窗: EXPAND (段内 expanding; 推满 kSegLen 自动归零, 段首也可手动 reset) ----
template <class C>
class Expand {
public:
  static constexpr T kT = T::EXPAND;
  explicit Expand(const Param &p) : p_(p) { reset(); }
  void reset() { c_.reset(p_), i_ = 0; }
  Val push(Val x) { return step(x, Val{}); }
  Val push(Val x, Val y) { return step(x, y); }

private:
  Val step(Val x, Val y) {
    if (i_ == kSegLen)
      reset();
    c_.add(i_, x, y);
    ++i_;
    return c_.get(Ctx{i_, x, y, p_});
  }
  Param p_;
  C c_;
  int i_ = 0;
};

// ---- 窗: ROLL (最近 d+Extra 期, 跨段不重置 → 没有 reset; 每步整窗重放) ----
//   Extra = 1 给 SHIFT 核 (Delay / Delta 要窗内最旧那格, 即 x_{t−d}, 所以窗多留一格)
//   未满窗 (契约: 窗 = 已有前缀) 重放已有的 n_ 格, 位置 i 锚定最新格 = L_−1 (与 Cpu / Gpu 的 Wma 权、Arg 距今同口径)
template <class C, int Extra = 0>
class Roll {
public:
  static constexpr T kT = T::ROLL;
  explicit Roll(const Param &p) : p_(p), L_(p.d + Extra), bx_(static_cast<size_t>(L_)), by_(static_cast<size_t>(L_)) {
    assert(p.d >= 1);
  }
  Val push(Val x) { return step(x, Val{}); }
  Val push(Val x, Val y) { return step(x, y); }

private:
  Val step(Val x, Val y) {
    bx_[head_] = x, by_[head_] = y;
    if (++head_ == L_)
      head_ = 0;
    n_ = std::min(n_ + 1, L_);
    c_.reset(p_);
    int j = n_ < L_ ? 0 : head_; // 未满: 槽 0 起即最旧; 满窗: head_ 处最旧
    for (int i = L_ - n_; i < L_; ++i) {
      c_.add(i, bx_[j], by_[j]);
      if (++j == L_)
        j = 0;
    }
    return c_.get(Ctx{p_.d, x, y, p_});
  }
  Param p_;
  int L_, head_ = 0, n_ = 0;
  std::vector<Val> bx_, by_;
  C c_;
};

// ---- 窗: EXPO (指数递推, 全程不重置 → 没有 reset; 核与窗一体, 只服务均值) ----
class Ema {
public:
  static constexpr T kT = T::EXPO;
  explicit Ema(const Param &p) : k_(p.k) { assert(k_ > 0.f && k_ <= 1.f); }
  Val push(Val x) { // x 无效 → 状态保持; 尚无有效样本 → 0
    if (x.m)
      y_ = on_ ? k_ * x.v + (1.0 - k_) * y_ : x.v, on_ = true;
    return mk(y_, true);
  }

private:
  double k_, y_ = 0.0;
  bool on_ = false;
};

// =========================== 实例化: 核 × 窗 ===========================
//   Ts<核>Cum / Ts<核>Roll = 同一个核挂两个窗; 公式只有核那一份. 此处按核分组, 同核不同窗相邻
//   (全局 idx 序只归 OpTable 管, 实现文件不跟着排).
//   Delay/Delta 要看 d 期之前那一格 → 窗长 d+1

using TsDelayRoll = Roll<core::Oldest, 1>;
using TsDeltaRoll = Roll<core::Diff, 1>;
using TsSumCum = Expand<core::Sum>;
using TsSumRoll = Roll<core::Sum>;
using TsMeanCum = Expand<core::Mean>;
using TsMeanRoll = Roll<core::Mean>;
using TsMeanEma = Ema;
using TsVarCum = Expand<core::Var>;
using TsVarRoll = Roll<core::Var>;
using TsStdCum = Expand<core::Std>;
using TsStdRoll = Roll<core::Std>;
using TsSkewCum = Expand<core::Skew>;
using TsSkewRoll = Roll<core::Skew>;
using TsKurtCum = Expand<core::Kurt>;
using TsKurtRoll = Roll<core::Kurt>;
using TsMaxCum = Expand<core::Extreme<true, false>>;
using TsMaxRoll = Roll<core::Extreme<true, false>>;
using TsMinCum = Expand<core::Extreme<false, false>>;
using TsMinRoll = Roll<core::Extreme<false, false>>;
using TsArgMaxCum = Expand<core::Extreme<true, true>>;
using TsArgMaxRoll = Roll<core::Extreme<true, true>>;
using TsArgMinCum = Expand<core::Extreme<false, true>>;
using TsArgMinRoll = Roll<core::Extreme<false, true>>;
using TsRankCum = Expand<core::Rank>;
using TsRankRoll = Roll<core::Rank>;
using TsQuantileRoll = Roll<core::Quantile>;
using TsZRoll = Roll<core::Z>;
using TsWmaRoll = Roll<core::Wma>;
using TsProductRoll = Roll<core::Product>;
using TsSlopeRoll = Roll<core::Slope>;
using TsHhiCum = Expand<core::Hhi>;
using TsEntropyCum = Expand<core::Entropy>;
using TsCovCum = Expand<core::Cov>;
using TsCovRoll = Roll<core::Cov>;
using TsCorrCum = Expand<core::Corr>;
using TsCorrRoll = Roll<core::Corr>;
using TsBetaCum = Expand<core::Beta>;
using TsBetaRoll = Roll<core::Beta>;
using TsResidCum = Expand<core::Resid>;
using TsResidRoll = Roll<core::Resid>;
using TsWMeanCum = Expand<core::WMean>;
using TsWMeanRoll = Roll<core::WMean>;

} // namespace factor::ts
