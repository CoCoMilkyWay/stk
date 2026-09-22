#pragma once

// =============================================================================
// TS 流式实现 (实盘: 每分钟一次 push, O(1) 或 O(d) 状态; 语义契约见 factor/Contract.hpp)
// =============================================================================
//   结构 = 两节, 对应 OpTable 的"窗"列 (每个 struct 带 static constexpr Win kWin, op_check 对表):
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
//   ROLL 窗满后 **reset + 整窗重放**: d ≤ 240 且每分钟只走一次, 代价可忽略, 换来零递推漂移
//   (Welford 在滑出样本时做减法会在长序列上失稳, 不值得).
//   退化判据按契约: 全并列用核内追踪的 lo/hi (精确), 相消用 den_ok; 分母不钳位.
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
  static constexpr Win kWin = Win::POINT;
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
struct TsAsinh : Point {
  static Val apply(Val x, const Param &) { return mk(std::asinh(x.v), x.m); }
};
struct TsTanh : Point {
  static Val apply(Val x, const Param &) { return mk(std::tanh(x.v), x.m); }
};
struct TsSqrt : Point {
  static Val apply(Val x, const Param &) { return mk(std::copysign(std::sqrt(std::fabs(x.v)), x.v), x.m); }
};
struct TsRelu : Point {
  static Val apply(Val x, const Param &) { return mk(std::fmax(0.f, x.v), x.m); }
};
struct TsRecip : Point { // x = 0 退化; 溢出由 mk 转无效
  static Val apply(Val x, const Param &) { return mk(1.f / x.v, x.m && x.v != 0.f); }
};
struct TsSignedPow : Point {
  static Val apply(Val x, const Param &p) { return mk(std::copysign(std::pow(std::fabs(x.v), p.k), x.v), x.m); }
};
struct TsClip : Point {
  static Val apply(Val x, const Param &p) {
    assert(p.k >= 0.f);
    return mk(std::clamp(x.v, -p.k, p.k), x.m);
  }
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
struct TsDiv : Point { // y = 0 退化; 溢出由 mk 转无效
  static Val apply(Val x, Val y, const Param &) { return mk(x.v / y.v, x.m && y.m && y.v != 0.f); }
};
struct TsMax : Point {
  static Val apply(Val x, Val y, const Param &) { return mk(std::fmax(x.v, y.v), x.m && y.m); }
};
struct TsMin : Point {
  static Val apply(Val x, Val y, const Param &) { return mk(std::fmin(x.v, y.v), x.m && y.m); }
};
struct TsImb : Point { // 和在两侧量级下相消 → 无效 (x ≈ −y 时比值无意义)
  static Val apply(Val x, Val y, const Param &) {
    const bool ok = x.m && y.m && den_ok(static_cast<double>(x.v) + y.v, std::fabs(x.v) + std::fabs(y.v));
    return mk((x.v - y.v) / (x.v + y.v), ok);
  }
};
struct TsShare : Point {
  static Val apply(Val x, Val y, const Param &) {
    const bool ok = x.m && y.m && den_ok(static_cast<double>(x.v) + y.v, std::fabs(x.v) + std::fabs(y.v));
    return mk(x.v / (x.v + y.v), ok);
  }
};
struct TsLogRatio : Point { // ln x − ln y: 两个正有限数各取对数再相减, 不会溢出
  static Val apply(Val x, Val y, const Param &) {
    const bool ok = x.m && y.m && x.v > 0.f && y.v > 0.f;
    return mk(std::log(x.v) - std::log(y.v), ok);
  }
};

struct TsWhere : Point { // x 只当条件用, 不取值; 未被选中的那支不要求有效
  static Val apply(Val x, Val y, Val z, const Param &) {
    const bool pick_y = x.v > 0.f;
    return mk(pick_y ? y.v : z.v, x.m && (pick_y ? y.m : z.m));
  }
};
struct TsClip3 : Point { // 上下界颠倒 (y > z) → 无效 (fmin/fmax 组合对颠倒的界无 UB, std::clamp 有)
  static Val apply(Val x, Val y, Val z, const Param &) {
    return mk(std::fmin(std::fmax(x.v, y.v), z.v), x.m && y.m && z.m && y.v <= z.v);
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

struct Sum : SumAcc {
  Val get(const Ctx &) const { return mk(s, n >= 1); }
};
struct Mean : SumAcc {
  Val get(const Ctx &) const { return mk(s / n, n >= 1); }
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
};
struct Var : MomAcc {
  Val get(const Ctx &) const { return mk(m2 / (n - 1), n >= 2 && disp()); }
};
struct Std : MomAcc {
  Val get(const Ctx &) const { return mk(std::sqrt(m2 / (n - 1)), n >= 2 && disp()); }
};
struct Skew : MomAcc { // 总体矩: m3/n / (m2/n)^1.5 = √n·m3/m2^1.5
  Val get(const Ctx &) const { return mk(std::sqrt(n) * m3 / std::pow(m2, 1.5), n >= 3 && disp()); }
};
struct Kurt : MomAcc { // 超额峰度
  Val get(const Ctx &) const { return mk(n * m4 / (m2 * m2) - 3.0, n >= 4 && disp()); }
};
struct Z : MomAcc { // 相对型: 描述当前点
  Val get(const Ctx &c) const {
    return mk((c.x.v - mean) / std::sqrt(m2 / (n - 1)), n >= 2 && disp() && c.x.m);
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
    return mk(v, n >= 1);
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
  Val get(const Ctx &) const { return mk(sx2 / (s * s), n >= 1 && den_ok(s * s, n * sx2)); }
};

struct Entropy { // 只对正值定义: ln S − Σ x ln x / S; 有正样本即有定义 (S > 0)
  double s = 0, sxlnx = 0;
  int npos = 0;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val) {
    if (!x.m || x.v <= 0.f)
      return;
    s += x.v, sxlnx += static_cast<double>(x.v) * std::log(x.v), ++npos;
  }
  Val get(const Ctx &) const { return mk(std::log(s) - sxlnx / s, npos >= 1); }
};

struct CountGt {
  int cnt = 0, n = 0;
  Param p;
  void reset(const Param &pp) { *this = {}, p = pp; }
  void add(int, Val x, Val) {
    if (!x.m)
      return;
    cnt += x.v > p.k, ++n;
  }
  Val get(const Ctx &) const { return mk(cnt, n >= 1); }
};

// ---- 局部峰计数: 峰在 s 处成立需三点相邻有效且 x_s > k·mean_{≤s}; 在 s+1 时刻确认计入 ----
struct Peaks {
  Val p2, p1;       // s−2, s−1 两点
  double mean1 = 0; // p1 时刻的段内 expanding 均值
  double s = 0;
  int n = 0, cnt = 0;
  Param p;
  void reset(const Param &pp) { *this = {}, p = pp; }
  void add(int, Val x, Val) {
    if (x.m)
      s += x.v, ++n;
    const double mean_cur = n >= 1 ? s / n : 0.0;
    if (p2.m && p1.m && x.m && p2.v < p1.v && p1.v > x.v && p1.v > p.k * mean1)
      ++cnt;
    p2 = p1, p1 = x, mean1 = mean_cur;
  }
  Val get(const Ctx &) const { return mk(cnt, n >= 1); }
};

struct Product { // Π(1+x) − 1, 走对数域避免连乘溢出; 任一 1+x ≤ 0 (x ≤ −1) → 无定义
  double sl = 0;
  int n = 0;
  bool bad = false;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val) {
    if (!x.m)
      return;
    if (x.v > -1.f)
      sl += std::log1p(x.v);
    else
      bad = true;
    ++n;
  }
  Val get(const Ctx &) const { return mk(std::expm1(sl), n >= 1 && !bad); }
};

struct Wma { // 线性权 w = i+1 (最旧 1 … 最新 d), 只在有效点上累权; n ≥ 1 ⇒ Σw ≥ 1
  double sw = 0, swx = 0;
  int n = 0;
  void reset(const Param &) { *this = {}; }
  void add(int i, Val x, Val) {
    if (!x.m)
      return;
    const double w = i + 1;
    sw += w, swx += w * x.v, ++n;
  }
  Val get(const Ctx &) const { return mk(swx / sw, n >= 1); }
};

struct Slope { // x 对窗内下标 i 的 OLS 斜率; 下标两两不同 ⇒ n ≥ 2 时 Σ(i−ī)² ≥ 1/2, 无需退化判据
  double si = 0, sii = 0, sx = 0, six = 0;
  int n = 0;
  void reset(const Param &) { *this = {}; }
  void add(int i, Val x, Val) {
    if (!x.m)
      return;
    const double t = i;
    si += t, sii += t * t, sx += x.v, six += t * x.v, ++n;
  }
  Val get(const Ctx &) const { return mk((six - si * sx / n) / (sii - si * si / n), n >= 2); }
};

struct Oldest { // 窗最旧一格 (TsDelayRoll: 窗长 d+1 → 最旧格即 x_{t−d})
  Val v0;
  void reset(const Param &) { *this = {}; }
  void add(int i, Val x, Val) {
    if (i == 0)
      v0 = x;
  }
  Val get(const Ctx &) const { return mk(v0.v, v0.m); }
};

struct Diff { // 最新 − 最旧
  Val v0;
  void reset(const Param &) { *this = {}; }
  void add(int i, Val x, Val) {
    if (i == 0)
      v0 = x;
  }
  Val get(const Ctx &c) const { return mk(static_cast<double>(c.x.v) - v0.v, v0.m && c.x.m); }
};

struct Age { // 距上次变动 (相邻有效样本精确不等) 的期数; 窗内无变动 → d
  Val prev;
  int last = -1;
  void reset(const Param &) { *this = {}; }
  void add(int i, Val x, Val) {
    if (i > 0 && prev.m && x.m && x.v != prev.v)
      last = i;
    prev = x;
  }
  Val get(const Ctx &c) const { return mk(last >= 0 ? c.d - 1 - last : c.d, c.x.m); }
};

// ---- 序统计族: 窗/段内样本缓存 + 分桶 (规则见 stream::Hist) ----
using stream::Hist;

// 样本缓存基类: 窗/段内所有有效值. scratch 是 get 用的第二缓存 (mutable: 复用容量, 不改语义)
struct SampleBase {
  std::vector<float> s;
  mutable std::vector<float> scratch;
  Param p;
  void reset(const Param &pp) {
    s.clear(), p = pp;
  }
  void add(int, Val x, Val) {
    if (x.m)
      s.push_back(x.v);
  }
};

struct Rank : SampleBase { // 相对型
  Val get(const Ctx &c) const {
    Hist h;
    h.build(s);
    return mk(h.rank(c.x.v), !s.empty() && c.x.m);
  }
};
struct Median : SampleBase {
  Val get(const Ctx &) const {
    Hist h;
    h.build(s);
    return mk(h.quantile(0.5), !s.empty());
  }
};
struct Mad : SampleBase { // median(|x − median|), 第二轮重新定桶
  Val get(const Ctx &) const {
    Hist h;
    h.build(s);
    const float med = h.quantile(0.5);
    scratch.clear();
    for (float v : s)
      scratch.push_back(std::fabs(v - med));
    Hist h2;
    h2.build(scratch);
    return mk(h2.quantile(0.5), !s.empty());
  }
};
struct TopK : SampleBase { // 前 K 大之和占比
  Val get(const Ctx &) const {
    const int n = static_cast<int>(s.size()), K = static_cast<int>(p.k);
    double tot = 0, atot = 0;
    for (float v : s)
      tot += v, atot += std::fabs(v);
    const bool ok = n >= K && K >= 1 && den_ok(tot, atot);
    Hist h;
    h.build(s);
    if (!h.ok)
      return mk(n >= 1 ? static_cast<double>(std::min(K, n)) / n : 0.0, ok);
    double sum = 0;
    int taken = 0;
    for (int b = kBuckets - 1; b >= 0 && taken < K; --b) {
      const int take = std::min(h.cnt[b], K - taken);
      sum += static_cast<double>(bin_center(b, h.lo, h.hi)) * take;
      taken += take;
    }
    return mk(sum / tot, ok);
  }
};
struct Gini : SampleBase { // Lorenz 曲线面积, 只对正值定义 (正样本 ⇒ 桶心 > 0 ⇒ tot > 0)
  Val get(const Ctx &) const {
    scratch.clear();
    for (float v : s)
      if (v > 0.f)
        scratch.push_back(v);
    const bool ok = scratch.size() >= 2;
    Hist h;
    h.build(scratch);
    if (!h.ok)
      return mk(0.0, ok);
    double tot = 0;
    for (int b = 0; b < kBuckets; ++b)
      tot += static_cast<double>(bin_center(b, h.lo, h.hi)) * h.cnt[b];
    double cp = 0, cl = 0, g = 1.0;
    for (int b = 0; b < kBuckets; ++b) {
      if (h.cnt[b] == 0)
        continue;
      const double np = cp + static_cast<double>(h.cnt[b]) / h.n;
      const double nl = cl + static_cast<double>(bin_center(b, h.lo, h.hi)) * h.cnt[b] / tot;
      g -= (np - cp) * (nl + cl);
      cp = np, cl = nl;
    }
    return mk(g, ok);
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

struct Cov : CoAcc {
  Val get(const Ctx &) const { return mk(cxy / (n - 1), n >= 2); }
};
struct Corr : CoAcc {
  Val get(const Ctx &) const { return mk(cxy / std::sqrt(cxx * cyy), n >= 2 && dx_ok() && dy_ok()); }
};
struct Beta : CoAcc { // x 对 y 的 OLS 斜率
  Val get(const Ctx &) const { return mk(cxy / cyy, n >= 2 && dy_ok()); }
};
struct Resid : CoAcc { // 相对型: 当前点对回归线的偏离
  Val get(const Ctx &c) const {
    return mk((c.x.v - mx) - cxy / cyy * (c.y.v - my), n >= 2 && dy_ok() && c.x.m && c.y.m);
  }
};

struct WMean { // y 为权
  double sy = 0, syx = 0, say = 0;
  int n = 0;
  void reset(const Param &) { *this = {}; }
  void add(int, Val x, Val y) {
    if (!x.m || !y.m)
      return;
    sy += y.v, syx += static_cast<double>(y.v) * x.v, say += std::fabs(y.v), ++n;
  }
  Val get(const Ctx &) const { return mk(syx / sy, n >= 1 && den_ok(sy, say)); }
};

struct CorrLag { // corr(x_s, y_{s−K}): 缓存两列后配对
  std::vector<Val> xs, ys;
  Param p;
  void reset(const Param &pp) { xs.clear(), ys.clear(), p = pp; }
  void add(int, Val x, Val y) { xs.push_back(x), ys.push_back(y); }
  Val get(const Ctx &) const {
    const int K = static_cast<int>(p.k), n = static_cast<int>(xs.size());
    CoAcc a;
    a.reset(Param{});
    for (int s = K; s < n; ++s)
      a.add(0, xs[s], ys[s - K]);
    return mk(a.cxy / std::sqrt(a.cxx * a.cyy), a.n >= 2 && a.dx_ok() && a.dy_ok());
  }
};

} // namespace core

// ---- 窗: EXPAND (段内 expanding; 推满 kSegLen 自动归零, 段首也可手动 reset) ----
template <class C>
class Expand {
public:
  static constexpr Win kWin = Win::EXPAND;
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

// ---- 窗: ROLL (最近 d+Extra 期, 跨段不重置 → 没有 reset; 满窗后整窗重放) ----
template <class C, int Extra = 0>
class Roll {
public:
  static constexpr Win kWin = Win::ROLL;
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
    if (n_ < L_)
      return Val{}; // 窗未满
    c_.reset(p_);
    int j = head_; // i = 0 最旧
    for (int i = 0; i < L_; ++i) {
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
  static constexpr Win kWin = Win::EXPO;
  explicit Ema(const Param &p) : k_(p.k) { assert(k_ > 0.f && k_ <= 1.f); }
  Val push(Val x) { // x 无效 → 状态与掩码都保持
    if (x.m)
      y_ = on_ ? k_ * x.v + (1.0 - k_) * y_ : x.v, on_ = true;
    return mk(y_, on_);
  }

private:
  double k_, y_ = 0.0;
  bool on_ = false;
};

// =========================== 实例化: 核 × 窗 ===========================
//   Ts<核>Cum / Ts<核>Roll = 同一个核挂两个窗; 公式只有核那一份. 顺序与 OpTable 的 OP_TS_WIN 一致.
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
using TsMedianRoll = Roll<core::Median>;
using TsMadRoll = Roll<core::Mad>;
using TsZRoll = Roll<core::Z>;
using TsWmaRoll = Roll<core::Wma>;
using TsProductRoll = Roll<core::Product>;
using TsSlopeRoll = Roll<core::Slope>;
using TsCountGtCum = Expand<core::CountGt>;
using TsCountGtRoll = Roll<core::CountGt>;
using TsAgeRoll = Roll<core::Age>;
using TsHhiCum = Expand<core::Hhi>;
using TsEntropyCum = Expand<core::Entropy>;
using TsTopKCum = Expand<core::TopK>;
using TsGiniCum = Expand<core::Gini>;
using TsPeaksCum = Expand<core::Peaks>;
using TsCovCum = Expand<core::Cov>;
using TsCovRoll = Roll<core::Cov>;
using TsCorrCum = Expand<core::Corr>;
using TsCorrRoll = Roll<core::Corr>;
using TsCorrLagCum = Expand<core::CorrLag>;
using TsBetaCum = Expand<core::Beta>;
using TsBetaRoll = Roll<core::Beta>;
using TsResidCum = Expand<core::Resid>;
using TsResidRoll = Roll<core::Resid>;
using TsWMeanCum = Expand<core::WMean>;
using TsWMeanRoll = Roll<core::WMean>;

} // namespace factor::ts
