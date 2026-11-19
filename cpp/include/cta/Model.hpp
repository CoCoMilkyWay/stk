#pragma once

// 单资产 band + 减仓线 + 加仓线 模型: 有效根 → 原料带 Tape (只读, 同资产各配置共用) → band (日频间距) + 减仓线核 (逐日换参, 逐根出值) → 分层 Ladder (内含加仓线核) 逐根出一刀 Cut (档线 + 目标仓位, 按本根收盘价调到)
//   方向 kDirs: long = 满多起步 / short = 满空起步; 顺向 = 方向所押的一侧, 逆向 = 另一侧
//   列 = 逐根一个量, 因果 ([i] 只看 ≤ i 的根); 窗口一律以根计 (日线 / 分钟同一套)
//   窗不满 = 用自首个有效值起的部分窗 (增量窗); 仍无值 = NaN: 比较恒假, 不入也不出
//   原料带 = 高 / 低 / 收 / 收益 / 对数收益 / 累计路程 + 日 K; band (ATR / dev, 日频, 价格单位) 由它算, 即档线间距
//   三个模型各一张表 (人选模型; 减仓线 / 加仓线的参数滚动调优, band 的窗人设):
//     band kBandKinds (atr / dev): 档线间距 g
//     减仓线 kReduceKinds (price / ema / kama / mid): 满仓随它, 目的是尽量保留顺向敞口; 一个模型一个核, 日首 day(d, p) 换参, 逐根 bar(i) 出值, 状态逐根接续;
//       定参 (调参打分) = 各日同参, 变参 (实盘) = 逐日所用参数, 同一个核
//     加仓线 kAddKinds (decay / chand / sar): 已减的份的加仓线怎么下移 (参数以日计, 逐根摊开), 回撤够深后下移做网格抄底; 各份的参数在它卖出时定下, 之后不变
//   分层: 减仓线链 (链顶锚定上一个触发点, 往下每条隔当日间距; 满仓随减仓线; 买回满仓后先挂锚定的止损链, 减仓线升过链顶且仍在收盘之下才回到随它) + 各份加仓线;
//     卖出即挂加仓线于成交价外 stop · 间距 (止损), 买回后链顶 = 买入价 − stop · 间距; 加仓线逐根按加仓线模型下移, 下移不越过 收 + stop · 间距 (减仓线不下移); 一份 = 1 / 份数
//   列只在两端: 原料带 (只读) → 逐根链 (源 → 成交 → 汇) → 轨迹列 Cuts; 核 / 分层的状态 (各份档线) 只在源里
//   源 = 日首 day(d) + 逐根出刀 Cut(i): Hold (买入持有) / Steps (减仓线当静态档线, 调参; Tune.hpp) / Laddered (完整分层; Tune.hpp)
//   成交 / 成本 / 逐根链归 Engine
#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iterator>
#include <limits>
#include <utility>
#include <vector>

namespace cta {

using Col = std::vector<float>;

inline constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
inline constexpr double kYearDays = 250; // 一年的交易日数 (窗以年设, 以日用)
inline constexpr int kMinYears = 1;      // 窗下限 (年)
inline constexpr int kMaxYears = 10;     // 窗上限 (年)
inline constexpr int kUnits = 20;        // 满仓份数默认 (Rule::units)
inline constexpr float kStop = 0.1f;     // 止损距默认 (Rule::stop)

inline size_t len(float x) { return static_cast<size_t>(std::lround(x)); }
// 窗 y 整年 → 交易日
inline size_t win_of(int y) {
  assert(y >= kMinYears && y <= kMaxYears && "窗 (年)");
  return static_cast<size_t>(std::lround(y * kYearDays));
}

// 一个资产的有效根 (跳过无数据的根)
struct Series {
  Col h, l, c;
  std::vector<uint32_t> t; // [i] → 时间格 (日下标 · 每日根数 + 槽)
};

// 日轴 (CSR): 日 d 的根 = [off[d], off[d + 1]); 日下标自首根所在日起, 无根的日 = 空段
struct Days {
  std::vector<size_t> off; // [nd + 1]
  size_t size() const { return off.size() - 1; }
};

inline Days days_of(const Series &se, size_t S) {
  const size_t d0 = se.t.empty() ? 0 : se.t.front() / S, nd = se.t.empty() ? 0 : se.t.back() / S - d0 + 1;
  Days o{std::vector<size_t>(nd + 1, 0)};
  for (const uint32_t t : se.t)
    ++o.off[t / S - d0 + 1];
  for (size_t d = 0; d < nd; ++d)
    o.off[d + 1] += o.off[d];
  return o;
}

// ---- 原料带 ----

// 一个资产: 有效根 + 逐根列 + 日 K; 建好即只读, 多线程共用
struct Tape {
  const size_t n;
  const Col &h, &l, &c;           // 高 / 低 / 收
  const Col a;                    // 逐根收益 (根 0 = 0)
  const Col lr;                   // 对数收益 ln(收 / 前收) (根 0 = NaN)
  const std::vector<double> trip; // 累计路程 Σ|Δ收| (根 0 = 0)
  const size_t S;                 // 每日根数 (时间格 t / S = 日下标)
  const Days days;                // 日轴 (days_of)
  const Col dh, dl, dc;           // [日] 日高 / 日低 / 日收 (当日各根合成; 无根的日 = NaN)

  Tape(const Series &s, size_t S)
      : n(s.c.size()), h(s.h), l(s.l), c(s.c), a(returns(c)), lr(logs(c)), trip(trips(c)), S(S), days(days_of(s, S)),
        dh(daily(h, [](float p, float q) { return std::max(p, q); })), dl(daily(l, [](float p, float q) { return std::min(p, q); })),
        dc(daily(c, [](float, float q) { return q; })) {}
  Tape(const Tape &) = delete;
  Tape &operator=(const Tape &) = delete;

private:
  static Col returns(const Col &c) {
    Col o(c.size(), 0);
    for (size_t i = 1; i < c.size(); ++i)
      o[i] = static_cast<float>(static_cast<double>(c[i]) / c[i - 1] - 1);
    return o;
  }
  static Col logs(const Col &c) {
    Col o(c.size(), kNaN);
    for (size_t i = 1; i < c.size(); ++i)
      o[i] = static_cast<float>(std::log(static_cast<double>(c[i]) / c[i - 1]));
    return o;
  }
  static std::vector<double> trips(const Col &c) {
    std::vector<double> o(c.size(), 0);
    for (size_t i = 1; i < c.size(); ++i)
      o[i] = o[i - 1] + std::fabs(c[i] - c[i - 1]);
    return o;
  }
  // 日 K: 日 d 的根合成一个量 (无根的日 = NaN)
  template <class F>
  Col daily(const Col &x, F f) const {
    Col o(days.size(), kNaN);
    for (size_t d = 0; d < days.size(); ++d)
      for (size_t i = days.off[d]; i < days.off[d + 1]; ++i)
        o[d] = i == days.off[d] ? x[i] : f(o[d], x[i]);
    return o;
  }
};

// 日 K 合成的有效根 (每日 1 根, 时间格 = 日下标; 无根的日跳过): 以它建 Tape(·, 1) 即日线原料带, 日下标与 t 的同
inline Series day_series(const Tape &t) {
  Series o;
  for (size_t d = 0; d < t.days.size(); ++d)
    if (!std::isnan(t.dc[d]))
      o.h.push_back(t.dh[d]), o.l.push_back(t.dl[d]), o.c.push_back(t.dc[d]), o.t.push_back(static_cast<uint32_t>(d));
  return o;
}

// ---- 模型表 ----
//   各模型自带参数轴: 格点自 v0 (q = 0) 向两侧铺, 相邻之比 kStep (Tune.hpp), 间距不小于轴的基本单位 u
//   (大值处等比, 小值处退成等差 u), 限在硬边界 [lo, hi] 内; 以日计的轴乘每日根数 (Grid::bars), 核拿到的都是根

struct Axis {
  const char *name;
  float v0, lo, hi; // 初始网格中心 (格点 q = 0) / 硬边界
  float u;          // 基本单位: 相邻格点的最小间距 (> 0)
  bool day = false; // 以日计
};

struct Kind {
  const char *name, *tip;
  std::vector<Axis> axes;
};

inline constexpr size_t kAxes = 2; // 参数轴数上限
template <class T>
using Ax = std::array<T, kAxes>; // 各轴一个量 (轴序; 模型轴数之外 = 0)
using P = Ax<float>;             // 一个模型的参数 (以根计)

// ---- band ----
//   价格单位, 日频: 日 d 盘中用前一交易日收盘时的值; 窗人设 (Spec::band_win), 不调

// band 模型 (下标同 kBandKinds)
enum : size_t { kAtr,
                kDev };
inline const Kind kBandKinds[] = {{"atr", "海龟 N: 日真实波幅 (含跳空) 的平滑, 不足 W 日 = 已有各日均值, 之后 N += (TR − N) / W", {}},
                                  {"dev", "日涨跌标准差: 近 W 个日涨跌 (收 − 前收) 的样本标准差, 不看高低价; 不足 W 个 = 已有的 (≥ 2 个)", {}}};

// 海龟 N 递推 (日频): 日真实波幅 TR = max(日高, 前收) − min(日低, 前收) (首日无前收 = 日高 − 日低); 窗 k 日,
//   不足 k 日 = 已有各日 TR 的均值, 之后 N += (TR − N) / k; 日 d 盘中所用 = 前一交易日收盘时的 N (首个有根的日 = NaN)
inline Col atr_of(const Tape &t, size_t k) {
  assert(k > 0 && "ATR 窗");
  Col o(t.days.size(), kNaN);
  double N = 0, pc = 0;
  size_t m = 0; // 已收日数
  for (size_t d = 0; d < t.days.size(); ++d) {
    if (m)
      o[d] = static_cast<float>(N);
    if (std::isnan(t.dc[d]))
      continue;
    const double tr = m ? std::max<double>(t.dh[d], pc) - std::min<double>(t.dl[d], pc) : t.dh[d] - t.dl[d];
    N += (tr - N) / static_cast<double>(m < k ? m + 1 : k);
    ++m, pc = t.dc[d];
  }
  return o;
}

// 日涨跌标准差 [日]: 日 d 盘中所用 = 此前 k 个日涨跌 (收 − 前收) 的样本标准差; 不足 k 个 = 已有的 (≥ 2 个), 再少 = NaN
inline Col dev_of(const Tape &t, size_t k) {
  assert(k > 1 && "dev 窗");
  Col o(t.days.size(), kNaN);
  std::vector<double> q;
  double s = 0, s2 = 0, pc = kNaN;
  for (size_t d = 0; d < t.days.size(); ++d) {
    if (const double w = static_cast<double>(std::min(q.size(), k)); w >= 2)
      o[d] = static_cast<float>(std::sqrt(std::max(0.0, (s2 - s * s / w) / (w - 1))));
    if (std::isnan(t.dc[d]))
      continue;
    if (!std::isnan(pc)) {
      const double x = t.dc[d] - pc;
      q.push_back(x), s += x, s2 += x * x;
      if (q.size() > k) {
        const double y = q[q.size() - 1 - k];
        s -= y, s2 -= y * y;
      }
    }
    pc = t.dc[d];
  }
  return o;
}

// band [日] (价格单位): kBandKinds[kind], 窗 k 日
inline Col band_of(const Tape &t, size_t kind, size_t k) {
  assert(kind < std::size(kBandKinds) && "band 模型");
  return kind == kAtr ? atr_of(t, k) : dev_of(t, k);
}

// ---- 减仓线 ----

// 减仓线模型 (下标同 kReduceKinds)
enum : size_t { kPrice,
                kEma,
                kKama,
                kMid };

inline constexpr Axis kDays{"n", 5, 1, 250, 0.25f, true}; // 周期 (日; 打分跑日线, 下限 1 日)

inline const Kind kReduceKinds[] = {
    {"price", "价格本身: 减仓线 = 收盘价, 无参数 (满仓随它时首条减仓线贴着收盘, 档线由止损推着走)", {}},
    {"ema", "指数均线, 跟得最匀", {kDays}},
    {"kama", "考夫曼自适应均线: n = 效率比窗, L = 震荡跨度", {kDays, {"L", 40, 4, 250, 1, true}}},
    {"mid", "唐奇安中线 = 近 n 根 (最高 + 最低) / 2, 区间内呈台阶", {kDays}}};

// ---- 减仓线核 ----
//   日首 day(d, p): 日 d 用参数 p (以根计, 可为小数); 逐根 bar(i) → 减仓线 (NaN = 未就绪); 日 d 的根之前先 day(d) (无根的日也调), 根按序逐个
//   状态逐根接续: 参数变只改跟随的快慢, 不在几条定参线之间换线; 窗不满 = 自首根起的部分窗

// price: 收盘价本身
struct Price {
  const Tape &t;
  void day(size_t, const P &) {}
  float bar(size_t i) const { return t.c[i]; }
};

// ema: α = 2 / (k + 1); 第 i 根的平滑系数 = max(α, 1 / (i + 1)) (前段 = 已有值的均值)
struct Ema {
  const Tape &t;
  double a = 0, v = 0;
  void day(size_t, const P &p) {
    assert(p[0] > 0 && "均线跨度");
    a = 2 / (static_cast<double>(p[0]) + 1);
  }
  float bar(size_t i) { return static_cast<float>(v += std::max(a, 1.0 / static_cast<double>(i + 1)) * (t.c[i] - v)); }
};

// kama: 效率比 ER = |收 − w 根前收| / 近 w 根路程 (w = min(round(n), i)); 平滑系数 (ER · (2/3 − α_slow) + α_slow)², α_slow = √(2 / (L + 1)):
//   震荡 (ER → 0) 时等效 EMA 跨度 L, 趋势 (ER → 1) 时贴近价格
struct Kama {
  const Tape &t;
  size_t k = 0;
  double as = 0, v = t.c[0];
  void day(size_t, const P &p) {
    k = len(p[0]), as = std::sqrt(2 / (static_cast<double>(p[1]) + 1));
    assert(k > 0 && p[1] > 3 && "kama 窗口 / 震荡跨度");
  }
  float bar(size_t i) {
    const size_t w = std::min(k, i);
    const double sum = t.trip[i] - t.trip[i - w], er = sum > 0 ? std::fabs(t.c[i] - t.c[i - w]) / sum : 0, sc = er * (2 / 3.0 - as) + as;
    return static_cast<float>(v += sc * sc * (t.c[i] - v));
  }
};

// 近 k 根最高 / 最低 (单调队列; 不足 k 根 = 自首根起): 日首 window(d, k) 定窗 (窗一变区间即可能跳, 自新窗首重扫), 逐根 push(i) 后读 hi / lo
struct Hilo {
  const Tape &t;
  size_t k = 0;
  std::deque<uint32_t> qh, ql; // 窗内最高 / 最低的候选 (下标升序)
  void push(size_t i) {
    while (!qh.empty() && t.h[qh.back()] <= t.h[i])
      qh.pop_back();
    while (!ql.empty() && t.l[ql.back()] >= t.l[i])
      ql.pop_back();
    qh.push_back(static_cast<uint32_t>(i)), ql.push_back(static_cast<uint32_t>(i));
    if (qh.front() + k <= i)
      qh.pop_front();
    if (ql.front() + k <= i)
      ql.pop_front();
  }
  void window(size_t d, size_t w) {
    assert(w > 0 && "最高 / 最低窗口");
    if (w == k)
      return;
    k = w, qh.clear(), ql.clear();
    const size_t b = t.days.off[d];
    for (size_t i = b + 1 >= k ? b + 1 - k : 0; i < b; ++i)
      push(i);
  }
  float hi() const { return t.h[qh.front()]; }
  float lo() const { return t.l[ql.front()]; }
};

// mid: 唐奇安中线 = 窗 round(n) 的 (最高 + 最低) / 2
struct Mid {
  const Tape &t;
  Hilo x{t};
  void day(size_t d, const P &p) { x.window(d, len(p[0])); }
  float bar(size_t i) {
    x.push(i);
    return (x.hi() + x.lo()) / 2;
  }
};

// 减仓线模型 kind 的核 (新起状态) 交给 f(核): 每个配置分派一次, 逐根无分派
template <class F>
void with_reduce(size_t kind, const Tape &t, F &&f) {
  switch (kind) {
  case kPrice: {
    Price k{t};
    return f(k);
  }
  case kEma: {
    Ema k{t};
    return f(k);
  }
  case kKama: {
    Kama k{t};
    return f(k);
  }
  case kMid: {
    Mid k{t};
    return f(k);
  }
  }
  assert(false && "减仓线模型");
  std::unreachable();
}

// ---- 加仓线 ----
//   顺向坐标 (多 = 价, 空 = −价) 下加仓线在价上; 逐根 (成交之后) 各加仓线 b 按模型下移 fall, 下移不越过 收 + 止损距 (分层里做; 不会自行触发, 也不上移)
//     参数以日计 (每日下移多少), 逐根摊开 (每根 = 日量的 1 / 每日根数), 加仓线随分钟平滑走, 不在日界跳
//   各份的参数在它卖出 (挂上加仓线) 时定下, 之后不变 (各份可不同); 当日参数只给当日新卖出的份
//   核: 日首 day(d, p) 换当日参数; sold(j, x) = 份 j 在 x 卖出 (定下它的参数); 逐根 bar(i, b) (成交前, b = 各份加仓线, 持有 = NaN) → 成交 → 逐份 fall(j, b, g); 日末 close(d)

// 加仓线模型 (下标同 kAddKinds)
enum : size_t { kDecay,
                kChand,
                kSar };

inline const Kind kAddKinds[] = {
    {"decay", "固定衰减: 每日下移 trail × 间距 (逐根摊开)", {{"trail", 0.1f, 0.01f, 2, 0.01f}}},
    {"chand", "吊灯: 压到 近 n 日最低 + m × 间距 以下 (只降不升; 逐根滚动)", {{"n", 20, 1, 250, 1, true}, {"m", 3, 0.25f, 10, 0.25f}}},
    {"sar", "抛物线 (各份自卖出起): 每日朝卖出以来的最低移 af 倍距离 (逐根摊开); 创新低的日 af += 步长 (≤ max)", {{"af", 0.02f, 0.002f, 0.2f, 0.002f}, {"max", 0.2f, 0.02f, 1, 0.02f}}}};

// 各份的参数: 日首换当日参数 cur, 份卖出时 freeze 定下
struct Frozen {
  P cur{};
  std::vector<P> at; // [份]
  void freeze(size_t j) {
    if (at.size() <= j)
      at.resize(j + 1);
    at[j] = cur;
  }
};

// decay: 每根 b − trail · g / 每日根数
struct Decay {
  const Tape &t;
  Frozen p;
  void day(size_t, const P &q) {
    assert(q[0] > 0 && "衰减距");
    p.cur = q;
  }
  void sold(size_t j, double) { p.freeze(j); }
  void bar(size_t, const std::vector<double> &) {}
  void close(size_t) {}
  double fall(size_t j, double b, double g) const { return b - p.at[j][0] * g / static_cast<double>(t.S); }
};

// chand: min(b, 近 round(n) 根 (n 以根给) 的顺向最低 + m · g) (含本根); 区间最低查逐根稀疏表 (建核时一遍, 层数够 n 的上限即止)
struct Chand {
  const Tape &t;
  float s;
  Frozen p;
  std::vector<Col> mn = lows(t, s); // [层 l][根] 自该根起 2^l 根的顺向最低
  size_t i = 0;                     // 本根
  void day(size_t, const P &q) {
    assert(q[1] > 0 && "吊灯倍数");
    p.cur = q;
  }
  void sold(size_t j, double) { p.freeze(j); }
  void bar(size_t x, const std::vector<double> &) { i = x; }
  void close(size_t) {}
  double fall(size_t j, double b, double g) const {
    const P &q = p.at[j];
    const size_t k = std::max<size_t>(1, len(q[0])), a = i + 1 > k ? i + 1 - k : 0;
    const size_t l = std::min(mn.size(), static_cast<size_t>(std::bit_width(i + 1 - a))) - 1, w = size_t{1} << l;
    double lo = mn[l][a];
    for (size_t e = a + w; e + w <= i + 1; e += w) // 窗长超过表的上限 (只在参数越界时) 分段取
      lo = std::min<double>(lo, mn[l][e]);
    return std::min(b, std::min<double>(lo, mn[l][i + 1 - w]) + q[1] * g);
  }
  static std::vector<Col> lows(const Tape &t, float s) {
    const size_t cap = static_cast<size_t>(std::ceil(kAddKinds[kChand].axes[0].hi * static_cast<float>(t.S)));
    std::vector<Col> o(1, Col(t.n));
    for (size_t e = 0; e < t.n; ++e)
      o[0][e] = s > 0 ? t.l[e] : -t.h[e];
    for (size_t l = 1, h = 1; 2 * h <= std::min(t.n, cap); ++l, h *= 2) {
      Col c(t.n - 2 * h + 1);
      for (size_t e = 0; e < c.size(); ++e)
        c[e] = std::min(o[l - 1][e], o[l - 1][e + h]);
      o.push_back(std::move(c));
    }
    return o;
  }
};

// sar: 各份自卖出起记顺向最低 ep (起于卖价) 与加速 af (起于该份的步长); 每日 b 朝 ep 走 af 倍距离, 逐根摊开 (每根走 1 − (1 − af)^(1 / 每日根数));
//   日末当日创过新低则 af += 步长 (≤ max)
struct Sar {
  const Tape &t;
  float s;
  Frozen p;
  std::vector<double> af, fb, ep; // [份] 加速 / 每根的步 / 顺向最低
  std::vector<uint8_t> nw;        // [份] 当日创了新低
  void day(size_t, const P &q) {
    assert(q[0] > 0 && "SAR 步长");
    p.cur = q;
  }
  void pace(size_t j) { fb[j] = 1 - std::pow(1 - std::min(af[j], 1.0), 1 / static_cast<double>(t.S)); }
  void sold(size_t j, double x) {
    p.freeze(j);
    if (af.size() <= j)
      af.resize(j + 1), fb.resize(j + 1), ep.resize(j + 1), nw.resize(j + 1);
    af[j] = p.at[j][0], ep[j] = x, nw[j] = 0;
    pace(j);
  }
  void bar(size_t i, const std::vector<double> &b) {
    const double v = s > 0 ? t.l[i] : -t.h[i];
    for (size_t j = 0; j < b.size(); ++j)
      if (!std::isnan(b[j]) && v < ep[j])
        ep[j] = v, nw[j] = 1;
  }
  void close(size_t) {
    for (size_t j = 0; j < nw.size(); ++j)
      if (nw[j]) {
        const P &q = p.at[j];
        af[j] = std::min<double>(af[j] + q[0], std::max(q[0], q[1])), nw[j] = 0;
        pace(j);
      }
  }
  double fall(size_t j, double b, double) const { return b + fb[j] * (ep[j] - b); }
};

// 加仓线模型 kind 的核 (新起状态, 顺向 s) 交给 f(核)
template <class F>
void with_add(size_t kind, const Tape &t, float s, F &&f) {
  switch (kind) {
  case kDecay: {
    Decay a{t};
    return f(a);
  }
  case kChand: {
    Chand a{t, s};
    return f(a);
  }
  case kSar: {
    Sar a{t, s};
    return f(a);
  }
  }
  assert(false && "加仓线模型");
  std::unreachable();
}

// ---- 刀 ----

inline constexpr uint32_t kNone = UINT32_MAX;

// 档线快照 (持久, 各根共用): 一组 = 各份 (Cuts::units) 一条档线 + 减仓线链的锚; 档线变了 / 换模式的根才压一组, 其余根沿用
//   减仓线链: 链顶 = 锚 − 偏 · h, 往下每 h 一条 (持有几份就几条; h 每日变); 持有的份记链序, 已减的份记加仓线价
struct Bands {
  std::vector<float> px;    // [组][份] 链序 (0 = 链顶; 减仓线) / 价 (加仓线)
  std::vector<uint8_t> add; // [组][份] 1 = 加仓线
  std::vector<float> ref;   // [组] 锚价 (NaN = 减仓线模型)
  std::vector<float> off;   // [组] 链顶离锚 (以 h 计)
};

// 一刀 = 一根的信号: 减仓线 + 档线间距 h = band (未就绪 = NaN) + 档线快照 snap (kNone = 未就绪) + 目标仓位 w
//   + 越线差 (占净值, 按本根调仓量 |Δw| 计): 分层按收盘成交比按档线价成交多付的差; 震荡 = 各份都按离收盘最近那档计, 跳空 = 远处各档多出的部分
struct Cut {
  float mid = kNaN, h = kNaN, w = 0, chop = 0, gap = 0;
  uint32_t snap = kNone;
};

// 一条轨迹的刀 (列, 与 Cut 同字段; 逐根链里录下, Engine.hpp Record) + 档线快照
struct Cuts {
  Col mid, h, w, chop, gap;
  std::vector<uint32_t> snap;
  Bands bands;
  float s = 1;        // 顺向 (kDirs)
  int units = kUnits; // 满仓份数 (快照一组的份数)
  Cuts() = default;
  explicit Cuts(size_t n) : mid(n, kNaN), h(n, kNaN), w(n, 0), chop(n, 0), gap(n, 0), snap(n, kNone) {}
  void put(size_t i, const Cut &x) { mid[i] = x.mid, h[i] = x.h, w[i] = x.w, chop[i] = x.chop, gap[i] = x.gap, snap[i] = x.snap; }

  // 根 i 的档线 f(j, 价, 加仓线), 份 j ∈ [0, units) (一份一条, 成交换极性); 未就绪 = 无
  template <class F>
  void rungs_at(size_t i, F f) const {
    if (!(h[i] > 0) || snap[i] == kNone)
      return;
    const size_t U = static_cast<size_t>(units), g = snap[i], b = g * U;
    const float top = (std::isnan(bands.ref[g]) ? mid[i] : bands.ref[g]) - s * bands.off[g] * h[i];
    for (size_t j = 0; j < U; ++j)
      if (bands.add[b + j])
        f(static_cast<int>(j), bands.px[b + j], true);
      else
        f(static_cast<int>(j), top - s * bands.px[b + j] * h[i], false);
  }
};

// 方向: s = 顺向 (+1 = 多 / −1 = 空)
enum : size_t { kLong,
                kShort };
struct Dir {
  const char *name, *tip;
  float s;
};
inline constexpr Dir kDirs[] = {{"long", "只做多: 满多, 跌破减仓线下方档线按分层减仓 / 加回", 1},
                                {"short", "只做空: 满空, 涨过减仓线上方档线按分层减仓 / 加回", -1}};

// 分层规则 (人设, 不调): 止损距 (以间距计) + 份数; 间距 = band
struct Rule {
  float stop = kStop; // 成交一份, 该份换极性并往反向偏 stop · 间距 (≥ 0)
  int units = kUnits; // 满仓份数 (一份 = 1 / units; ≥ 1)
  bool operator==(const Rule &) const = default;
};

// 一笔 = 一份自减仓 (根 i0 收盘) 到加回 (根 i1 收盘; kNone = 未加回); g = 卖出当日的间距
struct Trip {
  uint32_t i0, i1;
  float g;
};

// ---- 分层 (逐根, 有状态) ----
//   顺向坐标 x = s · 价; 下文的涨 / 跌 / 以上 / 以下都按 x; g = band (间距, 每日变), e = stop · g
//   持有的份挂减仓线 (在价下, 跌到即减一份), 已减的份各挂一条加仓线 (在价上, 涨到即加回该份); 成交按档线价
//   减仓线链: 链顶锚定上一个触发点, 往下每条 = 上一条 − g (锚定不变, 间距每日变)
//     随减仓线 (满仓): 链顶 = 减仓线; 卖出 @a 后: 链顶 = a − g; 买回 @L 后: 链顶 = L − e
//     满仓 (只剩减仓线) 且 链顶 ≤ 减仓线 < 收盘: 转随减仓线 (减仓线在收盘之上时不转, 免得一随即卖; 买回后留着买入价下方 e 的止损链)
//   加仓线: 卖出 @a 即挂 a + e (止损); 逐根各加仓线按加仓线核 add 下移, 下移不越过 收盘 + e (不自行触发, 也不上移)
//   一根越过几条成交几条 (逐条, 先近后远); 减仓与加回在同一根里互不触发 (新挂 / 下移的档线都在收盘的另一侧)
//   减仓线 / band 无值 = 未就绪的刀 (h = NaN; 轨迹按买入持有), 状态复位; 之后首根有值 = 满仓随减仓线起步 (已在链顶以下则逐条卖到对应档), 按收盘建仓, 不计越线差也不录笔
template <class A>
struct Ladder {
  const Tape &t;
  float s;                          // 顺向 (kDirs)
  Rule z;                           // 分层规则
  A &add;                           // 加仓线核
  Bands bands;                      // 档线快照 (只增)
  std::vector<double> b;            // [份] 加仓线的顺向坐标 (持有 = NaN)
  std::vector<uint32_t> ord;        // 持有的份按链序自底到顶 (末 = 链顶; 卖出取链顶, 买回的份成新链顶)
  int k = 0;                        // 持有份数
  bool on = false, follow = true;   // 已就位 (复位后首根有值时就位) / 链随减仓线
  double R = 0, off = 0;            // 链的锚 (顺向坐标; 随减仓线时不用) / 链顶离锚 (以 g 计)
  uint32_t cur = kNone;             // 当前快照
  size_t d = 0;                     // 当日
  std::vector<Trip> *log = nullptr; // 逐笔 (调参用; nullptr = 不录)
  std::vector<uint32_t> open;       // [份] 未加回那笔在 log 里的下标 (kNone = 无)

  void day(size_t x, const P &p) { d = x, add.day(x, p); }
  // 复位 (根 i 起买入持有): 未加回的笔记作在 i 收盘加回
  void reset(size_t i) {
    if (log && on)
      for (uint32_t &o : open)
        if (o != kNone)
          (*log)[o].i1 = static_cast<uint32_t>(i), o = kNone;
    k = 0, on = false, cur = kNone;
  }

  // 根 i (日 d), 减仓线 m, band g (当日所用) → 一刀
  Cut operator()(size_t i, float m, double g) {
    Cut o{.mid = m};
    if (!(m > 0 && g > 0)) {
      reset(i);
      return o;
    }
    assert(z.stop >= 0 && z.units >= 1 && "止损 / 份数设定");
    const size_t U = static_cast<size_t>(z.units);
    const double x = s * t.c[i], q = s * m, e = z.stop * g, u = 1.0 / z.units;
    const bool cold = !on, eod = i + 1 == t.days.off[d + 1];
    bool moved = false; // 本根档线变了 / 换了模式 (压新快照)
    if (cold) {
      on = moved = follow = true, k = z.units, off = 0, b.assign(U, kNaN), open.assign(U, kNone), ord.clear();
      for (uint32_t j = static_cast<uint32_t>(U); j-- > 0;)
        ord.push_back(j);
    } else
      add.bar(i, b);
    const auto top = [&] { return (follow ? q : R) - off * g; }; // 链顶
    int n = 0;                                                   // 本根成交份数
    double near = 0, sum = 0;                                    // 离收盘最近那档的越线差 / 各份越线差之和
    const auto fill = [&](double L) {                            // 档线 L 上成交一份
      moved = true, follow = false;
      if (cold)
        return;
      const double f = std::fabs(x / L - 1);
      near = n ? std::min(near, f) : f, sum += f, ++n;
    };
    const auto lowest = [&] { // 最低的加仓线 (无 = b.size())
      size_t a = b.size();
      for (size_t j = 0; j < b.size(); ++j)
        if (!std::isnan(b[j]) && (a == b.size() || b[j] < b[a]))
          a = j;
      return a;
    };
    while (k > 0 && x <= top()) {
      const double L = top();
      const uint32_t a = ord.back();
      ord.pop_back();
      b[a] = L + e, --k, R = L, off = 1;
      add.sold(a, x);
      if (log && !cold)
        open[a] = static_cast<uint32_t>(log->size()), log->push_back({static_cast<uint32_t>(i), kNone, static_cast<float>(g)});
      fill(L);
    }
    for (size_t a; (a = lowest()) < b.size() && x >= b[a];) {
      const double L = b[a];
      b[a] = kNaN, ord.push_back(static_cast<uint32_t>(a)), ++k, R = L, off = z.stop;
      if (log && open[a] != kNone)
        (*log)[open[a]].i1 = static_cast<uint32_t>(i), open[a] = kNone;
      fill(L);
    }
    for (size_t j = 0; j < U; ++j)
      if (!std::isnan(b[j])) {
        const double y = std::max(add.fall(j, b[j], g), std::min(b[j], x + e));
        moved |= y != b[j], b[j] = y;
      }
    if (eod)
      add.close(d);
    if (!follow && k == z.units && q >= top() && q < x)
      follow = moved = true, off = 0;
    if (moved) {
      const size_t g0 = bands.px.size();
      for (const double v : b)
        bands.px.push_back(static_cast<float>(s * v)), bands.add.push_back(!std::isnan(v));
      for (size_t r = 0; r < ord.size(); ++r)
        bands.px[g0 + ord[r]] = static_cast<float>(ord.size() - 1 - r);
      bands.ref.push_back(follow ? kNaN : static_cast<float>(s * R)), bands.off.push_back(static_cast<float>(off));
      cur = static_cast<uint32_t>(bands.ref.size() - 1);
    }
    o.h = static_cast<float>(g), o.snap = cur;
    o.w = static_cast<float>(s * u * k), o.chop = static_cast<float>(u * n * near), o.gap = static_cast<float>(u * (sum - n * near));
    return o;
  }
};

// ---- 源 (日首 day(d) + 逐根出刀) ----

// 买入持有: 无档线, 恒满多
struct Hold {
  void day(size_t) {}
  Cut operator()(size_t) const { return {.w = 1}; }
};

} // namespace cta
