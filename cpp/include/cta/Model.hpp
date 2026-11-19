#pragma once

// 单资产 band + 减仓 + 跟随 模型: 有效根 → 原料带 Tape (只读, 同资产各配置共用) → band (日频间距) + 减仓核 (逐日换参, 逐根出中线) → 分层 Ladder (内含跟随核) 逐根出一刀 Cut (档线 + 目标仓位, 按本根收盘价调到)
//   方向 kDirs: long = 满多起步 / short = 满空起步; 顺向 = 方向所押的一侧, 逆向 = 另一侧
//   列 = 逐根一个量, 因果 ([i] 只看 ≤ i 的根); 窗口一律以根计 (日线 / 分钟同一套)
//   窗不满 = 用自首个有效值起的部分窗 (增量窗); 仍无值 = NaN: 比较恒假, 不入也不出
//   原料带 = 高 / 低 / 收 / 收益 / 累计路程 + 日 K; band (ATR / dev, 日频, 价格单位) 由它算, 即档线间距
//   三个模型族 Family = 核的类型列表 (族表 kinds 由各核的 kind 生成, with(下标) 分派到核的类型; 人选模型, 减仓 / 跟随的参数滚动调优, band 的窗人设):
//     Band (atr / dev): 档线间距 g, 整列 of(原料带, 窗)
//     Reduce (price / ema / kama / mid): 中线, 第 j 份 (j = 1 起) 的减仓线 = 中线 − j · g (中线本身不触发), 目的是尽量保留顺向敞口; 日首 day(d, p) 换参, 逐根 bar(i) 出值, 状态逐根接续;
//       定参 (调参打分) = 各日同参, 变参 (实盘) = 逐日所用参数, 同一个核
//     Follow (decay / chand / sar): 成交过的份的跟随线怎么朝价漂 (参数以日计, 逐根摊开): 跟随加仓线往下漂做网格抄底, 跟随减仓线往上漂做移动止损;
//       一份一条 Line, 参数在它成交时定下, 之后不变
//   减仓 / 跟随核与分层都建于同一份上下文 Ctx (原料带 + band + 顺向): K{x}
//   分层: 每份各管自己的一档, 份与份独立; 同一时刻每份恰一条线 (减仓线 / 跟随加仓线 / 跟随减仓线), 一份 = 1 / 份数
//     减仓线跌到即卖, 挂跟随加仓线于成交价外 stop · 间距 (止损), 往下漂; 涨到即买回, 挂跟随减仓线于成交价内 stop · 间距, 往上漂;
//     跟随减仓线升到该份的减仓线之上 (或减仓线降到它之下) 即交回减仓模型
//   列只在两端: 原料带 (只读) → 逐根链 (源 → 成交 → 汇) → 轨迹列 Cuts; 核 / 分层的状态 (各份档线) 只在源里
//   源 / 成交 / 逐根链归 Engine
#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <type_traits>
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

inline size_t len(double x) { return static_cast<size_t>(std::lround(x)); }
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
  const std::vector<double> trip; // 累计路程 Σ|Δ收| (根 0 = 0)
  const size_t S;                 // 每日根数 (时间格 t / S = 日下标)
  const Days days;                // 日轴 (days_of)
  const Col dh, dl, dc;           // [日] 日高 / 日低 / 日收 (当日各根合成; 无根的日 = NaN)

  Tape(const Series &s, size_t S)
      : n(s.c.size()), h(s.h), l(s.l), c(s.c), a(returns(c)), trip(trips(c)), S(S), days(days_of(s, S)),
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

// ---- 模型族 ----
//   各模型自带参数轴: 格点自 v0 (q = 0) 向两侧铺, 相邻之比 kStep (Tune.hpp), 间距不小于轴的基本单位 u
//   (大值处等比, 小值处退成等差 u), 限在硬边界 [lo, hi] 内; 参数一律以轴的单位给, 核消费前由 Kind::bars 换成根

inline constexpr size_t kAxes = 2; // 参数轴数上限
template <class T>
using Ax = std::array<T, kAxes>; // 各轴一个量 (轴序; 模型轴数之外 = 0)
using P = Ax<double>;            // 一个模型的参数

struct Axis {
  const char *name = nullptr;
  float v0 = 0, lo = 0, hi = 0; // 初始网格中心 (格点 q = 0) / 硬边界
  float u = 0;                  // 基本单位: 相邻格点的最小间距 (> 0)
  bool day = false;             // 以日计
};

struct Kind {
  const char *name, *tip;
  size_t na = 0; // 轴数
  Ax<Axis> ax{}; // [轴]
  constexpr Kind(const char *name, const char *tip, std::initializer_list<Axis> axes = {}) : name(name), tip(tip), na(axes.size()) {
    std::ranges::copy(axes, ax.begin());
  }
  // 参数 p (轴的单位) → 以根计 (每日 S 根: 以日计的轴 × S)
  P bars(const P &p, size_t S) const {
    P o{};
    for (size_t a = 0; a < na; ++a)
      o[a] = p[a] * (ax[a].day ? static_cast<double>(S) : 1.0);
    return o;
  }
};

// 模型族: 核的类型列表; 族表 kinds[下标] = 各核的 kind, of<K> = 核 K 的下标
template <class... K>
struct Family {
  static constexpr Kind kinds[] = {K::kind...};
  template <class T>
  static constexpr size_t of = [] {
    static_assert((std::is_same_v<T, K> || ...), "核不在族里");
    constexpr bool m[] = {std::is_same_v<T, K>...};
    return static_cast<size_t>(std::ranges::find(m, true) - std::begin(m));
  }();
  // 下标 i 的核类型交给 f.template operator()<K>(): 每个配置分派一次, 逐根无分派
  template <class F>
  static void with(size_t i, F &&f) {
    assert(i < sizeof...(K) && "模型");
    size_t j = 0;
    ((j++ == i ? f.template operator()<K>() : void()), ...);
  }
};

// 减仓 / 跟随核与分层的上下文 (同一条链共用): 建核 K{x}
struct Ctx {
  const Tape &t;
  const Col &N; // [日] band
  float s;      // 顺向 (kDirs)
};

// ---- band ----
//   价格单位, 日频: 日 d 盘中用前一交易日收盘时的值; 窗人设 (Spec::band), 不调; of(原料带, 窗 k 日) → [日]

// 海龟 N 递推 (日频): 日真实波幅 TR = max(日高, 前收) − min(日低, 前收) (首日无前收 = 日高 − 日低); 窗 k 日,
//   不足 k 日 = 已有各日 TR 的均值, 之后 N += (TR − N) / k; 日 d 盘中所用 = 前一交易日收盘时的 N (首个有根的日 = NaN)
struct Atr {
  static constexpr Kind kind{"atr", "海龟 N: 日真实波幅 (含跳空) 的平滑, 不足 W 日 = 已有各日均值, 之后 N += (TR − N) / W"};
  static Col of(const Tape &t, size_t k) {
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
};

// 日涨跌标准差: 日 d 盘中所用 = 此前 k 个日涨跌 (收 − 前收) 的样本标准差; 不足 k 个 = 已有的 (≥ 2 个), 再少 = NaN
struct Dev {
  static constexpr Kind kind{"dev", "日涨跌标准差: 近 W 个日涨跌 (收 − 前收) 的样本标准差, 不看高低价; 不足 W 个 = 已有的 (≥ 2 个)"};
  static Col of(const Tape &t, size_t k) {
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
};

// ---- 减仓核 ----
//   日首 day(d, p): 日 d 用参数 p (以根计, 可为小数); 逐根 bar(i) → 中线 (NaN = 未就绪); 日 d 的根之前先 day(d) (无根的日也调), 根按序逐个
//   状态逐根接续: 参数变只改跟随的快慢, 不在几条定参线之间换线; 窗不满 = 自首根起的部分窗

inline constexpr Axis kDays{"n", 5, 1, 250, 0.25f, true}; // 周期 (日; 打分跑日线, 下限 1 日)

// price: 中线 = 收盘本身 (逐根, 无参数, 无状态)
struct Price : Ctx {
  static constexpr Kind kind{"price", "价格本身: 中线 = 收盘 (逐根, 无参数)"};
  void day(size_t, const P &) {}
  float bar(size_t i) const { return t.c[i]; }
};

// ema: α = 2 / (k + 1); 第 i 根的平滑系数 = max(α, 1 / (i + 1)) (前段 = 已有值的均值)
struct Ema : Ctx {
  static constexpr Kind kind{"ema", "指数均线, 跟得最匀", {kDays}};
  double a = 0, v = 0;
  void day(size_t, const P &p) {
    assert(p[0] > 0 && "均线跨度");
    a = 2 / (p[0] + 1);
  }
  float bar(size_t i) { return static_cast<float>(v += std::max(a, 1.0 / static_cast<double>(i + 1)) * (t.c[i] - v)); }
};

// kama: 效率比 ER = |收 − w 根前收| / 近 w 根路程 (w = min(round(n), i)); 平滑系数 (ER · (α_fast − α_slow) + α_slow)², α = √(2 / (跨度 + 1)):
//   震荡 (ER → 0) 时等效 EMA 跨度 L, 趋势 (ER → 1) 时等效跨度 kFast 日 (两端都以日计, 乘每日根数; 日线上 α_fast = 2/3)
struct Kama : Ctx {
  static constexpr Kind kind{"kama", "考夫曼自适应均线: n = 效率比窗, L = 震荡跨度; 趋势时跨度 3.5 日", {kDays, {"L", 40, 4, 250, 1, true}}};
  static constexpr double kFast = 3.5; // 趋势端跨度 (日)
  size_t k = 0;
  double af = std::sqrt(2 / (kFast * static_cast<double>(t.S) + 1)), as = 0, v = t.c[0];
  void day(size_t, const P &p) {
    k = len(p[0]), as = std::sqrt(2 / (p[1] + 1));
    assert(k > 0 && p[1] > kFast * static_cast<double>(t.S) && "kama 窗口 / 震荡跨度 (须慢于趋势端)");
  }
  float bar(size_t i) {
    const size_t w = std::min(k, i);
    const double sum = t.trip[i] - t.trip[i - w], er = sum > 0 ? std::fabs(t.c[i] - t.c[i - w]) / sum : 0, sc = er * (af - as) + as;
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
struct Mid : Ctx {
  static constexpr Kind kind{"mid", "唐奇安中线 = 近 n 根 (最高 + 最低) / 2, 区间内呈台阶", {kDays}};
  Hilo hl{t};
  void day(size_t d, const P &p) { hl.window(d, len(p[0])); }
  float bar(size_t i) {
    hl.push(i);
    return (hl.hi() + hl.lo()) / 2;
  }
};

// ---- 跟随核 ----
//   核的坐标 = 核的 s · 价 (Ctx::s); 此坐标下跟随线在价上, 逐根 (成交之后) 按模型下移 fall, 下移不越过 收 + 止损距 (分层里做; 不会自行触发, 也不上移)
//     分层建两个核: 跟随加仓线用顺向 s (顺向坐标里在价上, 往下漂), 跟随减仓线用 −s (镜像: 顺向坐标里在价下, 往上漂); 同一模型同一套参数
//     参数以日计 (每日漂多少), 逐根摊开 (每根 = 日量的 1 / 每日根数), 跟随线随分钟平滑走, 不在日界跳
//   一份一条 Line (该份成交时 open 定下参数, 之后不变; 各份可不同): open(p, x) = 当日参数 p 下在 x (核的坐标) 成交;
//     逐根 see(e, i) (成交前, 此前已挂的各份) → 成交 → 逐份 fall(e, i, b, g); 日末逐份 close(e)

// decay: 每根 b − trail · g / 每日根数
struct Decay : Ctx {
  static constexpr Kind kind{"decay", "固定漂移: 每日朝价漂 trail × 间距 (逐根摊开)", {{"trail", 0.1f, 0.01f, 2, 0.01f}}};
  struct Line {
    double trail;
  };
  Line open(const P &q, double) const {
    assert(q[0] > 0 && "衰减距");
    return {q[0]};
  }
  void see(Line &, size_t) const {}
  void close(Line &) const {}
  double fall(const Line &e, size_t, double b, double g) const { return b - e.trail * g / static_cast<double>(t.S); }
};

// chand: min(b, 近 round(n) 根 (n 以根给) 的最低 (核的坐标) + m · g) (含本根); 区间最低查逐根稀疏表 (建核时一遍, 层数够 n 的上限即止)
struct Chand : Ctx {
  static constexpr Kind kind{"chand", "吊灯: 跟随加仓线压到 近 n 日最低 + m × 间距 以下, 跟随减仓线抬到 近 n 日最高 − m × 间距 以上 (只朝价走; 逐根滚动)", {{"n", 20, 1, 250, 1, true}, {"m", 3, 0.25f, 10, 0.25f}}};
  struct Line {
    size_t k; // 窗 (根)
    double m; // 倍数
  };
  std::vector<Col> mn = lows(t, s); // [层 l][根] 自该根起 2^l 根的最低 (核的坐标)
  Line open(const P &q, double) const {
    assert(q[1] > 0 && "吊灯倍数");
    return {std::max<size_t>(1, len(q[0])), q[1]};
  }
  void see(Line &, size_t) const {}
  void close(Line &) const {}
  double fall(const Line &e, size_t i, double b, double g) const {
    const size_t a = i + 1 > e.k ? i + 1 - e.k : 0, l = static_cast<size_t>(std::bit_width(i + 1 - a)) - 1;
    assert(l < mn.size() && "吊灯窗超过稀疏表");
    return std::min(b, std::min<double>(mn[l][a], mn[l][i + 1 - (size_t{1} << l)]) + e.m * g);
  }
  static std::vector<Col> lows(const Tape &t, float s) {
    const size_t cap = static_cast<size_t>(std::ceil(kind.ax[0].hi * static_cast<float>(t.S)));
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

// sar: 各份自成交起记最低 ep (核的坐标, 起于成交价) 与加速 af (起于该份的步长); 每日 b 朝 ep 走 af 倍距离, 逐根摊开 (每根走 1 − (1 − af)^(1 / 每日根数));
//   日末当日创过新低则 af += 步长 (≤ max)
struct Sar : Ctx {
  static constexpr Kind kind{"sar", "抛物线 (各份自成交起): 每日朝成交以来的极值 (跟随加仓线 = 最低, 跟随减仓线 = 最高) 移 af 倍距离 (逐根摊开); 创新极值的日 af += 步长 (≤ max)", {{"af", 0.02f, 0.002f, 0.2f, 0.002f}, {"max", 0.2f, 0.02f, 1, 0.02f}}};
  struct Line {
    double step, top;  // 步长 / 加速上限
    double af, fb, ep; // 加速 / 每根的步 / 最低 (核的坐标)
    bool nw;           // 当日创了新低
  };
  double pace(double af) const { return 1 - std::pow(1 - std::min(af, 1.0), 1 / static_cast<double>(t.S)); }
  Line open(const P &q, double x) const {
    assert(q[0] > 0 && "SAR 步长");
    return {q[0], std::max(q[0], q[1]), q[0], pace(q[0]), x, false};
  }
  void see(Line &e, size_t i) const {
    if (const double v = s > 0 ? t.l[i] : -t.h[i]; v < e.ep)
      e.ep = v, e.nw = true;
  }
  void close(Line &e) const {
    if (e.nw)
      e.af = std::min(e.af + e.step, e.top), e.fb = pace(e.af), e.nw = false;
  }
  double fall(const Line &e, size_t, double b, double) const { return b + e.fb * (e.ep - b); }
};

using Band = Family<Atr, Dev>;
using Reduce = Family<Price, Ema, Kama, Mid>;
using Follow = Family<Decay, Chand, Sar>;

// ---- 刀 ----

inline constexpr uint32_t kNone = UINT32_MAX;

// 档线类别 (每份恰一条): 减仓线 (持有, 中线 − (j + 1) · h, 随减仓模型; j = 份下标) / 跟随加仓线 (已减, 往下漂) / 跟随减仓线 (持有, 往上漂)
enum : uint8_t { kReduce,
                 kFollowAdd,
                 kFollowReduce };

// 档线快照 (持久, 各根共用): 一组 = 各份 (Cuts::units) 的类别 + 跟随线的价; 跟随线变了 / 换类别的根才压一组, 其余根沿用 (减仓线由中线与间距逐根定)
struct Rungs {
  std::vector<float> px;     // [组][份] 跟随线的价 (减仓线 = NaN)
  std::vector<uint8_t> type; // [组][份] 类别
};

// 一刀 = 一根的信号: 中线 + 档线间距 h = band (未就绪 = NaN) + 档线快照 snap (kNone = 未就绪) + 目标仓位 w
//   + 越线差 (占净值, 按本根调仓量 |Δw| 计): 分层按收盘成交比按档线价成交多付的差; 震荡 = 各份都按离收盘最近那档计, 跳空 = 远处各档多出的部分
struct Cut {
  float mid = kNaN, h = kNaN, w = 0, chop = 0, gap = 0;
  uint32_t snap = kNone;
};

// 一条轨迹的档线 (列, 逐根链里录下) + 档线快照
struct Cuts {
  Col mid, h;
  std::vector<uint32_t> snap;
  Rungs rungs;
  float s = 1;        // 顺向 (kDirs)
  int units = kUnits; // 满仓份数 (快照一组的份数)
  Cuts() = default;
  explicit Cuts(size_t n) : mid(n, kNaN), h(n, kNaN), snap(n, kNone) {}
  void put(size_t i, const Cut &x) { mid[i] = x.mid, h[i] = x.h, snap[i] = x.snap; }

  // 根 i 的档线 f(j, 价, 类别), 份 j ∈ [0, units) (一份一条); 未就绪 = 无
  template <class F>
  void rungs_at(size_t i, F f) const {
    if (!(h[i] > 0) || snap[i] == kNone)
      return;
    const size_t U = static_cast<size_t>(units), b = snap[i] * U;
    for (size_t j = 0; j < U; ++j) {
      const uint8_t k = rungs.type[b + j];
      f(static_cast<int>(j), k == kReduce ? mid[i] - s * static_cast<float>(j + 1) * h[i] : rungs.px[b + j], k);
    }
  }
};

// 方向: s = 顺向 (+1 = 多 / −1 = 空)
enum : size_t { kLong,
                kShort };
struct Dir {
  const char *name, *tip;
  float s;
};
inline constexpr Dir kDirs[] = {{"long", "只做多: 满多, 跌破中线下方各档按分层减仓 / 加回", 1},
                                {"short", "只做空: 满空, 涨过中线上方各档按分层减仓 / 加回", -1}};

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
//   份 j 各管一档, 份与份独立, 每份恰一条线; 持有的份跌到线上即卖, 已减的份涨到线上即买回; 成交按档线价
//     止损锚定实际成交价 (本根收盘 x; 越过档线 L 的差另计越线差): 卖出 → 跟随加仓线 x + e, 买回 → 跟随减仓线 x − e
//     减仓线 (持有): q − (j + 1) · g (中线本身不触发), 随减仓模型逐根走; 跌到即卖 → 跟随加仓线
//     跟随加仓线 (已减): 逐根按跟随核 add 下移, 下移不越过 收盘 + e; 涨到即买回 → 跟随减仓线
//     跟随减仓线 (持有): 逐根按跟随核 reduce 上移 (add 的镜像, 同参数), 上移不越过 收盘 − e; 跌到即卖 → 跟随加仓线;
//       升到减仓线 q − (j + 1) · g 之上 (或减仓线降到它之下) 即交回减仓模型
//     成交的份用当日参数开它的跟随线 (之后不变); 跟随线都不会自行触发 (新挂 / 漂移的线都在收盘的另一侧)
//   一根越过几条成交几条; 减仓与加回在同一根里互不触发
//   中线 / band 无值 = 未就绪的刀 (h = NaN; 轨迹按买入持有), 状态复位; 之后首根有值 = 各份按减仓线就位 (已在线下的份即卖成跟随加仓线), 按收盘建仓, 不计越线差也不录笔
template <class F>
struct Ladder : Ctx {
  Rule z;                                     // 分层规则
  F add{Ctx{t, N, s}}, reduce{Ctx{t, N, -s}}; // 跟随核: 跟随加仓线 (顺向坐标) / 跟随减仓线 (逆向坐标, 即镜像)
  Rungs rungs;                                // 档线快照 (只增)
  std::vector<uint8_t> type;                  // [份] 类别
  std::vector<double> b;                      // [份] 跟随线的顺向坐标 (减仓线不用)
  std::vector<typename F::Line> line;         // [份] 跟随核的逐份状态 (跟随线的份)
  bool on = false;                            // 已就位 (复位后首根有值时就位)
  uint32_t cur = kNone;                       // 当前快照
  size_t d = 0;                               // 当日
  P pf{};                                     // 当日跟随参数 (以根计; 当日成交的份用它)
  std::vector<Trip> *log = nullptr;           // 逐笔 (调参用; nullptr = 不录)
  std::vector<uint32_t> pend;                 // [份] 未加回那笔在 log 里的下标 (kNone = 无)

  void day(size_t x, const P &p) { d = x, pf = p; }
  // 复位 (根 i 起买入持有): 未加回的笔记作在 i 收盘加回
  void reset(size_t i) {
    if (log && on)
      for (uint32_t &o : pend)
        if (o != kNone)
          (*log)[o].i1 = static_cast<uint32_t>(i), o = kNone;
    on = false, cur = kNone;
  }

  // 根 i (日 d), 中线 m → 一刀
  Cut operator()(size_t i, float m) {
    const double g = N[d];
    Cut o{.mid = m};
    if (!(m > 0 && g > 0)) {
      reset(i);
      return o;
    }
    assert(z.stop >= 0 && z.units >= 1 && "止损 / 份数设定");
    const size_t U = static_cast<size_t>(z.units);
    const double x = s * t.c[i], q = s * m, e = z.stop * g, u = 1.0 / z.units;
    const bool cold = !on, eod = i + 1 == t.days.off[d + 1];
    bool moved = cold; // 本根有份换了类别 / 跟随线动了 (压新快照)
    if (cold)
      on = true, type.assign(U, kReduce), b.assign(U, kNaN), line.resize(U), pend.assign(U, kNone);
    const auto core = [&](size_t j) -> F & { return type[j] == kFollowAdd ? add : reduce; }; // 跟随线 j 的核
    const auto sign = [&](size_t j) { return type[j] == kFollowAdd ? 1.0 : -1.0; };          // 跟随线 j 的核坐标 = sign · 顺向坐标
    const auto cut = [&](size_t j) { return q - static_cast<double>(j + 1) * g; };           // 份 j 的减仓线
    for (size_t j = 0; j < U; ++j)
      if (type[j] != kReduce)
        core(j).see(line[j], i);
    int n = 0, held = 0;      // 本根成交份数 / 持有份数
    double near = 0, sum = 0; // 离收盘最近那档的越线差 / 各份越线差之和
    for (size_t j = 0; j < U; ++j) {
      const double L = type[j] == kReduce ? cut(j) : b[j];
      const bool sell = type[j] != kFollowAdd;
      if (sell ? x > L : x < L)
        continue;
      type[j] = sell ? kFollowAdd : kFollowReduce, b[j] = sell ? x + e : x - e, line[j] = core(j).open(pf, sign(j) * x), moved = true;
      if (log && sell && !cold)
        pend[j] = static_cast<uint32_t>(log->size()), log->push_back({static_cast<uint32_t>(i), kNone, static_cast<float>(g)});
      if (log && !sell && pend[j] != kNone)
        (*log)[pend[j]].i1 = static_cast<uint32_t>(i), pend[j] = kNone;
      if (cold)
        continue;
      const double f = std::fabs(x / L - 1);
      near = n ? std::min(near, f) : f, sum += f, ++n;
    }
    for (size_t j = 0; j < U; ++j) {
      if (type[j] != kReduce) {
        F &k = core(j);
        const double r = sign(j), v = r * b[j], y = r * std::max(k.fall(line[j], i, v, g), std::min(v, r * x + e));
        moved |= y != b[j], b[j] = y;
        if (eod)
          k.close(line[j]);
        if (type[j] == kFollowReduce && b[j] >= cut(j))
          type[j] = kReduce, moved = true;
      }
      held += type[j] != kFollowAdd;
    }
    if (moved) {
      for (size_t j = 0; j < U; ++j)
        rungs.px.push_back(type[j] == kReduce ? kNaN : static_cast<float>(s * b[j])), rungs.type.push_back(type[j]);
      cur = static_cast<uint32_t>(rungs.type.size() / U - 1);
    }
    o.h = static_cast<float>(g), o.snap = cur;
    o.w = static_cast<float>(s * u * held), o.chop = static_cast<float>(u * n * near), o.gap = static_cast<float>(u * (sum - n * near));
    return o;
  }
};

} // namespace cta
