#pragma once

// 单资产 中线 + 分层 模型: 有效根 → 原料带 Tape (只读, 同资产各配置共用) → 中线核 (逐日换参, 逐根出值) → 分层 Ladder 逐根出一刀 Cut (档线 + 目标仓位, 按本根收盘价调到)
//   方向 kDirs: long = 满多起步 / short = 满空起步; 顺向 = 方向所押的一侧, 逆向 = 另一侧
//   列 = 逐根一个量, 因果 ([i] 只看 ≤ i 的根); 窗口一律以根计 (日线 / 分钟同一套)
//   窗不满 = 用自首个有效值起的部分窗 (增量窗); 仍无值 = NaN: 比较恒假, 不入也不出
//   原料带 = 高 / 低 / 收 / 收益 / 对数收益 / 累计路程 / 每根波动 + 日 K; 分层波动 (ATR / dev, 日频) 由它算
//   中线 (ema / kama / mid / chand / sar / price, 各有自己的参数): 一个模型一个核, 日首 day(d, p) 换参, 逐根 bar(i) 出值, 状态逐根接续;
//     定参 (调参打分) = 各日同参, 变参 (实盘) = 逐日所用参数, 同一个核; 满仓时带着档线走
//   分层 (参数固定, 不调): 减仓线链 (链顶锚定上一个触发点, 往下每条隔当日间距; 满仓随中线, 中线上穿链顶即回到随中线) + 各份加仓线;
//     卖出即挂加仓线于成交价外 stop · 间距 (止损), 买回后链顶 = 买入价 − stop · 间距; 加仓线每日收盘跟随 trail · 间距 (减仓线不跟随); 一份 = 1 / 份数
//   列只在两端: 原料带 (只读) → 逐根链 (源 → 仓位层 → 成交 → 汇) → 轨迹列 Cuts; 中线核 / 分层的状态 (各份档线) 只在源里
//   源 = 日首 day(d) + 逐根出刀 Cut(i): Hold (买入持有) / Switch (中线当开关, 调参; Tune.hpp) / Laddered (中线 + 分层, 策略; Trace.hpp)
//   成交 / 仓位层 / 成本 / 逐根链归 Engine
#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <utility>
#include <vector>

namespace cta {

using Col = std::vector<float>;

inline constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
inline constexpr size_t kVolSpan = 60;
inline constexpr size_t kSwingDays = 250; // 分层波动窗默认 (日; Spec::days)
inline constexpr float kSwingDamp = 0.1f; // 分层波动阻尼: 所用值每日相对变化上限
inline constexpr double kSpace = 1;       // 减仓线间距 = kSpace · 波动 (满仓随中线 / 锚定时的初始档线)
inline constexpr int kUnits = 20;         // 满仓份数默认 (Rule::units)
inline constexpr float kStop = 0.1f;      // 止损距默认 (Rule::stop)
inline constexpr float kTrail = 0.1f;     // 跟随距默认 (Rule::trail)

// 分层波动度量 (下标同 kSwings); 价格单位, 日频: 日 d 盘中用前一交易日收盘时的值
enum : size_t { kAtr,
                kDev };
struct Swing {
  const char *name, *tip;
};
inline constexpr Swing kSwings[] = {{"atr", "海龟 N: 日真实波幅 (含跳空) 的平滑, 不足 W 日 = 已有各日均值, 之后 N += (TR − N) / W"},
                                    {"dev", "日涨跌标准差: 近 W 个日涨跌 (收 − 前收) 的样本标准差, 不看高低价; 不足 W 个 = 已有的 (≥ 2 个)"}};

inline size_t len(float x) { return static_cast<size_t>(std::lround(x)); }

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

// 指数均线, 跨度 k: 第 m 个有效值的平滑系数 = max(2 / (k + 1), 1 / m) (前段 = 已有值的均值, 增量窗), NaN 跳过; 首个有效值前 NaN
inline Col ema_of(const Col &x, size_t k) {
  assert(k > 0 && "均线跨度");
  const double a = 2 / (static_cast<double>(k) + 1);
  Col o(x.size(), kNaN);
  double v = 0;
  size_t m = 0;
  for (size_t i = 0; i < x.size(); ++i) {
    if (!std::isnan(x[i]))
      ++m, v += std::max(a, 1.0 / static_cast<double>(m)) * (x[i] - v);
    if (m)
      o[i] = static_cast<float>(v);
  }
  return o;
}

// 均方根: √EMA_k(x²)
inline Col rms_of(const Col &x, size_t k) {
  Col o(x.size());
  for (size_t i = 0; i < x.size(); ++i)
    o[i] = x[i] * x[i];
  o = ema_of(o, k);
  for (float &v : o)
    v = std::sqrt(v);
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
  const Col vol;                  // 每根波动 σ̂ (近 kVolSpan 根日内对数收益均方根; 每日首根的隔夜跳空不计)
  const size_t S;                 // 每日根数 (时间格 t / S = 日下标)
  const Days days;                // 日轴 (days_of)
  const Col dh, dl, dc;           // [日] 日高 / 日低 / 日收 (当日各根合成; 无根的日 = NaN)

  Tape(const Series &s, size_t S)
      : n(s.c.size()), h(s.h), l(s.l), c(s.c), a(returns(c)), lr(logs(c)), trip(trips(c)), vol(rms_of(intraday(lr, s.t, S), kVolSpan)), S(S),
        days(days_of(s, S)), dh(daily(h, [](float p, float q) { return std::max(p, q); })), dl(daily(l, [](float p, float q) { return std::min(p, q); })),
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
  static Col intraday(const Col &lr, const std::vector<uint32_t> &t, size_t S) { // 每日首根 (隔夜跳空) = NaN
    Col o = lr;
    for (size_t i = 1; i < o.size(); ++i)
      if (t[i] / S != t[i - 1] / S)
        o[i] = kNaN;
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

// 海龟 N 递推 (日频): 日真实波幅 TR = max(日高, 前收) − min(日低, 前收) (首日无前收 = 日高 − 日低); 日 d 的窗 k 日 (逐日可变),
//   不足 k 日 = 已有各日 TR 的均值, 之后 N += (TR − N) / k; step(d) 返回日 d 盘中所用 = 前一交易日收盘时的 N (首个有根的日 = NaN), 再收进日 d
struct Atr {
  double N = 0, pc = 0;
  size_t m = 0; // 已收日数
  float step(const Tape &t, size_t d, size_t k) {
    assert(k > 0 && "ATR 窗");
    const float o = m ? static_cast<float>(N) : kNaN;
    if (std::isnan(t.dc[d]))
      return o;
    const double tr = m ? std::max<double>(t.dh[d], pc) - std::min<double>(t.dl[d], pc) : t.dh[d] - t.dl[d];
    N += (tr - N) / static_cast<double>(m < k ? m + 1 : k);
    ++m, pc = t.dc[d];
    return o;
  }
};

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

// 分层波动 [日] (价格单位): kSwings[kind], 窗 k 日; 阻尼: 所用值每日只朝新估计走, 相对变化 ≤ kSwingDamp (首个有值的日 = 估计本身)
inline Col swing_of(const Tape &t, size_t kind, size_t k) {
  Col v(t.days.size());
  if (kind == kAtr) {
    Atr x;
    for (size_t d = 0; d < v.size(); ++d)
      v[d] = x.step(t, d, k);
  } else
    v = dev_of(t, k);
  for (size_t d = 1; d < v.size(); ++d)
    if (!std::isnan(v[d - 1]))
      v[d] = std::isnan(v[d]) ? v[d - 1] : std::clamp(v[d], v[d - 1] * (1 - kSwingDamp), v[d - 1] * (1 + kSwingDamp));
  return v;
}

// ---- 中线 ----
//   中线模型表 kCenters; 各模型自带参数轴: 格点自 v0 (q = 0) 向两侧铺, 相邻之比 kStep (Tune.hpp), 间距不小于轴的基本单位 u
//   (大值处等比, 小值处退成等差 u), 限在硬边界 [lo, hi] 内; 以日计的轴乘每日根数 (Grid::bars), 模型拿到的都是根

// 中线模型 (下标同 kCenters)
enum : size_t { kEma,
                kKama,
                kMid,
                kChand,
                kSar,
                kPrice };

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

inline constexpr Axis kDays{"n", 5, 1.0f / 16, 250, 1.0f / 16, true}; // 周期 (日; 单位 = 15 分钟)

inline const Kind kCenters[] = {
    {"ema", "指数均线, 跟得最匀", {kDays}},
    {"kama", "考夫曼自适应均线: n = 效率比窗, L = 震荡跨度", {kDays, {"L", 40, 1, 250, 1, true}}},
    {"mid", "唐奇安中线 = 近 n 根 (最高 + 最低) / 2, 区间内呈台阶", {kDays}},
    {"chand", "吊灯 (自带翻转): 多头线 = 近 n 根最高 − m × ATR, 空头线 = 近 n 根最低 + m × ATR (ATR 窗 = n 日); 收盘穿过当前那条即翻到另一条", {kDays, {"m", 3, 0.5f, 10, 0.25f}}},
    {"sar", "抛物线 SAR (日线, 自带翻转): 每日收盘更新, 盘中用前一交易日的值; af = 加速步长, max = 加速上限", {{"af", 0.02f, 0.002f, 0.2f, 0.002f}, {"max", 0.2f, 0.02f, 1, 0.02f}}},
    {"price", "价格本身: 中线 = 收盘价, 无参数 (满仓随中线时首条减仓线贴着收盘, 档线由止损 / 跟随推着走)", {}}};

inline constexpr size_t kAxes = 2; // 参数轴数上限
template <class T>
using Ax = std::array<T, kAxes>; // 各轴一个量 (轴序; 模型轴数之外 = 0)
using P = Ax<float>;             // 一个中线模型的参数 (以根计)

// ---- 中线核 ----
//   日首 day(d, p): 日 d 用参数 p (以根计, 可为小数); 逐根 bar(i) → 中线 (NaN = 未就绪); 日 d 的根之前先 day(d) (无根的日也调), 根按序逐个
//   状态逐根接续: 参数变只改跟随的快慢, 不在几条定参中线之间换线; 窗不满 = 自首根起的部分窗

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

// chand: 窗 round(n) 的最高 / 最低 ∓ m · ATR (ATR 窗 = max(1, round(n / 每日根数)) 日); 多头线 / 空头线, 收盘穿过当前那条即翻转, 起于多头
struct Chand {
  const Tape &t;
  Hilo x{t};
  Atr atr;
  float n = kNaN, m = 0; // 当日所用 ATR / 倍数
  bool up = true;
  void day(size_t d, const P &p) {
    x.window(d, len(p[0])), m = p[1];
    n = atr.step(t, d, std::max<size_t>(1, len(p[0] / static_cast<float>(t.S))));
  }
  float bar(size_t i) {
    x.push(i);
    if (std::isnan(n))
      return kNaN;
    const float L = x.hi() - m * n, H = x.lo() + m * n;
    if (up ? t.c[i] < L : t.c[i] > H)
      up = !up;
    return up ? L : H;
  }
};

// sar: 日线, 日 d 收盘按当日参数 (af 步长, max 上限) 更新, 日 d + 1 盘中用; 首个有根的日起于多头 (SAR = 日低, EP = 日高), 次日起有值;
//   未翻转: 创新极值则 EP 换新、af += 步长 (≤ 上限), SAR += af · (EP − SAR); 日低 (多) / 日高 (空) 穿过 SAR = 翻转, SAR = 原 EP, af 复位;
//   SAR 不越过当日与前一日的低 (多) / 高 (空)
struct Sar {
  const Tape &t;
  bool on = false, up = true;
  double sar = 0, ep = 0, af = 0, ph = 0, pl = 0; // SAR / 极值 / 加速 / 前一日的高低
  float v = kNaN;                                 // 当日盘中所用
  void day(size_t d, const P &p) {
    v = on ? static_cast<float>(sar) : kNaN;
    if (std::isnan(t.dc[d]))
      return;
    const double hi = t.dh[d], lo = t.dl[d], st = p[0], mx = std::max(p[0], p[1]);
    assert(st > 0 && "SAR 步长");
    if (!on)
      on = true, sar = lo, ep = hi, af = st, ph = hi, pl = lo;
    else if (up ? lo < sar : hi > sar)
      up = !up, sar = ep, ep = up ? hi : lo, af = st;
    else {
      if (up ? hi > ep : lo < ep)
        ep = up ? hi : lo, af = std::min(af + st, mx);
      sar += af * (ep - sar);
    }
    sar = up ? std::min({sar, lo, pl}) : std::max({sar, hi, ph});
    ph = hi, pl = lo;
  }
  float bar(size_t) const { return v; }
};

// price: 收盘价本身
struct Price {
  const Tape &t;
  void day(size_t, const P &) {}
  float bar(size_t i) const { return t.c[i]; }
};

// 中线模型 kind 的核 (新起状态) 交给 f(核): 每个配置分派一次, 逐根无分派
template <class F>
void with_center(size_t kind, const Tape &t, F &&f) {
  switch (kind) {
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
  case kChand: {
    Chand k{t};
    return f(k);
  }
  case kSar: {
    Sar k{t};
    return f(k);
  }
  case kPrice: {
    Price k{t};
    return f(k);
  }
  }
  assert(false && "中线模型");
  std::unreachable();
}

// ---- 刀 ----

inline constexpr uint32_t kNone = UINT32_MAX;

// 档线快照 (持久, 各根共用): 一组 = 各份 (Cuts::units) 一条档线 + 减仓线链的锚; 档线变了 / 换模式的根才压一组, 其余根沿用
//   减仓线链: 链顶 = 锚 − 偏 · h, 往下每 h 一条 (持有几份就几条; h 每日变); 持有的份记链序, 已减的份记加仓线价
struct Bands {
  std::vector<float> px;    // [组][份] 链序 (0 = 链顶; 减仓线) / 价 (加仓线)
  std::vector<uint8_t> add; // [组][份] 1 = 加仓线
  std::vector<float> ref;   // [组] 锚价 (NaN = 中线)
  std::vector<float> off;   // [组] 链顶离锚 (以 h 计)
};

// 一刀 = 一根的信号: 中线 + 档线间距 h = kSpace · 波动 (未就绪 = NaN) + 档线快照 snap (kNone = 未就绪) + 目标仓位 w
//   + 越线差 (占净值, 按本根分层调仓量 |Δw| 计): 分层按收盘成交比按档线价成交多付的差; 震荡 = 各份都按离收盘最近那档计, 跳空 = 远处各档多出的部分
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
inline constexpr Dir kDirs[] = {{"long", "只做多: 满多, 跌破中线下方档线按分层减仓 / 加回", 1},
                                {"short", "只做空: 满空, 涨过中线上方档线按分层减仓 / 加回", -1}};

// 分层规则: 止损 / 跟随 (以间距计) + 份数; 间距 = kSpace · 分层波动 (度量与窗在 Spec)
struct Rule {
  float stop = kStop;   // 成交一份, 该份换极性并往反向偏 stop · 间距 (≥ 0)
  float trail = kTrail; // 加仓线每日收盘朝价格移 trail · 间距 (≥ 0)
  int units = kUnits;   // 满仓份数 (一份 = 1 / units; ≥ 1)
  bool operator==(const Rule &) const = default;
};

// ---- 分层 (逐根, 有状态) ----
//   顺向坐标 x = s · 价 (多 = 价, 空 = −价); 下文的涨 / 跌 / 以上 / 以下都按 x; g = kSpace · 波动 (间距, 每日变), d = stop · g, t = trail · g
//   持有的份挂减仓线 (在价下, 跌到即减一份), 已减的份各挂一条加仓线 (在价上, 涨到即加回该份); 成交按档线价
//   减仓线链: 链顶锚定上一个触发点, 往下每条 = 上一条 − g (锚定不变, 间距每日变)
//     随中线 (满仓): 链顶 = 中线; 卖出 @a 后: 链顶 = a − g; 买回 @L 后: 链顶 = L − d
//     满仓 (只剩减仓线) 且中线上穿链顶: 转随中线
//   加仓线: 卖出 @a 即挂 a + d (止损), 定价; 跟随只在加仓线: 每日收盘各加仓线下移 t, 不低于 收盘 + d (不自行触发)
//   一根越过几条成交几条 (逐条, 先近后远); 减仓与加回在同一根里互不触发 (新挂 / 跟随的档线都在收盘的另一侧)
//   中线 / 波动无值 = 未就绪的刀 (h = NaN; 轨迹按买入持有), 状态复位; 之后首根有值 = 满仓随中线起步 (已在链顶以下则逐条卖到对应档), 按收盘建仓, 不计越线差
struct Ladder {
  float s;                        // 顺向 (kDirs)
  Rule z;                         // 分层规则
  Bands bands;                    // 档线快照 (只增)
  std::vector<double> b;          // [份] 加仓线的顺向坐标 (持有 = NaN)
  std::vector<uint32_t> ord;      // 持有的份按链序自底到顶 (末 = 链顶; 卖出取链顶, 买回的份成新链顶)
  int k = 0;                      // 持有份数
  bool on = false, follow = true; // 已就位 (复位后首根有值时就位) / 链随中线
  double A = 0, off = 0;          // 链的锚 (顺向坐标; 随中线时不用) / 链顶离锚 (以 g 计)
  uint32_t cur = kNone;           // 当前快照

  void reset() { k = 0, on = false, cur = kNone; }

  // 本根收 c, 中线 m, 波动 N (当日所用), eod = 当日末根 → 一刀
  Cut operator()(float c, float m, double N, bool eod) {
    Cut o{.mid = m};
    if (!(m > 0 && N > 0)) {
      reset();
      return o;
    }
    assert(z.stop >= 0 && z.trail >= 0 && z.units >= 1 && "止损 / 跟随 / 份数设定");
    const double x = s * c, q = s * m, g = kSpace * N, d = z.stop * g, t = z.trail * g, u = 1.0 / z.units;
    const bool cold = !on;
    bool moved = false; // 本根档线变了 / 换了模式 (压新快照)
    if (cold) {
      on = moved = follow = true, k = z.units, off = 0, b.assign(static_cast<size_t>(z.units), kNaN), ord.clear();
      for (uint32_t j = static_cast<uint32_t>(z.units); j-- > 0;)
        ord.push_back(j);
    }
    const auto top = [&] { return (follow ? q : A) - off * g; }; // 链顶
    int n = 0;                                                   // 本根成交份数
    double near = 0, sum = 0;                                    // 离收盘最近那档的越线差 / 各份越线差之和
    const auto fill = [&](double L) {                            // 档线 L 上成交一份
      moved = true, follow = false;
      if (cold)
        return;
      const double e = std::fabs(x / L - 1);
      near = n ? std::min(near, e) : e, sum += e, ++n;
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
      b[a] = L + d, --k, A = L, off = 1;
      fill(L);
    }
    for (size_t a; (a = lowest()) < b.size() && x >= b[a];) {
      const double L = b[a];
      b[a] = kNaN, ord.push_back(static_cast<uint32_t>(a)), ++k, A = L, off = z.stop;
      fill(L);
    }
    for (double &v : b)
      if (eod && !std::isnan(v)) {
        const double y = std::max(v - t, x + d);
        moved |= y != v, v = y;
      }
    if (!follow && k == z.units && q >= top())
      follow = moved = true, off = 0;
    if (moved) {
      const size_t g0 = bands.px.size();
      for (const double v : b)
        bands.px.push_back(static_cast<float>(s * v)), bands.add.push_back(!std::isnan(v));
      for (size_t r = 0; r < ord.size(); ++r)
        bands.px[g0 + ord[r]] = static_cast<float>(ord.size() - 1 - r);
      bands.ref.push_back(follow ? kNaN : static_cast<float>(s * A)), bands.off.push_back(static_cast<float>(off));
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
