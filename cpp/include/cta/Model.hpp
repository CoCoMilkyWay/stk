#pragma once

// 单资产 中线 + 分层 模型: 有效根 → 指标列 (Tape 缓存, 同资产各配置共用) → 中线 (调参) → 分层 Ladder 逐根出一刀 Cut (触发线 + 目标仓位, 按本根收盘价调到)
//   方向 kDirs: long = 中线以上满多 / short = 中线以下满空; 顺向 = 方向所押的一侧, 逆向 = 另一侧
//   列 = 逐根一个量, 因果 ([i] 只看 ≤ i 的根); 窗口一律以根计 (日线 / 分钟同一套)
//   未就绪 = NaN: 比较恒假, 预热期不入也不出
//   指标列 = 算子作用于源列: 窗算子 ema / rms / top / bot / kama, 逐点算子 mean; 原料列 = 高 / 低 / 收 / 对数收益 / ATR (日频)
//   中线 (ema / kama / mid, 各有自己的参数, 滚动调参) 只当开关: 顺向侧满仓, 逆向侧交给分层
//   分层 (参数固定, 不调): 逆向侧以上一笔成交为基准的网格, 逆向每走一层减一份, 顺向每走一层加一份; 层距 h = kSpace · ATR, 一份 = 1 / kUnits
//   列只在两端: 指标列 (取齐后 Tape 只读) → 逐根链 (源 → 仓位层 → 成交 → 汇) → 轨迹列 Cuts; 分层的状态 (份数 / 基准) 只在拼接遍里
//   源 = 逐根出刀的可调用 Cut(i): Cuts (拼好的轨迹) / Hold (买入持有)
//   成交 / 仓位层 / 成本 / 逐根链归 Engine
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <utility>
#include <vector>

namespace cta {

using Col = std::vector<float>;

inline constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
inline constexpr size_t kVolSpan = 60;
inline constexpr size_t kAtrDays = 20; // ATR (海龟 N) 平滑日数
inline constexpr double kSpace = 0.5;  // 分层层距 h = kSpace · ATR
inline constexpr int kUnits = 20;      // 满仓份数 (一份 = 1 / kUnits)

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

// ---- 窗算子 (近 k 根, 含本根) ----

// 指数均线, 跨度 k (α = 2 / (k + 1)): 首个有效值即初值, NaN 跳过; 满 k 个有效值前 NaN
inline Col ema_of(const Col &x, size_t k) {
  assert(k > 0 && "均线跨度");
  const double a = 2 / (static_cast<double>(k) + 1);
  Col o(x.size(), kNaN);
  double v = 0;
  size_t m = 0;
  for (size_t i = 0; i < x.size(); ++i) {
    if (!std::isnan(x[i]))
      v = m++ ? v + a * (x[i] - v) : x[i];
    if (m >= k)
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

// 极值: 单调队列
template <class Keep>
Col roll_of(const Col &x, size_t k) {
  assert(k > 0 && "极值窗口");
  Col o(x.size(), kNaN);
  std::vector<size_t> q(x.size());
  size_t b = 0, e = 0;
  for (size_t i = 0; i < x.size(); ++i) {
    while (e > b && !Keep{}(x[q[e - 1]], x[i]))
      --e;
    q[e++] = i;
    if (q[b] + k <= i)
      ++b;
    if (i + 1 >= k)
      o[i] = x[q[b]];
  }
  return o;
}

// 考夫曼自适应均线: 效率比 ER = |x − k 根前 x| / 近 k 根 Σ|Δx|; 平滑系数 (ER · (2/3 − α_slow) + α_slow)²
//   α_slow = √(2 / (L + 1)): 震荡 (ER → 0) 时等效 EMA 跨度 L, 趋势 (ER → 1) 时贴近价格
inline Col kama_of(const Col &x, size_t k, size_t L) {
  assert(k > 0 && L > 3 && "kama 窗口 / 震荡跨度");
  const double as = std::sqrt(2 / (static_cast<double>(L) + 1));
  Col o(x.size(), kNaN);
  double sum = 0, v = x[0];
  for (size_t i = 1; i < x.size(); ++i) {
    sum += std::fabs(x[i] - x[i - 1]);
    if (i > k)
      sum -= std::fabs(x[i - k] - x[i - k - 1]);
    if (i < k) {
      v = x[i];
      continue;
    }
    const double er = sum > 0 ? std::fabs(x[i] - x[i - k]) / sum : 0, sc = er * (2 / 3.0 - as) + as;
    o[i] = static_cast<float>(v += sc * sc * (x[i] - v));
  }
  return o;
}

// ---- 指标列 ----

// 一个资产: 有效根 + 原料列 + 指标列缓存 (键 = 算子 + 源列 + 窗, 首次取时算)
//   列一律按引用给出, 地址随 Tape 终生不变 (可作别的列的源); 取齐所需的列后 Tape 只读, 可多线程共用
class Tape {
  enum class Op : uint8_t { Ema,
                            Rms,
                            Top,
                            Bot,
                            Kama,
                            Mean };
  struct Key {
    Op op;
    uintptr_t x, y; // 源列地址 (无 = 0)
    size_t k, L;    // 窗 (无 = 0)
    auto operator<=>(const Key &) const = default;
  };
  std::map<Key, Col> memo_;

  template <class F>
  const Col &memo(Op op, const Col *x, const Col *y, size_t k, size_t L, F make) {
    const Key key{op, reinterpret_cast<uintptr_t>(x), reinterpret_cast<uintptr_t>(y), k, L};
    if (const auto it = memo_.find(key); it != memo_.end())
      return it->second;
    Col v = make();
    assert(v.size() == n);
    return memo_.emplace(key, std::move(v)).first->second;
  }

  template <class F>
  Col zip(const Col &x, const Col &y, F f) const {
    Col o(n);
    for (size_t i = 0; i < n; ++i)
      o[i] = f(x[i], y[i]);
    return o;
  }

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
  static Col intraday(const Col &lr, const std::vector<uint32_t> &t, size_t S) { // 每日首根 (隔夜跳空) = NaN
    Col o = lr;
    for (size_t i = 1; i < o.size(); ++i)
      if (t[i] / S != t[i - 1] / S)
        o[i] = kNaN;
    return o;
  }
  // 海龟 N: 日真实波幅 TR = max(日高, 前收) − min(日低, 前收) (日高 / 低 / 收由当日各根合成; 首日无前收 = 日高 − 日低),
  //   初值 = 前 k 日 TR 均值, 之后 N = ((k − 1) · N + TR) / k; 根 i 取前一交易日收盘时的 N (盘中不变), 不足 k 日 = NaN
  static Col atr_of(const Col &h, const Col &l, const Col &c, const std::vector<uint32_t> &t, size_t S, size_t k) {
    Col o(c.size(), kNaN);
    double N = 0, pc = 0;
    size_t m = 0; // 已收日数
    for (size_t b = 0; b < c.size();) {
      size_t e = b;
      double hi = h[b], lo = l[b];
      for (; e < c.size() && t[e] / S == t[b] / S; ++e)
        hi = std::max(hi, static_cast<double>(h[e])), lo = std::min(lo, static_cast<double>(l[e]));
      if (m >= k)
        std::fill(o.begin() + static_cast<std::ptrdiff_t>(b), o.begin() + static_cast<std::ptrdiff_t>(e), static_cast<float>(N));
      const double tr = m ? std::max(hi, pc) - std::min(lo, pc) : hi - lo;
      N += m < k ? tr / static_cast<double>(k) : (tr - N) / static_cast<double>(k);
      ++m, pc = c[e - 1], b = e;
    }
    return o;
  }

public:
  const size_t n;
  const Col &h, &l, &c; // 高 / 低 / 收
  const Col a;          // 逐根收益 (根 0 = 0)
  const Col lr;         // 对数收益 ln(收 / 前收) (根 0 = NaN)
  const Col vol;        // 每根波动 σ̂ (近 kVolSpan 根日内对数收益均方根; 每日首根的隔夜跳空不计)
  const Col atr;        // 近 kAtrDays 日的海龟 N (价格单位)

  // S = 每日根数 (时间格 t / S = 日下标)
  Tape(const Series &s, size_t S)
      : n(s.c.size()), h(s.h), l(s.l), c(s.c), a(returns(c)), lr(logs(c)), vol(rms_of(intraday(lr, s.t, S), kVolSpan)), atr(atr_of(h, l, c, s.t, S, kAtrDays)) {}
  Tape(const Tape &) = delete;
  Tape &operator=(const Tape &) = delete;

  // 窗算子
  const Col &ema(const Col &x, size_t k) {
    return memo(Op::Ema, &x, nullptr, k, 0, [&] { return ema_of(x, k); });
  }
  const Col &rms(const Col &x, size_t k) {
    return memo(Op::Rms, &x, nullptr, k, 0, [&] { return rms_of(x, k); });
  }
  const Col &top(const Col &x, size_t k) {
    return memo(Op::Top, &x, nullptr, k, 0, [&] { return roll_of<std::greater<float>>(x, k); });
  }
  const Col &bot(const Col &x, size_t k) {
    return memo(Op::Bot, &x, nullptr, k, 0, [&] { return roll_of<std::less<float>>(x, k); });
  }
  const Col &kama(const Col &x, size_t k, size_t L) {
    return memo(Op::Kama, &x, nullptr, k, L, [&] { return kama_of(x, k, L); });
  }

  // 逐点算子
  const Col &mean(const Col &x, const Col &y) {
    return memo(Op::Mean, &x, &y, 0, 0, [&] { return zip(x, y, [](float p, float q) { return (p + q) / 2; }); });
  }
};

// ---- 中线 ----
//   中线模型表 kCenters; 各模型自带参数轴: 格点自 v0 (q = 0) 向两侧铺, 相邻之比 kStep (Tune.hpp), 间距不小于轴的基本单位 u
//   (大值处等比, 小值处退成等差 u), 限在硬边界 [lo, hi] 内; 以日计的轴乘每日根数 (Grid::p), 模型拿到的都是根

// 中线模型 (下标同 kCenters)
enum : size_t { kEma,
                kKama,
                kMid };

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

using P = std::vector<float>; // 一个中线模型的参数 (轴序, 以根计)

inline constexpr Axis kDays{"n", 5, 1.0f / 16, 250, 1.0f / 16, true}; // 周期 (日; 单位 = 15 分钟)

inline const Kind kCenters[] = {
    {"ema", "指数均线, 跟得最匀", {kDays}},
    {"kama", "考夫曼自适应均线: n = 效率比窗, L = 震荡跨度", {kDays, {"L", 40, 1, 250, 1, true}}},
    {"mid", "唐奇安中线 = 近 n 根 (最高 + 最低) / 2, 区间内呈台阶", {kDays}}};

// 中线 (周期 k 根): ema (跟得最匀) / kama (k = ER 窗, L = 震荡跨度) / mid (唐奇安中线, 区间内呈台阶)
inline const Col &center(Tape &t, size_t kind, const P &p) {
  const size_t k = len(p[0]);
  switch (kind) {
  case kEma:
    return t.ema(t.c, k);
  case kKama:
    return t.kama(t.c, k, len(p[1]));
  case kMid:
    return t.mean(t.top(t.h, k), t.bot(t.l, k));
  }
  assert(false && "中线模型");
  std::unreachable();
}

// ---- 刀 ----

// 一刀 = 一根的信号: 中线 + 下一笔的两条触发线 (上线 = 收涨到此成交 / 下线 = 收跌到此成交; 无 = NaN) + 目标仓位 w
//   + 越线差 (占净值, 按本根分层调仓量 |Δw| 计): 分层按收盘成交比按档线价成交多付的差; 震荡 = 各份都按离收盘最近那档计, 跳空 = 远处各档多出的部分
struct Cut {
  float up = kNaN, mid = kNaN, dn = kNaN, w = 0, chop = 0, gap = 0;
};

// 一条轨迹的刀 (列, 与 Cut 同字段); 本身是源: 逐根读回
struct Cuts {
  Col up, mid, dn, w, chop, gap;
  Cuts() = default;
  explicit Cuts(size_t n) : up(n, kNaN), mid(n, kNaN), dn(n, kNaN), w(n, 0), chop(n, 0), gap(n, 0) {}
  void put(size_t i, const Cut &x) { up[i] = x.up, mid[i] = x.mid, dn[i] = x.dn, w[i] = x.w, chop[i] = x.chop, gap[i] = x.gap; }
  Cut operator()(size_t i) const { return {up[i], mid[i], dn[i], w[i], chop[i], gap[i]}; }
};

// 方向: s = 顺向 (+1 = 多 / −1 = 空)
enum : size_t { kLong,
                kShort };
struct Dir {
  const char *name, *tip;
  float s;
};
inline constexpr Dir kDirs[] = {{"long", "只做多: 中线以上满多, 中线以下按分层减仓 / 加回", 1},
                                {"short", "只做空: 中线以下满空, 中线以上按分层减仓 / 加回", -1}};

// ---- 分层 (逐根, 有状态) ----
//   顺向坐标 x = s · 价 (多 = 价, 空 = −价); 下文的涨 / 跌 / 以上 / 以下都按 x
//   x ≥ 中线: 满仓 (缺的份按收盘补满, 档线 = 中线), 无基准
//   x < 中线: 基准 = 上一笔成交的档线 (刚穿到中线以下 = 当根中线); 跌到 基准 − h 减一份, 涨到 基准 + h 加一份;
//     一根越过几档成交几档; 份数到 0 / 满之后不再成交, 基准照样移到最后越过的档 (跌透之后反弹一层即加回)
//   中线 / 层距未就绪 = 空仓, 状态复位 (之后首根就绪: 中线以上满仓, 以下自空仓起, 基准 = 中线)
struct Ladder {
  float s;           // 顺向 (kDirs)
  int k = 0;         // 持有份数
  double ref = kNaN; // 基准 (顺向坐标; NaN = 中线以上 / 复位)

  // 本根收 c, 中线 m, 层距 h → 一刀
  Cut operator()(float c, float m, double h) {
    Cut o{.mid = m};
    if (!(m > 0 && h > 0)) {
      k = 0, ref = kNaN;
      return o;
    }
    const double x = s * c, q = s * m, u = 1.0 / kUnits;
    int n = 0;                               // 本根成交份数
    double near = 0, sum = 0;                // 离收盘最近那档的越线差 / 各份越线差之和
    const auto fill = [&](double L, int j) { // 档线 L 上成交 j 份
      const double e = std::fabs(x / L - 1);
      near = n ? std::min(near, e) : e, sum += j * e, n += j;
    };
    if (x >= q) {
      if (k < kUnits)
        fill(q, kUnits - k);
      k = kUnits, ref = kNaN;
    } else {
      if (std::isnan(ref))
        ref = q;
      if (x <= ref - h) {
        const double j = std::floor((ref - x) / h);
        const int d = static_cast<int>(std::min<double>(j, k));
        for (int i = 1; i <= d; ++i)
          fill(ref - i * h, 1);
        k -= d, ref -= j * h;
      } else if (x >= ref + h) {
        const double j = std::floor((x - ref) / h);
        const int d = static_cast<int>(std::min<double>(j, kUnits - k));
        for (int i = 1; i <= d; ++i)
          fill(ref + i * h, 1);
        k += d, ref += j * h;
      }
    }
    o.w = static_cast<float>(s * u * k), o.chop = static_cast<float>(u * n * near), o.gap = static_cast<float>(u * (sum - n * near));
    // 下一笔: 减 = 中线以上 → 中线 − h / 以下 → 基准 − h (有份可减); 加 = 基准 + h 与中线取近 (未满)
    const double cut = std::isnan(ref) ? q - h : k ? ref - h
                                                   : kNaN,
                 add = std::isnan(ref) || k == kUnits ? kNaN : std::min(ref + h, q);
    (s > 0 ? o.dn : o.up) = static_cast<float>(s * cut), (s > 0 ? o.up : o.dn) = static_cast<float>(s * add);
    return o;
  }
};

// ---- 源 (逐根出刀) ----

// 买入持有: 无触发线, 恒满多
struct Hold {
  Cut operator()(size_t) const { return {.w = 1}; }
};

} // namespace cta
