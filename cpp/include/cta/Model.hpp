#pragma once

// 单资产多空 band 模型: 有效根 → 指标列 (Tape 缓存, 同资产各配置共用) → 信号核 Signal 逐根出一刀 Cut (上线 / 中线 / 下线 + 目标仓位 [−1, 1], 按本根收盘价调到)
//   列 = 逐根一个量, 因果 ([i] 只看 ≤ i 的根); 窗口一律以根计 (日线 / 分钟同一套)
//   未就绪 = NaN: 比较恒假, 预热期不入也不出
//   指标列 = 算子作用于源列: 窗算子 ema / rms / top / bot / kama, 逐点算子 mean / half / sub / walk; 原料列 = 高 / 低 / 收 / 对数收益 / 对数真实波幅
//   三组部件, 每组选一个模型 + 该模型自己的参数: 中线 (ema / kama / mid) · 带 (vol / atr / dev / rng, 上线 / 下线 = 中线 ± k · 半宽) · 分层 (arith / geo)
//     分层 = 跟随网格: 中线 = 空仓, 中线到上线 / 下线各分 N 层, 每越过一层线开 / 平一份 (浮盈加仓, 回撤减仓), 到上线满多 / 到下线满空; 层距 等差 = 同一价差 / 等比 = 同一涨幅
//     半宽 = 源 × 量法:     收盘           高低
//              步长 · √n    vol            atr      (收 · √n · 均方根: 对数收益 / 对数真实波幅)
//              窗内离散     dev            rng      (收盘偏离中线的均方根 / 近 n 根最高最低的半差)
//     中线同按源: 收盘 ema / kama, 高低 mid; mid ↔ rng = 同一对 (近 n 根最高, 最低) 的均值 / 半差
//   列只在两端: 指标列 (取齐后 Tape 只读) → 逐根核 (信号 / 仓位层 / 目标 / 损益, 状态在核里, 不落列) → 轨迹列 Cuts
//   成交 / 仓位层 / 成本归 Engine
#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
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
                            Mean,
                            Half,
                            Sub,
                            Walk };
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
  static Col spans(const Col &h, const Col &l, const Col &c) {
    Col o(c.size(), kNaN);
    for (size_t i = 1; i < c.size(); ++i)
      o[i] = static_cast<float>(std::log(static_cast<double>(std::max(h[i], c[i - 1])) / std::min(l[i], c[i - 1])));
    return o;
  }

public:
  const size_t n;
  const Col &h, &l, &c; // 高 / 低 / 收
  const Col a;          // 逐根收益 (根 0 = 0)
  const Col lr, lt;     // 对数收益 ln(收 / 前收) / 对数真实波幅 ln(max(高, 前收) / min(低, 前收)) (根 0 = NaN)
  const Col vol;        // 每根波动 σ̂ (近 kVolSpan 根对数收益均方根)

  explicit Tape(const Series &s)
      : n(s.c.size()), h(s.h), l(s.l), c(s.c), a(returns(c)), lr(logs(c)), lt(spans(h, l, c)), vol(rms_of(lr, kVolSpan)) {}
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
  const Col &half(const Col &x, const Col &y) {
    return memo(Op::Half, &x, &y, 0, 0, [&] { return zip(x, y, [](float p, float q) { return (p - q) / 2; }); });
  }
  const Col &sub(const Col &x, const Col &y) {
    return memo(Op::Sub, &x, &y, 0, 0, [&] { return zip(x, y, [](float p, float q) { return p - q; }); });
  }
  const Col &walk(const Col &x, size_t k) { // x · 收 · √k: 每根相对波动 → k 根随机游走的价格宽度
    return memo(Op::Walk, &x, nullptr, k, 0, [&] {
      const float r = std::sqrt(static_cast<float>(k));
      return zip(x, c, [r](float p, float q) { return p * q * r; });
    });
  }
};

// ---- 部件与参数 ----
//   三组部件, 每组选一个模型; 各模型自带参数轴 (网格取值); 单值轴 = 固定参数, 自动优化不动它, 要换就改这里
//   以日计的轴乘每日根数 (Grid::p), 模型拿到的都是根

enum : size_t { kCenter, // 中线
                kBand,   // 带
                kLayer,  // 分层
                kNP };

// 各组模型 (下标同 kParts[组].kinds)
enum : size_t { kEma,
                kKama,
                kMid }; // 中线
enum : size_t { kVol,
                kAtr,
                kDev,
                kRng }; // 带
enum : size_t { kArith,
                kGeo }; // 分层

struct Axis {
  const char *name;
  std::vector<float> v;
  bool day = false; // 以日计
};

struct Kind {
  const char *name, *tip;
  std::vector<Axis> axes;
};

struct Part {
  const char *name, *title, *tip;
  std::vector<Kind> kinds;
};

using Kinds = std::array<size_t, kNP>; // 各组所选模型 (kParts[组].kinds 下标)
using P = std::vector<float>;          // 一个部件模型的参数 (轴序, 以根计)

inline const std::vector<Axis> kBandAxes = {{"n", {1, 5, 20}, true}, {"k", {0.5, 1, 2}}}; // 带: 周期 + 半宽倍数
inline const std::vector<Axis> kLayerAxes = {{"N", {10}}};                                // 分层: 每侧层数 (固定)

// clang-format off
inline const Part kParts[kNP] = {
    {"c", "中线", "中线 (周期 n)", {
        {"ema", "指数均线, 跟得最匀", {{"n", {1, 5, 20, 60}, true}}},
        {"kama", "考夫曼自适应均线: n = 效率比窗, L = 震荡跨度", {{"n", {1, 5, 20}, true}, {"L", {20, 60}, true}}},
        {"mid", "唐奇安中线 = 近 n 根 (最高 + 最低) / 2, 区间内呈台阶", {{"n", {1, 5, 20, 60}, true}}}}},
    {"w", "带", "上线 / 下线 = 中线 ± k · 半宽; 半宽 (周期 n) = 源 (收盘 / 高低) × 量法 (步长 · √n / 窗内离散)", {
        {"vol", "收 · √n · 对数收益的均方根", kBandAxes},
        {"atr", "收 · √n · 对数真实波幅的均方根 (含跳空与盘中振幅)", kBandAxes},
        {"dev", "收盘偏离中线的均方根 (布林式)", kBandAxes},
        {"rng", "唐奇安半宽 = 近 n 根 (最高 − 最低) / 2", kBandAxes}}},
    {"m", "分层", "跟随网格: 中线 = 空仓, 中线到上线 / 下线各分 N 层; 收每越过一层线开 / 平一份 (1 / N), 跳空越过几层一次开 / 平几份; 到上线满多, 到下线满空; 目标仓位 [−1, 1], 按本根收盘调到", {
        {"arith", "等差: 层距 = 同一价差 (中线两侧各按价格等分)", kLayerAxes},
        {"geo", "等比: 层距 = 同一涨幅 (中线两侧各按对数价等分)", kLayerAxes}}},
};
// clang-format on

// ---- 中线 / 半宽 (取列) ----

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

// 带半宽 (价格单位, 周期 k 根): vol ↔ atr (收 · √k · 均方根 对数收益 / 对数真实波幅) · dev ↔ rng (偏离中线 m 的均方根 / 唐奇安半宽)
inline const Col &width(Tape &t, size_t kind, const P &p, const Col &m) {
  const size_t k = len(p[0]);
  switch (kind) {
  case kVol:
    return t.walk(t.rms(t.lr, k), k);
  case kAtr:
    return t.walk(t.rms(t.lt, k), k);
  case kDev:
    return t.rms(t.sub(t.c, m), k);
  case kRng:
    return t.half(t.top(t.h, k), t.bot(t.l, k));
  }
  assert(false && "带模型");
  std::unreachable();
}

// 一个配置所读的指标列: 中线 + 半宽 (Tape 缓存里的列)
struct Src {
  const Col *m, *s;
};

inline Src src_of(Tape &t, const Kinds &k, const P &pm, const P &ps) {
  const Col &m = center(t, k[kCenter], pm);
  return {&m, &width(t, k[kBand], ps, m)};
}

// ---- 信号核 ----

// 一刀 = 一根的信号: 上线 / 中线 / 下线 (未就绪 = NaN) + 目标仓位
struct Cut {
  float up = kNaN, mid = kNaN, dn = kNaN, w = 0;
};

// 一条轨迹的刀 (列, 与 Cut 同字段)
struct Cuts {
  Col up, mid, dn, w;
  Cuts() = default;
  explicit Cuts(size_t n) : up(n, kNaN), mid(n, kNaN), dn(n, kNaN), w(n, 0) {}
  void put(size_t i, const Cut &x) { up[i] = x.up, mid[i] = x.mid, dn[i] = x.dn, w[i] = x.w; }
  Cut at(size_t i) const { return {up[i], mid[i], dn[i], w[i]}; }
};

// 带就绪: 0 < 下线 < 中线 < 上线
inline bool ready(const Cut &x) { return x.dn > 0 && x.mid > x.dn && x.up > x.mid; }

// 分层的仓位坐标: 收 c 自中线起走向上线 / 下线的几成 s ∈ [−1, 1] (中线 = 0, 上线 = 1, 下线 = −1, 带外截到边); 等差按价格 / 等比按对数价; 带须就绪
inline double pos(bool geo, double c, const Cut &x) {
  const double m = x.mid, e = c >= m ? x.up : x.dn, s = geo ? std::log(c / m) / std::log(e / m) : (c - m) / (e - m);
  return std::clamp(c >= m ? s : -s, -1.0, 1.0);
}

// pos 的逆: 仓位坐标 s ∈ [−1, 1] 处的层线价 (s ≥ 0 走向上线, s < 0 走向下线); 带须就绪
inline double line(bool geo, double s, const Cut &x) {
  const double m = x.mid, e = s >= 0 ? x.up : x.dn, f = std::fabs(s);
  return geo ? m * std::pow(e / m, f) : m + (e - m) * f;
}

// 分层核 (逐根): 读刀 x 的三线 + 本根收 c, 写目标仓位 w
//   跟随网格: 仓位 = 收自中线起越过的层线数 / N (向上 = 多, 向下 = 空); 跳空越过几层一次开 / 平几份; 带未就绪 = 空仓
struct Ladder {
  int N;
  bool geo;
  void operator()(Cut &x, float c) const { x.w = ready(x) ? static_cast<float>(std::trunc(pos(geo, c, x) * N) / N) : 0.0f; }
};

// 分层模型 kind (参数 p = [N]) 的核: arith / geo → Ladder
inline Ladder ladder_of(size_t kind, const P &p) {
  assert((kind == kArith || kind == kGeo) && "分层模型");
  assert(p[0] >= 1 && "分层参数");
  return {static_cast<int>(p[0]), kind == kGeo};
}

// 信号核: 一个配置逐根出一刀; 上线 / 下线 = 中线 ± k · 半宽 → 分层核 (只读 Tape)
struct Signal {
  const Tape &t;
  const Col &m, &s;
  float k;
  Ladder layer;
  Cut operator()(size_t i) const {
    Cut x{m[i] + k * s[i], m[i], m[i] - k * s[i]};
    layer(x, t.c[i]);
    return x;
  }
};

} // namespace cta
