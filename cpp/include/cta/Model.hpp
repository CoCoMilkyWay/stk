#pragma once

// 单资产只做多 band 模型, 列式: 有效根 → 指标列 (Tape 缓存, 同资产各配置共用) → 带 Band (上线 / 下线) → 仓位 Pos (目标仓位 [0, 1], 按本根收盘价调到)
//   列 = 逐根一个量, 因果 ([i] 只看 ≤ i 的根); 窗口一律以根计 (日线 / 分钟同一套)
//   未就绪 = NaN: 比较恒假, 预热期不入也不出
//   指标列 = 算子作用于源列: 窗算子 ema / rms / top / bot / kama, 逐点算子 mean / half / sub / mul / root; 原料列 = 高 / 低 / 收 / 对数收益 / 真实波幅
//   三组部件, 每组选一个模型 + 该模型自己的参数: 中线 (ema / kama / mid) · 带 (vol / rng / dev / atr, 上线 / 下线 = 中线 ± k · 半宽) · 分层 (grid / fade / chase)
//     中线与半宽对仗: ema ↔ atr (EMA) · mid ↔ rng (近 n 根最高 ± 最低) · kama ↔ vol / dev (均方根)
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
using P = std::vector<float>; // 一个部件模型的参数 (轴序)

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
                            Mul,
                            Root };
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
  static Col ranges(const Col &h, const Col &l, const Col &c) {
    Col o(c.size());
    for (size_t i = 0; i < c.size(); ++i)
      o[i] = i ? std::max(h[i], c[i - 1]) - std::min(l[i], c[i - 1]) : h[i] - l[i];
    return o;
  }

public:
  const size_t n;
  const Col &h, &l, &c; // 高 / 低 / 收
  const Col a, lr;      // 逐根收益 (根 0 = 0) / 对数收益 (根 0 = NaN)
  const Col tr, vol;    // 真实波幅 max(高, 前收) − min(低, 前收) / 每根波动 σ̂ (近 kVolSpan 根对数收益均方根)

  explicit Tape(const Series &s)
      : n(s.c.size()), h(s.h), l(s.l), c(s.c), a(returns(c)), lr(logs(c)), tr(ranges(h, l, c)), vol(rms_of(lr, kVolSpan)) {}
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
  const Col &mul(const Col &x, const Col &y) {
    return memo(Op::Mul, &x, &y, 0, 0, [&] { return zip(x, y, [](float p, float q) { return p * q; }); });
  }
  const Col &root(const Col &x, size_t k) { // x · √k
    return memo(Op::Root, &x, nullptr, k, 0, [&] {
      const float r = std::sqrt(static_cast<float>(k));
      Col o = x;
      for (float &v : o)
        v *= r;
      return o;
    });
  }
};

// ---- 部件与参数 ----
//   三组部件, 每组选一个模型; 各模型自带参数轴 (网格取值); 单值轴 = 固定参数, 自动优化不动它, 要换就改这里
//   以日计的轴经 to_bars 乘每日根数, 模型函数拿到的都是根

enum : size_t { kCenter, // 中线
                kBand,   // 带
                kLayer,  // 分层
                kNP };

// 各组模型 (下标同 kParts[组].kinds)
enum : size_t { kEma,
                kKama,
                kMid }; // 中线
enum : size_t { kVol,
                kRng,
                kDev,
                kAtr }; // 带
enum : size_t { kGrid,
                kFade,
                kChase }; // 分层

struct Axis {
  const char *name;
  std::vector<float> v;
  bool day = false; // 以日计
};

struct Kind {
  const char *name;
  std::vector<Axis> axes;
};

struct Part {
  const char *name, *title;
  std::vector<Kind> kinds;
};

inline const std::vector<Axis> kBandAxes = {{"n", {1, 5, 20}, true}, {"k", {0.5, 1, 2}}}; // 带: 周期 + 半宽倍数
inline const std::vector<Axis> kLayerAxes = {{"N", {20}}, {"h", {0, 0.1}}};               // 分层: 层数 (固定) + 滞回

// clang-format off
inline const Part kParts[kNP] = {
    {"c", "中线", {{"ema", {{"n", {1, 5, 20, 60}, true}}},
                   {"kama", {{"n", {1, 5, 20}, true}, {"L", {20, 60}, true}}},
                   {"mid", {{"n", {1, 5, 20, 60}, true}}}}},
    {"w", "带", {{"vol", kBandAxes}, {"rng", kBandAxes}, {"dev", kBandAxes}, {"atr", kBandAxes}}},
    {"m", "分层", {{"grid", kLayerAxes}, {"fade", kLayerAxes}, {"chase", {}}}},
};
// clang-format on

// 一个配置: 各组的模型下标 + 该模型的参数
struct Cfg {
  std::array<size_t, kNP> k{};
  std::array<P, kNP> p{};
};

inline const Axis &axis_of(size_t part, size_t kind, size_t i) { return kParts[part].kinds[kind].axes[i]; }

// 轴单位 → 根: 以日计的轴乘每日根数 S
inline Cfg to_bars(Cfg g, double S) {
  for (size_t j = 0; j < kNP; ++j) {
    assert(g.p[j].size() == kParts[j].kinds[g.k[j]].axes.size() && "参数个数与参数轴不符");
    for (size_t i = 0; i < g.p[j].size(); ++i)
      if (axis_of(j, g.k[j], i).day)
        g.p[j][i] *= static_cast<float>(S);
  }
  return g;
}

// 组合约束 (以根计): 中线 / 带的周期 ≥ 5 根
inline bool admit(const Cfg &g) { return g.p[kCenter][0] >= 5 && g.p[kBand][0] >= 5; }

// ---- 中线 / 带 ----

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

// 带半宽 (价格单位, 周期 k 根): vol (σ · 收 · √k) / rng (唐奇安半宽) / dev (偏离中线 m 的均方根, 布林式) / atr (ATR · √k, 含跳空与盘中振幅)
inline const Col &width(Tape &t, size_t kind, const P &p, const Col &m) {
  const size_t k = len(p[0]);
  switch (kind) {
  case kVol:
    return t.root(t.mul(t.rms(t.lr, k), t.c), k);
  case kRng:
    return t.half(t.top(t.h, k), t.bot(t.l, k));
  case kDev:
    return t.rms(t.sub(t.c, m), k);
  case kAtr:
    return t.root(t.ema(t.tr, k), k);
  }
  assert(false && "带模型");
  std::unreachable();
}

// 带: 上线 / 下线 (未就绪 = NaN)
struct Band {
  Col up, dn;
};

// 仓位: 目标仓位 + 吊灯线 (判本根用的; 无 = NaN)
struct Pos {
  Col w, stop;
};

// ---- 分层 ----

// 分层: 收在 [下线, 上线] 内的位置线性分 N 层 (fade 反向: 越低越满); 破网即到位; 通道未就绪 = 空仓
//   层只平滑仓位变动: 离当前层超过 0.5 + h · N 层才换层 (h 以通道全宽计, 带内小幅来回不动仓)
inline Pos grid(const Tape &t, const Band &b, int N, float h, bool fade) {
  assert(N > 0 && h >= 0 && "分层参数");
  Pos o{Col(t.n, 0), Col(t.n, kNaN)};
  int L = 0;
  for (size_t i = 0; i < t.n; ++i) {
    if (!(b.up[i] > b.dn[i])) {
      L = 0;
    } else {
      const double x = std::clamp((t.c[i] - b.dn[i]) / static_cast<double>(b.up[i] - b.dn[i]), 0.0, 1.0), q = (fade ? 1 - x : x) * N;
      if (q >= N)
        L = N;
      else if (q <= 0)
        L = 0;
      else if (std::fabs(q - L) > 0.5 + h * N)
        L = static_cast<int>(std::lround(q));
    }
    o.w[i] = static_cast<float>(L) / static_cast<float>(N);
  }
  return o;
}

// 突破 + 吊灯: 收 > 上线入场; 在场时收 ≤ 吊灯线出场 (同根先出后入)
//   吊灯线 = 在场以来最高价 − 通道全宽 (上线 − 下线), 本根判的是上根的线; 入场时即在下线之上
inline Pos chase(const Tape &t, const Band &b) {
  Pos o{Col(t.n, 0), Col(t.n, kNaN)};
  bool on = false;
  float top = 0, line = kNaN;
  for (size_t i = 0; i < t.n; ++i) {
    if (on)
      o.stop[i] = line;
    const bool stay = on && !(t.c[i] <= line);
    on = stay || t.c[i] > b.up[i];
    if (on) {
      top = stay ? std::max(top, t.h[i]) : t.h[i];
      line = top - (b.up[i] - b.dn[i]);
    }
    o.w[i] = on;
  }
  return o;
}

// 分层模型: grid / fade [N, h] · chase
inline Pos layer(const Tape &t, size_t kind, const P &p, const Band &b) {
  switch (kind) {
  case kGrid:
    return grid(t, b, static_cast<int>(p[0]), p[1], false);
  case kFade:
    return grid(t, b, static_cast<int>(p[0]), p[1], true);
  case kChase:
    return chase(t, b);
  }
  assert(false && "分层模型");
  std::unreachable();
}

// ---- 配置 ----

// 一个配置在一个资产上: 带 + 仓位
struct Sig {
  Band band;
  Pos pos;
};

// 一个配置所读的指标列: 中线 + 半宽 (Tape 缓存里的列)
struct Src {
  const Col *m, *s;
};

inline Src src_of(Tape &t, const Cfg &g) {
  const Col &m = center(t, g.k[kCenter], g.p[kCenter]);
  return {&m, &width(t, g.k[kBand], g.p[kBand], m)};
}

// 上线 / 下线 = 中线 ± k · 半宽, 再按分层给仓位 (只读 Tape)
inline Sig sig_of(const Tape &t, const Cfg &g, const Src &x) {
  const float k = g.p[kBand][1];
  assert(k > 0 && "带半宽倍数");
  const Col &m = *x.m, &s = *x.s;
  Band b{Col(t.n), Col(t.n)};
  for (size_t i = 0; i < t.n; ++i)
    b.up[i] = m[i] + k * s[i], b.dn[i] = m[i] - k * s[i];
  Pos p = layer(t, g.k[kLayer], g.p[kLayer], b);
  return {std::move(b), std::move(p)};
}

// 买入持有 (基准)
inline Col hold(const Tape &t) { return Col(t.n, 1); }

} // namespace cta
