#pragma once

// 单资产只做多 band 模型, 列式: 有效根 → 指标列 (Tape 缓存, 同资产各配置共用) → 上线 / 下线 → 目标仓位列 [0, 1] (按本根收盘价调到)
//   列 = 逐根一个量, 因果 ([i] 只看 ≤ i 的根); 窗口一律以根计 (日线 / 分钟同一套)
//   未就绪 = NaN: 比较恒假, 预热期不入也不出
//   三组部件, 每组选一个模型 + 该模型自己的参数: 中线 (ema / kama / mid) · 带 (vol / rng / dev / atr, 上线 / 下线 = 中线 ± k · 半宽) · 分层 (grid / fade / chase)
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

// ---- 列运算 ----

// 指数均线, 跨度 k (α = 2 / (k + 1)): 首个有效值即初值, NaN 跳过; 满 k 个有效值前 NaN
inline Col smoothed(const Col &x, size_t k) {
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

// 近 k 根 (含本根) 极值: 单调队列
template <class Keep>
Col rolled(const Col &x, size_t k) {
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

// ---- 指标列 ----

// 一个资产: 有效根 + 指标列缓存 (键 = 量 + 窗等参数, 首次取时算)
class Tape {
  enum Q : char { kTop,
                  kBot,
                  kEma,
                  kVol,
                  kAtr,
                  kKama,
                  kMid,
                  kRng,
                  kVolW,
                  kAtrW,
                  kRms };
  std::map<std::pair<Q, std::vector<size_t>>, Col> memo_;

  template <class F>
  const Col &memo(Q q, std::vector<size_t> x, F make) {
    auto key = std::make_pair(q, std::move(x));
    if (const auto it = memo_.find(key); it != memo_.end())
      return it->second;
    Col v = make();
    assert(v.size() == n);
    return memo_.emplace(std::move(key), std::move(v)).first->second;
  }

  static Col returns(const Col &c) {
    Col a(c.size(), 0);
    for (size_t i = 1; i < c.size(); ++i)
      a[i] = static_cast<float>(static_cast<double>(c[i]) / c[i - 1] - 1);
    return a;
  }

  // 每根量 × √k × 尺度 (价格单位)
  Col scaled(const Col &s, size_t k, bool price) {
    const double r = std::sqrt(static_cast<double>(k));
    Col o(n);
    for (size_t i = 0; i < n; ++i)
      o[i] = static_cast<float>(s[i] * r * (price ? c[i] : 1.f));
    return o;
  }

public:
  const size_t n;
  const Col &h, &l, &c;
  const Col a; // 资产逐根收益 (根 0 = 0)

  explicit Tape(const Series &s) : n(s.c.size()), h(s.h), l(s.l), c(s.c), a(returns(s.c)) {}

  const Col &top(size_t k) {
    return memo(kTop, {k}, [&] { return rolled<std::greater<float>>(h, k); });
  } // 近 k 根最高价 (含本根)
  const Col &bot(size_t k) {
    return memo(kBot, {k}, [&] { return rolled<std::less<float>>(l, k); });
  } // 近 k 根最低价 (含本根)
  const Col &ema(size_t k) {
    return memo(kEma, {k}, [&] { return smoothed(c, k); });
  } // 收的 EMA

  // 每根波动: 对数收益平方的 EMA 开方
  const Col &vol(size_t k) {
    return memo(kVol, {k}, [&] {
      Col r2(n, kNaN);
      for (size_t i = 1; i < n; ++i) {
        const double r = std::log(static_cast<double>(c[i]) / c[i - 1]);
        r2[i] = static_cast<float>(r * r);
      }
      Col o = smoothed(r2, k);
      for (float &x : o)
        x = std::sqrt(x);
      return o;
    });
  }

  // 平均真实波幅: max(高, 前收) − min(低, 前收) 的 EMA
  const Col &atr(size_t k) {
    return memo(kAtr, {k}, [&] {
      Col tr(n);
      for (size_t i = 0; i < n; ++i)
        tr[i] = i ? std::max(h[i], c[i - 1]) - std::min(l[i], c[i - 1]) : h[i] - l[i];
      return smoothed(tr, k);
    });
  }

  // 考夫曼自适应均线: 效率比 ER = |收 − k 根前收| / 近 k 根 Σ|Δ收|; 平滑系数 (ER · (2/3 − α_slow) + α_slow)²
  //   α_slow = √(2 / (L + 1)): 震荡 (ER → 0) 时等效 EMA 跨度 L, 趋势 (ER → 1) 时贴近价格
  const Col &kama(size_t k, size_t L) {
    return memo(kKama, {k, L}, [&] {
      assert(k > 0 && L > 3 && "kama 窗口 / 震荡跨度");
      const double as = std::sqrt(2 / (static_cast<double>(L) + 1));
      Col o(n, kNaN);
      double sum = 0, v = c[0];
      for (size_t i = 1; i < n; ++i) {
        sum += std::fabs(c[i] - c[i - 1]);
        if (i > k)
          sum -= std::fabs(c[i - k] - c[i - k - 1]);
        if (i < k) {
          v = c[i];
          continue;
        }
        const double er = sum > 0 ? std::fabs(c[i] - c[i - k]) / sum : 0, sc = er * (2 / 3.0 - as) + as;
        o[i] = static_cast<float>(v += sc * sc * (c[i] - v));
      }
      return o;
    });
  }

  // 唐奇安中线 / 半宽: 近 k 根 (最高 ± 最低) / 2
  const Col &mid(size_t k) {
    return memo(kMid, {k}, [&] {
      const Col &x = top(k), &y = bot(k);
      Col o(n);
      for (size_t i = 0; i < n; ++i)
        o[i] = (x[i] + y[i]) / 2;
      return o;
    });
  }
  const Col &rng(size_t k) {
    return memo(kRng, {k}, [&] {
      const Col &x = top(k), &y = bot(k);
      Col o(n);
      for (size_t i = 0; i < n; ++i)
        o[i] = (x[i] - y[i]) / 2;
      return o;
    });
  }

  // 中线 (参数以根计): ema [n] (跟得最匀) / kama [n = ER 窗, L = 震荡跨度] / mid [n] (唐奇安中线, 区间内呈台阶)
  const Col &center(size_t kind, const P &p) {
    switch (kind) {
    case 0:
      return ema(len(p[0]));
    case 1:
      return kama(len(p[0]), len(p[1]));
    case 2:
      return mid(len(p[0]));
    }
    assert(false && "中线模型");
    std::unreachable();
  }

  // 带半宽单位 (价格单位, 周期 k 根): vol (σ_bar · √k · 收) / rng (唐奇安半宽) / dev (偏离中线的均方根 √EMA_k((收 − 中线)²), 布林式)
  //   / atr (ATR_k · √k, 含跳空与盘中振幅); dev 要中线 (ck, cp)
  const Col &width(size_t kind, size_t k, size_t ck, const P &cp) {
    switch (kind) {
    case 0:
      return memo(kVolW, {k}, [&] { return scaled(vol(k), k, true); });
    case 1:
      return rng(k);
    case 2: {
      std::vector<size_t> key = {k, ck};
      for (const float x : cp)
        key.push_back(len(x));
      return memo(kRms, std::move(key), [&] {
        const Col &m = center(ck, cp);
        Col d(n);
        for (size_t i = 0; i < n; ++i)
          d[i] = (c[i] - m[i]) * (c[i] - m[i]);
        Col o = smoothed(d, k);
        for (float &x : o)
          x = std::sqrt(x);
        return o;
      });
    }
    case 3:
      return memo(kAtrW, {k}, [&] { return scaled(atr(k), k, false); });
    }
    assert(false && "带模型");
    std::unreachable();
  }
};

// ---- 分层 ----

// 一个配置在一个资产上: 目标仓位 + 上线 / 下线 + 吊灯线 (判本根用的; 无 = NaN)
struct Sig {
  Col w, up, dn, stop;
};

// 分层: 收在 [下线, 上线] 内的位置线性分 N 层 (fade 反向: 越低越满); 破网即到位; 通道未就绪 = 空仓
//   层只平滑仓位变动: 离当前层超过 0.5 + h · N 层才换层 (h 以通道全宽计, 带内小幅来回不动仓)
inline Sig grid(const Tape &t, Col up, Col dn, int N, float h, bool fade) {
  assert(N > 0 && h >= 0 && "分层参数");
  Col w(t.n, 0);
  int L = 0;
  for (size_t i = 0; i < t.n; ++i) {
    if (!(up[i] > dn[i])) {
      L = 0;
    } else {
      const double x = std::clamp((t.c[i] - dn[i]) / static_cast<double>(up[i] - dn[i]), 0.0, 1.0), q = (fade ? 1 - x : x) * N;
      if (q >= N)
        L = N;
      else if (q <= 0)
        L = 0;
      else if (std::fabs(q - L) > 0.5 + h * N)
        L = static_cast<int>(std::lround(q));
    }
    w[i] = static_cast<float>(L) / static_cast<float>(N);
  }
  return {std::move(w), std::move(up), std::move(dn), Col(t.n, kNaN)};
}

// 突破 + 吊灯: 收 > 上线入场; 在场时收 ≤ 吊灯线出场 (同根先出后入)
//   吊灯线 = 在场以来最高价 − 通道全宽 (上线 − 下线), 本根判的是上根的线; 入场时即在下线之上
inline Sig chase(const Tape &t, Col up, Col dn) {
  Sig s{Col(t.n, 0), std::move(up), std::move(dn), Col(t.n, kNaN)};
  bool on = false;
  float top = 0, line = kNaN;
  for (size_t i = 0; i < t.n; ++i) {
    if (on)
      s.stop[i] = line;
    const bool stay = on && !(t.c[i] <= line);
    on = stay || t.c[i] > s.up[i];
    if (on) {
      top = stay ? std::max(top, t.h[i]) : t.h[i];
      line = top - (s.up[i] - s.dn[i]);
    }
    s.w[i] = on;
  }
  return s;
}

// ---- 模型 ----

// 买入持有 (基准)
inline Col hold(const Tape &t) { return Col(t.n, 1); }

// ---- 部件与参数 ----
//   三组部件, 每组选一个模型; 各模型自带参数轴 (网格取值); 单值轴 = 固定参数, 自动优化不动它, 要换就改这里
//   以日计的轴经 to_bars 乘每日根数, 模型函数拿到的都是根

enum : size_t { kCenter, // 中线
                kBand,   // 带
                kLayer,  // 分层
                kNP };

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

// band: 上线 / 下线 = 中线 ± k · 半宽, 再按分层给仓位
inline Sig band(Tape &t, const Cfg &g) {
  const P &cp = g.p[kCenter], &wp = g.p[kBand], &mp = g.p[kLayer];
  const float k = wp[1];
  assert(k > 0 && "带半宽倍数");
  const Col &m = t.center(g.k[kCenter], cp), &s = t.width(g.k[kBand], len(wp[0]), g.k[kCenter], cp);
  Col up(t.n), dn(t.n);
  for (size_t i = 0; i < t.n; ++i)
    up[i] = m[i] + k * s[i], dn[i] = m[i] - k * s[i];
  switch (g.k[kLayer]) {
  case 0:
    return grid(t, std::move(up), std::move(dn), static_cast<int>(mp[0]), mp[1], false);
  case 1:
    return grid(t, std::move(up), std::move(dn), static_cast<int>(mp[0]), mp[1], true);
  case 2:
    return chase(t, std::move(up), std::move(dn));
  }
  assert(false && "分层模型");
  std::unreachable();
}

} // namespace cta
