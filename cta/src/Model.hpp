#pragma once

// 单资产时序模型 (只做多), 列式: 有效根 → 指标列 (Tape 缓存, 同资产各配置共用) → 上线 / 下线 → 目标仓位列 [0, 1] (按本根收盘价调到)
//   列 = 逐根一个量, 因果 ([i] 只看 ≤ i 的根); 窗口一律以根计 (日线 / 分钟同一套)
//   未就绪 = NaN: 比较恒假, 预热期不入也不出
//   上线 / 下线 = 判本根用的两条价格线: 收破上线入场 (满仓), 收破下线出场 (空仓); revert 反向 (跌破下线入, 收回上线出)
//   映射: cross 顺势 / dip 逆势 (二值, 同根先出后入) · grid (通道内分 N 档) · chase (突破入场 + 吊灯出场); 成交 / 仓位层 / 成本归 Engine
#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <tuple>
#include <utility>
#include <vector>

using Col = std::vector<float>;

inline constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
inline constexpr size_t kAtrSpan = 20, kVolSpan = 60;

// 一个资产的有效根 (跳过无数据的根)
struct Series {
  Col h, l, c;
  std::vector<uint32_t> t; // [i] → 张量时间下标
};

// ---- 列运算 ----

// 右移 k 根: [i] = x[i − k]
inline Col shifted(const Col &x, size_t k) {
  Col o(x.size(), kNaN);
  for (size_t i = k; i < x.size(); ++i)
    o[i] = x[i - k];
  return o;
}

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

inline constexpr const char *kCenters[] = {"kama", "mid"}, *kWidths[] = {"vol", "rng", "dev"};

// ---- 指标列 ----

// 一个资产: 有效根 + 指标列缓存 (键 = 量 + 窗, 首次取时算)
class Tape {
  enum Q : char { kAgo,
                  kTop,
                  kBot,
                  kHi,
                  kLo,
                  kPeak,
                  kEma,
                  kVol,
                  kAtr,
                  kKama,
                  kDev,
                  kMid,
                  kRng,
                  kVolW,
                  kRms };
  std::map<std::tuple<Q, size_t, size_t>, Col> memo_;

  template <class F>
  const Col &memo(Q q, size_t x, size_t y, F make) {
    const auto key = std::make_tuple(q, x, y);
    if (const auto it = memo_.find(key); it != memo_.end())
      return it->second;
    Col v = make();
    assert(v.size() == n);
    return memo_.emplace(key, std::move(v)).first->second;
  }

  static Col returns(const Col &c) {
    Col a(c.size(), 0);
    for (size_t i = 1; i < c.size(); ++i)
      a[i] = static_cast<float>(static_cast<double>(c[i]) / c[i - 1] - 1);
    return a;
  }

public:
  const size_t n;
  const Col &h, &l, &c;
  const Col a; // 资产逐根收益 (根 0 = 0)

  explicit Tape(const Series &s) : n(s.c.size()), h(s.h), l(s.l), c(s.c), a(returns(s.c)) {}

  const Col &ago(size_t k) {
    return memo(kAgo, k, 0, [&] { return shifted(c, k); });
  } // k 根前收
  const Col &top(size_t k) {
    return memo(kTop, k, 0, [&] { return rolled<std::greater<float>>(h, k); });
  } // 近 k 根最高价 (含本根)
  const Col &bot(size_t k) {
    return memo(kBot, k, 0, [&] { return rolled<std::less<float>>(l, k); });
  } // 近 k 根最低价 (含本根)
  const Col &hi(size_t k) {
    return memo(kHi, k, 0, [&] { return shifted(top(k), 1); });
  } // 前 k 根最高价 (不含本根)
  const Col &lo(size_t k) {
    return memo(kLo, k, 0, [&] { return shifted(bot(k), 1); });
  } // 前 k 根最低价 (不含本根)
  const Col &peak(size_t k) {
    return memo(kPeak, k, 0, [&] { return rolled<std::greater<float>>(c, k); });
  } // 近 k 根最高收
  const Col &ema(size_t k) {
    return memo(kEma, k, 0, [&] { return smoothed(c, k); });
  } // 收的 EMA

  // 每根波动: 对数收益平方的 EMA 开方
  const Col &vol(size_t k) {
    return memo(kVol, k, 0, [&] {
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
    return memo(kAtr, k, 0, [&] {
      Col tr(n);
      for (size_t i = 0; i < n; ++i)
        tr[i] = i ? std::max(h[i], c[i - 1]) - std::min(l[i], c[i - 1]) : h[i] - l[i];
      return smoothed(tr, k);
    });
  }

  // 考夫曼自适应均线: 效率比 ER = |收 − k 根前收| / 近 k 根 Σ|Δ收|; 平滑系数 (ER · (2/3 − α_slow) + α_slow)², α_slow = 2 / (slow + 1)
  //   震荡 (ER → 0) 时几乎不动, 趋势 (ER → 1) 时贴近价格
  const Col &kama(size_t k, size_t slow) {
    return memo(kKama, k, slow, [&] {
      assert(k > 0 && "kama 窗口");
      const double as = 2 / (static_cast<double>(slow) + 1);
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

  // 长期偏离 (慢变量): −ln(收 / EMA_L) / (σ_bar · √L); 正 = 低于长期均值 (便宜), 负 = 高于 (贵)
  const Col &dev(size_t L) {
    return memo(kDev, L, 0, [&] {
      const Col &m = ema(L), &s = vol(kVolSpan);
      const double r = std::sqrt(static_cast<double>(L));
      Col o(n);
      for (size_t i = 0; i < n; ++i) {
        const double v = s[i] * r;
        o[i] = v == 0 ? 0.f : static_cast<float>(-std::log(c[i] / m[i]) / v);
      }
      return o;
    });
  }

  // 唐奇安中线 / 半宽: 近 k 根 (最高 ± 最低) / 2
  const Col &mid(size_t k) {
    return memo(kMid, k, 0, [&] {
      const Col &x = top(k), &y = bot(k);
      Col o(n);
      for (size_t i = 0; i < n; ++i)
        o[i] = (x[i] + y[i]) / 2;
      return o;
    });
  }
  const Col &rng(size_t k) {
    return memo(kRng, k, 0, [&] {
      const Col &x = top(k), &y = bot(k);
      Col o(n);
      for (size_t i = 0; i < n; ++i)
        o[i] = (x[i] - y[i]) / 2;
      return o;
    });
  }

  // band 中线 (周期 k): kama (ER 窗与慢跨度都取 k, 震荡时等效跨度 ~ k² / 2) / mid (区间内呈台阶)
  const Col &center(int kind, size_t k) {
    assert((kind == 0 || kind == 1) && "中线种类");
    return kind == 0 ? kama(k, k) : mid(k);
  }

  // band 宽度 (价格单位, 周期 k): vol (k 根收益的波动 σ_bar · √k · 收) / rng (唐奇安半宽) / dev (偏离中线的均方根 √EMA_k((收 − 中线)²), 布林式)
  const Col &width(int kind, int center_kind, size_t k) {
    switch (kind) {
    case 0:
      return memo(kVolW, k, 0, [&] {
        const Col &s = vol(k);
        const double r = std::sqrt(static_cast<double>(k));
        Col o(n);
        for (size_t i = 0; i < n; ++i)
          o[i] = static_cast<float>(s[i] * r * c[i]);
        return o;
      });
    case 1:
      return rng(k);
    case 2:
      return memo(kRms, k, static_cast<size_t>(center_kind), [&] {
        const Col &m = center(center_kind, k);
        Col d(n);
        for (size_t i = 0; i < n; ++i)
          d[i] = (c[i] - m[i]) * (c[i] - m[i]);
        Col o = smoothed(d, k);
        for (float &x : o)
          x = std::sqrt(x);
        return o;
      });
    }
    assert(false && "宽度种类");
    std::unreachable();
  }
};

// ---- 映射 ----

// 一个配置在一个资产上: 目标仓位 + 判本根用的上线 / 下线 (无 = NaN)
struct Sig {
  Col w, up, dn;
};

// 二值滞回: 在 = (在 ∧ ¬出) ∨ 入 (同根先出后入)
template <class In, class Out>
Col latch(size_t n, In in, Out out) {
  Col w(n, 0);
  bool on = false;
  for (size_t i = 0; i < n; ++i)
    w[i] = on = (on && !out(i)) || in(i);
  return w;
}

// 顺势: 收 > 上线 入, 收 ≤ 下线 出
inline Sig cross(const Tape &t, Col up, Col dn) {
  const Col &c = t.c;
  Col w = latch(t.n, [&](size_t i) { return c[i] > up[i]; }, [&](size_t i) { return c[i] <= dn[i]; });
  return {std::move(w), std::move(up), std::move(dn)};
}

// 逆势: 收 < 下线 入, 收 ≥ 上线 出
inline Sig dip(const Tape &t, Col up, Col dn) {
  const Col &c = t.c;
  Col w = latch(t.n, [&](size_t i) { return c[i] < dn[i]; }, [&](size_t i) { return c[i] >= up[i]; });
  return {std::move(w), std::move(up), std::move(dn)};
}

// 通道分档: 收在 [下线, 上线] 内的位置线性分 N 档; 收 ≤ 下线 空仓, 收 ≥ 上线 满仓 (破网即到位); 通道未就绪 = 空仓
//   网格只平滑仓位变动: 离当前档超过 0.5 + h 档才换档 (N = 1: h = 0 即穿中线进出, h = 0.5 即上线入 / 下线出)
inline Sig grid(const Tape &t, Col up, Col dn, int N, float h) {
  Col w(t.n, 0);
  int L = 0;
  for (size_t i = 0; i < t.n; ++i) {
    if (!(up[i] > dn[i])) {
      L = 0;
    } else {
      const double q = std::clamp((t.c[i] - dn[i]) / static_cast<double>(up[i] - dn[i]), 0.0, 1.0) * N;
      if (q >= N)
        L = N;
      else if (q <= 0)
        L = 0;
      else if (std::fabs(q - L) > 0.5 + h)
        L = static_cast<int>(std::lround(q));
    }
    w[i] = static_cast<float>(L) / static_cast<float>(N);
  }
  return {std::move(w), std::move(up), std::move(dn)};
}

// 吊灯线: 在场以来最高价 − k · ATR; 本根判的是上根的线
struct Stop {
  const Col &c, &h, &atr;
  float k, top = 0, line = kNaN;
  Stop(Tape &t, float k) : c(t.c), h(t.h), atr(t.atr(kAtrSpan)), k(k) {}
  bool hit(size_t i) const { return c[i] <= line; }
  void mark(size_t i, bool stay) {
    top = stay ? std::max(top, h[i]) : h[i];
    line = top - k * atr[i];
  }
};

// 突破 + 吊灯: 不在场时问 in(i) (只在不在场的根上问, 可带状态), 在场时收 ≤ 吊灯线出场 (同根先出后入); 下线 = 在场时的吊灯线
template <class In>
Sig chase(Tape &t, Col up, float k, In in) {
  Stop st(t, k);
  Sig s{Col(t.n, 0), std::move(up), Col(t.n, kNaN)};
  bool on = false;
  for (size_t i = 0; i < t.n; ++i) {
    if (on)
      s.dn[i] = st.line;
    const bool stay = on && !st.hit(i);
    on = stay || in(i);
    if (on)
      st.mark(i, stay);
    s.w[i] = on;
  }
  return s;
}

// ---- 模型 ----

// 买入持有 (基准)
inline Sig hold(const Tape &t) { return {Col(t.n, 1), Col(t.n, kNaN), Col(t.n, kNaN)}; }

// 双均线: EMA(fast) > EMA(slow) 持有 ⇔ 收 > 交叉价 (由两线上根值解出的、使两线本根相等的收盘)
inline Sig ma(Tape &t, size_t fast, size_t slow) {
  const Col &f = t.ema(fast), &s = t.ema(slow);
  const double af = 2 / (static_cast<double>(fast) + 1), as = 2 / (static_cast<double>(slow) + 1);
  Col x(t.n, kNaN);
  for (size_t i = 1; i < t.n; ++i)
    x[i] = static_cast<float>(((1 - as) * s[i - 1] - (1 - af) * f[i - 1]) / (af - as));
  return cross(t, x, x);
}

// 时序动量: 收 > L 根前收 持有
inline Sig tsmom(Tape &t, size_t L) {
  const Col &x = t.ago(L);
  return cross(t, x, x);
}

// 多周期动量集成: 回看 lo · 2^j (j = 0..4), 仓位 = 收 > 回看前收 的周期占比 (最长周期满了才出); 上线 / 下线 = 各回看收的最高 / 最低
inline Sig ens(Tape &t, size_t lo) {
  std::array<const Col *, 5> x;
  for (size_t j = 0; j < x.size(); ++j)
    x[j] = &t.ago(lo << j);
  Sig s{Col(t.n, 0), Col(t.n, kNaN), Col(t.n, kNaN)};
  for (size_t i = 0; i < t.n; ++i) {
    if (std::isnan((*x.back())[i]))
      continue;
    int up = 0;
    float top = (*x[0])[i], bot = top;
    for (const Col *y : x)
      up += t.c[i] > (*y)[i], top = std::max(top, (*y)[i]), bot = std::min(bot, (*y)[i]);
    s.w[i] = static_cast<float>(up) / static_cast<float>(x.size()), s.up[i] = top, s.dn[i] = bot;
  }
  return s;
}

// 考夫曼自适应均线: 收 > KAMA 持有
inline Sig kama(Tape &t, size_t n, size_t slow) {
  const Col &x = t.kama(n, slow);
  return cross(t, x, x);
}

// 唐奇安通道 (海龟): 收破前 n 根最高价入场, 收 ≤ 前 m 根最低价出场
inline Sig donchian(Tape &t, size_t n, size_t m) { return cross(t, t.hi(n), t.lo(m)); }

// 吊灯: 收破前 n 根最高价入场, 收 ≤ 吊灯线出场
inline Sig chandelier(Tape &t, size_t n, float k) {
  const Col &c = t.c, &hi = t.hi(n);
  return chase(t, hi, k, [&](size_t i) { return c[i] > hi[i]; });
}

// 右侧抄底: 收自近 kPeak 根最高收的对数回撤 ≥ d · σ_bar · √kPeak → 待命; 待命中收破前 n 根最高价 → 入场;
//   收 ≤ 吊灯线出场, 之后回到看回撤 (仍够深 = 当根即待命)
inline Sig rebound(Tape &t, float d, size_t n, float k) {
  constexpr size_t kPeak = 250;
  const Col &c = t.c, &hi = t.hi(n), &pk = t.peak(kPeak), &s = t.vol(kVolSpan);
  const double r = std::sqrt(static_cast<double>(kPeak));
  bool armed = false;
  return chase(t, hi, k, [&](size_t i) {
    armed = armed || (s[i] > 0 && std::log(pk[i] / c[i]) >= d * s[i] * r);
    const bool go = armed && c[i] > hi[i];
    armed = armed && !go;
    return go;
  });
}

// 长周期均值回归: 偏离 > g 入场 (跌得够便宜), 偏离 < 0 出场 (回到均值) ⇔ 下线 = EMA_L · e^(−g · σ_bar · √L), 上线 = EMA_L
inline Sig revert(Tape &t, size_t L, float g) {
  const Col &m = t.ema(L), &s = t.vol(kVolSpan);
  const double r = std::sqrt(static_cast<double>(L));
  Col dn(t.n, kNaN);
  for (size_t i = 0; i < t.n; ++i)
    if (s[i] > 0)
      dn[i] = static_cast<float>(m[i] * std::exp(-g * s[i] * r));
  return dip(t, m, std::move(dn));
}

// 慢变量定区间 + 快变量择时: 偏离 > g 的区间内按短周期动量持有, 区间外空仓
//   g < 0 = 只避开偏贵 (摸顶), g > 0 = 只在偏便宜时做 (抄底)
//   动量 m = ln(收 / f 根前收) / (σ_bar · √f), 滞回: m > h 入场, m < −h 出场 ⇔ 上线 / 下线 = f 根前收 · e^(±h · σ_bar · √f)
inline Sig gate(Tape &t, size_t L, float g, size_t f, float h) {
  const Col &x = t.ago(f), &s = t.vol(kVolSpan), &v = t.dev(L);
  const double r = std::sqrt(static_cast<double>(f));
  Col up(t.n, kNaN), dn(t.n, kNaN);
  for (size_t i = 0; i < t.n; ++i)
    if (s[i] > 0) {
      const double e = h * s[i] * r;
      up[i] = static_cast<float>(x[i] * std::exp(e)), dn[i] = static_cast<float>(x[i] * std::exp(-e));
    }
  Sig o = cross(t, std::move(up), std::move(dn));
  for (size_t i = 0; i < t.n; ++i)
    o.w[i] = v[i] > g ? o.w[i] : 0;
  return o;
}

// 带宽网格: 上线 / 下线 = 中线 ± k · 宽度, 按 grid 分 N 档, 档内滞回 h
inline Sig band(Tape &t, int center, int width, size_t n, float k, int N, float h) {
  assert(n > 0 && k > 0 && N > 0 && h >= 0 && h <= 0.5 && "band 参数");
  const Col &m = t.center(center, n), &s = t.width(width, center, n);
  Col up(t.n), dn(t.n);
  for (size_t i = 0; i < t.n; ++i)
    up[i] = m[i] + k * s[i], dn[i] = m[i] - k * s[i];
  return grid(t, std::move(up), std::move(dn), N, h);
}

// 吊灯外挂: 任一模型持仓期间, 收 ≤ 吊灯线即清仓; 之后须底层先给 0 才再入场; 在场时下线 = max(下线, 吊灯线)
inline Sig trail(Tape &t, Sig s, float k) {
  Stop st(t, k);
  bool on = false, armed = true;
  for (size_t i = 0; i < t.n; ++i) {
    if (on)
      s.dn[i] = std::fmax(s.dn[i], st.line);
    const bool hit = on && st.hit(i), stay = on && !hit;
    armed = (armed && !hit) || s.w[i] == 0;
    on = armed && s.w[i] > 0;
    if (on)
      st.mark(i, stay);
    else
      s.w[i] = 0;
  }
  return s;
}
