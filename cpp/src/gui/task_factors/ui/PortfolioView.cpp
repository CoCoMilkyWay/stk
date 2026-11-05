// Factors→Portfolio 派生层 — 见头文件
#include "gui/task_factors/ui/PortfolioView.hpp"

#include "gui/task_factors/ui/TabFactors.hpp" // hold_desc
#include "misc/date.hpp"                      // to_days (滚动窗权重按日历天)
#include "shared/AssetAxis.hpp"               // asset_axis (成分行代码)
#include "shared/Basket.hpp"                  // basket_group / basket_name / cut_code

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <map>
#include <numeric>
#include <vector>

namespace GUI::Factors {

namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

// 篮子行 id = pf_ + 切片代码 + 权重后缀 [Weight]
constexpr const char *kWeightId[kWeights] = {"", "_cap"};
std::string basket_id(size_t line) { return "pf_" + cut_code(cut_of_line(line)) + kWeightId[weight_of(line)]; }

// 行名主体的显示宽上限 (格: ASCII 1, 其余 2): 超出 → 截到 kNameCells − 2 格补 "…" (表 / 图例 / 线尾同用; 全名进悬停)
constexpr int kNameCells = 14;
std::string clip_name(const std::string &s) {
  int w = 0;
  size_t cut = 0;
  for (size_t i = 0; i < s.size();) {
    const unsigned char c = static_cast<unsigned char>(s[i]);
    i += c < 0x80 ? 1 : c < 0xE0 ? 2
                    : c < 0xF0   ? 3
                                 : 4;
    w += c < 0x80 ? 1 : 2;
    if (w <= kNameCells - 2)
      cut = i;
  }
  return w <= kNameCells ? s : s.substr(0, cut) + "…";
}

// ---- 行配色 (策略外四组一起, 见 PortfolioDerived::rows): 相似度 s = max(成员重合, 残差相关), 平方距离 2 (1 − s) 上 Ward 树定叶序;
//      叶序相邻两行的色相差 ∝ 它们在树上分开处两侧的平均距离 (1 − s), 累加后铺满色环的前 kArc (首尾不相接) ----
constexpr size_t kSimDays = 60; // 残差相关取日并集轴末几日的日收益
constexpr size_t kSimMin = 20;  // 两行共同有收益的日数下限 (不足 → 相关记 0)
constexpr float kArc = 0.85f;   // 色相铺开的弧长 (整环 = 1)

// 线 c 在日并集轴 [lo, lo + n) 各日的日收益 (日末视图逐日比 − 1; 不可算 = NaN)
std::vector<double> rets_of(const PortfolioLine &c, size_t lo, size_t n) {
  std::vector<double> r(n);
  for (size_t k = 0; k < n; ++k) {
    const int d = static_cast<int>(lo + k);
    const double a = c.end(d), b = c.end(d + 1);
    r[k] = a > 0 && b > 0 ? b / a - 1.0 : kNaN;
  }
  return r;
}
// r 对 m 在共同日上回归 (带截距) 的残差 (共同日外 NaN); 共同日不足 kSimMin → 全 NaN
void residual(std::vector<double> &r, const std::vector<double> &m) {
  double n = 0, sr = 0, sm = 0, srm = 0, smm = 0;
  for (size_t k = 0; k < r.size(); ++k)
    if (std::isfinite(r[k]) && std::isfinite(m[k]))
      ++n, sr += r[k], sm += m[k], srm += r[k] * m[k], smm += m[k] * m[k];
  const double vm = n >= kSimMin ? smm - sm * sm / n : 0.0;
  if (!(vm > 0)) {
    std::fill(r.begin(), r.end(), kNaN);
    return;
  }
  const double b = (srm - sr * sm / n) / vm, a = (sr - b * sm) / n;
  for (size_t k = 0; k < r.size(); ++k)
    r[k] = std::isfinite(r[k]) && std::isfinite(m[k]) ? r[k] - a - b * m[k] : kNaN;
}
// 两条残差在共同日上的相关 (残差已零均值); 共同日不足 kSimMin / 无波动 → 0
double corr(const std::vector<double> &a, const std::vector<double> &b) {
  size_t n = 0;
  double ab = 0, aa = 0, bb = 0;
  for (size_t k = 0; k < a.size(); ++k)
    if (std::isfinite(a[k]) && std::isfinite(b[k]))
      ++n, ab += a[k] * b[k], aa += a[k] * a[k], bb += b[k] * b[k];
  return n >= kSimMin && aa > 0 && bb > 0 ? ab / std::sqrt(aa * bb) : 0.0;
}

// Ward 连接 (最近邻链, O(n²)): 平方距离 dist [n × n] 就地改写 → n − 1 次合并 (两簇各取一成员 a / b, 合并高 h), 按 h 升序.
// 合并代价 = 簇内平方和的增量, 偏好大小相近的簇 (不会一簇吞掉大半)
struct Merge {
  size_t a, b;
  double h;
};
std::vector<Merge> ward_linkage(std::vector<double> &dist, size_t n) {
  std::vector<size_t> sz(n, 1), chain;
  std::vector<uint8_t> live(n, 1);
  std::vector<Merge> ms;
  while (ms.size() + 1 < n) {
    if (chain.empty())
      chain.push_back(static_cast<size_t>(std::find(live.begin(), live.end(), 1) - live.begin()));
    const size_t a = chain.back(), p = chain.size() > 1 ? chain[chain.size() - 2] : n;
    size_t b = p; // 同距优先链上前一个 (否则可能来回不收)
    for (size_t c = 0; c < n; ++c)
      if (live[c] && c != a && (b == n || dist[a * n + c] < dist[a * n + b]))
        b = c;
    if (b != p) {
      chain.push_back(b);
      continue;
    }
    chain.resize(chain.size() - 2);
    ms.push_back({a, b, dist[a * n + b]});
    const double wa = static_cast<double>(sz[a]), wb = static_cast<double>(sz[b]), dab = dist[a * n + b];
    for (size_t c = 0; c < n; ++c)
      if (live[c] && c != a && c != b) {
        const double wc = static_cast<double>(sz[c]);
        dist[b * n + c] = dist[c * n + b] = ((wa + wc) * dist[a * n + c] + (wb + wc) * dist[b * n + c] - wc * dab) / (wa + wb + wc);
      }
    sz[b] += sz[a], live[a] = 0;
  }
  std::stable_sort(ms.begin(), ms.end(), [](const Merge &x, const Merge &y) { return x.h < y.h; });
  return ms;
}

// 指数 / 行业 / 概念 / 因子四组各行定色相 (见上); 因子没到齐 / 价格指数未就绪 / 日轴不足两日 → 全不定色 (灰, 免得边算边跳)
void paint(PortfolioDerived &d) {
  std::vector<PortfolioRow *> rs;
  size_t mkt = SIZE_MAX; // 全A.加 (残差的基准)
  for (int g = 0; g < kGroupStrat; ++g)
    for (PortfolioRow &r : d.rows[g]) {
      if (r.q.is(Database::kBasket) && r.q.key == basket_line(0, kWeightCap))
        mkt = rs.size();
      rs.push_back(&r);
      r.hue = std::numeric_limits<float>::quiet_NaN();
    }
  const size_t n = rs.size(), D = d.dates.size();
  assert(mkt < n);
  if (d.n_wait > 0 || !d.prices || D < 2)
    return;

  const size_t T = std::min(kSimDays, D - 1), lo = D - T;
  const std::vector<double> m = rets_of(*rs[mkt]->line[kIdeal], lo, T);
  std::vector<std::vector<double>> res(n); // [行] 残差日收益
  for (size_t i = 0; i < n; ++i) {
    res[i] = rets_of(*rs[i]->line[kIdeal], lo, T);
    residual(res[i], m);
  }
  std::fill(res[mkt].begin(), res[mkt].end(), kNaN); // 自身对自身回归无残差
  const size_t K = d.prices->keys.size();
  assert(d.prices->overlap.size() == K * K);
  std::vector<double> far(n * n, 0.0); // [行 × 行] 1 − s
  for (size_t i = 0; i < n; ++i)
    for (size_t j = i + 1; j < n; ++j) {
      double s = corr(res[i], res[j]);
      if (const PortfolioQuote &a = rs[i]->q, &b = rs[j]->q; a.is(Database::kBasket) && b.is(Database::kBasket))
        s = std::max(s, static_cast<double>(d.prices->overlap[a.key * K + b.key]));
      far[i * n + j] = far[j * n + i] = 1.0 - s;
    }

  // Ward 树: 结点 < n = 叶, n + k = 第 k 次合并; 子先于父 ⇒ 按号自底向上数叶数, 自顶向下定各结点叶段起点 (叶序 = 先左子后右子)
  std::vector<double> sq(n * n);
  std::transform(far.begin(), far.end(), sq.begin(), [](double x) { return 2.0 * x; });
  const std::vector<Merge> ms = ward_linkage(sq, n);
  std::vector<size_t> up(n), node(n);
  std::iota(up.begin(), up.end(), 0), std::iota(node.begin(), node.end(), 0);
  const auto find = [&](size_t x) {
    while (up[x] != x)
      x = up[x] = up[up[x]];
    return x;
  };
  std::vector<std::array<size_t, 2>> kids; // [结点 − n] 两子
  for (const Merge &mg : ms) {
    const size_t a = find(mg.a), b = find(mg.b);
    kids.push_back({node[a], node[b]});
    up[a] = b, node[b] = n + kids.size() - 1;
  }
  const size_t N = 2 * n - 1;
  std::vector<size_t> cnt(N, 1), at(N, 0); // [结点] 叶数 / 叶段起点
  for (size_t x = n; x < N; ++x)
    cnt[x] = cnt[kids[x - n][0]] + cnt[kids[x - n][1]];
  for (size_t x = N; x-- > n;)
    at[kids[x - n][0]] = at[x], at[kids[x - n][1]] = at[x] + cnt[kids[x - n][0]];
  std::vector<size_t> leaf(n); // [叶序] 行
  for (size_t i = 0; i < n; ++i)
    leaf[at[i]] = i;
  // 叶序第 k / k + 1 行之间的间隔 = 在 k + 1 处分开左右两子的那个结点的两侧平均 1 − s (各对行只在其最近公共祖先处算一次, 共 O(n²))
  std::vector<double> t(n, 0.0); // [叶序] 累加位置
  for (size_t x = n; x < N; ++x) {
    const size_t l0 = at[x], l1 = l0 + cnt[kids[x - n][0]], l2 = l0 + cnt[x];
    double sum = 0;
    for (size_t p = l0; p < l1; ++p)
      for (size_t q = l1; q < l2; ++q)
        sum += far[leaf[p] * n + leaf[q]];
    t[l1] = sum / static_cast<double>((l1 - l0) * (l2 - l1));
  }
  for (size_t k = 1; k < n; ++k)
    t[k] += t[k - 1];
  const double span = t[n - 1];
  for (size_t i = 0; i < n; ++i)
    rs[i]->hue = span > 0 ? kArc * static_cast<float>(t[at[i]] / span) : 0.0f;
}

// src (升序) 各日在 axis (升序) 上的 lower_bound 下标 (双指针)
std::vector<size_t> days_on(const std::vector<misc::Ymd> &axis, const std::vector<misc::Ymd> &src) {
  std::vector<size_t> di(src.size());
  size_t j = 0;
  for (size_t i = 0; i < src.size(); ++i) {
    while (j < axis.size() && axis[j] < src[i])
      ++j;
    di[i] = j;
  }
  return di;
}

// ---- 线的来路 (两种线源: 腿集合成 mix / 价格指数 levels): 源 → 点位 Levels → project (日并集轴) → PortfolioLine ----
// 源点位: 点 i 在日 date[i] 的日内分钟 min[i], 点位 lv[i]; 首点 = 首日开盘前 kStartDx 格的起点 (前收)
struct Levels {
  std::vector<misc::Ymd> date;
  std::vector<double> min, lv;
  void push(misc::Ymd d, double m, double v) { date.push_back(d), min.push_back(m), lv.push_back(v); }
};

// 腿 (fold_book 已实现口径, 与池指数对齐): 行 t 的增量 (cost: 毛 − 冲击 − 固定份数 · fixed_impact; 否则 = 毛; 账无成本列 → 恒 = 毛) /
// 增仓量 (账无增仓列 → 0)
struct BookLine {
  const float *c[kBookCols];
  explicit BookLine(const PortfolioLeg &lg) {
    for (int k = 0; k < kBookCols; ++k)
      c[k] = lg.b->at(k, lg.line);
    assert(!c[kBookImp] == !c[kBookFix] && "成本两列同有同无");
  }
  double inc(size_t t, bool cost, double fixed_impact) const {
    const double r = c[kBookRet][t];
    return cost && c[kBookImp] ? r - c[kBookImp][t] - c[kBookFix][t] * fixed_impact : r;
  }
  double buy(size_t t) const { return c[kBookBuy] ? c[kBookBuy][t] : 0.0; }
};

// 腿集合成 (线源 s 的 legs): 稠密格 [日并集轴][M] (M = 各腿层的行末分钟之并), 腿的行 t 落进 (所在日, 行末分钟) 的格;
//   腿的日在轴范围外 → 弃, 范围内而轴上没有 → 整日并进下一个轴上的日的该腿首格 (同 project: 收益并进下一个轴上的日);
//   同格同腿复利, 跨腿按份额 1 / 当日有数据的腿数加和 ⇒ 一腿 = 该账线自身逐行复利
// 点位 = 首个有增量的格所在日开盘前的起点 1 + 有增量的格逐格复利; 年换手 = 增仓量同份额累加到日, 自首个有增仓的日起在有腿的日上的日均 × 每年交易日
struct Mix {
  Levels lv;
  double turn = kNaN; // 倍/年; 无增仓 = NaN
};
Mix mix(const std::vector<misc::Ymd> &axis, const PortfolioLineSrc &s) {
  const std::vector<PortfolioLeg> &legs = s.legs;
  const size_t D = axis.size();
  assert(D > 0);
  std::array<int, kDayW> slot; // [日内分钟] 槽 (−1 = 无腿在此收行)
  slot.fill(-1);
  for (const PortfolioLeg &lg : legs) {
    const factor::Geo g = factor_level(lg.b->lv).geo();
    for (int ts = 0; ts < g.S; ++ts)
      slot[static_cast<size_t>(g.minute(ts))] = 0;
  }
  std::vector<int> mins; // [槽] 日内分钟
  for (size_t m = 0; m < kDayW; ++m)
    if (slot[m] == 0)
      slot[m] = static_cast<int>(mins.size()), mins.push_back(static_cast<int>(m));
  const size_t M = mins.size(), X = D * M;
  std::vector<std::vector<size_t>> dix(legs.size()); // [腿][腿的日] → 并入的轴日 (SIZE_MAX = 范围外)
  std::vector<int> n_act(D, 0);                      // [轴日] 当日有数据的腿数
  for (size_t q = 0; q < legs.size(); ++q) {
    const std::vector<misc::Ymd> &ds = legs[q].b->dates;
    dix[q] = days_on(axis, ds);
    for (size_t i = 0, last = SIZE_MAX; i < ds.size(); ++i) {
      size_t &d = dix[q][i];
      if (ds[i] < axis.front() || ds[i] > axis.back()) {
        d = SIZE_MAX;
      } else if (d != last) {
        ++n_act[d];
        last = d;
      }
    }
  }
  std::vector<double> acc(X, 0.0);
  std::vector<uint8_t> hit(X, 0);
  std::vector<double> buy(D, 0.0);
  for (size_t q = 0; q < legs.size(); ++q) {
    const PortfolioBook &b = *legs[q].b;
    const factor::Geo g = factor_level(b.lv).geo();
    const size_t S = static_cast<size_t>(g.S);
    assert(static_cast<size_t>(b.col[kBookRet].P) == b.dates.size() * S);
    const BookLine bl(legs[q]);
    size_t at = SIZE_MAX; // 在攒的格
    double k = 0.0;       // 其份额
    double run = 0.0;     // 本腿在该格的复利增长 Π (1 + r) − 1
    const auto flush = [&] {
      if (at == SIZE_MAX)
        return;
      acc[at] += run * k, run = 0.0;
      hit[at] = 1;
    };
    for (size_t i = 0; i < b.dates.size(); ++i) {
      const size_t d = dix[q][i];
      if (d == SIZE_MAX)
        continue;
      const bool on = axis[d] == b.dates[i];
      for (size_t ts = 0; ts < S; ++ts) {
        const size_t t = i * S + ts, c = d * M + static_cast<size_t>(slot[static_cast<size_t>(g.minute(on ? static_cast<int>(ts) : 0))]);
        if (c != at) {
          flush();
          at = c, k = 1.0 / n_act[d];
        }
        const double r = bl.inc(t, s.cost, s.impact);
        run += r + run * r;
        buy[d] += bl.buy(t) * k;
      }
    }
    flush();
  }
  Mix o;
  std::vector<size_t> xs; // 有增量的格
  for (size_t c = 0; c < X; ++c)
    if (hit[c])
      xs.push_back(c);
  if (!xs.empty()) {
    std::vector<double> r(xs.size());
    for (size_t i = 0; i < xs.size(); ++i)
      r[i] = acc[xs[i]];
    o.lv.push(axis[xs.front() / M], -kStartDx, 1.0);
    compound(r.data(), xs.size(), [&](size_t i, double v) { o.lv.push(axis[xs[i] / M], static_cast<double>(mins[xs[i] % M]), v); });
  }
  size_t d0 = 0;
  while (d0 < D && !(buy[d0] > 0))
    ++d0;
  if (d0 < D) {
    double sum = 0.0;
    size_t days = 0;
    for (size_t d = d0; d < D; ++d)
      sum += buy[d], days += n_act[d] > 0;
    o.turn = sum / static_cast<double>(days) * factor::stat::kDaysPerYear;
  }
  return o;
}
// 价格指数: 起点 = 首根前收 (无 → 首根收盘) + 每根收盘
Levels levels(const Database::Kline &k) {
  Levels s;
  if (k.daily.empty())
    return s;
  const Database::Candle &c0 = k.daily.front();
  s.push(c0.date, -kStartDx, c0.ref > 0 ? c0.ref : c0.close);
  for (const Database::Candle &c : k.daily)
    s.push(c.date, kClose, c.close);
  return s;
}

// 点位 s → 日并集轴 axis 上的线 (见 PortfolioLine): 只取 axis 范围内的点; 首个取到的点不是起点 → 补其前一点为起点 (所在日开盘前);
// 不在轴上的日只差点位, 收益自然并进下一个轴上的日. 同时建日末视图
PortfolioLine project(const std::vector<misc::Ymd> &axis, const Levels &s) {
  PortfolioLine c;
  if (axis.empty())
    return c;
  const std::vector<size_t> di = days_on(axis, s.date);
  std::vector<int> day; // [点] 所在日
  const auto push = [&](size_t d, double m, size_t i) {
    c.x.push_back(x_of(d, m)), c.y.push_back(s.lv[i]), day.push_back(static_cast<int>(d));
  };
  for (size_t i = 0; i < s.date.size(); ++i) {
    if (s.date[i] < axis.front() || s.date[i] > axis.back())
      continue;
    if (c.x.empty() && s.min[i] >= 0) {
      assert(i > 0);
      push(di[i], -kStartDx, i - 1);
    }
    if (axis[di[i]] == s.date[i])
      push(di[i], s.min[i], i);
  }
  if (c.x.empty())
    return c;
  c.f = day.front();
  c.l = c.x.size() > 1 ? day.back() : c.f - 1;
  const size_t n = static_cast<size_t>(c.l - c.f + 2);
  c.e.assign(n, kNaN);
  for (size_t j = 0; j < c.x.size(); ++j)
    c.e[j == 0 ? 0 : static_cast<size_t>(day[j] - c.f + 1)] = c.y[j];
  c.s1.assign(n, 0.0), c.s2.assign(n, 0.0);
  for (size_t k = 1; k < n; ++k) {
    if (std::isnan(c.e[k]))
      c.e[k] = c.e[k - 1];
    const double r = c.e[k - 1] > 0 ? c.e[k] / c.e[k - 1] - 1.0 : 0.0;
    c.s1[k] = c.s1[k - 1] + r, c.s2[k] = c.s2[k - 1] + r * r;
  }
  return c;
}

// 线源 s → 日并集轴 axis 上的线: 价格指数 = 点位直投; 腿集 = mix 合成后投 (带年换手); 无源 / 日轴空 → 空线
PortfolioLine build_line(const std::vector<misc::Ymd> &axis, const PortfolioLineSrc &s) {
  if (axis.empty() || (!s.price && s.legs.empty()))
    return {};
  if (s.price)
    return project(axis, levels(*s.price));
  const Mix m = mix(axis, s);
  PortfolioLine c = project(axis, m.lv);
  c.turn = m.turn;
  return c;
}
// 腿集合成的线源: cost = 扣成本; 腿账都无成本列 → 不扣 (扣与不扣同一线源 ⇒ 两条线共用一份)
PortfolioLineSrc mix_src(std::vector<PortfolioLeg> legs, bool cost, double fixed_impact) {
  cost = cost && std::ranges::any_of(legs, [](const PortfolioLeg &lg) { return lg.b->col[kBookImp].n > 0; });
  return {std::move(legs), cost, cost ? fixed_impact : 0.0, nullptr};
}
// 账 b 线键 key 的腿; 账空 / 不在 → 空腿
PortfolioLeg leg_of(const std::shared_ptr<const PortfolioBook> &b, uint32_t key) {
  if (!b)
    return {};
  const size_t line = b->line_of(key);
  return line < b->lines() ? PortfolioLeg{b.get(), line} : PortfolioLeg{};
}
// 一腿的腿集 (空腿 → 空集)
std::vector<PortfolioLeg> legs_of(const PortfolioLeg &lg) { return lg.b ? std::vector<PortfolioLeg>{lg} : std::vector<PortfolioLeg>{}; }

// ---- 行 (四组 + 成分同一套): 行名等每次现拼, 线源每次现定; 线按线源沿用上一份, 否则现算 ----
// 上一份派生的行 (按 id) + 日轴是否不变
struct Reuse {
  bool axis;
  std::map<std::string, const PortfolioRow *> rows;
  Reuse(const PortfolioDerived &o, const std::vector<misc::Ymd> &dates) : axis(dates == o.dates) {
    for (const std::vector<PortfolioRow> &g : o.rows)
      for (const PortfolioRow &r : g)
        rows[r.id] = &r;
  }
  // 日轴不变且上一份同 id 行有线源同 s 的线 → 它; 否则空
  std::shared_ptr<const PortfolioLine> take(const std::string &id, const PortfolioLineSrc &s) const {
    const auto it = rows.find(id);
    if (!axis || it == rows.end())
      return nullptr;
    for (int l = 0; l < kPairLines; ++l)
      if (it->second->src[l] == s)
        return it->second->line[l];
    return nullptr;
  }
};

// 行 r 进组 g (线源已定): 各线 = 同行前一线源同 → 共用, 否则沿用上一份 / 现算; 登记 index
void push(PortfolioDerived &d, int g, PortfolioRow r, const Reuse &ru) {
  for (int l = 0; l < kPairLines; ++l) {
    if (l > 0 && r.src[l] == r.src[l - 1]) {
      r.line[l] = r.line[l - 1];
      continue;
    }
    r.line[l] = ru.take(r.id, r.src[l]);
    if (!r.line[l])
      r.line[l] = std::make_shared<const PortfolioLine>(build_line(d.dates, r.src[l]));
  }
  d.index[r.id] = {g, d.rows[g].size()};
  d.rows[g].push_back(std::move(r));
}

// 算成的因子 [items 序] 的条目; 其账进 d.books (同序), 计数与日并集轴 (各账日的并) 写进 d
std::vector<const PortfolioItem *> ready_of(const std::vector<PortfolioItem> &items, const PortfolioResult &res, PortfolioDerived &d) {
  std::vector<const PortfolioItem *> its;
  for (const PortfolioItem &it : items) {
    if (!it.missing.empty())
      continue;
    const auto b = res.factors.find(it.e.key());
    if (b == res.factors.end()) {
      ++d.n_wait;
      continue;
    }
    if (!b->second->error.empty()) {
      ++d.n_error;
      continue;
    }
    ++d.n_ready;
    its.push_back(&it);
    d.books.push_back(b->second);
    d.dates.insert(d.dates.end(), b->second->dates.begin(), b->second->dates.end());
  }
  std::sort(d.dates.begin(), d.dates.end());
  d.dates.erase(std::unique(d.dates.begin(), d.dates.end()), d.dates.end());
  return its;
}

// 因子行 (见头文件): 腿 = 自己账的线 (键 0)
void derive_factors(const std::vector<const PortfolioItem *> &its, double fixed_impact, const Reuse &ru, PortfolioDerived &d) {
  for (size_t q = 0; q < its.size(); ++q) {
    const PortfolioItem &it = *its[q];
    const PortfolioLeg lg = leg_of(d.books[q], 0);
    PortfolioRow r;
    r.id = it.e.key();
    r.en = std::filesystem::path(it.e.file).stem().string();
    r.cn = clip_name(it.name_cn);
    r.tag = it.name_en + " " + factor::stat::hold_name(it.e.hold);
    r.hover = r.en + "  " + it.name_cn + "\n• " + it.e.pool + " " + factor_level(it.e.lv).name + "\n• " + hold_desc(it.e.hold, it.e.lv) +
              "\n• 冲击 " + amt_name(d.books[q]->amt) + "\n• 规范串 " + it.e.expr + (it.e.sign < 0 ? " · 取反" : "");
    r.src[kTrade] = mix_src(legs_of(lg), true, fixed_impact);
    r.src[kIdeal] = mix_src(legs_of(lg), false, 0.0);
    push(d, kGroupFactor, std::move(r), ru);
  }
}

// 策略行 (见头文件): 腿集 = 全部因子账的线 (键 0)
void derive_strat(const std::string &name, double fixed_impact, const Reuse &ru, PortfolioDerived &d) {
  PortfolioRow s;
  s.id = "pf_strat";
  s.en = name, s.cn = "G20 多头";
  s.tag = s.hover = name;
  std::vector<PortfolioLeg> legs;
  for (const std::shared_ptr<const PortfolioBook> &b : d.books)
    legs.push_back(leg_of(b, 0));
  s.src[kTrade] = mix_src(legs, true, fixed_impact);
  s.src[kIdeal] = mix_src(std::move(legs), false, 0.0);
  push(d, kGroupStrat, std::move(s), ru);
}

// 篮子行 (见头文件): 指数 / 行业 / 概念三组 = basket_group 三组; 腿 / 估值腿 = 篮子账的该线, 理想线 = 价格指数 (prices) 的该线;
// 另收个股账, 建日轴 → 两本账的行 (估值)
void derive_baskets(const PortfolioResult &res, const std::shared_ptr<const Database::Tape> &prices, double fixed_impact, const Reuse &ru,
                    PortfolioDerived &d) {
  d.baskets = res.baskets && res.baskets->error.empty() ? res.baskets : nullptr;
  d.stocks = d.baskets ? res.stocks : nullptr;
  assert(!d.baskets || (d.stocks && d.stocks->error.empty() && d.stocks->dates == d.baskets->dates));
  d.prices = prices && !prices->list.empty() ? prices : nullptr;
  d.val_at.assign(d.dates.size(), -1);
  if (d.baskets) {
    const std::vector<misc::Ymd> &hd = d.baskets->dates;
    const std::vector<size_t> di = days_on(hd, d.dates);
    for (size_t k = 0; k < d.dates.size(); ++k)
      if (di[k] < hd.size() && hd[di[k]] == d.dates[k])
        d.val_at[k] = static_cast<int32_t>(di[k]);
  }
  for (int g = 0; g < kBasketGroups; ++g)
    for (const size_t line : basket_group(static_cast<BasketGroup>(g))) {
      PortfolioRow r;
      r.id = basket_id(line);
      r.en = cut_code(cut_of_line(line));
      r.hover = r.en + "  " + basket_name(line);
      r.cn = r.tag = clip_name(cut_name(cut_of_line(line))) + weight_suffix(weight_of(line));
      r.q = {Database::kBasket, static_cast<uint32_t>(line)};
      r.val = leg_of(d.baskets, r.q.key);
      r.src[kTrade] = mix_src(legs_of(r.val), true, fixed_impact);
      if (d.prices) {
        assert(d.prices->keys[line] == line);
        r.src[kIdeal].price = d.prices->list[line].get();
      }
      push(d, g, std::move(r), ru);
    }
}

// 成分股的名: 简称 (无档案 / 无简称 → 代码)
std::string stock_name(const StockInfos &infos, uint32_t id) {
  if (id < infos.size() && infos[id].known && !infos[id].name.empty())
    return infos[id].name;
  return asset_axis().code(id);
}
// 成分行的悬停: 名 + 代码 / 权占比 (share; NaN = 不在展开的篮子里) / 行业 / 估值
std::string stock_hover(const StockInfos &infos, uint32_t id, double share) {
  const auto num = [](const char *label, double v, const char *fmt) {
    char s[48];
    if (std::isfinite(v))
      std::snprintf(s, sizeof(s), fmt, label, v);
    else
      std::snprintf(s, sizeof(s), "%s -", label);
    return std::string(s);
  };
  std::string h = stock_name(infos, id) + " " + asset_axis().code(id) + "\n• " + num("权占比", 100.0 * share, "%s %.2f%%");
  if (id < infos.size() && infos[id].known) {
    const StockInfo &s = infos[id];
    h += "\n• 行业 " + (s.ind_name.empty() ? std::string("-") : s.ind_name) + (s.st ? " · ST" : "");
    h += "\n• " + num("总市值", s.mcap, "%s %.0f亿") + "  " + num("PE", s.pe_ttm, "%s %.1f") + "  " + num("PB", s.pb_mrq, "%s %.2f") + "  " +
         num("PS", s.ps_ttm, "%s %.2f") + "  " + num("PCF", s.pcf_ttm, "%s %.1f") + "  " + num("DY1", s.dy1y, "%s %.2f%%");
  }
  return h + "\n• 点 = 选中 (同篮子行)";
}
// 成分行色相: 按全局 id 黄金角散开
float stock_hue(uint32_t id) {
  const double h = static_cast<double>(id) * 0.6180339887498949;
  return static_cast<float>(h - std::floor(h));
}
// 成分行 (见头文件): 展开的篮子行的成分 (Tape::cons, 权降序) + 其余选中的个股 (stocks 升序); 腿 / 估值腿 = 个股账该只
void derive_kids(const std::string &open, const std::vector<uint32_t> &stocks, const StockInfos &infos, double fixed_impact, const Reuse &ru,
                 PortfolioDerived &d) {
  std::vector<std::pair<uint32_t, double>> ks; // (全局 id, 权占比)
  if (const PortfolioAt o = d.at(open); o.g >= 0 && d.prices && !d.prices->lack) {
    assert(o.g < kBasketGroups);
    const uint32_t line = d.row(o).q.key;
    for (const Database::Constituent &c : d.prices->cons[line])
      ks.emplace_back(c.id, c.w / d.prices->weight[line]);
  }
  d.n_cons = ks.size();
  std::vector<uint32_t> cons(ks.size());
  std::ranges::transform(ks, cons.begin(), [](const auto &k) { return k.first; });
  std::ranges::sort(cons);
  for (const uint32_t id : stocks)
    if (!std::ranges::binary_search(cons, id))
      ks.emplace_back(id, kNaN);
  for (const auto &[id, share] : ks) {
    PortfolioRow r;
    r.id = stock_key(id);
    r.en = asset_axis().code(id);
    r.tag = stock_name(infos, id);
    r.cn = clip_name(r.tag);
    r.hover = stock_hover(infos, id, share);
    r.hue = stock_hue(id);
    r.q = {Database::kStock, id};
    r.val = leg_of(d.stocks, id);
    r.src[kTrade] = mix_src(legs_of(r.val), true, fixed_impact);
    r.src[kIdeal] = mix_src(legs_of(r.val), false, 0.0);
    push(d, kGroupKid, std::move(r), ru);
  }
}

// 估值行 (位置 + 估值腿; 连同日轴 = 滚动分位 / 末点分位的全部来源)
std::vector<std::pair<PortfolioAt, PortfolioLeg>> val_rows(const PortfolioDerived &d) {
  std::vector<std::pair<PortfolioAt, PortfolioLeg>> o;
  for (int g = 0; g < kRowGroups; ++g)
    for (size_t r = 0; r < d.rows[g].size(); ++r)
      if (d.rows[g][r].val.b)
        o.emplace_back(PortfolioAt{g, r}, d.rows[g][r].val);
  return o;
}

// 线 c 在日段 [d0, d1) 的切片 (见 PortfolioSlice); 整条在区间外 → 空. 年化 / 夏普只读日末视图: 有效日段 [a, b) = [d0, d1) ∩ [f, l + 1),
// 年化 = end(b) / end(a) 按 b − a 个交易日复利折年 (%); 夏普 = 同段日收益的 mean / sd (ddof = 1) × √每年交易日
PortfolioSlice slice_of(const PortfolioLine &c, int d0, int d1) {
  PortfolioSlice s;
  const double x0 = x_of(static_cast<size_t>(d0), kOpen), x1 = x_of(static_cast<size_t>(d1), kOpen);
  s.c = &c, s.x0 = x0;
  const size_t i0 = static_cast<size_t>(std::lower_bound(c.x.begin(), c.x.end(), x0) - c.x.begin());
  const size_t i1 = static_cast<size_t>(std::lower_bound(c.x.begin(), c.x.end(), x1) - c.x.begin());
  if (i0 == c.x.size() || i1 == 0)
    return s;
  s.j0 = i0 > 0 ? i0 - 1 : 0, s.j1 = i1;
  const int a = std::max(d0, c.f), b = std::min(d1, c.l + 1);
  s.base = c.end(a);
  assert(s.base == c.y[s.j0]);
  if (b <= a)
    return s;
  const double n = b - a;
  s.ann = static_cast<float>(100.0 * factor::stat::ann_geo(c.end(b) / s.base, n));
  const size_t ia = static_cast<size_t>(a - c.f), ib = static_cast<size_t>(b - c.f);
  const double s1 = c.s1[ib] - c.s1[ia], s2 = c.s2[ib] - c.s2[ia];
  const double var = n >= 2.0 ? (s2 - s1 * s1 / n) / (n - 1.0) : 0.0;
  if (var > 0.0)
    s.sharpe = static_cast<float>(s1 / n / std::sqrt(var) * std::sqrt(static_cast<double>(factor::stat::kDaysPerYear)));
  return s;
}

// ---- 估值口径 (一个核 pct_at; roll_of 逐日 / last_pct 末日, 同按日并集轴日) ----
// 估值 (EP / BP / SP / CP / DY 收益率) 可算: 有限 (≤ 0 照排, 在最贵端)
bool val_ok(double r) { return std::isfinite(r); }
// 收益率 ↔ 贵度 (取负; 自逆, 贵度升 = 贵): 分位 / 排序一律在贵度上
template <class F>
F dear_of(F x) {
  return -x;
}
// 自然年平移: 滚动窗 = (years_before(日, N), 日]; 史满 1 年 = 首个可算日 ≤ years_before(日, 1)
constexpr misc::Ymd years_before(misc::Ymd day, int years) { return day - years * 10000; }
// 估值分位 = 窗内贵度 ≤ 当日的权重和 le / 窗内可算点权重和 w (%, 高 = 贵)
float pct_of(double le, double w) { return static_cast<float>(100.0 * le / w); }
// 窗内权重 (指数饱和): u = 1 − a / T, a = 距当日的日历天数, T = 当日 − max(N 年前, 首个可算日) (史不足 N 年 → 按实际史长缩放);
// 窗内前 ~3/4 ≈ 等权, 窗尾段快速降到 0 (点出窗时权重已 ≈ 0, 分位不跳)
constexpr double kRollK = 10.0;
double roll_w(int a, int T) { return 1.0 - std::exp(-kRollK * (1.0 - static_cast<double>(a) / T)); }
int days_of(misc::Ymd d) { return static_cast<int>(misc::to_days(d).time_since_epoch().count()); }

// 估值账的日轴 (窗按日历天定界 / 加权; 篮子账 / 个股账同一条): 一次派生算一次, 各腿各口共用
struct ValAxis {
  const std::vector<misc::Ymd> &hd;
  std::vector<int> dd; // [D] 日历天
  explicit ValAxis(const PortfolioBook &b) : hd(b.dates), dd(b.dates.size()) {
    for (size_t i = 0; i < hd.size(); ++i)
      dd[i] = days_of(hd[i]);
  }
};
// 一腿一口的估值 v [D] + 窗: 日 t 的窗 = [lo(t), t] = (t 的 N 年前, t] 内的可算点, 点 u 的权 = roll_w(age(u, t), span(t));
// 首个可算日起史满 1 年 (ready) 才出
struct ValSeries {
  const ValAxis &ax;
  const float *v;
  int years;
  size_t first = 0; // 首个可算点 (无 = D)
  ValSeries(const ValAxis &a, const PortfolioLeg &lg, int m, int years_) : ax(a), v(lg.b->val_of(lg.line, m)), years(years_) {
    assert(lg.b->dates.size() == a.hd.size());
    while (first < size() && !ok(first))
      ++first;
  }
  size_t size() const { return ax.hd.size(); }
  bool ok(size_t t) const { return val_ok(v[t]); }
  bool ready(size_t t) const { return first < size() && ax.hd[first] <= years_before(ax.hd[t], 1); }
  bool gone(size_t u, size_t t) const { return ax.hd[u] <= years_before(ax.hd[t], years); } // u 已出 t 的窗
  size_t lo(size_t t) const {
    return static_cast<size_t>(std::upper_bound(ax.hd.begin(), ax.hd.begin() + static_cast<std::ptrdiff_t>(t), years_before(ax.hd[t], years)) -
                               ax.hd.begin());
  }
  int span(size_t t) const { return ax.dd[t] - days_of(std::max(years_before(ax.hd[t], years), ax.hd[first])); } // 窗跨度 T (史不足 N 年 → 实际史长)
  int age(size_t u, size_t t) const { return ax.dd[t] - ax.dd[u]; }
};

// 收益率序列 v[0, n) 中 v > 0 的点的倍数 1 / v, 取 [kContourTrim, 1 − kContourTrim] 分位为区间, 首尾含在内 kContours 等分 (升序); 不足两个不同倍数 → 空
std::vector<double> levels_of(const float *v, size_t n) {
  std::vector<double> m;
  for (size_t i = 0; i < n; ++i)
    if (v[i] > 0.f)
      m.push_back(1.0 / v[i]);
  if (m.empty())
    return {};
  const auto at = [&](double q) {
    const auto it = m.begin() + static_cast<std::ptrdiff_t>(q * static_cast<double>(m.size() - 1));
    std::nth_element(m.begin(), it, m.end());
    return *it;
  };
  const double lo = at(kContourTrim), hi = at(1.0 - kContourTrim);
  if (!(hi > lo))
    return {};
  std::vector<double> lv(kContours);
  for (int k = 0; k < kContours; ++k)
    lv[static_cast<size_t>(k)] = lo + (hi - lo) * k / (kContours - 1);
  return lv;
}

// 窗内权重表 wt[a] = roll_w(a, T) (a = 距当日的日历天数): 窗跨度 T 变才重建 (史满 N 年后 T 只差闰日); 一次派生内各篮子各口共用一份
struct RollW {
  int T = -1;
  std::vector<double> wt;
  const std::vector<double> &of(int T_) {
    assert(T_ > 0);
    if (T_ != T) {
      T = T_;
      wt.resize(static_cast<size_t>(T) + 1);
      for (int a = 0; a <= T; ++a)
        wt[static_cast<size_t>(a)] = roll_w(a, T);
    }
    return wt;
  }
};
// 窗内点 u 在日 t 的权
double w_at(const ValSeries &s, const std::vector<double> &wt, size_t u, size_t t) {
  assert(static_cast<size_t>(s.age(u, t)) < wt.size());
  return wt[static_cast<size_t>(s.age(u, t))];
}
// 日 t 的估值分位 (roll_of / last_pct 同一个核): 窗 [lo(t), t] 内可算点里贵度 ≤ 当日的权重和 / 权重和 (%, 高 = 贵); 当日不可算 / 史不满 1 年 → NaN
float pct_at(const ValSeries &s, size_t t, RollW &rw) {
  if (!s.ok(t) || !s.ready(t))
    return std::numeric_limits<float>::quiet_NaN();
  const std::vector<double> &wt = rw.of(s.span(t));
  const float m = dear_of(s.v[t]);
  double w = 0.0, le = 0.0;
  for (size_t u = s.lo(t); u <= t; ++u)
    if (s.ok(u)) {
      const double x = w_at(s, wt, u, t);
      w += x, le += dear_of(s.v[u]) <= m ? x : 0.0;
    }
  return pct_of(le, w);
}

// 估值 s 在日并集轴各日 (at = 轴日 → 账行, −1 = 无; 有效的升序) 上的滚动分位 → o [轴日]: 窗内可算点按贵度保持有序 (逐日二分插入 / 出窗删除);
// 出点日扫一遍窗取加权分位 (累计权重首个 ≥ q · 总权重的点) + 当日估值分位 pct_at; 等值线档按全史
void roll_of(const ValSeries &s, const std::vector<int32_t> &at, RollW &rw, PortfolioRoll &o) {
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  const size_t n = s.size(), D = at.size();
  const float *v = s.v;
  o.levels = levels_of(v, n);
  const auto by_dear = [&](size_t x, size_t y) { return dear_of(v[x]) < dear_of(v[y]); };
  std::vector<size_t> win; // 窗内可算点 (源下标), 贵度升序
  for (std::vector<float> &q : o.q)
    q.assign(D, kNaN);
  o.pct.assign(D, kNaN);
  o.band.assign(D, 0);
  size_t t = 0, lo = 0; // 已入窗 [0, t) / 未出窗的首点
  int wp = 0;           // 窗内收益率 > 0 的个数
  for (size_t k = 0; k < D; ++k) {
    if (at[k] < 0)
      continue;
    const size_t tk = static_cast<size_t>(at[k]);
    assert(tk < n && tk >= t && "at 须升序且落在账日内");
    for (; t <= tk; ++t)
      if (s.ok(t))
        win.insert(std::upper_bound(win.begin(), win.end(), t, by_dear), t), wp += v[t] > 0.f;
    for (; s.gone(lo, tk); ++lo)
      if (s.ok(lo)) {
        const auto [e0, e1] = std::equal_range(win.begin(), win.end(), lo, by_dear);
        const auto e = std::find(e0, e1, lo);
        assert(e != e1);
        win.erase(e), wp -= v[lo] > 0.f;
      }
    if (win.empty() || !s.ready(tk))
      continue;
    const std::vector<double> &wt = rw.of(s.span(tk));
    double W = 0.0;
    for (const size_t u : win)
      W += w_at(s, wt, u, tk);
    double cum = 0.0;
    int j = 0;
    for (const size_t u : win) {
      cum += w_at(s, wt, u, tk);
      for (; j < kBandQ && cum >= kBandQs[j] * W; ++j)
        o.q[j][k] = v[u];
    }
    assert(j == kBandQ);
    o.band[k] = wp == static_cast<int>(win.size());
    o.pct[k] = pct_at(s, tk, rw);
  }
}

// 估值 s 在轴日 [0, k1) 内末个可算日及其估值分位 (pct_at)
PortfolioLastPct last_pct(const ValSeries &s, const std::vector<int32_t> &at, int k1, RollW &rw) {
  for (int k = k1; k-- > 0;)
    if (at[static_cast<size_t>(k)] >= 0)
      if (const float p = pct_at(s, static_cast<size_t>(at[static_cast<size_t>(k)]), rw); !std::isnan(p))
        return {k, p};
  return {};
}

// 截面名次 (NAV 数值列 / VAL 格同一份): 组内 n 格 at(i) 的 x 在有限值里排名 → rank ∈ [0, 1] (并列取中; 只一个 = 0.5);
// good = +1 → x 高为 0 (好), −1 → x 低为 0; 不可算的不排. 排序一遍 O(n log n)
template <class At>
void rank_col(size_t n, int good, const At &at) {
  std::vector<std::pair<double, size_t>> v; // (−good · x, 格)
  for (size_t i = 0; i < n; ++i)
    if (const Ranked &c = at(i); std::isfinite(c.x))
      v.push_back({-good * c.x, i});
  std::sort(v.begin(), v.end());
  const size_t m = v.size();
  for (size_t a = 0, b = 0; a < m; a = b) {
    while (b < m && v[b].first == v[a].first)
      ++b;
    const double r = m > 1 ? (static_cast<double>(a) + 0.5 * static_cast<double>(b - a - 1)) / static_cast<double>(m - 1) : 0.5;
    for (size_t k = a; k < b; ++k)
      at(v[k].second).rank = r;
  }
}

} // namespace

void MakePortfolioDerived(const std::vector<PortfolioItem> &items, const std::string &name, const PortfolioResult &res,
                          const std::shared_ptr<const Database::Tape> &prices, double fixed_impact, const std::string &open,
                          const std::vector<uint32_t> &stocks, const StockInfos &infos, PortfolioDerived &d) {
  PortfolioDerived o = std::move(d);
  d = PortfolioDerived{};
  const std::vector<const PortfolioItem *> its = ready_of(items, res, d);
  const Reuse ru(o, d.dates);
  derive_factors(its, fixed_impact, ru, d);
  derive_strat(name, fixed_impact, ru, d);
  derive_baskets(res, prices, fixed_impact, ru, d);
  derive_kids(open, stocks, infos, fixed_impact, ru, d);
  paint(d);
  d.val_gen = o.val_gen + (d.dates != o.dates || val_rows(d) != val_rows(o));
}

void MakePortfolioRoll(const PortfolioDerived &d, const PortfolioAt &pin, int years, PortfolioRolls &r) {
  r = {};
  if (pin.g < 0 || !d.row(pin).val.b)
    return;
  const PortfolioLeg &lg = d.row(pin).val;
  const ValAxis ax(*lg.b);
  RollW rw;
  for (int b = 0; b < kPoolVals; ++b)
    roll_of(ValSeries(ax, lg, b, years), d.val_at, rw, r[b]);
}

void MakePortfolioPct(const PortfolioDerived &d, int d1, int years, PortfolioPct &p) {
  for (int g = 0; g < kRowGroups; ++g)
    p.rows[g].assign(d.rows[g].size(), {});
  if (!d.baskets)
    return;
  const ValAxis ax(*d.baskets);
  RollW rw;
  for (const auto &[at, lg] : val_rows(d))
    for (int b = 0; b < kPoolVals; ++b)
      p.rows[at.g][at.r][b] = last_pct(ValSeries(ax, lg, b, years), d.val_at, d1, rw);
}

void MakePortfolioView(const PortfolioDerived &d, int d0, int d1, const PortfolioAt &pin, const PortfolioRolls &roll, const PortfolioPct &pct,
                       PortfolioView &v) {
  // ---- 各组同一套: 各线切片 + NAV 数值列 + VAL 各口区间内末个可算日的估值分位 (= 区间末之前的末个可算日落在区间内, 含区间起点的前收日);
  //      各列同组同列名次 (VAL: 分位低 = 便宜 = 好) ----
  for (int g = 0; g < kRowGroups; ++g) {
    const size_t n = d.rows[g].size();
    assert(pct.rows[g].size() == n);
    v.slice[g].clear();
    v.num[g].assign(n, {});
    v.pct[g].assign(n, {});
    for (size_t i = 0; i < n; ++i) {
      const PortfolioRow &r = d.rows[g][i];
      v.slice[g].push_back({slice_of(*r.line[kTrade], d0, d1), slice_of(*r.line[kIdeal], d0, d1)});
      const std::array<PortfolioSlice, kPairLines> &s = v.slice[g].back();
      std::array<Ranked, kNavNums> &c = v.num[g][i];
      c[kNumTrade].x = s[kTrade].ann, c[kNumIdeal].x = s[kIdeal].ann;
      c[kNumSharpe].x = s[kTrade].sharpe, c[kNumTurn].x = r.line[kTrade]->turn;
      for (int b = 0; b < kPoolVals; ++b)
        if (const PortfolioLastPct &lp = pct.rows[g][i][b]; lp.k >= 0 && lp.k >= d0 - 1)
          v.pct[g][i][b].x = lp.pct;
    }
    for (int k = 0; k < kNavNums; ++k)
      rank_col(n, kNavGood[k], [&](size_t i) -> Ranked & { return v.num[g][i][k]; });
    for (int b = 0; b < kPoolVals; ++b)
      rank_col(n, -1, [&](size_t i) -> Ranked & { return v.pct[g][i][b]; });
  }

  // ---- VAL 标的各口 band: 标的可成交线的同一切片, y = P · v / q (v = 当日收益率; v ≤ 0 / q ≤ 0 → 价格无解, 断开; 窗内有任一无解点 → 不画);
  //      等值线单位点位 unit = P · v (只看当日, 不看滚动分位; v ≤ 0 → 断开), 档 = 二级的全史倍数档 ----
  v.band_x.clear();
  for (PortfolioBand &bd : v.band)
    bd = {};
  if (pin.g < 0 || !d.row(pin).val.b)
    return;
  const PortfolioLeg &lg = d.row(pin).val;
  const PortfolioSlice &sl = v.slice[pin.g][pin.r][kTrade];
  const size_t n = sl.size();
  std::vector<int> day(n); // [切片点] 点位所属日 (日并集轴; −1 = 轴外)
  for (size_t i = 0; i < n; ++i)
    v.band_x.push_back(sl.x(i)), day[i] = sl.c->day(sl.j0 + i);
  for (int b = 0; b < kPoolVals; ++b) {
    const PortfolioRoll &r = roll[b];
    assert(r.pct.size() == d.dates.size());
    PortfolioBand &bd = v.band[b];
    const float *hv = lg.b->val_of(lg.line, b);
    for (std::vector<double> &y : bd.y)
      y.assign(n, kNaN);
    bd.unit.assign(n, kNaN);
    bd.levels = r.levels;
    int last = -1; // 末个有分位的日
    for (size_t i = 0; i < n; ++i) {
      const int k = day[i];
      const int32_t t = k >= 0 ? d.val_at[static_cast<size_t>(k)] : -1;
      const double x = t >= 0 ? hv[t] : kNaN;
      if (x > 0.0)
        bd.unit[i] = sl.y(i) * x;
      if (t < 0 || !std::isfinite(r.pct[static_cast<size_t>(k)]))
        continue;
      last = k;
      if (!(x > 0.0) || !r.band[static_cast<size_t>(k)])
        continue;
      for (int q = 0; q < kBandQ; ++q)
        if (const float qk = r.q[q][static_cast<size_t>(k)]; qk > 0.f)
          bd.y[q][i] = sl.y(i) * x / qk;
      if (!(r.q[kBandQ - 1][static_cast<size_t>(k)] > 0.f)) // q 降序: 最贵档 > 0 ⇔ 各档全可画 (填色只画全有限段)
        continue;
      const int ii = static_cast<int>(i);
      if (bd.segs.empty() || bd.segs.back().second != ii)
        bd.segs.push_back({ii, ii + 1});
      else
        ++bd.segs.back().second;
    }
    if (last < 0)
      continue;
    bd.ok = true;
    bd.now = hv[d.val_at[static_cast<size_t>(last)]], bd.pct = r.pct[static_cast<size_t>(last)];
  }
}

} // namespace GUI::Factors
