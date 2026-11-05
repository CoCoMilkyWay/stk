// Factors→Portfolio 派生层 — 见头文件
#include "gui/task_factors/ui/PortfolioView.hpp"

#include "gui/task_factors/ui/TabFactors.hpp" // hold_desc
#include "misc/date.hpp"                      // to_days (滚动窗权重按日历天)
#include "shared/Basket.hpp"                  // basket_group / basket_name / cut_code

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
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

// ---- 行配色 (策略外四组一起, 见 PortfolioDerived::nav): 相似度 s = max(成员重合, 残差相关), 平方距离 2 (1 − s) 上 Ward 树定叶序;
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

// 策略外四组各行定色相 (见上); 因子没到齐 / 价格指数未就绪 / 日轴不足两日 → 全不定色 (灰, 免得边算边跳)
void paint(PortfolioDerived &d) {
  std::vector<PortfolioRow *> rs;
  size_t mkt = SIZE_MAX; // 全A.加 (残差的基准)
  for (int g = 0; g < kNavStrat; ++g)
    for (PortfolioRow &r : d.nav[g]) {
      if (r.basket == static_cast<int>(basket_line(0, kWeightCap)))
        mkt = rs.size();
      rs.push_back(&r);
      r.hue = std::numeric_limits<float>::quiet_NaN();
    }
  const size_t n = rs.size(), D = d.dates.size();
  assert(mkt < n);
  if (d.n_wait > 0 || !d.prices || D < 2)
    return;

  const size_t T = std::min(kSimDays, D - 1), lo = D - T;
  const std::vector<double> m = rets_of(rs[mkt]->line[kIdeal], lo, T);
  std::vector<std::vector<double>> res(n); // [行] 残差日收益
  for (size_t i = 0; i < n; ++i) {
    res[i] = rets_of(rs[i]->line[kIdeal], lo, T);
    residual(res[i], m);
  }
  std::fill(res[mkt].begin(), res[mkt].end(), kNaN); // 自身对自身回归无残差
  const size_t K = d.prices->keys.size();
  assert(d.prices->overlap.size() == K * K);
  std::vector<double> far(n * n, 0.0); // [行 × 行] 1 − s
  for (size_t i = 0; i < n; ++i)
    for (size_t j = i + 1; j < n; ++j) {
      double s = corr(res[i], res[j]);
      if (rs[i]->basket >= 0 && rs[j]->basket >= 0)
        s = std::max(s, static_cast<double>(d.prices->overlap[static_cast<size_t>(rs[i]->basket) * K + static_cast<size_t>(rs[j]->basket)]));
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

// ---- 线的来路 (账的一线 / 策略 / 价格指数同一条): 源 → 点位 Levels → project (日并集轴) → PortfolioLine ----
// 源点位: 点 i 在日 date[i] 的日内分钟 min[i], 点位 lv[i]; 首点 = 首日开盘前 kStartDx 格的起点 (前收), 其后点 i ↔ 源下标 i − 1
struct Levels {
  std::vector<misc::Ymd> date;
  std::vector<double> min, lv;
  void push(misc::Ymd d, double m, double v) { date.push_back(d), min.push_back(m), lv.push_back(v); }
};

// 账的一线 (fold_book 已实现口径, 与池指数对齐): 行 t 的成对线增量 (可成交 = 毛 − 冲击 − 固定份数 · fixed_impact, 理想 = 毛) / 增仓量
struct BookLine {
  const float *c[kBookCols];
  BookLine(const PortfolioBook &b, size_t line) {
    for (int k = 0; k < kBookCols; ++k)
      c[k] = b.at(k, line);
  }
  double inc(size_t t, int pair, double fixed_impact) const {
    const double r = c[kBookRet][t];
    return pair == kTrade ? r - c[kBookImp][t] - c[kBookFix][t] * fixed_impact : r;
  }
  double buy(size_t t) const { return c[kBookBuy][t]; }
};

// 账 b 的线 line 的成对线 pair: 起点 1 + 逐行复利, 点在行末分钟 (日层行末 = 收盘)
Levels levels(const PortfolioBook &b, size_t line, int pair, double fixed_impact) {
  const factor::Geo g = factor_level(b.lv).geo();
  const size_t S = static_cast<size_t>(g.S), T = b.dates.size() * S;
  assert(!b.dates.empty() && static_cast<size_t>(b.col[kBookRet].P) == T);
  const BookLine bl(b, line);
  std::vector<double> r(T);
  for (size_t t = 0; t < T; ++t)
    r[t] = bl.inc(t, pair, fixed_impact);
  Levels s;
  s.push(b.dates.front(), -kStartDx, 1.0);
  compound(r.data(), T, [&](size_t t, double v) { s.push(b.dates[t / S], g.minute(static_cast<int>(t % S)), v); });
  return s;
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

// 策略 (组合 G20 多头): 各因子账线 0 的逐行增量 × 份额 (1 / 当日有数据的因子数) 在稠密格 [日并集轴 · kDayW] 上累加 → 起点 1 + 有增量的格逐格复利;
// 增仓量同份额累加到日 → buy [日并集轴]. 单因子线 ≡ 只含它的策略
struct Strat {
  Levels lv[kPairLines];
  std::vector<double> buy;
};
Strat strat_of(const std::vector<misc::Ymd> &axis, const std::vector<std::shared_ptr<const PortfolioBook>> &bs, double fixed_impact) {
  const size_t D = axis.size(), X = D * kDayW;
  assert(D > 0);
  std::vector<std::vector<size_t>> dix(bs.size()); // [因子][该因子日] → 并集轴下标
  std::vector<int> n_act(D, 0);                    // [并集轴] 当日有数据的因子数
  for (size_t q = 0; q < bs.size(); ++q) {
    dix[q] = days_on(axis, bs[q]->dates);
    for (const size_t i : dix[q])
      ++n_act[i];
  }
  std::vector<double> acc[kPairLines];
  for (std::vector<double> &a : acc)
    a.assign(X, 0.0);
  std::vector<uint8_t> hit(X, 0);
  Strat o;
  o.buy.assign(D, 0.0);
  for (size_t q = 0; q < bs.size(); ++q) {
    const factor::Geo g = factor_level(bs[q]->lv).geo();
    const size_t S = static_cast<size_t>(g.S);
    const BookLine bl(*bs[q], 0);
    for (size_t dl = 0; dl < dix[q].size(); ++dl) {
      const size_t d = dix[q][dl];
      const double k = 1.0 / n_act[d];
      for (size_t ts = 0; ts < S; ++ts) {
        const size_t t = dl * S + ts, x = x_at(d, static_cast<size_t>(g.minute(static_cast<int>(ts))));
        for (int p = 0; p < kPairLines; ++p)
          acc[p][x] += bl.inc(t, p, fixed_impact) * k;
        hit[x] = 1;
        o.buy[d] += bl.buy(t) * k;
      }
    }
  }
  std::vector<size_t> xs; // 有增量的格
  for (size_t x = 0; x < X; ++x)
    if (hit[x])
      xs.push_back(x);
  std::vector<double> r(xs.size());
  for (int p = 0; p < kPairLines; ++p) {
    for (size_t i = 0; i < xs.size(); ++i)
      r[i] = acc[p][xs[i]];
    o.lv[p].push(axis.front(), -kStartDx, 1.0);
    compound(r.data(), xs.size(), [&](size_t i, double v) { o.lv[p].push(axis[xs[i] / kDayW], static_cast<double>(xs[i] % kDayW), v); });
  }
  return o;
}

// 年换手 (倍/年): 增仓量 buy [T] (每日 S 行) 自首个有增仓的行所在日起的日均 × 每年交易日; 无增仓 → NaN
template <class R>
double turn_of(const R *buy, size_t T, size_t S) {
  size_t t0 = 0;
  while (t0 < T && !(buy[t0] > 0))
    ++t0;
  if (t0 == T)
    return kNaN;
  double s = 0.0;
  for (size_t t = t0; t < T; ++t)
    s += buy[t];
  const size_t days = T / S - t0 / S;
  return s / static_cast<double>(days) * factor::stat::kDaysPerYear;
}
double turn_of(const PortfolioBook &b, size_t line) {
  const size_t S = static_cast<size_t>(factor_level(b.lv).geo().S);
  return turn_of(b.at(kBookBuy, line), b.dates.size() * S, S);
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
    c.x.push_back(x_of(d, m)), c.y.push_back(s.lv[i]), c.at.push_back(static_cast<int32_t>(i) - 1), day.push_back(static_cast<int>(d));
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

// ---- 行 (因子 / 策略 / 篮子同一套): 行名等每次现拼; 线 / 年换手按源身份沿用上一份, 否则现算 ----
// 上一份派生的行 (按 id) + 日轴是否不变
struct Reuse {
  bool axis;
  std::map<std::string, PortfolioRow *> rows;
  Reuse(PortfolioDerived &o, const std::vector<misc::Ymd> &dates) : axis(dates == o.dates) {
    for (std::vector<PortfolioRow> &g : o.nav)
      for (PortfolioRow &r : g)
        rows[r.id] = &r;
  }
  // 日轴不变且 r 的源身份同上一份 → 线 / 年换手搬来, 返回 true
  bool take(PortfolioRow &r) const {
    const auto it = rows.find(r.id);
    if (!axis || it == rows.end() || !(it->second->src == r.src))
      return false;
    for (int l = 0; l < kPairLines; ++l)
      r.line[l] = std::move(it->second->line[l]);
    r.turn = it->second->turn;
    return true;
  }
};

// 算成的因子 [items 序]: 条目 + 账; 计数与日并集轴 (各账日的并) 写进 d
struct Ready {
  std::vector<const PortfolioItem *> its;
  std::vector<std::shared_ptr<const PortfolioBook>> books;
};
Ready ready_of(const std::vector<PortfolioItem> &items, const PortfolioResult &res, PortfolioDerived &d) {
  Ready f;
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
    f.its.push_back(&it);
    f.books.push_back(b->second);
    d.dates.insert(d.dates.end(), b->second->dates.begin(), b->second->dates.end());
  }
  std::sort(d.dates.begin(), d.dates.end());
  d.dates.erase(std::unique(d.dates.begin(), d.dates.end()), d.dates.end());
  return f;
}

// 因子行 (见头文件): 该因子单独的 G20 多头 = 自己账的线 0
void derive_factors(const Ready &f, double fixed_impact, const Reuse &ru, PortfolioDerived &d) {
  for (size_t q = 0; q < f.books.size(); ++q) {
    const PortfolioItem &it = *f.its[q];
    const PortfolioBook &b = *f.books[q];
    PortfolioRow r;
    r.id = it.e.key();
    r.en = std::filesystem::path(it.e.file).stem().string();
    r.cn = clip_name(it.name_cn);
    r.tag = it.name_en + " " + factor::stat::hold_name(it.e.hold);
    r.hover = r.en + "  " + it.name_cn + "\n• " + it.e.pool + " " + factor_level(it.e.lv).name + "\n• " + hold_desc(it.e.hold, it.e.lv) +
              "\n• 冲击 " + amt_name(b.amt) + "\n• 规范串 " + it.e.expr + (it.e.sign < 0 ? " · 取反" : "");
    r.src = {{&b}, 0, fixed_impact};
    if (!ru.take(r)) {
      for (int p = 0; p < kPairLines; ++p)
        r.line[p] = project(d.dates, levels(b, 0, p, fixed_impact));
      r.turn = turn_of(b, 0);
    }
    d.nav[kNavFactor].push_back(std::move(r));
  }
}

// 策略行 (见头文件): 各因子账按当日份额累加 (strat_of)
void derive_strat(const Ready &f, const std::string &name, double fixed_impact, const Reuse &ru, PortfolioDerived &d) {
  PortfolioRow s;
  s.id = "pf_strat";
  s.en = name, s.cn = "G20 多头";
  s.tag = s.hover = name;
  for (const std::shared_ptr<const PortfolioBook> &b : f.books)
    s.src.of.push_back(b.get());
  s.src.impact = fixed_impact;
  if (!d.dates.empty() && !ru.take(s)) {
    const Strat st = strat_of(d.dates, f.books, fixed_impact);
    for (int p = 0; p < kPairLines; ++p)
      s.line[p] = project(d.dates, st.lv[p]);
    s.turn = turn_of(st.buy.data(), st.buy.size(), 1);
  }
  d.nav[kNavStrat].push_back(std::move(s));
}

// 篮子行 (见头文件): 指数 / 行业 / 概念三组 = basket_group 三组; 可成交线 = 篮子账的该线, 理想线 = 价格指数 (prices) 的该线. 返回: 有行现算
bool derive_baskets(const PortfolioResult &res, const std::shared_ptr<const Database::Tape> &prices, const Reuse &ru, PortfolioDerived &d) {
  d.baskets = res.baskets && res.baskets->error.empty() ? res.baskets : nullptr;
  d.prices = prices && !prices->list.empty() ? prices : nullptr;
  bool built = false;
  for (int g = 0; g < kBasketGroups; ++g)
    for (const size_t line : basket_group(static_cast<BasketGroup>(g))) {
      PortfolioRow r;
      r.id = basket_id(line);
      r.en = cut_code(cut_of_line(line));
      r.hover = r.en + "  " + basket_name(line);
      r.cn = r.tag = clip_name(cut_name(cut_of_line(line))) + weight_suffix(weight_of(line));
      r.basket = static_cast<int>(line);
      r.src = {{d.baskets.get(), d.prices.get()}, line, 0.0};
      if (!ru.take(r)) {
        built = true;
        if (d.baskets) {
          r.line[kTrade] = project(d.dates, levels(*d.baskets, line, kTrade, 0.0));
          r.turn = turn_of(*d.baskets, line);
        }
        if (d.prices) {
          assert(d.prices->keys[line] == line);
          r.line[kIdeal] = project(d.dates, levels(*d.prices->list[line]));
        }
      }
      d.nav[g].push_back(std::move(r));
    }
  return built;
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

// ---- 估值口径 (roll_of / last_pct 同一份) ----
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

// 篮子账的日轴 (窗按日历天定界 / 加权): 一本账算一次, 各篮子各口共用
struct ValAxis {
  const std::vector<misc::Ymd> &hd;
  std::vector<int> dd; // [D] 日历天
  explicit ValAxis(const PortfolioBook &b) : hd(b.dates), dd(b.dates.size()) {
    for (size_t i = 0; i < hd.size(); ++i)
      dd[i] = days_of(hd[i]);
  }
};
// 一篮子一口的估值 v [D] + 窗: 日 t 的窗 = [lo(t), t] = (t 的 N 年前, t] 内的可算点, 点 u 的权 = roll_w(age(u, t), span(t));
// 首个可算日起史满 1 年 (ready) 才出
struct ValSeries {
  const ValAxis &ax;
  const float *v;
  int years;
  size_t first = 0; // 首个可算点 (无 = D)
  ValSeries(const ValAxis &a, const PortfolioBook &b, int line, int m, int years_) : ax(a), v(b.val_of(static_cast<size_t>(line), m)), years(years_) {
    assert(line >= 0);
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

// 估值 s 在线点 at (源下标升序, 起点 −1 无估值) 上的滚动分位 → o [点]: 窗内可算点按贵度保持有序 (逐日二分插入 / 出窗删除);
// 出点日扫两遍窗: 权重和 → 加权分位 = 累计权重首个 ≥ q · 总权重的点 + 当日加权估值分位. 权重按距今天数查表 (T 变才重建); 等值线档按全史
void roll_of(const ValSeries &s, const std::vector<int32_t> &at, PortfolioRoll &o) {
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  const size_t n = s.size(), np = at.size();
  const float *v = s.v;
  o.levels = levels_of(v, n);
  const auto by_dear = [&](size_t x, size_t y) { return dear_of(v[x]) < dear_of(v[y]); };
  std::vector<size_t> win; // 窗内可算点 (源下标), 贵度升序
  std::vector<double> wt;  // wt[a] = roll_w(a, T)
  int wt_T = -1;
  for (std::vector<float> &q : o.q)
    q.assign(np, kNaN);
  o.pct.assign(np, kNaN);
  o.band.assign(np, 0);
  size_t lo = 0, k = 0;
  while (k < np && at[k] < 0)
    ++k;
  int wp = 0; // 窗内收益率 > 0 的个数
  for (size_t t = 0; t < n && k < np; ++t) {
    if (s.ok(t))
      win.insert(std::upper_bound(win.begin(), win.end(), t, by_dear), t), wp += v[t] > 0.f;
    for (; s.gone(lo, t); ++lo)
      if (s.ok(lo)) {
        const auto [e0, e1] = std::equal_range(win.begin(), win.end(), lo, by_dear);
        const auto e = std::find(e0, e1, lo);
        assert(e != e1);
        win.erase(e), wp -= v[lo] > 0.f;
      }
    if (static_cast<size_t>(at[k]) != t)
      continue;
    const size_t i = k++;
    if (win.empty() || !s.ready(t))
      continue;
    const int T = s.span(t);
    assert(T > 0);
    if (T != wt_T) {
      wt.resize(static_cast<size_t>(T) + 1);
      for (int a = 0; a <= T; ++a)
        wt[static_cast<size_t>(a)] = roll_w(a, T);
      wt_T = T;
    }
    const auto w_of = [&](size_t u) {
      assert(s.age(u, t) <= T);
      return wt[static_cast<size_t>(s.age(u, t))];
    };
    double W = 0.0;
    for (const size_t u : win)
      W += w_of(u);
    double cum = 0.0, le = 0.0;
    int j = 0;
    for (const size_t u : win) {
      const double x = w_of(u);
      cum += x;
      for (; j < kBandQ && cum >= kBandQs[j] * W; ++j)
        o.q[j][i] = v[u];
      if (val_ok(v[t]) && dear_of(v[u]) <= dear_of(v[t]))
        le += x;
    }
    assert(j == kBandQ);
    o.band[i] = wp == static_cast<int>(win.size());
    if (val_ok(v[t]))
      o.pct[i] = pct_of(le, W);
  }
  assert(k == np && "at 须升序且落在账日内");
}

// 估值 s 在线点 [0, j1) 内末个可算点及其估值分位: 与 roll_of 的 pct 同一定义, 只一点 → 直接数窗, O(窗)
PortfolioLastPct last_pct(const ValSeries &s, const std::vector<int32_t> &at, size_t j1) {
  for (size_t j = j1; j > 0 && at[j - 1] >= 0; --j) {
    const size_t t = static_cast<size_t>(at[j - 1]);
    if (!s.ok(t) || !s.ready(t))
      continue;
    const float m = dear_of(s.v[t]);
    const int T = s.span(t);
    double w = 0.0, le = 0.0;
    for (size_t u = s.lo(t); u <= t; ++u)
      if (s.ok(u)) {
        const double x = roll_w(s.age(u, t), T);
        w += x, le += dear_of(s.v[u]) <= m ? x : 0.0;
      }
    return {j - 1, pct_of(le, w)};
  }
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
                          const std::shared_ptr<const Database::Tape> &prices, double fixed_impact, PortfolioDerived &d) {
  PortfolioDerived o = std::move(d);
  d = PortfolioDerived{};
  const Ready f = ready_of(items, res, d);
  const Reuse ru(o, d.dates);
  derive_factors(f, fixed_impact, ru, d);
  derive_strat(f, name, fixed_impact, ru, d);
  d.basket_gen = o.basket_gen + derive_baskets(res, prices, ru, d);
  for (int g = 0; g < kNavGroups; ++g)
    for (size_t r = 0; r < d.nav[g].size(); ++r)
      d.index[d.nav[g][r].id] = {g, r};
  paint(d);
}

void MakePortfolioRoll(const PortfolioDerived &d, PortfolioAt vs, int years, std::array<PortfolioRoll, kPoolVals> &r) {
  for (PortfolioRoll &x : r)
    x = {};
  if (vs.g < 0 || !d.baskets)
    return;
  const ValAxis ax(*d.baskets);
  const PortfolioRow &s = d.nav[vs.g][vs.r];
  for (int b = 0; b < kPoolVals; ++b)
    roll_of(ValSeries(ax, *d.baskets, s.basket, b, years), s.line[kTrade].at, r[b]);
}

void MakePortfolioPct(const PortfolioDerived &d, int d1, int years, PortfolioPct &p) {
  for (int g = 0; g < kBasketGroups; ++g)
    p[g].assign(d.nav[g].size(), {});
  if (!d.baskets)
    return;
  const ValAxis ax(*d.baskets);
  const double x1 = x_of(static_cast<size_t>(d1), kOpen);
  for (int g = 0; g < kBasketGroups; ++g)
    for (size_t i = 0; i < d.nav[g].size(); ++i) {
      const PortfolioRow &s = d.nav[g][i];
      const PortfolioLine &c = s.line[kTrade];
      const size_t j1 = static_cast<size_t>(std::lower_bound(c.x.begin(), c.x.end(), x1) - c.x.begin()); // = 切片的 j1
      for (int b = 0; b < kPoolVals; ++b)
        p[g][i][b] = last_pct(ValSeries(ax, *d.baskets, s.basket, b, years), c.at, j1);
    }
}

void MakePortfolioView(const PortfolioDerived &d, int d0, int d1, PortfolioAt vs, const std::array<PortfolioRoll, kPoolVals> &roll,
                       const PortfolioPct &pct, PortfolioView &v) {
  // ---- 各线同一份切片; NAV 右表: 各数值列 + 同组同列名次 ----
  for (int g = 0; g < kNavGroups; ++g) {
    const size_t n = d.nav[g].size();
    v.nav[g].clear();
    v.num[g].assign(n, {});
    for (size_t i = 0; i < n; ++i) {
      const PortfolioRow &r = d.nav[g][i];
      v.nav[g].push_back({slice_of(r.line[kTrade], d0, d1), slice_of(r.line[kIdeal], d0, d1)});
      const std::array<PortfolioSlice, kPairLines> &s = v.nav[g].back();
      std::array<Ranked, kNavNums> &c = v.num[g][i];
      c[kNumTrade].x = s[kTrade].ann, c[kNumIdeal].x = s[kIdeal].ann;
      c[kNumSharpe].x = s[kTrade].sharpe, c[kNumTurn].x = r.turn;
    }
    for (int k = 0; k < kNavNums; ++k)
      rank_col(n, kNavGood[k], [&](size_t i) -> Ranked & { return v.num[g][i][k]; });
  }

  // ---- VAL 表格: 各篮子行各口切片内末个可算点的估值分位 (= 区间末之前的末点落在切片内) + 同组同口名次 (分位低 = 便宜 = 好) ----
  for (int g = 0; g < kBasketGroups; ++g) {
    const size_t n = d.nav[g].size();
    assert(pct[g].size() == n);
    v.pct[g].assign(n, {});
    for (size_t i = 0; i < n; ++i) {
      const PortfolioSlice &sl = v.nav[g][i][kTrade];
      for (int b = 0; b < kPoolVals; ++b)
        if (const PortfolioLastPct &lp = pct[g][i][b]; sl.size() > 0 && lp.j != SIZE_MAX && lp.j >= sl.j0)
          v.pct[g][i][b].x = lp.pct;
    }
    for (int b = 0; b < kPoolVals; ++b)
      rank_col(n, -1, [&](size_t i) -> Ranked & { return v.pct[g][i][b]; });
  }

  // ---- VAL 选中篮子各口 band: 与价格线同一切片, y = P · v / q (v = 当日收益率; v ≤ 0 / q ≤ 0 → 价格无解, 断开; 窗内有任一无解点 → 不画);
  //      等值线单位点位 unit = P · v (只看当日, 不看滚动分位; v ≤ 0 → 断开), 档 = 二级的全史倍数档 ----
  v.band_x.clear();
  for (PortfolioBand &bd : v.band)
    bd = {};
  if (vs.g < 0 || !d.baskets)
    return;
  const PortfolioRow &s = d.nav[vs.g][vs.r];
  const std::vector<int32_t> &at = s.line[kTrade].at;
  const PortfolioSlice &sl = v.nav[vs.g][vs.r][kTrade];
  const size_t n = sl.size();
  for (size_t i = 0; i < n; ++i)
    v.band_x.push_back(sl.x(i));
  for (int b = 0; b < kPoolVals; ++b) {
    const PortfolioRoll &r = roll[b];
    assert(r.pct.size() == at.size());
    PortfolioBand &bd = v.band[b];
    const float *hv = d.baskets->val_of(static_cast<size_t>(s.basket), b);
    for (std::vector<double> &y : bd.y)
      y.assign(n, kNaN);
    bd.unit.assign(n, kNaN);
    bd.levels = r.levels;
    size_t last = SIZE_MAX; // 末个有分位的点 (线点下标)
    for (size_t i = 0; i < n; ++i) {
      const size_t j = sl.j0 + i;
      const double x = at[j] >= 0 ? hv[at[j]] : kNaN;
      if (x > 0.0)
        bd.unit[i] = sl.y(i) * x;
      if (!std::isfinite(r.pct[j]))
        continue;
      last = j;
      if (!(x > 0.0) || !r.band[j])
        continue;
      for (int q = 0; q < kBandQ; ++q)
        if (r.q[q][j] > 0.f)
          bd.y[q][i] = sl.y(i) * x / r.q[q][j];
      if (!(r.q[kBandQ - 1][j] > 0.f)) // q 降序: 最贵档 > 0 ⇔ 各档全可画 (填色只画全有限段)
        continue;
      const int k = static_cast<int>(i);
      if (bd.segs.empty() || bd.segs.back().second != k)
        bd.segs.push_back({k, k + 1});
      else
        ++bd.segs.back().second;
    }
    if (last == SIZE_MAX)
      continue;
    bd.ok = true;
    bd.now = hv[at[last]], bd.pct = r.pct[last];
  }
}

} // namespace GUI::Factors
