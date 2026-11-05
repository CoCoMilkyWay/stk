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

// 线 c 在日并集轴 [lo, lo + n) 各日的日收益 (日末值逐日比 − 1): 无点的日沿用前一日末值 (lo 前的末点起), 不可算 = NaN
std::vector<double> rets_of(const PortfolioCurve &c, size_t lo, size_t n) {
  const auto it = std::lower_bound(c.x.begin(), c.x.end(), x_of(lo, kOpen));
  std::vector<double> e(n + 1, kNaN); // [0] = lo 前末点, [1 + k] = lo + k 日末
  if (it != c.x.begin())
    e[0] = c.y[static_cast<size_t>(it - c.x.begin()) - 1];
  for (auto i = it; i != c.x.end(); ++i) {
    const size_t k = static_cast<size_t>(*i) / kDayW - lo;
    assert(k < n);
    e[1 + k] = c.y[static_cast<size_t>(i - c.x.begin())];
  }
  for (size_t k = 1; k <= n; ++k)
    if (std::isnan(e[k]))
      e[k] = e[k - 1];
  std::vector<double> r(n);
  for (size_t k = 0; k < n; ++k)
    r[k] = e[k] > 0 && e[k + 1] > 0 ? e[k + 1] / e[k] - 1.0 : kNaN;
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

// ---- 增量流 (组合 / 单因子同一份) ----
// x 严格升序; d[PairLine] = 该时刻实现的净值增量 (占本金; 有冲击 / 无冲击)
struct Inc {
  std::vector<double> x, d[kPairLines];
  // 追加 at 处的增量 r[kPairLines] (at 不降; 同 x 并入末点)
  void add(double at, const double *r) {
    assert(x.empty() || at >= x.back());
    if (x.empty() || at != x.back()) {
      x.push_back(at);
      for (std::vector<double> &v : d)
        v.push_back(0.0);
    }
    for (int l = 0; l < kPairLines; ++l)
      d[l].back() += r[l];
  }
};
// 两条增量流按 x 归并, 同 x 相加
Inc merge(const Inc &a, const Inc &b) {
  Inc o;
  size_t i = 0, j = 0;
  while (i < a.x.size() || j < b.x.size()) {
    const bool ta = j == b.x.size() || (i < a.x.size() && a.x[i] <= b.x[j]);
    const Inc &s = ta ? a : b;
    const size_t k = ta ? i++ : j++;
    double r[kPairLines];
    for (int l = 0; l < kPairLines; ++l)
      r[l] = s.d[l][k];
    o.add(s.x[k], r);
  }
  return o;
}
// 多条增量流之和: 两两归并 (O(P log n), P = 总点数)
Inc sum_of(std::vector<Inc> v) {
  assert(!v.empty());
  while (v.size() > 1) {
    std::vector<Inc> h;
    for (size_t i = 0; i + 1 < v.size(); i += 2)
      h.push_back(merge(v[i], v[i + 1]));
    if (v.size() % 2)
      h.push_back(std::move(v.back()));
    v = std::move(h);
  }
  return std::move(v.front());
}
// 增量流的线 l → 净值线: 起点 (x0, 1) + 每个 x 一点, 逐点复利. 起点取首日开盘前 kStartDx 格: 首行可在开盘点就有增量 (L_1min 行 0 末分钟 = 开盘)
constexpr double kStartDx = 0.5;
PortfolioCurve curve_of(const Inc &s, int l, double x0) {
  assert(s.x.empty() || s.x.front() > x0);
  PortfolioCurve c;
  c.x.reserve(s.x.size() + 1), c.y.reserve(s.x.size() + 1);
  c.x.push_back(x0), c.y.push_back(1.0);
  compound(s.d[l].data(), s.x.size(), [&](size_t k, double y) { c.x.push_back(s.x[k]), c.y.push_back(y); });
  return c;
}

// 因子 f 的增量流 (di = f 各日在并集轴上的下标): FactorCurve 逐行增量 (fold_book 已实现口径, 与池指数对齐) × wt(实现日并集轴下标), 记在行末分钟
// (日层行末 = 收盘)
template <class Wt>
Inc inc_of(const FactorCurve &f, const std::vector<size_t> &di, double fixed_impact, const Wt &wt) {
  const factor::Geo g = factor_level(f.lv).geo();
  const int S = g.S;
  assert(!f.dates.empty() && f.nav.n == kCurveLines && static_cast<size_t>(f.nav.P) == f.dates.size() * static_cast<size_t>(S));
  const float *pnl = f.nav_at(kCurvePnl), *imp = f.nav_at(kCurveImp), *fix = f.nav_at(kCurveFix);
  const auto row_x = [&](size_t dl, int ts) -> size_t { return x_at(di[dl], static_cast<size_t>(g.minute(ts))); };
  Inc o;
  for (size_t dl = 0; dl < di.size(); ++dl) {
    const double k = wt(di[dl]);
    for (int ts = 0; ts < S; ++ts) {
      const size_t t = dl * S + static_cast<size_t>(ts);
      double r[kPairLines];
      r[kTrade] = (pnl[t] - imp[t] - fix[t] * fixed_impact) * k;
      r[kIdeal] = pnl[t] * k;
      o.add(static_cast<double>(row_x(dl, ts)), r);
    }
  }
  return o;
}

// ---- 篮子投影 (可成交线 = BasketCurve, 理想线 = 价格指数; 同一投影): 源 = 日点位线, 只取日并集轴范围内 = 范围首日开盘一点 (其前一日点位; 源首日无)
//      + 轴上每日收盘一点; 不在轴上的日只差点位, 收益自然并进下一个轴上的日 ----
// 日点位线: dates[i] 收盘点位 lv[i]
struct Levels {
  std::vector<misc::Ymd> dates;
  std::vector<double> lv;
};
// 可成交指数篮子线: 日收益复利 (起点 1)
Levels levels(const BasketCurve &bc, size_t line) {
  Levels s{bc.dates, std::vector<double>(bc.dates.size())};
  compound(bc.index_of(line), s.dates.size(), [&](size_t i, double l) { s.lv[i] = l; });
  return s;
}
// 价格指数篮子线: 日线收盘
Levels levels(const Database::Kline &kl) {
  Levels s;
  for (const Database::Candle &e : kl.daily)
    s.dates.push_back(e.date), s.lv.push_back(e.close);
  return s;
}
// → c (须空); 返回 [c.x] → s.dates 下标 (严格升序)
std::vector<uint32_t> project(const std::vector<misc::Ymd> &axis, const Levels &s, PortfolioCurve &c) {
  assert(c.x.empty() && !axis.empty());
  std::vector<uint32_t> at;
  const std::vector<size_t> di = days_on(axis, s.dates);
  const auto push = [&](double x, size_t i) { c.x.push_back(x), c.y.push_back(s.lv[i]), at.push_back(static_cast<uint32_t>(i)); };
  for (size_t i = 0; i < s.dates.size(); ++i) {
    if (s.dates[i] < axis.front() || s.dates[i] > axis.back())
      continue;
    if (at.empty() && i > 0)
      push(x_of(di[i], kOpen), i - 1);
    if (axis[di[i]] == s.dates[i])
      push(x_of(di[i], kClose), i);
  }
  return at;
}

// 组合 + 单因子 (见头文件): 每因子两条增量流 — 自身 (权 1) → 单因子线; 份额 (权 1 / 当日因子数) → Σ → 组合线. 单因子线 ≡ 只含它的组合.
// o = 上一份派生: 同一条 FactorCurve 且日轴 / fixed_impact 不变 → 单因子线沿用
void derive_factors(const std::vector<PortfolioItem> &items, const std::string &name, const PortfolioResult &res, double fixed_impact,
                    PortfolioDerived &o, PortfolioDerived &d) {
  std::vector<const PortfolioItem *> its;
  std::vector<std::shared_ptr<const FactorCurve>> &fc = d.src;
  for (const PortfolioItem &it : items) {
    if (!it.missing.empty())
      continue;
    const auto f = res.factors.find(it.e.key());
    if (f == res.factors.end()) {
      ++d.n_wait;
      continue;
    }
    if (!f->second->error.empty()) {
      ++d.n_error;
      continue;
    }
    ++d.n_ready;
    its.push_back(&it);
    fc.push_back(f->second);
  }
  for (const std::shared_ptr<const FactorCurve> &f : fc)
    d.dates.insert(d.dates.end(), f->dates.begin(), f->dates.end());
  std::sort(d.dates.begin(), d.dates.end());
  d.dates.erase(std::unique(d.dates.begin(), d.dates.end()), d.dates.end());
  d.impact = fixed_impact;
  std::map<const FactorCurve *, size_t> keep; // 可沿用的: 源 → o 的因子行
  if (d.dates == o.dates && fixed_impact == o.impact)
    for (size_t q = 0; q < o.src.size(); ++q)
      keep[o.src[q].get()] = q;
  std::vector<std::vector<size_t>> dix(fc.size()); // [因子][该因子日] → 并集轴下标
  for (size_t q = 0; q < fc.size(); ++q)
    dix[q] = days_on(d.dates, fc[q]->dates);
  std::vector<int> n_act(d.dates.size(), 0); // [并集轴] 当日有数据的因子数
  for (const std::vector<size_t> &di : dix)
    for (const size_t i : di)
      ++n_act[i];

  std::vector<Inc> shares; // [因子] 份额流
  double strat_buy = 0.0;  // Σ 份额 · 增仓量 (组合年换手)
  for (size_t q = 0; q < fc.size(); ++q) {
    const FactorCurve &f = *fc[q];
    const std::vector<size_t> &di = dix[q];
    const size_t S = static_cast<size_t>(factor_level(f.lv).geo().S);
    const float *buy = f.nav_at(kCurveBuy);
    double own_buy = 0.0;
    for (size_t dl = 0; dl < di.size(); ++dl)
      for (size_t ts = 0; ts < S; ++ts) {
        own_buy += buy[dl * S + ts];
        strat_buy += buy[dl * S + ts] / n_act[di[dl]];
      }
    const PortfolioItem &it = *its[q];
    const auto kp = keep.find(&f);
    PortfolioRow r;
    r.id = it.e.key();
    r.en = std::filesystem::path(it.e.file).stem().string();
    r.cn = clip_name(it.name_cn);
    r.tag = it.name_en + " " + factor::stat::hold_name(f.hold);
    r.hover = r.en + "  " + it.name_cn + "\n• " + it.e.pool + " " + factor_level(it.e.lv).name + "\n• " + hold_desc(it.e.hold, it.e.lv) +
              "\n• 冲击 " + amt_name(f.amt) + "\n• 规范串 " + it.e.expr + (it.e.sign < 0 ? " · 取反" : "");
    r.turn = own_buy / static_cast<double>(f.dates.size()) * factor::stat::kDaysPerYear;
    if (kp != keep.end()) {
      for (int l = 0; l < kPairLines; ++l)
        r.line[l] = std::move(o.nav[kNavFactor][kp->second].line[l]);
    } else {
      const Inc own = inc_of(f, di, fixed_impact, [](size_t) { return 1.0; });
      for (int l = 0; l < kPairLines; ++l)
        r.line[l] = curve_of(own, l, x_of(di.front(), kOpen) - kStartDx);
    }
    d.nav[kNavFactor].push_back(std::move(r));
    shares.push_back(inc_of(f, di, fixed_impact, [&](size_t i) { return 1.0 / n_act[i]; }));
  }
  const Inc sum = shares.empty() ? Inc{} : sum_of(std::move(shares));
  PortfolioRow s;
  s.id = "pf_strat";
  s.en = name, s.cn = "G20 多头";
  s.tag = s.hover = name;
  if (!d.dates.empty()) {
    s.turn = strat_buy / static_cast<double>(d.dates.size()) * factor::stat::kDaysPerYear;
    for (int l = 0; l < kPairLines; ++l)
      s.line[l] = curve_of(sum, l, x_of(0, kOpen) - kStartDx);
  }
  d.nav[kNavStrat].push_back(std::move(s));
}

// 篮子行 (见头文件): 指数 / 行业 / 概念三组 = basket_group 三组, 一行两条 = BasketCurve 可成交指数 / 价格指数 (prices) 的同一篮子线, 同一 project.
// o = 上一份派生: 同一份 BasketCurve、同一份价格指数且日轴不变 → 整份沿用
void derive_baskets(const PortfolioResult &res, const std::shared_ptr<const Database::Tape> &prices, PortfolioDerived &o, PortfolioDerived &d) {
  d.baskets = res.baskets && res.baskets->error.empty() ? res.baskets : nullptr;
  d.prices = prices && !prices->list.empty() ? prices : nullptr;
  if (o.basket_gen > 0 && d.baskets == o.baskets && d.prices == o.prices && d.dates == o.dates) {
    for (int g = 0; g < kBasketGroups; ++g)
      d.nav[g] = std::move(o.nav[g]);
    d.basket_gen = o.basket_gen;
    return;
  }
  d.basket_gen = o.basket_gen + 1;
  for (int g = 0; g < kBasketGroups; ++g)
    for (const size_t line : basket_group(static_cast<BasketGroup>(g))) {
      PortfolioRow r;
      r.id = basket_id(line);
      r.en = cut_code(cut_of_line(line));
      r.hover = r.en + "  " + basket_name(line);
      r.cn = r.tag = clip_name(cut_name(cut_of_line(line))) + weight_suffix(weight_of(line));
      r.basket = static_cast<int>(line);
      if (d.baskets && !d.dates.empty()) {
        r.turn = d.baskets->turn[line];
        r.at = project(d.dates, levels(*d.baskets, line), r.line[kTrade]);
      }
      if (d.prices && !d.dates.empty()) {
        assert(d.prices->keys[line] == line);
        project(d.dates, levels(d.prices->list[line]), r.line[kIdeal]);
      }
      d.nav[g].push_back(std::move(r));
    }
}

// 曲线 c 在 [x0, x1) 的切片 (见 PortfolioSlice); 整条在区间外 → 空. 年化 = 末点净值按首末点所跨交易日数复利折年 (%);
// 夏普 = 同一段交易日逐日收益 (日末点净值 / 上日末点 − 1, 首日对首点) 的 mean / sd (ddof = 1) × √每年交易日
PortfolioSlice slice_of(const PortfolioCurve &c, double x0, double x1) {
  PortfolioSlice s;
  s.c = &c, s.x0 = x0;
  const size_t i0 = static_cast<size_t>(std::lower_bound(c.x.begin(), c.x.end(), x0) - c.x.begin());
  const size_t i1 = static_cast<size_t>(std::lower_bound(c.x.begin(), c.x.end(), x1) - c.x.begin());
  if (i0 == c.x.size() || i1 == 0)
    return s;
  s.j0 = i0 > 0 ? i0 - 1 : 0, s.j1 = i1;
  s.base = c.y[s.j0];
  if (s.size() < 2)
    return s;
  const size_t e = s.size() - 1;
  const double days = std::floor(s.x(e) / kDayW) - std::floor(s.x(0) / kDayW) + 1.0;
  s.ann = static_cast<float>(100.0 * factor::stat::ann_geo(s.y(e), days));
  double n = 0.0, s1 = 0.0, s2 = 0.0, prev = s.y(0), last = s.y(0);
  const auto close_day = [&] {
    const double r = last / prev - 1.0;
    n += 1.0, s1 += r, s2 += r * r, prev = last;
  };
  double day = std::floor(s.x(0) / kDayW);
  for (size_t i = 1; i <= e; ++i) {
    for (const double di = std::floor(s.x(i) / kDayW); day < di; ++day)
      close_day();
    last = s.y(i);
  }
  close_day();
  assert(n == days);
  const double var = (s2 - s1 * s1 / n) / (n - 1.0);
  if (n >= 2.0 && var > 0.0)
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
// 当日 day 的窗跨度 T (日历天; first = 首个可算日)
int span_of(misc::Ymd day, misc::Ymd first, int years) { return days_of(day) - days_of(std::max(years_before(day, years), first)); }

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

// 篮子行 s 的口 b 的滚动分位 → o [s.at]: 窗内可算点按贵度保持有序 (逐日二分插入 / 出窗删除);
// 出点日扫两遍窗: 权重和 → 加权分位 = 累计权重首个 ≥ q · 总权重的点 + 当日加权估值分位. 权重按距今天数查表 (T 变才重建); 等值线档按全史
void roll_of(const BasketCurve &bc, const PortfolioRow &s, int b, int years, PortfolioRoll &o) {
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  const std::vector<misc::Ymd> &hd = bc.dates;
  const size_t n = hd.size(), np = s.at.size();
  const float *v = bc.val_of(static_cast<size_t>(s.basket), b);
  o.levels = levels_of(v, n);
  std::vector<int> dd(n); // 日历天
  for (size_t i = 0; i < n; ++i)
    dd[i] = days_of(hd[i]);
  const auto by_dear = [&](size_t x, size_t y) { return dear_of(v[x]) < dear_of(v[y]); };
  std::vector<size_t> win; // 窗内可算点 (hd 下标), 贵度升序
  std::vector<double> wt;  // wt[a] = roll_w(a, T)
  int wt_T = -1;
  for (std::vector<float> &q : o.q)
    q.assign(np, kNaN);
  o.pct.assign(np, kNaN);
  o.band.assign(np, 0);
  misc::Ymd first = 0; // 首个可算日
  size_t lo = 0, k = 0;
  int wp = 0; // 窗内收益率 > 0 的个数
  for (size_t t = 0; t < n && k < np; ++t) {
    if (val_ok(v[t])) {
      win.insert(std::upper_bound(win.begin(), win.end(), t, by_dear), t), wp += v[t] > 0.f;
      if (first == 0)
        first = hd[t];
    }
    for (; hd[lo] <= years_before(hd[t], years); ++lo)
      if (val_ok(v[lo])) {
        const auto [e0, e1] = std::equal_range(win.begin(), win.end(), lo, by_dear);
        const auto e = std::find(e0, e1, lo);
        assert(e != e1);
        win.erase(e), wp -= v[lo] > 0.f;
      }
    if (s.at[k] != t)
      continue;
    const size_t i = k++;
    if (win.empty() || first == 0 || first > years_before(hd[t], 1))
      continue;
    const int T = span_of(hd[t], first, years);
    assert(T > 0);
    if (T != wt_T) {
      wt.resize(static_cast<size_t>(T) + 1);
      for (int a = 0; a <= T; ++a)
        wt[static_cast<size_t>(a)] = roll_w(a, T);
      wt_T = T;
    }
    const auto w_of = [&](size_t u) {
      assert(dd[t] - dd[u] <= T);
      return wt[static_cast<size_t>(dd[t] - dd[u])];
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
  assert(k == np && "at 须升序且落在 hd 内");
}

// 篮子行 s 的口 b 在切片 sl 内末个可算点的估值分位 (NaN = 无): 与 roll_of 的 pct 同一定义, 只一点 → 直接数窗, O(窗)
double last_pct(const BasketCurve &bc, const PortfolioRow &s, const PortfolioSlice &sl, int b, int years) {
  const std::vector<misc::Ymd> &hd = bc.dates;
  const float *v = bc.val_of(static_cast<size_t>(s.basket), b);
  size_t f = 0; // 首个可算日 (无 → hd.size(), 下面 val_ok 先短路)
  while (f < hd.size() && !val_ok(v[f]))
    ++f;
  for (size_t j = sl.j1; j > sl.j0; --j) {
    const size_t t = s.at[j - 1];
    if (!val_ok(v[t]) || hd[f] > years_before(hd[t], 1))
      continue;
    const float m = dear_of(v[t]);
    const int T = span_of(hd[t], hd[f], years), d = days_of(hd[t]);
    double w = 0.0, le = 0.0;
    for (size_t u = static_cast<size_t>(std::upper_bound(hd.begin(), hd.begin() + t, years_before(hd[t], years)) - hd.begin()); u <= t; ++u)
      if (val_ok(v[u])) {
        const double x = roll_w(d - days_of(hd[u]), T);
        w += x, le += dear_of(v[u]) <= m ? x : 0.0;
      }
    return pct_of(le, w);
  }
  return std::numeric_limits<double>::quiet_NaN();
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
// 切片的区间年化 (%); 不足两点 → NaN
double ann_of(const PortfolioSlice &s) { return s.size() >= 2 ? static_cast<double>(s.ann) : kNaN; }

} // namespace

void MakePortfolioDerived(const std::vector<PortfolioItem> &items, const std::string &name, const PortfolioResult &res,
                          const std::shared_ptr<const Database::Tape> &prices, double fixed_impact, PortfolioDerived &d) {
  PortfolioDerived o = std::move(d);
  d = PortfolioDerived{};
  derive_factors(items, name, res, fixed_impact, o, d);
  derive_baskets(res, prices, o, d);
  paint(d);
}

void MakePortfolioRoll(const PortfolioDerived &d, PortfolioAt vs, int years, std::array<PortfolioRoll, kPoolVals> &r) {
  for (int b = 0; b < kPoolVals; ++b)
    if (vs.g >= 0 && d.baskets)
      roll_of(*d.baskets, d.nav[vs.g][vs.r], b, years, r[b]);
    else
      r[b] = {};
}

void MakePortfolioView(const PortfolioDerived &d, int d0, int d1, PortfolioAt vs, int years, const std::array<PortfolioRoll, kPoolVals> &roll,
                       PortfolioView &v) {
  const double x0 = x_of(static_cast<size_t>(d0), kOpen), x1 = x_of(static_cast<size_t>(d1), kOpen);
  // ---- 各线同一份切片; NAV 右表: 各数值列 + 同组同列名次 ----
  for (int g = 0; g < kNavGroups; ++g) {
    const size_t n = d.nav[g].size();
    v.nav[g].clear();
    v.num[g].assign(n, {});
    for (size_t i = 0; i < n; ++i) {
      const PortfolioRow &r = d.nav[g][i];
      v.nav[g].push_back({slice_of(r.line[kTrade], x0, x1), slice_of(r.line[kIdeal], x0, x1)});
      const std::array<PortfolioSlice, kPairLines> &s = v.nav[g].back();
      std::array<Ranked, kNavNums> &c = v.num[g][i];
      c[kNumTrade].x = ann_of(s[kTrade]), c[kNumIdeal].x = ann_of(s[kIdeal]);
      c[kNumSharpe].x = s[kTrade].sharpe, c[kNumTurn].x = r.turn;
    }
    for (int k = 0; k < kNavNums; ++k)
      rank_col(n, kNavGood[k], [&](size_t i) -> Ranked & { return v.num[g][i][k]; });
  }

  // ---- VAL 表格: 各篮子行各口切片内末个可算点的估值分位 + 同组同口名次 (分位低 = 便宜 = 好) ----
  for (int g = 0; g < kBasketGroups; ++g) {
    const size_t n = d.nav[g].size();
    v.pct[g].assign(n, {});
    if (d.baskets)
      for (size_t i = 0; i < n; ++i)
        for (int b = 0; b < kPoolVals; ++b)
          v.pct[g][i][b].x = last_pct(*d.baskets, d.nav[g][i], v.nav[g][i][kTrade], b, years);
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
  const BasketCurve &bc = *d.baskets;
  const PortfolioRow &s = d.nav[vs.g][vs.r];
  const PortfolioSlice &sl = v.nav[vs.g][vs.r][kTrade];
  const size_t n = sl.size();
  for (size_t i = 0; i < n; ++i)
    v.band_x.push_back(sl.x(i));
  for (int b = 0; b < kPoolVals; ++b) {
    const PortfolioRoll &r = roll[b];
    assert(r.pct.size() == s.at.size());
    PortfolioBand &bd = v.band[b];
    const float *hv = bc.val_of(static_cast<size_t>(s.basket), b);
    for (std::vector<double> &y : bd.y)
      y.assign(n, kNaN);
    bd.unit.assign(n, kNaN);
    bd.levels = r.levels;
    size_t last = SIZE_MAX; // 末个有分位的点 (源下标)
    for (size_t i = 0; i < n; ++i) {
      const size_t j = sl.j0 + i;
      const double x = hv[s.at[j]];
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
    bd.now = hv[s.at[last]], bd.pct = r.pct[last];
  }
}

} // namespace GUI::Factors
