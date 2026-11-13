// Market 行情页派生层 — 见头文件
#include "gui/task_market/ui/LinesView.hpp"

#include "fundamental/AssetAxis.hpp" // asset_axis (成分行代码)
#include "gui/engine/Hold.hpp"       // hold_desc
#include "gui/util/Chart.hpp"        // rank_by
#include "misc/date.hpp"             // to_days (滚动窗权重按日历天)
#include "misc/exec.hpp"             // ForkJoin (缺的线并行现算)
#include "shared/Basket.hpp"         // basket_table

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iterator>
#include <limits>
#include <map>
#include <numeric>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace GUI::Market {

namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

// 有行情键的行 id (两侧各一个前缀): 篮子 = pf_ + 切片代码 + 权重后缀 [Weight] (概念换快照线号会变, 切片代码不变) / 个股 = stk_ + 全局 id
constexpr const char *kWeightId[pool::kWeights] = {"", "_cap"};
constexpr std::string_view kStockId = "stk_";
std::string basket_id(size_t b) { return "pf_" + basket_table().code(b) + kWeightId[basket_table().weight(b)]; }
std::string stock_id(uint32_t id) { return std::string(kStockId) + std::to_string(id); }

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

// ---- 行配色 (配色组一起, 见 LinesDerived::rows): 相似度 s = max(成员重合, 残差相关), 平方距离 2 (1 − s) 上 Ward 树定叶序;
//      叶序相邻两行的色相差 ∝ 它们在树上分开处两侧的平均距离 (1 − s), 累加后铺满色环的前 kArc (首尾不相接) ----
constexpr size_t kSimDays = 60; // 残差相关取日并集轴末几日的日收益
constexpr size_t kSimMin = 20;  // 两行共同有收益的日数下限 (不足 → 相关记 0)
constexpr float kArc = 0.85f;   // 色相铺开的弧长 (整环 = 1)

// 线 c 在日并集轴 [lo, lo + n) 各日的日收益 (日末视图逐日比 − 1; 不可算 = NaN)
std::vector<double> rets_of(const Line &c, size_t lo, size_t n) {
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

// 配色组 (kGroupDefs::paint) 各行定色相 (见上); 因子没到齐 / 价格指数未就绪 / 日轴不足两日 → 全不定色 (灰, 免得边算边跳).
// 配色来源 (LinesPaintKey) 同上一份 o → 按 id 照搬
void paint(LinesDerived &d, const LinesDerived &o) {
  std::vector<LinesRow *> rs;
  size_t mkt = SIZE_MAX; // 全A.加 (残差的基准)
  LinesPaintKey &key = d.paint;
  for (int g = 0; g < kRowGroups; ++g)
    if (kGroupDefs[g].paint)
      for (LinesRow &r : d.rows[g]) {
        if (r.ref.is(Quote::kBasket) && r.ref.key == basket_table().at_pool(kAllAPool[pool::kWeightCap]))
          mkt = rs.size();
        rs.push_back(&r);
        key.rows.emplace_back(r.id, r.line[kIdeal].get());
        r.hue = std::numeric_limits<float>::quiet_NaN();
      }
  const size_t n = rs.size(), D = d.dates.size();
  assert(mkt < n);
  const Quote::Tape *bt = d.baskets();
  key.prices = bt;
  key.ready = d.n_wait == 0 && bt && D >= 2;
  if (key == o.paint) {
    for (LinesRow *r : rs)
      r->hue = o.row(o.at(r->id)).hue;
    return;
  }
  if (!key.ready)
    return;

  const size_t T = std::min(kSimDays, D - 1), lo = D - T;
  const std::vector<double> m = rets_of(*rs[mkt]->line[kIdeal], lo, T);
  std::vector<std::vector<double>> res(n); // [行] 残差日收益
  for (size_t i = 0; i < n; ++i) {
    res[i] = rets_of(*rs[i]->line[kIdeal], lo, T);
    residual(res[i], m);
  }
  std::fill(res[mkt].begin(), res[mkt].end(), kNaN); // 自身对自身回归无残差
  const size_t K = bt->keys.size();
  assert(bt->overlap.size() == K * K);
  std::vector<double> far(n * n, 0.0); // [行 × 行] 1 − s
  for (size_t i = 0; i < n; ++i)
    for (size_t j = i + 1; j < n; ++j) {
      double s = corr(res[i], res[j]);
      if (const Quote::Ref &a = rs[i]->ref, &b = rs[j]->ref; a.is(Quote::kBasket) && b.is(Quote::kBasket))
        s = std::max(s, static_cast<double>(bt->overlap[a.key * K + b.key]));
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

// ---- 线的来路 (两种线源: 腿集合成 mix / 价格指数 levels): 源 → 点位 Levels → project (日并集轴) → Line ----
// 源点位: 点 i 在日 date[i] 的槽 slot[i], 点位 lv[i]; 首点 = 首日首根前 kStartDx 格的起点 (前收)
struct Levels {
  std::vector<misc::Ymd> date;
  std::vector<double> slot, lv;
  void push(misc::Ymd d, double s, double v) { date.push_back(d), slot.push_back(s), lv.push_back(v); }
};

// 腿 (fold_book 已实现口径, 与池指数对齐): 行 t 的增量 (cost: 毛 − 冲击 − 固定份数 · fixed_impact; 否则 = 毛; 账无成本列 → 恒 = 毛) /
// 增仓量 (账无增仓列 → 0)
struct BookLine {
  const float *c[kBookCols];
  explicit BookLine(const LinesLeg &lg) {
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

// 腿集合成 (线源 s 的 legs): 稠密格 [日并集轴][M] (M = 各腿层的行末槽之并; 行末分钟经 row_slot, 竞价段并入首根), 腿的行 t 落进 (所在日, 行末槽) 的格;
//   腿的日在轴范围外 → 弃, 范围内而轴上没有 → 整日并进下一个轴上的日的该腿首格 (同 project: 收益并进下一个轴上的日);
//   同格同腿复利, 跨腿按份额 1 / 当日有数据的腿数加和 ⇒ 一腿 = 该账线自身逐行复利
// 点位 = 首个有增量的格所在日首根前的起点 1 + 有增量的格逐格复利; 年换手 = 增仓量同份额累加到日, 自首个有增仓的日起在有腿的日上的日均 × 每年交易日
struct Mix {
  Levels lv;
  double turn = kNaN; // 倍/年; 无增仓 = NaN
};
Mix mix(const std::vector<misc::Ymd> &axis, const LineSrc &s) {
  const std::vector<LinesLeg> &legs = s.legs;
  const size_t D = axis.size();
  assert(D > 0);
  // 腿层 g 的行 ts → 槽
  const auto slot_of = [](const factor::Geo &g, int ts) { return static_cast<size_t>(row_slot(static_cast<size_t>(g.minute(ts)))); };
  std::array<int, kDayW> col; // [槽] 格列 (−1 = 无腿在此收行)
  col.fill(-1);
  for (const LinesLeg &lg : legs) {
    const factor::Geo g = factor_level(lg.b->lv).geo();
    for (int ts = 0; ts < g.S; ++ts)
      col[slot_of(g, ts)] = 0;
  }
  std::vector<int> slots; // [格列] 槽
  for (size_t m = 0; m < kDayW; ++m)
    if (col[m] == 0)
      col[m] = static_cast<int>(slots.size()), slots.push_back(static_cast<int>(m));
  const size_t M = slots.size(), X = D * M;
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
        const size_t t = i * S + ts, c = d * M + static_cast<size_t>(col[slot_of(g, on ? static_cast<int>(ts) : 0)]);
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
    compound(r.data(), xs.size(), [&](size_t i, double v) { o.lv.push(axis[xs[i] / M], static_cast<double>(slots[xs[i] % M]), v); });
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
// 点位 s 之后接价格历史 k 在日 after 之后的各根: 点位 × 收 / 前收 (前收链; 个股 = 后复权, 篮子点位本已接续 ⇒ 即收盘)
void chain(Levels &s, const Quote::Kline &k, misc::Ymd after) {
  assert(!s.lv.empty());
  const auto it = std::ranges::upper_bound(k.daily, after, {}, &Quote::Candle::date);
  for (auto c = it; c != k.daily.end(); ++c) {
    assert(c->ref > 0 && "前收链: 接续的日根须有前收");
    s.push(c->date, kClose, s.lv.back() * c->close / c->ref);
  }
}
// 价格历史: 起点 = 首根前收 (无 → 首根收盘) + 首根收盘, 其后接前收链
Levels levels(const Quote::Kline &k) {
  Levels s;
  if (k.daily.empty())
    return s;
  const Quote::Candle &c0 = k.daily.front();
  s.push(c0.date, -kStartDx, c0.ref > 0 ? c0.ref : c0.close);
  s.push(c0.date, kClose, c0.close);
  chain(s, k, c0.date);
  return s;
}

// 点位 s → 日并集轴 axis 上的线 (见 Line): 只取 axis 范围内的点; 首个取到的点不是起点 → 补其前一点为起点 (所在日首根前);
// 不在轴上的日只差点位, 收益自然并进下一个轴上的日. 同时建日末视图
Line project(const std::vector<misc::Ymd> &axis, const Levels &s) {
  Line c;
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
    if (c.x.empty() && s.slot[i] >= 0) {
      assert(i > 0);
      push(di[i], -kStartDx, i - 1);
    }
    if (axis[di[i]] == s.date[i])
      push(di[i], s.slot[i], i);
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

// 线 c (日末视图已建) 的点换成日内形 m: 各日以当日末点为锚, 锚 = 末点所在槽及以前的末个有根槽, 该日各有根槽 (至锚) 的点位 = 末点 × 收 / 锚处收;
// m 无该日 / 锚前无根 → 该日照留原点. 起点 (前收) 与各日末点不动 ⇒ 日末视图不变, 切片的 base 仍落在点上
void intraday(const std::vector<misc::Ymd> &axis, const Quote::Minutes &m, Line &c) {
  constexpr size_t F = Quote::Minutes::kFields, C = Quote::Minutes::kClose;
  const auto close = [&](const float *p, int s) { return p[static_cast<size_t>(s) * F + C]; };
  if (c.x.size() < 2)
    return;
  std::vector<double> x{c.x[0]}, y{c.y[0]};
  for (size_t j = 1, e = 1; j < c.x.size(); j = e) {
    const int k = c.day(j);
    while (e < c.x.size() && c.day(e) == k)
      ++e;
    const float *p = m.on(axis[static_cast<size_t>(k)]);
    int a = p ? static_cast<int>(c.x[e - 1] - x_of(static_cast<size_t>(k), 0.0)) : -1;
    while (a >= 0 && !(close(p, a) > 0))
      --a;
    if (a < 0) {
      x.insert(x.end(), c.x.begin() + static_cast<std::ptrdiff_t>(j), c.x.begin() + static_cast<std::ptrdiff_t>(e));
      y.insert(y.end(), c.y.begin() + static_cast<std::ptrdiff_t>(j), c.y.begin() + static_cast<std::ptrdiff_t>(e));
      continue;
    }
    const double ly = c.y[e - 1];
    for (int s = 0; s <= a; ++s)
      if (close(p, s) > 0)
        x.push_back(x_of(static_cast<size_t>(k), static_cast<double>(s))),
            y.push_back(ly * (static_cast<double>(close(p, s)) / static_cast<double>(close(p, a))));
  }
  c.x = std::move(x), c.y = std::move(y);
}

// 线源 s 的历史段 → 日并集轴 axis 上的线: 价格 = 点位直投; 腿集 = mix 合成 (带年换手), 有价格则接在其后再投;
// 带日内形 → 各日换成分钟 (intraday); 无源 / 日轴空 → 空线
Line build_line(const std::vector<misc::Ymd> &axis, const LineSrc &s) {
  if (axis.empty() || (!s.price && s.legs.empty()))
    return {};
  Line c;
  if (s.legs.empty()) {
    c = project(axis, levels(*s.price));
  } else {
    Mix m = mix(axis, s);
    if (s.price && !m.lv.lv.empty())
      chain(m.lv, *s.price, m.lv.date.back());
    c = project(axis, m.lv);
    c.turn = m.turn;
  }
  if (s.minutes)
    intraday(axis, *s.minutes, c);
  return c;
}
// 线 c 接当日 y 于日轴第 k 日 (y 接得上日脊 ⇒ c 止于 k 之前) 的尾: 锚 = 末点, 点位 = 锚 × 收 / 前收; minutes = 各有根槽一点 (同日内形),
// 否则此刻 (末个有根槽; 无根 = 末根) 一点. 空线 / 无前收 → 无尾
LineTail tail_of(const Line &c, size_t k, const Quote::Day &y, bool minutes) {
  LineTail o;
  if (c.e.empty() || !(y.candle.ref > 0))
    return o;
  assert(c.l < static_cast<int>(k));
  o.k = static_cast<int>(k);
  const double a = c.y.back() / y.candle.ref;
  const Quote::Bars &b = y.bars;
  int last = static_cast<int>(kClose);
  while (last >= 0 && !b.has[static_cast<size_t>(last)])
    --last;
  if (minutes && last >= 0) {
    for (int s = 0; s <= last; ++s)
      if (b.has[static_cast<size_t>(s)])
        o.x.push_back(x_of(k, s)), o.y.push_back(a * b.cell[static_cast<size_t>(s)].close);
  } else {
    o.x.push_back(x_of(k, last >= 0 ? last : static_cast<double>(kClose))), o.y.push_back(a * y.candle.close);
  }
  o.r = c.e.back() > 0 ? o.y.back() / c.e.back() - 1.0 : 0.0;
  return o;
}
// 腿集合成的线源: cost = 扣成本; 腿账都无成本列 → 不扣 (扣与不扣同一线源 ⇒ 两条线共用一份)
LineSrc mix_src(std::vector<LinesLeg> legs, bool cost, double fixed_impact) {
  cost = cost && std::ranges::any_of(legs, [](const LinesLeg &lg) { return lg.b->col[kBookImp].n > 0; });
  return {std::move(legs), cost, cost ? fixed_impact : 0.0};
}
// 账 b 线键 key 的腿; 账空 / 不在 → 空腿
LinesLeg leg_of(const std::shared_ptr<const PortfolioBook> &b, uint32_t key) {
  if (!b)
    return {};
  const size_t line = b->line_of(key);
  return line < b->lines() ? LinesLeg{b.get(), line} : LinesLeg{};
}
// 一腿的腿集 (空腿 → 空集)
std::vector<LinesLeg> legs_of(const LinesLeg &lg) { return lg.b ? std::vector<LinesLeg>{lg} : std::vector<LinesLeg>{}; }

// ---- 行 (两种构造互为对仗: 有行情键 push_quote ↔ 无行情键 push_book): 行名等每次现拼 (各组一个取名), 线源每次现定;
//      线按线源沿用上一份, 否则现算 ----
// 上一份派生的行 (按 id) + 日轴是否不变
struct Reuse {
  bool axis;
  std::map<std::string, const LinesRow *> rows;
  Reuse(const LinesDerived &o, const std::vector<misc::Ymd> &dates) : axis(dates == o.dates) {
    for (const std::vector<LinesRow> &g : o.rows)
      for (const LinesRow &r : g)
        rows[r.id] = &r;
  }
  // 日轴不变且上一份同 id 行有线源同 s 的线 → 它; 否则空
  std::shared_ptr<const Line> take(const std::string &id, const LineSrc &s) const {
    const auto it = rows.find(id);
    if (!axis || it == rows.end())
      return nullptr;
    for (int l = 0; l < kPairLines; ++l)
      if (it->second->src[l] == s)
        return it->second->line[l];
    return nullptr;
  }
};

// 行 r 进组 g (线源已定): 各线沿用上一份 (同行前一线源同的留给 build_lines 共用); 登记 index
void push(LinesDerived &d, int g, LinesRow r, const Reuse &ru) {
  for (int l = 0; l < kPairLines; ++l)
    if (l == 0 || !(r.src[l] == r.src[l - 1]))
      r.line[l] = ru.take(r.id, r.src[l]);
  d.index[r.id] = {g, d.rows[g].size()};
  d.rows[g].push_back(std::move(r));
}

// 建线的池 (只在派生里用; 派生只在界面线程 ⇒ 不重入)
misc::exec::ForkJoin &line_pool() {
  static misc::exec::ForkJoin p(misc::exec::hw());
  return p;
}
// 各行没沿用上的线并行现算 (各线互不相干), 再让同行线源同前一线的共用
void build_lines(LinesDerived &d) {
  std::vector<std::pair<LinesRow *, int>> todo;
  for (std::vector<LinesRow> &rs : d.rows)
    for (LinesRow &r : rs)
      for (int l = 0; l < kPairLines; ++l)
        if (!r.line[l] && (l == 0 || !(r.src[l] == r.src[l - 1])))
          todo.emplace_back(&r, l);
  line_pool().tasks(static_cast<int>(todo.size()), [&](int i, int) {
    const auto [r, l] = todo[static_cast<size_t>(i)];
    r->line[l] = std::make_shared<const Line>(build_line(d.dates, r->src[l]));
  });
  for (std::vector<LinesRow> &rs : d.rows)
    for (LinesRow &r : rs)
      for (int l = 1; l < kPairLines; ++l)
        if (r.src[l] == r.src[l - 1])
          r.line[l] = r.line[l - 1];
}

// 算成的因子 [items 序] 的条目; 其账进 d.books (同序), 计数与日并集轴 (各账日的并) 写进 d
std::vector<const PortfolioItem *> ready_of(const std::vector<PortfolioItem> &items, const PortfolioResult &res, LinesDerived &d) {
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

// 行情两侧的账 (同有同无, 同日轴; 没算成 → 空) + 价格库的发布 + 当日 → d;
// 日轴并入账日与日脊 (两侧同一条), 当日 (接得上日脊才给) 再并其日; 日轴 → 两侧账的行 (val_at)
void bind_quotes(const PortfolioResult &res, const std::shared_ptr<const Quote::PriceView> &view, misc::Ymd live, LinesDerived &d) {
  const PortfolioBook *s = res.quotes[Quote::kStock].get(), *b = res.quotes[Quote::kBasket].get();
  assert(!s == !b && (!b || (s->error == b->error && s->dates == b->dates)));
  if (b && b->error.empty())
    d.quotes = res.quotes;
  d.view = view;
  d.live = live;
  const auto join = [&](const std::vector<misc::Ymd> &ds) {
    std::vector<misc::Ymd> u;
    std::ranges::set_union(d.dates, ds, std::back_inserter(u));
    d.dates = std::move(u);
  };
  if (d.quotes[Quote::kBasket])
    join(b->dates);
  if (d.baskets())
    join(*d.view->dates);
  if (live)
    join({live});
  d.val_at.assign(d.dates.size(), -1);
  if (d.quotes[Quote::kBasket]) {
    const std::vector<misc::Ymd> &hd = b->dates;
    const std::vector<size_t> di = days_on(hd, d.dates);
    for (size_t k = 0; k < d.dates.size(); ++k)
      if (di[k] < hd.size() && hd[di[k]] == d.dates[k])
        d.val_at[k] = static_cast<int32_t>(di[k]);
  }
}

// ---- 各组的取名 (名 + 行情键; 见头文件): 篮子线 ↔ 个股, 因子条目 ↔ 组合 ----
LinesRow basket_row(size_t b) {
  const BasketTable &bt = basket_table();
  const std::string suf = pool::weight_suffix(bt.weight(b));
  LinesRow r;
  r.id = basket_id(b);
  r.en = bt.code(b);
  r.hover = r.en + "  " + bt.name(b);
  if (bt.group(b) == kGroupPool)
    r.pool = bt.pool_of(b);
  r.cn = r.tag = clip_name(bt.stem(b)) + suf;
  r.ref = {Quote::kBasket, static_cast<uint32_t>(b)};
  r.watch = r.en + suf;
  return r;
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
// 个股 id 的行; share = 在展开的篮子里的权占比 (NaN = 不在)
LinesRow stock_row(const StockInfos &infos, uint32_t id, double share) {
  LinesRow r;
  r.id = stock_id(id);
  r.en = asset_axis().code(id);
  r.tag = stock_name(infos, id);
  r.cn = clip_name(r.tag);
  r.hover = stock_hover(infos, id, share);
  r.hue = stock_hue(id);
  r.ref = {Quote::kStock, id};
  r.watch = r.en;
  return r;
}
// 因子条目 it (账 b) 的行
LinesRow factor_row(const PortfolioItem &it, const PortfolioBook &b) {
  LinesRow r;
  r.id = it.e.key();
  r.en = std::filesystem::path(it.e.file).stem().string();
  r.cn = clip_name(it.name_cn);
  r.tag = it.name_en + " " + factor::stat::hold_name(it.e.hold);
  r.hover = r.en + "  " + it.name_cn + "\n• " + it.e.pool + " " + factor_level(it.e.lv).name + "\n• " + Engine::hold_desc(it.e.hold, it.e.lv) +
            "\n• 冲击 " + amt_name(b.amt) + "\n• 规范串 " + it.e.expr + (it.e.sign < 0 ? " · 取反" : "");
  return r;
}
// 组合 name 的行
LinesRow strat_row(const std::string &name) {
  LinesRow r;
  r.id = "pf_strat";
  r.en = name, r.cn = "G20 多头";
  r.tag = r.hover = name;
  return r;
}

// 有行情键的行 r (已取名) 进组 g: 腿 / 估值腿 = 该侧账的键线; 可成交 = 腿扣成本合成 (两侧账都无成本列 ⇒ 不扣);
// 理想 = 该侧的价格线: 篮子 = 价格指数的该线 (未就绪 → 空线) / 个股 = 腿不扣成本合成 (个股账即后复权收盘) 再接个股历史 (价格库留着的才有; 无则与可成交同一条);
//   两条都带该键的日内形 (有则)
void push_quote(LinesDerived &d, int g, LinesRow r, double fixed_impact, const Reuse &ru) {
  r.val = leg_of(d.quotes[r.ref.side], r.ref.key);
  r.src[kTrade] = mix_src(legs_of(r.val), true, fixed_impact);
  if (r.ref.is(Quote::kStock))
    r.src[kIdeal] = mix_src(legs_of(r.val), false, 0.0);
  if (d.view) {
    const Quote::Tape *t = d.view->tape[r.ref.side].get();
    r.src[kIdeal].price = t ? t->line(r.ref.key) : nullptr;
    for (LineSrc &s : r.src)
      s.minutes = d.view->minute(r.ref).get();
  }
  push(d, g, std::move(r), ru);
}
// 无行情键的行 r (已取名) 进组 g: 可成交 / 理想 = 腿集 legs 扣 / 不扣成本合成
void push_book(LinesDerived &d, int g, LinesRow r, std::vector<LinesLeg> legs, double fixed_impact, const Reuse &ru) {
  r.src[kTrade] = mix_src(legs, true, fixed_impact);
  r.src[kIdeal] = mix_src(std::move(legs), false, 0.0);
  push(d, g, std::move(r), ru);
}

// 有行情键的行: 篮子三组 = 篮子表三段 (段内序); 成分 = 展开的篮子行的成分 (Tape::cons, 权降序) + 其余个股 kids (升序)
void derive_quotes(const std::string &open, const std::vector<uint32_t> &kids, const StockInfos &infos, double fixed_impact, const Reuse &ru,
                   LinesDerived &d) {
  for (int g = 0; g < kBasketGroups; ++g)
    for (const size_t b : basket_table().of(static_cast<BasketGroup>(g)))
      push_quote(d, g, basket_row(b), fixed_impact, ru);
  std::vector<std::pair<uint32_t, double>> ks; // (全局 id, 权占比)
  if (const LinesAt o = d.at(open); o.g >= 0 && d.baskets()) {
    assert(o.g < kBasketGroups);
    const Quote::Tape &bt = *d.baskets();
    const uint32_t line = d.row(o).ref.key;
    for (const Quote::Constituent &c : bt.cons[line])
      ks.emplace_back(c.id, c.w / bt.weight[line]);
  }
  d.n_cons = ks.size();
  std::vector<uint32_t> cons(ks.size());
  std::ranges::transform(ks, cons.begin(), [](const auto &k) { return k.first; });
  std::ranges::sort(cons);
  for (const uint32_t id : kids)
    if (!std::ranges::binary_search(cons, id))
      ks.emplace_back(id, kNaN);
  for (const auto &[id, share] : ks)
    push_quote(d, kGroupKid, stock_row(infos, id, share), fixed_impact, ru);
}
// 无行情键的行: 因子 = [算成的条目序] 各自账的线 (键 0); 策略 = 全部因子的线
void derive_books(const std::vector<const PortfolioItem *> &its, const std::string &name, double fixed_impact, const Reuse &ru, LinesDerived &d) {
  std::vector<LinesLeg> all;
  for (size_t q = 0; q < its.size(); ++q) {
    const LinesLeg lg = leg_of(d.books[q], 0);
    all.push_back(lg);
    push_book(d, kGroupFactor, factor_row(*its[q], *d.books[q]), legs_of(lg), fixed_impact, ru);
  }
  push_book(d, kGroupStrat, strat_row(name), std::move(all), fixed_impact, ru);
}

// 自选键里的个股 (全局 id; 篮子键 / 不在资产轴的码不收)
std::vector<uint32_t> watch_stocks(const std::vector<std::string> &watch) {
  std::vector<uint32_t> o;
  for (const std::string &w : watch)
    if (!w.ends_with(pool::weight_suffix(pool::kWeightEq)) && !w.ends_with(pool::weight_suffix(pool::kWeightCap)))
      if (const size_t id = asset_axis().find(w); id < asset_axis().size())
        o.push_back(static_cast<uint32_t>(id));
  return o;
}
// 自选行 (见头文件): 可自选组 (kGroupDefs::watch) 同自选键行的副本, 不登记 index (选中 / 展开按 id 落回原行)
void derive_watch(const std::vector<std::string> &watch, LinesDerived &d) {
  std::map<std::string, LinesAt> at;
  for (int g = 0; g < kRowGroups; ++g)
    if (kGroupDefs[g].watch)
      for (size_t r = 0; r < d.rows[g].size(); ++r)
        at.emplace(d.rows[g][r].watch, LinesAt{g, r});
  for (const std::string &w : watch)
    if (const auto it = at.find(w); it != at.end())
      d.rows[kGroupWatch].push_back(d.row(it->second));
    else
      d.watch_lost.push_back(w);
}

// 估值行 (位置 + 估值腿; 连同日轴 = 滚动分位 / 末点分位的全部来源)
std::vector<std::pair<LinesAt, LinesLeg>> val_rows(const LinesDerived &d) {
  std::vector<std::pair<LinesAt, LinesLeg>> o;
  for (int g = 0; g < kRowGroups; ++g)
    for (size_t r = 0; r < d.rows[g].size(); ++r)
      if (d.rows[g][r].val.b)
        o.emplace_back(LinesAt{g, r}, d.rows[g][r].val);
  return o;
}

// 线 c (+ 尾) 的点里首个 x ≥ v 的下标
size_t lower(const LineRef &c, double v) {
  const size_t n = c.c->x.size(), i = static_cast<size_t>(std::ranges::lower_bound(c.c->x, v) - c.c->x.begin());
  return i < n || !c.tail() ? i : n + static_cast<size_t>(std::ranges::lower_bound(c.t->x, v) - c.t->x.begin());
}
// 线 c (+ 尾) 在日段 [d0, d1) 的切片 (见 LinesSlice); 整条在区间外 → 空. 年化 / 夏普只读日末视图: 有效日段 [a, b) = [d0, d1) ∩ [f, l + 1),
// 年化 = end(b) / end(a) 按 b − a 个交易日复利折年 (%); 夏普 = 同段日收益的 mean / sd (ddof = 1) × √每年交易日
LinesSlice slice_of(const LineRef &c, int d0, int d1) {
  LinesSlice s;
  const double x0 = x_of(static_cast<size_t>(d0), kOpen), x1 = x_of(static_cast<size_t>(d1), kOpen);
  s.c = c, s.x0 = x0;
  const size_t i0 = lower(c, x0), i1 = lower(c, x1);
  if (i0 == c.size() || i1 == 0)
    return s;
  s.j0 = i0 > 0 ? i0 - 1 : 0, s.j1 = i1;
  const int a = std::max(d0, c.c->f), b = std::min(d1, c.l() + 1);
  s.base = c.end(a);
  assert(s.base == c.y(s.j0));
  if (b <= a)
    return s;
  const double n = b - a;
  s.ann = static_cast<float>(100.0 * factor::stat::ann_geo(c.end(b) / s.base, n));
  const double s1 = c.s1(b) - c.s1(a), s2 = c.s2(b) - c.s2(a);
  const double var = n >= 2.0 ? (s2 - s1 * s1 / n) / (n - 1.0) : 0.0;
  if (var > 0.0)
    s.sharpe = static_cast<float>(s1 / n / std::sqrt(var) * std::sqrt(static_cast<double>(factor::stat::kDaysPerYear)));
  return s;
}

// ---- 估值口径 (一个定义 pct_of; roll_of 逐日在有序窗上取前缀 / last_pct 末日直扫窗 pct_at, 同按日并集轴日) ----
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
  ValSeries(const ValAxis &a, const LinesLeg &lg, int m, int years_) : ax(a), v(lg.b->val_of(lg.line, m)), years(years_) {
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
// 日 t 的估值分位 (last_pct 的核; roll_of 在有序窗上取同一值): 窗 [lo(t), t] 内可算点里贵度 ≤ 当日的权重和 / 权重和 (%, 高 = 贵); 当日不可算 / 史不满 1 年 → NaN
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
// 出点日扫一遍窗得贵度序的累计权重 cw: 各档分位 = cw 首个 ≥ q · 总权重处的点, 当日估值分位 = 贵度 ≤ 当日处的 cw / 总权重 (pct_of); 等值线档按全史
void roll_of(const ValSeries &s, const std::vector<int32_t> &at, RollW &rw, LinesRoll &o) {
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  const size_t n = s.size(), D = at.size();
  const float *v = s.v;
  o.levels = levels_of(v, n);
  const auto by_dear = [&](size_t x, size_t y) { return dear_of(v[x]) < dear_of(v[y]); };
  std::vector<size_t> win; // 窗内可算点 (源下标), 贵度升序
  std::vector<double> cw;  // [窗内序] 累计权重
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
    cw.resize(win.size());
    double W = 0.0;
    for (size_t i = 0; i < win.size(); ++i)
      cw[i] = W += w_at(s, wt, win[i], tk);
    for (int j = 0; j < kBandQ; ++j) {
      const size_t i = static_cast<size_t>(std::lower_bound(cw.begin(), cw.end(), kBandQs[j] * W) - cw.begin());
      assert(i < win.size());
      o.q[j][k] = v[win[i]];
    }
    o.band[k] = wp == static_cast<int>(win.size());
    if (s.ok(tk)) {
      const float m = dear_of(v[tk]);
      const size_t le = static_cast<size_t>(std::upper_bound(win.begin(), win.end(), m, [&](float x, size_t u) { return x < dear_of(v[u]); }) - win.begin());
      assert(le > 0 && "当日在窗内");
      o.pct[k] = pct_of(cw[le - 1], W);
    }
  }
}

// 估值 s 在轴日 [0, k1) 内末个可算日及其估值分位 (pct_at)
LinesLastPct last_pct(const ValSeries &s, const std::vector<int32_t> &at, int k1, RollW &rw) {
  for (int k = k1; k-- > 0;)
    if (at[static_cast<size_t>(k)] >= 0)
      if (const float p = pct_at(s, static_cast<size_t>(at[static_cast<size_t>(k)]), rw); !std::isnan(p))
        return {k, p};
  return {};
}

// 截面名次 (NAV 数值列 / VAL 格同一份): 组内 n 格 at(i) 的 x → rank (GUI::rank_by)
template <class At>
void rank_col(size_t n, int good, const At &at) {
  rank_by(n, good, [&](size_t i) { return at(i).x; }, [&](size_t i, double u) { at(i).rank = u; });
}

} // namespace

Quote::Ref RowRef(const std::string &id) {
  if (id.starts_with(kStockId))
    return {Quote::kStock, static_cast<uint32_t>(std::stoul(id.substr(kStockId.size())))};
  for (size_t b = 0; b < basket_table().size(); ++b)
    if (basket_id(b) == id)
      return {Quote::kBasket, static_cast<uint32_t>(b)};
  return {};
}

void MakeLinesDerived(const std::vector<PortfolioItem> &items, const std::string &name, const PortfolioResult &res,
                      const std::shared_ptr<const Quote::PriceView> &view, misc::Ymd live, double fixed_impact, const std::string &open,
                      const std::vector<uint32_t> &stocks, const std::vector<std::string> &watch, const StockInfos &infos, LinesDerived &d) {
  LinesDerived o = std::move(d);
  d = LinesDerived{};
  const std::vector<const PortfolioItem *> its = ready_of(items, res, d);
  bind_quotes(res, view, live, d);
  const Reuse ru(o, d.dates);
  std::vector<uint32_t> kids = watch_stocks(watch);
  kids.insert(kids.end(), stocks.begin(), stocks.end());
  std::ranges::sort(kids);
  kids.erase(std::unique(kids.begin(), kids.end()), kids.end());
  derive_quotes(open, kids, infos, fixed_impact, ru, d);
  derive_books(its, name, fixed_impact, ru, d);
  build_lines(d);
  paint(d, o);
  derive_watch(watch, d);
  const bool axis = d.dates != o.dates || d.quotes != o.quotes;
  const auto sizes = [](const LinesDerived &x) {
    std::array<size_t, kRowGroups> n{};
    for (int g = 0; g < kRowGroups; ++g)
      n[static_cast<size_t>(g)] = x.rows[g].size();
    return n;
  };
  const auto lines = [](const LinesDerived &x) {
    std::vector<const Line *> v;
    for (const std::vector<LinesRow> &rs : x.rows)
      for (const LinesRow &r : rs)
        for (const std::shared_ptr<const Line> &c : r.line)
          v.push_back(c.get());
    return v;
  };
  d.hist_gen = o.hist_gen + (lines(d) != lines(o));
  d.axis_gen = o.axis_gen + axis;
  d.val_gen = o.val_gen + (axis || sizes(d) != sizes(o) || val_rows(d) != val_rows(o));
}

void MakeLinesLive(const LinesDerived &d, const Quote::Live *y, LinesLive &t) {
  for (int g = 0; g < kRowGroups; ++g)
    t.tail[g].assign(d.rows[g].size(), {});
  if (!y || !d.live)
    return;
  assert(y->date == d.live);
  const auto k = std::ranges::lower_bound(d.dates, d.live);
  assert(k != d.dates.end() && *k == d.live && "当日已并进日轴");
  const size_t di = static_cast<size_t>(k - d.dates.begin());
  for (int g = 0; g < kRowGroups; ++g)
    for (size_t r = 0; r < d.rows[g].size(); ++r) {
      const LinesRow &w = d.rows[g][r];
      if (!w.ref.ok() || !w.src[kIdeal].price)
        continue;
      if (const Quote::Day *day = y->at(w.ref))
        t.tail[g][r] = tail_of(*w.line[kIdeal], di, *day, w.src[kIdeal].minutes != nullptr);
    }
}

void MakeLinesRoll(const LinesDerived &d, const LinesLeg &lg, int years, LinesRolls &r) {
  r = {};
  if (!lg.b)
    return;
  const ValAxis ax(*lg.b);
  RollW rw;
  for (int b = 0; b < kPoolVals; ++b)
    roll_of(ValSeries(ax, lg, b, years), d.val_at, rw, r[b]);
}

void MakeLinesPct(const LinesDerived &d, int d1, int years, LinesPct &p) {
  for (int g = 0; g < kRowGroups; ++g)
    p.rows[g].assign(d.rows[g].size(), {});
  if (!d.quotes[Quote::kBasket])
    return;
  const ValAxis ax(*d.quotes[Quote::kBasket]);
  RollW rw;
  for (const auto &[at, lg] : val_rows(d))
    for (int b = 0; b < kPoolVals; ++b)
      p.rows[at.g][at.r][b] = last_pct(ValSeries(ax, lg, b, years), d.val_at, d1, rw);
}

void MakeLinesView(const LinesDerived &d, const LinesLive &t, int d0, int d1, const LinesAt &pin, const LinesRolls &roll, const LinesPct &pct,
                   LinesView &v) {
  // ---- 各组同一套: 各线切片 + NAV 数值列 + VAL 各口区间内末个可算日的估值分位 (= 区间末之前的末个可算日落在区间内, 含区间起点的前收日);
  //      各列同组同列名次 (VAL: 分位低 = 便宜 = 好) ----
  for (int g = 0; g < kRowGroups; ++g) {
    const size_t n = d.rows[g].size();
    assert(pct.rows[g].size() == n && t.tail[g].size() == n);
    v.slice[g].clear();
    v.num[g].assign(n, {});
    v.pct[g].assign(n, {});
    for (size_t i = 0; i < n; ++i) {
      const LinesRow &r = d.rows[g][i];
      v.slice[g].push_back({slice_of({r.line[kTrade].get(), nullptr}, d0, d1), slice_of({r.line[kIdeal].get(), &t.tail[g][i]}, d0, d1)});
      const std::array<LinesSlice, kPairLines> &s = v.slice[g].back();
      std::array<Ranked, kNavNums> &c = v.num[g][i];
      c[kNumTrade].x = s[kTrade].ann, c[kNumIdeal].x = s[kIdeal].ann;
      c[kNumSharpe].x = s[kTrade].sharpe, c[kNumTurn].x = r.line[kTrade]->turn;
      for (int b = 0; b < kPoolVals; ++b)
        if (const LinesLastPct &lp = pct.rows[g][i][b]; lp.k >= 0 && lp.k >= d0 - 1)
          v.pct[g][i][b].x = lp.pct;
    }
    for (int k = 0; k < kNavNums; ++k)
      rank_col(n, kNavGood[k], [&](size_t i) -> Ranked & { return v.num[g][i][k]; });
    for (int b = 0; b < kPoolVals; ++b)
      rank_col(n, -1, [&](size_t i) -> Ranked & { return v.pct[g][i][b]; });
  }

  // ---- VAL 标的各口 band: 标的理想线切片的日末点 (起点 + 各日末点), y = P · v / q (v = 当日收益率; v ≤ 0 / q ≤ 0 → 价格无解, 断开; 窗内有任一无解点 → 不画);
  //      等值线单位点位 unit = P · v (只看当日, 不看滚动分位; v ≤ 0 → 断开), 档 = 二级的全史倍数档 ----
  v.band_x.clear(), v.band_p.clear();
  for (LinesBand &bd : v.band)
    bd = {};
  if (pin.g < 0 || !d.row(pin).val.b)
    return;
  const LinesLeg &lg = d.row(pin).val;
  const LinesSlice &sl = v.slice[pin.g][pin.r][kIdeal];
  std::vector<int> day; // [日末点] 所属日 (日并集轴; −1 = 轴外)
  for (size_t i = 0; i < sl.size(); ++i)
    if (const int k = sl.day(i); i + 1 == sl.size() || sl.day(i + 1) != k)
      v.band_x.push_back(sl.x(i)), v.band_p.push_back(sl.y(i)), day.push_back(k);
  const size_t n = day.size();
  for (int b = 0; b < kPoolVals; ++b) {
    const LinesRoll &r = roll[b];
    assert(r.pct.size() == d.dates.size());
    LinesBand &bd = v.band[b];
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
        bd.unit[i] = v.band_p[i] * x;
      if (t < 0 || !std::isfinite(r.pct[static_cast<size_t>(k)]))
        continue;
      last = k;
      if (!(x > 0.0) || !r.band[static_cast<size_t>(k)])
        continue;
      for (int q = 0; q < kBandQ; ++q)
        if (const float qk = r.q[q][static_cast<size_t>(k)]; qk > 0.f)
          bd.y[q][i] = v.band_p[i] * x / qk;
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

} // namespace GUI::Market