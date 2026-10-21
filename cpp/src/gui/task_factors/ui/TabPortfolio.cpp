// Factors→Portfolio — 见头文件
#include "gui/task_factors/ui/TabPortfolio.hpp"

#include "features/Method/Fundamental.hpp"    // fund::SW2021_L1_NAMES (VAL 行业)
#include "features/TimeIndex.hpp"             // TRADE_MINUTES_PER_DAY (横轴每日跨度)
#include "gui/Tasks.hpp"                      // StatusColor
#include "gui/task_factors/ui/TabFactors.hpp" // tip
#include "shared/FeatureLib.hpp"              // feature_pools

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h" // GetItem (主图例显隐 / 池线色)

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <ranges>
#include <set>

namespace GUI::Factors {

namespace {

// 横轴格 x = 日下标 · kDayW + 日内分钟 (同 OrderFlow 图 2); 横轴编码只在这里
constexpr size_t kDayW = TRADE_MINUTES_PER_DAY;
constexpr size_t kOpen = 0, kClose = kDayW - 1; // 日内分钟: 开盘 / 收盘
constexpr size_t x_at(size_t di, size_t minute) { return di * kDayW + minute; }
double x_of(size_t di, size_t minute) { return static_cast<double>(x_at(di, minute)); }
const ImVec4 kHead(0.4f, 0.8f, 1.0f, 1.0f);

void load_current(PortfolioUIState &ui) {
  ui.entries = ui.name.empty() ? std::vector<PortfolioEntry>{} : LoadPortfolio(ui.dir, ui.name);
  ui.items.clear();
  ++ui.items_gen;
  ui.sel = -1;
  ++ui.content_gen;
}
// 重列目录, 选 want (不在 → 第一个 / 无) 并载入
void relist(PortfolioUIState &ui, const std::string &want) {
  ui.names = ListPortfolios(ui.dir);
  ui.name = std::find(ui.names.begin(), ui.names.end(), want) != ui.names.end() ? want : ui.names.empty() ? std::string{}
                                                                                                          : ui.names.front();
  load_current(ui);
}
void save_current(PortfolioUIState &ui) {
  assert(!ui.name.empty());
  SavePortfolio(ui.dir, ui.name, ui.entries);
  ui.items.clear();
  ++ui.items_gen;
  ++ui.content_gen;
}

std::string entry_label(const PortfolioEntry &e) {
  return "(" + e.pool + ", " + factor::stat::hold_name(e.hold) + "): " + std::filesystem::path(e.file).stem().string();
}

// 横轴刻度 (主图 / 区间条): x = 日下标 · kDayW + 分钟 → YY/MM/DD, 只到日 (user_data = 日并集轴)
int day_formatter(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<misc::Ymd> *>(ud);
  if (dates.empty() || v < 0)
    return std::snprintf(buf, size, " ");
  const misc::Ymd y = dates[std::min(static_cast<size_t>(v) / kDayW, dates.size() - 1)];
  return std::snprintf(buf, size, "%02d/%02d/%02d", y / 10000 % 100, y / 100 % 100, y % 100);
}

// 组合策略线表 [kImpacts]: 透明度 / 线宽 / 名 / ID 按冲击 (名 = 图例第二列; 线尾小字 = 组合名 + 后缀)
const ImVec4 kStratCol(1.0f, 0.55f, 0.15f, 1.0f);
constexpr float kImpactAlpha[kImpacts] = {1.0f, 0.5f};
constexpr float kImpactWidth[kImpacts] = {2.0f, 1.0f};
constexpr const char *kImpactName[kImpacts] = {"有冲击", "无冲击"};
constexpr const char *kImpactId[kImpacts] = {"", "0"};
constexpr const char *kImpactTag[kImpacts] = {"", " 无冲击"};
// 池指数线表 [kWeights] (all 池两条; 其余池只画 kWeightEq, 名用池中文名)
constexpr const char *kWeightName[kWeights] = {"全A等权", "全A加权"};
constexpr const char *kWeightId[kWeights] = {"", "_cap"};
// VAL 估值口表 [PoolVal]: 名 / 价格倍数 = 1 / 值 / 显示 (百分比 : 倍) / 小数位
struct ValMeta {
  const char *name;
  bool inv, pct;
  int prec;
};
constexpr ValMeta kVal[kPoolVals] = {{"PE", false, false, 1}, {"PB", false, false, 2}, {"PS", false, false, 2}, {"PCF", false, false, 1}, {"DY", true, true, 2}};
// VAL 线表: 价格线色; band 线尾小字的分位名 [kBandQ] (与 kBandQs 同序); band 填色 / 线 / 小字 / 右表格底色的 alpha
const ImVec4 kValPxCol(0.95f, 0.95f, 0.95f, 1.0f);
constexpr const char *kBandQName[kBandQ] = {"5%", "10%", "25%", "50%", "75%", "90%", "95%"};
constexpr float kBandFillA = 0.12f, kBandLineA = 0.35f, kBandTagA = 0.9f, kValCellA = 0.45f;
// band 色: u ∈ [0, 1] (0 = 最便宜 绿 → 1 = 最贵 红) 按色相渐变
ImVec4 band_col(double u, float a) {
  ImVec4 c(0.f, 0.f, 0.f, a);
  ImGui::ColorConvertHSVtoRGB(0.33f * static_cast<float>(1.0 - u), 0.8f, 0.9f, c.x, c.y, c.z);
  return c;
}
// 估值值的显示 (按 kVal: 倍 / 百分比), 不可算 → "-"
void fmt_val(int b, double x, char *buf, size_t n) {
  const ValMeta &m = kVal[b];
  if (std::isfinite(x))
    std::snprintf(buf, n, "%.*f%s", m.prec, m.pct ? 100.0 * x : x, m.pct ? "%" : "x");
  else
    std::snprintf(buf, n, "-");
}

// 右补空格到 px 宽 (等宽字体, 中文占两格: 按实测像素补, 列对齐)
std::string pad_to(const char *s, float px) {
  const float w = ImGui::CalcTextSize(s).x, sp = ImGui::CalcTextSize(" ").x;
  return s + std::string(static_cast<size_t>(std::lround(std::max(0.f, px - w) / sp)), ' ');
}

// 单因子线色: 接在池线 (自动配色, 先建先取) 之后取色表
ImVec4 factor_color(const PortfolioDerived &d, size_t i) { return ImPlot::GetColormapColor(static_cast<int>(d.pools.size() + i)); }

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
// x 严格升序; d[Impact] = 该时刻实现的净值增量 (占本金)
struct Inc {
  std::vector<double> x, d[kImpacts];
  // 追加 at 处的增量 r[kImpacts] (at 不降; 同 x 并入末点)
  void add(double at, const double *r) {
    assert(x.empty() || at >= x.back());
    if (x.empty() || at != x.back()) {
      x.push_back(at);
      for (std::vector<double> &v : d)
        v.push_back(0.0);
    }
    for (int i = 0; i < kImpacts; ++i)
      d[i].back() += r[i];
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
    double r[kImpacts];
    for (int m = 0; m < kImpacts; ++m)
      r[m] = s.d[m][k];
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
// 增量流的冲击口径 i → 净值线: 起点 (x0, 1) + 每个 x 一点, 逐点复利
PortfolioCurve compound(const Inc &s, int i, double x0) {
  assert(s.x.empty() || s.x.front() > x0);
  PortfolioCurve c;
  c.x.reserve(s.x.size() + 1), c.y.reserve(s.x.size() + 1);
  c.x.push_back(x0), c.y.push_back(1.0);
  double nav = 1.0;
  for (size_t k = 0; k < s.x.size(); ++k) {
    nav *= 1.0 + s.d[i][k];
    c.x.push_back(s.x[k]), c.y.push_back(nav);
  }
  return c;
}
// 因子 f 的增量流 (di = f 各日在并集轴上的下标): 行增量 = (行收益 − 行成本) / h · wt(建仓日并集轴下标), 记在平仓时刻 (与池指数同为已实现口径):
//   分钟型 = 行末分钟 + n (过收盘 → 收盘); 收盘型 = 当日收盘; T+N = 其后第 N 个交易日开盘 (越过区间末 → 末日收盘, 标签按最后收盘结算)
template <class Wt>
Inc inc_of(const FactorCurve &f, const std::vector<size_t> &di, double fixed_impact, const Wt &wt) {
  const int S = f.seg;
  const factor::Geo g = factor::geo_of(S);
  const int h = factor::stat::hold_rows(f.hold, g);
  assert(!f.dates.empty() && f.ret.size() == f.dates.size() * static_cast<size_t>(S));
  // 每个新进者: 平仓 fixed_impact + 建仓 (Lob = 金额段冲击, 已在 wimp; Bar 无段 = 再一份 fixed_impact)
  const double ke = f.amt > 0 ? fixed_impact : 2.0 * fixed_impact;
  const auto exit_x = [&](size_t dl, int ts) -> size_t {
    if (factor::stat::hold_intraday(f.hold))
      return x_at(di[dl], std::min(static_cast<size_t>(g.minute(ts) + f.hold), kClose));
    if (f.hold == factor::stat::kHoldClose)
      return x_at(di[dl], kClose);
    const size_t de = dl + static_cast<size_t>(f.hold - factor::stat::kHoldDayBase);
    return de < di.size() ? x_at(di[de], kOpen) : x_at(di.back(), kClose);
  };
  Inc o;
  for (size_t dl = 0; dl < di.size(); ++dl) {
    const double k = wt(di[dl]) / h;
    for (int ts = 0; ts < S; ++ts) {
      const size_t t = dl * S + static_cast<size_t>(ts);
      double r[kImpacts];
      r[kImpactNet] = (f.ret[t] - f.wimp[t] - f.we[t] * ke) * k;
      r[kImpactGross] = f.ret[t] * k;
      o.add(static_cast<double>(exit_x(dl, ts)), r);
    }
  }
  return o;
}

// ---- 篮子投影 (池线 / VAL 同一份, 见 PortfolioBasket): 只取日并集轴范围内; 不在轴上的日只差点位, 收益自然并进下一个轴上的日 ----
void project(const std::vector<misc::Ymd> &axis, PortfolioBasket &b) {
  const std::vector<misc::Ymd> &src = b.pc->dates;
  const float *v = b.pc->index_of(b.k, b.w);
  const std::vector<size_t> di = days_on(axis, src);
  const auto push = [&](double x, size_t i) { b.c.x.push_back(x), b.c.y.push_back(v[i]), b.at.push_back(static_cast<uint32_t>(i)); };
  for (size_t i = 0; i < src.size(); ++i) {
    if (src[i] < axis.front() || src[i] > axis.back())
      continue;
    if (b.at.empty() && i > 0)
      push(x_of(di[i], kOpen), i - 1);
    if (axis[di[i]] == src[i])
      push(x_of(di[i], kClose), i);
  }
}

// 池 name 算成的篮子 (缺 / 失败 → 空)
std::shared_ptr<const PoolCurve> pool_of(const PortfolioResult &res, const char *name) {
  const auto it = res.pools.find(name);
  return it != res.pools.end() && it->second->error.empty() ? it->second : nullptr;
}

// 组合 + 单因子 (见头文件): 每因子两条增量流 — 自身 (权 1) → 单因子线; 份额 (权 1 / 当日因子数) → Σ → 组合线. 单因子线 ≡ 只含它的组合
void derive_factors(const std::vector<PortfolioItem> &items, const std::string &name, const PortfolioResult &res, double fixed_impact,
                    PortfolioDerived &d) {
  std::vector<const PortfolioItem *> its;
  std::vector<const FactorCurve *> fc;
  for (const PortfolioItem &it : items) {
    if (!it.missing.empty())
      continue;
    const auto f = res.factors.find(it.e.key());
    if (f == res.factors.end())
      continue;
    if (!f->second->error.empty()) {
      ++d.n_error;
      continue;
    }
    ++d.n_ready;
    its.push_back(&it);
    fc.push_back(f->second.get());
  }
  for (const FactorCurve *f : fc)
    d.dates.insert(d.dates.end(), f->dates.begin(), f->dates.end());
  std::sort(d.dates.begin(), d.dates.end());
  d.dates.erase(std::unique(d.dates.begin(), d.dates.end()), d.dates.end());
  std::vector<std::vector<size_t>> dix(fc.size()); // [因子][该因子日] → 并集轴下标
  for (size_t q = 0; q < fc.size(); ++q)
    dix[q] = days_on(d.dates, fc[q]->dates);
  std::vector<int> n_act(d.dates.size(), 0); // [并集轴] 当日有数据的因子数
  for (const std::vector<size_t> &di : dix)
    for (const size_t i : di)
      ++n_act[i];

  std::vector<Inc> shares; // [因子] 份额流
  for (size_t q = 0; q < fc.size(); ++q) {
    const FactorCurve &f = *fc[q];
    const std::vector<size_t> &di = dix[q];
    PortfolioFactor pf;
    pf.id = its[q]->e.key();
    pf.en = std::filesystem::path(its[q]->e.file).stem().string();
    pf.note = f.amt > 0 ? std::to_string(f.amt) + "w" : std::string("固定");
    pf.tag = its[q]->name_en + " " + factor::stat::hold_name(f.hold);
    pf.c = compound(inc_of(f, di, fixed_impact, [](size_t) { return 1.0; }), kImpactNet, x_of(di.front(), kOpen));
    pf.item = *its[q];
    d.factors.push_back(std::move(pf));
    shares.push_back(inc_of(f, di, fixed_impact, [&](size_t i) { return 1.0 / n_act[i]; }));
  }
  const Inc sum = shares.empty() ? Inc{} : sum_of(std::move(shares));
  for (int i = 0; i < kImpacts; ++i) {
    PortfolioLine &s = d.strat[i];
    s.id = std::string("pf_strat") + kImpactId[i];
    s.en = name, s.cn = kImpactName[i];
    s.tag = name + kImpactTag[i];
    s.on = i == kImpactNet; // 默认只显有冲击
    if (!d.dates.empty())
      s.c = compound(sum, i, x_of(0, kOpen));
  }
}

// 池线 + VAL 篮子 (见头文件): 同一 PortfolioBasket + project
void derive_baskets(const std::vector<PortfolioItem> &items, const PortfolioResult &res, PortfolioDerived &d) {
  const auto basket = [&](std::string id, std::string en, std::string cn, std::string tag, std::shared_ptr<const PoolCurve> pc, size_t k, Weight w) {
    PortfolioBasket b;
    b.id = std::move(id), b.en = std::move(en), b.cn = std::move(cn), b.tag = std::move(tag);
    b.pc = std::move(pc), b.k = k, b.w = w;
    if (b.pc && !d.dates.empty())
      project(d.dates, b);
    return b;
  };

  // ---- 池线 (篮子 0): all 池两条 = 全A加权 (kWeightCap) / 全A等权 (kWeightEq), 其余池一条等权; note = 年换手 (日均 × kDaysPerYear) ----
  std::set<std::string> own; // 组合里有因子的池
  for (const PortfolioItem &it : items)
    if (it.missing.empty())
      own.insert(it.e.pool);
  for (const PoolDef &pd : feature_pools()) {
    const bool all = pd.pool_col == SIZE_MAX;
    const std::shared_ptr<const PoolCurve> pc = pool_of(res, pd.name);
    for (const Weight w : {kWeightCap, kWeightEq}) {
      if (!all && w != kWeightEq)
        continue;
      PortfolioBasket p = basket(std::string("pf_pool_") + pd.name + kWeightId[w], pd.name, all ? kWeightName[w] : pd.name_cn,
                                 all ? kWeightName[w] : pd.name, pc, 0, w);
      p.on = own.contains(pd.name);
      if (pc) {
        char turn[48];
        std::snprintf(turn, sizeof(turn), "  (年换手=%5.1f倍)", pc->turn[w] * factor::stat::kDaysPerYear);
        p.note = turn;
      }
      d.pools.push_back(std::move(p));
    }
  }

  // ---- VAL 篮子: 各池整体 (篮子 0) / 申万一级行业 (all 池篮子 k ≥ 1), 流通市值加权; 全史估值留在 PoolCurve ----
  if (d.dates.empty())
    return;
  std::shared_ptr<const PoolCurve> all;
  for (const PoolDef &pd : feature_pools()) {
    const std::shared_ptr<const PoolCurve> pc = pool_of(res, pd.name);
    if (!pc)
      continue;
    assert(pc->index[kWeightCap].size() == fund::SW2021_L1_COUNT * pc->dates.size());
    if (pd.pool_col == SIZE_MAX)
      all = pc;
    d.vals.push_back(basket(pd.name, pd.name, pd.pool_col == SIZE_MAX ? kWeightName[kWeightCap] : pd.name_cn, pd.name, pc, 0, kWeightCap));
  }
  d.n_val_pools = d.vals.size();
  if (all)
    for (size_t k = 1; k < fund::SW2021_L1_COUNT; ++k) {
      char id[8];
      std::snprintf(id, sizeof(id), "sw%02zu", k);
      const std::string nm(fund::SW2021_L1_NAMES[k]);
      d.vals.push_back(basket(id, id, nm, nm, all, k, kWeightCap));
    }
}

void derive(const std::vector<PortfolioItem> &items, const std::string &name, const PortfolioResult &res, double fixed_impact, PortfolioDerived &d) {
  d = PortfolioDerived{};
  derive_factors(items, name, res, fixed_impact, d);
  derive_baskets(items, res, d);
}

// 曲线 c 在 [x0, x1) 的切片 (见 PortfolioSlice); 整条在区间外 → 空. 年化 = 末点净值按首末点所跨交易日数复利折年 (%)
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
  s.ann = static_cast<float>(100.0 * (std::pow(s.y(e), static_cast<double>(factor::stat::kDaysPerYear) / days) - 1.0));
  return s;
}
// 同一组线逐条切片
template <class L>
void slice_all(const std::vector<L> &ls, double x0, double x1, std::vector<PortfolioSlice> &o) {
  o.clear();
  for (const L &l : ls)
    o.push_back(slice_of(l.c, x0, x1));
}

// ---- 估值口径 (roll_of / last_pct 同一份) ----
// 估值 (PE / PB / PS / PCF / DY) 可算: 有限 ∧ > 0
bool val_ok(double r) { return std::isfinite(r) && r > 0.0; }
// 估值 ↔ 价格倍数 (kVal.inv → 倒数; 自逆, 倍数升 = 贵)
template <class F>
F mult_of(int b, F x) {
  return kVal[b].inv ? F(1) / x : x;
}
// 自然年平移: 滚动窗 = (years_before(日, N), 日]; 史满 1 年 = 首个可算日 ≤ years_before(日, 1)
constexpr misc::Ymd years_before(misc::Ymd day, int years) { return day - years * 10000; }
// 估值分位 = 窗内倍数 ≤ 当日的个数 le / 窗内可算个数 w (%, 高 = 贵)
float pct_of(int le, int w) { return 100.f * static_cast<float>(le) / static_cast<float>(w); }

// 篮子 s 的口 b 的滚动分位 → o [s.c.x]: 价格倍数全史排序定秩 (同值同秩), 秩上 Fenwick 计窗内个数;
// 沿全史逐日入 1 点 / 出窗游标; 分位 = 窗内第 round(q · (n − 1)) + 1 小 (Fenwick 倍增下降). 每日 O(kBandQ · log), 一口全史 ~ 亚毫秒
void roll_of(const PortfolioBasket &s, int b, int years, PortfolioRoll &o) {
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  const std::vector<misc::Ymd> &hd = s.pc->dates;
  const size_t n = hd.size(), np = s.at.size();
  const float *v = s.pc->val_of(s.k, b);
  std::vector<float> srt;
  for (size_t i = 0; i < n; ++i)
    if (val_ok(v[i]))
      srt.push_back(mult_of(b, v[i]));
  std::sort(srt.begin(), srt.end());
  std::vector<int> rk(n, -1); // 秩 (−1 = 不可算)
  for (size_t i = 0; i < n; ++i)
    if (val_ok(v[i]))
      rk[i] = static_cast<int>(std::lower_bound(srt.begin(), srt.end(), mult_of(b, v[i])) - srt.begin());
  const int S = static_cast<int>(srt.size());
  std::vector<int> bit(static_cast<size_t>(S) + 1, 0);
  const auto add = [&](int r, int dv) {
    for (int i = r + 1; i <= S; i += i & -i)
      bit[static_cast<size_t>(i)] += dv;
  };
  const auto upto = [&](int r) { // 秩 ≤ r 的个数
    int c = 0;
    for (int i = r + 1; i > 0; i -= i & -i)
      c += bit[static_cast<size_t>(i)];
    return c;
  };
  int top = 1;
  while (top * 2 <= S)
    top *= 2;
  const auto kth = [&](int k) { // 第 k (1 起) 小的秩
    int p = 0;
    for (int st = top; st > 0; st >>= 1)
      if (p + st <= S && bit[static_cast<size_t>(p + st)] < k)
        p += st, k -= bit[static_cast<size_t>(p)];
    return p;
  };
  for (std::vector<float> &q : o.q)
    q.assign(np, kNaN);
  o.pct.assign(np, kNaN);
  misc::Ymd first = 0; // 首个可算日
  size_t lo = 0, k = 0;
  int w = 0;
  for (size_t t = 0; t < n && k < np; ++t) {
    if (rk[t] >= 0) {
      add(rk[t], 1), ++w;
      if (first == 0)
        first = hd[t];
    }
    for (; hd[lo] <= years_before(hd[t], years); ++lo)
      if (rk[lo] >= 0)
        add(rk[lo], -1), --w;
    if (s.at[k] != t)
      continue;
    const size_t i = k++;
    if (w == 0 || first == 0 || first > years_before(hd[t], 1))
      continue;
    for (int j = 0; j < kBandQ; ++j)
      o.q[j][i] = srt[static_cast<size_t>(kth(static_cast<int>(std::lround(kBandQs[j] * (w - 1))) + 1))];
    if (rk[t] >= 0)
      o.pct[i] = pct_of(upto(rk[t]), w);
  }
  assert(k == np && "at 须升序且落在 hd 内");
}

// 篮子 s 的口 b 在切片 sl 内末个可算点的估值分位 (NaN = 无): 与 roll_of 的 pct 同一定义, 只一点 → 直接数窗, O(窗)
double last_pct(const PortfolioBasket &s, const PortfolioSlice &sl, int b, int years) {
  const std::vector<misc::Ymd> &hd = s.pc->dates;
  const float *v = s.pc->val_of(s.k, b);
  size_t f = 0; // 首个可算日 (无 → hd.size(), 下面 val_ok 先短路)
  while (f < hd.size() && !val_ok(v[f]))
    ++f;
  for (size_t j = sl.j1; j > sl.j0; --j) {
    const size_t t = s.at[j - 1];
    if (!val_ok(v[t]) || hd[f] > years_before(hd[t], 1))
      continue;
    const float m = mult_of(b, v[t]);
    int w = 0, le = 0;
    for (size_t u = static_cast<size_t>(std::upper_bound(hd.begin(), hd.begin() + t, years_before(hd[t], years)) - hd.begin()); u <= t; ++u)
      if (val_ok(v[u]))
        ++w, le += mult_of(b, v[u]) <= m;
    return pct_of(le, w);
  }
  return std::numeric_limits<double>::quiet_NaN();
}

// 区间视图 (见 PortfolioView). vs = VAL 选中篮子 (der.vals 下标, −1 = 无), roll = 它的滚动分位, years = 窗长
void make_view(const PortfolioDerived &d, int d0, int d1, int vs, int years, const std::array<PortfolioRoll, kPoolVals> &roll, PortfolioView &v) {
  const double x0 = x_of(static_cast<size_t>(d0), kOpen), x1 = x_of(static_cast<size_t>(d1), kOpen);
  // ---- 各线同一份切片 ----
  for (int i = 0; i < kImpacts; ++i)
    v.strat[i] = slice_of(d.strat[i].c, x0, x1);
  slice_all(d.factors, x0, x1, v.factors);
  slice_all(d.pools, x0, x1, v.pools);
  slice_all(d.vals, x0, x1, v.vals);

  // ---- VAL 表格: 各篮子各口切片内末个可算点的估值分位 + 同组 (池 / 行业) 同口内的截面名次 (并列取中) ----
  v.cell.assign(d.vals.size(), {});
  for (size_t i = 0; i < d.vals.size(); ++i)
    for (int b = 0; b < kPoolVals; ++b)
      v.cell[i][b].pct = last_pct(d.vals[i], v.vals[i], b, years);
  for (int b = 0; b < kPoolVals; ++b)
    for (const auto &[g0, g1] : {std::pair{size_t{0}, d.n_val_pools}, std::pair{d.n_val_pools, d.vals.size()}})
      for (size_t i = g0; i < g1; ++i) {
        const double p = v.cell[i][b].pct;
        if (!std::isfinite(p))
          continue;
        int n = 0, lt = 0, eq = 0;
        for (size_t j = g0; j < g1; ++j)
          if (const double q = v.cell[j][b].pct; std::isfinite(q))
            ++n, lt += q < p, eq += q == p;
        v.cell[i][b].rank = n > 1 ? (lt + 0.5 * (eq - 1)) / (n - 1) : 0.5;
      }

  // ---- VAL 选中篮子各口 band: 与价格线同一切片, y = P · q / m ----
  v.band_x.clear();
  for (PortfolioBand &bd : v.band)
    bd = {};
  if (vs < 0)
    return;
  const PortfolioBasket &s = d.vals[static_cast<size_t>(vs)];
  const PortfolioSlice &sl = v.vals[static_cast<size_t>(vs)];
  const size_t n = sl.size();
  for (size_t i = 0; i < n; ++i)
    v.band_x.push_back(sl.x(i));
  for (int b = 0; b < kPoolVals; ++b) {
    const PortfolioRoll &r = roll[b];
    assert(r.pct.size() == s.at.size());
    PortfolioBand &bd = v.band[b];
    const float *hv = s.pc->val_of(s.k, b);
    for (std::vector<double> &y : bd.y)
      y.assign(n, std::numeric_limits<double>::quiet_NaN());
    size_t last = SIZE_MAX; // 末个可画点 (源下标)
    for (size_t i = 0; i < n; ++i) {
      const size_t j = sl.j0 + i;
      const double x = hv[s.at[j]];
      if (!val_ok(x) || !std::isfinite(r.q[0][j]))
        continue;
      const double m = mult_of(b, x);
      for (int q = 0; q < kBandQ; ++q)
        bd.y[q][i] = sl.y(i) * r.q[q][j] / m;
      const int k = static_cast<int>(i);
      if (bd.segs.empty() || bd.segs.back().second != k)
        bd.segs.push_back({k, k + 1});
      else
        ++bd.segs.back().second;
      last = j;
    }
    if (last == SIZE_MAX)
      continue;
    bd.ok = true;
    bd.now = hv[s.at[last]], bd.pct = r.pct[last];
    for (int q = 0; q < kBandQ; ++q)
      bd.q[q] = mult_of(b, static_cast<double>(r.q[q][last]));
  }
}

// 线尾小字: 视野内末点的像素位置
struct EndTag {
  ImVec2 at;
  ImU32 col;
  std::string text;
};
// n 点线 (pt(i) = 第 i 点, x 升序)
template <class Pt>
void push_tag(std::vector<EndTag> &tags, const ImPlotRect &lim, size_t n, const Pt &pt, ImU32 col, std::string text) {
  const auto r = std::views::iota(size_t{0}, n);
  const size_t e = static_cast<size_t>(std::ranges::partition_point(r, [&](size_t i) { return pt(i).x <= lim.X.Max; }) - r.begin());
  if (e == 0)
    return;
  const ImPlotPoint p = pt(e - 1);
  if (p.x < lim.X.Min || !std::isfinite(p.y))
    return;
  tags.push_back({ImPlot::PlotToPixels(p), col, std::move(text)});
}

// ---- 切片画线 (零拷贝: ImPlot 按下标回调取点) ----
ImPlotPoint slice_pt(const PortfolioSlice &s, size_t i) { return {s.x(i), s.y(i)}; }
ImPlotPoint slice_getter(int i, void *s) { return slice_pt(*static_cast<const PortfolioSlice *>(s), static_cast<size_t>(i)); }
void plot_slice(const char *label, const PortfolioSlice &s) {
  ImPlot::PlotLineG(label, slice_getter, const_cast<PortfolioSlice *>(&s), static_cast<int>(s.size()));
}
void push_tag(std::vector<EndTag> &tags, const ImPlotRect &lim, const PortfolioSlice &s, ImU32 col, std::string text) {
  push_tag(tags, lim, s.size(), [&](size_t i) { return slice_pt(s, i); }, col, std::move(text));
}
constexpr float kTagScale = 0.95f, kTagGap = 4.0f; // 小字字号 (× 字体) / 离末点的间距 (px)
// 小字占的最宽像素 (含间距 + 右边余量)
float tags_width(const std::vector<EndTag> &tags) {
  ImFont *font = ImGui::GetFont();
  const float fs = ImGui::GetFontSize() * kTagScale;
  float w = 0.0f;
  for (const EndTag &t : tags)
    w = std::max(w, font->CalcTextSizeA(fs, FLT_MAX, 0.0f, t.text.c_str()).x);
  return w + 2.0f * kTagGap;
}
// 按像素 y 排开不重叠, 贴点右侧 (出界则左移进图内)
void draw_tags(std::vector<EndTag> &tags) {
  std::sort(tags.begin(), tags.end(), [](const EndTag &a, const EndTag &b) { return a.at.y < b.at.y; });
  ImFont *font = ImGui::GetFont();
  const float fs = ImGui::GetFontSize() * kTagScale;
  for (size_t i = 1; i < tags.size(); ++i)
    tags[i].at.y = std::max(tags[i].at.y, tags[i - 1].at.y + fs);
  const float right = ImPlot::GetPlotPos().x + ImPlot::GetPlotSize().x;
  ImDrawList *dl = ImPlot::GetPlotDrawList();
  ImPlot::PushPlotClipRect();
  for (const EndTag &t : tags) {
    const ImVec2 sz = font->CalcTextSizeA(fs, FLT_MAX, 0.0f, t.text.c_str());
    dl->AddText(font, fs, ImVec2(std::min(t.at.x + kTagGap, right - sz.x - 2.0f), t.at.y - 0.5f * sz.y), t.col, t.text.c_str());
  }
  ImPlot::PopPlotClipRect();
}

// 主图一帧的公共件 (NAV / VAL 同一份): 小字筛选视野 + 线尾小字 + 显隐签名 (与上帧不同 → 下帧重新 fit)
struct PlotFrame {
  ImPlotRect lim;
  std::vector<EndTag> tags;
  std::string shown;
  // 图例项 label 的显隐记进 shown; 显示 → 该项 (Color = 实际线色), 否则 nullptr
  const ImPlotItem *item(const char *label) {
    const ImPlotItem *it = ImPlot::GetItem(label);
    assert(it);
    shown += it->Show ? '1' : '0';
    return it->Show ? it : nullptr;
  }
};

// ---- 头: 组合 / 成本 / Recompute / Cancel / 状态 + 失效 / 失败条目原因. 返回 1 = Recompute, -1 = Cancel, 0 = 无 ----
int head(PortfolioService &svc, PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioResult &res, const std::string &message,
         double fixed_impact) {
  int action = 0;
  size_t n_bad = 0;
  for (const PortfolioItem &it : ui.items)
    n_bad += !it.missing.empty();
  ImGui::Text("%s  %zu 因子 (算成 %d", ui.name.c_str(), ui.entries.size(), d.n_ready);
  ImGui::SameLine(0, 0);
  if (d.n_error > 0) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), ", 失败 %d", d.n_error);
    ImGui::SameLine(0, 0);
  }
  if (n_bad > 0) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), ", 失效 %zu", n_bad);
    ImGui::SameLine(0, 0);
  }
  ImGui::Text(")");
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  ImGui::TextDisabled("有冲击");
  if (ImGui::IsItemHovered())
    tip("换手成本 = 各因子自己的最小冲击段, 不可选\n"
        "• 每行成本 = 多腿新进者 (t−h 不在该组) 的建仓 + 平仓冲击, 按持仓加权 (Stat 契约【换手成本】)\n"
        "• Lob 层: 建仓 = 最小金额段冲击 (封顶 max_impact), 平仓 = 固定冲击 %.1f bp\n"
        "• Bar 层 (无冲击段): 建仓 / 平仓都按固定冲击 %.1f bp\n"
        "• 组合只算 G20 多头, 扣多腿; 无冲击线不扣",
        fixed_impact * 1e4, fixed_impact * 1e4);
  ImGui::SameLine();
  ImGui::Checkbox("log", &ui.log_y);
  if (ImGui::IsItemHovered())
    tip("y 轴 (净值) 切对数刻度");
  ImGui::SameLine();
  const bool busy = svc.busy();
  ImGui::BeginDisabled(busy);
  if (ImGui::Button("Recompute", ImVec2(90, 0)))
    action = 1;
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("弃缓存重算全部因子 + 池指数 (特征库重算过之后用)\n"
        "• 平时不用: 组合加 / 删因子自动只算新的那个");
  ImGui::SameLine();
  ImGui::BeginDisabled(!busy);
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    action = -1;
  ImGui::EndDisabled();
  ImGui::SameLine();
  {
    const Phase ph = svc.phase.load(std::memory_order_acquire);
    const int jd = svc.jobs_done.load(std::memory_order_relaxed), jt = svc.jobs_total.load(std::memory_order_relaxed);
    const int done = static_cast<int>(svc.done.load(std::memory_order_relaxed)), total = static_cast<int>(svc.total.load(std::memory_order_relaxed));
    const ImVec4 bc = StatusColor(TaskStatus::Kind::Busy);
    switch (ph) {
    case Phase::Idle:
    case Phase::Scanning:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
      break;
    case Phase::Loading:
      ImGui::TextColored(bc, "[%d/%d] loading %d/%d days", jd + 1, jt, done, total);
      break;
    case Phase::Running:
      ImGui::TextColored(bc, "[%d/%d] running %d/%d", jd + 1, jt, done, total);
      break;
    case Phase::Done:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "done");
      break;
    case Phase::Cancelled:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelled %d/%d", jd, jt);
      break;
    }
  }
  if (!message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "| %s", message.c_str());
  }
  for (const PortfolioItem &it : ui.items) {
    const auto f = res.factors.find(it.e.key());
    const std::string &why = !it.missing.empty() ? it.missing : f != res.factors.end() ? f->second->error
                                                                                       : it.missing;
    if (!why.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "  %s  %s", entry_label(it.e).c_str(), why.c_str());
  }
  return action;
}

// ---- 页签: NAV = 资金曲线 / VAL = 池估值 (同一布局; 区间 / log 共用). 返回 true = NAV ----
bool pages() {
  bool nav = true;
  if (ImGui::BeginTabBar("##pf_page")) {
    const bool nav_open = ImGui::BeginTabItem("NAV");
    if (ImGui::IsItemHovered())
      tip("NAV = 资金曲线 (Net Asset Value, 净值)\n"
          "• 橙线 = 本组合 G20 多头: 各因子独立回测的逐行收益等权聚合, 扣换手冲击 (细线 = 无冲击)\n"
          "• 池指数线 = 各池等权指数 (all 池拆 全A等权 / 全A加权), 默认只显组合里因子所在的池\n"
          "• 右侧勾选 = 单因子各自的 G20 多头净值\n"
          "• 底部区间条选日段: 各线在区间起点归一 = 1, 年化按区间复利");
    if (nav_open)
      ImGui::EndTabItem();
    const bool val_open = ImGui::BeginTabItem("VAL");
    if (ImGui::IsItemHovered())
      tip("VAL = 估值 (Valuation)\n"
          "• 右侧单选一个池或申万一级行业: 主图 = 它的流通市值加权指数 + 估值 Band\n"
          "• PE 市盈率 / PB 市净率 / PS 市销率 / PCF 市现率 / DY 股息率 (整体法), 主图例逐个开关\n"
          "• Band = 把过去 N 年的估值分位换算成价格线: 指数落在哪档 = 当前估值在历史里的位置 (绿 = 便宜, 红 = 贵)\n"
          "• 区间 / log 与 NAV 页共用");
    if (val_open) {
      nav = false;
      ImGui::EndTabItem();
    }
    ImGui::EndTabBar();
  }
  return nav;
}

// ---- NAV 主图: 池线 → 单因子 → 策略线 (最上层). 主图例 (池 / 策略同格式) = en | cn (补空格同列宽) + 区间年化 + note, ###id 定 ID (年化变了不换 item) ----
void plot_nav(PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioView &v, bool set_vis, PlotFrame &pf) {
  float w_en = 0.f, w_cn = 0.f;
  const auto widen = [&](const PortfolioLine &l) {
    w_en = std::max(w_en, ImGui::CalcTextSize(l.en.c_str()).x), w_cn = std::max(w_cn, ImGui::CalcTextSize(l.cn.c_str()).x);
  };
  for (const PortfolioLine &l : d.strat)
    widen(l);
  for (const PortfolioBasket &l : d.pools)
    widen(l);
  const auto main_line = [&](const PortfolioLine &l, const PortfolioSlice &s) {
    char ann[48];
    std::snprintf(ann, sizeof(ann), "  (年化=%+5.1f%%)", s.ann);
    const std::string lb = pad_to(l.en.c_str(), w_en) + "  " + pad_to(l.cn.c_str(), w_cn) + ann + l.note + "###" + l.id;
    if (set_vis)
      ImPlot::HideNextItem(!l.on, ImPlotCond_Always);
    plot_slice(lb.c_str(), s);
    if (const ImPlotItem *it = pf.item(lb.c_str()))
      push_tag(pf.tags, pf.lim, s, it->Color, l.tag);
  };
  for (size_t i = 0; i < d.pools.size(); ++i)
    main_line(d.pools[i], v.pools[i]);
  for (size_t i = 0; i < d.factors.size(); ++i) {
    const PortfolioFactor &f = d.factors[i];
    const bool on = ui.factor_show[f.id];
    pf.shown += on ? '1' : '0';
    if (!on)
      continue;
    const ImVec4 col = factor_color(d, i);
    ImPlot::SetNextLineStyle(col, 1.0f);
    plot_slice(("##pf_f" + f.id).c_str(), v.factors[i]);
    push_tag(pf.tags, pf.lim, v.factors[i], ImGui::ColorConvertFloat4ToU32(col), f.tag);
  }
  for (int i = 0; i < kImpacts; ++i) {
    ImPlot::SetNextLineStyle(ImVec4(kStratCol.x, kStratCol.y, kStratCol.z, kImpactAlpha[i]), kImpactWidth[i]);
    main_line(d.strat[i], v.strat[i]);
  }
}

// ---- VAL 主图: 选中篮子各口滚动 band (填色 + kBandQ 条线共一个图例项, 一键开关; PE 以外首次默认关) + 价格线 (最上层) ----
void plot_val(const PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioView &v, int vs, PlotFrame &pf) {
  const PortfolioBasket &s = d.vals[static_cast<size_t>(vs)];
  const PortfolioSlice &sl = v.vals[static_cast<size_t>(vs)];
  const double *bx = v.band_x.data();
  const int bn = static_cast<int>(v.band_x.size());
  pf.shown += s.id;
  for (int b = 0; b < kPoolVals; ++b) {
    const PortfolioBand &bd = v.band[b];
    char lb[128];
    if (bd.ok) {
      char now[24];
      fmt_val(b, bd.now, now, sizeof(now));
      std::snprintf(lb, sizeof(lb), "%s Band  当前 %s  (%d年估值分位 %.0f%%)###pf_band%d", kVal[b].name, now, ui.roll.key.years, bd.pct, b);
    } else
      std::snprintf(lb, sizeof(lb), "%s Band  (区间内不可算)###pf_band%d", kVal[b].name, b);
    if (b != kValPe)
      ImPlot::HideNextItem(true, ImPlotCond_Once);
    // 色块 c ∈ [0, kBandQ]: 0 = 最便宜档以下 (到图底), kBandQ = 最贵档以上 (到图顶; 两头不进 fit), 其余 = 相邻两档之间
    for (const auto &[i0, i1] : bd.segs) {
      const int n = i1 - i0;
      const double *x = bx + i0;
      for (int c = 0; c <= kBandQ; ++c) {
        ImPlot::SetNextFillStyle(band_col(static_cast<double>(c) / kBandQ, 1.0f), kBandFillA);
        if (c == 0)
          ImPlot::PlotShaded(lb, x, bd.y[0].data() + i0, n, -HUGE_VAL, ImPlotItemFlags_NoFit);
        else if (c == kBandQ)
          ImPlot::PlotShaded(lb, x, bd.y[kBandQ - 1].data() + i0, n, HUGE_VAL, ImPlotItemFlags_NoFit);
        else
          ImPlot::PlotShaded(lb, x, bd.y[c - 1].data() + i0, bd.y[c].data() + i0, n);
      }
    }
    for (int j = 0; j < kBandQ; ++j) {
      ImPlot::SetNextLineStyle(band_col((j + 0.5) / kBandQ, kBandLineA), j == kBandQ / 2 ? 1.5f : 1.0f);
      ImPlot::PlotLine(lb, bx, bd.y[j].data(), bn, ImPlotLineFlags_SkipNaN);
    }
    if (pf.item(lb) && bd.ok)
      for (int j = 0; j < kBandQ; ++j) {
        char q[24];
        fmt_val(b, bd.q[j], q, sizeof(q));
        push_tag(pf.tags, pf.lim, v.band_x.size(), [&](size_t i) { return ImPlotPoint(bx[i], bd.y[j][i]); }, ImGui::ColorConvertFloat4ToU32(band_col((j + 0.5) / kBandQ, kBandTagA)), std::string(kBandQName[j]) + " " + q);
      }
  }
  const std::string plb = s.tag + "  流通市值加权指数###pf_val_px";
  ImPlot::SetNextLineStyle(kValPxCol, 2.0f);
  plot_slice(plb.c_str(), sl);
  if (const ImPlotItem *it = pf.item(plb.c_str()))
    push_tag(pf.tags, pf.lim, sl, it->Color, s.tag);
}

// ---- 区间条: 全段组合 (有冲击) / VAL 选中篮子价格; 选段 [d0, d1) 两边吸附到月初 (每月首个交易日; 右边 = 下月月初 / 末尾), 拖动中即吸附即生效 ----
//   左右边 = 竖把手 (拖改起止, 双击拉到头); 段内拖 = 平移 (月数不变); 悬停 / 拖动时把手旁标起止日
void range_bar(PortfolioUIState &ui, bool nav, int vs, float pw, float bar_h) {
  const PortfolioDerived &d = ui.der.val;
  PortfolioRange &rg = ui.range;
  const int D = static_cast<int>(d.dates.size());
  if (!ImPlot::BeginPlot("##pf_range", ImVec2(pw, bar_h),
                         ImPlotFlags_NoTitle | ImPlotFlags_NoLegend | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoMouseText))
    return;
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_None, ImPlotAxisFlags_NoDecorations | ImPlotAxisFlags_AutoFit);
  if (D > 0)
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, x_of(d.dates.size(), kOpen), ImPlotCond_Always);
  ImPlot::SetupAxisFormat(ImAxis_X1, day_formatter, &ui.der.val.dates);
  if (ui.log_y)
    ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
  static const PortfolioCurve kNone;
  const PortfolioCurve &rc = nav ? d.strat[kImpactNet].c : vs >= 0 ? d.vals[static_cast<size_t>(vs)].c
                                                                   : kNone;
  ImPlot::SetNextLineStyle(nav ? kStratCol : kValPxCol, 1.0f);
  ImPlot::PlotLine("##pf_range_line", rc.x.data(), rc.y.data(), static_cast<int>(rc.x.size()));
  if (D > 0) {
    std::vector<int> ms; // 吸附点 = 月初下标, 末尾补 D
    for (int i = 0; i < D; ++i)
      if (i == 0 || d.dates[i] / 100 != d.dates[i - 1] / 100)
        ms.push_back(i);
    ms.push_back(D);
    const int K = static_cast<int>(ms.size()) - 1;
    const auto near_k = [&](double day, int lo, int hi) { // ms[lo..hi] 里离 day 最近的那个的下标
      int best = lo;
      for (int k = lo + 1; k <= hi; ++k)
        if (std::abs(ms[k] - day) < std::abs(ms[best] - day))
          best = k;
      return best;
    };
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    const auto px_of = [&](int day) { return ImPlot::PlotToPixels(x_of(static_cast<size_t>(day), kOpen), lim.Y.Min).x; };
    const ImVec2 pp = ImPlot::GetPlotPos(), ps = ImPlot::GetPlotSize(), mp = ImGui::GetMousePos();
    const double mday = ImPlot::GetPlotMousePos().x / static_cast<double>(kDayW);
    constexpr float kGrab = 7.0f; // 把手抓取半宽 (px)
    int over = 0;                 // 鼠标下: 1 左把手 / 2 右把手 / 3 段内
    if (mp.x >= pp.x && mp.x <= pp.x + ps.x && mp.y >= pp.y && mp.y <= pp.y + ps.y &&
        ImGui::IsWindowHovered(ImGuiHoveredFlags_AllowWhenBlockedByActiveItem)) {
      const float x0 = px_of(rg.d0), x1 = px_of(rg.d1);
      over = std::abs(mp.x - x0) <= kGrab ? 1 : std::abs(mp.x - x1) <= kGrab ? 2
                                            : (mp.x > x0 && mp.x < x1)       ? 3
                                                                             : 0;
    }
    int k0 = near_k(rg.d0, 0, K), k1 = near_k(rg.d1, 0, K);
    if (k1 <= k0) // 保 0 ≤ k0 < k1 ≤ K (下面按边拖的取值范围要它)
      k1 = std::min(k0 + 1, K), k0 = k1 - 1;
    if (rg.drag == 0 && over != 0 && ImGui::IsMouseClicked(0))
      rg.drag = over, rg.grab_day = mday, rg.grab_k0 = k0, rg.grab_k1 = k1;
    if (rg.drag != 0 && !ImGui::IsMouseDown(0))
      rg.drag = 0;
    if (rg.drag == 1)
      k0 = near_k(mday, 0, k1 - 1);
    else if (rg.drag == 2)
      k1 = near_k(mday, k0 + 1, K);
    else if (rg.drag == 3) {
      const int w = rg.grab_k1 - rg.grab_k0;
      k0 = near_k(ms[rg.grab_k0] + (mday - rg.grab_day), 0, K - w), k1 = k0 + w;
    }
    if (over == 1 && ImGui::IsMouseDoubleClicked(0))
      k0 = 0;
    if (over == 2 && ImGui::IsMouseDoubleClicked(0))
      k1 = K;
    if (k1 <= k0)
      k1 = std::min(k0 + 1, K), k0 = k1 - 1;
    if (ms[k0] != rg.d0 || ms[k1] != rg.d1) {
      rg.d0 = ms[k0], rg.d1 = ms[k1];
      ui.refit = true;
    }
    const int act = rg.drag != 0 ? rg.drag : over;
    if (act == 1 || act == 2)
      ImGui::SetMouseCursor(ImGuiMouseCursor_ResizeEW);
    else if (act == 3)
      ImGui::SetMouseCursor(ImGuiMouseCursor_Hand);

    // 画: 段外压暗 + 段内淡色 + 两边竖把手 (带握纹, 悬停 / 拖动高亮)
    const float x0 = px_of(rg.d0), x1 = px_of(rg.d1), yb = pp.y + ps.y;
    ImDrawList &dl = *ImPlot::GetPlotDrawList();
    ImPlot::PushPlotClipRect();
    dl.AddRectFilled(pp, ImVec2(x0, yb), IM_COL32(0, 0, 0, 120));
    dl.AddRectFilled(ImVec2(x1, pp.y), ImVec2(pp.x + ps.x, yb), IM_COL32(0, 0, 0, 120));
    dl.AddRectFilled(ImVec2(x0, pp.y), ImVec2(x1, yb), IM_COL32(102, 204, 255, 28));
    const auto handle = [&](float x, bool hot) {
      const ImU32 c = hot ? IM_COL32(190, 240, 255, 255) : IM_COL32(102, 204, 255, 255);
      dl.AddLine(ImVec2(x, pp.y), ImVec2(x, yb), c, hot ? 3.0f : 2.0f);
      constexpr float hw = 5.0f;
      const float cx = std::clamp(x, pp.x + hw, pp.x + ps.x - hw), cy = pp.y + ps.y * 0.5f, hh = ps.y * 0.28f;
      dl.AddRectFilled(ImVec2(cx - hw, cy - hh), ImVec2(cx + hw, cy + hh), c, 3.0f);
      for (const float gx : {cx - 1.5f, cx + 1.5f})
        dl.AddLine(ImVec2(gx, cy - hh * 0.5f), ImVec2(gx, cy + hh * 0.5f), IM_COL32(20, 50, 70, 255), 1.0f);
    };
    handle(x0, act == 1 || act == 3);
    handle(x1, act == 2 || act == 3);
    if (act != 0) { // 起止日贴把手外侧 (放不下改内侧)
      char s0[16], s1[16];
      const misc::Ymd a = d.dates[rg.d0], b = d.dates[rg.d1 - 1];
      std::snprintf(s0, sizeof(s0), "%d/%02d/%02d", a / 10000, a / 100 % 100, a % 100);
      std::snprintf(s1, sizeof(s1), "%d/%02d/%02d", b / 10000, b / 100 % 100, b % 100);
      const ImVec2 t0 = ImGui::CalcTextSize(s0), t1 = ImGui::CalcTextSize(s1);
      float lx = x0 - 9.0f - t0.x, rx = x1 + 9.0f;
      if (lx < pp.x)
        lx = x0 + 9.0f;
      if (rx + t1.x > pp.x + ps.x)
        rx = x1 - 9.0f - t1.x;
      const auto label = [&](float tx, const char *s, const ImVec2 &ts) {
        dl.AddRectFilled(ImVec2(tx - 3.0f, pp.y + 2.0f), ImVec2(tx + ts.x + 3.0f, pp.y + 4.0f + ts.y), IM_COL32(10, 20, 30, 220), 3.0f);
        dl.AddText(ImVec2(tx, pp.y + 3.0f), IM_COL32(255, 255, 255, 255), s);
      };
      label(lx, s0, t0);
      label(rx, s1, t1);
    }
    ImPlot::PopPlotClipRect();
    if (act != 0)
      tip("区间 %d/%02d/%02d ~ %d/%02d/%02d (%d 日)\n"
          "• 拖左右把手改起止, 拖段内平移 (月数不变); 吸附到月初\n"
          "• 双击把手 = 拉到头",
          d.dates[rg.d0] / 10000, d.dates[rg.d0] / 100 % 100, d.dates[rg.d0] % 100, d.dates[rg.d1 - 1] / 10000, d.dates[rg.d1 - 1] / 100 % 100,
          d.dates[rg.d1 - 1] % 100, rg.d1 - rg.d0);
  }
  ImPlot::EndPlot();
}

// ---- NAV 右图例: 单因子 G20 多头表格 (默认全关), 勾选 = 主图画该线; 列 = 池 / 层 / 持有期 / 因子 / 冲击 / 区间年化 ----
void legend_nav(PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioView &v) {
  ImGui::TextColored(kHead, "单因子 G20 多头");
  if (ImGui::IsItemHovered())
    tip("勾选 = 主图画该因子单独的 G20 多头净值\n"
        "• 带它自己的冲击 (冲击列: Lob 最小金额段 / Bar 固定冲击)\n"
        "• 与组合同一区间起点归一; 年化按区间复利");
  if (!ImGui::BeginTable("##pf_nav_tbl", 7, ImGuiTableFlags_ScrollY | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerV))
    return;
  ImGui::TableSetupScrollFreeze(0, 1);
  ImGui::TableSetupColumn("##on");
  ImGui::TableSetupColumn("池");
  ImGui::TableSetupColumn("层");
  ImGui::TableSetupColumn("持有");
  ImGui::TableSetupColumn("因子", ImGuiTableColumnFlags_WidthStretch);
  ImGui::TableSetupColumn("冲击");
  ImGui::TableSetupColumn("年化");
  ImGui::TableHeadersRow();
  for (size_t i = 0; i < d.factors.size(); ++i) {
    const PortfolioFactor &f = d.factors[i];
    const PortfolioItem &it = f.item;
    const ImVec4 col = factor_color(d, i);
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    bool on = ui.factor_show[f.id];
    ImGui::PushStyleColor(ImGuiCol_CheckMark, col);
    if (ImGui::Checkbox(("##pff" + std::to_string(i)).c_str(), &on))
      ui.factor_show[f.id] = on;
    ImGui::PopStyleColor();
    ImGui::TableSetColumnIndex(1);
    ImGui::TextUnformatted(it.e.pool.c_str());
    ImGui::TableSetColumnIndex(2);
    ImGui::TextUnformatted(factor_level(it.e.seg).name);
    ImGui::TableSetColumnIndex(3);
    ImGui::TextUnformatted(factor::stat::hold_name(it.e.hold).c_str());
    ImGui::TableSetColumnIndex(4);
    ImGui::TextColored(col, "%s", f.en.c_str());
    if (ImGui::IsItemHovered())
      tip("%s  %s\n• 规范串 %s", f.en.c_str(), it.name_cn.c_str(), it.e.expr.c_str());
    ImGui::TableSetColumnIndex(5);
    ImGui::TextUnformatted(f.note.c_str());
    ImGui::TableSetColumnIndex(6);
    ImGui::Text("%+6.1f%%", v.factors[i].ann);
  }
  ImGui::EndTable();
}

// ---- VAL 右图例: 滚动窗长 + 篮子单选表格 (池 / 申万一级); 格 = 区间末各口的估值分位, 底色 = 同组同口内的截面名次 (便宜绿 → 贵红) ----
void legend_val(PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioView &v, int vs) {
  ImGui::SetNextItemWidth(-FLT_MIN);
  ImGui::SliderInt("##pf_band_years", &ui.band_years, 1, kBandYearsMax, "Band 滚动窗 %d 年", ImGuiSliderFlags_AlwaysClamp);
  if (ImGui::IsItemHovered())
    tip("Band 分位的滚动窗长 N: 每日取过去 N 年 (含当日) 的倍数分位, 拖动即时重算\n"
        "• 池估值史不足 N 年时先扩张窗; 不满 1 年不画");
  ImGui::TextColored(kHead, "池 / 行业 (单选)");
  if (ImGui::IsItemHovered())
    tip("选一个池或申万一级行业: 主图画它的价格 + PE / PB / PS / PCF / DY Band\n"
        "• 价格 = 流通市值加权指数 (all = 全A加权; 行业 = 全A 内该行业)\n"
        "• 行业成员 = 全A 成分按上一交易日的 SW2021 一级行业 (ind_l1) 切; 行业未知的只进全A\n"
        "• PE = Σ总市值 / Σ归母净利TTM, PB = Σ总市值 / Σ归母权益MRQ, PS = Σ总市值 / Σ营业总收入TTM,\n"
        "  PCF = Σ总市值 / Σ经营现金流TTM, DY = Σ近365日税前分红 / Σ总市值 (整体法, 亏损 / 负权益 / 负现金流照计; 合计 ≤ 0 不画)\n"
        "• 成分 = 上一交易日收盘定的池 (同池指数); 市值 = 收盘价 × 当日适用总股本, 财务取当日适用 (无前视)\n"
        "• Band = 价格 × q / 当日倍数 (倍数 = PE / PB / PS / PCF / 1÷DY), q = 过去 N 年 (含当日; 上方滑条) 倍数的 5/10/25/50/75/90/95%% 分位, 逐日滚动;\n"
        "  估值史不满 1 年不画. 最便宜档以下绿 → 最贵档以上红; 估值分位 = 当日倍数在窗内的分位 (高 = 贵)\n"
        "• 表格 = 区间末各口的估值分位 (DY 按 1÷DY 排, 高 = 贵); 绝对值看主图 Band 图例 / 线尾\n"
        "• 底色 = 该分位在同组 (池之间 / 行业之间) 同一口内的截面名次: 组内最便宜绿 → 最贵红");
  if (!ImGui::BeginTable("##pf_val_tbl", 2 + kPoolVals, ImGuiTableFlags_ScrollY | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerV))
    return;
  ImGui::TableSetupScrollFreeze(0, 1);
  ImGui::TableSetupColumn("池 / 行业");
  ImGui::TableSetupColumn("名", ImGuiTableColumnFlags_WidthStretch);
  for (const ValMeta &m : kVal)
    ImGui::TableSetupColumn(m.name, ImGuiTableColumnFlags_WidthFixed, ImGui::CalcTextSize("100%").x);
  ImGui::TableHeadersRow();
  for (size_t i = 0; i < d.vals.size(); ++i) {
    const PortfolioBasket &s = d.vals[i];
    if (i == 0 || i == d.n_val_pools) {
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::TextColored(kHead, i == 0 ? "池" : "申万一级 (全A)");
    }
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    if (ImGui::Selectable((s.en + "##pfv" + std::to_string(i)).c_str(), vs == static_cast<int>(i), ImGuiSelectableFlags_SpanAllColumns))
      ui.val_pool = s.id;
    ImGui::TableSetColumnIndex(1);
    ImGui::TextUnformatted(s.cn.c_str());
    for (int b = 0; b < kPoolVals; ++b) {
      ImGui::TableSetColumnIndex(2 + b);
      const PortfolioCell &c = v.cell[i][b];
      if (!std::isfinite(c.pct)) {
        ImGui::TextDisabled("   -");
        continue;
      }
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::ColorConvertFloat4ToU32(band_col(c.rank, kValCellA)));
      ImGui::Text("%3.0f%%", c.pct);
    }
  }
  ImGui::EndTable();
}

} // namespace

void SyncPortfolioDir(PortfolioUIState &ui, const std::string &dir) {
  if (ui.dir == dir)
    return;
  ui.dir = dir;
  relist(ui, ui.name);
}

void RenderPortfolioBoxes(PortfolioUIState &ui, const FactorRow *view_row, const std::string &pool, int seg, int hold) {
  const ImGuiStyle &st = ImGui::GetStyle();
  const float h = 3.0f * ImGui::GetFrameHeightWithSpacing() - st.ItemSpacing.y + 2.0f * st.WindowPadding.y;
  constexpr float kW1 = 250.0f, kW2 = 380.0f;

  // ---- 小框 1: 选组合 / New / Rename / Del ----
  ImGui::BeginChild("##pf_box", ImVec2(kW1, h), ImGuiChildFlags_Borders, ImGuiWindowFlags_NoScrollbar);
  ImGui::TextColored(kHead, "Portfolio");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(-FLT_MIN);
  if (ImGui::BeginCombo("##pf_sel", ui.name.empty() ? "-" : ui.name.c_str())) {
    for (const std::string &n : ui.names)
      if (ImGui::Selectable(n.c_str(), n == ui.name) && n != ui.name) {
        ui.name = n;
        load_current(ui);
        ui.msg.clear();
        ui.confirm_delete = false;
      }
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    tip("当前组合 (%s/<name>.json)\n"
        "• 组合内容 = 文件, 改了即存; 选哪个只在内存\n"
        "• Portfolio 页画它的等权组合曲线",
        ui.dir.c_str());
  const std::string nm = ui.name_buf;
  const bool nm_ok = ValidFactorName(nm) && std::find(ui.names.begin(), ui.names.end(), nm) == ui.names.end();
  ImGui::SetNextItemWidth(96);
  ImGui::InputTextWithHint("##pf_name", "name", ui.name_buf, sizeof(ui.name_buf));
  ImGui::SameLine();
  ImGui::BeginDisabled(!nm_ok);
  if (ImGui::SmallButton("New")) {
    SavePortfolio(ui.dir, nm, {});
    relist(ui, nm);
    ui.msg = "新建 " + nm;
    ui.name_buf[0] = '\0';
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("用左边的名字新建空组合 (仅 [A-Za-z0-9_], 不与已有重名)");
  ImGui::SameLine();
  ImGui::BeginDisabled(!nm_ok || ui.name.empty());
  if (ImGui::SmallButton("Rename")) {
    const std::string old = ui.name;
    const bool ok = RenamePortfolio(ui.dir, old, nm);
    assert(ok && "重名已在按钮处排除");
    (void)ok;
    relist(ui, nm);
    ui.msg = old + " → " + nm;
    ui.name_buf[0] = '\0';
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("当前组合改成左边的名字");
  ImGui::SameLine();
  ImGui::BeginDisabled(ui.name.empty());
  if (ImGui::SmallButton("Del"))
    ui.confirm_delete = true;
  ImGui::EndDisabled();
  if (ui.confirm_delete) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "删除 %s?", ui.name.c_str());
    ImGui::SameLine();
    if (ImGui::SmallButton("Yes")) {
      DeletePortfolio(ui.dir, ui.name);
      ui.msg = "已删除 " + ui.name;
      relist(ui, "");
      ui.confirm_delete = false;
    }
    ImGui::SameLine();
    if (ImGui::SmallButton("No"))
      ui.confirm_delete = false;
  } else {
    ImGui::TextDisabled("%s", ui.msg.c_str());
  }
  ImGui::EndChild();

  // ---- 小框 2: 当前组合的条目 ----
  ImGui::SameLine();
  ImGui::BeginChild("##pf_items", ImVec2(kW2, h), ImGuiChildFlags_Borders, ImGuiWindowFlags_NoScrollbar);
  ImGui::TextColored(kHead, "Factors (%zu)", ui.entries.size());
  ImGui::SameLine();
  const bool row_ok = view_row && view_row->error.empty() && !view_row->expr.empty();
  ImGui::BeginDisabled(ui.name.empty() || !row_ok || hold <= 0);
  if (ImGui::SmallButton("Add")) {
    const PortfolioEntry e{pool, seg, view_row->file, view_row->expr, hold};
    const auto dup = std::find_if(ui.entries.begin(), ui.entries.end(), [&](const PortfolioEntry &x) { return x.key() == e.key(); });
    if (dup != ui.entries.end()) {
      ui.msg = entry_label(e) + " 已在组合里";
    } else {
      ui.entries.push_back(e);
      save_current(ui);
      ui.sel = static_cast<int>(ui.entries.size()) - 1;
      ui.msg = "加入 " + entry_label(e);
    }
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
    if (ui.name.empty())
      tip("先在左框新建 / 选一个组合");
    else if (!row_ok)
      tip("先在表格里点一行高光 (有效权重; 父行 = 选定 Hold 下 score 最高的权重)");
    else
      tip("把高光的权重加进组合\n"
          "• %s / %s  %s\n"
          "• 持有期 = 本页 Hold (%s)",
          pool.c_str(), factor_level(seg).name, view_row->key.c_str(), factor::stat::hold_name(hold).c_str());
  }
  ImGui::SameLine();
  const bool sel_ok = ui.sel >= 0 && ui.sel < static_cast<int>(ui.entries.size());
  ImGui::BeginDisabled(!sel_ok);
  if (ImGui::SmallButton("Del##pf_item")) {
    ui.msg = "移除 " + entry_label(ui.entries[static_cast<size_t>(ui.sel)]);
    ui.entries.erase(ui.entries.begin() + ui.sel);
    save_current(ui);
    ui.sel = -1;
  }
  ImGui::EndDisabled();
  ImGui::BeginChild("##pf_list", ImVec2(0, 0));
  const bool synced = ui.items.size() == ui.entries.size();
  for (size_t i = 0; i < ui.entries.size(); ++i) {
    const PortfolioEntry &e = ui.entries[i];
    const PortfolioItem *it = synced && ui.items[i].e.key() == e.key() ? &ui.items[i] : nullptr;
    const bool bad = it && !it->missing.empty();
    if (bad)
      ImGui::PushStyleColor(ImGuiCol_Text, StatusColor(TaskStatus::Kind::Error));
    const std::string label = entry_label(e) + "##pfe" + std::to_string(i);
    if (ImGui::Selectable(label.c_str(), ui.sel == static_cast<int>(i)))
      ui.sel = ui.sel == static_cast<int>(i) ? -1 : static_cast<int>(i);
    if (bad)
      ImGui::PopStyleColor();
    if (ImGui::IsItemHovered())
      tip("%s / %s  %s  %s\n"
          "• 规范串 %s%s%s",
          e.pool.c_str(), factor_level(e.seg).name, e.file.c_str(), it ? it->name_cn.c_str() : "", e.expr.c_str(), bad ? "\n• 失效: " : "",
          bad ? it->missing.c_str() : "");
  }
  ImGui::EndChild();
  ImGui::EndChild();
}

int RenderTabPortfolio(PortfolioService &svc, PortfolioUIState &ui, double fixed_impact) {
  PortfolioResult res;
  uint64_t gen = 0;
  std::string message;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    res = svc.result; // shared_ptr 拷贝, 不拷序列
    gen = svc.result_gen;
    message = svc.message;
  }
  PortfolioDerived &d = ui.der.val;
  bool refit = ui.refit;
  ui.refit = false;

  // ---- 三级缓存: 派生 → 滚动分位 → 视图 (键带上级 gen: 上级重算 → 下级自动重算) ----
  ui.der.update(PortfolioDerKey{gen, ui.items_gen, fixed_impact}, [&](PortfolioDerived &dv) {
    const std::vector<misc::Ymd> old = dv.dates;
    derive(ui.items, ui.name, res, fixed_impact, dv);
    if (dv.dates != old) { // 日轴变 → 区间回全段
      ui.range = PortfolioRange{.d0 = 0, .d1 = static_cast<int>(dv.dates.size())};
      refit = true;
    }
  });
  // VAL 选中篮子: 换组合 → 回默认 (组合首个因子所在池; 它还没算出 → 暂用首个池, 不记)
  if (ui.val_for != ui.name)
    ui.val_for = ui.name, ui.val_pool.clear();
  const auto val_of = [&](const std::string &id) {
    for (size_t i = 0; i < d.vals.size(); ++i)
      if (d.vals[i].id == id)
        return static_cast<int>(i);
    return -1;
  };
  int vs = val_of(ui.val_pool);
  if (vs < 0) {
    const auto first = std::find_if(ui.items.begin(), ui.items.end(), [](const PortfolioItem &it) { return it.missing.empty(); });
    vs = first != ui.items.end() ? val_of(first->e.pool) : -1;
    if (vs >= 0)
      ui.val_pool = d.vals[static_cast<size_t>(vs)].id;
    else if (!d.vals.empty())
      vs = 0;
  }
  ui.roll.update(PortfolioRollKey{ui.der.gen, ui.band_years, vs}, [&](std::array<PortfolioRoll, kPoolVals> &r) {
    for (int b = 0; b < kPoolVals; ++b)
      if (vs >= 0)
        roll_of(d.vals[static_cast<size_t>(vs)], b, ui.band_years, r[b]);
      else
        r[b] = {};
  });
  ui.view.update(PortfolioViewKey{ui.roll.gen, ui.range.d0, ui.range.d1},
                 [&](PortfolioView &vw) { make_view(d, ui.range.d0, ui.range.d1, vs, ui.roll.key.years, ui.roll.val, vw); });
  const PortfolioView &v = ui.view.val;

  ImGui::TextColored(kHead, "Portfolio:");
  ImGui::SameLine();
  if (ui.name.empty()) {
    ImGui::TextDisabled("无 (在 Factors 页 1. 区右侧新建 / 选组合)");
    return 0;
  }
  const int action = head(svc, ui, d, res, message, fixed_impact);
  const bool nav = pages();

  // ---- 图: 左 = 主图 (plot_nav ↔ plot_val) / 区间条; 右 = 图例 (legend_nav ↔ legend_val) ----
  bool set_vis = false; // NAV 主图例默认显隐 (组合 + 其因子所在池变了重设)
  if (nav) {
    std::string k = ui.name;
    for (const PortfolioBasket &p : d.pools)
      k += p.on ? "1" : "0";
    set_vis = k != ui.vis_key;
    ui.vis_key = std::move(k);
  }
  if (refit || set_vis)
    ImPlot::SetNextAxesToFit();
  constexpr float kLegendW = 420.0f, kValLegendW = 400.0f, kBarH = 90.0f;
  const ImGuiStyle &st = ImGui::GetStyle();
  const ImVec2 av = ImGui::GetContentRegionAvail();
  const float pw = av.x - (nav ? kLegendW : kValLegendW) - st.ItemSpacing.x;
  ImGui::BeginGroup();
  if (ImPlot::BeginPlot(nav ? "##portfolio" : "##pf_val", ImVec2(pw, av.y - kBarH - st.ItemSpacing.y))) {
    // 线性: y 下限钉在 0 (净值 / 点位恒正), fit / 缩放只动上限; log: 正常
    ImPlot::SetupAxes(nullptr, nav ? "净值 (复利, 区间起点 = 1)" : "点位 (区间起点 = 1)", ImPlotAxisFlags_None,
                      ui.log_y ? ImPlotAxisFlags_None : ImPlotAxisFlags_LockMin);
    ImPlot::SetupAxisFormat(ImAxis_X1, day_formatter, &d.dates);
    if (ui.log_y)
      ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    else
      ImPlot::GetCurrentPlot()->Axes[ImAxis_Y1].SetMin(0.0, /*force=*/true);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
    // fit 帧的视野还是上帧的 (区间刚变时可能整段在外): 小字不按视野筛, 全收来量留白宽度
    const ImPlotRect cur = ImPlot::GetPlotLimits(); // 先锁 setup, FitThisFrame 才定
    PlotFrame pf{ImPlot::FitThisFrame() ? ImPlotRect(-DBL_MAX, DBL_MAX, -DBL_MAX, DBL_MAX) : cur, {}, {}};
    if (nav)
      plot_nav(ui, d, v, set_vis, pf);
    else if (vs >= 0)
      plot_val(ui, d, v, vs, pf);
    pf.shown += ui.log_y ? 'L' : 'l';
    if (pf.shown != ui.shown_key) {
      ui.shown_key = std::move(pf.shown);
      ui.refit = true;
    }
    // 本帧 fit (自动 / 双击): x 右端再扩一段, 让区间末点右侧留出线尾小字的宽度
    if (ImPlot::FitThisFrame() && !pf.tags.empty() && ui.range.d1 > ui.range.d0) {
      const float tw = tags_width(pf.tags), W = ImPlot::GetPlotSize().x;
      const double x0 = x_of(static_cast<size_t>(ui.range.d0), kOpen), x1 = x_of(static_cast<size_t>(ui.range.d1), kOpen);
      if (W > 2.0f * tw)
        ImPlot::FitPointX(x1 + (x1 - x0) * tw / (W - tw));
    }
    draw_tags(pf.tags);
    ImPlot::EndPlot();
  }
  range_bar(ui, nav, vs, pw, kBarH);
  ImGui::EndGroup();

  ImGui::SameLine();
  ImGui::BeginChild("##pf_flegend", ImVec2(0, av.y), ImGuiChildFlags_Borders);
  if (nav)
    legend_nav(ui, d, v);
  else
    legend_val(ui, d, v, vs);
  ImGui::EndChild();
  return action;
}

} // namespace GUI::Factors
