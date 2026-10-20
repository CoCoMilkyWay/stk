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
#include <set>

namespace GUI::Factors {

namespace {

constexpr size_t kDayW = TRADE_MINUTES_PER_DAY; // 横轴每日跨度 (同 OrderFlow 图 2: x = 日下标 · 255 + 分钟)
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
// VAL 线表: 价格线色; 估值名 [kPoolVals]; band 线尾小字的分位名 [kBandQ] (与 kBandQs 同序); band 填色 / 线 / 小字的 alpha
const ImVec4 kValPxCol(0.95f, 0.95f, 0.95f, 1.0f);
constexpr const char *kValName[kPoolVals] = {"PE", "PB", "DY"};
constexpr const char *kBandQName[kBandQ] = {"5%", "10%", "25%", "50%", "75%", "90%", "95%"};
constexpr float kBandFillA = 0.12f, kBandLineA = 0.35f, kBandTagA = 0.9f;
// band 色: u ∈ [0, 1] (0 = 最便宜 绿 → 1 = 最贵 红) 按色相渐变
ImVec4 band_col(double u, float a) {
  ImVec4 c(0.f, 0.f, 0.f, a);
  ImGui::ColorConvertHSVtoRGB(0.33f * static_cast<float>(1.0 - u), 0.8f, 0.9f, c.x, c.y, c.z);
  return c;
}
// 估值值的显示: PE / PB 倍, DY 百分比
void fmt_val(int b, double x, char *buf, size_t n) {
  if (b == kValDy)
    std::snprintf(buf, n, "%.2f%%", 100.0 * x);
  else
    std::snprintf(buf, n, "%.*fx", b == kValPe ? 1 : 2, x);
}

// 右补空格到 px 宽 (等宽字体, 中文占两格: 按实测像素补, 列对齐)
std::string pad_to(const char *s, float px) {
  const float w = ImGui::CalcTextSize(s).x, sp = ImGui::CalcTextSize(" ").x;
  return s + std::string(static_cast<size_t>(std::lround(std::max(0.f, px - w) / sp)), ' ');
}

// 单因子线色: 接在池线 (自动配色, 先建先取) 之后取色表
ImVec4 factor_color(const PortfolioDerived &d, size_t i) { return ImPlot::GetColormapColor(static_cast<int>(d.pools.size() + i)); }

// 组合聚合 + 单因子 + 池线 (见头文件), 全段净值
void derive(const PortfolioUIState &ui, const PortfolioResult &res, double fixed_impact, PortfolioDerived &d) {
  d = PortfolioDerived{};
  std::vector<const PortfolioItem *> its;
  std::vector<const FactorCurve *> fc;
  std::set<std::string> own;
  for (const PortfolioItem &it : ui.items) {
    if (!it.missing.empty())
      continue;
    own.insert(it.e.pool);
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
  const auto day_of = [&](misc::Ymd y) { return static_cast<size_t>(std::lower_bound(d.dates.begin(), d.dates.end(), y) - d.dates.begin()); };

  // ---- 组合: 每日在当日有数据的因子间等分; 增量按整数 x 稠密累加 (与因子数 / 行数无关, 免排序) ----
  std::vector<int> n_act(d.dates.size(), 0);
  for (const FactorCurve *f : fc)
    for (const misc::Ymd y : f->dates)
      ++n_act[day_of(y)];
  struct Cell {
    double inc[kImpacts];
    uint8_t hit;
  };
  std::vector<Cell> cell(d.dates.size() * kDayW);
  for (size_t q = 0; q < fc.size(); ++q) {
    const FactorCurve *f = fc[q];
    const int S = f->seg;
    const factor::Geo g = factor::geo_of(S);
    const int h = factor::stat::hold_rows(f->hold, g);
    assert(!f->dates.empty() && f->ret.size() == f->dates.size() * static_cast<size_t>(S));
    // 每个新进者: 平仓 fixed_impact + 建仓 (Lob = 金额段冲击, 已在 wimp; Bar 无段 = 再一份 fixed_impact)
    const double ke = f->amt > 0 ? fixed_impact : 2.0 * fixed_impact;
    PortfolioDerived::Factor pf;
    pf.key = its[q]->e.key();
    pf.label = entry_label(its[q]->e) + (f->amt > 0 ? " (" + std::to_string(f->amt) + "w冲击)" : std::string(" (固定冲击)"));
    pf.tag = its[q]->name_en + " " + factor::stat::hold_name(f->hold);
    pf.c.x.push_back(static_cast<double>(day_of(f->dates.front()) * kDayW)), pf.c.y.push_back(1.0);
    // 行收益记在平仓时刻 (与池指数同为已实现口径): 分钟型 = 行末分钟 + n (过收盘 → 收盘); 收盘型 = 当日收盘;
    // T+N = 其后第 N 个交易日开盘 (越过区间末 → 末日收盘, 标签按最后收盘结算)
    const auto exit_x = [&](size_t dl, int ts) -> size_t {
      const size_t di = day_of(f->dates[dl]);
      if (factor::stat::hold_intraday(f->hold))
        return di * kDayW + std::min(static_cast<size_t>(g.minute(ts) + f->hold), kDayW - 1);
      if (f->hold == factor::stat::kHoldClose)
        return di * kDayW + kDayW - 1;
      const size_t de = dl + static_cast<size_t>(f->hold - factor::stat::kHoldDayBase);
      if (de >= f->dates.size())
        return day_of(f->dates.back()) * kDayW + kDayW - 1;
      return day_of(f->dates[de]) * kDayW;
    };
    double nav = 1.0;
    for (size_t dl = 0; dl < f->dates.size(); ++dl) {
      const size_t di = day_of(f->dates[dl]);
      for (int ts = 0; ts < S; ++ts) {
        const size_t t = dl * S + static_cast<size_t>(ts);
        double r[kImpacts]; // 本行增量 (/ h)
        r[kImpactNet] = (f->ret[t] - f->wimp[t] - f->we[t] * ke) / h;
        r[kImpactGross] = f->ret[t] / h;
        const size_t x = exit_x(dl, ts);
        Cell &c = cell[x];
        c.hit = 1;
        for (int i = 0; i < kImpacts; ++i)
          c.inc[i] += r[i] / n_act[di];
        nav *= 1.0 + r[kImpactNet];
        pf.c.x.push_back(static_cast<double>(x)), pf.c.y.push_back(nav);
      }
    }
    d.factors.push_back(std::move(pf));
  }
  if (!d.dates.empty()) {
    std::vector<double> xs{0.0};
    double nav[kImpacts];
    for (int i = 0; i < kImpacts; ++i)
      nav[i] = 1.0, d.strat[i].y.push_back(1.0);
    for (size_t x = 0; x < cell.size(); ++x) {
      if (!cell[x].hit)
        continue;
      xs.push_back(static_cast<double>(x));
      for (int i = 0; i < kImpacts; ++i)
        nav[i] *= 1.0 + cell[x].inc[i], d.strat[i].y.push_back(nav[i]);
    }
    for (int i = 0; i < kImpacts; ++i)
      d.strat[i].x = xs;
  }

  // ---- 池线: 只在日并集轴范围内; 不在轴上的日的收益并进下一个轴上的日 ----
  // 图例 = 池名 | 中文名 | 年换手 (日均 × kDaysPerYear) 三列, 各列按最宽者补空格对齐
  // all 池拆两条: 全A加权 (流通市值, kWeightCap) / 全A等权 (kWeightEq); 其余池一条等权
  struct Line {
    const PoolDef *pd;
    const char *cn, *tag;
    Weight w;
  };
  std::vector<Line> lines;
  for (const PoolDef &pd : feature_pools())
    if (pd.pool_col == SIZE_MAX)
      for (const Weight w : {kWeightCap, kWeightEq})
        lines.push_back({&pd, kWeightName[w], kWeightName[w], w});
    else
      lines.push_back({&pd, pd.name_cn, pd.name, kWeightEq});
  float &w_en = d.w_en, &w_cn = d.w_cn; // 列宽含策略线 (组合名 | 冲击口径), 与池线对齐
  w_en = ImGui::CalcTextSize(ui.name.c_str()).x, w_cn = 0.f;
  for (const char *cn : kImpactName)
    w_cn = std::max(w_cn, ImGui::CalcTextSize(cn).x);
  for (const Line &ln : lines)
    w_en = std::max(w_en, ImGui::CalcTextSize(ln.pd->name).x), w_cn = std::max(w_cn, ImGui::CalcTextSize(ln.cn).x);
  for (const Line &ln : lines) {
    const PoolDef &pd = *ln.pd;
    PortfolioDerived::Pool p;
    const auto pit = res.pools.find(pd.name);
    const PoolCurve *pc = pit != res.pools.end() && pit->second->error.empty() ? pit->second.get() : nullptr;
    char turn[48] = "";
    if (pc)
      std::snprintf(turn, sizeof(turn), "  (年换手=%5.1f倍)", pc->turnover[ln.w] * factor::stat::kDaysPerYear);
    p.label = pad_to(pd.name, w_en) + "  " + pad_to(ln.cn, w_cn) + turn + "###pf_pool_" + pd.name + kWeightId[ln.w];
    p.tag = ln.tag;
    p.own = own.contains(pd.name);
    if (pc && !d.dates.empty()) {
      const PoolCurve &c = *pc;
      const std::vector<float> &ix = c.index[ln.w];
      double base = 0.0;
      for (size_t i = 0; i < c.dates.size(); ++i) {
        if (c.dates[i] < d.dates.front() || c.dates[i] > d.dates.back())
          continue;
        const size_t di = day_of(c.dates[i]);
        if (p.c.x.empty()) {
          base = i > 0 ? ix[i - 1] : ix[i];
          p.c.x.push_back(static_cast<double>(di * kDayW)), p.c.y.push_back(1.0);
        }
        if (d.dates[di] != c.dates[i])
          continue;
        p.c.x.push_back(static_cast<double>(di * kDayW + kDayW - 1)), p.c.y.push_back(ix[i] / base);
      }
    }
    d.pools.push_back(std::move(p));
  }

  // ---- VAL: 各池 / 申万一级行业 (all 池按行业切) 的流通市值加权指数 + 同日估值 (含滚动分位), 只取日并集轴上的日 (收盘一点);
  //      右图例 = 池名 | 中文名 (all = 全A加权) / sw<ID> | 行业名 ----
  if (d.dates.empty())
    return;
  std::vector<std::string> v_cns;
  const auto add_val = [&](std::string name, std::string cn, std::string tag, const PoolCurve &c, const std::vector<float> &ix,
                           const ValCurve(&vc)[kPoolVals]) {
    PortfolioDerived::Val vl;
    vl.name = std::move(name), vl.tag = std::move(tag);
    assert(ix.size() == c.dates.size());
    for (const ValCurve &s : vc)
      assert(s.v.size() == c.dates.size() && s.pct.size() == c.dates.size());
    for (size_t i = 0; i < c.dates.size(); ++i) {
      if (c.dates[i] < d.dates.front() || c.dates[i] > d.dates.back())
        continue;
      const size_t di = day_of(c.dates[i]);
      if (d.dates[di] != c.dates[i])
        continue;
      vl.p.x.push_back(static_cast<double>(di * kDayW + kDayW - 1)), vl.p.y.push_back(ix[i]);
      for (int b = 0; b < kPoolVals; ++b) {
        ValCurve &o = vl.val[b];
        o.v.push_back(vc[b].v[i]), o.pct.push_back(vc[b].pct[i]);
        for (int j = 0; j < kBandQ; ++j)
          o.q[j].push_back(vc[b].q[j][i]);
      }
    }
    v_cns.push_back(std::move(cn));
    d.vals.push_back(std::move(vl));
  };
  const PoolCurve *all = nullptr;
  for (const PoolDef &pd : feature_pools()) {
    const auto pit = res.pools.find(pd.name);
    if (pit == res.pools.end() || !pit->second->error.empty())
      continue;
    const PoolCurve &c = *pit->second;
    if (pd.pool_col == SIZE_MAX)
      all = &c;
    add_val(pd.name, pd.pool_col == SIZE_MAX ? kWeightName[kWeightCap] : pd.name_cn, pd.name, c, c.index[kWeightCap], c.val);
  }
  d.n_val_pools = d.vals.size();
  if (all) {
    assert(all->inds.size() == fund::SW2021_L1_COUNT);
    for (size_t k = 1; k < fund::SW2021_L1_COUNT; ++k) {
      char id[8];
      std::snprintf(id, sizeof(id), "sw%02zu", k);
      const std::string nm(fund::SW2021_L1_NAMES[k]);
      add_val(id, nm, nm, *all, all->inds[k].index, all->inds[k].val);
    }
  }
  float v_en = 0.f, v_cn = 0.f;
  for (size_t i = 0; i < d.vals.size(); ++i)
    v_en = std::max(v_en, ImGui::CalcTextSize(d.vals[i].name.c_str()).x), v_cn = std::max(v_cn, ImGui::CalcTextSize(v_cns[i].c_str()).x);
  for (size_t i = 0; i < d.vals.size(); ++i)
    d.vals[i].label = pad_to(d.vals[i].name.c_str(), v_en) + "  " + pad_to(v_cns[i].c_str(), v_cn);
}

// 切到 [x0, x1) 并在 x0 归一: x0 前有点 → 起点 (x0, 1), 基 = x0 前末点; 否则基 = 自身首点. 整条在区间外 → 空
void rebase(const PortfolioCurve &s, double x0, double x1, PortfolioCurve &o) {
  o.x.clear(), o.y.clear();
  const size_t i0 = static_cast<size_t>(std::lower_bound(s.x.begin(), s.x.end(), x0) - s.x.begin());
  const size_t i1 = static_cast<size_t>(std::lower_bound(s.x.begin(), s.x.end(), x1) - s.x.begin());
  if (i0 == s.x.size() || i1 == 0)
    return;
  const double base = i0 > 0 ? s.y[i0 - 1] : s.y[0];
  if (i0 > 0)
    o.x.push_back(x0), o.y.push_back(1.0);
  for (size_t i = i0; i < i1; ++i)
    o.x.push_back(s.x[i]), o.y.push_back(s.y[i] / base);
}

// 复利年化 (%): 末点净值按首末点所跨交易日数折年
float ann_of(const PortfolioCurve &c) {
  if (c.x.size() < 2)
    return 0.f;
  const double days = std::floor(c.x.back() / kDayW) - std::floor(c.x.front() / kDayW) + 1.0;
  return static_cast<float>(100.0 * (std::pow(c.y.back(), static_cast<double>(factor::stat::kDaysPerYear) / days) - 1.0));
}

// 估值 (PE / PB / DY) 可算: 有限 ∧ > 0
bool val_ok(double r) { return std::isfinite(r) && r > 0.0; }

// vs = VAL 选中池 (der.vals 下标, −1 = 无)
void make_view(const PortfolioDerived &d, int d0, int d1, int vs, PortfolioView &v) {
  const double x0 = static_cast<double>(static_cast<size_t>(d0) * kDayW), x1 = static_cast<double>(static_cast<size_t>(d1) * kDayW);
  // ---- VAL: 各池区间末个可算点的 PE / PB / DY; 选中池的价格线 (区间首点归一) + 各口滚动 band ----
  constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
  const auto range_of = [&](const PortfolioCurve &c) {
    return std::pair{static_cast<size_t>(std::lower_bound(c.x.begin(), c.x.end(), x0) - c.x.begin()),
                     static_cast<size_t>(std::lower_bound(c.x.begin(), c.x.end(), x1) - c.x.begin())};
  };
  for (int b = 0; b < kPoolVals; ++b) {
    v.val_now[b].assign(d.vals.size(), kNaN);
    for (size_t i = 0; i < d.vals.size(); ++i) {
      const std::vector<float> &r = d.vals[i].val[b].v;
      const auto [i0, i1] = range_of(d.vals[i].p);
      for (size_t j = i1; j > i0; --j)
        if (val_ok(r[j - 1])) {
          v.val_now[b][i] = r[j - 1];
          break;
        }
    }
  }
  v.val = {};
  for (PortfolioBand &bd : v.band)
    bd = {};
  if (vs >= 0) {
    const PortfolioDerived::Val &s = d.vals[static_cast<size_t>(vs)];
    const auto [i0, i1] = range_of(s.p);
    for (size_t i = i0; i < i1; ++i)
      v.val.x.push_back(s.p.x[i]), v.val.y.push_back(s.p.y[i] / s.p.y[i0]);
    for (int b = 0; b < kPoolVals; ++b) {
      const ValCurve &r = s.val[b];
      PortfolioBand &bd = v.band[b];
      const auto inv = [&](double x) { return kValInv[b] ? 1.0 / x : x; };
      size_t last = SIZE_MAX;
      for (int j = 0; j < kBandQ; ++j)
        bd.line[j].x = v.val.x, bd.line[j].y.assign(i1 - i0, kNaN);
      for (size_t i = i0; i < i1; ++i) {
        if (!val_ok(r.v[i]) || !std::isfinite(r.q[0][i]))
          continue;
        const double m = inv(r.v[i]);
        for (int j = 0; j < kBandQ; ++j)
          bd.line[j].y[i - i0] = v.val.y[i - i0] * r.q[j][i] / m;
        const int k = static_cast<int>(i - i0);
        if (bd.segs.empty() || bd.segs.back().second != k)
          bd.segs.push_back({k, k + 1});
        else
          ++bd.segs.back().second;
        last = i;
      }
      if (last == SIZE_MAX)
        continue;
      bd.ok = true;
      bd.now = r.v[last], bd.pct = r.pct[last];
      for (int j = 0; j < kBandQ; ++j)
        bd.q[j] = inv(r.q[j][last]);
    }
  }

  for (int i = 0; i < kImpacts; ++i) {
    rebase(d.strat[i], x0, x1, v.strat[i]);
    v.ann[i] = ann_of(v.strat[i]);
  }
  v.pools.resize(d.pools.size());
  for (size_t i = 0; i < d.pools.size(); ++i)
    rebase(d.pools[i].c, x0, x1, v.pools[i]);
  v.factors.resize(d.factors.size());
  v.ann_f.resize(d.factors.size());
  for (size_t i = 0; i < d.factors.size(); ++i) {
    rebase(d.factors[i].c, x0, x1, v.factors[i]);
    v.ann_f[i] = ann_of(v.factors[i]);
  }
}

// 线尾小字: 视野内末点的像素位置
struct EndTag {
  ImVec2 at;
  ImU32 col;
  const char *text;
};
void push_tag(std::vector<EndTag> &tags, const ImPlotRect &lim, const PortfolioCurve &c, ImU32 col, const char *text) {
  const auto it = std::upper_bound(c.x.begin(), c.x.end(), lim.X.Max);
  if (it == c.x.begin())
    return;
  const size_t i = static_cast<size_t>(it - c.x.begin()) - 1;
  if (c.x[i] < lim.X.Min || !std::isfinite(c.y[i]))
    return;
  tags.push_back({ImPlot::PlotToPixels(c.x[i], c.y[i]), col, text});
}
constexpr float kTagScale = 0.95f, kTagGap = 4.0f; // 小字字号 (× 字体) / 离末点的间距 (px)
// 小字占的最宽像素 (含间距 + 右边余量)
float tags_width(const std::vector<EndTag> &tags) {
  ImFont *font = ImGui::GetFont();
  const float fs = ImGui::GetFontSize() * kTagScale;
  float w = 0.0f;
  for (const EndTag &t : tags)
    w = std::max(w, font->CalcTextSizeA(fs, FLT_MAX, 0.0f, t.text).x);
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
    const ImVec2 sz = font->CalcTextSizeA(fs, FLT_MAX, 0.0f, t.text);
    dl->AddText(font, fs, ImVec2(std::min(t.at.x + kTagGap, right - sz.x - 2.0f), t.at.y - 0.5f * sz.y), t.col, t.text);
  }
  ImPlot::PopPlotClipRect();
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
  int action = 0;
  PortfolioResult res;
  uint64_t gen = 0;
  std::string message;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    res = svc.result; // shared_ptr 拷贝, 不拷序列
    gen = svc.result_gen;
    message = svc.message;
  }
  PortfolioDerived &d = ui.der;
  bool refit = ui.refit;
  ui.refit = false;
  if (gen != ui.der_gen || ui.items_gen != ui.der_items || fixed_impact != ui.der_impact) {
    const std::vector<misc::Ymd> old = d.dates;
    derive(ui, res, fixed_impact, d);
    ui.der_gen = gen, ui.der_items = ui.items_gen, ui.der_impact = fixed_impact;
    if (d.dates != old) { // 日轴变 → 区间回全段
      ui.d0 = 0, ui.d1 = static_cast<int>(d.dates.size());
      ui.drag = 0;
      refit = true;
    }
    ui.view_d0 = -1;
  }
  // VAL 选中池: 换组合 → 回默认 (组合首个因子所在池; 它还没算出 → 暂用首个池, 不记)
  if (ui.val_for != ui.name)
    ui.val_for = ui.name, ui.val_pool.clear();
  const auto val_of = [&](const std::string &name) {
    for (size_t i = 0; i < d.vals.size(); ++i)
      if (d.vals[i].name == name)
        return static_cast<int>(i);
    return -1;
  };
  int vs = val_of(ui.val_pool);
  if (vs < 0) {
    const auto first = std::find_if(ui.items.begin(), ui.items.end(), [](const PortfolioItem &it) { return it.missing.empty(); });
    vs = first != ui.items.end() ? val_of(first->e.pool) : -1;
    if (vs >= 0)
      ui.val_pool = d.vals[static_cast<size_t>(vs)].name;
    else if (!d.vals.empty())
      vs = 0;
  }
  if (ui.view_d0 != ui.d0 || ui.view_d1 != ui.d1 || ui.view_val != vs) {
    make_view(d, ui.d0, ui.d1, vs, ui.view);
    ui.view_d0 = ui.d0, ui.view_d1 = ui.d1, ui.view_val = vs;
  }
  const PortfolioView &v = ui.view;

  // ---- 头: 组合 / 成本 / Recompute / Cancel / 状态 ----
  ImGui::TextColored(kHead, "Portfolio:");
  ImGui::SameLine();
  if (ui.name.empty()) {
    ImGui::TextDisabled("无 (在 Factors 页 1. 区右侧新建 / 选组合)");
    return 0;
  }
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
  // 失效 / 失败的条目逐条列原因
  for (const PortfolioItem &it : ui.items) {
    const auto f = res.factors.find(it.e.key());
    const std::string &why = !it.missing.empty() ? it.missing : f != res.factors.end() ? f->second->error
                                                                                       : it.missing;
    if (!why.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "  %s  %s", entry_label(it.e).c_str(), why.c_str());
  }

  // ---- 页: NAV = 资金曲线 / VAL = 池估值 (同一布局; 区间 / log 共用) ----
  bool nav = true;
  if (ImGui::BeginTabBar("##pf_page")) {
    if (ImGui::BeginTabItem("NAV"))
      ImGui::EndTabItem();
    if (ImGui::BeginTabItem("VAL")) {
      nav = false;
      ImGui::EndTabItem();
    }
    ImGui::EndTabBar();
  }

  // ---- 图: 左 = 主图 (NAV: 组合 + 池 + 单因子; VAL: 选中池价格 + PE / PB Band) / 区间条; 右 = 单因子图例 / 池单选 ----
  bool set_vis = false; // NAV 池线默认显隐 (组合 + 其因子所在池变了重设)
  if (nav) {
    std::string k = ui.name;
    for (const PortfolioDerived::Pool &p : d.pools)
      k += p.own ? "1" : "0";
    set_vis = k != ui.vis_key;
    ui.vis_key = std::move(k);
  }
  if (refit || set_vis)
    ImPlot::SetNextAxesToFit();
  constexpr float kLegendW = 320.0f, kBarH = 90.0f;
  const ImGuiStyle &st = ImGui::GetStyle();
  const ImVec2 av = ImGui::GetContentRegionAvail();
  const float pw = av.x - kLegendW - st.ItemSpacing.x;
  ImGui::BeginGroup();
  if (ImPlot::BeginPlot(nav ? "##portfolio" : "##pf_val", ImVec2(pw, av.y - kBarH - st.ItemSpacing.y))) {
    // 线性: y 下限钉在 0 (净值 / 点位恒正), fit / 缩放只动上限; log: 正常
    ImPlot::SetupAxes(nullptr, nav ? "净值 (复利, 区间起点 = 1)" : "点位 (区间首点 = 1)", ImPlotAxisFlags_None,
                      ui.log_y ? ImPlotAxisFlags_None : ImPlotAxisFlags_LockMin);
    ImPlot::SetupAxisFormat(ImAxis_X1, day_formatter, &d.dates);
    if (ui.log_y)
      ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    else
      ImPlot::GetCurrentPlot()->Axes[ImAxis_Y1].SetMin(0.0, /*force=*/true);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
    // fit 帧的视野还是上帧的 (区间刚变时可能整段在外): 小字不按视野筛, 全收来量留白宽度
    const ImPlotRect cur = ImPlot::GetPlotLimits(); // 先锁 setup, FitThisFrame 才定
    const ImPlotRect lim = ImPlot::FitThisFrame() ? ImPlotRect(-DBL_MAX, DBL_MAX, -DBL_MAX, DBL_MAX) : cur;
    std::vector<EndTag> tags;
    std::string shown; // 各线显隐 + log: 与上帧不同 → 下帧重新 fit
    const auto legend_shown = [&](const char *label) {
      const ImPlotItem *it = ImPlot::GetItem(label);
      assert(it);
      shown += it->Show ? '1' : '0';
      return it->Show ? it : nullptr;
    };
    std::string stag[kImpacts], btag[kPoolVals][kBandQ]; // 线尾小字 (EndTag 只存指针, 活到 draw_tags)
    if (nav) {
      for (size_t i = 0; i < d.pools.size(); ++i) {
        const PortfolioDerived::Pool &p = d.pools[i];
        const PortfolioCurve &c = v.pools[i];
        if (set_vis)
          ImPlot::HideNextItem(!p.own, ImPlotCond_Always);
        ImPlot::PlotLine(p.label.c_str(), c.x.data(), c.y.data(), static_cast<int>(c.x.size()));
        if (const ImPlotItem *it = legend_shown(p.label.c_str()))
          push_tag(tags, lim, c, it->Color, p.tag.c_str());
      }
      for (size_t i = 0; i < d.factors.size(); ++i) {
        const PortfolioDerived::Factor &f = d.factors[i];
        const bool on = ui.factor_show[f.key];
        shown += on ? '1' : '0';
        if (!on)
          continue;
        const PortfolioCurve &c = v.factors[i];
        const ImVec4 col = factor_color(d, i);
        ImPlot::SetNextLineStyle(col, 1.0f);
        ImPlot::PlotLine(("##pf_f" + f.key).c_str(), c.x.data(), c.y.data(), static_cast<int>(c.x.size()));
        push_tag(tags, lim, c, ImGui::ColorConvertFloat4ToU32(col), f.tag.c_str());
      }
      // 策略线图例 = 组合名 | 冲击口径 | 区间年化, 列宽同池线
      for (int i = 0; i < kImpacts; ++i) {
        const PortfolioCurve &c = v.strat[i];
        const ImVec4 col(kStratCol.x, kStratCol.y, kStratCol.z, kImpactAlpha[i]);
        char ann[48];
        std::snprintf(ann, sizeof(ann), "  (年化=%+5.1f%%)", v.ann[i]);
        const std::string lb = pad_to(ui.name.c_str(), d.w_en) + "  " + pad_to(kImpactName[i], d.w_cn) + ann + "###pf_strat" + kImpactId[i];
        if (set_vis && i != kImpactNet) // 策略线默认只显有冲击
          ImPlot::HideNextItem(true, ImPlotCond_Always);
        ImPlot::SetNextLineStyle(col, kImpactWidth[i]);
        ImPlot::PlotLine(lb.c_str(), c.x.data(), c.y.data(), static_cast<int>(c.x.size()));
        stag[i] = ui.name + kImpactTag[i];
        if (legend_shown(lb.c_str()))
          push_tag(tags, lim, c, ImGui::ColorConvertFloat4ToU32(col), stag[i].c_str());
      }
    } else if (vs >= 0) { // VAL: 选中池各口滚动 band (填色 + kBandQ 条线共一个图例项, 一键开关; PB / DY 首次默认关) + 价格线 (最上层)
      const PortfolioDerived::Val &s = d.vals[static_cast<size_t>(vs)];
      shown += s.name;
      for (int b = 0; b < kPoolVals; ++b) {
        const PortfolioBand &bd = v.band[b];
        char lb[128];
        if (bd.ok) {
          char now[16];
          fmt_val(b, bd.now, now, sizeof(now));
          std::snprintf(lb, sizeof(lb), "%s Band  当前 %s  (%d年估值分位 %.0f%%)###pf_band%d", kValName[b], now, kBandYears, bd.pct, b);
        } else
          std::snprintf(lb, sizeof(lb), "%s Band  (区间内不可算)###pf_band%d", kValName[b], b);
        if (b != kValPe)
          ImPlot::HideNextItem(true, ImPlotCond_Once);
        // 色块 c ∈ [0, kBandQ]: 0 = 最便宜档以下 (到图底), kBandQ = 最贵档以上 (到图顶; 两头不进 fit), 其余 = 相邻两档之间
        for (const auto &[i0, i1] : bd.segs) {
          const int n = i1 - i0;
          const double *x = bd.line[0].x.data() + i0;
          for (int c = 0; c <= kBandQ; ++c) {
            ImPlot::SetNextFillStyle(band_col(static_cast<double>(c) / kBandQ, 1.0f), kBandFillA);
            if (c == 0)
              ImPlot::PlotShaded(lb, x, bd.line[0].y.data() + i0, n, -HUGE_VAL, ImPlotItemFlags_NoFit);
            else if (c == kBandQ)
              ImPlot::PlotShaded(lb, x, bd.line[kBandQ - 1].y.data() + i0, n, HUGE_VAL, ImPlotItemFlags_NoFit);
            else
              ImPlot::PlotShaded(lb, x, bd.line[c - 1].y.data() + i0, bd.line[c].y.data() + i0, n);
          }
        }
        for (int j = 0; j < kBandQ; ++j) {
          const PortfolioCurve &c = bd.line[j];
          ImPlot::SetNextLineStyle(band_col((j + 0.5) / kBandQ, kBandLineA), j == kBandQ / 2 ? 1.5f : 1.0f);
          ImPlot::PlotLine(lb, c.x.data(), c.y.data(), static_cast<int>(c.x.size()), ImPlotLineFlags_SkipNaN);
        }
        if (legend_shown(lb) && bd.ok)
          for (int j = 0; j < kBandQ; ++j) {
            char t[32], q[16];
            fmt_val(b, bd.q[j], q, sizeof(q));
            std::snprintf(t, sizeof(t), "%s %s", kBandQName[j], q);
            btag[b][j] = t;
            push_tag(tags, lim, bd.line[j], ImGui::ColorConvertFloat4ToU32(band_col((j + 0.5) / kBandQ, kBandTagA)), btag[b][j].c_str());
          }
      }
      const std::string plb = s.tag + "  流通市值加权指数###pf_val_px";
      ImPlot::SetNextLineStyle(kValPxCol, 2.0f);
      ImPlot::PlotLine(plb.c_str(), v.val.x.data(), v.val.y.data(), static_cast<int>(v.val.x.size()));
      if (legend_shown(plb.c_str()))
        push_tag(tags, lim, v.val, ImGui::ColorConvertFloat4ToU32(kValPxCol), s.tag.c_str());
    }
    shown += ui.log_y ? 'L' : 'l';
    if (shown != ui.shown_key) {
      ui.shown_key = std::move(shown);
      ui.refit = true;
    }
    // 本帧 fit (自动 / 双击): x 右端再扩一段, 让区间末点右侧留出线尾小字的宽度
    if (ImPlot::FitThisFrame() && !tags.empty() && ui.d1 > ui.d0) {
      const float tw = tags_width(tags), W = ImPlot::GetPlotSize().x;
      const double x0 = static_cast<double>(static_cast<size_t>(ui.d0) * kDayW), x1 = static_cast<double>(static_cast<size_t>(ui.d1) * kDayW);
      if (W > 2.0f * tw)
        ImPlot::FitPointX(x1 + (x1 - x0) * tw / (W - tw));
    }
    draw_tags(tags);
    ImPlot::EndPlot();
  }

  // ---- 区间条: 全段组合 (有冲击) / VAL 选中池价格; 选段 [d0, d1) 两边吸附到月初 (每月首个交易日; 右边 = 下月月初 / 末尾), 拖动中即吸附即生效 ----
  //   左右边 = 竖把手 (拖改起止, 双击拉到头); 段内拖 = 平移 (月数不变); 悬停 / 拖动时把手旁标起止日
  const int D = static_cast<int>(d.dates.size());
  if (ImPlot::BeginPlot("##pf_range", ImVec2(pw, kBarH),
                        ImPlotFlags_NoTitle | ImPlotFlags_NoLegend | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoMouseText)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_None, ImPlotAxisFlags_NoDecorations | ImPlotAxisFlags_AutoFit);
    if (D > 0)
      ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, static_cast<double>(d.dates.size() * kDayW), ImPlotCond_Always);
    ImPlot::SetupAxisFormat(ImAxis_X1, day_formatter, &d.dates);
    if (ui.log_y)
      ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    static const PortfolioCurve kNone;
    const PortfolioCurve &rc = nav ? d.strat[kImpactNet] : vs >= 0 ? d.vals[static_cast<size_t>(vs)].p
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
      const auto px_of = [&](int day) { return ImPlot::PlotToPixels(static_cast<double>(static_cast<size_t>(day) * kDayW), lim.Y.Min).x; };
      const ImVec2 pp = ImPlot::GetPlotPos(), ps = ImPlot::GetPlotSize(), mp = ImGui::GetMousePos();
      const double mday = ImPlot::GetPlotMousePos().x / static_cast<double>(kDayW);
      constexpr float kGrab = 7.0f; // 把手抓取半宽 (px)
      int over = 0;                 // 鼠标下: 1 左把手 / 2 右把手 / 3 段内
      if (mp.x >= pp.x && mp.x <= pp.x + ps.x && mp.y >= pp.y && mp.y <= pp.y + ps.y &&
          ImGui::IsWindowHovered(ImGuiHoveredFlags_AllowWhenBlockedByActiveItem)) {
        const float x0 = px_of(ui.d0), x1 = px_of(ui.d1);
        over = std::abs(mp.x - x0) <= kGrab ? 1 : std::abs(mp.x - x1) <= kGrab ? 2
                                              : (mp.x > x0 && mp.x < x1)       ? 3
                                                                               : 0;
      }
      int k0 = near_k(ui.d0, 0, K), k1 = near_k(ui.d1, 0, K);
      if (k1 <= k0) // 保 0 ≤ k0 < k1 ≤ K (下面按边拖的取值范围要它)
        k1 = std::min(k0 + 1, K), k0 = k1 - 1;
      if (ui.drag == 0 && over != 0 && ImGui::IsMouseClicked(0))
        ui.drag = over, ui.grab_day = mday, ui.grab_k0 = k0, ui.grab_k1 = k1;
      if (ui.drag != 0 && !ImGui::IsMouseDown(0))
        ui.drag = 0;
      if (ui.drag == 1)
        k0 = near_k(mday, 0, k1 - 1);
      else if (ui.drag == 2)
        k1 = near_k(mday, k0 + 1, K);
      else if (ui.drag == 3) {
        const int w = ui.grab_k1 - ui.grab_k0;
        k0 = near_k(ms[ui.grab_k0] + (mday - ui.grab_day), 0, K - w), k1 = k0 + w;
      }
      if (over == 1 && ImGui::IsMouseDoubleClicked(0))
        k0 = 0;
      if (over == 2 && ImGui::IsMouseDoubleClicked(0))
        k1 = K;
      if (k1 <= k0)
        k1 = std::min(k0 + 1, K), k0 = k1 - 1;
      if (ms[k0] != ui.d0 || ms[k1] != ui.d1) {
        ui.d0 = ms[k0], ui.d1 = ms[k1];
        ui.refit = true;
      }
      const int act = ui.drag != 0 ? ui.drag : over;
      if (act == 1 || act == 2)
        ImGui::SetMouseCursor(ImGuiMouseCursor_ResizeEW);
      else if (act == 3)
        ImGui::SetMouseCursor(ImGuiMouseCursor_Hand);

      // 画: 段外压暗 + 段内淡色 + 两边竖把手 (带握纹, 悬停 / 拖动高亮)
      const float x0 = px_of(ui.d0), x1 = px_of(ui.d1), yb = pp.y + ps.y;
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
        const misc::Ymd a = d.dates[ui.d0], b = d.dates[ui.d1 - 1];
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
            d.dates[ui.d0] / 10000, d.dates[ui.d0] / 100 % 100, d.dates[ui.d0] % 100, d.dates[ui.d1 - 1] / 10000, d.dates[ui.d1 - 1] / 100 % 100,
            d.dates[ui.d1 - 1] % 100, ui.d1 - ui.d0);
    }
    ImPlot::EndPlot();
  }
  ImGui::EndGroup();

  // ---- 右图例: NAV = 单因子 G20 多头 (默认全关); VAL = 池单选 + 各池区间末 PE / PB ----
  ImGui::SameLine();
  ImGui::BeginChild("##pf_flegend", ImVec2(0, av.y), ImGuiChildFlags_Borders);
  if (!nav) {
    ImGui::TextColored(kHead, "池 / 行业 (单选)");
    if (ImGui::IsItemHovered())
      tip("选一个池或申万一级行业: 主图画它的价格 + PE / PB / DY Band\n"
          "• 价格 = 流通市值加权指数 (all = 全A加权; 行业 = 全A 内该行业)\n"
          "• 行业成员 = 全A 成分按上一交易日的 SW2021 一级行业 (ind_l1) 切; 行业未知的只进全A\n"
          "• PE = Σ总市值 / Σ归母净利TTM, PB = Σ总市值 / Σ归母权益MRQ, DY = Σ近365日税前分红 / Σ总市值 (整体法, 亏损 / 负权益照计)\n"
          "• 成分 = 上一交易日收盘定的池 (同池指数); 市值 = 收盘价 × 当日适用总股本, 财务取当日适用 (无前视)\n"
          "• Band = 价格 × q / 当日倍数 (倍数 = PE / PB / 1÷DY), q = 过去 5 年 (含当日) 倍数的 5/10/25/50/75/90/95%% 分位, 逐日滚动;\n"
          "  不足 1 年样本不画. 最便宜档以下绿 → 最贵档以上红; 估值分位 = 当日倍数在窗内的分位 (高 = 贵)\n"
          "• 右列 = 区间末的 PE / PB / DY");
    for (size_t i = 0; i < d.vals.size(); ++i) {
      if (i == d.n_val_pools)
        ImGui::SeparatorText("申万一级 (全A)");
      char now[80];
      const double pe = v.val_now[kValPe][i], pb = v.val_now[kValPb][i], dy = v.val_now[kValDy][i];
      char spe[16] = "    -", spb[16] = "   -", sdy[16] = "    -";
      if (std::isfinite(pe))
        std::snprintf(spe, sizeof(spe), "%5.1f", pe);
      if (std::isfinite(pb))
        std::snprintf(spb, sizeof(spb), "%4.2f", pb);
      if (std::isfinite(dy))
        std::snprintf(sdy, sizeof(sdy), "%4.2f%%", 100.0 * dy);
      std::snprintf(now, sizeof(now), "  PE=%s  PB=%s  DY=%s##pfv%zu", spe, spb, sdy, i);
      if (ImGui::Selectable((d.vals[i].label + now).c_str(), vs == static_cast<int>(i)))
        ui.val_pool = d.vals[i].name;
    }
    ImGui::EndChild();
    return action;
  }
  ImGui::TextColored(kHead, "单因子 G20 多头");
  if (ImGui::IsItemHovered())
    tip("勾选 = 主图画该因子单独的 G20 多头净值\n"
        "• 带它自己的冲击 (括号里: Lob 最小金额段 / Bar 固定冲击)\n"
        "• 与组合同一区间起点归一; 年化按区间复利");
  for (size_t i = 0; i < d.factors.size(); ++i) {
    const PortfolioDerived::Factor &f = d.factors[i];
    const ImVec4 col = factor_color(d, i);
    char ann[32];
    std::snprintf(ann, sizeof(ann), "  %+.1f%%/年##pff%zu", v.ann_f[i], i);
    bool on = ui.factor_show[f.key];
    ImGui::PushStyleColor(ImGuiCol_CheckMark, col);
    ImGui::PushStyleColor(ImGuiCol_Text, col);
    if (ImGui::Checkbox((f.label + ann).c_str(), &on))
      ui.factor_show[f.key] = on;
    ImGui::PopStyleColor(2);
  }
  ImGui::EndChild();
  return action;
}

} // namespace GUI::Factors
