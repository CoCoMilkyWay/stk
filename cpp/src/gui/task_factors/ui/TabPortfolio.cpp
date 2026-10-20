// Factors→Portfolio — 见头文件
#include "gui/task_factors/ui/TabPortfolio.hpp"

#include "features/TimeIndex.hpp"             // L_1min_to_Clock / TRADE_MINUTES_PER_DAY (横轴刻度)
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
#include <set>

namespace GUI::Factors {

namespace {

using factor::stat::kLong;
using factor::stat::kShort;

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

// 横轴刻度: x = 日下标 · kDayW + 分钟 → YY/MM/DD HH:MM (user_data = 日并集轴)
int clock_formatter(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<misc::Ymd> *>(ud);
  if (dates.empty() || v < 0)
    return std::snprintf(buf, size, " ");
  const size_t d = std::min(static_cast<size_t>(v) / kDayW, dates.size() - 1);
  const ClockTime ct = L_1min_to_Clock(static_cast<size_t>(v) % kDayW);
  const misc::Ymd y = dates[d];
  return std::snprintf(buf, size, "%02d/%02d/%02d %02d:%02d", y / 10000 % 100, y / 100 % 100, y % 100, ct.hour, ct.minute);
}

// 区间条刻度: 只到日
int day_formatter(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<misc::Ymd> *>(ud);
  if (dates.empty() || v < 0)
    return std::snprintf(buf, size, " ");
  const misc::Ymd y = dates[std::min(static_cast<size_t>(v) / kDayW, dates.size() - 1)];
  return std::snprintf(buf, size, "%02d/%02d/%02d", y / 10000 % 100, y / 100 % 100, y % 100);
}

const ImVec4 kColLong(1.0f, 0.55f, 0.15f, 1.0f), kColLs(1.0f, 1.0f, 1.0f, 1.0f);
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

  // ---- 组合: 每日在当日有数据的因子间等分 ----
  std::vector<int> n_act(d.dates.size(), 0);
  for (const FactorCurve *f : fc)
    for (const misc::Ymd y : f->dates)
      ++n_act[day_of(y)];
  struct Ev {
    double x, l, ls, l0, ls0; // 带冲击 / 无冲击
  };
  std::vector<Ev> ev;
  for (size_t q = 0; q < fc.size(); ++q) {
    const FactorCurve *f = fc[q];
    const int S = f->seg;
    const factor::Geo g = factor::geo_of(S);
    const int h = factor::stat::hold_rows(f->hold, g);
    assert(!f->dates.empty() && f->lng.size() == f->dates.size() * static_cast<size_t>(S));
    // 每个新进者: 平仓 fixed_impact + 建仓 (Lob = 金额段冲击, 已在 wimp; Bar 无段 = 再一份 fixed_impact)
    const double ke = f->amt > 0 ? fixed_impact : 2.0 * fixed_impact;
    const float *wl = f->wimp_at(kLong), *el = f->we_at(kLong), *ws = f->wimp_at(kShort), *es = f->we_at(kShort);
    PortfolioDerived::Factor pf;
    pf.key = its[q]->e.key();
    pf.label = entry_label(its[q]->e) + (f->amt > 0 ? " (" + std::to_string(f->amt) + "w冲击)" : std::string(" (固定冲击)"));
    pf.tag = its[q]->name_en + " " + factor::stat::hold_name(f->hold);
    pf.c.x.push_back(static_cast<double>(day_of(f->dates.front()) * kDayW)), pf.c.y.push_back(1.0);
    double nav = 1.0;
    for (size_t dl = 0; dl < f->dates.size(); ++dl) {
      const size_t di = day_of(f->dates[dl]);
      for (int ts = 0; ts < S; ++ts) {
        const size_t t = dl * S + static_cast<size_t>(ts);
        const double cl = wl[t] + el[t] * ke, cs = ws[t] + es[t] * ke;
        const double x = static_cast<double>(di * kDayW + static_cast<size_t>(g.minute(ts)));
        const double rl = (f->lng[t] - cl) / h, rls = (f->ls[t] - cl - cs) / h;
        ev.push_back({x, rl / n_act[di], rls / n_act[di], f->lng[t] / h / n_act[di], f->ls[t] / h / n_act[di]});
        nav *= 1.0 + rl;
        pf.c.x.push_back(x), pf.c.y.push_back(nav);
      }
    }
    d.factors.push_back(std::move(pf));
  }
  std::sort(ev.begin(), ev.end(), [](const Ev &a, const Ev &b) { return a.x < b.x; });
  if (!d.dates.empty()) {
    d.l.x.push_back(0.0), d.l.y.push_back(1.0), d.ls.y.push_back(1.0), d.l0.y.push_back(1.0), d.ls0.y.push_back(1.0);
    double nl = 1.0, nls = 1.0, nl0 = 1.0, nls0 = 1.0;
    for (size_t i = 0; i < ev.size();) {
      double il = 0.0, ils = 0.0, il0 = 0.0, ils0 = 0.0;
      size_t j = i;
      for (; j < ev.size() && ev[j].x == ev[i].x; ++j)
        il += ev[j].l, ils += ev[j].ls, il0 += ev[j].l0, ils0 += ev[j].ls0;
      nl *= 1.0 + il, nls *= 1.0 + ils, nl0 *= 1.0 + il0, nls0 *= 1.0 + ils0;
      d.l.x.push_back(ev[i].x), d.l.y.push_back(nl), d.ls.y.push_back(nls), d.l0.y.push_back(nl0), d.ls0.y.push_back(nls0);
      i = j;
    }
    d.ls.x = d.l0.x = d.ls0.x = d.l.x;
  }

  // ---- 池线: 只在日并集轴范围内; 不在轴上的日的收益并进下一个轴上的日 ----
  // 图例 = 池名 | 中文名 | 年换手 (日均 × kDaysPerYear) 三列, 各列按最宽者补空格对齐
  float w_en = 0.f, w_cn = 0.f;
  for (const PoolDef &pd : feature_pools())
    w_en = std::max(w_en, ImGui::CalcTextSize(pd.name).x), w_cn = std::max(w_cn, ImGui::CalcTextSize(pd.name_cn).x);
  for (const PoolDef &pd : feature_pools()) {
    PortfolioDerived::Pool p;
    const auto pit = res.pools.find(pd.name);
    const PoolCurve *pc = pit != res.pools.end() && pit->second->error.empty() ? pit->second.get() : nullptr;
    char turn[48] = "";
    if (pc && pc->turnover >= 0.0)
      std::snprintf(turn, sizeof(turn), "  (年换手=%5.1f倍)", pc->turnover * factor::stat::kDaysPerYear);
    p.label = pad_to(pd.name, w_en) + "  " + pad_to(pd.name_cn, w_cn) + turn + "###pf_pool_" + pd.name;
    p.tag = pd.name;
    p.own = own.contains(pd.name);
    if (pc && !d.dates.empty()) {
      const PoolCurve &c = *pc;
      double base = 0.0;
      for (size_t i = 0; i < c.dates.size(); ++i) {
        if (c.dates[i] < d.dates.front() || c.dates[i] > d.dates.back())
          continue;
        const size_t di = day_of(c.dates[i]);
        if (p.c.x.empty()) {
          base = i > 0 ? c.index[i - 1] : c.index[i];
          p.c.x.push_back(static_cast<double>(di * kDayW)), p.c.y.push_back(1.0);
        }
        if (d.dates[di] != c.dates[i])
          continue;
        p.c.x.push_back(static_cast<double>(di * kDayW + kDayW - 1)), p.c.y.push_back(c.index[i] / base);
      }
    }
    d.pools.push_back(std::move(p));
  }
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

void make_view(const PortfolioDerived &d, int d0, int d1, PortfolioView &v) {
  const double x0 = static_cast<double>(static_cast<size_t>(d0) * kDayW), x1 = static_cast<double>(static_cast<size_t>(d1) * kDayW);
  rebase(d.l, x0, x1, v.l);
  rebase(d.ls, x0, x1, v.ls);
  rebase(d.l0, x0, x1, v.l0);
  rebase(d.ls0, x0, x1, v.ls0);
  v.ann_l = ann_of(v.l), v.ann_ls = ann_of(v.ls), v.ann_l0 = ann_of(v.l0), v.ann_ls0 = ann_of(v.ls0);
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
  if (c.x[i] < lim.X.Min)
    return;
  tags.push_back({ImPlot::PlotToPixels(c.x[i], c.y[i]), col, text});
}
// 按像素 y 排开不重叠, 贴点右侧 (出界则左移进图内)
void draw_tags(std::vector<EndTag> &tags) {
  std::sort(tags.begin(), tags.end(), [](const EndTag &a, const EndTag &b) { return a.at.y < b.at.y; });
  ImFont *font = ImGui::GetFont();
  const float fs = ImGui::GetFontSize() * 0.95f;
  for (size_t i = 1; i < tags.size(); ++i)
    tags[i].at.y = std::max(tags[i].at.y, tags[i - 1].at.y + fs);
  const float right = ImPlot::GetPlotPos().x + ImPlot::GetPlotSize().x;
  ImDrawList *dl = ImPlot::GetPlotDrawList();
  ImPlot::PushPlotClipRect();
  for (const EndTag &t : tags) {
    const ImVec2 sz = font->CalcTextSizeA(fs, FLT_MAX, 0.0f, t.text);
    dl->AddText(font, fs, ImVec2(std::min(t.at.x + 4.0f, right - sz.x - 2.0f), t.at.y - 0.5f * sz.y), t.col, t.text);
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
      ui.rx0 = 0.0, ui.rx1 = static_cast<double>(d.dates.size() * kDayW);
      refit = true;
    }
    ui.view_d0 = -1;
  }
  if (ui.view_d0 != ui.d0 || ui.view_d1 != ui.d1) {
    make_view(d, ui.d0, ui.d1, ui.view);
    ui.view_d0 = ui.d0, ui.view_d1 = ui.d1;
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
  ImGui::TextDisabled("带冲击");
  if (ImGui::IsItemHovered())
    tip("换手成本 = 各因子自己的最小冲击段, 不可选\n"
        "• 每行成本 = 两腿新进者 (t−h 不在该组) 的建仓 + 平仓冲击, 按持仓加权 (Stat 契约【换手成本】)\n"
        "• Lob 层: 建仓 = 最小金额段冲击 (封顶 max_impact), 平仓 = 固定冲击 %.1f bp\n"
        "• Bar 层 (无冲击段): 建仓 / 平仓都按固定冲击 %.1f bp\n"
        "• G20 多头扣多腿; LS 扣两腿",
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

  // ---- 图: 左 = 主图 (组合 + 池 + 单因子) / 区间条; 右 = 单因子图例 ----
  const std::string vis_key = [&] {
    std::string k = ui.name;
    for (const PortfolioDerived::Pool &p : d.pools)
      k += p.own ? "1" : "0";
    return k;
  }();
  const bool set_vis = vis_key != ui.vis_key;
  ui.vis_key = vis_key;
  if (refit || set_vis)
    ImPlot::SetNextAxesToFit();
  constexpr float kLegendW = 320.0f, kBarH = 90.0f;
  const ImGuiStyle &st = ImGui::GetStyle();
  const ImVec2 av = ImGui::GetContentRegionAvail();
  const float pw = av.x - kLegendW - st.ItemSpacing.x;
  ImGui::BeginGroup();
  if (ImPlot::BeginPlot("##portfolio", ImVec2(pw, av.y - kBarH - st.ItemSpacing.y))) {
    // 线性: y 下限钉在 0 (净值恒正), fit / 缩放只动上限; log: 正常
    ImPlot::SetupAxes(nullptr, "净值 (复利, 区间起点 = 1)", ImPlotAxisFlags_None, ui.log_y ? ImPlotAxisFlags_None : ImPlotAxisFlags_LockMin);
    ImPlot::SetupAxisFormat(ImAxis_X1, clock_formatter, &d.dates);
    if (ui.log_y)
      ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    else
      ImPlot::GetCurrentPlot()->Axes[ImAxis_Y1].SetMin(0.0, /*force=*/true);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    std::vector<EndTag> tags;
    std::string shown; // 各线显隐 + log: 与上帧不同 → 下帧重新 fit
    const auto legend_shown = [&](const char *label) {
      const ImPlotItem *it = ImPlot::GetItem(label);
      assert(it);
      shown += it->Show ? '1' : '0';
      return it->Show ? it : nullptr;
    };
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
    char lb[96];
    std::snprintf(lb, sizeof(lb), "G20 多头 (带冲击)  %+.1f%%/年###pf_long", v.ann_l);
    ImPlot::SetNextLineStyle(kColLong, 2.0f);
    ImPlot::PlotLine(lb, v.l.x.data(), v.l.y.data(), static_cast<int>(v.l.x.size()));
    if (legend_shown(lb))
      push_tag(tags, lim, v.l, ImGui::ColorConvertFloat4ToU32(kColLong), "G20");
    std::snprintf(lb, sizeof(lb), "LS (带冲击)  %+.1f%%/年###pf_ls", v.ann_ls);
    if (set_vis) // 策略线默认只显 G20 多头 (带冲击)
      ImPlot::HideNextItem(true, ImPlotCond_Always);
    ImPlot::SetNextLineStyle(kColLs, 2.0f);
    ImPlot::PlotLine(lb, v.ls.x.data(), v.ls.y.data(), static_cast<int>(v.ls.x.size()));
    if (legend_shown(lb))
      push_tag(tags, lim, v.ls, ImGui::ColorConvertFloat4ToU32(kColLs), "LS");
    const ImVec4 col_l0(kColLong.x, kColLong.y, kColLong.z, 0.5f), col_ls0(kColLs.x, kColLs.y, kColLs.z, 0.5f);
    std::snprintf(lb, sizeof(lb), "G20 多头 (无冲击)  %+.1f%%/年###pf_long0", v.ann_l0);
    if (set_vis)
      ImPlot::HideNextItem(true, ImPlotCond_Always);
    ImPlot::SetNextLineStyle(col_l0, 1.0f);
    ImPlot::PlotLine(lb, v.l0.x.data(), v.l0.y.data(), static_cast<int>(v.l0.x.size()));
    if (legend_shown(lb))
      push_tag(tags, lim, v.l0, ImGui::ColorConvertFloat4ToU32(col_l0), "G20 无冲击");
    std::snprintf(lb, sizeof(lb), "LS (无冲击)  %+.1f%%/年###pf_ls0", v.ann_ls0);
    if (set_vis)
      ImPlot::HideNextItem(true, ImPlotCond_Always);
    ImPlot::SetNextLineStyle(col_ls0, 1.0f);
    ImPlot::PlotLine(lb, v.ls0.x.data(), v.ls0.y.data(), static_cast<int>(v.ls0.x.size()));
    if (legend_shown(lb))
      push_tag(tags, lim, v.ls0, ImGui::ColorConvertFloat4ToU32(col_ls0), "LS 无冲击");
    shown += ui.log_y ? 'L' : 'l';
    if (shown != ui.shown_key) {
      ui.shown_key = std::move(shown);
      ui.refit = true;
    }
    draw_tags(tags);
    ImPlot::EndPlot();
  }

  // ---- 区间条: 全段组合 LS; 矩形 = 选定日段, 松手吸附到日后生效 (各线在新起点归一) ----
  const int D = static_cast<int>(d.dates.size());
  if (ImPlot::BeginPlot("##pf_range", ImVec2(pw, kBarH),
                        ImPlotFlags_NoTitle | ImPlotFlags_NoLegend | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoMouseText)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_None, ImPlotAxisFlags_NoDecorations | ImPlotAxisFlags_AutoFit);
    if (D > 0)
      ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, static_cast<double>(d.dates.size() * kDayW), ImPlotCond_Always);
    ImPlot::SetupAxisFormat(ImAxis_X1, day_formatter, &d.dates);
    if (ui.log_y)
      ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    ImPlot::SetNextLineStyle(kColLs, 1.0f);
    ImPlot::PlotLine("##pf_range_ls", d.ls.x.data(), d.ls.y.data(), static_cast<int>(d.ls.x.size()));
    if (D > 0) {
      const ImPlotRect lim = ImPlot::GetPlotLimits();
      double y0 = lim.Y.Min, y1 = lim.Y.Max;
      bool hovered = false, held = false;
      ImPlot::DragRect(0, &ui.rx0, &y0, &ui.rx1, &y1, ImVec4(0.4f, 0.8f, 1.0f, 1.0f), ImPlotDragToolFlags_NoFit, nullptr, &hovered, &held);
      if (!held) {
        const int a = std::clamp(static_cast<int>(std::lround(std::min(ui.rx0, ui.rx1) / kDayW)), 0, D - 1);
        const int b = std::clamp(static_cast<int>(std::lround(std::max(ui.rx0, ui.rx1) / kDayW)), a + 1, D);
        if (a != ui.d0 || b != ui.d1) {
          ui.d0 = a, ui.d1 = b;
          ui.refit = true;
        }
        ui.rx0 = static_cast<double>(static_cast<size_t>(a) * kDayW), ui.rx1 = static_cast<double>(static_cast<size_t>(b) * kDayW);
        if (hovered)
          tip("区间 %d/%02d/%02d ~ %d/%02d/%02d (%d 日)\n"
              "• 拖中心点平移, 拖左右边改起止; 松手吸附到日\n"
              "• 双击左 / 右边 = 拉到头",
              d.dates[ui.d0] / 10000, d.dates[ui.d0] / 100 % 100, d.dates[ui.d0] % 100, d.dates[ui.d1 - 1] / 10000, d.dates[ui.d1 - 1] / 100 % 100,
              d.dates[ui.d1 - 1] % 100, ui.d1 - ui.d0);
      }
    }
    ImPlot::EndPlot();
  }
  ImGui::EndGroup();

  // ---- 右图例: 单因子 G20 多头 (默认全关) ----
  ImGui::SameLine();
  ImGui::BeginChild("##pf_flegend", ImVec2(0, av.y), ImGuiChildFlags_Borders);
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
