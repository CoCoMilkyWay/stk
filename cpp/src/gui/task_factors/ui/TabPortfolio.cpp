// Factors→Portfolio — 见头文件
#include "gui/task_factors/ui/TabPortfolio.hpp"

#include "features/TimeIndex.hpp"             // L_1min_to_Clock / TRADE_MINUTES_PER_DAY (横轴刻度)
#include "gui/Tasks.hpp"                      // StatusColor
#include "gui/task_factors/ui/TabFactors.hpp" // tip
#include "shared/FeatureLib.hpp"              // feature_pools

#include "imgui.h"
#include "implot.h"

#include <algorithm>
#include <cassert>
#include <cfloat>
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

// 组合聚合 + 池线 (见头文件). amt ≤ 0 = 毛
void derive(const PortfolioUIState &ui, const PortfolioResult &res, int amt, double sell, PortfolioDerived &d) {
  d = PortfolioDerived{};
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
    fc.push_back(f->second.get());
  }
  {
    std::set<int> as;
    for (const FactorCurve *f : fc)
      as.insert(f->amts.begin(), f->amts.end());
    d.amts.assign(as.begin(), as.end());
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
    double x, l, ls;
  };
  std::vector<Ev> ev;
  for (const FactorCurve *f : fc) {
    const int S = f->seg;
    const factor::Geo g = factor::geo_of(S);
    const int h = factor::stat::hold_rows(f->hold, g);
    assert(f->lng.size() == f->dates.size() * static_cast<size_t>(S));
    const auto ait = std::find(f->amts.begin(), f->amts.end(), amt);
    const int k = amt > 0 && ait != f->amts.end() ? static_cast<int>(ait - f->amts.begin()) : -1;
    const float *wl = k >= 0 ? f->wimp_at(k, kLong) : nullptr, *el = k >= 0 ? f->we_at(k, kLong) : nullptr;
    const float *ws = k >= 0 ? f->wimp_at(k, kShort) : nullptr, *es = k >= 0 ? f->we_at(k, kShort) : nullptr;
    for (size_t dl = 0; dl < f->dates.size(); ++dl) {
      const size_t di = day_of(f->dates[dl]);
      const double w = 1.0 / (static_cast<double>(h) * n_act[di]);
      for (int ts = 0; ts < S; ++ts) {
        const size_t t = dl * S + static_cast<size_t>(ts);
        const double cl = wl ? wl[t] + el[t] * sell : 0.0, cs = ws ? ws[t] + es[t] * sell : 0.0;
        ev.push_back({static_cast<double>(di * kDayW + static_cast<size_t>(g.minute(ts))), (f->lng[t] - cl) * w, (f->ls[t] - cl - cs) * w});
      }
    }
  }
  std::sort(ev.begin(), ev.end(), [](const Ev &a, const Ev &b) { return a.x < b.x; });
  if (!d.dates.empty()) {
    d.x.push_back(0.0), d.cum_l.push_back(0.0), d.cum_ls.push_back(0.0);
    double cl = 0.0, cls = 0.0;
    for (size_t i = 0; i < ev.size();) {
      size_t j = i;
      for (; j < ev.size() && ev[j].x == ev[i].x; ++j)
        cl += ev[j].l, cls += ev[j].ls;
      d.x.push_back(ev[i].x), d.cum_l.push_back(100.0 * cl), d.cum_ls.push_back(100.0 * cls);
      i = j;
    }
    const double ann = static_cast<double>(factor::stat::kDaysPerYear) / static_cast<double>(d.dates.size());
    d.ann_l = static_cast<float>(d.cum_l.back() * ann), d.ann_ls = static_cast<float>(d.cum_ls.back() * ann);
  }

  // ---- 池线: 只在日并集轴范围内; 不在轴上的日的收益并进下一个轴上的日 ----
  for (const PoolDef &pd : feature_pools()) {
    PortfolioDerived::Pool p;
    p.label = std::string(pd.name) + " " + pd.name_cn;
    p.own = own.contains(pd.name);
    if (const auto it = res.pools.find(pd.name); it != res.pools.end() && it->second->error.empty() && !d.dates.empty()) {
      const PoolCurve &c = *it->second;
      double cum = 0.0, base = 0.0;
      for (size_t i = 0; i < c.dates.size(); ++i) {
        if (c.dates[i] < d.dates.front() || c.dates[i] > d.dates.back())
          continue;
        const size_t di = day_of(c.dates[i]);
        if (p.x.empty()) {
          base = i > 0 ? c.index[i - 1] : c.index[i];
          p.x.push_back(static_cast<double>(di * kDayW)), p.y.push_back(0.0);
        }
        if (d.dates[di] != c.dates[i])
          continue;
        cum += c.index[i] / base - 1.0;
        base = c.index[i];
        p.x.push_back(static_cast<double>(di * kDayW + kDayW - 1)), p.y.push_back(100.0 * cum);
      }
    }
    d.pools.push_back(std::move(p));
  }
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

int RenderTabPortfolio(PortfolioService &svc, PortfolioUIState &ui, double sell_impact) {
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
  bool refit = false;
  if (gen != ui.der_gen || ui.items_gen != ui.der_items || ui.amt != ui.der_amt || sell_impact != ui.der_sell) {
    const std::vector<misc::Ymd> old = d.dates;
    derive(ui, res, ui.amt, sell_impact, d);
    ui.der_gen = gen, ui.der_items = ui.items_gen, ui.der_amt = ui.amt, ui.der_sell = sell_impact;
    refit = d.dates != old;
  }
  if (ui.amt == -1 && !d.amts.empty()) { // 默认最小非零金额段 (下一帧按它重派生)
    const auto it = std::find_if(d.amts.begin(), d.amts.end(), [](int a) { return a > 0; });
    if (it != d.amts.end())
      ui.amt = *it;
  }

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
  ImGui::SetNextItemWidth(80);
  {
    char cur[24];
    std::snprintf(cur, sizeof(cur), ui.amt > 0 ? "%dw" : "毛", ui.amt);
    if (ImGui::BeginCombo("Cost", cur)) {
      if (ImGui::Selectable("毛", ui.amt <= 0))
        ui.amt = 0;
      for (const int a : d.amts) {
        char lb[24];
        std::snprintf(lb, sizeof(lb), "%dw", a);
        if (ImGui::Selectable(lb, ui.amt == a))
          ui.amt = a;
      }
      ImGui::EndCombo();
    }
  }
  if (ImGui::IsItemHovered())
    tip("换手成本的下单金额段 (万)\n"
        "• 每行成本 = 两腿新进者 (t−h 不在该组) 的建仓冲击 + sell_impact %.4f, 按持仓加权 (Stat 契约【换手成本】)\n"
        "• G20 多头扣多腿; LS 扣两腿\n"
        "• Bar 层 (5min .. day) 没有冲击成本段, 恒为毛",
        sell_impact);
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

  // ---- 图: 组合两条线 + 全部池指数 ----
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
  if (ImPlot::BeginPlot("##portfolio", ImVec2(-1, -1))) {
    ImPlot::SetupAxes(nullptr, "Σ (ret − cost) / h  (%)");
    ImPlot::SetupAxisFormat(ImAxis_X1, clock_formatter, &d.dates);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
    for (const PortfolioDerived::Pool &p : d.pools) {
      if (set_vis)
        ImPlot::HideNextItem(!p.own, ImPlotCond_Always);
      ImPlot::PlotLine(p.label.c_str(), p.x.data(), p.y.data(), static_cast<int>(p.x.size()));
    }
    char lb[64];
    const int n = static_cast<int>(d.x.size());
    std::snprintf(lb, sizeof(lb), "G20 多头  %+.1f%%/年###pf_long", d.ann_l);
    ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.55f, 0.15f, 1.0f), 2.0f);
    ImPlot::PlotLine(lb, d.x.data(), d.cum_l.data(), n);
    std::snprintf(lb, sizeof(lb), "LS  %+.1f%%/年###pf_ls", d.ann_ls);
    ImPlot::SetNextLineStyle(ImVec4(1.0f, 1.0f, 1.0f, 1.0f), 2.0f);
    ImPlot::PlotLine(lb, d.x.data(), d.cum_ls.data(), n);
    ImPlot::EndPlot();
  }
  return action;
}

} // namespace GUI::Factors
