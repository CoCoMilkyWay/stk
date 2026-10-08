// Tab Inspect — 见头文件
#include "gui/task_factors/ui/TabInspect.hpp"
#include "gui/Tasks.hpp" // StatusColor

#include "imgui.h"
#include "implot.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <limits>
#include <mutex>
#include <string>

namespace GUI::Factors {

namespace {

constexpr int K = factor::stat::kGroups;

// 横轴刻度: 交易日下标 → YYMMDD (user_data = InspectDerived::dates)
int date_formatter(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<std::string> *>(ud);
  if (dates.empty())
    return std::snprintf(buf, size, "%g", v);
  const int i = std::clamp(static_cast<int>(std::floor(v)), 0, static_cast<int>(dates.size()) - 1);
  return std::snprintf(buf, size, "%s", dates[static_cast<size_t>(i)].c_str() + 2);
}

ImVec4 group_color(int k) { return ImPlot::SampleColormap(static_cast<float>(k) / static_cast<float>(K - 1), ImPlotColormap_Jet); }

// 一级 Row[T] (选定 amt × hold) → 日级序列. 累计按 1/h 折算: 相邻 h 行的标签是同一段收益, Σ_t r_t / h = h 个相位非重叠链的平均
void derive(const InspectResult &res, int sel, bool absolute, InspectDerived &d) {
  d = InspectDerived{};
  if (res.key.empty() || !res.error.empty() || res.rows.empty())
    return;
  assert(sel >= 0 && sel < res.hd.n);
  const int T = res.scope.T, days = res.scope.days, S = factor::kSegLen;
  assert(T == days * S);
  const int hold = res.hd.h[sel];
  const double h = factor::stat::hold_minutes(hold);
  const factor::stat::Row *r = res.rows.data() + static_cast<size_t>(sel) * T;
  d.days = days;
  d.dates = res.dates;
  d.x_day.resize(static_cast<size_t>(days) + 1);
  for (int i = 0; i <= days; ++i)
    d.x_day[static_cast<size_t>(i)] = static_cast<float>(i);
  for (int k = 0; k < K; ++k)
    d.grp_cum[k].assign(static_cast<size_t>(days) + 1, 0.f);
  d.ls_cum.assign(static_cast<size_t>(days) + 1, 0.f);
  d.x_mid.resize(static_cast<size_t>(days));
  d.ic_day.resize(static_cast<size_t>(days));
  d.ic_cum.resize(static_cast<size_t>(days));
  double gc[K] = {}, lc = 0.0, icc = 0.0;
  for (int day = 0; day < days; ++day) {
    double g[K] = {}, ls = 0.0, ic = 0.0;
    int nic = 0;
    for (int t = day * S; t < (day + 1) * S; ++t) {
      const factor::stat::Row &w = r[t];
      if (!w.ok)
        continue;
      ++nic;
      ic += w.ic;
      ls += w.ls;
      for (int k = 0; k < K; ++k)
        if (w.cnt[k]) // TS 口径组可空: 该刻无仓, 贡献 0
          g[k] += w.grp[k] / w.cnt[k] + (absolute ? w.mkt : 0.0);
    }
    for (int k = 0; k < K; ++k) {
      gc[k] += g[k] / h;
      d.grp_cum[k][static_cast<size_t>(day) + 1] = static_cast<float>(gc[k]);
    }
    lc += ls / h;
    d.ls_cum[static_cast<size_t>(day) + 1] = static_cast<float>(lc);
    const double icm = nic ? ic / nic : 0.0;
    icc += icm;
    d.x_mid[static_cast<size_t>(day)] = static_cast<float>(day) + 0.5f;
    d.ic_day[static_cast<size_t>(day)] = nic ? static_cast<float>(icm) : std::numeric_limits<float>::quiet_NaN();
    d.ic_cum[static_cast<size_t>(day)] = static_cast<float>(icc);
  }
  d.hs = res.hold[sel / res.n_hold][sel % res.n_hold];
  d.valid = true;
}

// 空图占位 (统一样式)
void empty_plot(const char *title, const ImVec2 &size, const char *hint) {
  if (ImPlot::BeginPlot(title, size, ImPlotFlags_NoLegend | ImPlotFlags_NoMenus)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoGridLines, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoGridLines);
    ImPlot::SetupAxesLimits(0, 1, 0, 1, ImPlotCond_Always);
    ImPlot::PlotText(hint, 0.5, 0.5);
    ImPlot::EndPlot();
  }
}

// 左上: 分层累计 + LS
void plot_layers(const InspectDerived &d, bool absolute, const ImVec2 &size) {
  char title[128];
  std::snprintf(title, sizeof(title), "分层累计 %s  h=%s  mono %+.3f###layers", absolute ? "(绝对 lv)" : "(超额 lv − mkt)",
                factor::stat::hold_name(d.hs.hold).c_str(), d.hs.mono);
  if (!ImPlot::BeginPlot(title, size, ImPlotFlags_NoMenus))
    return;
  ImPlot::SetupAxes(nullptr, absolute ? "Σ ret / h" : "Σ excess / h", ImPlotAxisFlags_None, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupAxisFormat(ImAxis_X1, date_formatter, const_cast<std::vector<std::string> *>(&d.dates));
  ImPlot::SetupAxisLimits(ImAxis_X1, 0, std::max(1, d.days), ImPlotCond_Always);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  const int n = d.days + 1;
  ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 1.2f);
  for (int k = 0; k < K; ++k) {
    ImPlot::SetNextLineStyle(group_color(k));
    char id[16];
    std::snprintf(id, sizeof(id), "##g%d", k);
    ImPlot::PlotLine(id, d.x_day.data(), d.grp_cum[k].data(), n);
  }
  ImPlot::PopStyleVar();
  char ls[64];
  std::snprintf(ls, sizeof(ls), "LS  SR %+.2f  t %+.2f###ls", d.hs.sharpe, d.hs.ls_t);
  ImPlot::SetNextLineStyle(ImVec4(1.f, 1.f, 1.f, 1.f), 2.5f);
  ImPlot::PlotLine(ls, d.x_day.data(), d.ls_cum.data(), n);
  // 末端标 G20 (top) / G1 (bottom)
  const float xe = static_cast<float>(d.days);
  ImPlot::Annotation(xe, d.grp_cum[K - 1].back(), group_color(K - 1), ImVec2(-8, 0), true, "G%d", K);
  ImPlot::Annotation(xe, d.grp_cum[0].back(), group_color(0), ImVec2(-8, 0), true, "G1");
  if (ImPlot::IsPlotHovered()) { // 鼠标所在日: 各组 / LS 累计值
    const ImPlotPoint mp = ImPlot::GetPlotMousePos();
    const int di = std::clamp(static_cast<int>(std::floor(mp.x)), 0, d.days - 1);
    ImGui::BeginTooltip();
    ImGui::Text("%s  (day %d)", d.dates[static_cast<size_t>(di)].c_str(), di + 1);
    ImGui::Text("LS  %+.5f", d.ls_cum[static_cast<size_t>(di) + 1]);
    ImGui::Separator();
    for (int k = K - 1; k >= 0; --k)
      ImGui::TextColored(group_color(k), "G%-2d %+.5f", k + 1, d.grp_cum[k][static_cast<size_t>(di) + 1]);
    ImGui::EndTooltip();
  }
  ImPlot::EndPlot();
}

// 右上: 期限结构 (全部持有期的 Top / Bottom / LS 超额均值). hs[n_hold] 来自 Inspect 结果或文件 stat
void plot_term(const factor::stat::HoldStat *hs, int n_hold, int sel_hold, const char *source, const ImVec2 &size) {
  char title[128];
  std::snprintf(title, sizeof(title), "期限结构: 超额均值 / 持有期  (%s)###term", source);
  if (!ImPlot::BeginPlot(title, size, ImPlotFlags_NoMenus))
    return;
  std::vector<double> pos(static_cast<size_t>(n_hold));
  std::vector<std::string> names(static_cast<size_t>(n_hold));
  std::vector<const char *> labels(static_cast<size_t>(n_hold));
  std::vector<double> vals(3 * static_cast<size_t>(n_hold), 0.0); // [item][group]: Top / Bottom / LS
  for (int i = 0; i < n_hold; ++i) {
    pos[static_cast<size_t>(i)] = i;
    names[static_cast<size_t>(i)] = factor::stat::hold_name(hs[i].hold);
    labels[static_cast<size_t>(i)] = names[static_cast<size_t>(i)].c_str();
    if (hs[i].n < 3)
      continue;
    vals[static_cast<size_t>(i)] = hs[i].grp[K - 1];
    vals[static_cast<size_t>(n_hold + i)] = hs[i].grp[0];
    vals[static_cast<size_t>(2 * n_hold + i)] = hs[i].ls_mean;
  }
  ImPlot::SetupAxes("hold", "mean excess", ImPlotAxisFlags_None, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupAxisTicks(ImAxis_X1, pos.data(), n_hold, labels.data());
  ImPlot::SetupAxisLimits(ImAxis_X1, -0.6, n_hold - 0.4, ImPlotCond_Always);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  const char *items[3] = {"Top (G20 long)", "Bottom (G1 long)", "LS"};
  ImPlot::PushColormap(ImPlotColormap_Deep);
  ImPlot::PlotBarGroups(items, vals.data(), 3, n_hold, 0.7);
  ImPlot::PopColormap();
  if (sel_hold >= 0 && sel_hold < n_hold)
    ImPlot::TagX(static_cast<double>(sel_hold), ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "sel");
  if (ImPlot::IsPlotHovered()) {
    const ImPlotPoint mp = ImPlot::GetPlotMousePos();
    const int i = static_cast<int>(std::lround(mp.x));
    if (i >= 0 && i < n_hold) {
      const factor::stat::HoldStat &h = hs[i];
      ImGui::BeginTooltip();
      if (h.n < 3)
        ImGui::TextDisabled("h=%s  n=%d (不足)", labels[static_cast<size_t>(i)], h.n);
      else
        ImGui::Text("h=%-5s n=%d/%d  rIC %+.4f IR %+.3f t %+.2f pos %.2f | LS %+.5f t %+.2f pos %.2f SR %+.2f β %+.3f | mono %+.3f | rAC %+.3f\n"
                    "Top %+.5f  Bottom %+.5f",
                    labels[static_cast<size_t>(i)], h.n, h.n_ac, h.ic_mean, h.icir, h.ic_t, h.ic_pos, h.ls_mean, h.ls_t, h.ls_pos, h.sharpe,
                    h.beta, h.mono, h.rank_ac, h.grp[K - 1], h.grp[0]);
      ImGui::EndTooltip();
    }
  }
  ImPlot::EndPlot();
}

// 左下: 日均 IC 柱 + 累计 IC 线 (Y2)
void plot_ic(const InspectDerived &d, const ImVec2 &size) {
  char title[160];
  std::snprintf(title, sizeof(title), "IC 时序 (日均 rank IC)  mean %+.4f  IR %+.3f  t %+.2f  pos %.2f  n=%d###ic", d.hs.ic_mean, d.hs.icir, d.hs.ic_t,
                d.hs.ic_pos, d.hs.n);
  if (!ImPlot::BeginPlot(title, size, ImPlotFlags_NoMenus))
    return;
  ImPlot::SetupAxes(nullptr, "IC / day", ImPlotAxisFlags_None, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupAxis(ImAxis_Y2, "Σ IC", ImPlotAxisFlags_AuxDefault | ImPlotAxisFlags_AutoFit);
  ImPlot::SetupAxisFormat(ImAxis_X1, date_formatter, const_cast<std::vector<std::string> *>(&d.dates));
  ImPlot::SetupAxisLimits(ImAxis_X1, 0, std::max(1, d.days), ImPlotCond_Always);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
  ImPlot::SetNextFillStyle(ImVec4(0.4f, 0.7f, 1.0f, 0.8f));
  ImPlot::PlotBars("IC", d.x_mid.data(), d.ic_day.data(), d.days, 1.0);
  ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
  ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.75f, 0.3f, 1.0f), 2.0f);
  ImPlot::PlotLine("Σ IC", d.x_mid.data(), d.ic_cum.data(), d.days);
  if (ImPlot::IsPlotHovered()) {
    const ImPlotPoint mp = ImPlot::GetPlotMousePos();
    const int di = std::clamp(static_cast<int>(std::floor(mp.x)), 0, d.days - 1);
    ImGui::SetTooltip("%s  IC %+.4f  ΣIC %+.3f", d.dates[static_cast<size_t>(di)].c_str(), d.ic_day[static_cast<size_t>(di)],
                      d.ic_cum[static_cast<size_t>(di)]);
  }
  ImPlot::EndPlot();
}

} // namespace

int RenderTabInspect(FactorsService &fsvc, InspectService &isvc, const FactorsUIState &fui, InspectUIState &ui, const FactorsUIContext &ctx) {
  int action = 0;
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Factor:");
  ImGui::SameLine();
  if (fui.view_file.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "(无) 在 Factors 页点一行高光");
    return 0;
  }
  // 短锁拷该行 (rows 由 worker 重扫时整体替换)
  FactorRow row;
  bool found = false;
  {
    std::lock_guard<std::mutex> lock(fsvc.mutex);
    for (const FactorRow &r : fsvc.rows)
      if (r.file == fui.view_file) {
        row = r;
        found = true;
        break;
      }
  }
  if (!found) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s (重扫中 / 已不在)", fui.view_file.c_str());
    return 0;
  }
  ImGui::Text("%s/%s", ctx.factor_dir.c_str(), row.file.c_str());
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  if (!row.error.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "BROKEN: %s", row.error.c_str());
    return 0;
  }
  ImGui::Text("[%s] %s", factor::stat::frame_name(row.frame), row.expr.c_str());
  if (!row.note.empty())
    ImGui::TextDisabled("%s", row.note.c_str());

  // ---- 控件行 ----
  const FeatureTable &ft = isvc.feats();
  const InspectStatus st = isvc.status();
  const bool busy = st == InspectStatus::Loading || st == InspectStatus::Running;
  const int n_amt = static_cast<int>(ft.amts.size()), n_hold = static_cast<int>(ft.labels.size());
  ui.amt_idx = std::clamp(ui.amt_idx, 0, std::max(0, n_amt - 1));
  ui.hold_idx = std::clamp(ui.hold_idx, 0, std::max(0, n_hold - 1));
  const int cur_amt = n_amt ? ft.amts[static_cast<size_t>(ui.amt_idx)] : 0;
  const int cur_hold = n_hold ? ft.labels[static_cast<size_t>(ui.hold_idx)].hold : 0;
  ImGui::SetNextItemWidth(70);
  if (ImGui::BeginCombo("Amt", n_amt ? (std::to_string(cur_amt) + "w").c_str() : "-")) {
    for (int i = 0; i < n_amt; ++i)
      if (ImGui::Selectable((std::to_string(ft.amts[static_cast<size_t>(i)]) + "w").c_str(), i == ui.amt_idx))
        ui.amt_idx = i;
    ImGui::EndCombo();
  }
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  if (ImGui::BeginCombo("Hold", n_hold ? factor::stat::hold_name(cur_hold).c_str() : "-")) {
    for (int i = 0; i < n_hold; ++i)
      if (ImGui::Selectable(factor::stat::hold_name(ft.labels[static_cast<size_t>(i)].hold).c_str(), i == ui.hold_idx))
        ui.hold_idx = i;
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("分层累计 / IC 时序 / 顶部 stat 看哪个持有期 (只影响显示; 一次算全部 amt × hold)");
  ImGui::SameLine();
  if (ImGui::Button(ui.absolute ? "绝对收益" : "超额收益", ImVec2(90, 0)))
    ui.absolute = !ui.absolute;
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("左上分层累计的口径: 超额 = 组均 (lv − mkt), 围着 0 看形状; 绝对 = 组均 lv, 含市场\nLS = top(lv) + bottom(sv) 对市场本就中性, 两种口径同一条");
  ImGui::SameLine();
  const bool can_run = !busy && ctx.axis_ready && n_hold > 0;
  if (!can_run)
    ImGui::BeginDisabled();
  if (ImGui::Button("Compute", ImVec2(80, 0))) {
    action = 1;
    ui.req_row = row;
    ui.req_reload = true;
  }
  if (!can_run)
    ImGui::EndDisabled();
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip(ctx.axis_ready ? "弃缓存整体重读特征库 + 算这一个因子 (CPU) + Stat 留一级 Row[H][T]; 换因子会自动起算 (只补缺的特征)"
                                     : "资产轴未就绪 (先在 Database 页扫描), 读不了特征库");
  ImGui::SameLine();
  if (!busy)
    ImGui::BeginDisabled();
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    action = -1;
  if (!busy)
    ImGui::EndDisabled();
  ImGui::SameLine();
  ImGui::Text("Status:");
  ImGui::SameLine();
  const int done = isvc.done(), total = isvc.total();
  switch (st) {
  case InspectStatus::Idle:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
    break;
  case InspectStatus::Loading:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "loading %d/%d days", done, total);
    break;
  case InspectStatus::Running:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "running %d/%d nodes", done, total);
    break;
  case InspectStatus::Done:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "done");
    break;
  case InspectStatus::Cancelled:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelled %d/%d", done, total);
    break;
  }

  // ---- 结果快照 (持锁派生, 不拷 rows) ----
  const InspectRequest probe{row.file, row.expr, row.frame, {}, ctx.universe, ctx.start_date, ctx.end_date, false};
  const std::string key = probe.key();
  const int sel = ui.amt_idx * n_hold + ui.hold_idx;
  std::string message, res_error, res_key;
  StatScope res_scope;
  float res_valid = 0.f;
  double res_ms = 0.0;
  factor::stat::HoldStat res_holds[factor::stat::kMaxHold]; // 选定 amt 的全部持有期 (期限结构)
  bool res_has = false;
  {
    std::lock_guard<std::mutex> lock(isvc.mutex);
    message = isvc.message;
    const InspectResult &res = isvc.result;
    res_key = res.key;
    res_error = res.error;
    if (!res.key.empty() && res.error.empty()) {
      assert(res.n_hold == n_hold && res.n_amt == n_amt && "Inspect 与 FeatureTable 的标签组不一致");
      res_has = true;
      res_scope = res.scope;
      res_valid = res.valid_pct;
      res_ms = res.eval_ms;
      for (int i = 0; i < n_hold; ++i)
        res_holds[i] = res.hold[ui.amt_idx][i];
    }
    const uint64_t ep = isvc.epoch();
    if (ep != ui.derived_epoch || sel != ui.derived_sel || ui.absolute != ui.derived_abs) {
      ui.derived_epoch = ep;
      ui.derived_sel = sel;
      ui.derived_abs = ui.absolute;
      if (res_has && sel < res.hd.n)
        derive(res, sel, ui.absolute, ui.der);
      else
        ui.der = InspectDerived{};
    }
  }
  if (!message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "| %s", message.c_str());
  }
  // 自动起算: 高光因子 / 作用域变了 且 没在算 (取消过的 key 不反复起; Compute 手动)
  if (action == 0 && !busy && can_run && res_key != key && ui.last_req_key != key) {
    action = 1;
    ui.req_row = row;
    ui.req_reload = false;
  }
  if (action == 1)
    ui.last_req_key = key;

  // ---- 作用域 + 选定持有期的 stat 行 ----
  const bool match = res_has && res_key == key;
  if (!res_error.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "算不了: %s", res_error.c_str());
  } else if (match) {
    ImGui::TextDisabled("%s  %s..%s  %d 天 × %d 资产 (T=%d)  %s %s | valid %.2f%%  eval %.1f ms", res_scope.universe.c_str(),
                        res_scope.start_date.c_str(), res_scope.end_date.c_str(), res_scope.days, res_scope.A, res_scope.T, res_scope.backend.c_str(),
                        res_scope.time.c_str(), res_valid, res_ms);
    const factor::stat::HoldStat &h = ui.der.hs;
    if (ui.der.valid && h.n >= 3)
      ImGui::Text("h=%-5s n=%d/%d  rIC %+.4f std %.4f IR %+.3f t %+.2f pos %.2f skew %+.2f kurt %+.2f | LS %+.5f t %+.2f pos %.2f SR %+.2f β %+.3f "
                  "| mono %+.3f | rAC %+.3f",
                  factor::stat::hold_name(h.hold).c_str(), h.n, h.n_ac, h.ic_mean, h.ic_std, h.icir, h.ic_t, h.ic_pos, h.ic_skew, h.ic_kurt, h.ls_mean,
                  h.ls_t, h.ls_pos, h.sharpe, h.beta, h.mono, h.rank_ac);
    else
      ImGui::TextDisabled("h=%s  n=%d (有效行不足, 无 stat)", factor::stat::hold_name(cur_hold).c_str(), h.n);
  } else {
    ImGui::TextDisabled("%s", busy ? "计算中…" : (ctx.axis_ready ? "尚未计算 (自动起算 / Compute)" : "资产轴未就绪, 先在 Database 页扫描"));
  }
  ImGui::Separator();

  // ---- 2×2 图 ----
  const ImVec2 avail = ImGui::GetContentRegionAvail();
  const float sp = ImGui::GetStyle().ItemSpacing.x;
  const ImVec2 cell(std::max(100.f, (avail.x - sp) * 0.5f), std::max(100.f, (avail.y - ImGui::GetStyle().ItemSpacing.y) * 0.5f));
  const char *wait_hint = busy ? "计算中…" : "Compute 后可见";

  if (match && ui.der.valid)
    plot_layers(ui.der, ui.absolute, cell);
  else
    empty_plot("分层累计###layers", cell, wait_hint);
  ImGui::SameLine();
  // 期限结构: Inspect 结果优先, 否则文件 stat (Factors 表的 row.hold)
  if (match)
    plot_term(res_holds, n_hold, ui.hold_idx, "Inspect", cell);
  else if (row.has_stat && row.n_hold == n_hold && ui.amt_idx < row.n_amt)
    plot_term(row.hold[ui.amt_idx], n_hold, ui.hold_idx, row.stat_from_file ? "文件 stat" : "Factors Run", cell);
  else
    empty_plot("期限结构###term", cell, "无 stat (Factors 页 Run 或此处 Compute)");

  if (match && ui.der.valid)
    plot_ic(ui.der, cell);
  else
    empty_plot("IC 时序###ic", cell, wait_hint);
  ImGui::SameLine();
  empty_plot("留位###reserved", cell, "Markowitz CDF 仓位映射 (待做)");
  return action;
}

} // namespace GUI::Factors
