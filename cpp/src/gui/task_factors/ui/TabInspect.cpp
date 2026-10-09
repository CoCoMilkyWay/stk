// Tab Inspect — 见头文件
#include "gui/task_factors/ui/TabInspect.hpp"
#include "gui/Tasks.hpp" // StatusColor

#include "imgui.h"
#include "implot.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
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

// 分层色: 两端两色 (G1 冷蓝 / G20 暖橙), 中间线性渐变 —— 单调, 一眼分出谁高谁低 (Jet 在中段来回翻色, 相邻组交织分不清)
constexpr ImVec4 kColBottom(0.25f, 0.55f, 1.00f, 1.f), kColTop(1.00f, 0.45f, 0.20f, 1.f), kColLS(1.f, 1.f, 1.f, 1.f);
ImVec4 group_color(int k) {
  const float t = static_cast<float>(k) / static_cast<float>(K - 1);
  return ImVec4(kColBottom.x + (kColTop.x - kColBottom.x) * t, kColBottom.y + (kColTop.y - kColBottom.y) * t,
                kColBottom.z + (kColTop.z - kColBottom.z) * t, 1.f);
}

// 单调三次 Hermite (Fritsch–Carlson) 把节点 (x 严格升序) 稠密采样成折线: 圆滑且不过冲 (期限结构点少, 直连折线生硬)
void pchip_dense(const std::vector<double> &x, const std::vector<double> &y, std::vector<double> &xs, std::vector<double> &ys) {
  const int n = static_cast<int>(x.size());
  assert(y.size() == x.size());
  xs.clear();
  ys.clear();
  if (n < 2) {
    xs = x;
    ys = y;
    return;
  }
  std::vector<double> h(static_cast<size_t>(n) - 1), del(static_cast<size_t>(n) - 1), m(static_cast<size_t>(n), 0.0);
  for (int i = 0; i + 1 < n; ++i) {
    h[static_cast<size_t>(i)] = x[static_cast<size_t>(i) + 1] - x[static_cast<size_t>(i)];
    assert(h[static_cast<size_t>(i)] > 0.0);
    del[static_cast<size_t>(i)] = (y[static_cast<size_t>(i) + 1] - y[static_cast<size_t>(i)]) / h[static_cast<size_t>(i)];
  }
  if (n == 2) {
    m[0] = m[1] = del[0];
  } else {
    for (int i = 1; i + 1 < n; ++i) {
      const double d0 = del[static_cast<size_t>(i) - 1], d1 = del[static_cast<size_t>(i)];
      if (d0 * d1 <= 0.0)
        continue; // 极值点斜率 0
      const double w1 = 2.0 * h[static_cast<size_t>(i)] + h[static_cast<size_t>(i) - 1], w2 = h[static_cast<size_t>(i)] + 2.0 * h[static_cast<size_t>(i) - 1];
      m[static_cast<size_t>(i)] = (w1 + w2) / (w1 / d0 + w2 / d1);
    }
    auto endpoint = [](double h0, double h1, double d0, double d1) { // 三点外推 + 保形钳位
      const double s = ((2.0 * h0 + h1) * d0 - h0 * d1) / (h0 + h1);
      if (s * d0 <= 0.0)
        return 0.0;
      if (d0 * d1 <= 0.0 && std::abs(s) > 3.0 * std::abs(d0))
        return 3.0 * d0;
      return s;
    };
    m[0] = endpoint(h[0], h[1], del[0], del[1]);
    m[static_cast<size_t>(n) - 1] = endpoint(h[static_cast<size_t>(n) - 2], h[static_cast<size_t>(n) - 3], del[static_cast<size_t>(n) - 2], del[static_cast<size_t>(n) - 3]);
  }
  constexpr int kPerSeg = 16;
  xs.reserve(static_cast<size_t>(n - 1) * kPerSeg + 1);
  ys.reserve(xs.capacity());
  for (int i = 0; i + 1 < n; ++i) {
    const size_t u = static_cast<size_t>(i);
    for (int j = 0; j < kPerSeg; ++j) {
      const double t = static_cast<double>(j) / kPerSeg, t2 = t * t, t3 = t2 * t;
      const double h00 = 2 * t3 - 3 * t2 + 1, h10 = t3 - 2 * t2 + t, h01 = -2 * t3 + 3 * t2, h11 = t3 - t2;
      xs.push_back(x[u] + t * h[u]);
      ys.push_back(h00 * y[u] + h10 * h[u] * m[u] + h01 * y[u + 1] + h11 * h[u] * m[u + 1]);
    }
  }
  xs.push_back(x.back());
  ys.push_back(y.back());
}

// 一级 Row[T] (选定 hold) → 日级序列 + IC 分布. 累计按 1/h 折算: 相邻 h 行的标签是同一段收益, Σ_t r_t / h = h 个相位非重叠链的平均;
// IC 分布 = ok 行的 ic 逐行进 KLL (与 summarize 的 ic_mean/std/skew/kurt 同一组样本)
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
  std::vector<float> ics;
  ics.reserve(static_cast<size_t>(T));
  double gc[K] = {}, lc = 0.0;
  for (int day = 0; day < days; ++day) {
    double g[K] = {}, ls = 0.0;
    for (int t = day * S; t < (day + 1) * S; ++t) {
      const factor::stat::Row &w = r[t];
      if (!w.ok)
        continue;
      ics.push_back(w.ic);
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
  }
  KLLcache kll(analysis::kAggKllCapacity, analysis::kAggKllResolution);
  kll.addBatch(ics);
  d.ic_pdf.fill(kll, 3); // n < 3 与 summarize 同口径: 无 stat 也无 PDF
  d.hs = res.hold[sel];
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

// 左上: 分层累计 + LS. 超额|绝对 切换按钮叠在绘图区右上角 (只影响这张图): ImPlot 绘图区按 AllowOverlap 吃输入, 后提交的 ImGui 控件优先;
// 整体包在 Group 里, 控件的 ItemSize 不打乱外层 SameLine 排版
void plot_layers(const InspectDerived &d, bool &absolute, bool &net_cost, const ImVec2 &size) {
  char title[128];
  std::snprintf(title, sizeof(title), "分层累计 %s  h=%s  mono %+.3f###layers", absolute ? "(绝对 lv)" : "(超额 lv − mkt)",
                factor::stat::hold_name(d.hs.hold).c_str(), d.hs.mono);
  ImGui::BeginGroup();
  if (!ImPlot::BeginPlot(title, size, ImPlotFlags_NoMenus)) {
    ImGui::EndGroup();
    return;
  }
  ImPlot::SetupAxes(nullptr, absolute ? "Σ ret / h" : "Σ excess / h", ImPlotAxisFlags_None, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupAxisFormat(ImAxis_X1, date_formatter, const_cast<std::vector<std::string> *>(&d.dates));
  ImPlot::SetupAxisLimits(ImAxis_X1, 0, std::max(1, d.days), ImPlotCond_Always);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  {
    const ImVec2 pp = ImPlot::GetPlotPos(), ps = ImPlot::GetPlotSize(); // 锁 setup
    const ImGuiStyle &st = ImGui::GetStyle();
    const char *lbl = absolute ? "绝对 lv##abs" : "超额 lv−mkt##abs";
    const char *lbl_cost = net_cost ? "净 (扣税佣)##cost" : "毛##cost";
    const float w = ImGui::CalcTextSize(lbl, nullptr, true).x + 2.f * st.FramePadding.x;
    const float wc = ImGui::CalcTextSize(lbl_cost, nullptr, true).x + 2.f * st.FramePadding.x;
    ImGui::SetCursorScreenPos(ImVec2(pp.x + ps.x - w - wc - st.ItemSpacing.x - 6.f, pp.y + 6.f));
    if (ImGui::SmallButton(lbl_cost))
      net_cost = !net_cost;
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("标签是价格收益 (entry 中间价 → exit 分钟 VWAP, 不含冲击 / 税佣; 冲击由控件行的 冲击 选项扣);\n"
                        "净 = 每往返再扣 2×commission + stamp (Config), 分层线各 1 次, LS 2 次\n(尚未接线: 只切状态, 曲线不变)");
    ImGui::SameLine();
    if (ImGui::SmallButton(lbl))
      absolute = !absolute;
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("口径: 超额 = 组均 (lv − mkt), 围着 0 看形状; 绝对 = 组均 lv, 含市场\nLS = top(lv) + bottom(sv) 对市场本就中性, 两种口径同一条");
  }
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
  ImPlot::SetNextLineStyle(kColLS, 2.5f);
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
  ImGui::EndGroup();
}

// 右上: 期限结构 = 全部持有期的 L (G20 做多超额) / S (G1 做多超额) / LS 均值, PCHIP 圆滑折线 + 节点圆点; L / S 默认隐藏, 点图例切.
// 口径 = 全 universe 全时刻池化 (HoldStat::grp = Σ 组内超额 / Σ 组样本数; ls_mean = 逐 t ls 的时间均值), 不是逐资产信号平均.
// hs[n_hold] 来自 Inspect 结果或文件 stat; n < 3 的持有期不作节点
void plot_term(const factor::stat::HoldStat *hs, int n_hold, int sel_hold, const char *source, const ImVec2 &size) {
  char title[128];
  std::snprintf(title, sizeof(title), "期限结构: 池化均值超额 / 持有期  (%s)###term", source);
  if (!ImPlot::BeginPlot(title, size, ImPlotFlags_NoMenus))
    return;
  std::vector<double> pos(static_cast<size_t>(n_hold));
  std::vector<std::string> names(static_cast<size_t>(n_hold));
  std::vector<const char *> labels(static_cast<size_t>(n_hold));
  std::vector<double> xk, yk[3]; // 节点: [L / S / LS]
  for (int i = 0; i < n_hold; ++i) {
    pos[static_cast<size_t>(i)] = i;
    names[static_cast<size_t>(i)] = factor::stat::hold_name(hs[i].hold);
    labels[static_cast<size_t>(i)] = names[static_cast<size_t>(i)].c_str();
    if (hs[i].n < 3)
      continue;
    xk.push_back(i);
    yk[0].push_back(hs[i].grp[K - 1]);
    yk[1].push_back(hs[i].grp[0]);
    yk[2].push_back(hs[i].ls_mean);
  }
  ImPlot::SetupAxes("hold", "mean excess", ImPlotAxisFlags_None, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupAxisTicks(ImAxis_X1, pos.data(), n_hold, labels.data());
  ImPlot::SetupAxisLimits(ImAxis_X1, -0.6, n_hold - 0.4, ImPlotCond_Always);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  const char *items[3] = {"L (G20 long)", "S (G1 long)", "LS"};
  const ImVec4 cols[3] = {kColTop, kColBottom, kColLS};
  std::vector<double> xs, ys;
  for (int c = 0; c < 3; ++c) {
    pchip_dense(xk, yk[c], xs, ys);
    if (c < 2)
      ImPlot::HideNextItem(true, ImPlotCond_Once); // 默认只显 LS, 图例可点开
    ImPlot::SetNextLineStyle(cols[c], c == 2 ? 2.5f : 1.5f);
    ImPlot::PlotLine(items[c], xs.data(), ys.data(), static_cast<int>(xs.size()));
    ImPlot::SetNextMarkerStyle(ImPlotMarker_Circle, 3.f, cols[c], IMPLOT_AUTO, cols[c]);
    ImPlot::PlotScatter(items[c], xk.data(), yk[c].data(), static_cast<int>(xk.size())); // 同名 → 同一图例项, 一起显隐
  }
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
                    "L %+.5f  S %+.5f  LS %+.5f",
                    labels[static_cast<size_t>(i)], h.n, h.n_ac, h.ic_mean, h.icir, h.ic_t, h.ic_pos, h.ls_mean, h.ls_t, h.ls_pos, h.sharpe,
                    h.beta, h.mono, h.rank_ac, h.grp[K - 1], h.grp[0], h.ls_mean);
      ImGui::EndTooltip();
    }
  }
  ImPlot::EndPlot();
}

// 左下: IC 分布 — 逐行 rank IC 当随机变量, KLL 导出的 PDF (阴影 + 线) + 0 线 + 均值线; 标题的矩与顶部 stat 行同一组样本
void plot_ic(const InspectDerived &d, const ImVec2 &size) {
  char title[160];
  std::snprintf(title, sizeof(title), "IC 分布 (逐行 rank IC)  mean %+.4f  std %.4f  skew %+.2f  kurt %+.2f  pos %.2f  n=%d###ic", d.hs.ic_mean, d.hs.ic_std,
                d.hs.ic_skew, d.hs.ic_kurt, d.hs.ic_pos, d.hs.n);
  if (!ImPlot::BeginPlot(title, size, ImPlotFlags_NoMenus))
    return;
  ImPlot::SetupAxes("IC", "density", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  const analysis::AggPdf &p = d.ic_pdf;
  const int np = static_cast<int>(p.n_pts);
  if (np >= 2) {
    const ImVec4 col(0.4f, 0.7f, 1.0f, 1.0f);
    ImPlot::SetNextFillStyle(col, 0.35f);
    ImPlot::PlotShaded("PDF", p.x.data(), p.y.data(), np, 0.0);
    ImPlot::SetNextLineStyle(col, 2.0f);
    ImPlot::PlotLine("PDF", p.x.data(), p.y.data(), np);
  }
  const double zero = 0.0, mean = d.hs.ic_mean;
  ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
  ImPlot::PlotInfLines("##zero", &zero, 1);
  ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.75f, 0.3f, 1.0f), 1.5f);
  ImPlot::PlotInfLines("mean", &mean, 1);
  if (np >= 2 && ImPlot::IsPlotHovered()) {
    const ImPlotPoint mp = ImPlot::GetPlotMousePos();
    const float *lo = std::lower_bound(p.x.data(), p.x.data() + np, static_cast<float>(mp.x));
    const int i = std::clamp(static_cast<int>(lo - p.x.data()), 0, np - 1);
    ImGui::SetTooltip("IC %+.4f  density %.3f", p.x[static_cast<size_t>(i)], p.y[static_cast<size_t>(i)]);
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
  const int n_hold = static_cast<int>(ft.labels.size());
  ui.hold_idx = std::clamp(ui.hold_idx, 0, std::max(0, n_hold - 1));
  const int cur_hold = n_hold ? ft.labels[static_cast<size_t>(ui.hold_idx)].hold : 0;
  {
    bool impact_ok = ui.impact_amt == 0; // 字段表没有该档 (特征库换过) → 回毛
    for (const CostCol &cc : ft.costs)
      impact_ok |= cc.amt == ui.impact_amt;
    if (!impact_ok)
      ui.impact_amt = 0;
  }
  ImGui::SetNextItemWidth(70);
  if (ImGui::BeginCombo("Hold", n_hold ? factor::stat::hold_name(cur_hold).c_str() : "-")) {
    for (int i = 0; i < n_hold; ++i)
      if (ImGui::Selectable(factor::stat::hold_name(ft.labels[static_cast<size_t>(i)].hold).c_str(), i == ui.hold_idx))
        ui.hold_idx = i;
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("分层累计 / IC 分布 / 顶部 stat 看哪个持有期 (只影响显示; 一次算全部持有期)");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(70);
  if (ImGui::BeginCombo("冲击", ui.impact_amt ? (std::to_string(ui.impact_amt) + "w").c_str() : "无")) {
    if (ImGui::Selectable("无", ui.impact_amt == 0))
      ui.impact_amt = 0;
    for (const CostCol &cc : ft.costs)
      if (ImGui::Selectable((std::to_string(cc.amt) + "w").c_str(), cc.amt == ui.impact_amt))
        ui.impact_amt = cc.amt;
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("标签口径. 无 = 价格收益 (entry 中间价 → exit 分钟 VWAP; 默认, 与 Factors Run 同);\n"
                      "<amt>w = 再扣建仓冲击 (lb_cost_buy/sell_<amt>w: 同一盘口吃 amt 万的 VWAP 偏离中间价) + 平仓固定冲击 (Config 平仓冲击 %.4f)\n"
                      "净标签即兴算不缓存 (换档只重算 Stat, 不重读库); 吃不到的格 (NaN) 无效",
                      ctx.sell_impact);
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
  InspectRequest probe;
  probe.file = row.file, probe.expr = row.expr, probe.frame = row.frame;
  probe.universe = ctx.universe, probe.start_date = ctx.start_date, probe.end_date = ctx.end_date;
  probe.impact_amt = ui.impact_amt, probe.sell_impact = ui.impact_amt > 0 ? ctx.sell_impact : 0.0;
  const std::string key = probe.key();
  const int sel = ui.hold_idx;
  std::string message, res_error, res_key;
  StatScope res_scope;
  float res_valid = 0.f;
  double res_ms = 0.0;
  factor::stat::HoldStat res_holds[factor::stat::kMaxHold]; // 全部持有期 (期限结构)
  bool res_has = false;
  {
    std::lock_guard<std::mutex> lock(isvc.mutex);
    message = isvc.message;
    const InspectResult &res = isvc.result;
    res_key = res.key;
    res_error = res.error;
    if (!res.key.empty() && res.error.empty()) {
      assert(res.n_hold == n_hold && "Inspect 与 FeatureTable 的标签组不一致");
      res_has = true;
      res_scope = res.scope;
      res_valid = res.valid_pct;
      res_ms = res.eval_ms;
      for (int i = 0; i < n_hold; ++i)
        res_holds[i] = res.hold[i];
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
    ImGui::TextDisabled("%s  %s..%s  %d 天 × %d 资产 (T=%d)  %s %s | valid %.2f%%  eval %.1f ms | 标签 %s", res_scope.universe.c_str(),
                        res_scope.start_date.c_str(), res_scope.end_date.c_str(), res_scope.days, res_scope.A, res_scope.T, res_scope.backend.c_str(),
                        res_scope.time.c_str(), res_valid, res_ms,
                        ui.impact_amt ? ("扣冲击 " + std::to_string(ui.impact_amt) + "w + 平仓 " + std::to_string(ctx.sell_impact)).c_str() : "毛 (价格收益)");
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
    plot_layers(ui.der, ui.absolute, ui.net_cost, cell);
  else
    empty_plot("分层累计###layers", cell, wait_hint);
  ImGui::SameLine();
  // 期限结构: Inspect 结果优先, 否则文件 stat (Factors 表的 row.hold)
  if (match)
    plot_term(res_holds, n_hold, ui.hold_idx, ui.impact_amt ? "Inspect (扣冲击)" : "Inspect", cell);
  else if (row.has_stat && row.n_hold == n_hold && ui.impact_amt == 0) // 文件 stat 只有毛口径
    plot_term(row.hold, n_hold, ui.hold_idx, row.stat_from_file ? "文件 stat" : "Factors Run", cell);
  else
    empty_plot("期限结构###term", cell, "无 stat (Factors 页 Run 或此处 Compute)");

  if (match && ui.der.valid)
    plot_ic(ui.der, cell);
  else
    empty_plot("IC 分布###ic", cell, wait_hint);
  ImGui::SameLine();
  empty_plot("留位###reserved", cell, "Markowitz CDF 仓位映射 (待做)");
  return action;
}

} // namespace GUI::Factors
