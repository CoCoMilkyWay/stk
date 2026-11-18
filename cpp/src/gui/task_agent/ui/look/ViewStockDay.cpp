// view_stock_day 的展示: 当日概况 + 价 (高低带) / 量 / 因子分位 / 特征的逐行图 + 分段表 + 逐行表
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include "implot.h"

#include <algorithm>
#include <cmath>
#include <cstdio>

namespace GUI::Agent {

using namespace ui;

void StockDayRes::draw() const {
  if (!period.empty())
    wrapped(color(TaskStatus::Kind::Warn), period);
  heading(code + " · " + date + " · " + level);
  char b[320];
  std::snprintf(b, sizeof(b), "昨收 %.2f · 高 %.2f (%+.2f%%) · 低 %.2f (%+.2f%%) · 收 %.2f (%+.2f%%) · 涨停 %.2f / 跌停 %.2f · 在池 %d / %d 行", pre, hi, (hi / pre - 1) * 100, lo,
                (lo / pre - 1) * 100, last, (last / pre - 1) * 100, lim_up, lim_dn, in, seg);
  wrapped(b);
  for (const std::string &e : exprs)
    wrapped_dim(e);
  const int n = static_cast<int>(close.size());
  std::vector<double> xs(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i)
    xs[static_cast<size_t>(i)] = i;
  std::vector<double> at;
  std::vector<const char *> names;
  for (int i = 0; i < n; i += std::max(1, n / 8))
    at.push_back(i), names.push_back(clock[static_cast<size_t>(i)].c_str());
  if (n > 1 && ImPlot::BeginPlot("价##px", ImVec2(-1, 200), ImPlotFlags_NoMenus)) {
    const bool mid = std::isfinite(pre) && hi >= lo; // 昨收居中 (0% 在 y 中间, 上下取两侧较大者对称); 当日无成交 → 贴合
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_AutoFit, mid ? ImPlotAxisFlags_None : ImPlotAxisFlags_AutoFit);
    if (mid) {
      const double d = std::max({hi - pre, pre - lo, pre * 1e-3}) * 1.06;
      ImPlot::SetupAxisLimits(ImAxis_Y1, pre - d, pre + d, ImPlotCond_Always);
    }
    ImPlot::SetupAxisTicks(ImAxis_X1, at.data(), static_cast<int>(at.size()), names.data());
    ImPlot::PlotShaded("高低", xs.data(), low.data(), high.data(), n);
    ImPlot::PlotLine("收", xs.data(), close.data(), n);
    ImPlot::EndPlot();
  }
  if (n > 1 && ImPlot::BeginPlot("量##vol", ImVec2(-1, 100), ImPlotFlags_NoMenus | ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
    ImPlot::SetupAxisTicks(ImAxis_X1, at.data(), static_cast<int>(at.size()), names.data());
    ImPlot::PlotBars("量", xs.data(), vol.data(), n, 0.8);
    ImPlot::EndPlot();
  }
  if (has_fac)
    line_plot(("因子 " + fac + " 分位##rank").c_str(), rank, 110.0f, "分位", &clock);
  for (size_t i = 0; i < fv.size(); ++i) {
    ImGui::PushID(static_cast<int>(i));
    line_plot(("E" + std::to_string(i + 1) + "##fv").c_str(), fv[i], 100.0f, "值", &clock);
    ImGui::PopID();
  }
  heading("分段");
  std::vector<std::string> hs{"时段", "涨跌", "段内高", "段内低", "量占比"};
  if (has_fac)
    hs.push_back("因子分位"), hs.push_back("多/空行");
  hs.push_back("后续超额 bp");
  for (size_t i = 0; i < fv.size(); ++i)
    hs.push_back("E" + std::to_string(i + 1) + " 段末");
  if (grid("##parts", hs)) {
    for (const Part &p : parts) {
      ImGui::TableNextRow();
      cell(p.name), cell_num(p.chg * 100, "%+.2f%%", true), cell_num(p.hi * 100, "%+.2f%%", true), cell_num(p.lo * 100, "%+.2f%%", true), cell_pct(p.vol);
      if (has_fac)
        cell_num(p.rank * 100, "%.0f"), cell(std::to_string(p.up) + "/" + std::to_string(p.dn));
      cell_bp(p.ex);
      for (const double v : p.end)
        cell_num(v, "%.4g");
    }
    ImGui::EndTable();
  }
  if (rows.empty())
    return;
  heading("逐行 " + rows);
  std::vector<std::string> rh{"时刻", "收", "涨跌", "当日", "量", "额 万"};
  if (has_fac)
    rh.push_back("因子分位"), rh.push_back("组");
  rh.push_back("后续超额 bp");
  if (!grid("##rows", rh))
    return;
  for (int k = r0; k < r1; ++k) {
    const size_t q = static_cast<size_t>(k);
    const double prev = k == 0 ? pre : close[q - 1];
    ImGui::TableNextRow();
    cell(clock[q]), cell_num(close[q], "%.2f"), cell_num((close[q] / prev - 1) * 100, "%+.2f%%", true), cell_num((close[q] / pre - 1) * 100, "%+.2f%%", true),
        cell_num(vol[q], "%.0f"), cell_num(amt[q] / 1e4, "%.1f");
    if (has_fac)
      cell_num(rank[q] * 100, "%.0f"), cell(grp[q] < 0 ? std::string("—") : "G" + std::to_string(grp[q] + 1));
    cell_bp(ex[q]);
  }
  ImGui::EndTable();
}

} // namespace GUI::Agent
