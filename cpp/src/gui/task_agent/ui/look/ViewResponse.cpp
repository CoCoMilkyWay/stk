// view_response 的展示: 累计超额路径 (带 ± 标准误) + 各时点因子分位 + 与此前收益的相关 + 按开仓时段的路径
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include "implot.h"

#include <cstdio>

namespace GUI::Agent {

using namespace ui;

void ResponseRes::draw() const {
  draw_head();
  if (!events) {
    ImGui::TextDisabled("本端没有真持有过的格");
    return;
  }
  ImGui::Text("事件 %zu 段%s", events, kept < events ? (" (等距抽 " + std::to_string(kept) + " 段)").c_str() : "");
  const size_t P = pts.size();
  std::vector<double> xs(P), ys(P), es(P), rk(P);
  std::vector<const char *> names(P);
  for (size_t j = 0; j < P; ++j)
    xs[j] = static_cast<double>(j), ys[j] = pts[j].zero ? 0.0 : pts[j].ex * 1e4, es[j] = pts[j].zero || !std::isfinite(pts[j].se) ? 0.0 : pts[j].se * 1e4,
    rk[j] = pts[j].rank * 100.0, names[j] = pts[j].name.c_str();
  if (ImPlot::BeginPlot("累计超额 (bp)##path", ImVec2(-1, 220), ImPlotFlags_NoMenus)) {
    ImPlot::SetupAxes(nullptr, "bp", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
    ImPlot::SetupAxisTicks(ImAxis_X1, xs.data(), static_cast<int>(P), names.data());
    ImPlot::PlotErrorBars("± 标准误", xs.data(), ys.data(), es.data(), static_cast<int>(P));
    ImPlot::PlotLine("全部", xs.data(), ys.data(), static_cast<int>(P));
    for (const Part &q : parts) {
      std::vector<double> v(P);
      for (size_t j = 0; j < P; ++j)
        v[j] = pts[j].zero ? 0.0 : q.ex[j] * 1e4;
      ImPlot::PlotLine(q.name.c_str(), xs.data(), v.data(), static_cast<int>(P));
    }
    ImPlot::EndPlot();
  }
  if (ImPlot::BeginPlot("各时点因子分位##rank", ImVec2(-1, 130), ImPlotFlags_NoMenus | ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes(nullptr, "分位", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
    ImPlot::SetupAxisTicks(ImAxis_X1, xs.data(), static_cast<int>(P), names.data());
    ImPlot::PlotLine("分位", xs.data(), rk.data(), static_cast<int>(P));
    ImPlot::EndPlot();
  }
  if (grid("##pts", {"时点", "段数", "累计超额 bp", "± 标准误", "为正占比", "因子分位"})) {
    for (const Pt &p : pts) {
      ImGui::TableNextRow();
      cell(p.name), cell(std::to_string(p.n));
      if (p.zero)
        cell("0"), cell_dim("—"), cell_dim("—");
      else
        cell_bp(p.ex), cell_num(p.se * 1e4, "%.1f"), cell_pct(p.up);
      cell_num(p.rank * 100.0, "%.0f");
    }
    ImGui::EndTable();
  }
  heading("开仓行因子分位与此前收益的截面秩相关 (≈ −1 反转代理 / ≈ +1 动量代理)");
  if (grid("##lags", {"窗口", "相关"})) {
    for (const auto &[n, v] : lags) {
      ImGui::TableNextRow();
      cell(n), cell_num(v, "%.3f", true);
    }
    ImGui::EndTable();
  }
}

} // namespace GUI::Agent
