// view_stock_day 的图: 价 (高低带) / 量 / 因子分位 / 特征的逐行图 (节与表见 tools/look/ViewStockDay.cpp)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include "implot.h"

#include <algorithm>
#include <cmath>

namespace GUI::Agent {

using namespace ui;

void StockDayRes::plot(int) const {
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
}

} // namespace GUI::Agent
