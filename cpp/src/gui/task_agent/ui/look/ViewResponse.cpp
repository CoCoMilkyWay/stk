// view_response 的图: 累计超额路径 (带 ± 标准误, 各开仓时段) + 各时点因子分位 (节与表见 tools/look/ViewResponse.cpp)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include "implot.h"

namespace GUI::Agent {

using namespace ui;

void ResponseRes::plot(int) const {
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
}

} // namespace GUI::Agent
