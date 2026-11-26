// view_snapshot 的图: 各列与因子分位的截面秩相关条 (节与表见 tools/look/ViewSnapshot.cpp)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void SnapshotRes::plot(int) const {
  std::vector<std::string> names;
  std::vector<double> vals;
  for (const auto &[k, v] : corr)
    names.push_back(k), vals.push_back(v);
  bars_plot("与因子分位的截面秩相关##corr", names, vals, 30.0f + 18.0f * static_cast<float>(corr.size()));
}

} // namespace GUI::Agent
