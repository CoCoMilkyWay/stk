// view_layers 的图: 20 组柱 + 各组真持有只数 / 各段 mono 条 / 条件三段的 20 组柱 (节与表见 tools/look/ViewLayers.cpp)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void LayersRes::plot(int i) const {
  if (i == 0) {
    groups_plot("20 组每期超额 (bp)##grp", all.grp, 200.0f);
    line_plot("各组行均真持有只数##held", held, 110.0f, "只数");
  } else if (i == 1) {
    std::vector<std::string> names;
    std::vector<double> mono;
    for (const LayerRow &r : splits)
      names.push_back(r.name), mono.push_back(r.mono);
    bars_plot("各段 mono##split_mono", names, mono, 30.0f + 18.0f * static_cast<float>(splits.size()));
  } else {
    for (size_t j = 1; j < conds.size(); ++j) {
      ImGui::PushID(static_cast<int>(j));
      groups_plot((conds[j].name + " 的 20 组##cg").c_str(), conds[j].grp, 120.0f);
      ImGui::PopID();
    }
  }
}

} // namespace GUI::Agent
