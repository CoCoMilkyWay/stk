// view_explain 的图: 各基准与因子的相关条 / 原因子与剩余部分的 20 组柱 (节与表见 tools/look/ViewExplain.cpp)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void ExplainRes::plot(int i) const {
  if (i == 0) {
    std::vector<std::string> names;
    std::vector<double> corr;
    for (const Base &x : bases)
      names.push_back(x.name), corr.push_back(x.corr);
    bars_plot("与因子的截面秩相关##corr", names, corr, 30.0f + 18.0f * static_cast<float>(bases.size()));
    return;
  }
  groups_plot("原因子 20 组##orig", orig.grp, 130.0f);
  groups_plot("剩余部分 20 组##rest", rest.grp, 130.0f);
}

} // namespace GUI::Agent
