// view_holders 的图: 各资产本端行数条 (节与表见 tools/look/ViewHolders.cpp)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void HoldersRes::plot(int) const {
  std::vector<std::string> names;
  std::vector<double> n;
  for (const Row &r : rows)
    names.push_back(r.code), n.push_back(r.side);
  bars_plot("本端行数##side", names, n, 30.0f + 16.0f * static_cast<float>(rows.size()));
}

} // namespace GUI::Agent
