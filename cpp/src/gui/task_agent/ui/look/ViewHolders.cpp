// view_holders 的展示: 集中度 + 本端行数条 + 资产表
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <cstdio>

namespace GUI::Agent {

using namespace ui;

void HoldersRes::draw() const {
  draw_head();
  if (!total) {
    ImGui::TextDisabled("本端没有行");
    return;
  }
  char b[256];
  std::snprintf(b, sizeof(b), "本端共 %zu 行 · 落过本端的资产 %zu / %d · 前 10 只占 %.0f%% · 凑满一半要 %zu 只", total, any, active, top10 * 100.0, half);
  ImGui::TextUnformatted(b);
  std::vector<std::string> names;
  std::vector<double> n;
  for (const Row &r : rows)
    names.push_back(r.code), n.push_back(r.side);
  bars_plot("本端行数##side", names, n, 30.0f + 16.0f * static_cast<float>(rows.size()));
  if (grid("##rows", {"代码", "本端行", "在池行", "占其在池", "天数", "平均连续行", "持仓格", "持仓格均超额 bp"})) {
    for (const Row &r : rows) {
      ImGui::TableNextRow();
      cell(r.code), cell(std::to_string(r.side)), cell(std::to_string(r.in)), cell_pct(r.share), cell(std::to_string(r.days)), cell_num(r.run, "%.1f"),
          cell(std::to_string(r.held)), cell_bp(r.ex);
    }
    ImGui::EndTable();
  }
}

} // namespace GUI::Agent
