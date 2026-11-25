// view_explain 的展示: 概况 + 基准表 (与因子的相关条) + 原因子 / 剩余部分的分层对照
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <cstdio>

namespace GUI::Agent {

using namespace ui;

void ExplainRes::draw() const {
  draw_head();
  for (const std::string &e : exprs)
    wrapped_dim(e);
  char b[256];
  std::snprintf(b, sizeof(b), "抽 %zu 行 · 因子 rIC %.3f → 剥全部基准剩 %.3f (平均 R² %.3f)", rows, fric, rric, r2);
  ImGui::TextUnformatted(b);
  std::vector<std::string> names;
  std::vector<double> corr;
  for (const Base &x : bases)
    names.push_back(x.name), corr.push_back(x.corr);
  bars_plot("与因子的截面秩相关##corr", names, corr, 30.0f + 18.0f * static_cast<float>(bases.size()));
  if (grid("##bases", {"基准", "与因子的相关", "基准自身 rIC", "只剥它后的因子 rIC"})) {
    for (const Base &x : bases) {
      ImGui::TableNextRow();
      cell(x.name), cell_num(x.corr, "%.3f", true), cell_num(x.ric, "%.3f", true), cell_num(x.rest, "%.3f", true);
    }
    ImGui::EndTable();
  }
  heading("原因子 vs 剩余部分 (近似分层, 只作两行对照)");
  DrawLayerTable("##layers", {orig, rest});
  groups_plot("原因子 20 组##orig", orig.grp, 130.0f);
  groups_plot("剩余部分 20 组##rest", rest.grp, 130.0f);
}

} // namespace GUI::Agent
