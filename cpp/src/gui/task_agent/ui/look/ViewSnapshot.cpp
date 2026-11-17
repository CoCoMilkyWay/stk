// view_snapshot 的展示: 本行概况 + 截面秩相关条 + 两端表 (中夹池中位, 多 / 空着色)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <cstdio>

namespace GUI::Agent {

using namespace ui;

void SnapshotRes::draw() const {
  if (!period.empty())
    wrapped(color(TaskStatus::Kind::Warn), period);
  heading("因子 " + label + " · " + when + " (" + level + ")");
  char b[256];
  if (row_ok)
    std::snprintf(b, sizeof(b), "在池且有秩 %zu 只 · rIC %.3f · G20 %+.1fbp (%d 只) · G1 %+.1fbp (%d 只)", n, ric, g20 * 1e4, c20, g1 * 1e4, c1);
  else
    std::snprintf(b, sizeof(b), "在池且有秩 %zu 只 · 本行无统计 (建不了仓 / 标签无效)", n);
  ImGui::TextUnformatted(b);
  for (const std::string &e : exprs)
    wrapped_dim(e);
  std::vector<std::string> names;
  std::vector<double> vals;
  for (const auto &[k, v] : corr)
    names.push_back(k), vals.push_back(v);
  bars_plot("与因子分位的截面秩相关##corr", names, vals, 30.0f + 18.0f * static_cast<float>(corr.size()));
  std::vector<std::string> hs{"端", "代码", "分位", "组", "真持有", "当日涨跌"};
  if (seg > 1)
    hs.push_back("本行涨跌"), hs.push_back("日内位置");
  hs.push_back("至此额 万"), hs.push_back("后续超额 bp"), hs.push_back("盘面");
  for (size_t i = 0; i < exprs.size(); ++i)
    hs.push_back("E" + std::to_string(i + 1));
  if (!grid("##cells", hs))
    return;
  for (const Cell &x : cells) {
    ImGui::TableNextRow();
    if (x.mid)
      ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg0, ImGui::GetColorU32(ImGuiCol_TableHeaderBg));
    cell(x.end), cell(x.code), x.mid ? cell("50") : cell_num(x.rank * 100.0, "%.0f"), x.mid ? cell("") : cell("G" + std::to_string(x.grp + 1));
    ImGui::TableNextColumn();
    if (!x.mid && x.held)
      ImGui::TextColored(color(x.held > 0 ? TaskStatus::Kind::Ready : TaskStatus::Kind::Warn), "%s", x.held > 0 ? "多" : "空");
    cell_num(x.chg * 100.0, "%+.2f%%", true);
    if (seg > 1)
      cell_num(x.step * 100.0, "%+.2f%%", true), cell_num(x.pos, "%.2f");
    cell_num(x.amt / 1e4, "%.0f"), cell_bp(x.ex), cell_dim(x.tags);
    for (const double v : x.fv)
      cell_num(v, "%.4g");
  }
  ImGui::EndTable();
}

} // namespace GUI::Agent
