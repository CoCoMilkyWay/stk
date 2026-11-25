// view_layers 的展示: 概况 + 20 组柱 + 各组真持有只数 + 尾部表 + 切分各段 (表 + mono 条) + 条件三段; 另有观察共用的分层表与公共头
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <cstdio>
#include <string>

namespace GUI::Agent {

using namespace ui;

void DrawLayerTable(const char *id, const std::vector<LayerRow> &rows) {
  if (!grid(id, {"", "行", "有统计", "mono", "多空 bp", "rIC", "G1 bp", "G10 bp", "G20 bp"}))
    return;
  for (const LayerRow &b : rows) {
    ImGui::TableNextRow();
    cell(b.name), cell(std::to_string(b.rows)), cell(std::to_string(b.ok));
    cell_num(b.mono, "%.3f", true), cell_bp(b.ls), cell_num(b.ric, "%.3f", true);
    if (b.grp.size() == static_cast<size_t>(kGroupN))
      cell_bp(b.grp[0]), cell_bp(b.grp[kGroupN / 2 - 1]), cell_bp(b.grp[kGroupN - 1]);
    else
      cell_dim("—"), cell_dim("—"), cell_dim("—");
  }
  ImGui::EndTable();
}

void LookRes::draw_head() const {
  if (!period.empty())
    wrapped(color(TaskStatus::Kind::Warn), period);
  if (std::string h = headline(); !h.empty()) {
    std::erase(h, '`');
    heading(h);
  }
}

void LayersRes::draw() const {
  draw_head();
  char b[256];
  std::snprintf(b, sizeof(b), "有统计行 %d / %d · mono %.3f · 每期多空 %+.1fbp · rIC %.4f (t %.1f, 正 %.0f%%)", all.ok, all.rows, all.mono, all.ls * 1e4, all.ric, ric_t, ric_up * 100.0);
  ImGui::TextUnformatted(b);
  groups_plot("20 组每期超额 (bp)##grp", all.grp, 200.0f);
  line_plot("各组行均真持有只数##held", held, 110.0f, "只数");

  heading("两端尾部 (组内按池内秩再截更极端的一截)");
  if (grid("##tails", {"截取", "G20 bp", "G20 只数", "G1 bp", "G1 只数", "多空 bp"})) {
    for (const Tail &t : tails) {
      ImGui::TableNextRow();
      std::snprintf(b, sizeof(b), "组内最极端 %g%%", t.frac * 100.0);
      cell(b), cell_bp(t.top), cell_num(t.top_n, "%.1f"), cell_bp(t.bot), cell_num(t.bot_n, "%.1f"), cell_bp((t.top - t.bot) / 2.0);
    }
    ImGui::EndTable();
  }
  if (!splits.empty()) {
    heading(split == "tod" ? "按日内时段" : "按月");
    DrawLayerTable("##splits", splits);
    std::vector<std::string> names;
    std::vector<double> mono;
    for (const LayerRow &r : splits)
      names.push_back(r.name), mono.push_back(r.mono);
    bars_plot("各段 mono##split_mono", names, mono, 30.0f + 18.0f * static_cast<float>(splits.size()));
  } else if (split == "tod" && seg == 1) {
    wrapped_dim("日层每天一行, 没有日内时段可切");
  }
  if (!cond.empty()) {
    heading("按条件三等分: " + cond);
    if (!cond_err.empty())
      wrapped(color(TaskStatus::Kind::Error), "条件没算成: " + cond_err);
    else if (!has_cond)
      wrapped_dim("没有可用的行");
    else {
      std::snprintf(b, sizeof(b), "条件与因子的截面秩相关 %.3f · 条件自身 rIC %.3f", cond_rho, cond_ric);
      ImGui::TextUnformatted(b);
      DrawLayerTable("##conds", conds);
      for (size_t j = 1; j < conds.size(); ++j) {
        ImGui::PushID(static_cast<int>(j));
        groups_plot((conds[j].name + " 的 20 组##cg").c_str(), conds[j].grp, 120.0f);
        ImGui::PopID();
      }
    }
  }
}

} // namespace GUI::Agent
