// view_trades 的展示: 概况 + 段超额分布 + 开仓时段 (表 + 均值条) + 盘面标签 (提升) + 明细
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <cstdio>

namespace GUI::Agent {

using namespace ui;

void TradesRes::draw() const {
  draw_head();
  if (!cells) {
    ImGui::TextDisabled("本端没有真持有过的格");
    return;
  }
  char b[320];
  std::snprintf(b, sizeof(b), "真持有 %zu 格 → %zu 段 · 格超额 %+.1fbp (胜率 %.0f%%) · 段超额 均值 %+.1f / 中位 %+.1f / p10 %+.1f / p90 %+.1f bp · 胜率 %.0f%%", cells, segs, cell_mean * 1e4,
                cell_up * 100.0, seg_mean * 1e4, seg_med * 1e4, seg_p10 * 1e4, seg_p90 * 1e4, seg_up * 100.0);
  wrapped(b);
  std::snprintf(b, sizeof(b), "集中度: 最差 10%% 的段占全部亏损 %.0f%% · 最好 10%% 的段占全部盈利 %.0f%%", worst * 100.0, best * 100.0);
  ImGui::TextUnformatted(b);
  hist_plot("段超额分布 (p1~p99)##hist", hist, h_lo, h_hi, 130.0f);
  if (!parts.empty()) {
    heading("开仓时段");
    if (grid("##parts", {"时段", "段数", "占比", "格超额 bp", "段胜率"})) {
      for (const Part &p : parts) {
        ImGui::TableNextRow();
        cell(p.name), cell(std::to_string(p.n)), cell_pct(p.share), cell_bp(p.mean), cell_pct(p.up);
      }
      ImGui::EndTable();
    }
  }
  if (!tags.empty()) {
    heading("开仓那行的盘面");
    if (grid("##tags", {"盘面", "本端格占比", "池内格占比", "提升", "带此标签 格超额 bp", "不带 bp"})) {
      for (const Tag &t : tags) {
        ImGui::TableNextRow();
        cell(t.name), cell_pct(t.held), cell_pct(t.base);
        cell_num(t.base > 0.0 ? t.held / t.base : NAN, "%.2f");
        cell_bp(t.with), cell_bp(t.without);
      }
      ImGui::EndTable();
    }
  }
  heading("明细 (" + sort + ")");
  if (grid("##list", {"#", "代码", "日期", "时刻", "行", "开仓分位", "段超额 bp", "当日涨跌", "日内位置", "盘面"})) {
    for (size_t i = 0; i < list.size(); ++i) {
      const Seg &x = list[i];
      ImGui::TableNextRow();
      cell(std::to_string(i + 1)), cell(x.code), cell(x.date), cell(x.time), cell(std::to_string(x.rows));
      cell_num(x.rank * 100.0, "%.0f"), cell_bp(x.ex), cell_num(x.chg * 100.0, "%+.2f%%", true), cell_num(x.pos, "%.2f"), cell_dim(x.tags);
    }
    ImGui::EndTable();
  }
}

} // namespace GUI::Agent
