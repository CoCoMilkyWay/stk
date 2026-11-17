// search_factors 的展示: 命中概况 + 类别 / 出处流派条 + 数据周期 × 预测期矩阵 + 因子表
#include "gui/task_agent/tools/Query.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void FactorsRes::draw() const {
  heading("命中 " + std::to_string(hit) + " 个因子 (出自 " + std::to_string(docs) + " 篇) " + cond);
  if (loose || !hit)
    wrapped(color(TaskStatus::Kind::Warn), "各条件单独命中: " + alone);
  if (!hit)
    return;
  std::vector<std::string> fn;
  std::vector<double> fv;
  for (const auto &[k, n] : families)
    fn.push_back(k), fv.push_back(n);
  bars_plot("类别##fam", fn, fv, 30.0f + 16.0f * static_cast<float>(fn.size()));
  heading("数据周期 × 预测期");
  std::vector<std::string> hs{"数据 \\ 预测"};
  hs.insert(hs.end(), cols.begin(), cols.end());
  if (grid("##matrix", hs)) {
    for (size_t r = 0; r < rows.size(); ++r) {
      ImGui::TableNextRow();
      cell(rows[r]);
      for (size_t h = 0; h < cols.size(); ++h) {
        const int n = cells[r * cols.size() + h];
        n ? cell(std::to_string(n)) : cell_dim("·");
      }
    }
    ImGui::EndTable();
  }
  heading("因子 (前 " + std::to_string(list.size()) + " 个; 统计是原文口径, 只看机制)");
  if (!begin_table("##list", {"名", "类别", "方向", "周期 → 预测", "原文统计", "出处", "公式"}, 0.0f))
    return;
  for (const Row &f : list) {
    ImGui::TableNextRow();
    cell(f.name), cell(f.family), cell(f.direction), cell(f.period + " → " + f.horizon), cell_dim(f.stats), cell_dim(f.doc);
    ImGui::TableNextColumn();
    wrapped(kColExpr, f.formula);
  }
  ImGui::EndTable();
}

} // namespace GUI::Agent
