// Tab Inspect — 见头文件
#include "gui/task_factors/ui/TabInspect.hpp"
#include "gui/Tasks.hpp" // StatusColor

#include "imgui.h"

#include <mutex>

namespace GUI::Factors {

void RenderTabInspect(FactorsService &svc, const FactorsUIState &ui, const FactorsUIContext &ctx) {
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Factor:");
  ImGui::SameLine();
  if (ui.view_file.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "(无) 在 Factors 页点一行高光");
    return;
  }
  // 短锁拷该行 (rows 由 worker 重扫时整体替换)
  FactorRow row;
  bool found = false;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    for (const FactorRow &r : svc.rows)
      if (r.file == ui.view_file) {
        row = r;
        found = true;
        break;
      }
  }
  if (!found) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s (重扫中 / 已不在)", ui.view_file.c_str());
    return;
  }
  ImGui::Text("%s/%s", ctx.factor_dir.c_str(), row.file.c_str());
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  if (!row.error.empty())
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "BROKEN: %s", row.error.c_str());
  else
    ImGui::TextUnformatted(row.expr.c_str());
  if (!row.note.empty())
    ImGui::TextDisabled("%s", row.note.c_str());
  ImGui::Separator();
}

} // namespace GUI::Factors
