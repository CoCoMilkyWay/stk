#include "gui/task_features/ui/Common.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"

namespace GUI::Features {

// ============================================================================
// 状态面
// ============================================================================

const char *StatusText(analysis::Status s) {
  switch (s) {
  case analysis::Status::Idle:
    return "Idle";
  case analysis::Status::Building:
    return "Building...";
  case analysis::Status::Done:
    return "Done";
  case analysis::Status::Cancelled:
    return "Cancelled";
  }
  return "?";
}

ImVec4 StatusColor(analysis::Status s) {
  switch (s) {
  case analysis::Status::Idle:
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f); // 灰
  case analysis::Status::Building:
    return ImVec4(0.2f, 0.7f, 1.0f, 1.0f); // 蓝
  case analysis::Status::Done:
    return ImVec4(0.2f, 0.8f, 0.4f, 1.0f); // 绿
  case analysis::Status::Cancelled:
    return ImVec4(0.9f, 0.6f, 0.2f, 1.0f); // 橙
  }
  return ImVec4(1.0f, 1.0f, 1.0f, 1.0f);
}

int RenderStreamControl(const analysis::StreamState &st, const SharedData &data, const char *unit) {
  const auto status = st.status.load(std::memory_order_acquire);
  const auto &sel = data.feature.selection;
  const bool is_l1 = sel.selected_level == static_cast<int>(analysis::kLevel);
  int action = 0;

  ImGui::BeginDisabled(status == analysis::Status::Building || sel.primary_feature_idx() < 0 || !is_l1);
  if (ImGui::Button("Compute"))
    action = 1;
  ImGui::EndDisabled();
  if (!is_l1) {
    ImGui::SameLine();
    ImGui::TextDisabled("(仅 L1)");
  }
  ImGui::SameLine();
  if (ImGui::Button("Cancel"))
    action = -1;

  ImGui::SameLine();
  ImGui::Text("Status: ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(StatusColor(status), "%s", StatusText(status));
  ImGui::SameLine(0, 0);
  ImGui::Text(" (%s %zu/%zu)", unit, st.done.load(std::memory_order_relaxed), st.total.load(std::memory_order_relaxed));
  return action;
}

// ============================================================================
// 值账目
// ============================================================================

ImVec4 GetMinMaxColor(float val) {
  if (val > 100.0f || val < -100.0f)
    return ImVec4(1.0f, 0.3f, 0.3f, 1.0f);
  return ImVec4(0.2f, 0.8f, 0.4f, 1.0f);
}

ImVec4 GetZeroPctColor(float pct) {
  if (pct >= 10.0f)
    return ImVec4(1.0f, 0.3f, 0.3f, 1.0f);
  if (pct >= 5.0f)
    return ImVec4(1.0f, 0.9f, 0.3f, 1.0f);
  return ImVec4(0.2f, 0.8f, 0.4f, 1.0f);
}

ImVec4 GetNanInfPctColor(float pct) {
  if (pct >= 1.0f)
    return ImVec4(1.0f, 0.3f, 0.3f, 1.0f);
  if (pct > 0.0f)
    return ImVec4(1.0f, 0.9f, 0.3f, 1.0f);
  return ImVec4(0.2f, 0.8f, 0.4f, 1.0f);
}

void RenderIntegrity(const analysis::Integrity &it) {
  auto item = [](const char *name, size_t n, const ImVec4 &color, float pct) {
    ImGui::Text("%s: %zu (", name, n);
    ImGui::SameLine(0, 0);
    ImGui::TextColored(color, "%.1f%%", pct);
    ImGui::SameLine(0, 0);
    ImGui::Text(")");
    ImGui::SameLine();
  };
  const float zero_pct = it.zero_pct(), nan_pct = it.nan_pct(), inf_pct = it.inf_pct();
  item("Zero", it.n_zero, GetZeroPctColor(zero_pct), zero_pct);
  item("NaN", it.n_nan, GetNanInfPctColor(nan_pct), nan_pct);
  item("+Inf", it.n_pos_inf, GetNanInfPctColor(inf_pct), inf_pct);
  item("-Inf", it.n_neg_inf, GetNanInfPctColor(inf_pct), inf_pct);

  // 无有效样本时 min/max 恒为 ±inf, 显示 --
  if (it.n_valid == 0) {
    ImGui::Text("Min: -- Max: --");
    return;
  }
  ImGui::Text("Min: ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(GetMinMaxColor(it.val_min), "%.2f", it.val_min);
  ImGui::SameLine();
  ImGui::Text("Max: ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(GetMinMaxColor(it.val_max), "%.2f", it.val_max);
}

// ============================================================================
// 绘图
// ============================================================================

void PlotHighlightLine(const float *x, const float *y, size_t n) {
  ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 5.0f);
  ImPlot::SetNextLineStyle(ImVec4(1, 1, 1, 1), 1.0f);
  ImPlot::PlotLine("##hl_outline", x, y, static_cast<int>(n));
  ImPlot::PopStyleVar();
  ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 3.0f);
  ImPlot::SetNextLineStyle(ImVec4(0, 1, 1, 1), 1.0f);
  ImPlot::PlotLine("##hl", x, y, static_cast<int>(n));
  ImPlot::PopStyleVar();
}

} // namespace GUI::Features
