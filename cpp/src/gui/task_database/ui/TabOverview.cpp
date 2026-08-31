// Tab Overview - 基本面数据源 (BigQuant + Tushare) 状态与同步入口
// 数据落地 = output/fundamental/YYYY-MM/*.parquet (水位增量, 常量见 shared/Config.hpp);
// AssetInfo 由 FundamentalService 从 parquet 构建, 此处只渲染状态 + 回写按钮 flag.
#include "gui/task_database/ui/TabOverview.hpp"
#include "imgui.h"

namespace GUI::Database {

namespace {

ImVec4 status_color(FundamentalStatus s) {
  switch (s) {
  case FundamentalStatus::Ready:
    return ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
  case FundamentalStatus::Updating:
  case FundamentalStatus::Building:
    return ImVec4(1.0f, 0.95f, 0.3f, 1.0f);
  case FundamentalStatus::Error:
    return ImVec4(1.0f, 0.3f, 0.2f, 1.0f);
  case FundamentalStatus::Idle:
    return ImVec4(0.7f, 0.7f, 0.7f, 1.0f);
  }
  return ImVec4(0.7f, 0.7f, 0.7f, 1.0f);
}

} // namespace

void RenderTabOverview(
    const FundamentalState &state,
    bool *update_clicked,
    bool *refresh_scan_clicked,
    bool disable_update_controls,
    bool disable_scan_controls) {

  ImGui::Spacing();
  ImGui::SeparatorText("Fundamental Data (BigQuant DAI + Tushare)");

  // 状态行
  ImGui::Text("Status:");
  ImGui::SameLine();
  ImGui::TextColored(status_color(state.status),
                     "[%s]", GetFundamentalStatusName(state.status));
  if (!state.message.empty()) {
    ImGui::SameLine();
    ImGui::TextDisabled("%s", state.message.c_str());
  }
  if (state.status == FundamentalStatus::Updating ||
      state.status == FundamentalStatus::Building) {
    // 不确定进度条 (总量未知: 水位增量按表滚动)
    float t = static_cast<float>(ImGui::GetTime());
    ImGui::ProgressBar(-1.0f * t, ImVec2(-1.0f, 0.0f), "syncing...");
  }

  ImGui::Spacing();

  // 构建结果统计
  if (state.status == FundamentalStatus::Ready) {
    ImGui::Text("Stocks: %zu", state.stock_count);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::Text("Adjust-factor series: %zu", state.factor_stock_count);
    ImGui::SameLine(0.0f, 24.0f);
    ImGui::Text("Trading days: %zu", state.trading_days_count);

    if (!state.date_range_start.empty()) {
      ImGui::Text("Calendar range: %s ~ %s", state.date_range_start.c_str(),
                  state.date_range_end.c_str());
    }
    if (!state.last_update.empty()) {
      ImGui::TextDisabled("Last build: %s", state.last_update.c_str());
    }
  }

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // 操作区
  ImGui::BeginDisabled(disable_update_controls);
  if (ImGui::Button("Update (sync + rebuild)", ImVec2(220.0f, 0.0f))) {
    *update_clicked = true;
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
    ImGui::SetTooltip("pending 判定 → BigQuant/Tushare 水位增量同步 → 重建 AssetInfo\n"
                      "已到水位时零网络, 秒级完成");
  }

  ImGui::SameLine();
  ImGui::BeginDisabled(disable_scan_controls);
  if (ImGui::Button("Refresh Scan State", ImVec2(180.0f, 0.0f))) {
    *refresh_scan_clicked = true;
  }
  ImGui::EndDisabled();
}

} // namespace GUI::Database
