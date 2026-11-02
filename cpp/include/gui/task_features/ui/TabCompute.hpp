// Tab Compute - Feature Computation Control Panel
// Controls multi-threaded feature extraction (TS + CS + IO workers)
#pragma once

#include "features/Storage/FeatureLevels.hpp" // LibId / PerLib
#include "gui/task_features/services/ComputeService.hpp"

#include <cstdint>
#include <optional>
#include <vector>

struct SharedData;

namespace GUI::Features {

// ============================================================================
// Compute Tab State
// ============================================================================

enum class ComputeUIState {
  Idle,         // Waiting for user to click start
  ShowingPopup, // Popup displayed, rendering first frame
  WaitingStart, // Triggered, waiting for computation to start
  Computing     // Computation running (GUI frozen)
};

struct ComputeState {
  // 核布局由机器核数 + 按档截面核数推导 (StageLayout::make)
  ComputeConfig config;

  // State machine
  ComputeUIState ui_state = ComputeUIState::Idle;
  int popup_frame_count = 0; // Frame counter to ensure popup is rendered

  std::optional<LibId> confirm; // 待确认全量计算的库 (空 = 无确认弹窗)

  // 库表 (plan_libs; 未扫 = 空): 一行一池, 每池各档一库
  std::optional<PerLib<LibPlan>> libs;
  bool libs_dirty = true;    // 下一帧重扫
  uint64_t generation = 0;   // 对应的 Asset::generation (数据换进即重扫)
  char pool_filter[32] = {}; // 库表行过滤 (池名 / 中文名子串; 空 = 全部)

  // 自动续算: 数据换进即起一轮, 依次 Append 可续算且未判废的库; 无可续算 / 取消即止
  bool auto_append = false;
  std::vector<LibId> auto_done; // 本轮已自动 Append 过的库 (每库至多一次)
};

// ============================================================================
// Render Function
// ============================================================================

// 每帧 (TaskFeatures Update, 不论 Compute 页画没画): 弹窗状态机 + 库表重扫 + 自动续算. inputs_ready 假 → 只走状态机
void TickCompute(ComputeService &service, ComputeState &state, SharedData &data, bool inputs_ready);

void RenderTabCompute(
    ComputeService *service,
    ComputeState &state,
    SharedData &data);

} // namespace GUI::Features
