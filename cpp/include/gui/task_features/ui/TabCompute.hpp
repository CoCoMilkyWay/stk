// Tab Compute - Feature Computation Control Panel
// Controls multi-threaded feature extraction (TS + CS + IO workers)
#pragma once

#include "features/Storage/FeatureLevels.hpp" // TierId
#include "gui/task_features/services/ComputeService.hpp"
#include "misc/date.hpp"

#include <string>
#include <vector>

struct Asset;
struct Config;

namespace GUI::Features {

// ============================================================================
// Compute Tab State
// ============================================================================

// 库状态行 (库清单 + 已落盘日数; 扫盘有成本, 只在进页 / 一轮算完 / Refresh 时重扫)
struct LibStatus {
  std::string name;
  TierId tier = TierId::Lob;
  bool ready = false; // 有清单 (已建库)
  misc::Ymd first = 0, last = 0;
  size_t assets = 0, days = 0, done = 0; // A 轴 / 日期轴长 / 已落盘日
};

enum class ComputeUIState {
  Idle,         // Waiting for user to click start
  ShowingPopup, // Popup displayed, rendering first frame
  WaitingStart, // Triggered, waiting for computation to start
  Computing     // Computation running (GUI frozen)
};

struct ComputeState {
  // worker 数不可配: 核布局由机器核数唯一推导 (StageLayout::make)
  ComputeConfig config;

  // State machine
  ComputeUIState ui_state = ComputeUIState::Idle;
  int popup_frame_count = 0; // Frame counter to ensure popup is rendered

  // Trigger for starting compute (set by UI, consumed by TaskFeatures)
  bool trigger_start = false;

  // 库表: all + 各选池 (行序 = all 在前, 选池按字段表序)
  std::vector<LibStatus> libs;
  bool libs_dirty = true; // 下一帧重扫
};

// ============================================================================
// Render Function
// ============================================================================

void RenderTabCompute(
    ComputeService *service,
    ComputeState &state,
    Asset &asset,
    Config &config);

} // namespace GUI::Features
