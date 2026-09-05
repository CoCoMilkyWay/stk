// Tab Compute - Feature Computation Control Panel
// Controls multi-threaded feature extraction (TS + CS + IO workers)
#pragma once

#include "gui/task_features/services/ComputeService.hpp"

struct Asset;
struct Config;

namespace GUI::Features {

// ============================================================================
// Compute Tab State
// ============================================================================

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
  // worker 数不可配: 核布局由机器核数唯一推导 (StageLayout::make)
  ComputeConfig config;

  // State machine
  ComputeUIState ui_state = ComputeUIState::Idle;
  int popup_frame_count = 0; // Frame counter to ensure popup is rendered

  // Trigger for starting compute (set by UI, consumed by TaskFeatures)
  bool trigger_start = false;
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
