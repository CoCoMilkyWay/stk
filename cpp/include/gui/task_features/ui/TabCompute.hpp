// Tab Compute - Feature Computation Control Panel
// Controls multi-threaded feature extraction (TS + CS + IO workers)
#pragma once

// Forward declarations
namespace GUI::Features {
class ComputeService;
struct ComputeProgress;
} // namespace GUI::Features

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
  int num_workers = 0; // 0 means auto-detect (use max cores)

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
