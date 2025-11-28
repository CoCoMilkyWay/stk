// Tab Compute - Feature Computation Control Panel
// Controls multi-threaded feature extraction (TS + CS + IO workers)
#pragma once

// Forward declarations
namespace GUI::Features {
class ComputeService;
struct ComputeProgress;
} // namespace GUI::Features

struct Asset;

namespace GUI::Features {

// ============================================================================
// Compute Tab State
// ============================================================================

struct ComputeState {
  int num_workers = 0; // 0 means auto-detect (use max cores)

  // Compute dialog states
  bool show_warning_popup = false;
  float warning_popup_timer = 0.0f; // Auto-close timer
  
  // Trigger for starting compute (set by UI, consumed by TaskFeatures)
  bool trigger_start = false;
};

// ============================================================================
// Render Function
// ============================================================================

void RenderTabCompute(
    ComputeService *service,
    ComputeState &state,
    Asset &asset);

} // namespace GUI::Features
