// TabDist - Distribution Analysis Tab
// Displays comprehensive distribution statistics and visualizations
// for the primary feature across all assets
//
// Lifecycle:
//   - RenderTabDist: spawns compute coroutine on first call
//   - StopTabDist: stops compute coroutine when tab closes
#pragma once

struct SharedData;

namespace GUI::Features {

class DistService;

// ============================================================================
// Distribution Tab UI State
// ============================================================================

struct DistUIState {
  // Time grouping selection
  int selected_grouping = 0; // 0=NONE, 1=HOUR, 2=WEEKDAY, 3=MONTH, 4=YEAR
  
  // PDF sensitivity slider
  float pdf_sensitivity_ui = 1.0f;
  
  // Display options
  bool show_by_asset = false;
  bool show_trajectory = true;
  bool show_quantile_heatmap = true;
  
  // Selected bin for detail view
  int selected_bin_idx = 0;
  
  // Asset filter
  bool show_asset_filter = false;
  char asset_filter_buffer[256] = {0};
};

// ============================================================================
// Render Functions
// ============================================================================

// Render distribution analysis tab - spawns compute coroutine on first call
void RenderTabDist(DistService *service, SharedData &data, DistUIState &ui_state);

// Stop compute coroutine - call when tab is closed
void StopTabDist(DistService *service, SharedData &data);

} // namespace GUI::Features

