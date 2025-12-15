// TabDist - Distribution Analysis Tab (KLL-based Monthly View)
//
// UI Layout:
//   1. Integrity panel - Zero/NaN/Inf counts
//   2. Window control - By selector | Status (n/m) | Compute | Cancel | Month slider
//   3. Moments panel (color bands) + PDF evolution (side by side)
//   4. Trajectory plot - Asset distribution evolution
//
// Threading:
//   - UI runs on main thread
//   - Computation via coroutine -> thread pool (one thread per month)
#pragma once

struct SharedData;

namespace GUI::Features {

class DistService;

// ============================================================================
// UI State
// ============================================================================

struct DistUIState {
  // Grouping: 0=NONE, 1=HOUR, 2=WEEKDAY, 3=MONTH
  int group_by = 3;

  // Month focus slider (index into available months)
  int focus_month_idx = 0;

  // Hovered asset in trajectory (for tooltip)
  int hovered_asset = -1;
};

// ============================================================================
// API
// ============================================================================

// Render tab - auto-spawns compute coroutine
void RenderTabDist(DistService *service, SharedData &data, DistUIState &ui);

// Stop coroutine on tab close
void StopTabDist(DistService *service, SharedData &data);

} // namespace GUI::Features
