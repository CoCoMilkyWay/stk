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
  // Selected dimension: 0=MONTH, 1=WEEKDAY, 2=HOUR, 3=ASSETS
  int selected_dimension = 0;

  // Month focus slider (index into available months)
  int focus_month_idx = 0;

  // Autofit trigger (set when compute completes)
  bool need_autofit = false;
};

// ============================================================================
// API
// ============================================================================

// Render tab - auto-spawns compute coroutine
void RenderTabDist(DistService *service, SharedData &data, DistUIState &ui);

// Stop coroutine on tab close
void StopTabDist(DistService *service, SharedData &data);

} // namespace GUI::Features
