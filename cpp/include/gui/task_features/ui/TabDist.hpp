// TabDist - Distribution Analysis Tab (KLL-based, 资产优先流式)
//
// UI Layout:
//   1. Integrity panel - Zero/NaN/Inf counts
//   2. Window control - Compute | Cancel | Status (IO/资产进度) | Month slider
//   3. Moments panel (color bands) + PDF evolution (side by side)
//   4. Assets PDF - 流式渲染已完成资产前缀, stability 完成后叠加 W2/Ward
//
// Threading:
//   - UI runs on main thread, 渲染帧内持 dist.mutex
//   - Computation via DistService 单 worker 线程 (资产维度流式发布)
#pragma once

#include <string>
#include <vector>

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

  // 行业色缓存 (资产表静态, 构建一次): 一个行业一个颜色
  std::vector<int> industry_idx;           // [A], -1 = 未知
  std::vector<std::string> industry_names; // [n_industries]
};

// ============================================================================
// API
// ============================================================================

// Render tab - auto-spawns compute coroutine
void RenderTabDist(DistService *service, SharedData &data, DistUIState &ui);

// Stop coroutine on tab close
void StopTabDist(DistService *service, SharedData &data);

} // namespace GUI::Features
