// TabTimeSeries - Time Series Analysis Tab
// ============================================================================
//
// 特征时序诊断 (不建模, 只看记忆结构):
//
// UI布局:
//   控制栏: Status (done/total)
//   左侧: 诊断步骤面板 (3 个 Step, 可点击切换)
//   右侧: 可视化面板 (根据选中Step切换图表)
//
// Step与图表对应:
//   Step 0: 平稳性检验 → ADF / KPSS 热力图 (月 × 资产)
//   Step 1: 频域分析   → PSD 热力图 (日 × 尺度) + 单日功率谱
//   Step 2: 自相关     → ACF + PACF 双图 (含置信带)
//
// ============================================================================
#pragma once

struct SharedData;

namespace GUI::Features {

class TimeSeriesService;

// ============================================================================
// UI State
// ============================================================================

struct TimeSeriesUIState {
  // 当前选中的Step (0-2)
  int selected_step = 0;

  // Autofit trigger (set when compute completes)
  bool need_autofit = false;
};

// ============================================================================
// API
// ============================================================================

// Render tab - displays time series analysis workflow
void RenderTabTimeSeries(TimeSeriesService *service, SharedData &data,
                         TimeSeriesUIState &ui);

// Stop computation on tab close
void StopTabTimeSeries(TimeSeriesService *service, SharedData &data);

} // namespace GUI::Features
