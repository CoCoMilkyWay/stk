// TabTimeSeries - Time Series Analysis Tab
// ============================================================================
//
// 时序分析流程 (SARIMA + GARCH 框架):
//   目标: 如果存在稳定可预测的成分，剥离它，减少与其他特征的虚假相关性
//
// UI布局:
//   控制栏: Compute | Cancel | Status (step/5)
//   左侧: 检验流程面板 (5个Step，可点击切换)
//   右侧: 可视化面板 (根据选中Step切换图表)
//
// Step与图表对应:
//   Step 0: 平稳性检验 → 原序列 + 去趋势/去季节后序列
//   Step 1: 频域分析   → Power Spectrum + Q因子标注
//   Step 2: ARMA建模   → ACF + PACF 双图 (含置信区间)
//   Step 3: 残差分析   → Q-Q图 + 残差时序 + CUSUM
//   Step 4: 时间衰减   → Gini(t) / HHI(t) / RankCorr(t)
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
  // 当前选中的Step (0-5)
  int selected_step = 0;

  // Autofit trigger (set when compute completes)
  bool need_autofit = false;
};

// ============================================================================
// API
// ============================================================================

// Render tab - displays time series analysis workflow
void RenderTabTimeSeries(SharedData &data, TimeSeriesUIState &ui);

// Stop computation on tab close
void StopTabTimeSeries(SharedData &data);

} // namespace GUI::Features

