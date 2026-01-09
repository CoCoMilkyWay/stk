// Tab Transform - Feature Transformation (Stationarity & Normalization)
//
// UI布局:
//   控制栏: Status | Level显示 | 数据块选择 | 时间拖动
//   左侧: 平稳化配置 (3种方法 + 参数拖动)
//   右侧: 归一化配置 (所有方法 + 参数拖动)
//   下方:
//     - ADF/KPSS热力图 (颜色编码)
//     - K线 + 原始/处理后特征对比
//     - 横截面PDF | FFT功率谱
//
#pragma once

struct SharedData;

namespace GUI::Features {

class TransformService;

// ============================================================================
// UI State
// ============================================================================

struct TransformUIState {
  // 参数变化标记 (触发重计算)
  bool params_changed = false;

  // Autofit trigger
  bool need_autofit = false;

  // 面板折叠状态
  bool config_expanded = true;
  bool heatmap_expanded = true;
  bool plots_expanded = true;

  // Autozoom 控制: 记录上次触发时的状态
  int last_autofit_asset = -2;           // -2 = 未初始化, -1 = ALL, >=0 = 单asset
  size_t last_autofit_generation = 0;    // 上次 autofit 时的 generation
};

// ============================================================================
// API
// ============================================================================

// Render tab - auto-spawns compute coroutine
void RenderTabTransform(TransformService *service, SharedData &data,
                        TransformUIState &ui);

// Stop coroutine on tab close
void StopTabTransform(TransformService *service, SharedData &data);

} // namespace GUI::Features
