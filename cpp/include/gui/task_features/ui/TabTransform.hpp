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

#include <cstddef>
#include <cstdint>

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
  uint64_t last_autofit_generation = 0;  // 上次 autofit 时的 generation

  // 光标位置 (样本索引，两个 plot 共享)
  double anchor_x = 0.0;

  // 光标缓存 (避免每帧重复计算)
  struct AnchorCache {
    size_t idx = SIZE_MAX;           // 缓存时的索引
    uint64_t generation = 0;         // 缓存时的 generation
    int selected_asset = -2;         // 缓存时的选中 asset
    double raw_y = 0.0;              // 原始特征 y 值
    double norm_y = 0.0;             // 归一化特征 y 值
    char time_str[32] = {};          // 时间字符串
    bool valid = false;              // 缓存是否有效
  } anchor_cache;

  // 渲染缓存: 记录上次有效的 generation
  uint64_t last_rendered_generation = 0;

  // 带通光标 (非标 bin 索引, 0-127, PSD plot 上拖动)
  double bandpass_lo = 20.0;
  double bandpass_hi = 80.0;

  // 特征图 axis limits 同步 (plot1 和 plot2 共享 x 轴)
  struct FeaturePlotLimits {
    // 上次读取到的每个 plot 的 limits [0]=Raw, [1]=Proc
    double last_x_min[2] = {0.0, 0.0};
    double last_x_max[2] = {1.0, 1.0};
    
    // 需要同步的目标 limits (从另一个 plot 更新而来)
    double sync_x_min = 0.0;
    double sync_x_max = 1.0;
    
    // 哪个 plot 需要同步: -1=无, 0=Raw, 1=Proc
    int sync_target = -1;
  } feature_limits;
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
