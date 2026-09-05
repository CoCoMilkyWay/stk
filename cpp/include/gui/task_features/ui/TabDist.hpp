// TabDist - Distribution Analysis Tab (KLL-based, 分批流式)
//
// UI Layout:
//   1. Integrity panel - Zero/NaN/Inf counts
//   2. Window control - Compute | Cancel | Status (天进度) | Month slider
//   3. Left: Color Mode Selector + Hovered Asset Info | Right: PDF evolution (3 panels)
//   4. Assets PDF - 消费 dist.lines 发布快照 (绘制子集), 零计算只画
//
// Threading:
//   - UI runs on main thread, 渲染帧内持 dist.mutex
//   - Computation via DistService 单 worker 线程 (分批流式, 批末发布快照)
#pragma once

#include <cstdint>
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

  // Autofit: 任何新发布 epoch (= 数据变了) 即跟随, 稳态把缩放还给用户
  bool need_autofit = false;
  uint64_t last_lines_epoch = 0;

  // 跨帧 hover 的线 (dist.lines 下标; 资产截面图输出, 左栏详情面板消费)
  int hovered_line = -1;

  // config 区间月份表缓存 (滑条每帧要用, 日期变了才重算)
  std::vector<std::string> months;
  std::string months_key; // start_date + "|" + end_date

  // 行业色缓存 (资产表静态, 构建一次): 一个行业一个颜色
  std::vector<int> industry_idx;           // [A], -1 = 未知
  std::vector<std::string> industry_names; // [n_industries]

  // 资产截面图帧内缓冲 (可画线的 dist.lines 下标 + 归一化 W2)
  std::vector<size_t> line_indices;
  std::vector<float> w2_norm;

  // 图4 顶部散点 + PDF 折线的染色模式:
  // 0=行业(Jet), 1=市值, 2=PE, 3=PB, 4=PS, 5=PCF, 6=股息率(连续值用 Viridis)
  int color_mode = 0;
  // 连续值缓存 (资产表静态, 每模式构建一次): [A], NaN = 缺失
  std::vector<float> color_values;
  float color_lo = 0.0f, color_hi = 1.0f; // 5/95 分位 winsorize 范围
  int color_cache_mode = -1;              // -1 = 未构建
};

// ============================================================================
// API
// ============================================================================

// Render tab - auto-spawns compute coroutine
void RenderTabDist(DistService *service, SharedData &data, DistUIState &ui);

// Stop coroutine on tab close
void StopTabDist(DistService *service, SharedData &data);

} // namespace GUI::Features
