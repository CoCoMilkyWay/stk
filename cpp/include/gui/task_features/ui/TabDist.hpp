// TabDist - Distribution Analysis Tab (KLL-based, 分批流式)
//
// UI Layout:
//   1. Integrity panel - Zero/NaN/Inf counts
//   2. Window control - Compute | Cancel | Status (天进度) | 通用焦点滑条 (按选中维度切换)
//   3. Left: Color Mode Selector + Asset Info (hover 优先, 否则焦点资产) | Right: PDF 三维度 (月/周/日内)
//   4. Assets PDF - 消费 dist.lines 发布快照 (绘制子集), 零计算只画
//
// 四维度对仗 (点图选中 → 滑条切到该维度的焦点):
//   0 月度漂移 (焦点 = 月)  1 周内偏移 (焦点 = 星期)  2 日内偏移 (焦点 = 10 分钟桶)  3 资产截面 (焦点 = 资产)
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
  static constexpr int kDims = 4;

  // Selected dimension: 0=MONTH, 1=WEEKDAY, 2=TOD, 3=ASSETS (滑条作用于该维度)
  int selected_dimension = 0;

  // 各维度的焦点槽 (滑条值, 每维独立记忆): 月下标 / 星期 / 日内桶 / 资产下标
  int focus[kDims] = {0, 0, 0, 0};
  // 滑条按住中 (本帧): 选中维度的图进入高亮模式 (焦点线置顶, 其余线压暗), 与图4 hover 同一套
  bool focus_active = false;

  // 待 autofit (新发布 epoch = 数据变了 即置位), 稳态把缩放还给用户.
  // 粘滞: 不按帧清, 等该图真正画上数据那帧才消费 —— reset 只 +epoch 不填数据 (首次构建槽表
  // 全空), 按帧清会把 fit 浪费在空图上. 四维就绪时机不同, 与 focus 同样逐维记账.
  bool fit[kDims] = {};
  uint64_t last_epoch = 0; // 数据版本 (reset/clear/每批发布 +1), 跨构建单调

  // 跨帧 hover 的线 (dist.lines 下标; 资产截面图输出, 左栏详情面板消费; 无 hover 时详情落到 focus[3])
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
