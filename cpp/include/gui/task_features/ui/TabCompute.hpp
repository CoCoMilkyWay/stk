// Tab Compute - Feature Computation Control Panel
// Controls multi-threaded feature extraction (TS + CS + IO workers)
#pragma once

#include "features/Storage/FeatureLevels.hpp" // TierId
#include "gui/task_features/services/ComputeService.hpp"
#include "misc/date.hpp"
#include "shared/AssetInfo.hpp" // StockDaysVec

#include <string>
#include <vector>

struct Config;

namespace GUI::Features {

// ============================================================================
// Compute Tab State
// ============================================================================

// 库状态行 (库清单 + 已落盘日数; 扫盘有成本, 只在进页 / 一轮算完 / Refresh 时重扫)
struct LibStatus {
  std::string name;
  TierId tier = TierId::Lob;
  bool ready = false; // 有清单 (已建库)
  misc::Ymd first = 0, last = 0;
  size_t lag = 0;                        // T-lag: 交易日历里 last 之后的交易日数
  size_t assets = 0, days = 0, done = 0; // A 轴 / 日期轴长 / 已落盘日
};

enum class ComputeUIState {
  Idle,         // Waiting for user to click start
  ShowingPopup, // Popup displayed, rendering first frame
  WaitingStart, // Triggered, waiting for computation to start
  Computing     // Computation running (GUI frozen)
};

struct ComputeState {
  // 核布局由机器核数 + 按档截面核数推导 (StageLayout::make)
  ComputeConfig config;

  // State machine
  ComputeUIState ui_state = ComputeUIState::Idle;
  int popup_frame_count = 0; // Frame counter to ensure popup is rendered

  // Trigger for starting compute (set by UI, consumed by TaskFeatures)
  bool trigger_start = false;

  // 池表: all + 各池 (行序 = all 在前, 池按字段表序)
  std::vector<LibStatus> libs;
  bool libs_dirty = true; // 下一帧重扫
};

// ============================================================================
// Render Function
// ============================================================================

void RenderTabCompute(
    ComputeService *service,
    ComputeState &state,
    const StockDaysVec &stock_days,
    Config &config);

} // namespace GUI::Features
