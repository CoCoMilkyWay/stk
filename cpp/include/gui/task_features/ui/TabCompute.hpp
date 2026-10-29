// Tab Compute - Feature Computation Control Panel
// Controls multi-threaded feature extraction (TS + CS + IO workers)
#pragma once

#include "features/Storage/FeatureLevels.hpp" // TierId
#include "gui/task_features/services/ComputeService.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Features {

// ============================================================================
// Compute Tab State
// ============================================================================

// 库状态行 (库清单 + 已落盘日数 + 规划; 扫盘有成本, 只在进页 / 一轮算完 / 数据换进 / Refresh 时重扫)
struct LibStatus {
  std::string name;
  TierId tier = TierId::Lob;
  bool ready = false; // 有清单 (已建库)
  misc::Ymd first = 0, last = 0;
  size_t lag = 0;                        // 未计算: 交易日历里 last 之后的交易日数
  size_t assets = 0, days = 0, done = 0; // A 轴 / 日期轴长 / 已落盘日
  size_t outside = 0;                    // 轴外资产数 (FeatureLib::outside; 要纳入就全量重算)
  bool resumable = false;                // 续算点对得上清单 (见 ComputeService resumable)
  std::vector<misc::Ymd> dates;          // 库日期轴
  std::vector<misc::Ymd> plan;           // 现在重算的日期轴 (见 source_dates; 空 = 不可算), 末日 > last 即落后
  std::string side_partial;              // 未落盘齐的旁读池 (非空 ⇒ plan 因它清空)
  bool complete() const { return ready && done == days; }
  // 可续算: 落盘齐 + 续算点在 + 库日期轴是规划的真前缀 (有新日)
  bool appendable() const {
    return complete() && resumable && plan.size() > dates.size() && std::equal(dates.begin(), dates.end(), plan.begin());
  }
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
  std::string confirm_pool; // 待确认全量计算的池 (空 = 无确认弹窗)

  // 池表: all + 各池 (行序 = all 在前, 池按字段表序)
  std::vector<LibStatus> libs;
  bool libs_dirty = true;  // 下一帧重扫
  uint64_t generation = 0; // 对应的 Asset::generation (数据换进即重扫)
};

// ============================================================================
// Render Function
// ============================================================================

void RenderTabCompute(
    ComputeService *service,
    ComputeState &state,
    SharedData &data);

} // namespace GUI::Features
