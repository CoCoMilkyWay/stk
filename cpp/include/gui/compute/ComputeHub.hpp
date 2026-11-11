// ComputeHub: 特征计算面 (一个 ComputeService + 一张库表), Beta / Alpha 两个 Compute 页同一份; 一轮收尾即进程内重启, 读库方随之按新库重建.
// 一张库表分两页: Beta → Compute = all.bar (全市场根库, 算完即可 Style 调优 / 看 Market);
//                 Alpha → Compute = 其余全部库 (all.lob + 子池各档: 因子挖掘的张量)
#pragma once

#include "features/Storage/FeatureLevels.hpp" // LibId / PerLib
#include "gui/Tasks.hpp"                      // TaskStatus
#include "gui/compute/services/ComputeService.hpp"

#include <cstdint>
#include <memory>
#include <optional>

struct SharedData;

namespace GUI::Compute {

enum class LibSide { Beta,
                     Alpha };
inline bool on_side(LibSide s, LibId l) { return (l == kAllBar) == (s == LibSide::Beta); }

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

  std::optional<LibId> confirm;        // 待确认全量计算的库 (空 = 无确认弹窗)
  std::optional<LibId> confirm_remove; // 待确认删除的库 (空 = 无确认弹窗)

  // 库表 (plan_libs; 未扫 = 空): 一行一池, 每池各档一库
  std::optional<PerLib<LibPlan>> libs;
  bool libs_dirty = true;    // 下一帧重扫
  uint64_t generation = 0;   // 对应的 Asset::generation (数据换进即重扫)
  char pool_filter[32] = {}; // 库表行过滤 (池名 / 中文名子串; 空 = 全部)

  // 自动续算: 数据换进即起, 依次 Append 可续算且未判废的库 (一轮收尾即进程内重启, 新一代接着找); 无可续算 / 取消即止
  bool auto_append = false;

  // 起一轮 (弹窗状态机从 ShowingPopup 走)
  void begin(const LibPlan &s, bool resume);
};

// CreateAllTasks 建, 三任务各持一个 shared_ptr
struct ComputeHub {
  std::unique_ptr<ComputeService> service; // 输入就绪即建 (自动续算不等进页)
  ComputeState state;
  bool inputs_ready = false; // 数据库扫描完 + 回测日历就绪 (帧首 TickCompute 写)
};

// 每帧 (Beta Update, 不论 Compute 页画没画): 建服务 + 弹窗状态机 (一轮收尾 → 请求进程内重启) + 库表重扫 + 自动续算 + task_state.compute. 输入未就绪 → 只走状态机
void TickCompute(ComputeHub &hub, SharedData &data);

// 左栏 Compute 子行标签 (本页库集): 有库被读端判废 → pending; 在算 / 刚算完的库在本页 → 进度
TaskStatus ComputeStatus(const ComputeHub &hub, const SharedData &data, LibSide side);

} // namespace GUI::Compute