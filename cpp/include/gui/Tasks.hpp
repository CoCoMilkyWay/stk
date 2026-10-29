#pragma once
#include "shared/Progress.hpp"

#include <functional>
#include <memory>
#include <string>
#include <vector>

struct SharedData;
struct ImVec4;

namespace GUI {

// ============================================================================
// 状态标签: 左栏每行名字后的 [text] 彩色标签.
// 文字各任务自定义 (支持动态进度如 "encoding 45%"), 颜色只由 Kind 决定 ——
// 全局一张色表 (StatusColor), 不允许各任务自配颜色.
// ============================================================================
struct TaskStatus {
  enum class Kind {
    None,  // 无标签
    Muted, // 灰: 前置未满足 / 已取消 / 背景常态 (waiting / cancelled / live)
    Busy,  // 紫: 进行中, 动词 -ing (+ 百分比) (syncing / encoding 45% / cancelling)
    Warn,  // 黄: 需要用户点一下 (pending / selecting / confirm xxx / N broken)
    Error, // 红: 出错 (error / N failed)
    Ready, // 绿: 产物齐全 (done)
  };
  Kind kind = Kind::None;
  std::string text;
};

ImVec4 StatusColor(TaskStatus::Kind kind);

// 后台进度 (shared/Progress.hpp) 的唯一展示口径, 全部 Service 共用:
//   PhaseKind:      Idle / Cancelled 灰 / 忙 紫 / Done 绿
//   ProgressStatus: 行标签 = 阶段名 (+ 有总量时百分比); Idle 无标签. 带业务结论的 Done (broken / failed) 由调用方先判
//   TextPhase:      tab 内一段彩色阶段名
TaskStatus::Kind PhaseKind(Phase p);
TaskStatus ProgressStatus(const Progress &p);
void TextPhase(Phase p);

// ============================================================================
// 任务节点: 左栏一行 (tabs 空 = 任务本身是叶子) 或一个可展开父节点 (tabs = 子叶子).
//
// 行接口三件套统一用 idx 寻址 (最大化对仗): idx == -1 任务行, idx >= 0 子行.
//   Status(data, idx)  -> 该行状态标签 (缺省无标签; 有子项任务只答子行, 任务行由 RowStatus 聚合)
//   Enabled(data, idx) -> 该行是否可点 (缺省可点)
//   Draw(data, idx)    -> 右栏内容 (选中行才调; 无子项任务 idx == -1)
//
// 生命周期 (对仗):
//   Init(data)    创建后立即一次, 与选中态解耦 (后台任务提前起)
//   Update(data)  每帧一次, 无论是否选中 —— 状态标签/使能在这里更新,
//                 左栏渲染同帧读取 (无一帧延迟, 无 "打开页面才更新" 的陈旧标签)
//   Draw(data, idx) 每帧一次, 仅选中任务 —— 右栏渲染 + tab 级生命周期/自动触发
//   OnExpand/OnCollapse 跨任务切换时成对触发
//   Destroy       清理一次
// ============================================================================
struct TaskHandle {
  std::string name;
  std::vector<std::string> tabs; // 空 = 任务本身是叶子
  int selected_tab = -1;         // 当前选中子行, -1 = 无子项

  // 生命周期
  std::function<void(SharedData &)> Init;
  std::function<void(SharedData &)> Update;
  std::function<void()> OnExpand;
  std::function<void()> OnCollapse;
  std::function<void()> Destroy;

  // 行接口
  std::function<TaskStatus(const SharedData &, int)> Status;
  std::function<bool(const SharedData &, int)> Enabled;
  std::function<void(SharedData &, int)> Draw;

  std::shared_ptr<void> storage;
};

// 左栏行标签的唯一入口. 有子项任务的任务行 = 子行聚合: 档位 Busy > Error > Warn > Ready > Muted
// 取最高档, 同档取首个子行的文字; Ready 档统一显示 done
TaskStatus RowStatus(const SharedData &data, const TaskHandle &t, int idx);

// ============================================================================
// 任务树: 左栏全部状态收敛于此 (tasks + 唯一选中态), Run_* 只持有一个 TaskTree.
// 选中叶子 = tasks[selected] + 其 selected_tab.
// ============================================================================
struct TaskTree {
  std::vector<TaskHandle> tasks;
  int selected = 0;

  // 切换选中叶子: 跨任务触发旧 OnCollapse / 新 OnExpand, 同任务内切 tab 不触发
  void Select(int task_idx, int tab_idx);
};

// Create task tree (Init 按 push 顺序立即触发一遍, 顺序即依赖: Settings 先落盘配置到内存)
TaskTree CreateAllTasks(SharedData &data);

// Cleanup tasks
void CleanupAllTasks(TaskTree &tree);

// Reinitialize all tasks (cleanup + recreate)
void ReinitAllTasks(TaskTree &tree, SharedData &data);

} // namespace GUI
