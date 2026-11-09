#pragma once

// ============================================================================
// Task State - 统一的任务状态管理 (纯数据, 跨任务通信面)
// 各 Task 写入自己的状态，读取其他 Task 的状态
// 通过 data.task_state.xxx 直接访问，无需设计 API
// 显示层 (文字/颜色) 在各任务的 TaskHandle::Status 回调里 (见 gui/Tasks.hpp)
// ============================================================================

struct TaskState {
  // ==========================================================================
  // Settings Task
  // ==========================================================================
  struct Settings {
    enum class Status { None,
                        Initializing,
                        Syncing,
                        Synced };
    Status status = Status::None;
    bool initialized = false;
  } settings;

  // ==========================================================================
  // Database Task
  // ==========================================================================
  struct Database {
    // 产物就绪 (其他 Task 可读取)
    bool asset_info_ready = false; // AssetInfo 已从 parquet 构建并换进
    bool scanned = false;          // orders / 归档覆盖扫描至少收过一件

    // 流水线在飞 (StateManager 每帧写)
    bool sync_inflight = false;
    bool scan_inflight = false;
    bool encode_inflight = false; // 编码改写日文件: 在飞时特征计算不开跑
  } database;

  // ==========================================================================
  // Beta / Alpha Task (两个 Compute 页同一个特征计算服务)
  // ==========================================================================
  struct Compute {
    bool computing = false; // 特征计算在飞 (点下 Rebuild / Append 起到收尾): 共享数据冻结 (Database 件留邮箱, 不发编码)
  } compute;

  // ==========================================================================
  // Features Task
  // ==========================================================================
  struct Features {
    bool has_selection = false; // 主 feature 已选好
  } features;
};
