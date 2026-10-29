#pragma once

// ============================================================================
// Task State - 统一的任务状态管理 (纯数据, 跨任务通信面)
// 各 Task 写入自己的状态，读取其他 Task 的状态
// 通过 data.taskstate.xxx 直接访问，无需设计 API
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
    // 关键状态标志 (其他 Task 可读取)
    bool binary_scanned = false; // 二进制数据库已扫描
    bool all_json_ready = false; // AssetInfo 已从 parquet 构建

    // 流水线在飞 (StateManager 每帧写)
    bool json_update_inflight = false;
    bool l2_scan_inflight = false;
    bool encode_inflight = false; // 编码改写日文件: 在飞时特征计算不开跑
  } database;

  // ==========================================================================
  // Features Task
  // ==========================================================================
  struct Features {
    bool computing = false;     // 特征计算在飞 (点下 Compute 起到收尾): 共享数据冻结 (Database 件留邮箱, 不发编码)
    bool has_selection = false; // 主 feature 已选好
  } features;
};
