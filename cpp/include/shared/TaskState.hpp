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
                        Writing,
                        Synced };
    Status status = Status::None;
    bool initialized = false;
  } settings;

  // ==========================================================================
  // Database Task
  // ==========================================================================
  struct Database {
    enum class Status { None,
                        Initializing,
                        Syncing,  // 基本面同步中 (json_update_inflight)
                        Scanning, // L2 覆盖扫描中 (l2_scan_inflight)
                        NotScanned,
                        Incomplete,
                        Error,
                        Ready };
    Status status = Status::NotScanned;

    // 关键状态标志 (其他 Task 可读取)
    bool binary_scanned = false; // 二进制数据库已扫描
    bool binary_pass = false;    // 覆盖检查通过
    bool all_json_ready = false; // AssetInfo 已从 parquet 构建

    // 便捷方法
    bool ready() const { return binary_pass && all_json_ready; }

    // 内部握手信号 (防止并发操作)
    bool json_update_inflight = false;
    bool l2_scan_inflight = false;
  } database;

  // ==========================================================================
  // Features Task
  // ==========================================================================
  struct Features {
    enum class Status { None,
                        Waiting,
                        Selecting,
                        Computing,
                        Ready,
                        Error };
    Status status = Status::None;

    bool computing = false;
    bool has_selection = false; // 主 feature 已选好
  } features;
};
