#pragma once

// ============================================================================
// Task State - 统一的任务状态管理
// 各 Task 写入自己的状态，读取其他 Task 的状态
// 通过 data.taskstate.xxx 直接访问，无需设计 API
// ============================================================================

// Forward declaration for ImVec4
struct ImVec4;

struct TaskState {
  // ==========================================================================
  // Settings Task
  // ==========================================================================
  struct Settings {
    enum class Status { None, Initializing, Syncing, Writing, Synced };
    Status status = Status::None;
    bool initialized = false;

    const char *status_text() const;
    ImVec4 status_color() const;
  } settings;

  // ==========================================================================
  // Database Task
  // ==========================================================================
  struct Database {
    enum class Status { None, Initializing, NotScanned, Incomplete, Error, Ready };
    Status status = Status::NotScanned;

    // 关键状态标志 (其他 Task 可读取)
    bool binary_scanned = false; // 二进制数据库已扫描
    bool binary_pass = false;    // 覆盖检查通过
    bool all_json_ready = false; // 所有 JSON 文件就绪

    // 便捷方法
    bool ready() const { return binary_pass && all_json_ready; }

    // 内部握手信号 (防止并发操作)
    bool json_update_inflight = false;
    bool l2_scan_inflight = false;

    const char *status_text() const;
    ImVec4 status_color() const;
  } database;

  // ==========================================================================
  // Features Task
  // ==========================================================================
  struct Features {
    enum class Status { None, Waiting, Selecting, Computing, Ready, Error };
    Status status = Status::None;

    bool computing = false;
    bool has_selection = false;  // 主 feature 已选好

    const char *status_text() const;
    ImVec4 status_color() const;
  } features;
};

