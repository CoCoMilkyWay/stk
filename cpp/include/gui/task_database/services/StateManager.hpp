// State Manager - Aggregates state and manages gatekeeper logic
// Central coordinator for all database subsystems
#pragma once

#include "gui/task_database/services/AssetLoader.hpp"
#include "gui/task_database/services/FundamentalService.hpp"
#include "gui/task_database/services/ScanService.hpp"
#include <boost/asio/awaitable.hpp>

// Forward declaration
struct SharedData;

namespace GUI::Database {

using boost::asio::awaitable;

// ============================================================================
// Tab Access Control State
// ============================================================================

struct TabAccessState {
  // Tab unlock progression (基本面 → L2 → 消费端)
  bool can_access_overview = true;  // 基本面面板, 流水线第一步, 永远可进
  bool can_access_encode = false;   // 基本面 Ready 后解锁 (覆盖检查依赖日历)
  bool can_access_table = false;    // 基本面 Ready 且 L2 覆盖 Pass
  bool can_access_browser = false;  // 基本面 Ready 且 L2 覆盖 Pass
};

// ============================================================================
// Aggregated State (lightweight, view-only)
// ============================================================================

struct AggregatedState {
  // Tab access control
  TabAccessState tabs;

  // Fundamental data status (BigQuant + Tushare → AssetInfo)
  FundamentalStatus fundamental_status = FundamentalStatus::Idle;

  // Helper: fundamental data ready (AssetInfo built from parquet)
  bool all_json_ready() const {
    return fundamental_status == FundamentalStatus::Ready;
  }
};

// ============================================================================
// State Manager - Coordinates all services
// ============================================================================

class StateManager {
private:
  SharedData &data_;
  FundamentalService *fundamental_svc_;
  ScanService *scan_svc_;
  AggregatedState state_;

public:
  StateManager(SharedData &data, FundamentalService *fs, ScanService *scan)
      : data_(data), fundamental_svc_(fs), scan_svc_(scan) {}

  // ============================================================================
  // Lifecycle
  // ============================================================================

  // Initialize: check all files and load data
  awaitable<void> initialize();

  // ============================================================================
  // State Management
  // ============================================================================

  // Refresh state from all services
  void refresh_state();

  // Get current aggregated state
  const AggregatedState &get_state() const { return state_; }

  // Get tab access state (managed centrally)
  const TabAccessState &get_tab_access() const { return state_.tabs; }
};

} // namespace GUI::Database
