// State Manager - Aggregates state and manages gatekeeper logic
// Central coordinator for all database subsystems
#pragma once

#include "gui/task_database/services/AssetLoader.hpp"
#include "gui/task_database/services/BaostockService.hpp"
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_database/services/L2DatabaseService.hpp"
#include <boost/asio/awaitable.hpp>

// Forward declaration
struct SharedData;

namespace GUI::Database {

using boost::asio::awaitable;

// ============================================================================
// Tab Access Control State
// ============================================================================

struct TabAccessState {
  // Tab unlock progression (top-down)
  bool can_access_encode = true;    // Always accessible
  bool can_access_overview = false; // Unlocked when database check passes
  bool can_access_table = false;    // Unlocked when all JSON ready
  bool can_access_browser = false;  // Unlocked when all JSON ready
};

// ============================================================================
// Aggregated State (lightweight, view-only)
// ============================================================================

struct AggregatedState {
  // Tab access control
  TabAccessState tabs;

  // JSON file statuses
  JsonFileStatus stock_factor_status = JsonFileStatus::Idle;
  JsonFileStatus stock_info_status = JsonFileStatus::Idle;
  JsonFileStatus stock_days_status = JsonFileStatus::Idle;

  // Helper: all JSON files ready
  bool all_json_ready() const {
    return stock_factor_status == JsonFileStatus::Ready &&
           stock_info_status == JsonFileStatus::Ready &&
           stock_days_status == JsonFileStatus::Ready;
  }
};

// ============================================================================
// State Manager - Coordinates all services
// ============================================================================

class StateManager {
private:
  SharedData &data_;
  BaostockService *baostock_svc_;
  EncodingService *encoding_svc_;
  AggregatedState state_;

public:
  StateManager(SharedData &data, BaostockService *bs, EncodingService *enc)
      : data_(data), baostock_svc_(bs), encoding_svc_(enc) {}

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
