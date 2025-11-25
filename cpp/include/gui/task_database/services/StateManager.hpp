// State Manager - Aggregates state and manages gatekeeper logic
// Central coordinator for all database subsystems
#pragma once

#include "gui/task_database/services/BaostockService.hpp"
#include "gui/task_database/services/L2DatabaseService.hpp"
#include <boost/asio/awaitable.hpp>

namespace GUI::Database {

using boost::asio::awaitable;

// ============================================================================
// Aggregated State (lightweight, view-only)
// ============================================================================

struct AggregatedState {
  // JSON file statuses
  JsonFileStatus stock_factor_status = JsonFileStatus::Idle;
  JsonFileStatus stock_info_status = JsonFileStatus::Idle;
  JsonFileStatus stock_days_status = JsonFileStatus::Idle;

  // L2 database status
  L2ScanStatus l2_status = L2ScanStatus::NotScanned;

  // Crawler status
  CrawlerStatus crawler_status = CrawlerStatus::Idle;

  // ============================================================================
  // Gatekeeper Logic
  // ============================================================================

  // All JSON files must be ready to access Table/Browser tabs
  bool all_json_ready() const {
    return stock_factor_status == JsonFileStatus::Ready &&
           stock_info_status == JsonFileStatus::Ready &&
           stock_days_status == JsonFileStatus::Ready;
  }

  // Check if any update operation is in progress
  bool is_updating() const {
    return stock_factor_status == JsonFileStatus::Updating ||
           stock_info_status == JsonFileStatus::Updating ||
           stock_days_status == JsonFileStatus::Updating ||
           crawler_status == CrawlerStatus::Running;
  }

  // Check if L2 scan is in progress
  bool is_scanning() const {
    return l2_status == L2ScanStatus::Scanning;
  }

  // Get overall status string for parent task
  const char *get_overall_status() const {
    if (is_updating())
      return "updating";
    if (crawler_status == CrawlerStatus::Initializing)
      return "initializing";
    if (is_scanning())
      return "scanning";
    if (!all_json_ready())
      return "incomplete";
    return "ready";
  }

  // Get status string for individual JSON file
  const char *get_json_status_string(JsonFileStatus status) const {
    switch (status) {
    case JsonFileStatus::Idle:
      return "";
    case JsonFileStatus::Loading:
      return "loading";
    case JsonFileStatus::Ready:
      return "ready";
    case JsonFileStatus::Outdated:
      return "outdated";
    case JsonFileStatus::Error:
      return "error";
    case JsonFileStatus::Updating:
      return "updating";
    default:
      return "";
    }
  }
};

// ============================================================================
// State Manager - Coordinates all services
// ============================================================================

class StateManager {
private:
  BaostockService *baostock_svc_;
  L2DatabaseService *l2_svc_;
  AggregatedState state_;

public:
  StateManager(BaostockService *bs, L2DatabaseService *l2)
      : baostock_svc_(bs), l2_svc_(l2) {}

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

  // ============================================================================
  // Gatekeeper Checks
  // ============================================================================

  bool can_access_table_tab() const { return state_.all_json_ready(); }
  bool can_access_browser_tab() const { return state_.all_json_ready(); }
};

} // namespace GUI::Database
