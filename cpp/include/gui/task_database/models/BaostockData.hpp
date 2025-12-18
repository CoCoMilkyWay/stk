// Baostock data structures
// Pure data types + State structures for JSON files and crawler
// Core data structures (StockInfo, StockInfoMap, etc.) moved to shared/AssetInfo.hpp
#pragma once

#include "SharedTypes.hpp"
#include <string>
#include <vector>

namespace GUI::Database {

// ============================================================================
// Integrity Check Results
// ============================================================================

struct IntegrityResult {
  bool passed = true;
  std::vector<std::string> missing_stocks;
  std::vector<std::string> incomplete_stocks;
  std::vector<std::string> errors;
  std::vector<std::string> warnings;
};

// ============================================================================
// Status Summary for UI Display
// ============================================================================

struct StatusSummary {
  std::string last_weekly_update;
  std::string last_daily_update;
  float weekly_progress_pct; // 0-100
  float daily_progress_pct;  // 0-100
  std::string next_weekly_update;
  std::string next_daily_update;
  int total_stocks;
  int stocks_with_factor_data;
  int stocks_with_info_data;
  int trading_days_count;
};

// ============================================================================
// JSON File State (for service layer tracking)
// ============================================================================

struct JsonFileState {
  JsonFileStatus status = JsonFileStatus::Idle;
  std::string last_update_time;
  std::string error_message;

  size_t stock_count = 0;
  size_t record_count = 0;
  size_t trading_days_count = 0;
  std::string date_range_start;
  std::string date_range_end;

  bool integrity_passed = false;
  std::vector<std::string> integrity_errors;
  std::vector<std::string> integrity_warnings;

  // Update progress details
  struct UpdateProgress {
    float percentage = 0.0;  // 0.0 - 1.0
    size_t current_index = 0; // current item being processed
    size_t total = 0;         // total items to process
    std::string current_item; // e.g. "sh.600448"
    float speed = 0.0;       // items per second
    int eta_seconds = 0;      // estimated time remaining
    UpdateStage stage = UpdateStage::Idle;
  } progress;

  // Clean API for status updates
  void set_updating(const std::string& item, size_t current, size_t total, UpdateStage stage) {
    status = JsonFileStatus::Updating;
    progress.current_item = item;
    progress.current_index = current;
    progress.total = total;
    progress.percentage = total > 0 ? static_cast<float>(current) / total : 0.0;
    progress.stage = stage;
  }

  void set_stage(UpdateStage stage) {
    progress.stage = stage;
  }

  void clear_progress() {
    progress = UpdateProgress{};
  }
};

// ============================================================================
// Crawler State (uses CrawlerProgress from SharedTypes.hpp)
// ============================================================================

struct CrawlerState {
  CrawlerStatus status = CrawlerStatus::Idle;
  CrawlerProgress progress;
};

} // namespace GUI::Database
