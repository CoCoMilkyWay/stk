// State Manager Implementation
#include "gui/task_database/services/StateManager.hpp"
#include "shared/SharedData.hpp" // NOLINT: Required for AssetLoader::load_from_config(data_)

namespace GUI::Database {

// ============================================================================
// Lifecycle
// ============================================================================

awaitable<void> StateManager::initialize() {
  // Step 1: Load asset list from assets.json (single source of truth, path from config)
  AssetLoader::load_from_config(data_);

  // Step 2: Build stock codes list from assets (format: exchange.code, lowercase exchange)
  std::vector<std::string> stock_codes;
  for (const auto &item : data_.asset.items) {
    std::string exchange_lower = item.exchange;
    std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
    stock_codes.push_back(exchange_lower + "." + item.asset_code);
  }

  // Step 3: Set stock codes to DataManager
  if (!stock_codes.empty()) {
    co_await baostock_svc_->get_data_manager()->set_stock_codes(stock_codes);
  }

  // Step 4: Scan binary database (heavy operation, done once)
  // This will initialize all_dates, date_info, and scan all existing binaries
  data_.asset.scan_binary_database(data_.config.database_dir, data_.config.binary_extension);

  // Step 5: Scan archive database (heavy operation, done once)
  // This will populate archive metadata for coverage check
  data_.asset.scan_archive_database(data_.config.archive_dir, data_.config.archive_extension);

  // Step 6: Check database coverage for backtest period
  encoding_svc_->check_database_coverage();

  // Step 7: Initialize JSON files (fast, no network)
  // Workers will login lazily when first API call is made
  co_await baostock_svc_->load_all_json();

  // Step 8: Compute browser statistics (requires stock_info for delist dates and stock_days for all trading days)
  data_.asset.compute_browser_statistics(
      baostock_svc_->get_stock_info_data(),
      baostock_svc_->get_stock_days_data());

  refresh_state();
}

// ============================================================================
// State Management
// ============================================================================

void StateManager::refresh_state() {
  // Refresh all services
  baostock_svc_->refresh_state();

  // Update JSON file statuses
  state_.stock_factor_status = baostock_svc_->get_stock_factor_state().status;
  state_.stock_info_status = baostock_svc_->get_stock_info_state().status;
  state_.stock_days_status = baostock_svc_->get_stock_days_state().status;

  // Get database check result
  auto check_result = encoding_svc_->get_last_check_result();

  // ============================================================================
  // Tab Access Control (Top-down unlock progression)
  // ============================================================================

  // Encode: always accessible
  state_.tabs.can_access_encode = true;

  // Overview: unlocked when database check passes (binary完整覆盖backtest period)
  state_.tabs.can_access_overview = check_result.can_unlock_overview();

  // Table/Browser: unlocked when Overview accessible AND all JSON files ready
  bool json_ready = state_.all_json_ready();
  state_.tabs.can_access_table = state_.tabs.can_access_overview && json_ready;
  state_.tabs.can_access_browser = state_.tabs.can_access_overview && json_ready;
}

} // namespace GUI::Database
