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

  // Yield immediately after asset loading to let GUI render
  co_await boost::asio::steady_timer(co_await boost::asio::this_coro::executor, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

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

  // Step 4-6: Start database check (inline - immediately update status then yield)
  auto enc_status = encoding_svc_->get_status();
  if (enc_status == EncodingStatus::Idle ||
      enc_status == EncodingStatus::Completed ||
      enc_status == EncodingStatus::Cancelled ||
      enc_status == EncodingStatus::Error) {

    // Immediately clear old result and update status (BEFORE spawning coroutine)
    encoding_svc_->last_check_ = DatabaseCheckResult{};
    encoding_svc_->status_ = EncodingStatus::InitializingCheck;

    // Yield immediately to let GUI render the InitializingCheck status
    co_await boost::asio::steady_timer(co_await boost::asio::this_coro::executor, std::chrono::milliseconds(1)).async_wait(boost::asio::use_awaitable);

    // Now spawn the coroutine (after GUI has rendered the status)
    boost::asio::co_spawn(encoding_svc_->io_, encoding_svc_->coro_database_check(), boost::asio::detached);
  }

  // Step 7: Initialize JSON files (fast, no network)
  // Workers will login lazily when first API call is made
  co_await baostock_svc_->load_all_json();

  // Step 8: Browser statistics computed lazily on first Browser tab access
  // (after database scan completes and baostock data is ready)

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
