// State Manager Implementation
#include "gui/task_database/services/StateManager.hpp"

namespace GUI::Database {

// ============================================================================
// Lifecycle
// ============================================================================

awaitable<void> StateManager::initialize() {
  // Initialize: load existing JSON files (fast, no network)
  // Workers will login lazily when first API call is made
  co_await baostock_svc_->load_all_json();
  refresh_state();
}

// ============================================================================
// State Management
// ============================================================================

void StateManager::refresh_state() {
  // Refresh Baostock service state
  baostock_svc_->refresh_state();

  // Update aggregated state from services
  state_.stock_factor_status = baostock_svc_->get_stock_factor_state().status;
  state_.stock_info_status = baostock_svc_->get_stock_info_state().status;
  state_.stock_days_status = baostock_svc_->get_stock_days_state().status;

  state_.crawler_status = baostock_svc_->get_crawler_state().status;

  state_.l2_status = l2_svc_->get_status();
}

} // namespace GUI::Database
