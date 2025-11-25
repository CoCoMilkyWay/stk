// Baostock Service Implementation
#include "gui/task_database/services/BaostockService.hpp"
#include <chrono>
#include <iomanip>
#include <sstream>

namespace GUI::Database {

// ============================================================================
// Constructor / Destructor
// ============================================================================

BaostockService::BaostockService(boost::asio::io_context &io,
                                 Config *config,
                                 TaskTerminal *terminal) {
  data_mgr_ = std::make_unique<DataManager>(io, config, terminal);

  // Setup progress callback - clean routing based on file_type
  data_mgr_->set_progress_callback([this](const std::string &file_type, const std::string &item, size_t current, size_t total, UpdateStage stage) {
    if (file_type == "stock_factor") {
      if (stage == UpdateStage::Complete) {
        update_stock_factor_state();
      } else {
        stock_factor_state_.set_updating(item, current, total, stage);
      }
    } else if (file_type == "stock_info") {
      if (stage == UpdateStage::Complete) {
        update_stock_info_state();
      } else {
        stock_info_state_.set_updating(item, current, total, stage);
      }
    } else if (file_type == "stock_days") {
      if (stage == UpdateStage::Complete) {
        update_stock_days_state();
      } else {
        stock_days_state_.set_updating(item, current, total, stage);
      }
    } else if (file_type == "all") {
      // Global operations
      if (stage == UpdateStage::CheckingIntegrity) {
        stock_factor_state_.set_stage(stage);
        stock_info_state_.set_stage(stage);
        stock_days_state_.set_stage(stage);
      } else if (stage == UpdateStage::Complete) {
        refresh_state();
      }
    }
  });
  
  // Setup crawler progress callback (for session status and query count)
  data_mgr_->set_crawler_progress_callback([this](const CrawlerProgress &progress) {
    crawler_state_.progress = progress;
  });
}

BaostockService::~BaostockService() = default;

// ============================================================================
// Lifecycle
// ============================================================================

awaitable<bool> BaostockService::initialize() {
  // Initialize DataManager: load config + load existing JSON files
  // No login - will be done lazily on first API call
  bool success = co_await data_mgr_->initialize();
  if (!success) {
    stock_factor_state_.status = JsonFileStatus::Error;
    stock_info_state_.status = JsonFileStatus::Error;
    stock_days_state_.status = JsonFileStatus::Error;
    co_return false;
  }

  // Refresh state (JSON already loaded by data_mgr_->initialize())
  refresh_state();

  co_return true;
}

awaitable<void> BaostockService::shutdown() {
  co_await data_mgr_->shutdown();
}

// ============================================================================
// JSON File Operations
// ============================================================================

awaitable<void> BaostockService::load_all_json() {
  stock_factor_state_.status = JsonFileStatus::Loading;
  stock_info_state_.status = JsonFileStatus::Loading;
  stock_days_state_.status = JsonFileStatus::Loading;

  co_await data_mgr_->initialize();

  refresh_state();
}

awaitable<void> BaostockService::update_stock_factor(bool force) {
  stock_factor_state_.status = JsonFileStatus::Updating;
  crawler_state_.status = CrawlerStatus::Running;

  co_await data_mgr_->update_stock_factor(force);

  update_stock_factor_state();
  crawler_state_.status = CrawlerStatus::Complete;
}

awaitable<void> BaostockService::update_stock_info(bool force) {
  stock_info_state_.status = JsonFileStatus::Updating;
  crawler_state_.status = CrawlerStatus::Running;

  co_await data_mgr_->update_stock_info_weekly(force, false);
  co_await data_mgr_->update_stock_info_daily(force, true); // skip_days=true to avoid redundant update

  update_stock_info_state();
  crawler_state_.status = CrawlerStatus::Complete;
}

awaitable<void> BaostockService::update_stock_days(bool force) {
  stock_days_state_.status = JsonFileStatus::Updating;
  crawler_state_.status = CrawlerStatus::Running;

  co_await data_mgr_->update_stock_days(force);

  update_stock_days_state();
  crawler_state_.status = CrawlerStatus::Complete;
}

awaitable<void> BaostockService::update_all(bool force) {
  crawler_state_.status = CrawlerStatus::Running;

  co_await data_mgr_->update_all(force);

  refresh_state();
  crawler_state_.status = CrawlerStatus::Complete;
}

bool BaostockService::force_remove_stock_factor() {
  bool success = data_mgr_->force_remove_stock_factor();
  if (success) {
    stock_factor_state_.status = JsonFileStatus::Idle;
    stock_factor_state_.stock_count = 0;
    stock_factor_state_.record_count = 0;
  }
  return success;
}

bool BaostockService::force_remove_stock_info() {
  bool success = data_mgr_->force_remove_stock_info();
  if (success) {
    stock_info_state_.status = JsonFileStatus::Idle;
    stock_info_state_.stock_count = 0;
  }
  return success;
}

bool BaostockService::force_remove_stock_days() {
  bool success = data_mgr_->force_remove_stock_days();
  if (success) {
    stock_days_state_.status = JsonFileStatus::Idle;
    stock_days_state_.trading_days_count = 0;
  }
  return success;
}

// ============================================================================
// Integrity Checks
// ============================================================================

IntegrityResult BaostockService::check_integrity_stock_factor() {
  auto result = data_mgr_->check_stock_factor_integrity();
  stock_factor_state_.integrity_passed = result.passed;
  stock_factor_state_.integrity_errors = result.errors;
  stock_factor_state_.integrity_warnings = result.warnings;
  return result;
}

IntegrityResult BaostockService::check_integrity_stock_info() {
  auto result = data_mgr_->check_stock_info_integrity();
  stock_info_state_.integrity_passed = result.passed;
  stock_info_state_.integrity_errors = result.errors;
  stock_info_state_.integrity_warnings = result.warnings;
  return result;
}

IntegrityResult BaostockService::check_integrity_stock_days() {
  auto result = data_mgr_->check_stock_days_integrity();
  stock_days_state_.integrity_passed = result.passed;
  stock_days_state_.integrity_errors = result.errors;
  stock_days_state_.integrity_warnings = result.warnings;
  return result;
}

IntegrityResult BaostockService::check_all_integrity() {
  auto result = data_mgr_->check_all_integrity();

  // Update all state integrity info
  stock_factor_state_.integrity_passed = true;
  stock_info_state_.integrity_passed = true;
  stock_days_state_.integrity_passed = true;

  return result;
}

// ============================================================================
// Data Access
// ============================================================================

const StockFactorMap &BaostockService::get_stock_factor_data() const {
  return data_mgr_->get_stock_factor();
}

const StockInfoMap &BaostockService::get_stock_info_data() const {
  return data_mgr_->get_stock_info();
}

const StockDaysVec &BaostockService::get_stock_days_data() const {
  return data_mgr_->get_stock_days();
}

// ============================================================================
// Ready Checks
// ============================================================================

bool BaostockService::is_stock_factor_ready() const {
  return stock_factor_state_.status == JsonFileStatus::Ready;
}

bool BaostockService::is_stock_info_ready() const {
  return stock_info_state_.status == JsonFileStatus::Ready;
}

bool BaostockService::is_stock_days_ready() const {
  return stock_days_state_.status == JsonFileStatus::Ready;
}

bool BaostockService::all_ready() const {
  return is_stock_factor_ready() &&
         is_stock_info_ready() &&
         is_stock_days_ready();
}

// ============================================================================
// Status Summary
// ============================================================================

StatusSummary BaostockService::get_status_summary() const {
  return data_mgr_->get_status_summary();
}

// ============================================================================
// State Update (Internal)
// ============================================================================

void BaostockService::refresh_state() {
  update_stock_factor_state();
  update_stock_info_state();
  update_stock_days_state();
}

void BaostockService::update_stock_factor_state() {
  const auto &data = get_stock_factor_data();

  stock_factor_state_.stock_count = data.size();
  stock_factor_state_.record_count = 0;
  for (const auto &[code, records] : data) {
    stock_factor_state_.record_count += records.data.size();
  }

  // Check integrity
  auto integrity = check_integrity_stock_factor();

  // Determine status
  if (integrity.passed && data_mgr_->is_stock_factor_uptodate()) {
    stock_factor_state_.status = JsonFileStatus::Ready;
  } else if (integrity.passed) {
    stock_factor_state_.status = JsonFileStatus::Outdated;
  } else if (data.empty()) {
    stock_factor_state_.status = JsonFileStatus::Error;
    stock_factor_state_.error_message = "File missing or empty";
  } else {
    stock_factor_state_.status = JsonFileStatus::Outdated;
  }

  // Update timestamp
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
  localtime_r(&time_t_now, &tm_now);

  std::ostringstream oss;
  oss << std::put_time(&tm_now, "%Y-%m-%d %H:%M:%S");
  stock_factor_state_.last_update_time = oss.str();
}

void BaostockService::update_stock_info_state() {
  const auto &data = get_stock_info_data();

  stock_info_state_.stock_count = data.size();

  // Check integrity
  auto integrity = check_integrity_stock_info();

  // Determine status
  if (integrity.passed && data_mgr_->is_stock_info_uptodate()) {
    stock_info_state_.status = JsonFileStatus::Ready;
  } else if (integrity.passed) {
    stock_info_state_.status = JsonFileStatus::Outdated;
  } else if (data.empty()) {
    stock_info_state_.status = JsonFileStatus::Error;
    stock_info_state_.error_message = "File missing or empty";
  } else {
    stock_info_state_.status = JsonFileStatus::Outdated;
  }

  // Update timestamp
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
  localtime_r(&time_t_now, &tm_now);

  std::ostringstream oss;
  oss << std::put_time(&tm_now, "%Y-%m-%d %H:%M:%S");
  stock_info_state_.last_update_time = oss.str();
}

void BaostockService::update_stock_days_state() {
  const auto &data = get_stock_days_data();

  stock_days_state_.trading_days_count = 0;
  for (const auto &day : data) {
    if (day.size() >= 2 && day[1] == "1") {
      stock_days_state_.trading_days_count++;
    }
  }

  if (!data.empty()) {
    stock_days_state_.date_range_start = data.front()[0];
    stock_days_state_.date_range_end = data.back()[0];
  }

  // Check integrity
  auto integrity = check_integrity_stock_days();

  // Determine status
  if (integrity.passed && data_mgr_->is_stock_days_uptodate()) {
    stock_days_state_.status = JsonFileStatus::Ready;
  } else if (integrity.passed) {
    stock_days_state_.status = JsonFileStatus::Outdated;
  } else if (data.empty()) {
    stock_days_state_.status = JsonFileStatus::Error;
    stock_days_state_.error_message = "File missing or empty";
  } else {
    stock_days_state_.status = JsonFileStatus::Outdated;
  }

  // Update timestamp
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
  localtime_r(&time_t_now, &tm_now);

  std::ostringstream oss;
  oss << std::put_time(&tm_now, "%Y-%m-%d %H:%M:%S");
  stock_days_state_.last_update_time = oss.str();
}

} // namespace GUI::Database
