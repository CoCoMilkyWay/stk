// Data source: Baostock (证券宝) - http://baostock.com
// Stock Data Manager - Core data management logic
// Handles stock_factor, stock_info, and stock_days with automatic updates

#pragma once

#include "gui/task_database/infrastructure/BaostockPool.hpp"
#include "gui/task_database/models/BaostockData.hpp"
#include "shared/Config.hpp"
#include <boost/asio/awaitable.hpp>
#include <functional>
#include <map>
#include <string>
#include <vector>

// Forward declarations
class TaskTerminal;

namespace GUI::Database {

using boost::asio::awaitable;

// ============================================================================
// Main Data Manager Class
// ============================================================================

class DataManager {
private:
  // Configuration
  Config *config_ = nullptr;
  std::vector<std::string> stock_codes_;

  // Metadata (stock_info only needs unified checked_date)
  std::map<std::string, std::string> stock_info_last_update_;

  // Data structures
  StockFactorMap stock_factor_;
  StockInfoMap stock_info_;
  StockDaysVec stock_days_;

  // Connection pool for API calls
  std::shared_ptr<BaostockPool> pool_;
  boost::asio::io_context &io_context_;

  // Terminal for logging
  TaskTerminal *terminal_ = nullptr;

  // Progress callback: (file_type, current_item, current, total, stage)
  // file_type: "stock_days", "stock_factor", "stock_info"
  std::function<void(const std::string &file_type, const std::string &current_item, size_t current, size_t total, UpdateStage stage)> progress_callback_;

  // User session state (IP-level, shared across all workers)
  bool user_logged_in_;

  // Helper methods - Date utilities
  std::string get_today_date() const;
  std::string get_date_from_days_ago(int days) const;
  std::string increment_date(const std::string &date) const;
  bool is_trading_day(const std::string &date) const;
  std::string get_last_trading_day() const;

  // Helper methods - Update scheduling
  bool should_run_weekly_update() const;
  bool should_run_daily_update() const;

  // Helper methods - File operations
  void deduplicate_and_sort_factor(const std::string &code);
  void deduplicate_and_sort_days();

public:
  DataManager(boost::asio::io_context &io_context,
              Config *config,
              TaskTerminal *terminal = nullptr);
  ~DataManager() = default;

  // Logging helper
  void Log(const std::string &message, bool is_error = false);

  // Progress callback
  void set_progress_callback(std::function<void(const std::string &, const std::string &, size_t, size_t, UpdateStage)> callback) {
    progress_callback_ = callback;
  }

  // Data access
  const StockFactorMap &get_stock_factor() const { return stock_factor_; }
  const StockInfoMap &get_stock_info() const { return stock_info_; }
  const StockDaysVec &get_stock_days() const { return stock_days_; }

  // Login status (read-only, managed internally by ensure_logged_in/out)
  bool is_logged_in() const { return user_logged_in_; }

  // Initialization and cleanup
  awaitable<bool> initialize();
  awaitable<void> shutdown();
  awaitable<bool> login_all();
  awaitable<bool> ensure_logged_in();  // Lazy login helper
  awaitable<void> ensure_logged_out(); // Logout helper

  // Configuration
  awaitable<void> load_config(const std::string &config_file);
  awaitable<void> save_config(const std::string &config_file);

  // JSON persistence
  awaitable<void> load_stock_factor();
  awaitable<void> load_stock_info();
  awaitable<void> load_stock_days();
  awaitable<void> save_stock_factor();
  awaitable<void> save_stock_info();
  awaitable<void> save_stock_days();

  // Update operations
  awaitable<void> update_stock_factor(bool force = false, bool skip_login = false, bool skip_logout = false);
  awaitable<void> update_stock_info_weekly(bool force = false, bool skip_days = false, bool skip_login = false, bool skip_logout = false);
  awaitable<void> update_stock_info_daily(bool force = false, bool skip_days = false, bool skip_login = false, bool skip_logout = false);
  awaitable<void> update_stock_days(bool force = false, bool skip_login = false, bool skip_logout = false);
  awaitable<void> update_all(bool force = false);

  // Integrity checks
  IntegrityResult check_stock_factor_integrity();
  IntegrityResult check_stock_info_integrity();
  IntegrityResult check_stock_days_integrity();
  IntegrityResult check_all_integrity();

  // Progress tracking
  std::string get_next_update_time_weekly() const;
  std::string get_next_update_time_daily() const;
  double get_update_progress_weekly() const;
  double get_update_progress_daily() const;
  StatusSummary get_status_summary() const;

  // Force remove operations
  bool force_remove_stock_factor();
  bool force_remove_stock_info();
  bool force_remove_stock_days();

  // Check if data is up-to-date
  bool is_stock_factor_uptodate() const;
  bool is_stock_info_uptodate() const;
  bool is_stock_days_uptodate() const;
};

} // namespace GUI::Database
