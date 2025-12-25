// Data source: Baostock (证券宝) - http://www.baostock.com
// Stock Data Manager - Core data management logic
// Handles stock_factor, stock_info, and stock_days with automatic updates

#pragma once

#include "gui/task_database/infrastructure/BaostockPool.hpp"
#include "gui/task_database/models/BaostockData.hpp"
#include "shared/AssetInfo.hpp"
#include <boost/asio/awaitable.hpp>
#include <functional>
#include <map>
#include <string>
#include <vector>

// Forward declarations
struct SharedData;
struct TaskTerminal;

namespace GUI::Database {

using boost::asio::awaitable;

// ============================================================================
// Main Data Manager Class
// ============================================================================

class DataManager {
private:
  // Shared data reference (contains assetinfo with stock data)
  SharedData &data_;
  
  // Configuration
  std::vector<std::string> stock_codes_;

  // Metadata
  std::map<std::string, std::string> stock_info_last_update_;
  std::string l2_database_start_date_; // YYYYMMDD format, from L2 database scan
  std::string l2_database_end_date_;   // YYYYMMDD format

  // Connection pool for API calls
  std::shared_ptr<BaostockPool> pool_;
  boost::asio::io_context &io_context_;

  // Terminal for logging
  TaskTerminal *terminal_ = nullptr;

  // Progress callback: (file_type, current_item, current, total, stage)
  // file_type: "stock_days", "stock_factor", "stock_info"
  std::function<void(const std::string &file_type, const std::string &current_item, size_t current, size_t total, UpdateStage stage)> progress_callback_;

  // Crawler progress callback (for pool-level progress with session info)
  std::function<void(const CrawlerProgress &)> crawler_progress_callback_;

  // User session state (IP-level, shared across all workers)
  BaostockSessionStatus session_status_ = BaostockSessionStatus::Idle;
  size_t session_query_count_ = 0; // Track queries in current login session
  std::atomic<int> active_workers_{0}; // Track number of workers currently executing queries

  // Helper methods - Date utilities
  std::string get_today_date() const;
  std::string get_date_from_days_ago(int days) const;
  std::string increment_date(const std::string &date) const;
  bool is_trading_day(const std::string &date) const;

  // Helper methods - File operations
  void deduplicate_and_sort_factor(const std::string &code);
  void deduplicate_and_sort_days();

public:
  DataManager(boost::asio::io_context &io_context,
              SharedData &data,
              TaskTerminal *terminal = nullptr);
  ~DataManager() = default;

  // Logging helper
  void Log(const std::string &message, bool is_error = false);

  // Progress callbacks
  void set_progress_callback(std::function<void(const std::string &, const std::string &, size_t, size_t, UpdateStage)> callback) {
    progress_callback_ = callback;
  }

  void set_crawler_progress_callback(std::function<void(const CrawlerProgress &)> callback) {
    crawler_progress_callback_ = callback;
  }

  // Data access (delegated to SharedData::assetinfo)
  const StockFactorMap &get_stock_factor() const;
  const StockInfoMap &get_stock_info() const;
  const StockDaysVec &get_stock_days() const;
  const std::vector<std::string> &get_stock_codes() const { return stock_codes_; }

  // Login status (read-only, managed internally by ensure_logged_in/out)
  bool is_logged_in() const { return session_status_ == BaostockSessionStatus::Active; }

  // Update scheduling checks
  std::string get_last_trading_day() const;
  bool should_run_weekly_update() const;

  // Set stock codes from external source (e.g., L2 database scan)
  awaitable<void> set_stock_codes(const std::vector<std::string> &codes);

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
  awaitable<void> update_stock_factor(bool skip_login = false, bool skip_logout = false);
  awaitable<void> update_stock_info_weekly(bool skip_days = false, bool skip_login = false, bool skip_logout = false);
  awaitable<void> update_stock_info_daily(bool skip_days = false, bool skip_login = false, bool skip_logout = false);
  awaitable<void> update_stock_days(bool skip_login = false, bool skip_logout = false, const std::string &force_start_date = "");
  awaitable<void> update_all(const std::string &l2_database_start_date = "");

  // Integrity checks
  IntegrityResult check_stock_factor_integrity();
  IntegrityResult check_stock_info_integrity();
  IntegrityResult check_stock_days_integrity(const std::string &l2_database_start_date = "");
  IntegrityResult check_all_integrity(const std::string &l2_database_start_date = "");

  // L2 database date range management
  void set_l2_database_date_range(const std::string &start_date, const std::string &end_date);
  std::string get_l2_database_start_date() const { return l2_database_start_date_; }
  std::string get_l2_database_end_date() const { return l2_database_end_date_; }

  // Progress tracking
  std::string get_next_update_time_weekly() const;
  std::string get_next_update_time_daily() const;
  float get_update_progress_weekly() const;
  float get_update_progress_daily() const;
  StatusSummary get_status_summary() const;

  // Force remove operations
  bool force_remove_stock_factor();
  bool force_remove_stock_info();
  bool force_remove_stock_days();

  // Check if data is up-to-date
  bool is_stock_factor_uptodate() const;
  bool is_stock_info_uptodate() const;
  bool is_stock_days_uptodate() const;

private:
  // Progress reporting - single unified API
  void report_progress();
};

} // namespace GUI::Database
