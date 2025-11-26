// Baostock Service - Encapsulates DataManager with clean interface
// Manages stock_factor, stock_info, stock_days JSON files
#pragma once

#include "gui/task_database/infrastructure/DataManager.hpp"
#include "gui/task_database/models/BaostockData.hpp"
#include <boost/asio/awaitable.hpp>
#include <memory>

// Forward declarations
class TaskTerminal;

namespace GUI::Database {

using boost::asio::awaitable;

// ============================================================================
// Baostock Service - High-level API for JSON data management
// ============================================================================

class BaostockService {
private:
  std::unique_ptr<DataManager> data_mgr_;

  // State tracking
  JsonFileState stock_factor_state_;
  JsonFileState stock_info_state_;
  JsonFileState stock_days_state_;
  CrawlerState crawler_state_;

public:
  BaostockService(boost::asio::io_context &io, Config *config, TaskTerminal *terminal = nullptr);
  ~BaostockService();

  // ============================================================================
  // Lifecycle
  // ============================================================================

  awaitable<bool> initialize();
  awaitable<void> shutdown();

  // ============================================================================
  // JSON File Operations
  // ============================================================================

  awaitable<void> load_all_json();
  awaitable<void> update_stock_factor();
  awaitable<void> update_stock_info();
  awaitable<void> update_stock_days();
  awaitable<void> update_all(const std::string &l2_database_start_date = "");

  // Force remove operations (with backup)
  bool force_remove_stock_factor();
  bool force_remove_stock_info();
  bool force_remove_stock_days();

  // ============================================================================
  // Integrity Checks
  // ============================================================================

  IntegrityResult check_integrity_stock_factor();
  IntegrityResult check_integrity_stock_info();
  IntegrityResult check_integrity_stock_days();
  IntegrityResult check_all_integrity();

  // ============================================================================
  // State Query (Read-Only)
  // ============================================================================

  const JsonFileState &get_stock_factor_state() const { return stock_factor_state_; }
  const JsonFileState &get_stock_info_state() const { return stock_info_state_; }
  const JsonFileState &get_stock_days_state() const { return stock_days_state_; }
  const CrawlerState &get_crawler_state() const { return crawler_state_; }
  
  // Access to underlying DataManager
  DataManager *get_data_manager() { return data_mgr_.get(); }

  // ============================================================================
  // Data Access (Read-Only)
  // ============================================================================

  const StockFactorMap &get_stock_factor_data() const;
  const StockInfoMap &get_stock_info_data() const;
  const StockDaysVec &get_stock_days_data() const;

  // ============================================================================
  // Ready Checks
  // ============================================================================

  bool is_stock_factor_ready() const;
  bool is_stock_info_ready() const;
  bool is_stock_days_ready() const;
  bool all_ready() const;

  // ============================================================================
  // Status Summary
  // ============================================================================

  StatusSummary get_status_summary() const;

  // ============================================================================
  // Internal State Update
  // ============================================================================

  void refresh_state(); // Update all state tracking from DataManager

private:
  void update_stock_factor_state();
  void update_stock_info_state();
  void update_stock_days_state();
};

} // namespace GUI::Database
