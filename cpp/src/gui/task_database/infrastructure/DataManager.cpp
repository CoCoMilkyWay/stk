// Data source: Baostock (证券宝) - http://www.baostock.com
// Stock Data Manager - Implementation
// Robust data management with automatic updates and integrity checks

#include "gui/task_database/infrastructure/DataManager.hpp"
#include "gui/task_database/infrastructure/BaostockPool.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "gui/util/Color.hpp"
#include "package/nlohmann/json.hpp"
#include <algorithm>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <set>
#include <sstream>

using json = nlohmann::json;

namespace GUI::Database {

namespace {
// No default stock list - stocks will be derived from L2 database scan
// Config file will be empty initially until first scan completes
constexpr const char *kMetaStockInfoWeekly = "weekly";
constexpr const char *kMetaStockInfoDaily = "daily";
} // namespace

namespace fs = std::filesystem;

// ============================================================================
// Constructor
// ============================================================================

DataManager::DataManager(boost::asio::io_context &io_context,
                         Config *config,
                         TaskTerminal *terminal)
    : config_(config), io_context_(io_context), terminal_(terminal),
      user_logged_in_(false), session_query_count_(0), active_workers_(0) {

  if (!config_) {
    throw std::runtime_error("DataManager: config is null");
  }

  pool_ = std::make_shared<BaostockPool>(io_context_, config_->baostock_max_workers);
}

// ============================================================================
// Logging Helper
// ============================================================================

void DataManager::Log(const std::string &message, bool is_error) {
  if (terminal_) {
    Color color = is_error ? Color::Red() : Color::White();
    terminal_->AddLine(message, color);
  }
}

// ============================================================================
// Helper methods
// ============================================================================

std::string DataManager::get_today_date() const {
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
  localtime_r(&time_t_now, &tm_now);

  std::ostringstream oss;
  oss << std::put_time(&tm_now, "%Y-%m-%d");
  return oss.str();
}

std::string DataManager::get_date_from_days_ago(int days) const {
  auto now = std::chrono::system_clock::now();
  auto past = now - std::chrono::hours(24 * days);
  auto time_t_past = std::chrono::system_clock::to_time_t(past);
  std::tm tm_past;
  localtime_r(&time_t_past, &tm_past);

  std::ostringstream oss;
  oss << std::put_time(&tm_past, "%Y-%m-%d");
  return oss.str();
}

std::string DataManager::increment_date(const std::string &date) const {
  std::tm tm = {};
  std::istringstream ss(date);
  ss >> std::get_time(&tm, "%Y-%m-%d");

  auto time_point = std::chrono::system_clock::from_time_t(std::mktime(&tm));
  time_point += std::chrono::hours(24);
  auto time_t_result = std::chrono::system_clock::to_time_t(time_point);
  std::tm tm_result;
  localtime_r(&time_t_result, &tm_result);

  std::ostringstream oss;
  oss << std::put_time(&tm_result, "%Y-%m-%d");
  return oss.str();
}

bool DataManager::is_trading_day(const std::string &date) const {
  for (const auto &day : stock_days_) {
    if (day[0] == date && day[1] == "1") {
      return true;
    }
  }
  return false;
}

std::string DataManager::get_last_trading_day() const {
  std::string today = get_today_date();
  std::string yesterday = get_date_from_days_ago(1);

  for (auto it = stock_days_.rbegin(); it != stock_days_.rend(); ++it) {
    if ((*it)[0] <= yesterday && (*it)[1] == "1") {
      return (*it)[0];
    }
  }

  return yesterday;
}

bool DataManager::should_run_weekly_update() const {
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
  localtime_r(&time_t_now, &tm_now);

  int weekday = (tm_now.tm_wday == 0) ? 7 : tm_now.tm_wday;
  if (weekday != config_->baostock_weekly_update_day) {
    return false;
  }

  auto it = stock_info_last_update_.find(kMetaStockInfoWeekly);
  if (it == stock_info_last_update_.end()) {
    return true;
  }
  return it->second != get_today_date();
}

void DataManager::deduplicate_and_sort_factor(const std::string &code) {
  auto &records = stock_factor_[code].data;
  if (records.empty())
    return;

  std::sort(records.begin(), records.end(),
            [](const auto &a, const auto &b) { return a[0] < b[0]; });

  auto it = std::unique(records.rbegin(), records.rend(),
                        [](const auto &a, const auto &b) { return a[0] == b[0]; });
  records.erase(records.begin(), it.base());
}

void DataManager::deduplicate_and_sort_days() {
  if (stock_days_.empty())
    return;

  std::sort(stock_days_.begin(), stock_days_.end(),
            [](const auto &a, const auto &b) { return a[0] < b[0]; });

  auto it = std::unique(stock_days_.rbegin(), stock_days_.rend(),
                        [](const auto &a, const auto &b) { return a[0] == b[0]; });
  stock_days_.erase(stock_days_.begin(), it.base());
}

// ============================================================================
// Initialization
// ============================================================================

awaitable<bool> DataManager::login_all() {
  Log(std::format("[DataManager] Logging in {} workers...", config_->baostock_max_workers));

  // If pool not initialized, create clients first
  if (pool_->size() == 0) {
    bool success = co_await pool_->initialize();
    if (!success) {
      Log("[DataManager] [ERROR] Failed to initialize pool", true);
      co_return false;
    }
    Log(std::format("[DataManager] [OK] Pool initialized with {} workers", config_->baostock_max_workers));
    co_return true;
  }

  // Re-login on existing clients
  Log("[DataManager] Re-logging in existing workers...");
  for (auto &client : pool_->clients_) {
    bool success = co_await client->login();
    if (!success) {
      Log("[DataManager] [ERROR] Failed to re-login worker", true);
      co_return false;
    }
  }

  Log(std::format("[DataManager] [OK] All {} workers re-logged in", pool_->clients_.size()));
  co_return true;
}

awaitable<void> DataManager::ensure_logged_out() {
  if (!user_logged_in_) {
    Log("Already logged out");
    report_session_status(BaostockSessionStatus::Idle);
    co_return;
  }

  Log("Logging out user session...");
  report_session_status(BaostockSessionStatus::LoggingOut);

  // Logout is at user level - only need to call once
  if (!pool_->clients_.empty()) {
    try {
      co_await pool_->clients_[0]->logout();
      user_logged_in_ = false;
      Log("User session logged out (all workers logged out)");
    } catch (const std::exception &e) {
      Log(std::format("[ERROR] Logout failed: {}", e.what()), true);
      user_logged_in_ = false; // Still mark as logged out locally
    }
  } else {
    Log("[ERROR] No workers available to logout", true);
    user_logged_in_ = false; // Still mark as logged out locally
  }

  // Always set to Idle after logout attempt
  report_session_status(BaostockSessionStatus::Idle);
}

awaitable<bool> DataManager::initialize() {
  Log("[DataManager] ========================================");
  Log("[DataManager] Initializing DataManager...");
  Log("[DataManager] ========================================");

  co_await load_config(config_->config_dir + "/" + config_->baostock_data_manager_config);
  Log(std::format("[DataManager] Loaded config: {} stocks", stock_codes_.size()));

  // Initialize pool (create clients + first login)
  report_session_status(BaostockSessionStatus::LoggingIn);
  bool pool_init = co_await pool_->initialize();
  if (pool_init) {
    user_logged_in_ = true;
    session_query_count_ = 0; // Reset query counter for new session
    Log("[DataManager] Pool initialized and logged in");
    report_session_status(BaostockSessionStatus::Active);
  } else {
    Log("[DataManager] [ERROR] Failed to initialize pool", true);
    report_session_status(BaostockSessionStatus::Idle);
    co_return false;
  }

  co_await load_stock_days();
  co_await load_stock_factor();
  co_await load_stock_info();

  if (progress_callback_) {
    progress_callback_("all", "", 0, 1, UpdateStage::CheckingIntegrity);
  }

  auto days_result = check_stock_days_integrity();
  if (!days_result.passed) {
    Log("[DataManager] [WARNING] Stock days integrity issues detected, auto-fixing...");
    deduplicate_and_sort_days();
  }

  for (const auto &code : stock_codes_) {
    if (stock_factor_.find(code) != stock_factor_.end()) {
      deduplicate_and_sort_factor(code);
    }
  }

  // Final check
  auto final_integrity = check_all_integrity();
  if (!final_integrity.passed) {
    Log("[DataManager] Integrity check failed. Resetting update timers to force fresh update.");
    stock_info_last_update_.clear();
    co_await save_config(config_->config_dir + "/" + config_->baostock_data_manager_config);
  }

  if (progress_callback_) {
    progress_callback_("all", "", 1, 1, UpdateStage::Complete);
  }

  Log("[DataManager] [OK] DataManager initialized successfully!");
  co_return true;
}

awaitable<bool> DataManager::ensure_logged_in() {
  if (user_logged_in_) {
    co_return true; // User session already active
  }

  Log("[DataManager] User session inactive, re-logging in...");
  report_session_status(BaostockSessionStatus::LoggingIn);

  bool success = co_await login_all();
  if (success) {
    user_logged_in_ = true;
    session_query_count_ = 0; // Reset query counter for new session
    Log("[DataManager] User session active");
    report_session_status(BaostockSessionStatus::Active);
  } else {
    Log("[DataManager] [ERROR] Failed to activate user session");
    report_session_status(BaostockSessionStatus::Idle);
  }

  co_return success;
}

awaitable<void> DataManager::shutdown() {
  Log("=== Shutting down DataManager ===");

  co_await save_stock_factor();
  co_await save_stock_info();
  co_await save_stock_days();
  co_await save_config(config_->config_dir + "/" + config_->baostock_data_manager_config);

  // Logout user session
  co_await ensure_logged_out();

  // Clear pool (clients already logged out, just cleanup resources)
  pool_->clients_.clear();

  Log("DataManager shutdown complete");
}

// ============================================================================
// Configuration
// ============================================================================

awaitable<void> DataManager::load_config(const std::string &config_file) {
  std::string filepath = config_file;

  if (!fs::exists(filepath)) {
    // Start with empty stock list - will be populated from L2 scan
    stock_codes_.clear();
    stock_info_last_update_.clear();
    config_->baostock_weekly_update_day = 1;

    co_await save_config(config_file);
    Log("Created config with empty stock list (will be populated from L2 database scan)");
    co_return;
  }

  std::ifstream infile(filepath);
  json j;
  infile >> j;
  infile.close();

  // Load stocks from config, default to empty if not present
  stock_codes_ = j.value("stocks", std::vector<std::string>{});

  // Load L2 database date range
  if (j.contains("l2_database")) {
    l2_database_start_date_ = j["l2_database"].value("date_range_start", "");
    l2_database_end_date_ = j["l2_database"].value("date_range_end", "");
  }

  if (j.contains("metadata")) {
    stock_info_last_update_ = j["metadata"].value("stock_info_last_update", std::map<std::string, std::string>{});
  }

  if (j.contains("settings")) {
    config_->baostock_weekly_update_day = j["settings"].value("weekly_update_day", 1);
  }

  if (stock_codes_.empty()) {
    Log("Loaded config with empty stock list (waiting for L2 database scan)");
  } else {
    Log(std::format("Loaded config: {} stocks", stock_codes_.size()));
  }
  co_return;
}

awaitable<void> DataManager::save_config(const std::string &config_file) {
  json j;

  // L2 database date range
  j["l2_database"]["date_range_start"] = l2_database_start_date_;
  j["l2_database"]["date_range_end"] = l2_database_end_date_;

  // Stocks list
  j["stocks"] = stock_codes_;

  // Metadata
  j["metadata"]["stock_info_last_update"] = stock_info_last_update_;

  // Settings
  j["settings"]["weekly_update_day"] = config_->baostock_weekly_update_day;

  std::string filepath = config_file;
  std::string temp_filepath = filepath + ".tmp";

  std::ofstream outfile(temp_filepath);
  outfile << j.dump(2) << std::endl;
  outfile.close();

  fs::rename(temp_filepath, filepath);
  co_return;
}

void DataManager::set_l2_database_date_range(const std::string &start_date, const std::string &end_date) {
  l2_database_start_date_ = start_date;
  l2_database_end_date_ = end_date;
  Log(std::format("Updated L2 database date range: {} ~ {}", start_date, end_date));
}

awaitable<void> DataManager::set_stock_codes(const std::vector<std::string> &codes) {
  if (codes.empty()) {
    Log("[WARNING] Attempt to set empty stock codes list, ignoring", true);
    co_return;
  }

  Log(std::format("Updating stock codes from {} to {} stocks",
                  stock_codes_.size(), codes.size()));

  stock_codes_ = codes;

  // Save to config file
  co_await save_config(config_->config_dir + "/" + config_->baostock_data_manager_config);

  Log(std::format("Stock codes updated successfully: {} stocks", stock_codes_.size()));
  co_return;
}

// ============================================================================
// JSON Persistence
// ============================================================================

awaitable<void> DataManager::load_stock_factor() {
  std::string filepath = config_->config_dir + "/" + config_->baostock_stock_factor_file;

  if (!fs::exists(filepath)) {
    Log("stock_factor.json not found, creating empty structure");
    stock_factor_.clear();
    co_return;
  }

  try {
    std::ifstream infile(filepath);
    json j;
    infile >> j;
    infile.close();

    stock_factor_.clear();
    for (auto &[code, obj] : j.items()) {
      StockFactorData factor_data;
      factor_data.last_update = obj.value("last_update", "");
      factor_data.data = obj.value("data", std::vector<std::vector<std::string>>{});
      stock_factor_[code] = std::move(factor_data);
    }

    Log(std::format("Loaded stock_factor.json: {} stocks", stock_factor_.size()));
  } catch (const std::exception &e) {
    Log(std::format("Error loading stock_factor.json: {}", e.what()), true);
    stock_factor_.clear();
    if (fs::exists(filepath))
      fs::remove(filepath);
  }

  co_return;
}

awaitable<void> DataManager::load_stock_info() {
  std::string filepath = config_->config_dir + "/" + config_->baostock_stock_info_file;

  if (!fs::exists(filepath)) {
    Log("stock_info.json not found, creating empty structure");
    stock_info_.clear();
    co_return;
  }

  try {
    std::ifstream infile(filepath);
    json j;
    infile >> j;
    infile.close();

    stock_info_.clear();
    for (auto &[code, info_json] : j.items()) {
      StockInfo info;
      info.name = info_json.value("name", "");
      info.ipoDate = info_json.value("ipoDate", "");
      info.outDate = info_json.value("outDate", "");
      info.ind_code = info_json.value("ind_code", "");
      info.ind_name = info_json.value("ind_name", "");
      info.update_date = info_json.value("update_date", "");
      info.volume = info_json.value("volume", "");
      info.amount = info_json.value("amount", "");
      info.turn = info_json.value("turn", "");
      info.tradestatus = info_json.value("tradestatus", "");
      info.isST = info_json.value("isST", "");
      info.peTTM = info_json.value("peTTM", "");
      info.pbMRQ = info_json.value("pbMRQ", "");
      info.psTTM = info_json.value("psTTM", "");
      info.pcfNcfTTM = info_json.value("pcfNcfTTM", "");
      stock_info_[code] = info;
    }

    Log(std::format("Loaded stock_info.json: {} stocks", stock_info_.size()));
  } catch (const std::exception &e) {
    Log(std::format("Error loading stock_info.json: {}", e.what()), true);
    stock_info_.clear();
    if (fs::exists(filepath))
      fs::remove(filepath);
  }

  co_return;
}

awaitable<void> DataManager::load_stock_days() {
  std::string filepath = config_->config_dir + "/" + config_->baostock_stock_days_file;

  if (!fs::exists(filepath)) {
    Log("stock_days.json not found, will fetch initial data");
    stock_days_.clear();
    co_return;
  }

  try {
    std::ifstream infile(filepath);
    json j;
    infile >> j;
    infile.close();

    stock_days_ = j.get<std::vector<std::vector<std::string>>>();
    Log(std::format("Loaded stock_days.json: {} days", stock_days_.size()));
  } catch (const std::exception &e) {
    Log(std::format("Error loading stock_days.json: {}", e.what()), true);
    stock_days_.clear();
    if (fs::exists(filepath))
      fs::remove(filepath);
  }

  co_return;
}

awaitable<void> DataManager::save_stock_factor() {
  std::string filepath = config_->config_dir + "/" + config_->baostock_stock_factor_file;
  std::string temp_filepath = filepath + ".tmp";

  std::ofstream outfile(temp_filepath);
  outfile << "{\n";

  bool first_stock = true;
  for (const auto &[code, factor_data] : stock_factor_) {
    if (!first_stock)
      outfile << ",\n";
    first_stock = false;

    outfile << "  \"" << code << "\": {\n";
    outfile << "    \"last_update\": \"" << factor_data.last_update << "\",\n";
    outfile << "    \"data\": [\n";
    for (size_t i = 0; i < factor_data.data.size(); ++i) {
      outfile << "      [\"" << factor_data.data[i][0] << "\",\"" << factor_data.data[i][1] << "\"]";
      if (i < factor_data.data.size() - 1)
        outfile << ",";
      outfile << "\n";
    }
    outfile << "    ]\n";
    outfile << "  }";
  }

  outfile << "\n}\n";
  outfile.close();

  fs::rename(temp_filepath, filepath);
  Log("Saved stock_factor.json");
  co_return;
}

awaitable<void> DataManager::save_stock_info() {
  std::string filepath = config_->config_dir + "/" + config_->baostock_stock_info_file;
  std::string temp_filepath = filepath + ".tmp";

  std::ofstream outfile(temp_filepath);
  outfile << "{\n";

  bool first_stock = true;
  for (const auto &[code, info] : stock_info_) {
    if (!first_stock)
      outfile << ",\n";
    first_stock = false;

    outfile << "  \"" << code << "\": {";
    outfile << "\"name\":\"" << info.name << "\",";
    outfile << "\"ipoDate\":\"" << info.ipoDate << "\",";
    outfile << "\"outDate\":\"" << info.outDate << "\",";
    outfile << "\"ind_code\":\"" << info.ind_code << "\",";
    outfile << "\"ind_name\":\"" << info.ind_name << "\",";
    outfile << "\"update_date\":\"" << info.update_date << "\",";
    outfile << "\"volume\":\"" << info.volume << "\",";
    outfile << "\"amount\":\"" << info.amount << "\",";
    outfile << "\"turn\":\"" << info.turn << "\",";
    outfile << "\"tradestatus\":\"" << info.tradestatus << "\",";
    outfile << "\"isST\":\"" << info.isST << "\",";
    outfile << "\"peTTM\":\"" << info.peTTM << "\",";
    outfile << "\"pbMRQ\":\"" << info.pbMRQ << "\",";
    outfile << "\"psTTM\":\"" << info.psTTM << "\",";
    outfile << "\"pcfNcfTTM\":\"" << info.pcfNcfTTM << "\"}";
  }

  outfile << "\n}\n";
  outfile.close();

  fs::rename(temp_filepath, filepath);
  Log("Saved stock_info.json");
  co_return;
}

awaitable<void> DataManager::save_stock_days() {
  std::string filepath = config_->config_dir + "/" + config_->baostock_stock_days_file;
  std::string temp_filepath = filepath + ".tmp";

  std::ofstream outfile(temp_filepath);
  outfile << "[\n";

  for (size_t i = 0; i < stock_days_.size(); ++i) {
    outfile << "  [\"" << stock_days_[i][0] << "\",\"" << stock_days_[i][1] << "\"]";
    if (i < stock_days_.size() - 1)
      outfile << ",";
    outfile << "\n";
  }

  outfile << "]\n";
  outfile.close();

  fs::rename(temp_filepath, filepath);
  Log("Saved stock_days.json");
  co_return;
}

// ============================================================================
// Update Operations
// ============================================================================

awaitable<void> DataManager::update_stock_days(bool skip_login, bool skip_logout, const std::string &force_start_date) {
  if (progress_callback_) {
    progress_callback_("stock_days", "", 0, 0, UpdateStage::UpdatingStockDays);
  }

  // Step 1: Load local file + check integrity
  co_await load_stock_days();
  auto integrity = check_stock_days_integrity(l2_database_start_date_);

  // Step 2: Determine start date
  std::string start_date;

  if (stock_days_.empty() || !integrity.passed) {
    // Empty file OR integrity failed → rebuild from L2 database start
    std::string l2_start = !force_start_date.empty() ? force_start_date : l2_database_start_date_;

    if (l2_start.empty()) {
      Log("[ERROR] Cannot update stock_days: L2 database start date not available. Please run 'Update All' or 'Scan Assets' first.", true);
      if (progress_callback_) {
        progress_callback_("stock_days", "ERROR: Missing L2 database start date", 0, 0, UpdateStage::Complete);
      }
      co_return;
    }

    // Convert from YYYYMMDD to YYYY-MM-DD
    if (l2_start.length() == 8) {
      start_date = l2_start.substr(0, 4) + "-" + l2_start.substr(4, 2) + "-" + l2_start.substr(6, 2);
    } else {
      start_date = l2_start; // Already in YYYY-MM-DD format
    }

    if (!integrity.passed) {
      Log("[WARN] stock_days integrity check failed - rebuilding from L2 database start", true);
    }
    Log(std::format("Fetching stock_days from L2 database start: {}", start_date));
    stock_days_.clear(); // Clear corrupted data

  } else {
    // Incremental update from last day
    start_date = increment_date(stock_days_.back()[0]);
  }

  std::string end_date = get_today_date();

  // Step 3: Check if already up-to-date
  if (start_date > end_date) {
    Log("stock_days already up to date");
    if (progress_callback_) {
      progress_callback_("stock_days", "Already up-to-date", 0, 0, UpdateStage::Complete);
    }
    co_return;
  }

  // Step 3: Update needed → Lazy login
  Log("=== Updating stock_days ===");

  if (!skip_login) {
    if (!co_await ensure_logged_in()) {
      Log("[ERROR] Failed to login workers", true);
      if (progress_callback_) {
        progress_callback_("stock_days", "Login failed", 0, 0, UpdateStage::Complete);
      }
      co_return;
    }
  }

  if (progress_callback_) {
    progress_callback_("stock_days", "Fetching trade dates", 0, 0, UpdateStage::Fetching);
  }

  auto client = pool_->get_client(0);
  auto result = co_await client->query_trade_dates(start_date, end_date);

  // Increment query count after each query
  if (result.success()) {
    increment_query_count();
  }

  if (result.success()) {
    if (progress_callback_) {
      progress_callback_("stock_days", std::format("Adding {} days", result.records.size()), 0, 0, UpdateStage::Saving);
    }

    for (const auto &record : result.records) {
      stock_days_.push_back(record);
    }
    deduplicate_and_sort_days();
    co_await save_stock_days();
    Log(std::format("Updated stock_days: added {} days", result.records.size()));
  } else {
    Log(std::format("Failed to update stock_days: {}", result.error_msg), true);
  }

  // Ensure logged out (unless skipped)
  if (!skip_logout) {
    co_await ensure_logged_out();
  }

  if (result.success()) {
    if (progress_callback_) {
      progress_callback_("stock_days", "", 0, 0, UpdateStage::Complete);
    }
  } else {
    if (progress_callback_) {
      progress_callback_("stock_days", "Failed: " + result.error_msg, 0, 0, UpdateStage::Complete);
    }
  }

  co_return;
}

awaitable<void> DataManager::update_stock_factor(bool skip_login, bool skip_logout) {
  if (progress_callback_) {
    progress_callback_("stock_factor", "", 0, stock_codes_.size(), UpdateStage::UpdatingStockFactor);
  }

  // Step 1: Load local file + check integrity (always)
  co_await load_stock_factor();
  auto integrity = check_stock_factor_integrity();
  if (!integrity.passed) {
    Log("[WARN] stock_factor integrity check failed", true);
  }

  // Step 2: Update stock_days first (needed for date range calculations)
  co_await update_stock_days(true, true);

  // Step 3: Check if any stock needs update
  std::string today = get_today_date();
  size_t total_stocks = stock_codes_.size();
  bool needs_update = false;

  for (const auto &code : stock_codes_) {
    bool this_stock_needs_update = false;

    // Case 1: Stock has no data yet
    if (stock_factor_.find(code) == stock_factor_.end() ||
        stock_factor_[code].data.empty()) {
      this_stock_needs_update = true;
    }
    // Case 2: last_update is not today (triggered or auto)
    else if (stock_factor_[code].last_update != today) {
      std::string start_date = increment_date(stock_factor_[code].data.back()[0]);
      // Only if there's new data to fetch
      if (start_date <= today) {
        this_stock_needs_update = true;
      }
    }
    // Case 3: last_update == today, no update needed

    if (this_stock_needs_update) {
      needs_update = true;
      break;
    }
  }

  if (!needs_update) {
    Log("stock_factor already up to date (all stocks current)");
    if (progress_callback_) {
      progress_callback_("stock_factor", "Already up-to-date", total_stocks, total_stocks, UpdateStage::Complete);
    }
    co_return; // No login needed!
  }

  // Step 4: Update needed → Lazy login
  Log("=== Updating stock_factor ===");

  if (!skip_login) {
    if (!co_await ensure_logged_in()) {
      Log("[ERROR] Failed to login workers", true);
      if (progress_callback_) {
        progress_callback_("stock_factor", "Login failed", 0, total_stocks, UpdateStage::Complete);
      }
      co_return;
    }
  }

  // Step 5: Submit tasks
  auto processed_count = std::make_shared<std::atomic<size_t>>(0);
  int updated_count = 0;
  int error_count = 0;

  for (const auto &code : stock_codes_) {
    std::string start_date = "1990-01-01";

    if (stock_factor_.find(code) != stock_factor_.end() &&
        !stock_factor_[code].data.empty()) {
      start_date = increment_date(stock_factor_[code].data.back()[0]);

      // Check if already updated today (per-stock check)
      // Trigger update doesn't mean "force update all", just "check and fill gaps"
      if (stock_factor_[code].last_update == today) {
        size_t current = processed_count->fetch_add(1) + 1;
        if (progress_callback_) {
          progress_callback_("stock_factor", code + " (up-to-date)", current, total_stocks, UpdateStage::Fetching);
        }
        continue;
      }

      // Skip if start_date is beyond today (no future data)
      if (start_date > today) {
        size_t current = processed_count->fetch_add(1) + 1;
        if (progress_callback_) {
          progress_callback_("stock_factor", code + " (no new data)", current, total_stocks, UpdateStage::Fetching);
        }
        continue;
      }
    }

    // Submit task for update
    Task task;
    task.description = "adjust_factor:" + code;
    task.executor = [code, start_date, today](BaostockClient &client) -> awaitable<QueryResult> {
      co_return co_await client.query_adjust_factor(code, start_date, today);
    };

    pool_->submit_task(std::move(task));
  }

  auto pending_workers = std::make_shared<std::atomic<int>>(config_->baostock_max_workers);

  for (size_t i = 0; i < static_cast<size_t>(config_->baostock_max_workers); ++i) {
    boost::asio::co_spawn(
        io_context_,
        [this, i, pending_workers, &updated_count, &error_count, processed_count, total_stocks]() -> awaitable<void> {
          auto client = pool_->get_client(i);
          while (pool_->has_pending_tasks()) {
            auto task_opt = pool_->get_next_task();
            if (!task_opt)
              break;

            std::string desc = task_opt->description;
            std::string code = desc.substr(desc.find(':') + 1);

            ++active_workers_; // Start executing query
            auto result = co_await task_opt->executor(*client);

            // Increment query count BEFORE decrementing active workers (so UI sees active workers)
            if (result.success()) {
              increment_query_count();
            }

            --active_workers_; // Finish executing query

            if (result.success() && !result.records.empty()) {
              for (const auto &record : result.records) {
                stock_factor_[code].data.push_back({record[1], record[4]});
              }
              deduplicate_and_sort_factor(code);
              stock_factor_[code].last_update = get_today_date();
              ++updated_count;
            } else if (result.success() && result.records.empty()) {
              // No new data, but still update last_update to today
              stock_factor_[code].last_update = get_today_date();
              ++updated_count;
            } else if (!result.success()) {
              ++error_count;
            }

            size_t current = processed_count->fetch_add(1) + 1;
            if (progress_callback_) {
              progress_callback_("stock_factor", code, current, total_stocks, UpdateStage::Fetching);
            }
          }
          pending_workers->fetch_sub(1);
        },
        boost::asio::detached);
  }

  while (pending_workers->load() > 0) {
    co_await boost::asio::post(boost::asio::use_awaitable);
  }

  if (progress_callback_) {
    progress_callback_("stock_factor", "", total_stocks, total_stocks, UpdateStage::Saving);
  }

  co_await save_stock_factor();

  // Ensure logged out (unless skipped)
  if (!skip_logout) {
    co_await ensure_logged_out();
  }

  if (progress_callback_) {
    progress_callback_("stock_factor", "", total_stocks, total_stocks, UpdateStage::Complete);
  }

  co_return;
}

awaitable<void> DataManager::update_stock_info_weekly(bool skip_days, bool skip_login, bool skip_logout) {
  if (progress_callback_) {
    progress_callback_("stock_info", "", 0, 0, UpdateStage::UpdatingStockInfoWeekly);
  }

  // Step 1: Load local file + check integrity (always)
  co_await load_stock_info();
  auto integrity = check_stock_info_integrity();
  if (!integrity.passed) {
    Log("[WARN] stock_info integrity check failed", true);
  }

  // Step 2: Update stock_days if not already done
  if (!skip_days) {
    co_await update_stock_days(true, true);
  }

  // Step 3: Check if update is needed
  // Only update if: integrity fail, or any stock has incomplete weekly data
  bool integrity_failed = !integrity.passed;

  // If integrity failed: clear all data and refetch everything
  if (integrity_failed) {
    Log("[WARN] Integrity failed, clearing stock_info for full refetch");
    stock_info_.clear();
  }

  // Check if any stock needs weekly update (incomplete or missing weekly fields)
  bool needs_update = false;
  for (const auto &code : stock_codes_) {
    if (stock_info_.find(code) == stock_info_.end()) {
      needs_update = true;
      break;
    }
    const auto &info = stock_info_[code];
    if (info.name.empty() || info.ipoDate.empty()) {
      needs_update = true;
      break;
    }
  }

  if (!integrity_failed && !needs_update) {
    Log("stock_info_weekly already up to date (all stocks have complete weekly data)");
    if (progress_callback_) {
      progress_callback_("stock_info", "Already up-to-date", 0, 0, UpdateStage::Complete);
    }
    co_return; // No login needed!
  }

  // Step 4: Update needed → Lazy login
  Log("=== Updating stock_info (weekly) ===");

  if (!skip_login) {
    if (!co_await ensure_logged_in()) {
      Log("[ERROR] Failed to login workers", true);
      if (progress_callback_) {
        progress_callback_("stock_info", "Login failed", 0, 0, UpdateStage::Complete);
      }
      co_return;
    }
  }

  // Step 5: Fetch industry data
  std::map<std::string, std::pair<std::string, std::string>> industry_map;
  auto client = pool_->get_client(0);
  auto result = co_await client->query_stock_industry();

  // Increment query count after each query
  if (result.success()) {
    increment_query_count();
  }

  if (result.success()) {
    for (const auto &record : result.records) {
      std::string code = record[1];
      std::string industry = record[3];
      std::string ind_code, ind_name;
      if (industry.length() > 3) {
        ind_code = industry.substr(0, 3);
        ind_name = industry.substr(3);
      }
      industry_map[code] = {ind_code, ind_name};
    }
    Log(std::format("Fetched industry data for {} stocks", industry_map.size()));
  } else {
    Log("Failed to fetch industry data, aborting weekly update", true);
    if (progress_callback_) {
      progress_callback_("stock_info", "Failed to fetch industry data", 0, 0, UpdateStage::Complete);
    }
    if (!skip_logout) {
      co_await ensure_logged_out();
    }
    co_return;
  }
  size_t total_stocks = stock_codes_.size();
  auto processed_count = std::make_shared<std::atomic<size_t>>(0);

  for (const auto &code : stock_codes_) {
    // Only query stocks that actually need weekly updates
    bool this_stock_needs_update = false;

    if (stock_info_.find(code) == stock_info_.end()) {
      this_stock_needs_update = true;
    } else {
      const auto &info = stock_info_[code];
      // Check for mandatory weekly fields
      if (info.name.empty() || info.ipoDate.empty()) {
        this_stock_needs_update = true;
      }
    }

    if (!integrity_failed && !this_stock_needs_update) {
      size_t current = processed_count->fetch_add(1) + 1;
      if (progress_callback_) {
        progress_callback_("stock_info", code + " (skipped)", current, total_stocks, UpdateStage::Fetching);
      }
      continue;
    }

    Task task;
    task.description = "stock_basic:" + code;
    task.executor = [code](BaostockClient &client) -> awaitable<QueryResult> {
      co_return co_await client.query_stock_basic(code);
    };
    pool_->submit_task(std::move(task));
  }

  int updated_count = 0;
  int error_count = 0;
  auto pending_workers = std::make_shared<std::atomic<int>>(config_->baostock_max_workers);

  for (size_t i = 0; i < static_cast<size_t>(config_->baostock_max_workers); ++i) {
    boost::asio::co_spawn(
        io_context_,
        [this, i, pending_workers, &industry_map, &updated_count, &error_count, processed_count, total_stocks]() -> awaitable<void> {
          auto client = pool_->get_client(i);
          while (pool_->has_pending_tasks()) {
            auto task_opt = pool_->get_next_task();
            if (!task_opt)
              break;

            std::string desc = task_opt->description;
            std::string code = desc.substr(desc.find(':') + 1);

            ++active_workers_; // Start executing query
            auto result = co_await task_opt->executor(*client);

            // Increment query count BEFORE decrementing active workers (so UI sees active workers)
            if (result.success()) {
              increment_query_count();
            }

            --active_workers_; // Finish executing query

            // Create or get existing stock info
            StockInfo info;
            if (stock_info_.find(code) != stock_info_.end()) {
              info = stock_info_[code]; // Keep existing data if integrity_failed=false
            }

            if (result.success() && !result.records.empty()) {
              auto &record = result.records[0];
              // Update weekly fields
              info.name = record[1];
              info.ipoDate = record[2];
              info.outDate = record[3];

              if (industry_map.find(code) != industry_map.end()) {
                info.ind_code = industry_map[code].first;
                info.ind_name = industry_map[code].second;
              }
              ++updated_count;
            } else {
              // Query failed
              ++error_count;
            }

            stock_info_[code] = info;

            size_t current = processed_count->fetch_add(1) + 1;
            if (progress_callback_) {
              progress_callback_("stock_info", code, current, total_stocks, UpdateStage::Fetching);
            }
          }
          pending_workers->fetch_sub(1);
        },
        boost::asio::detached);
  }

  while (pending_workers->load() > 0) {
    co_await boost::asio::post(boost::asio::use_awaitable);
  }

  if (progress_callback_) {
    progress_callback_("stock_info", "", total_stocks, total_stocks, UpdateStage::Saving);
  }

  co_await save_stock_info();
  std::string today = get_today_date();
  stock_info_last_update_[kMetaStockInfoWeekly] = today;
  co_await save_config(config_->config_dir + "/" + config_->baostock_data_manager_config);

  Log("=== Weekly update complete, now updating daily fields ===");

  // Weekly update = complete update, so also update daily fields
  // skip_days=true, skip_login=true (already logged in), skip_logout=skip_logout (inherited)
  co_await update_stock_info_daily(true, true, skip_logout);

  if (progress_callback_) {
    progress_callback_("stock_info", "", total_stocks, total_stocks, UpdateStage::Complete);
  }

  co_return;
}

awaitable<void> DataManager::update_stock_info_daily(bool skip_days, bool skip_login, bool skip_logout) {
  if (progress_callback_) {
    progress_callback_("stock_info", "", 0, 0, UpdateStage::UpdatingStockInfoDaily);
  }

  // Step 1: Load local file + check integrity (always)
  co_await load_stock_info();
  auto integrity = check_stock_info_integrity();

  // If integrity failed: abort, need to run weekly update first
  if (!integrity.passed) {
    Log("[ERROR] stock_info integrity check failed - run weekly update first", true);
    if (progress_callback_) {
      progress_callback_("stock_info", "Integrity failed - run weekly update", 0, 0, UpdateStage::Complete);
    }
    co_return; // No login needed!
  }

  // Step 2: Update stock_days if not already done
  if (!skip_days) {
    co_await update_stock_days(true, true);
  }

  // Step 3: Check if daily update is needed
  // Only update daily fields for stocks whose update_date < target_date
  std::string target_date = get_last_trading_day();
  bool needs_update = false;

  for (const auto &code : stock_codes_) {
    // Skip delisted stocks
    if (stock_info_.find(code) != stock_info_.end() &&
        !stock_info_[code].outDate.empty()) {
      continue;
    }

    // Check if already updated to target date
    if (stock_info_.find(code) != stock_info_.end() &&
        stock_info_[code].update_date >= target_date) {
      continue;
    }

    // At least one stock needs daily update
    needs_update = true;
    break;
  }

  if (!needs_update) {
    Log("stock_info_daily already up to date (all stocks current for " + target_date + ")");
    if (progress_callback_) {
      progress_callback_("stock_info", "Already up-to-date", 0, 0, UpdateStage::Complete);
    }
    co_return; // No login needed!
  }

  // Step 4: Update needed → Lazy login
  Log(std::format("=== Updating stock_info (daily) for {} ===", target_date));

  if (!skip_login) {
    if (!co_await ensure_logged_in()) {
      Log("[ERROR] Failed to login workers", true);
      if (progress_callback_) {
        progress_callback_("stock_info", "Login failed", 0, 0, UpdateStage::Complete);
      }
      co_return;
    }
  }

  // Step 5: Submit tasks
  size_t total_stocks = stock_codes_.size();
  auto processed_count = std::make_shared<std::atomic<size_t>>(0);
  int updated_count = 0;
  int error_count = 0;

  for (const auto &code : stock_codes_) {
    // Skip delisted stocks
    if (stock_info_.find(code) != stock_info_.end() &&
        !stock_info_[code].outDate.empty()) {
      size_t current = processed_count->fetch_add(1) + 1;
      if (progress_callback_) {
        progress_callback_("stock_info", code + " (delisted)", current, total_stocks, UpdateStage::Fetching);
      }
      continue;
    }

    // Skip if already updated to target date
    // Trigger update doesn't mean "force update all", just "check and fill gaps"
    if (stock_info_.find(code) != stock_info_.end() &&
        stock_info_[code].update_date >= target_date) {
      size_t current = processed_count->fetch_add(1) + 1;
      if (progress_callback_) {
        progress_callback_("stock_info", code + " (skipped)", current, total_stocks, UpdateStage::Fetching);
      }
      continue;
    }

    // Submit task for update
    Task task;
    task.description = "k_data:" + code;
    task.executor = [code, target_date](BaostockClient &client) -> awaitable<QueryResult> {
      co_return co_await client.query_history_k_data_plus(
          code, "date,volume,amount,turn,tradestatus,isST,peTTM,pbMRQ,psTTM,pcfNcfTTM",
          target_date, target_date, "d", "3");
    };
    pool_->submit_task(std::move(task));
  }

  auto pending_workers = std::make_shared<std::atomic<int>>(config_->baostock_max_workers);

  for (size_t i = 0; i < static_cast<size_t>(config_->baostock_max_workers); ++i) {
    boost::asio::co_spawn(
        io_context_,
        [this, i, pending_workers, target_date, &updated_count, &error_count, processed_count, total_stocks]() -> awaitable<void> {
          auto client = pool_->get_client(i);
          while (pool_->has_pending_tasks()) {
            auto task_opt = pool_->get_next_task();
            if (!task_opt)
              break;

            std::string desc = task_opt->description;
            std::string code = desc.substr(desc.find(':') + 1);

            ++active_workers_; // Start executing query
            auto result = co_await task_opt->executor(*client);

            // Increment query count BEFORE decrementing active workers (so UI sees active workers)
            if (result.success()) {
              increment_query_count();
            }

            --active_workers_; // Finish executing query

            if (stock_info_.find(code) == stock_info_.end()) {
              stock_info_[code] = StockInfo();
            }

            if (result.success() && !result.records.empty()) {
              auto &record = result.records[0];
              stock_info_[code].update_date = record[0];
              stock_info_[code].volume = record[1];
              stock_info_[code].amount = record[2];
              stock_info_[code].turn = record[3];
              stock_info_[code].tradestatus = record[4];
              stock_info_[code].isST = record[5];
              stock_info_[code].peTTM = record[6];
              stock_info_[code].pbMRQ = record[7];
              stock_info_[code].psTTM = record[8];
              stock_info_[code].pcfNcfTTM = record[9];
              ++updated_count;
            } else if (result.success() && result.records.empty()) {
              stock_info_[code].update_date = target_date;
              stock_info_[code].volume = "";
              stock_info_[code].amount = "";
              stock_info_[code].turn = "";
              stock_info_[code].tradestatus = "";
              stock_info_[code].isST = "";
              stock_info_[code].peTTM = "";
              stock_info_[code].pbMRQ = "";
              stock_info_[code].psTTM = "";
              stock_info_[code].pcfNcfTTM = "";
              ++updated_count;
            } else {
              ++error_count;
            }

            size_t current = processed_count->fetch_add(1) + 1;
            if (progress_callback_) {
              progress_callback_("stock_info", code, current, total_stocks, UpdateStage::Fetching);
            }
          }
          pending_workers->fetch_sub(1);
        },
        boost::asio::detached);
  }

  while (pending_workers->load() > 0) {
    co_await boost::asio::post(boost::asio::use_awaitable);
  }

  if (progress_callback_) {
    progress_callback_("stock_info", "", total_stocks, total_stocks, UpdateStage::Saving);
  }

  co_await save_stock_info();
  stock_info_last_update_[kMetaStockInfoDaily] = target_date;
  co_await save_config(config_->config_dir + "/" + config_->baostock_data_manager_config);

  // Ensure logged out (unless skipped)
  if (!skip_logout) {
    co_await ensure_logged_out();
  }

  if (progress_callback_) {
    progress_callback_("stock_info", "", total_stocks, total_stocks, UpdateStage::Complete);
  }

  co_return;
}

awaitable<void> DataManager::update_all(const std::string &l2_database_start_date) {
  Log("=== Running full update cycle ===");

  // Step 0: Quick check if ANY update is needed (without login)
  // Load all data and check what needs updating
  co_await load_stock_days();
  co_await load_stock_factor();
  co_await load_stock_info();

  bool needs_days = false;
  bool needs_factor = false;
  bool needs_info_weekly = false;
  bool needs_info_daily = false;

  // Check stock_days
  if (stock_days_.empty()) {
    needs_days = true;
  } else {
    std::string last_date = stock_days_.back()[0];
    std::string today = get_today_date();
    if (last_date < today) {
      needs_days = true;
    }
  }

  // Check stock_factor (per-stock last_update)
  std::string today = get_today_date();
  for (const auto &code : stock_codes_) {
    if (stock_factor_.find(code) == stock_factor_.end()) {
      needs_factor = true;
      break;
    }
    const auto &last_update = stock_factor_[code].last_update;
    if (last_update < today) {
      needs_factor = true;
      break;
    }
  }

  // Check stock_info integrity
  auto info_integrity = check_stock_info_integrity();
  if (!info_integrity.passed) {
    // Integrity failed → need both weekly and daily updates
    needs_info_weekly = true;
    needs_info_daily = true;
  } else {
    // Check if weekly data needs update
    for (const auto &code : stock_codes_) {
      if (stock_info_.find(code) == stock_info_.end() ||
          stock_info_[code].name.empty() || stock_info_[code].ipoDate.empty()) {
        needs_info_weekly = true;
        break;
      }
    }

    // Check if daily data needs update
    std::string target_date = get_last_trading_day();
    for (const auto &code : stock_codes_) {
      if (stock_info_.find(code) == stock_info_.end())
        continue;
      if (!stock_info_[code].outDate.empty())
        continue; // Skip delisted
      if (stock_info_[code].update_date < target_date) {
        needs_info_daily = true;
        break;
      }
    }
  }

  // If nothing needs updating, ensure logged out and return
  if (!needs_days && !needs_factor && !needs_info_weekly && !needs_info_daily) {
    Log("All data is up-to-date, no update needed");
    co_await ensure_logged_out(); // Ensure clean state
    if (progress_callback_) {
      progress_callback_("all", "All up-to-date", 0, 0, UpdateStage::Complete);
    }
    co_return;
  }

  // Step 1: Unified login (only if we need to update something)
  Log(std::format("Update needed: days={}, factor={}, info_weekly={}, info_daily={}",
                  needs_days, needs_factor, needs_info_weekly, needs_info_daily));

  if (!co_await ensure_logged_in()) {
    Log("[ERROR] Failed to login workers for update_all", true);
    co_await ensure_logged_out(); // Ensure clean state
    if (progress_callback_) {
      progress_callback_("all", "Login failed", 0, 0, UpdateStage::Complete);
    }
    co_return;
  }

  // Step 2: Update stock_days (skip login/logout, managed by update_all)
  if (needs_days) {
    co_await update_stock_days(true, true, l2_database_start_date);
  }

  // Step 3: Update stock_factor (skip login/logout, managed by update_all)
  if (needs_factor) {
    co_await update_stock_factor(true, true);
  }

  // Step 4: Update stock_info (weekly + daily)
  // skip_days=true to avoid redundant update_days calls
  // skip_login=true, skip_logout=true as login/logout managed by update_all
  if (needs_info_weekly) {
    co_await update_stock_info_weekly(true, true, true);
  }
  if (needs_info_daily) {
    co_await update_stock_info_daily(true, true, true);
  }

  // Step 5: Unified logout after all operations
  co_await ensure_logged_out();

  if (progress_callback_) {
    progress_callback_("all", "", 0, 0, UpdateStage::Complete);
  }

  Log("=== Full update cycle complete ===");
  co_return;
}

// ============================================================================
// Integrity Checks
// ============================================================================

IntegrityResult DataManager::check_stock_days_integrity(const std::string &l2_database_start_date) {
  IntegrityResult result;

  if (stock_days_.empty()) {
    result.passed = false;
    result.errors.push_back("stock_days is empty");
    return result;
  }

  // Check if stock_days starts from L2 database start (if available)
  std::string l2_start = !l2_database_start_date.empty() ? l2_database_start_date : l2_database_start_date_;
  if (!l2_start.empty()) {
    // Get stock_days start date (YYYY-MM-DD format)
    std::string stock_days_start = stock_days_[0][0];
    // Remove dashes: YYYY-MM-DD -> YYYYMMDD
    std::string stock_days_start_yyyymmdd = stock_days_start;
    stock_days_start_yyyymmdd.erase(std::remove(stock_days_start_yyyymmdd.begin(),
                                                stock_days_start_yyyymmdd.end(), '-'),
                                    stock_days_start_yyyymmdd.end());

    // Compare with L2 database start (YYYYMMDD format)
    if (stock_days_start_yyyymmdd > l2_start) {
      result.passed = false;
      result.errors.push_back(
          "stock_days starts at " + stock_days_start +
          " but L2 database starts at " + l2_start +
          " - missing date coverage! Delete stock_days.json to rebuild from L2 start.");
    }
  }

  std::set<std::string> seen_dates;
  for (const auto &day : stock_days_) {
    if (seen_dates.count(day[0])) {
      result.warnings.push_back("Duplicate date: " + day[0]);
    }
    seen_dates.insert(day[0]);
  }

  for (size_t i = 1; i < stock_days_.size(); ++i) {
    std::string prev = stock_days_[i - 1][0];
    std::string curr = stock_days_[i][0];
    std::string expected = increment_date(prev);
    if (curr != expected) {
      result.warnings.push_back("Date gap: " + prev + " -> " + curr);
    }
  }

  result.passed = result.errors.empty();
  return result;
}

IntegrityResult DataManager::check_stock_factor_integrity() {
  IntegrityResult result;

  for (const auto &code : stock_codes_) {
    // if (stock_factor_.find(code) == stock_factor_.end()) {
    //   result.missing_stocks.push_back(code);
    //   continue;
    // }

    auto &records = stock_factor_[code].data;
    if (records.empty()) {
      result.missing_stocks.push_back(code);
      continue;
    }

    for (size_t i = 1; i < records.size(); ++i) {
      if (records[i][0] < records[i - 1][0]) {
        result.warnings.push_back(code + ": dates not sorted");
        break;
      }
    }

    std::set<std::string> seen_dates;
    for (const auto &record : records) {
      if (seen_dates.count(record[0])) {
        result.warnings.push_back(code + ": duplicate date " + record[0]);
      }
      seen_dates.insert(record[0]);
    }
  }

  result.passed = result.missing_stocks.empty() && result.errors.empty();
  return result;
}

IntegrityResult DataManager::check_stock_info_integrity() {
  IntegrityResult result;

  for (const auto &code : stock_codes_) {
    if (stock_info_.find(code) == stock_info_.end()) {
      result.missing_stocks.push_back(code);
      continue;
    }

    const auto &info = stock_info_.at(code);

    if (info.outDate.empty()) {
      if (info.name.empty() || info.ipoDate.empty()) {
        result.incomplete_stocks.push_back(code);
      }
    }
  }

  result.passed = result.missing_stocks.empty() &&
                  result.incomplete_stocks.empty() &&
                  result.errors.empty();
  return result;
}

IntegrityResult DataManager::check_all_integrity(const std::string &l2_database_start_date) {
  IntegrityResult combined;

  auto days_result = check_stock_days_integrity(l2_database_start_date);
  auto factor_result = check_stock_factor_integrity();
  auto info_result = check_stock_info_integrity();

  combined.passed = days_result.passed && factor_result.passed && info_result.passed;

  combined.errors.insert(combined.errors.end(), days_result.errors.begin(), days_result.errors.end());
  combined.errors.insert(combined.errors.end(), factor_result.errors.begin(), factor_result.errors.end());
  combined.errors.insert(combined.errors.end(), info_result.errors.begin(), info_result.errors.end());

  combined.warnings.insert(combined.warnings.end(), days_result.warnings.begin(), days_result.warnings.end());
  combined.warnings.insert(combined.warnings.end(), factor_result.warnings.begin(), factor_result.warnings.end());
  combined.warnings.insert(combined.warnings.end(), info_result.warnings.begin(), info_result.warnings.end());

  combined.missing_stocks.insert(combined.missing_stocks.end(),
                                 factor_result.missing_stocks.begin(),
                                 factor_result.missing_stocks.end());
  combined.missing_stocks.insert(combined.missing_stocks.end(),
                                 info_result.missing_stocks.begin(),
                                 info_result.missing_stocks.end());

  combined.incomplete_stocks = info_result.incomplete_stocks;

  return combined;
}

// ============================================================================
// Progress Tracking
// ============================================================================

std::string DataManager::get_next_update_time_weekly() const {
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
  localtime_r(&time_t_now, &tm_now);

  int current_weekday = (tm_now.tm_wday == 0) ? 7 : tm_now.tm_wday;
  int days_until_monday = (config_->baostock_weekly_update_day - current_weekday + 7) % 7;
  auto it = stock_info_last_update_.find(kMetaStockInfoWeekly);
  if (days_until_monday == 0 && it != stock_info_last_update_.end() && it->second == get_today_date()) {
    days_until_monday = 7;
  }

  auto next_update = now + std::chrono::hours(24 * days_until_monday);
  auto time_t_next = std::chrono::system_clock::to_time_t(next_update);
  std::tm tm_next;
  localtime_r(&time_t_next, &tm_next);

  std::ostringstream oss;
  oss << std::put_time(&tm_next, "%Y-%m-%d");
  return oss.str();
}

std::string DataManager::get_next_update_time_daily() const {
  std::string today = get_today_date();

  for (size_t i = 0; i < stock_days_.size(); ++i) {
    if (stock_days_[i][0] > today && stock_days_[i][1] == "1") {
      return stock_days_[i][0];
    }
  }

  return "Unknown";
}

double DataManager::get_update_progress_weekly() const {
  auto now = std::chrono::system_clock::now();

  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
  localtime_r(&time_t_now, &tm_now);

  int current_weekday = (tm_now.tm_wday == 0) ? 7 : tm_now.tm_wday;
  int days_since_monday = (current_weekday - config_->baostock_weekly_update_day + 7) % 7;

  return (days_since_monday / 7.0) * 100.0;
}

double DataManager::get_update_progress_daily() const {
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
  localtime_r(&time_t_now, &tm_now);

  double hours_elapsed = tm_now.tm_hour + tm_now.tm_min / 60.0;
  return (hours_elapsed / 24.0) * 100.0;
}

StatusSummary DataManager::get_status_summary() const {
  StatusSummary summary;

  auto it_weekly = stock_info_last_update_.find(kMetaStockInfoWeekly);
  auto it_daily = stock_info_last_update_.find(kMetaStockInfoDaily);
  summary.last_weekly_update = (it_weekly != stock_info_last_update_.end()) ? it_weekly->second : "";
  summary.last_daily_update = (it_daily != stock_info_last_update_.end()) ? it_daily->second : "";
  summary.weekly_progress_pct = get_update_progress_weekly();
  summary.daily_progress_pct = get_update_progress_daily();
  summary.next_weekly_update = get_next_update_time_weekly();
  summary.next_daily_update = get_next_update_time_daily();
  summary.total_stocks = stock_codes_.size();
  summary.stocks_with_factor_data = stock_factor_.size();
  summary.stocks_with_info_data = stock_info_.size();
  summary.trading_days_count = stock_days_.size();

  return summary;
}

// ============================================================================
// Force Remove Operations
// ============================================================================

bool DataManager::force_remove_stock_factor() {
  std::string filepath = config_->config_dir + "/" + config_->baostock_stock_factor_file;
  if (fs::exists(filepath)) {
    fs::remove(filepath);
    stock_factor_.clear();
    Log("Removed stock_factor.json");
    return true;
  }
  return false;
}

bool DataManager::force_remove_stock_info() {
  std::string filepath = config_->config_dir + "/" + config_->baostock_stock_info_file;
  if (fs::exists(filepath)) {
    fs::remove(filepath);
    stock_info_.clear();
    stock_info_last_update_.clear();
    Log("Removed stock_info.json (timers reset)");
    return true;
  }
  return false;
}

bool DataManager::force_remove_stock_days() {
  std::string filepath = config_->config_dir + "/" + config_->baostock_stock_days_file;
  if (fs::exists(filepath)) {
    fs::remove(filepath);
    stock_days_.clear();
    Log("Removed stock_days.json");
    return true;
  }
  return false;
}

// ============================================================================
// Up-to-date Checks
// ============================================================================

bool DataManager::is_stock_factor_uptodate() const {
  if (stock_factor_.empty()) {
    return false;
  }

  std::string today = get_today_date();
  // Check if all stocks have been updated today
  for (const auto &[code, factor_data] : stock_factor_) {
    if (factor_data.last_update != today) {
      return false;
    }
  }
  return true;
}

bool DataManager::is_stock_info_uptodate() const {
  if (stock_info_.empty()) {
    return false;
  }
  std::string last_trading_day = get_last_trading_day();
  for (const auto &code : stock_codes_) {
    auto it = stock_info_.find(code);
    if (it == stock_info_.end()) {
      return false;
    }
    if (it->second.name.empty() || it->second.ipoDate.empty()) {
      return false;
    }
    if (!it->second.outDate.empty()) {
      continue;
    }
    if (it->second.update_date < last_trading_day) {
      return false;
    }
  }
  return true;
}

bool DataManager::is_stock_days_uptodate() const {
  if (stock_days_.empty()) {
    return false;
  }
  std::string today = get_today_date();
  return stock_days_.back()[0] >= today;
}

// ============================================================================
// Session Status Reporting
// ============================================================================

void DataManager::report_session_status(BaostockSessionStatus status) {
  if (!crawler_progress_callback_) {
    return;
  }

  CrawlerProgress progress;
  progress.session_status = status;
  progress.session_query_count = session_query_count_;
  progress.total_workers = config_->baostock_max_workers;
  progress.active_workers = active_workers_.load();

  crawler_progress_callback_(progress);
}

void DataManager::increment_query_count() {
  ++session_query_count_;

  // Report updated count
  if (crawler_progress_callback_) {
    CrawlerProgress progress;
    progress.session_status = BaostockSessionStatus::Active;
    progress.session_query_count = session_query_count_;
    progress.total_workers = config_->baostock_max_workers;
    progress.active_workers = active_workers_.load();

    crawler_progress_callback_(progress);
  }
}

} // namespace GUI::Database
