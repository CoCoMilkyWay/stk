#pragma once
#include <chrono>
#include <filesystem>
#include <functional>
#include <string>

struct Config {

  // Backtest/Analysis Period
  std::string start_date = "2025-01-01";
  std::string end_date = "2025-02-01";

  // Paths
  std::string archive_dir = "/mnt/dev/sde/A_stock/L2";
  std::string database_dir = "../../../../output/database";
  std::string feature_dir = "../../../../output/features";
  std::string factor_dir = "../../../../output/factors";
  std::string log_dir = "../../../../output/log";
  std::string config_dir = "../../../../config";

  // L2 Raw Data
  std::string csv_market_data = "行情.csv";
  std::string csv_market_trade = "逐笔成交.csv";
  std::string csv_market_order = "逐笔委托.csv";

  // L2 Binary Database Decompression/Encoding
  std::string archive_extension = ".rar";
  std::string archive_tool = "unrar";
  std::string archive_extract_cmd = "x";
  std::string binary_extension = ".bin";

  // Config file path
  std::string filepath = "../../../../config/config.json";

  // Database (Baostock) Configuration
  std::string assets_file = "assets.json";
  std::string baostock_data_manager_file = "data_manager.json";
  std::string baostock_stock_factor_file = "stock_factor.json";
  std::string baostock_stock_info_file = "stock_info.json";
  std::string baostock_stock_days_file = "stock_days.json";
  int baostock_max_workers = 10;
  int baostock_weekly_update_day = 1; // Monday

  // String buffers for GUI (max 512 chars for path)
  char start_date_buf[64] = "";
  char end_date_buf[64] = "";
  char archive_dir_buf[512] = "";
  char database_dir_buf[512] = "";
  char feature_dir_buf[512] = "";
  char factor_dir_buf[512] = "";
  char log_dir_buf[512] = "";
  char config_dir_buf[512] = "";
  char csv_market_data_buf[128] = "";
  char csv_tick_trade_buf[128] = "";
  char csv_tick_order_buf[128] = "";
  char archive_extension_buf[32] = "";
  char archive_tool_buf[64] = "";
  char archive_extract_cmd_buf[32] = "";
  char binary_extension_buf[32] = "";
  char assets_file_buf[128] = "";
  char baostock_stock_factor_file_buf[128] = "";
  char baostock_stock_info_file_buf[128] = "";
  char baostock_stock_days_file_buf[128] = "";
  int baostock_max_workers_buf = 4;
  int baostock_weekly_update_day_buf = 1;

  // Auto-sync state
  bool dirty = false;
  std::chrono::steady_clock::time_point last_modified;
  std::filesystem::file_time_type last_file_time;

  // Log callback
  std::function<void(const std::string &)> log_callback;
  
  // Reinit callback (triggered after config save)
  std::function<void()> reinit_callback;

  // Initialize config (load or create default)
  void Initialize();

  // Mark config as modified (will auto-save after debounce)
  void MarkDirty();

  // Sync string buffers from config
  void SyncStringBuffers();

  // Auto-sync: check file changes and debounced save
  void AutoSync();

private:
  // Load config from JSON file
  bool LoadFromFile();

  // Save config to JSON file
  bool SaveToFile();
};