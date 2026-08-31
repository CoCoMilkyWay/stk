#include "shared/Config.hpp"
#include "nlohmann/json.hpp"
#include <fstream>

using json = nlohmann::json;
namespace fs = std::filesystem;

void Config::Initialize() {
  // Try to load existing file
  if (fs::exists(filepath)) {
    if (LoadFromFile()) {
      SyncStringBuffers();
      // Track file modification time
      last_file_time = fs::last_write_time(filepath);
      if (log_callback) {
        log_callback("Config loaded from: " + filepath);
      }
      return;
    }
  }

  // File doesn't exist, create with default values
  if (log_callback) {
    log_callback("Config file not found, creating with default values: " + filepath);
  }
  SaveToFile();
  SyncStringBuffers();
  if (fs::exists(filepath)) {
    last_file_time = fs::last_write_time(filepath);
  }
}

void Config::MarkDirty() {
  dirty = true;
  last_modified = std::chrono::steady_clock::now();
}

void Config::SyncStringBuffers() {
  snprintf(start_date_buf, sizeof(start_date_buf), "%s", start_date.c_str());
  snprintf(end_date_buf, sizeof(end_date_buf), "%s", end_date.c_str());
  snprintf(archive_dir_buf, sizeof(archive_dir_buf), "%s", archive_dir.c_str());
  snprintf(database_dir_buf, sizeof(database_dir_buf), "%s", database_dir.c_str());
  snprintf(feature_dir_buf, sizeof(feature_dir_buf), "%s", feature_dir.c_str());
  snprintf(factor_dir_buf, sizeof(factor_dir_buf), "%s", factor_dir.c_str());
  snprintf(log_dir_buf, sizeof(log_dir_buf), "%s", log_dir.c_str());
  snprintf(config_dir_buf, sizeof(config_dir_buf), "%s", config_dir.c_str());
  snprintf(csv_market_data_buf, sizeof(csv_market_data_buf), "%s", csv_market_data.c_str());
  snprintf(csv_tick_trade_buf, sizeof(csv_tick_trade_buf), "%s", csv_market_trade.c_str());
  snprintf(csv_tick_order_buf, sizeof(csv_tick_order_buf), "%s", csv_market_order.c_str());
  snprintf(archive_extension_buf, sizeof(archive_extension_buf), "%s", archive_extension.c_str());
  snprintf(archive_tool_buf, sizeof(archive_tool_buf), "%s", archive_tool.c_str());
  snprintf(archive_extract_cmd_buf, sizeof(archive_extract_cmd_buf), "%s", archive_extract_cmd.c_str());
  snprintf(binary_extension_buf, sizeof(binary_extension_buf), "%s", binary_extension.c_str());
}

void Config::AutoSync() {
  // Check if file was modified externally
  if (fs::exists(filepath)) {
    auto current_file_time = fs::last_write_time(filepath);
    if (current_file_time != last_file_time) {
      last_file_time = current_file_time;
      if (log_callback) {
        log_callback("Config file changed externally, reloading...");
      }
      LoadFromFile();
      SyncStringBuffers();
      dirty = false; // Reset dirty flag since we just loaded
      return;
    }
  }

  // Debounced auto-save (200ms after last modification)
  if (dirty) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_modified);
    if (elapsed.count() >= 200) {
      SaveToFile();
      dirty = false;
      if (fs::exists(filepath)) {
        last_file_time = fs::last_write_time(filepath);
      }
    }
  }
}

bool Config::LoadFromFile() {
  std::ifstream file(filepath);
  if (!file.is_open()) {
    return false;
  }

  json j;
  file >> j;

  // Parse JSON to Config with default values as fallback
  start_date = j.value("start_date", start_date);
  end_date = j.value("end_date", end_date);
  archive_dir = j.value("archive_dir", archive_dir);
  database_dir = j.value("database_dir", database_dir);
  feature_dir = j.value("feature_dir", feature_dir);
  factor_dir = j.value("factor_dir", factor_dir);
  log_dir = j.value("log_dir", log_dir);
  config_dir = j.value("config_dir", config_dir);
  csv_market_data = j.value("csv_market_data", csv_market_data);
  csv_market_trade = j.value("csv_market_trade", csv_market_trade);
  csv_market_order = j.value("csv_market_order", csv_market_order);
  archive_extension = j.value("archive_extension", archive_extension);
  archive_tool = j.value("archive_tool", archive_tool);
  archive_extract_cmd = j.value("archive_extract_cmd", archive_extract_cmd);
  binary_extension = j.value("binary_extension", binary_extension);

  return true;
}

bool Config::SaveToFile() {
  json j;

  // Convert Config to JSON
  j["start_date"] = start_date;
  j["end_date"] = end_date;
  j["archive_dir"] = archive_dir;
  j["database_dir"] = database_dir;
  j["feature_dir"] = feature_dir;
  j["factor_dir"] = factor_dir;
  j["log_dir"] = log_dir;
  j["config_dir"] = config_dir;
  j["csv_market_data"] = csv_market_data;
  j["csv_market_trade"] = csv_market_trade;
  j["csv_market_order"] = csv_market_order;
  j["archive_extension"] = archive_extension;
  j["archive_tool"] = archive_tool;
  j["archive_extract_cmd"] = archive_extract_cmd;
  j["binary_extension"] = binary_extension;

  std::ofstream file(filepath);
  if (!file.is_open()) {
    if (log_callback) {
      log_callback("Failed to save config file: " + filepath);
    }
    return false;
  }

  file << j.dump(2); // Pretty print with 2 spaces indent
  if (log_callback) {
    log_callback("Config auto-saved to: " + filepath);
  }

  // Trigger GUI reinitialization after config save
  if (reinit_callback) {
    reinit_callback();
  }

  return true;
}
