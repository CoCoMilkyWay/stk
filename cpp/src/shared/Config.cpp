#include "shared/Config.hpp"
#include "nlohmann/json.hpp"
#include "shared/Universe.hpp"
#include <cassert>
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
  snprintf(start_date_buf, sizeof(start_date_buf), "%s", misc::dash(start_date).c_str());
  snprintf(end_date_buf, sizeof(end_date_buf), "%s", misc::dash(end_date).c_str());
  snprintf(universe_buf, sizeof(universe_buf), "%s", universe.c_str());
  snprintf(archive_dir_buf, sizeof(archive_dir_buf), "%s", archive_dir.c_str());
  snprintf(orders_dir_buf, sizeof(orders_dir_buf), "%s", orders_dir.c_str());
  snprintf(feature_dir_buf, sizeof(feature_dir_buf), "%s", feature_dir.c_str());
  snprintf(factor_dir_buf, sizeof(factor_dir_buf), "%s", factor_dir.c_str());
  snprintf(log_dir_buf, sizeof(log_dir_buf), "%s", log_dir.c_str());
  snprintf(config_dir_buf, sizeof(config_dir_buf), "%s", config_dir.c_str());
  snprintf(csv_market_data_buf, sizeof(csv_market_data_buf), "%s", csv_market_data.c_str());
  snprintf(csv_tick_trade_buf, sizeof(csv_tick_trade_buf), "%s", csv_market_trade.c_str());
  snprintf(csv_tick_order_buf, sizeof(csv_tick_order_buf), "%s", csv_market_order.c_str());
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

std::string Config::UniversePath() const {
  assert(universe != kAllTierDir && "universe 名 all 已归 Bar 档, Lob 档无此名单");
  return (fs::path(config_dir) / "universe" / (universe + ".json")).string();
}

std::vector<std::string> Config::UniverseCodes() const {
  // 并集只看张量覆盖的回测区间 [start_date, end_date]
  return universe::Pool::load(UniversePath()).codes(start_date, end_date);
}

bool Config::LoadFromFile() {
  std::ifstream file(filepath);
  if (!file.is_open()) {
    return false;
  }

  json j;
  file >> j;

  // Parse JSON to Config with default values as fallback
  start_date = misc::parse_ymd(j.value("start_date", misc::dash(start_date)));
  end_date = misc::parse_ymd(j.value("end_date", misc::dash(end_date)));
  assert(start_date && end_date && "config.json: start_date / end_date 须为 YYYY-MM-DD");
  universe = j.value("universe", universe);
  archive_dir = j.value("archive_dir", archive_dir);
  orders_dir = j.value("orders_dir", orders_dir);
  feature_dir = j.value("feature_dir", feature_dir);
  factor_dir = j.value("factor_dir", factor_dir);
  log_dir = j.value("log_dir", log_dir);
  config_dir = j.value("config_dir", config_dir);
  csv_market_data = j.value("csv_market_data", csv_market_data);
  csv_market_trade = j.value("csv_market_trade", csv_market_trade);
  csv_market_order = j.value("csv_market_order", csv_market_order);
  network_sync = j.value("network_sync", network_sync);
  // 数值配置: 越界则警告并保留旧值 (不读入越界值), 避免手动编辑文件导致 assert 退出
  auto try_assign = [&](double &dst, double src, bool ok, const char *msg) {
    if (ok) {
      dst = src;
    } else if (log_callback) {
      log_callback(std::string("警告: ") + msg + ", 保留旧值");
    }
  };
  double new_commission = j.value("commission", commission);
  double new_stamp = j.value("stamp", stamp);
  double new_sell_impact = j.value("sell_impact", sell_impact);
  double new_max_impact = j.value("max_impact", max_impact);
  try_assign(commission, new_commission,
             new_commission >= 0.0 && new_commission < 0.01, "佣金比率越界 (万 1 = 0.0001)");
  try_assign(stamp, new_stamp,
             new_stamp >= 0.0 && new_stamp < 0.01, "印花税比率越界 (万 5 = 0.0005)");
  try_assign(sell_impact, new_sell_impact,
             new_sell_impact >= 0.0 && new_sell_impact < 0.05, "平仓冲击比率越界 (20bp = 0.0020)");
  try_assign(max_impact, new_max_impact,
             new_max_impact > 0.0 && new_max_impact <= 0.2, "买入冲击封顶越界 (1% = 0.01)");

  return true;
}

bool Config::SaveToFile() {
  json j;

  // Convert Config to JSON
  j["start_date"] = misc::dash(start_date);
  j["end_date"] = misc::dash(end_date);
  j["universe"] = universe;
  j["archive_dir"] = archive_dir;
  j["orders_dir"] = orders_dir;
  j["feature_dir"] = feature_dir;
  j["factor_dir"] = factor_dir;
  j["log_dir"] = log_dir;
  j["config_dir"] = config_dir;
  j["csv_market_data"] = csv_market_data;
  j["csv_market_trade"] = csv_market_trade;
  j["csv_market_order"] = csv_market_order;
  j["network_sync"] = network_sync;
  j["commission"] = commission;
  j["stamp"] = stamp;
  j["sell_impact"] = sell_impact;
  j["max_impact"] = max_impact;

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
