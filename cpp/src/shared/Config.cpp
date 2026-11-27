#include "shared/Config.hpp"
#include "disk/Disk.hpp"
#include "nlohmann/json.hpp"
#include <cassert>

using json = nlohmann::json;
namespace fs = std::filesystem;

void Config::Initialize() {
  filepath = disk::path(disk::SettingsFile).string();
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
  snprintf(archive_dir_buf, sizeof(archive_dir_buf), "%s", archive_dir.c_str());
  snprintf(csv_market_data_buf, sizeof(csv_market_data_buf), "%s", csv_market_data.c_str());
  snprintf(csv_market_trade_buf, sizeof(csv_market_trade_buf), "%s", csv_market_trade.c_str());
  snprintf(csv_market_order_buf, sizeof(csv_market_order_buf), "%s", csv_market_order.c_str());
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
  if (!disk::exists(disk::SettingsFile)) {
    return false;
  }

  const json j = json::parse(disk::read(disk::SettingsFile));

  // Parse JSON to Config with default values as fallback
  start_date = misc::parse_ymd(j.value("start_date", misc::dash(start_date)));
  assert(start_date && "settings.json: start_date 须为 YYYY-MM-DD");
  archive_dir = j.value("archive_dir", archive_dir);
  csv_market_data = j.value("csv_market_data", csv_market_data);
  csv_market_trade = j.value("csv_market_trade", csv_market_trade);
  csv_market_order = j.value("csv_market_order", csv_market_order);
  sync_fundamental = j.value("sync_fundamental", sync_fundamental);
  sync_bar1m = j.value("sync_bar1m", sync_bar1m);
  // 数值配置: 越界则警告并保留旧值 (不读入越界值), 避免手动编辑文件导致 assert 退出
  auto try_assign = [&](double &dst, double src, bool ok, const char *msg) {
    if (ok) {
      dst = src;
    } else if (log_callback) {
      log_callback(std::string("警告: ") + msg + ", 保留旧值");
    }
  };
  double new_commission = j.value("commission", fee.commission);
  double new_stamp = j.value("stamp", fee.stamp);
  double new_transfer = j.value("transfer", fee.transfer);
  double new_impact = j.value("impact", fee.impact);
  double new_top = j.value("cost_top", cost.top);
  try_assign(fee.commission, new_commission, new_commission >= 0.0 && new_commission < 0.01, "佣金比率越界 (万 1 = 0.0001)");
  try_assign(fee.stamp, new_stamp, new_stamp >= 0.0 && new_stamp < 0.01, "印花税比率越界 (万 5 = 0.0005)");
  try_assign(fee.transfer, new_transfer, new_transfer >= 0.0 && new_transfer < 0.01, "过户费比率越界 (0.001% = 0.00001)");
  try_assign(fee.impact, new_impact, new_impact >= 0.0 && new_impact < 0.05, "固定冲击比率越界 (20bp = 0.0020)");
  try_assign(cost.top, new_top, new_top > 0.0 && new_top <= 0.2, "盘口冲击封顶越界 (1% = 0.01)");
  AmtId amt = kAmtFixed;
  if (amt_of_wan(j.value("cost_amt", kAmtWan[cost.amt]), amt))
    cost.amt = amt;
  else if (log_callback)
    log_callback("警告: 成本金额段不在 LABEL_AMTS 里, 保留旧值");

  return true;
}

bool Config::SaveToFile() {
  json j;

  // Convert Config to JSON
  j["start_date"] = misc::dash(start_date);
  j["archive_dir"] = archive_dir;
  j["csv_market_data"] = csv_market_data;
  j["csv_market_trade"] = csv_market_trade;
  j["csv_market_order"] = csv_market_order;
  j["sync_fundamental"] = sync_fundamental;
  j["sync_bar1m"] = sync_bar1m;
  j["commission"] = fee.commission;
  j["stamp"] = fee.stamp;
  j["transfer"] = fee.transfer;
  j["impact"] = fee.impact;
  j["cost_amt"] = kAmtWan[cost.amt];
  j["cost_top"] = cost.top;
  disk::write(disk::SettingsFile, {}, j.dump(2)); // Pretty print with 2 spaces indent
  if (log_callback) {
    log_callback("Config auto-saved to: " + filepath);
  }

  // Trigger GUI reinitialization after config save
  if (reinit_callback) {
    reinit_callback();
  }

  return true;
}
