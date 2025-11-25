#pragma once
#include "gui/task_database/models/L2AssetData.hpp"
#include <boost/asio/awaitable.hpp>
#include <string>
#include <vector>

namespace GUI::Database {

// Simple result structure for scanner
struct ScanResult {
  std::vector<AssetInfo> assets;
  std::vector<std::string> all_dates;
};

class CoroScanner {
public:
  CoroScanner(ScanResult &result);

  // Scan binary database directory or load from targets.json
  void scan_binary_directory(const std::string &db_dir);
  void load_targets_json(const std::string &config_path);

private:
  ScanResult &result_;

  std::string infer_exchange(const std::string &code);
};

} // namespace GUI::Database
