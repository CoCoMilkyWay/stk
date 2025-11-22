#pragma once
#include "gui/task_database/AssetInfo.hpp"
#include <boost/asio/awaitable.hpp>
#include <memory>

namespace asio = boost::asio;

namespace GUI::Database {

class CoroScanner {
public:
  CoroScanner(DatabaseState &state);
  
  // Scan binary database directory or load from targets.json
  asio::awaitable<void> scan_database();
  
private:
  DatabaseState &state_;
  
  void scan_binary_directory(const std::string &db_dir);
  void load_targets_json(const std::string &config_path);
  std::string infer_exchange(const std::string &code);
};

} // namespace GUI::Database

