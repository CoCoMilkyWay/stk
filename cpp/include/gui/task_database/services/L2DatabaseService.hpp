// L2 Database Service - Encapsulates Scanner with clean interface
// Manages L2 binary database scanning and statistics
#pragma once

#include "gui/task_database/models/L2AssetData.hpp"
#include "gui/task_database/models/SharedTypes.hpp"
#include <boost/asio/awaitable.hpp>
#include <string>
#include <vector>

namespace GUI::Database {

using boost::asio::awaitable;

// ============================================================================
// L2 Database Service
// ============================================================================

class L2DatabaseService {
private:
  std::vector<AssetInfo> assets_;
  std::vector<std::string> all_dates_;
  std::string database_dir_;

  L2ScanStatus scan_status_ = L2ScanStatus::NotScanned;
  std::string error_message_;

public:
  L2DatabaseService(const std::string &db_dir)
      : database_dir_(db_dir) {}

  // ============================================================================
  // Lifecycle
  // ============================================================================

  awaitable<void> scan_database();
  awaitable<void> refresh_asset(size_t asset_idx);

  // ============================================================================
  // Data Access (Read-Only)
  // ============================================================================

  const std::vector<AssetInfo> &get_assets() const { return assets_; }
  const std::vector<std::string> &get_all_dates() const { return all_dates_; }

  // ============================================================================
  // Statistics
  // ============================================================================

  L2Summary get_summary() const;

  // ============================================================================
  // Status Query
  // ============================================================================

  L2ScanStatus get_status() const { return scan_status_; }
  bool is_scanned() const { return scan_status_ == L2ScanStatus::Scanned; }
  bool is_scanning() const { return scan_status_ == L2ScanStatus::Scanning; }
  const std::string &get_error_message() const { return error_message_; }
};

} // namespace GUI::Database
