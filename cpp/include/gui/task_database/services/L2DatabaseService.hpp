// L2 Database Service - Manages L2 binary database statistics
#pragma once

#include "shared/Asset.hpp"
#include "shared/SharedData.hpp"
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
  SharedData &data_;

public:
  L2DatabaseService(SharedData &data)
      : data_(data) {}

  // ============================================================================
  // Data Access (Read-Only)
  // ============================================================================
  // Note: No refresh methods needed - asset scanning done once at startup

  const std::vector<AssetItem> &get_assets() const { return data_.asset.items; }
  const std::vector<std::string> &get_all_dates() const { return data_.asset.all_dates; }
  const Asset &get_asset_data() const { return data_.asset; }
};

} // namespace GUI::Database
