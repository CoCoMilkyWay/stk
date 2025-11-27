// Asset Loader - Unified asset list initialization
// Loads from config/assets.json as single source of truth
#pragma once

#include <string>

// Forward declaration
struct SharedData;

namespace GUI::Database {

class AssetLoader {
public:
  // Load assets from assets.json into SharedData (path from config)
  static void load_from_config(SharedData &data);

private:
  // Infer exchange from asset code
  static std::string infer_exchange(const std::string &code);
};

} // namespace GUI::Database

