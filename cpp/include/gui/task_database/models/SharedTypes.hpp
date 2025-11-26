// Shared types and enums across database modules
#pragma once

#include <string>

namespace GUI::Database {

// ============================================================================
// Board Classification
// ============================================================================

enum class BoardType {
  All, // 所有板块
  Unknown,
  SH_Main, // 沪市主板: 600/601/603/605
  SZ_Main, // 深市主板: 000/001/002/003/004
  STAR,    // 科创板: 688/689
  ChiNext, // 创业板: 300/301/302/309
  BSE      // 北交所: 87/88/92
};

inline const char *GetBoardName(BoardType type) {
  switch (type) {
  case BoardType::SH_Main:
    return "沪市主板";
  case BoardType::SZ_Main:
    return "深市主板";
  case BoardType::STAR:
    return "科创板";
  case BoardType::ChiNext:
    return "创业板";
  case BoardType::BSE:
    return "北交所";
  default:
    return "Unknown";
  }
}

inline BoardType GetBoardType(const std::string &code) {
  if (code.length() < 3)
    return BoardType::Unknown;

  std::string prefix = code.substr(0, 3);

  // Shanghai Main Board
  if (prefix == "600" || prefix == "601" || prefix == "603" || prefix == "605") {
    return BoardType::SH_Main;
  }

  // Shenzhen Main Board
  if (prefix == "000" || prefix == "001" || prefix == "002" ||
      prefix == "003" || prefix == "004") {
    return BoardType::SZ_Main;
  }

  // STAR Board (科创板)
  if (prefix == "688" || prefix == "689") {
    return BoardType::STAR;
  }

  // ChiNext (创业板)
  if (prefix == "300" || prefix == "301" || prefix == "302" || prefix == "309") {
    return BoardType::ChiNext;
  }

  // Beijing Stock Exchange
  if (code.length() >= 2) {
    std::string prefix2 = code.substr(0, 2);
    if (prefix2 == "87" || prefix2 == "88" || prefix2 == "92") {
      return BoardType::BSE;
    }
  }

  return BoardType::Unknown;
}

// ============================================================================
// Status Enums
// ============================================================================

enum class JsonFileStatus {
  Idle,     // Not yet checked
  Loading,  // Loading from disk
  Ready,    // File exists, loaded, integrity passed, up-to-date
  Outdated, // File OK but needs update
  Error,    // Missing or integrity failed
  Updating  // Update in progress
};

enum class CrawlerStatus {
  Idle,
  Initializing,
  Running,
  Complete,
  Error
};

// Baostock session status (for detailed network activity tracking)
enum class BaostockSessionStatus {
  Idle,        // No active session
  LoggingIn,   // Login in progress
  Active,      // Session active, can query
  LoggingOut   // Logout in progress
};

enum class L2ScanStatus {
  NotScanned,
  Scanning,
  Scanned,
  Error
};

// ============================================================================
// Update Stage (for progress tracking)
// ============================================================================

enum class UpdateStage {
  Idle,
  CheckingIntegrity,
  Networking,          // Network activity (login/logout/querying)
  UpdatingStockDays,
  UpdatingStockFactor,
  UpdatingStockInfoWeekly,
  UpdatingStockInfoDaily,
  Fetching,            // Generic fetching stage
  Saving,              // Saving to JSON
  Complete
};

inline const char *GetStageName(UpdateStage stage) {
  switch (stage) {
  case UpdateStage::Idle: return "Idle";
  case UpdateStage::CheckingIntegrity: return "Checking Integrity";
  case UpdateStage::Networking: return "Networking";
  case UpdateStage::UpdatingStockDays: return "Updating Stock Days";
  case UpdateStage::UpdatingStockFactor: return "Updating Stock Factor";
  case UpdateStage::UpdatingStockInfoWeekly: return "Updating Stock Info (Weekly)";
  case UpdateStage::UpdatingStockInfoDaily: return "Updating Stock Info (Daily)";
  case UpdateStage::Fetching: return "Fetching";
  case UpdateStage::Saving: return "Saving";
  case UpdateStage::Complete: return "Complete";
  default: return "Unknown";
  }
}

// ============================================================================
// Progress Tracking Structures
// ============================================================================

struct CrawlerProgress {
  // Current stage
  UpdateStage current_stage = UpdateStage::Idle;
  
  // Session status (for Baostock)
  BaostockSessionStatus session_status = BaostockSessionStatus::Idle;
  size_t session_query_count = 0; // Total queries in current session
  
  // Worker stats
  size_t active_workers = 0;  // Workers currently making TCP requests
  size_t total_workers = 0;
  
  // Task progress (per stage)
  size_t completed_tasks = 0;
  size_t total_tasks = 0;
  
  // Performance metrics
  double requests_per_second = 0.0;
  double eta_seconds = 0.0;
  double elapsed_seconds = 0.0;
  
  // Success/Error tracking
  size_t success_count = 0;
  size_t error_count = 0;
  double success_rate = 0.0; // 0.0 - 1.0
  
  // Current item being processed (for display)
  std::string current_item;
};

} // namespace GUI::Database
