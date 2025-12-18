// Stock classification service (board type and ST status)
// Header-only utility for classification logic
#pragma once

#include "gui/task_database/models/SharedTypes.hpp"
#include "shared/AssetInfo.hpp"
#include <string>

namespace GUI::Database {

class ClassifierService {
public:
  // ============================================================================
  // Board Classification (based on stock code)
  // ============================================================================

  static BoardType classify_board(const std::string &code) {
    if (code.length() < 3)
      return BoardType::Unknown;

    std::string prefix = code.substr(0, 3);

    // Shanghai Main Board: 600/601/603/605
    if (prefix == "600" || prefix == "601" ||
        prefix == "603" || prefix == "605") {
      return BoardType::SH_Main;
    }

    // Shenzhen Main Board: 000/001/002/003/004
    if (prefix == "000" || prefix == "001" || prefix == "002" ||
        prefix == "003" || prefix == "004") {
      return BoardType::SZ_Main;
    }

    // STAR Board (科创板): 688/689
    if (prefix == "688" || prefix == "689") {
      return BoardType::STAR;
    }

    // ChiNext (创业板): 300/301/302/309
    if (prefix == "300" || prefix == "301" ||
        prefix == "302" || prefix == "309") {
      return BoardType::ChiNext;
    }

    // Beijing Stock Exchange: 87/88/92
    if (code.length() >= 2) {
      std::string prefix2 = code.substr(0, 2);
      if (prefix2 == "87" || prefix2 == "88" || prefix2 == "92") {
        return BoardType::BSE;
      }
    }

    return BoardType::Unknown;
  }

  // ============================================================================
  // ST Status Check
  // ============================================================================

  // Check if a stock is ST from StockInfo
  static bool is_st(const StockInfo &info) {
    return info.isST == "1";
  }

  // Check if a stock is ST from map lookup
  static bool is_st(const std::string &full_code,
                    const StockInfoMap &stock_info_map) {
    auto it = stock_info_map.find(full_code);
    return (it != stock_info_map.end()) && (it->second.isST == "1");
  }

  // ============================================================================
  // Helper: Construct full stock code (exchange.code)
  // ============================================================================

  static std::string make_full_code(const std::string &exchange,
                                    const std::string &code) {
    return exchange + "." + code;
  }
};

} // namespace GUI::Database
