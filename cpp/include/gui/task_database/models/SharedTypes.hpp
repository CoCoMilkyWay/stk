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
  BSE      // 北交所: 43/83/87/88/92
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
  // 43/83 = 新三板平移的存量代码段 (未换号直接上北交所)
  if (code.length() >= 2) {
    std::string prefix2 = code.substr(0, 2);
    if (prefix2 == "43" || prefix2 == "83" || prefix2 == "87" ||
        prefix2 == "88" || prefix2 == "92") {
      return BoardType::BSE;
    }
  }

  return BoardType::Unknown;
}

} // namespace GUI::Database
