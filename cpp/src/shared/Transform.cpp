// Transform Implementation

#include "shared/Transform.hpp"

// ============================================================================
// Generate Data Blocks (based on level and dates)
// ============================================================================

static std::string format_display(int level, const std::string &date) {
  if (level == 0 && date.size() >= 8) {
    // YYYYMMDD -> YY/MM/DD
    char buf[16];
    snprintf(buf, sizeof(buf), "%c%c/%c%c/%c%c", date[2], date[3], date[4],
             date[5], date[6], date[7]);
    return buf;
  } else if (level == 1 && date.size() >= 6) {
    // YYYYMM -> YY/MM
    char buf[16];
    snprintf(buf, sizeof(buf), "%c%c/%c%c", date[2], date[3], date[4], date[5]);
    return buf;
  }
  return date;
}

void Transform::generate_blocks(int level,
                                const std::vector<std::string> &dates) {
  blocks.clear();

  if (dates.empty())
    return;

  if (level == 0) {
    // L0: 按天分块
    blocks.reserve(dates.size());
    for (const auto &date : dates) {
      Block block;
      block.date = date;
      block.display = format_display(0, date);
      block.n_samples = 0; // 加载时填充
      blocks.push_back(block);
    }
  } else if (level == 1) {
    // L1: 按月分块
    std::string current_month;

    for (const auto &date : dates) {
      std::string month = date.substr(0, 6); // YYYYMM

      if (month != current_month) {
        Block block;
        block.date = month;
        block.display = format_display(1, month);
        block.n_samples = 0;
        blocks.push_back(block);
        current_month = month;
      }
    }
  } else {
    // L2: 整个回测区间作为一个块
    Block block;
    block.date = "全区间";
    block.display = "全区间";
    block.n_samples = 0;
    blocks.push_back(block);
  }

  selected_block = 0;
}
