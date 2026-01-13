// Transform Implementation

#include "shared/Transform.hpp"

// ============================================================================
// Generate Data Blocks (based on level and dates)
// ============================================================================

static std::string format_label(int level, const std::string &key) {
  if (level == 0 && key.size() >= 8) {
    // YYYYMMDD -> YY/MM/DD
    char buf[16];
    snprintf(buf, sizeof(buf), "%c%c/%c%c/%c%c", key[2], key[3], key[4],
             key[5], key[6], key[7]);
    return buf;
  } else if (level == 1 && key.size() >= 6) {
    // YYYYMM -> YY/MM
    char buf[16];
    snprintf(buf, sizeof(buf), "%c%c/%c%c", key[2], key[3], key[4], key[5]);
    return buf;
  }
  return key;
}

void Transform::generate_blocks(int level,
                                const std::vector<std::string> &dates) {
  blocks.clear();

  if (dates.empty())
    return;

  if (level == 0) {
    // L0: 按天分块 (每块1天)
    blocks.reserve(dates.size());
    for (const auto &date : dates) {
      Block block;
      block.label = format_label(0, date);
      block.dates = {date};
      block.n_samples = 0;
      blocks.push_back(block);
    }
  } else if (level == 1) {
    // L1: 按月分块 (每块包含该月所有天)
    std::string current_month;

    for (const auto &date : dates) {
      std::string month = date.substr(0, 6); // YYYYMM

      if (month != current_month) {
        Block block;
        block.label = format_label(1, month);
        block.n_samples = 0;
        blocks.push_back(block);
        current_month = month;
      }
      blocks.back().dates.push_back(date);
    }
  } else {
    // L2: 整个区间作为一个块 (包含所有天)
    Block block;
    block.label = "全区间";
    block.dates = dates;
    block.n_samples = 0;
    blocks.push_back(block);
  }

  selected_block = 0;
}
