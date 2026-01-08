// Transform Implementation

#include "shared/Transform.hpp"
#include <algorithm>
#include <cmath>

// ============================================================================
// Generate Data Blocks (based on level and dates)
// ============================================================================

void Transform::generate_blocks(int level, const std::vector<std::string> &dates) {
  blocks.clear();

  if (dates.empty())
    return;

  if (level == 0) {
    // L0: 按天分块
    for (size_t i = 0; i < dates.size(); ++i) {
      DataBlock block;
      block.label = dates[i];
      block.start_idx = i;
      block.length = 1;
      block.valid = true;
      blocks.push_back(block);
    }
  } else if (level == 1) {
    // L1: 按月分块
    std::string current_month;
    DataBlock current_block;

    for (size_t i = 0; i < dates.size(); ++i) {
      std::string month = dates[i].substr(0, 6); // YYYYMM

      if (month != current_month) {
        if (!current_month.empty()) {
          blocks.push_back(current_block);
        }
        current_month = month;
        current_block = DataBlock{};
        current_block.label = month;
        current_block.start_idx = i;
        current_block.length = 0;
        current_block.valid = true;
      }
      ++current_block.length;
    }

    if (!current_month.empty()) {
      blocks.push_back(current_block);
    }
  } else {
    // L2: 整个回测区间作为一个块
    DataBlock block;
    block.label = "全区间";
    block.start_idx = 0;
    block.length = dates.size();
    block.valid = true;
    blocks.push_back(block);
  }

  selected_block = 0;
}

// ============================================================================
// Update Cross-Section (given time index)
// ============================================================================

void Transform::update_cross_section(size_t time_idx) {
  cross_section.clear();
  cross_section.time_idx = time_idx;

  if (results.empty())
    return;

  // 收集所有资产在该时间点的值
  std::vector<float> values;
  values.reserve(results.size());

  for (const auto &r : results) {
    if (r.valid && time_idx < r.normalized.size()) {
      float v = r.normalized[time_idx];
      if (std::isfinite(v)) {
        values.push_back(v);
      }
    }
  }

  if (values.empty())
    return;

  cross_section.values = values;

  // 计算统计量
  size_t n = values.size();

  // Mean
  double sum = 0.0;
  for (float v : values)
    sum += v;
  cross_section.mean = static_cast<float>(sum / n);

  // Std
  double var_sum = 0.0;
  for (float v : values) {
    double d = v - cross_section.mean;
    var_sum += d * d;
  }
  cross_section.std = static_cast<float>(std::sqrt(var_sum / n));

  // Min/Max
  auto [min_it, max_it] = std::minmax_element(values.begin(), values.end());
  cross_section.min = *min_it;
  cross_section.max = *max_it;

  // Skewness & Kurtosis
  if (cross_section.std > 1e-10f) {
    double m3 = 0.0, m4 = 0.0;
    for (float v : values) {
      double z = (v - cross_section.mean) / cross_section.std;
      m3 += z * z * z;
      m4 += z * z * z * z;
    }
    cross_section.skew = static_cast<float>(m3 / n);
    cross_section.kurt = static_cast<float>(m4 / n - 3.0); // Excess kurtosis
  }

  // 直方图
  constexpr size_t N_BINS = CrossSectionSlice::N_BINS;
  cross_section.hist_x.resize(N_BINS);
  cross_section.hist_y.resize(N_BINS, 0.0f);

  float range = cross_section.max - cross_section.min;
  if (range < 1e-10f)
    range = 1.0f;

  float bin_width = range / N_BINS;
  for (size_t i = 0; i < N_BINS; ++i) {
    cross_section.hist_x[i] = cross_section.min + (i + 0.5f) * bin_width;
  }

  for (float v : values) {
    size_t bin = static_cast<size_t>((v - cross_section.min) / bin_width);
    if (bin >= N_BINS)
      bin = N_BINS - 1;
    cross_section.hist_y[bin] += 1.0f;
  }

  // 归一化为频率
  float inv_n = 1.0f / n;
  for (auto &y : cross_section.hist_y) {
    y *= inv_n;
  }

  cross_section.valid = true;
}
