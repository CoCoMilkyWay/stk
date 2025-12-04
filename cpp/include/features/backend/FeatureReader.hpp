#pragma once

#include "FeatureStoreConfig.hpp"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

// ============================================================================
// FEATURE READER - Read tensor binary files for GUI visualization
// ============================================================================
// File format (per level):
//   Header: [T: size_t] [F: size_t] [A: size_t]
//   Data:   [T × F × A] row-major (_Float16)
//   Access: data[(t * F + f) * A + a]
// ============================================================================

class FeatureReader {
public:
  // Tensor data for a single day
  struct DayTensor {
    std::string date;                      // "YYYYMMDD"
    size_t T[LEVEL_COUNT] = {0};           // Time dimension per level
    size_t F[LEVEL_COUNT] = {0};           // Feature dimension per level
    size_t A = 0;                          // Asset dimension
    std::vector<feature_storage_t> data[LEVEL_COUNT]; // Flat [T][F][A] data

    // Template access by field enum (FOOL-PROOF + ZERO-COST: compile-time optimization)
    // Level: compile-time constant (0, 1, 2)
    // f_enum: field enum index (e.g., L0_FieldOffset::_mid_price)
    // Get single value by enum (auto-converts to offset)
    template<size_t Level>
    inline feature_storage_t get(size_t t, size_t f_enum, size_t a) const {
      static_assert(Level < LEVEL_COUNT, "Invalid level");
      assert(t < T[Level] && a < A);
      
      // Compile-time selection of offset array (zero runtime cost with if constexpr)
      size_t f_offset;
      if constexpr (Level == 0) {
        f_offset = L0_FIELD_OFFSETS[f_enum];
      } else if constexpr (Level == 1) {
        f_offset = L1_FIELD_OFFSETS[f_enum];
      } else {
        f_offset = L2_FIELD_OFFSETS[f_enum];
      }
      
      assert(f_offset < F[Level] && "Field offset out of bounds");
      return data[Level][(t * F[Level] + f_offset) * A + a];
    }


    // Template get pointer to all assets (ZERO-COST: compile-time optimization)
    template<size_t Level>
    inline const feature_storage_t *get_all_assets(size_t t, size_t f_enum) const {
      static_assert(Level < LEVEL_COUNT, "Invalid level");
      assert(t < T[Level]);
      
      // Compile-time selection of offset array (zero runtime cost with if constexpr)
      size_t f_offset;
      if constexpr (Level == 0) {
        f_offset = L0_FIELD_OFFSETS[f_enum];
      } else if constexpr (Level == 1) {
        f_offset = L1_FIELD_OFFSETS[f_enum];
      } else {
        f_offset = L2_FIELD_OFFSETS[f_enum];
      }
      
      assert(f_offset < F[Level] && "Field offset out of bounds");
      return data[Level].data() + (t * F[Level] + f_offset) * A;
    }

    bool is_loaded() const { return A > 0; }
  };

  // Multi-day tensor cache
  struct MultiDayCache {
    std::vector<DayTensor> days;
    std::string anchor_date;
    size_t span_days = 0;

    // Get total time indices across all days for a level
    size_t total_T(size_t level) const {
      size_t total = 0;
      for (const auto &day : days)
        total += day.T[level];
      return total;
    }

    // Find day and local time index from global time index
    std::pair<size_t, size_t> locate(size_t level, size_t global_t) const {
      size_t acc = 0;
      for (size_t d = 0; d < days.size(); ++d) {
        if (global_t < acc + days[d].T[level]) {
          return {d, global_t - acc};
        }
        acc += days[d].T[level];
      }
      assert(false && "global_t out of range");
      return {0, 0};
    }

    bool is_loaded() const { return !days.empty() && days[0].is_loaded(); }
  };

  explicit FeatureReader(const std::string &base_dir) : base_dir_(base_dir) {}

  // Load tensor data for a single date (all levels)
  bool load_day(const std::string &date, DayTensor &out) const {
    assert(date.size() == 8);
    out.date = date;

    std::string year = date.substr(0, 4);
    std::string month = date.substr(4, 2);
    std::string day = date.substr(6, 2);
    std::string dir = base_dir_ + "/" + year + "/" + month + "/" + day;

    // Load each level
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      if (!load_level(dir, lvl, out))
        return false;
    }

    return true;
  }

  // Load tensor data for a single date, specific level only
  bool load_day_level(const std::string &date, size_t level, DayTensor &out) const {
    assert(date.size() == 8);
    assert(level < LEVEL_COUNT);
    out.date = date;

    std::string year = date.substr(0, 4);
    std::string month = date.substr(4, 2);
    std::string day = date.substr(6, 2);
    std::string dir = base_dir_ + "/" + year + "/" + month + "/" + day;

    return load_level(dir, level, out);
  }

private:
  // Load a single level from directory
  bool load_level(const std::string &dir, size_t lvl, DayTensor &out) const {
    std::string path = dir + "/features_L" + std::to_string(lvl) + ".bin";

    if (!std::filesystem::exists(path)) {
      return false;
    }

    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) {
      return false;
    }

    // Read header
    ifs.read(reinterpret_cast<char *>(&out.T[lvl]), sizeof(size_t));
    ifs.read(reinterpret_cast<char *>(&out.F[lvl]), sizeof(size_t));
    ifs.read(reinterpret_cast<char *>(&out.A), sizeof(size_t));

    // Validate dimensions
    assert(out.T[lvl] > 0 && out.T[lvl] <= MAX_ROWS_PER_LEVEL[lvl]);
    assert(out.F[lvl] > 0 && out.F[lvl] <= FIELDS_PER_LEVEL[lvl]);
    assert(out.A > 0);

    // Read data
    size_t total_elements = out.T[lvl] * out.F[lvl] * out.A;
    out.data[lvl].resize(total_elements);
    ifs.read(reinterpret_cast<char *>(out.data[lvl].data()),
             total_elements * sizeof(feature_storage_t));

    return true;
  }

public:

  // Load tensor data for anchor_date + span_days forward
  // Returns: [anchor_date, anchor_date + span_days - 1]
  bool load_days(const std::string &anchor_date, size_t span_days, MultiDayCache &out) const {
    out.anchor_date = anchor_date;
    out.span_days = span_days;
    out.days.clear();
    out.days.resize(span_days);

    // Generate date list (forward from anchor)
    std::vector<std::string> dates = generate_date_range(anchor_date, span_days);

    // Load each day
    for (size_t i = 0; i < dates.size(); ++i) {
      if (!load_day(dates[i], out.days[i])) {
        // Day not available, clear cache
        out.days.clear();
        return false;
      }
    }

    return true;
  }

  // Check if data exists for a date
  bool has_date(const std::string &date) const {
    assert(date.size() == 8);
    std::string year = date.substr(0, 4);
    std::string month = date.substr(4, 2);
    std::string day = date.substr(6, 2);
    std::string path = base_dir_ + "/" + year + "/" + month + "/" + day + "/features_L0.bin";
    return std::filesystem::exists(path);
  }

  // List available dates in a year-month
  std::vector<std::string> list_dates(const std::string &year, const std::string &month) const {
    std::vector<std::string> dates;
    std::string dir = base_dir_ + "/" + year + "/" + month;

    if (!std::filesystem::exists(dir))
      return dates;

    for (const auto &entry : std::filesystem::directory_iterator(dir)) {
      if (entry.is_directory()) {
        std::string day = entry.path().filename().string();
        if (day.size() == 2) {
          std::string date = year + month + day;
          if (has_date(date)) {
            dates.push_back(date);
          }
        }
      }
    }

    std::sort(dates.begin(), dates.end());
    return dates;
  }

private:
  std::string base_dir_;

  // Generate date range [start, start + days - 1]
  // Simple implementation: just increment day, handle month/year overflow
  static std::vector<std::string> generate_date_range(const std::string &start, size_t days) {
    std::vector<std::string> result;
    result.reserve(days);

    int year = std::stoi(start.substr(0, 4));
    int month = std::stoi(start.substr(4, 2));
    int day = std::stoi(start.substr(6, 2));

    for (size_t i = 0; i < days; ++i) {
      char buf[16];
      std::snprintf(buf, sizeof(buf), "%04d%02d%02d", year, month, day);
      result.push_back(buf);

      // Increment day
      ++day;

      // Days per month (simplified, no leap year check for trading days)
      static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
      int max_day = days_in_month[month];
      if (month == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
        max_day = 29; // Leap year
      }

      if (day > max_day) {
        day = 1;
        ++month;
        if (month > 12) {
          month = 1;
          ++year;
        }
      }
    }

    return result;
  }
};

// ============================================================================
// MULTI-WIDTH FIELD ACCESS MACROS - Only use enums, 100% static compilation
// ============================================================================
// TENSOR_GET(tensor, lvl, t, field_enum, a)           - Single-width field
// TENSOR_GET_MULTI(tensor, lvl, t, field_enum, i, a)  - Multi-width field[i]
// ============================================================================

#define TENSOR_GET(tensor, lvl, t, field_enum, a) \
  ((tensor).data[lvl][((t) * (tensor).F[lvl] + L##lvl##_FIELD_OFFSETS[field_enum]) * (tensor).A + (a)])

#define TENSOR_GET_MULTI(tensor, lvl, t, field_enum, i, a) \
  ((tensor).data[lvl][((t) * (tensor).F[lvl] + L##lvl##_FIELD_OFFSETS[field_enum] + (i)) * (tensor).A + (a)])

