#pragma once

#include "FeatureStoreConfig.hpp"
#include "ZstdHelper.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>
#include <windows.h>

// ============================================================================
// FEATURE READER - Hybrid Compressed Format
// ============================================================================
// Storage format:
//   L0: features_L0_f{idx}.zst - [Header: T,1,A][Zstd compressed column]
//       (columnar for selective loading in Dist analysis)
//   L1: features_L1.zst - [Header: T,F_L1,A][Zstd compressed merged data]
//   L2: features_L2.zst - [Header: T,F_L2,A][Zstd compressed merged data]
//       (merged for fewer files and faster writes)
//   Depth: depth.zst - [Header: T,F_depth,A][Zstd compressed data]
//
// APIs:
//   1. load_day_level() - for GUI visualization (all features, single day)
//   2. load_depth() - for OrderFlow GUI visualization
//   3. load_month_columns() - for Dist analysis (batch monthly loading, selective features)
// ============================================================================

class FeatureReader {
private:
  // Fast read: always read header to get actual A (files may have different asset counts)
  bool read_compressed_data(const std::string &filepath, size_t T_expected, size_t F_expected,
                            std::vector<feature_storage_t> &out, size_t &A_out) const {
    HANDLE hFile = CreateFileA(filepath.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL,
                               OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    if (hFile == INVALID_HANDLE_VALUE)
      return false;

    constexpr size_t header_size = 3 * sizeof(size_t);

    // Always read header to get actual file dimensions
    size_t header[3];
    DWORD bytes_read;
    BOOL result = ReadFile(hFile, header, header_size, &bytes_read, NULL);
    assert(result && bytes_read == header_size);
    
    const size_t T = header[0];
    const size_t F = header[1];
    const size_t A = header[2];
    
    assert(T == T_expected && F == F_expected);
    A_out = A; // Return actual asset count from file

    // Get compressed data size
    LARGE_INTEGER file_size;
    result = GetFileSizeEx(hFile, &file_size);
    assert(result);
    const size_t compressed_size = static_cast<size_t>(file_size.QuadPart) - header_size;

    // Allocate output buffer
    const size_t decompressed_size = T * F * A * sizeof(feature_storage_t);
    out.resize(T * F * A);

    // Read compressed data
    std::vector<uint8_t> compressed(compressed_size);
    size_t remaining = compressed_size;
    char *current = reinterpret_cast<char *>(compressed.data());
    constexpr size_t READ_CHUNK = 64 * 1024 * 1024;

    while (remaining > 0) {
      DWORD to_read = (remaining > READ_CHUNK) ? static_cast<DWORD>(READ_CHUNK) : static_cast<DWORD>(remaining);
      result = ReadFile(hFile, current, to_read, &bytes_read, NULL);
      assert(result && bytes_read == to_read);
      current += bytes_read;
      remaining -= bytes_read;
    }

    CloseHandle(hFile);

    // Decompress
    ZstdHelper::decompress(compressed.data(), compressed_size, out.data(), decompressed_size);
    return true;
  }

public:
  // Depth tensor for OrderFlow visualization
  struct DepthTensor {
    std::string date;
    size_t T = 0;
    size_t F = 0;
    size_t A = 0;
    std::vector<feature_storage_t> data;

    inline feature_storage_t get(size_t t, size_t f_enum, size_t a) const {
      assert(t < T && a < A);
      size_t f_offset = DEPTH_FIELD_OFFSETS[f_enum];
      assert(f_offset < F);
      return data[(t * F + f_offset) * A + a];
    }

    inline const feature_storage_t *get_all_assets(size_t t, size_t f_enum) const {
      assert(t < T);
      size_t f_offset = DEPTH_FIELD_OFFSETS[f_enum];
      assert(f_offset < F);
      return data.data() + (t * F + f_offset) * A;
    }

    bool is_loaded() const { return A > 0; }
  };

  // Month tensor for Dist analysis (temporary, zero-copy optimized)
  struct MonthTensor {
    std::vector<std::string> dates;      // [N_days]
    std::vector<size_t> day_offsets;     // [N_days+1] cumulative T
    std::vector<feature_storage_t> data; // [total_T × F_selected × A]
    size_t A = 0;
    std::vector<size_t> feature_indices; // which features loaded

    void clear() {
      dates.clear();
      day_offsets.clear();
      data.clear();
      A = 0;
      feature_indices.clear();
    }
  };

  explicit FeatureReader(const std::string &base_dir) : base_dir_(base_dir) {}

  // ========================================================================
  // Single Day Loading (for OrderFlow GUI - loads all features)
  // ========================================================================

  // DayTensor structure (for OrderFlow GUI compatibility)
  struct DayTensor {
    std::string date;
    size_t T[LEVEL_COUNT] = {0};
    size_t F[LEVEL_COUNT] = {0};
    size_t A = 0;
    std::vector<feature_storage_t> data[LEVEL_COUNT];

    template <size_t Level>
    inline feature_storage_t get(size_t t, size_t f_enum, size_t a) const {
      static_assert(Level < LEVEL_COUNT);
      assert(t < T[Level] && a < A);
      size_t f_offset;
      if constexpr (Level == 0) {
        f_offset = L0_FIELD_OFFSETS[f_enum];
      } else if constexpr (Level == 1) {
        f_offset = L1_FIELD_OFFSETS[f_enum];
      } else {
        f_offset = L2_FIELD_OFFSETS[f_enum];
      }
      assert(f_offset < F[Level]);
      return data[Level][(t * F[Level] + f_offset) * A + a];
    }

    template <size_t Level>
    inline const feature_storage_t *get_all_assets(size_t t, size_t f_enum) const {
      static_assert(Level < LEVEL_COUNT);
      assert(t < T[Level]);
      size_t f_offset;
      if constexpr (Level == 0) {
        f_offset = L0_FIELD_OFFSETS[f_enum];
      } else if constexpr (Level == 1) {
        f_offset = L1_FIELD_OFFSETS[f_enum];
      } else {
        f_offset = L2_FIELD_OFFSETS[f_enum];
      }
      assert(f_offset < F[Level]);
      return data[Level].data() + (t * F[Level] + f_offset) * A;
    }

    bool is_loaded() const { return A > 0; }
  };

  // Load single day, specific level, all features (for GUI)
  bool load_day_level(const std::string &date, size_t level, DayTensor &out) const {
    assert(date.size() == 8);
    assert(level < LEVEL_COUNT);
    out.date = date;

    std::string day_dir = base_dir_ + "/" + date.substr(0, 4) + "/" +
                          date.substr(4, 2) + "/" + date.substr(6, 2);

    const size_t F_level = FIELDS_PER_LEVEL[level];

    if (level == 0) {
      std::vector<std::vector<feature_storage_t>> columns(F_level);
      size_t A = 0;

      for (size_t f = 0; f < F_level; ++f) {
        std::string col_path = day_dir + "/features_L0_f" + std::to_string(f) + ".zst";

        if (!std::filesystem::exists(col_path))
          return false;

        size_t A_col;
        if (!read_compressed_data(col_path, MAX_ROWS_PER_LEVEL[0], 1, columns[f], A_col))
          return false;

        if (f == 0) {
          A = A_col;
        } else {
          assert(A == A_col);
        }
      }

      out.T[level] = MAX_ROWS_PER_LEVEL[0];
      out.F[level] = F_level;
      out.A = A;

      // Interleave columns into row-major tensor
      out.data[level].resize(out.T[level] * out.F[level] * out.A);
      for (size_t t = 0; t < out.T[level]; ++t) {
        for (size_t f = 0; f < out.F[level]; ++f) {
          std::memcpy(&out.data[level][(t * out.F[level] + f) * out.A],
                      &columns[f][t * out.A],
                      out.A * sizeof(feature_storage_t));
        }
      }
    } else {
      std::string merged_path = day_dir + "/features_L" + std::to_string(level) + ".zst";

      if (!std::filesystem::exists(merged_path))
        return false;

      size_t A;
      if (!read_compressed_data(merged_path, MAX_ROWS_PER_LEVEL[level], F_level, out.data[level], A))
        return false;

      out.T[level] = MAX_ROWS_PER_LEVEL[level];
      out.F[level] = F_level;
      out.A = A;
    }

    return true;
  }

  // ========================================================================
  // Depth Loading (for OrderFlow GUI)
  // ========================================================================

  bool load_depth(const std::string &date, DepthTensor &out) const {
    assert(date.size() == 8);
    out.date = date;

    std::string path = base_dir_ + "/" + date.substr(0, 4) + "/" +
                       date.substr(4, 2) + "/" + date.substr(6, 2) + "/depth.zst";

    if (!std::filesystem::exists(path))
      return false;

    size_t A;
    if (!read_compressed_data(path, MAX_ROWS_PER_LEVEL[0], DEPTH_TOTAL_WIDTH, out.data, A))
      return false;

    out.T = MAX_ROWS_PER_LEVEL[0];
    out.F = DEPTH_TOTAL_WIDTH;
    out.A = A;
    return true;
  }

  // ========================================================================
  // Batch Monthly Loading (for Dist analysis)
  // ========================================================================

  bool load_month_columns(
      const std::string &year,
      const std::string &month,
      size_t level,
      const std::vector<size_t> &feature_indices,
      MonthTensor &out) const {

    assert(level < LEVEL_COUNT);
    out.clear();

    out.dates = list_dates(year, month);
    if (out.dates.empty())
      return false;

    out.feature_indices = feature_indices;
    const size_t F_selected = feature_indices.size();

    if (level == 0) {
      // L0: use compile-time known dimensions
      constexpr size_t T_per_day = MAX_ROWS_PER_LEVEL[0];
      const size_t num_days = out.dates.size();
      const size_t total_T = num_days * T_per_day;

      // Get A from first file
      size_t A = 0;
      bool first_read = true;

      // Build day_offsets: [0, T, 2T, ..., num_days*T]
      out.day_offsets.resize(num_days + 1);
      for (size_t i = 0; i <= num_days; ++i) {
        out.day_offsets[i] = i * T_per_day;
      }

      // Will allocate out.data after first file read
      for (size_t day_idx = 0; day_idx < num_days; ++day_idx) {
        const auto &date = out.dates[day_idx];
        std::string day_dir = base_dir_ + "/" + date.substr(0, 4) + "/" +
                              date.substr(4, 2) + "/" + date.substr(6, 2);

        const size_t t_offset = day_idx * T_per_day;

        for (size_t f_local = 0; f_local < F_selected; ++f_local) {
          size_t f_global = feature_indices[f_local];
          std::string col_path = day_dir + "/features_L0_f" + std::to_string(f_global) + ".zst";

          if (!std::filesystem::exists(col_path))
            return false;

          std::vector<feature_storage_t> col_data;
          size_t A_file;
          if (!read_compressed_data(col_path, T_per_day, 1, col_data, A_file))
            return false;

          if (first_read) {
            A = A_file;
            out.A = A;
            out.data.resize(total_T * F_selected * A);
            first_read = false;
          } else {
            assert(A_file == A);
          }

          for (size_t t = 0; t < T_per_day; ++t) {
            std::memcpy(&out.data[(t_offset + t) * F_selected * A + f_local * A],
                        &col_data[t * A],
                        A * sizeof(feature_storage_t));
          }
        }
      }
    } else {
      // L1/L2: use compile-time known dimensions
      const size_t T_per_day = MAX_ROWS_PER_LEVEL[level];
      const size_t F_total = FIELDS_PER_LEVEL[level];
      const size_t num_days = out.dates.size();
      const size_t total_T = num_days * T_per_day;

      // Get A from first file
      size_t A = 0;
      bool first_read = true;

      // Build day_offsets: [0, T, 2T, ..., num_days*T]
      out.day_offsets.resize(num_days + 1);
      for (size_t i = 0; i <= num_days; ++i) {
        out.day_offsets[i] = i * T_per_day;
      }

      // Will allocate out.data after first file read
      for (size_t day_idx = 0; day_idx < num_days; ++day_idx) {
        const auto &date = out.dates[day_idx];
        std::string merged_file = base_dir_ + "/" + date.substr(0, 4) + "/" +
                                  date.substr(4, 2) + "/" + date.substr(6, 2) +
                                  "/features_L" + std::to_string(level) + ".zst";

        const size_t t_offset = day_idx * T_per_day;

        std::vector<feature_storage_t> day_data;
        size_t A_file;
        if (!read_compressed_data(merged_file, T_per_day, F_total, day_data, A_file))
          return false;

        if (first_read) {
          A = A_file;
          out.A = A;
          out.data.resize(total_T * F_selected * A);
          first_read = false;
        } else {
          assert(A_file == A);
        }

        // Extract selected features
        for (size_t t = 0; t < T_per_day; ++t) {
          for (size_t f_local = 0; f_local < F_selected; ++f_local) {
            size_t f_global = feature_indices[f_local];
            std::memcpy(&out.data[(t_offset + t) * F_selected * A + f_local * A],
                        &day_data[t * F_total * A + f_global * A],
                        A * sizeof(feature_storage_t));
          }
        }
      }
    }

    return true;
  }

  // ========================================================================
  // Utility Functions
  // ========================================================================

  bool has_date(const std::string &date) const {
    assert(date.size() == 8);
    std::string path = base_dir_ + "/" + date.substr(0, 4) + "/" +
                       date.substr(4, 2) + "/" + date.substr(6, 2) +
                       "/features_L0_f0.zst";
    return std::filesystem::exists(path);
  }

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

  std::string base_dir_;
};
