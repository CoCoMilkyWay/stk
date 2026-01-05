#pragma once

#include "FeatureStoreConfig.hpp"
#include "ZstdHelper.hpp"
#include "misc/profiler.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <fstream>
#endif

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
  // Read compressed data with optional header validation
  // T_max, F_max, A: max capacity (for buffer size validation)
  // T_actual, F_actual, A_actual: optional output for actual dimensions from header
  void read_compressed_data(const std::string &filepath,
                            size_t T_max, size_t F_max, size_t A,
                            feature_storage_t *buffer_ptr,
                            size_t *T_actual = nullptr,
                            size_t *F_actual = nullptr,
                            size_t *A_actual = nullptr) const {
    Trace;

#ifdef _WIN32
    HANDLE hFile;
    {
      TraceN("OpenFile");
      hFile = CreateFileA(filepath.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL,
                          OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
      assert(hFile != INVALID_HANDLE_VALUE && "File not found");
    }
#else
    // Use standard C++ file I/O for cross-platform compatibility
    std::ifstream file(filepath, std::ios::binary);
    assert(file.is_open() && "File not found");
#endif
    constexpr size_t header_size = 3 * sizeof(size_t);

    size_t T, F, A_file;
    {
      TraceN("ReadHeader");
      size_t header[3];
#ifdef _WIN32
      DWORD bytes_read;
      BOOL result = ReadFile(hFile, header, header_size, &bytes_read, NULL);
      assert(result && bytes_read == header_size);
#else
      file.read(reinterpret_cast<char *>(header), header_size);
      assert(file.gcount() == static_cast<std::streamsize>(header_size));
#endif

      T = header[0];
      F = header[1];
      A_file = header[2];

      // Return actual dimensions if requested
      if (T_actual)
        *T_actual = T;
      if (F_actual)
        *F_actual = F;
      if (A_actual)
        *A_actual = A_file;

      // Validate dimensions don't exceed buffer capacity
      assert(T <= T_max && "T exceeds buffer capacity");
      assert(F <= F_max && "F exceeds buffer capacity");
      assert(A_file == A && "A mismatch");
    }

    size_t compressed_size;
    {
      TraceN("GetFileSize");
#ifdef _WIN32
      LARGE_INTEGER file_size;
      BOOL result = GetFileSizeEx(hFile, &file_size);
      assert(result);
      compressed_size = static_cast<size_t>(file_size.QuadPart) - header_size;
#else
      file.seekg(0, std::ios::end);
      compressed_size = static_cast<size_t>(file.tellg()) - header_size;
      file.seekg(header_size, std::ios::beg);
#endif
    }

    std::vector<uint8_t> compressed;
    {
      TraceN("ReadCompressed");
      compressed.resize(compressed_size);

#ifdef _WIN32
      size_t remaining = compressed_size;
      char *current = reinterpret_cast<char *>(compressed.data());
      constexpr size_t READ_CHUNK = 64 * 1024 * 1024;

      while (remaining > 0) {
        DWORD to_read = (remaining > READ_CHUNK) ? static_cast<DWORD>(READ_CHUNK) : static_cast<DWORD>(remaining);
        DWORD bytes_read;
        BOOL result = ReadFile(hFile, current, to_read, &bytes_read, NULL);
        assert(result && bytes_read == to_read);
        current += bytes_read;
        remaining -= bytes_read;
      }
    }

    CloseHandle(hFile);
#else
      file.read(reinterpret_cast<char *>(compressed.data()), compressed_size);
      assert(file.gcount() == static_cast<std::streamsize>(compressed_size));
    }

    file.close();
#endif
    {
      TraceN("Decompress");
      const size_t decompressed_size = T * F * A * sizeof(feature_storage_t);
      ZstdHelper::decompress(compressed.data(), compressed_size, buffer_ptr, decompressed_size);
    }
  }

public:
  // Depth tensor for OrderFlow visualization
  struct DepthTensor {
    std::string date;
    size_t T = 0; // Actual rows (read from file header)
    size_t F = 0; // Actual feature width (read from file header)
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

    void preallocate(size_t A_) {
      A = A_;
      // Preallocate for max capacity, actual T/F set by load_depth from header
      data.resize(MAX_ROWS_PER_LEVEL[0] * DEPTH_TOTAL_WIDTH * A);
    }
  };

  // Month tensor for Dist analysis (preallocated, zero-copy)
  struct MonthTensor {
    std::vector<std::string> dates;      // [N_days]
    std::vector<size_t> day_offsets;     // [N_days+1] cumulative T
    std::vector<feature_storage_t> data; // [total_T × F_selected × A]
    size_t A = 0;
    size_t level = 0;
    size_t max_days = 0;
    size_t max_features = 0;
    std::vector<size_t> feature_indices;

    // Temp buffers (reused across days)
    std::vector<feature_storage_t> temp_column; // L0: single column [T × 1 × A]
    std::vector<feature_storage_t> temp_day;    // L1/L2: full day [T × F_total × A]

    void preallocate(size_t A_, size_t max_days_, size_t max_features_, size_t level_) {
      A = A_;
      level = level_;
      max_days = max_days_;
      max_features = max_features_;

      const size_t T_per_day = MAX_ROWS_PER_LEVEL[level];
      data.resize(max_days * T_per_day * max_features * A);
      day_offsets.resize(max_days + 1);

      if (level == 0) {
        temp_column.resize(T_per_day * 1 * A);
      } else {
        temp_day.resize(T_per_day * FIELDS_PER_LEVEL[level] * A);
      }
    }

    void reset() {
      dates.clear();
      feature_indices.clear();
      // Note: data/temp buffers not cleared - just reused
    }
  };

  explicit FeatureReader(const std::string &base_dir) : base_dir_(base_dir) {}

  // ========================================================================
  // Single Day Loading (for OrderFlow GUI - loads all features)
  // ========================================================================

  // DayTensor structure (for OrderFlow GUI compatibility)
  struct DayTensor {
    std::string date;
    size_t T[LEVEL_COUNT] = {0}; // Actual rows per level (read from file header)
    size_t F[LEVEL_COUNT] = {0}; // Actual feature width per level (read from file header)
    size_t A = 0;
    std::vector<feature_storage_t> data[LEVEL_COUNT];
    std::vector<feature_storage_t> temp_column; // L0 interleave buffer (reused)

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

    // Preallocate for specific level only (on-demand allocation)
    // Upper layer manages buffer lifetime and reuses across multiple loads
    void preallocate_level(size_t A_, size_t level) {
      A = A_;
      // Preallocate for max capacity, actual T/F set by load_day_level from header
      data[level].resize(MAX_ROWS_PER_LEVEL[level] * FIELDS_PER_LEVEL[level] * A);
      if (level == 0) {
        temp_column.resize(MAX_ROWS_PER_LEVEL[0] * 1 * A); // L0 needs interleave buffer
      }
    }
  };

  // Load single day, specific level, all features (for GUI)
  // Reads actual T/F from file headers, buffer already preallocated
  void load_day_level(const std::string &date, size_t level, DayTensor &out) const {
    Trace;
    assert(date.size() == 8);
    assert(level < LEVEL_COUNT);
    assert(out.A > 0 && "Must preallocate() before load_day_level()");
    out.date = date;

    std::string day_dir = base_dir_ + "/" + date.substr(0, 4) + "/" +
                          date.substr(4, 2) + "/" + date.substr(6, 2);

    const size_t T_max = MAX_ROWS_PER_LEVEL[level];
    const size_t F_max = FIELDS_PER_LEVEL[level];
    const size_t A = out.A;

    if (level == 0) {
      // L0: columnar storage - read each column and interleave
      feature_storage_t *dest = out.data[level].data();
      feature_storage_t *temp = out.temp_column.data();

      size_t T_actual = 0;
      {
        TraceN("ReadL0Columns");
        for (size_t f = 0; f < F_max; ++f) {
          std::string col_path = day_dir + "/features_L0_f" + std::to_string(f) + ".zst";
          assert(std::filesystem::exists(col_path) && "L0 column file missing");

          // Read column into temp buffer and get actual dimensions
          size_t T_col, F_col, A_col;
          read_compressed_data(col_path, T_max, 1, A, temp, &T_col, &F_col, &A_col);

          if (f == 0) {
            T_actual = T_col; // First column sets T
          } else {
            assert(T_col == T_actual && "Column T mismatch");
          }

          // Interleave into destination
          for (size_t t = 0; t < T_actual; ++t) {
            std::memcpy(&dest[(t * F_max + f) * A], &temp[t * A], A * sizeof(feature_storage_t));
          }
        }
      }

      // Update dynamic dimensions
      out.T[level] = T_actual;
      out.F[level] = F_max; // F is total column count
    } else {
      // L1/L2: merged storage - read directly into data buffer
      std::string merged_path = day_dir + "/features_L" + std::to_string(level) + ".zst";
      assert(std::filesystem::exists(merged_path) && "Merged file missing");

      size_t T_actual, F_actual, A_actual;
      read_compressed_data(merged_path, T_max, F_max, A, out.data[level].data(),
                           &T_actual, &F_actual, &A_actual);

      // Update dynamic dimensions
      out.T[level] = T_actual;
      out.F[level] = F_actual;
    }
  }

  // ========================================================================
  // Depth Loading (for OrderFlow GUI)
  // ========================================================================

  void load_depth(const std::string &date, DepthTensor &out) const {
    Trace;
    assert(date.size() == 8);
    assert(out.A > 0 && "Must preallocate() before load_depth()");
    out.date = date;

    std::string path = base_dir_ + "/" + date.substr(0, 4) + "/" +
                       date.substr(4, 2) + "/" + date.substr(6, 2) + "/depth.zst";
    assert(std::filesystem::exists(path) && "Depth file missing");

    // Read data and get actual dimensions from header
    size_t T_actual, F_actual, A_actual;
    read_compressed_data(path, MAX_ROWS_PER_LEVEL[0], DEPTH_TOTAL_WIDTH, out.A,
                         out.data.data(), &T_actual, &F_actual, &A_actual);

    // Update dynamic dimensions (buffer already preallocated, no resize)
    out.T = T_actual;
    out.F = F_actual;
    assert(out.A == A_actual && "Asset count mismatch");
  }

  // ========================================================================
  // Batch Monthly Loading (for Dist analysis)
  // ========================================================================

  void load_month_columns(
      const std::string &year,
      const std::string &month,
      const std::vector<size_t> &feature_indices,
      MonthTensor &out) const {
    Trace;

    assert(out.A > 0 && "Must preallocate() before load_month_columns()");
    assert(feature_indices.size() <= out.max_features && "Feature count exceeds preallocated");

    out.reset();
    out.feature_indices = feature_indices;

    const size_t level = out.level;
    const size_t A = out.A;
    const size_t F_selected = feature_indices.size();
    const size_t T_per_day = MAX_ROWS_PER_LEVEL[level];

    {
      TraceN("ListDates");
      out.dates = list_dates(year, month);
      assert(!out.dates.empty() && "No dates found");
      assert(out.dates.size() <= out.max_days && "Day count exceeds preallocated");
    }

    const size_t num_days = out.dates.size();

    // Build day_offsets: [0, T, 2T, ..., num_days*T]
    for (size_t i = 0; i <= num_days; ++i) {
      out.day_offsets[i] = i * T_per_day;
    }

    if (level == 0) {
      // L0: columnar storage - use temp_column buffer
      feature_storage_t *temp = out.temp_column.data();

      for (size_t day_idx = 0; day_idx < num_days; ++day_idx) {
        TraceN("LoadDay");
        const auto &date = out.dates[day_idx];
        TraceTextS(date.c_str());

        std::string day_dir = base_dir_ + "/" + date.substr(0, 4) + "/" +
                              date.substr(4, 2) + "/" + date.substr(6, 2);
        const size_t t_offset = day_idx * T_per_day;

        {
          TraceN("ReadL0Columns");
          for (size_t f_local = 0; f_local < F_selected; ++f_local) {
            size_t f_global = feature_indices[f_local];
            std::string col_path = day_dir + "/features_L0_f" + std::to_string(f_global) + ".zst";
            assert(std::filesystem::exists(col_path) && "L0 column file missing");

            // Read into temp buffer
            {
              TraceN("ReadColumn");
              read_compressed_data(col_path, T_per_day, 1, A, temp);
            }

            // Copy to destination
            {
              TraceN("InterleaveColumn");
              for (size_t t = 0; t < T_per_day; ++t) {
                std::memcpy(&out.data[(t_offset + t) * F_selected * A + f_local * A],
                            &temp[t * A],
                            A * sizeof(feature_storage_t));
              }
            }
          }
        }
      }
    } else {
      // L1/L2: merged storage - use temp_day buffer
      const size_t F_total = FIELDS_PER_LEVEL[level];
      feature_storage_t *temp = out.temp_day.data();

      for (size_t day_idx = 0; day_idx < num_days; ++day_idx) {
        TraceN("LoadDay");
        const auto &date = out.dates[day_idx];
        TraceTextS(date.c_str());

        std::string merged_file = base_dir_ + "/" + date.substr(0, 4) + "/" +
                                  date.substr(4, 2) + "/" + date.substr(6, 2) +
                                  "/features_L" + std::to_string(level) + ".zst";
        assert(std::filesystem::exists(merged_file) && "Merged file missing");

        const size_t t_offset = day_idx * T_per_day;

        // Read into temp buffer
        {
          TraceN("ReadMerged");
          read_compressed_data(merged_file, T_per_day, F_total, A, temp);
        }

        // Extract selected features
        {
          TraceN("ExtractFeatures");
          for (size_t t = 0; t < T_per_day; ++t) {
            for (size_t f_local = 0; f_local < F_selected; ++f_local) {
              size_t f_global = feature_indices[f_local];
              size_t f_offset = get_field_offset(level, f_global);
              std::memcpy(&out.data[(t_offset + t) * F_selected * A + f_local * A],
                          &temp[t * F_total * A + f_offset * A],
                          A * sizeof(feature_storage_t));
            }
          }
        }
      }
    }
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
