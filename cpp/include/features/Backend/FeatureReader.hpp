#pragma once

#include "FeatureStoreConfig.hpp"
#include "ZstdHelper.hpp"
#include "misc/profiler.hpp"
#include "shared/AssetAxis.hpp"
#include <algorithm>
#include <cassert>
#include <cstdint>
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
// Storage format (header = 5 × size_t: T, F, A, axis_hash, table_fingerprint), 每层按 LEVELS[lvl].columnar:
//   逐列 (L0):        features_L0_f{idx}.zst   [Header: T,1,A,h][Zstd column]   (Dist 按列选读)
//   整层 (L1/DEPTH):  features_<LVL>.zst       [Header: T,F,A,h][Zstd merged]
//
// axis_hash = AssetAxis::hash_at(A): 列 → 资产的映射不在文件里, 靠 A 轴顺序.
//   存指纹后读文件时 O(1) 就能确认列序没漂移 (见 shared/AssetAxis.hpp).
// table_fingerprint = 写入时字段表指纹 (LEVELS[lvl].fingerprint):
//   字段表改了旧文件立刻断言失败, 不会静默错位.
//
// APIs:
//   1. load_day_level(date, lvl, DayTensor)  - GUI: 单日整层 (L0/L1/DEPTH 同一套)
//   2. load_month_columns()                  - Dist: 整月, 选列
// ============================================================================

class FeatureReader {
private:
  // Read compressed data with optional header validation
  // T_max, F_max, A: max capacity (for buffer size validation)
  // T_actual, F_actual, A_actual: optional output for actual dimensions from header
  void read_compressed_data(const std::string &filepath,
                            size_t T_max, size_t F_max, size_t A, uint64_t table_fp,
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
    constexpr size_t header_size = FEATURE_FILE_HEADER_WORDS * sizeof(size_t);

    size_t T, F, A_file;
    {
      TraceN("ReadHeader");
      size_t header[FEATURE_FILE_HEADER_WORDS];
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

      // A 轴列序锁定: 文件头存的是写入时 AssetAxis::hash_at(A) — 轴 append-only,
      // 该值只依赖前 A 条, 所以历史文件的指纹永久有效. 一次 O(1) 比对即可确认
      // "列 → 资产的映射没有漂移", 轴被重排/截断或文件被换掉都会立刻炸.
      assert(static_cast<std::uint64_t>(header[3]) == asset_axis().hash_at(A_file) &&
             "A 轴指纹不符: 特征文件与当前 asset_axis.json 列序不一致 (需重算特征)");
      assert(static_cast<std::uint64_t>(header[4]) == table_fp &&
             "字段表指纹不符: 特征文件是旧字段表写的 (需重算特征)");
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
    std::vector<feature_storage_t> temp_day;    // L1: full day [T × F_total × A]

    void preallocate(size_t A_, size_t max_days_, size_t max_features_, size_t level_) {
      A = A_;
      level = level_;
      max_days = max_days_;
      max_features = max_features_;

      const size_t T_per_day = LEVELS[level].rows;
      data.resize(max_days * T_per_day * max_features * A);
      day_offsets.resize(max_days + 1);

      if (LEVELS[level].columnar) {
        temp_column.resize(T_per_day * 1 * A);
      } else {
        temp_day.resize(T_per_day * LEVELS[level].width * A);
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
  // Single Day Loading (GUI: 单日整层, 任一层)
  // ========================================================================

  // 一天若干层的张量; 每层 [T][F][A], T/F 以文件头为准
  struct DayTensor {
    std::string date;
    size_t T[LEVEL_COUNT] = {0}; // Actual rows per level (read from file header)
    size_t F[LEVEL_COUNT] = {0}; // Actual feature width per level (read from file header)
    size_t A = 0;
    std::vector<feature_storage_t> data[LEVEL_COUNT];
    std::vector<feature_storage_t> temp_column; // 逐列层的交织缓冲 (复用)

    // 宽字段 (DEPTH 的 N 档) 用 sub 取档内下标
    template <size_t Level>
    inline feature_storage_t get(size_t t, size_t field, size_t a, size_t sub = 0) const {
      static_assert(Level < LEVEL_COUNT);
      assert(t < T[Level] && a < A && sub < LEVELS[Level].fields[field].width);
      const size_t f_offset = LEVELS[Level].offsets[field] + sub;
      assert(f_offset < F[Level]);
      return data[Level][(t * F[Level] + f_offset) * A + a];
    }

    template <size_t Level>
    inline const feature_storage_t *get_all_assets(size_t t, size_t field, size_t sub = 0) const {
      static_assert(Level < LEVEL_COUNT);
      assert(t < T[Level] && sub < LEVELS[Level].fields[field].width);
      const size_t f_offset = LEVELS[Level].offsets[field] + sub;
      assert(f_offset < F[Level]);
      return data[Level].data() + (t * F[Level] + f_offset) * A;
    }

    bool is_loaded() const { return A > 0; }

    // Preallocate for specific level only (on-demand allocation)
    // Upper layer manages buffer lifetime and reuses across multiple loads
    void preallocate_level(size_t A_, size_t level) {
      A = A_;
      // Preallocate for max capacity, actual T/F set by load_day_level from header
      data[level].resize(LEVELS[level].rows * LEVELS[level].width * A);
      if (LEVELS[level].columnar)
        temp_column.resize(LEVELS[level].rows * 1 * A);
    }
  };

  // Load single day, specific level, all features (for GUI)
  // Reads actual T/F from file headers, buffer already preallocated
  void load_day_level(const std::string &date, size_t level, DayTensor &out) const {
    Trace;
    assert(date.size() == 8 && "");
    assert(level < LEVEL_COUNT);
    assert(out.A > 0 && "Must preallocate() before load_day_level()");
    out.date = date;

    const std::string day_dir = feature_day_dir(base_dir_, date);
    const auto &L = LEVELS[level];
    const size_t A = out.A;

    if (L.columnar) {
      // 逐列: 每列读到 temp 再交织进 [T][F][A]
      feature_storage_t *dest = out.data[level].data();
      feature_storage_t *temp = out.temp_column.data();

      size_t T_actual = 0;
      {
        TraceN("ReadColumns");
        for (size_t f = 0; f < L.width; ++f) {
          const std::string col_path = feature_column_file(day_dir, level, f);
          assert(std::filesystem::exists(col_path) && "column file missing");

          size_t T_col, F_col, A_col;
          read_compressed_data(col_path, L.rows, 1, A, L.fingerprint, temp, &T_col, &F_col, &A_col);

          if (f == 0) {
            T_actual = T_col; // First column sets T
          } else {
            assert(T_col == T_actual && "Column T mismatch");
          }

          for (size_t t = 0; t < T_actual; ++t) {
            std::memcpy(&dest[(t * L.width + f) * A], &temp[t * A], A * sizeof(feature_storage_t));
          }
        }
      }

      out.T[level] = T_actual;
      out.F[level] = L.width;
    } else {
      const std::string merged_path = feature_file(day_dir, level);
      assert(std::filesystem::exists(merged_path) && "Merged file missing");

      size_t T_actual, F_actual, A_actual;
      read_compressed_data(merged_path, L.rows, L.width, A, L.fingerprint, out.data[level].data(),
                           &T_actual, &F_actual, &A_actual);

      out.T[level] = T_actual;
      out.F[level] = F_actual;
    }
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
    const auto &L = LEVELS[level];
    const size_t A = out.A;
    const size_t F_selected = feature_indices.size();
    const size_t T_per_day = L.rows;

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

    if (L.columnar) {
      // 逐列层: 只读选中的列文件
      feature_storage_t *temp = out.temp_column.data();

      for (size_t day_idx = 0; day_idx < num_days; ++day_idx) {
        TraceN("LoadDay");
        const auto &date = out.dates[day_idx];
        TraceTextS(date.c_str());

        const std::string day_dir = feature_day_dir(base_dir_, date);
        const size_t t_offset = day_idx * T_per_day;

        {
          TraceN("ReadColumns");
          for (size_t f_local = 0; f_local < F_selected; ++f_local) {
            size_t f_global = feature_indices[f_local];
            const std::string col_path = feature_column_file(day_dir, level, f_global);
            assert(std::filesystem::exists(col_path) && "column file missing");

            // Read into temp buffer
            {
              TraceN("ReadColumn");
              read_compressed_data(col_path, T_per_day, 1, A, L.fingerprint, temp);
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
      // 整层文件: 读整天再抽选中的列
      const size_t F_total = L.width;
      feature_storage_t *temp = out.temp_day.data();

      for (size_t day_idx = 0; day_idx < num_days; ++day_idx) {
        TraceN("LoadDay");
        const auto &date = out.dates[day_idx];
        TraceTextS(date.c_str());

        const std::string merged_file = feature_file(feature_day_dir(base_dir_, date), level);
        assert(std::filesystem::exists(merged_file) && "Merged file missing");

        const size_t t_offset = day_idx * T_per_day;

        // Read into temp buffer
        {
          TraceN("ReadMerged");
          read_compressed_data(merged_file, T_per_day, F_total, A, L.fingerprint, temp);
        }

        // Extract selected features
        {
          TraceN("ExtractFeatures");
          for (size_t t = 0; t < T_per_day; ++t) {
            for (size_t f_local = 0; f_local < F_selected; ++f_local) {
              size_t f_global = feature_indices[f_local];
              size_t f_offset = L.offsets[f_global];
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

  // 该日是否有特征文件 (以最后一层的整层文件为准: IO worker 按层序落盘, 它在则全在)
  static bool has_date(const std::string &base_dir, const std::string &date) {
    assert(date.size() == 8);
    return std::filesystem::exists(feature_file(feature_day_dir(base_dir, date), LEVEL_COUNT - 1));
  }
  bool has_date(const std::string &date) const { return has_date(base_dir_, date); }

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
