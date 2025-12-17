#pragma once

#include "FeatureStoreConfig.hpp"
#include "ZstdHelper.hpp"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

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
    std::vector<std::string> dates;           // [N_days]
    std::vector<size_t> day_offsets;          // [N_days+1] cumulative T
    std::vector<feature_storage_t> data;      // [total_T × F_selected × A]
    size_t A = 0;
    std::vector<size_t> feature_indices;      // which features loaded
    
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
    
    template<size_t Level>
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
    
    template<size_t Level>
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
      // L0: Read individual feature columns
      std::vector<std::vector<feature_storage_t>> columns(F_level);
      
      for (size_t f = 0; f < F_level; ++f) {
        std::string col_path = day_dir + "/features_L0_f" + std::to_string(f) + ".zst";
        
        if (!std::filesystem::exists(col_path))
          return false;
        
        std::ifstream ifs(col_path, std::ios::binary);
        size_t T, F, A;
        ifs.read(reinterpret_cast<char*>(&T), sizeof(size_t));
        ifs.read(reinterpret_cast<char*>(&F), sizeof(size_t));
        ifs.read(reinterpret_cast<char*>(&A), sizeof(size_t));
        
        assert(F == 1);
        
        if (f == 0) {
          out.T[level] = T;
          out.F[level] = F_level;
          out.A = A;
        } else {
          assert(out.T[level] == T && out.A == A);
        }
        
        // Read and decompress column
        ifs.seekg(0, std::ios::end);
        size_t file_size = ifs.tellg();
        size_t compressed_size = file_size - 3 * sizeof(size_t);
        ifs.seekg(3 * sizeof(size_t), std::ios::beg);
        
        std::vector<uint8_t> compressed(compressed_size);
        ifs.read(reinterpret_cast<char*>(compressed.data()), compressed_size);
        
        columns[f].resize(T * A);
        size_t decompressed_size = T * A * sizeof(feature_storage_t);
        ZstdHelper::decompress(compressed.data(), compressed_size,
                               columns[f].data(), decompressed_size);
      }
      
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
      // L1/L2: Read merged compressed file
      std::string merged_path = day_dir + "/features_L" + std::to_string(level) + ".zst";
      
      if (!std::filesystem::exists(merged_path))
        return false;
      
      std::ifstream ifs(merged_path, std::ios::binary);
      if (!ifs)
        return false;
      
      // Read header
      size_t T, F, A;
      ifs.read(reinterpret_cast<char*>(&T), sizeof(size_t));
      ifs.read(reinterpret_cast<char*>(&F), sizeof(size_t));
      ifs.read(reinterpret_cast<char*>(&A), sizeof(size_t));
      
      assert(F == F_level);
      out.T[level] = T;
      out.F[level] = F;
      out.A = A;
      
      // Read compressed data
      ifs.seekg(0, std::ios::end);
      size_t file_size = ifs.tellg();
      size_t compressed_size = file_size - 3 * sizeof(size_t);
      ifs.seekg(3 * sizeof(size_t), std::ios::beg);
      
      std::vector<uint8_t> compressed(compressed_size);
      ifs.read(reinterpret_cast<char*>(compressed.data()), compressed_size);
      
      // Decompress directly into output tensor
      size_t decompressed_size = T * F * A * sizeof(feature_storage_t);
      out.data[level].resize(T * F * A);
      ZstdHelper::decompress(compressed.data(), compressed_size,
                             out.data[level].data(), decompressed_size);
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
    
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs)
      return false;
    
    // Read header
    ifs.read(reinterpret_cast<char*>(&out.T), sizeof(size_t));
    ifs.read(reinterpret_cast<char*>(&out.F), sizeof(size_t));
    ifs.read(reinterpret_cast<char*>(&out.A), sizeof(size_t));
    
    // Read compressed data
    ifs.seekg(0, std::ios::end);
    size_t file_size = ifs.tellg();
    size_t compressed_size = file_size - 3 * sizeof(size_t);
    ifs.seekg(3 * sizeof(size_t), std::ios::beg);

    std::vector<uint8_t> compressed(compressed_size);
    ifs.read(reinterpret_cast<char*>(compressed.data()), compressed_size);

    // Decompress directly into output
    size_t decompressed_size = out.T * out.F * out.A * sizeof(feature_storage_t);
    out.data.resize(out.T * out.F * out.A);
    ZstdHelper::decompress(compressed.data(), compressed_size,
                           out.data.data(), decompressed_size);
    
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
      // L0: Read individual column files
      out.day_offsets.push_back(0);
      size_t total_T = 0;
      
      for (const auto &date : out.dates) {
        std::string first_col = base_dir_ + "/" + date.substr(0, 4) + "/" + 
                               date.substr(4, 2) + "/" + date.substr(6, 2) + 
                               "/features_L0_f" + std::to_string(feature_indices[0]) + ".zst";
        
        if (!std::filesystem::exists(first_col))
          return false;

        std::ifstream ifs(first_col, std::ios::binary);
        size_t T, F, A;
        ifs.read(reinterpret_cast<char*>(&T), sizeof(size_t));
        ifs.read(reinterpret_cast<char*>(&F), sizeof(size_t));
        ifs.read(reinterpret_cast<char*>(&A), sizeof(size_t));
        
        assert(F == 1);
        
        if (out.A == 0) {
          out.A = A;
        } else {
          assert(out.A == A);
        }

        total_T += T;
        out.day_offsets.push_back(total_T);
      }

      out.data.resize(total_T * F_selected * out.A);

      size_t global_t_offset = 0;
      for (size_t day_idx = 0; day_idx < out.dates.size(); ++day_idx) {
        const auto &date = out.dates[day_idx];
        std::string day_dir = base_dir_ + "/" + date.substr(0, 4) + "/" + 
                              date.substr(4, 2) + "/" + date.substr(6, 2);
        
        size_t day_T = out.day_offsets[day_idx + 1] - out.day_offsets[day_idx];

        for (size_t f_local = 0; f_local < F_selected; ++f_local) {
          size_t f_global = feature_indices[f_local];
          std::string col_path = day_dir + "/features_L0_f" + std::to_string(f_global) + ".zst";

          if (!std::filesystem::exists(col_path))
            return false;

          std::ifstream ifs(col_path, std::ios::binary);
          size_t T, F, A;
          ifs.read(reinterpret_cast<char*>(&T), sizeof(size_t));
          ifs.read(reinterpret_cast<char*>(&F), sizeof(size_t));
          ifs.read(reinterpret_cast<char*>(&A), sizeof(size_t));
          
          assert(T == day_T && F == 1 && A == out.A);

          ifs.seekg(0, std::ios::end);
          size_t file_size = ifs.tellg();
          size_t compressed_size = file_size - 3 * sizeof(size_t);
          ifs.seekg(3 * sizeof(size_t), std::ios::beg);

          std::vector<uint8_t> compressed(compressed_size);
          ifs.read(reinterpret_cast<char*>(compressed.data()), compressed_size);

          std::vector<feature_storage_t> col_data(T * A);
          size_t decompressed_size = T * A * sizeof(feature_storage_t);
          ZstdHelper::decompress(compressed.data(), compressed_size,
                                 col_data.data(), decompressed_size);

          for (size_t t = 0; t < T; ++t) {
            std::memcpy(&out.data[(global_t_offset + t) * F_selected * out.A + f_local * out.A],
                        &col_data[t * out.A],
                        out.A * sizeof(feature_storage_t));
          }
        }

        global_t_offset += day_T;
      }
    } else {
      // L1/L2: Read merged files and extract selected columns
      out.day_offsets.push_back(0);
      size_t total_T = 0;
      
      for (const auto &date : out.dates) {
        std::string merged_file = base_dir_ + "/" + date.substr(0, 4) + "/" + 
                                 date.substr(4, 2) + "/" + date.substr(6, 2) + 
                                 "/features_L" + std::to_string(level) + ".zst";
        
        if (!std::filesystem::exists(merged_file))
          return false;

        std::ifstream ifs(merged_file, std::ios::binary);
        size_t T, F, A;
        ifs.read(reinterpret_cast<char*>(&T), sizeof(size_t));
        ifs.read(reinterpret_cast<char*>(&F), sizeof(size_t));
        ifs.read(reinterpret_cast<char*>(&A), sizeof(size_t));
        
        if (out.A == 0) {
          out.A = A;
        } else {
          assert(out.A == A);
        }

        total_T += T;
        out.day_offsets.push_back(total_T);
      }

      out.data.resize(total_T * F_selected * out.A);

      size_t global_t_offset = 0;
      for (size_t day_idx = 0; day_idx < out.dates.size(); ++day_idx) {
        const auto &date = out.dates[day_idx];
        std::string merged_file = base_dir_ + "/" + date.substr(0, 4) + "/" + 
                                 date.substr(4, 2) + "/" + date.substr(6, 2) + 
                                 "/features_L" + std::to_string(level) + ".zst";

        std::ifstream ifs(merged_file, std::ios::binary);
        size_t T, F, A;
        ifs.read(reinterpret_cast<char*>(&T), sizeof(size_t));
        ifs.read(reinterpret_cast<char*>(&F), sizeof(size_t));
        ifs.read(reinterpret_cast<char*>(&A), sizeof(size_t));
        
        assert(A == out.A);
        size_t day_T = out.day_offsets[day_idx + 1] - out.day_offsets[day_idx];
        assert(T == day_T);

        ifs.seekg(0, std::ios::end);
        size_t file_size = ifs.tellg();
        size_t compressed_size = file_size - 3 * sizeof(size_t);
        ifs.seekg(3 * sizeof(size_t), std::ios::beg);

        std::vector<uint8_t> compressed(compressed_size);
        ifs.read(reinterpret_cast<char*>(compressed.data()), compressed_size);

        // Decompress entire day tensor
        std::vector<feature_storage_t> day_data(T * F * A);
        size_t decompressed_size = T * F * A * sizeof(feature_storage_t);
        ZstdHelper::decompress(compressed.data(), compressed_size,
                               day_data.data(), decompressed_size);

        // Extract selected features
        for (size_t t = 0; t < T; ++t) {
          for (size_t f_local = 0; f_local < F_selected; ++f_local) {
            size_t f_global = feature_indices[f_local];
            std::memcpy(&out.data[(global_t_offset + t) * F_selected * out.A + f_local * out.A],
                        &day_data[t * F * A + f_global * A],
                        A * sizeof(feature_storage_t));
          }
        }

        global_t_offset += day_T;
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

private:
  std::string base_dir_;
};
