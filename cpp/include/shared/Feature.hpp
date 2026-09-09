#pragma once

#include "features/Backend/FeatureStoreConfig.hpp" // 字段表 + LEVELS (width / valid / psd 由此推出)
#include <set>
#include <string>
#include <string_view>
#include <vector>

// ============================================================================
// Feature Metadata Structure (for UI display)
// ============================================================================

struct FeatureMetadata {
  const char *code;          // tick_ret_z
  uint8_t width;             // 1 (由 SRC 推出)
  L2::ValidType valid_type;  // ALL/DATA/DEPTH (由 SRC / 节点 flush 域推出)
  FeatureDataType data_type; // TS/CS/LB/META (由 SRC 列推出)
  const char *cat_l1;        // Operator/TS/<dir> or Operator/CS/<dir>
  const char *cat_l2;        // FIELDS row token, generated into FeatureCategoryL2_ALL
  NormMethod norm_method;    // ZSCORE, CLIP, etc.
  const char *formula;       // "(r-μ)/σ, W=50"
  const char *name_en;       // "Tick Return Z-score"
  const char *name_cn;       // "微小对数收益"
  const char *description;   // "滚动窗口标准化..."
  uint8_t level;             // 0=L0, 1=L1
};

// ============================================================================
// Compile-time Metadata Generation: 每层一张表 (字段表行 + 层信息)
// ============================================================================
#define GENERATE_METADATA(code, cat_l1, cat_l2, norm_method, name_en, name_cn, description, formula, src) \
  {#code, SRC_WIDTH_##src, SRC_VALID_##src, SRC_KIND_##src, cat_l1, #cat_l2, NormMethod::norm_method, formula, name_en, name_cn, description, kLevel},
#define GENERATE_METADATA_TABLE(name, num, fields, rows, psd, columnar, xor_delta) \
  namespace name##_meta_detail {                                                   \
    constexpr uint8_t kLevel = num;                                                \
    inline constexpr FeatureMetadata TABLE[] = {fields(GENERATE_METADATA)};        \
  }
#define METADATA_TABLE_PTR(name, num, fields, rows, psd, columnar, xor_delta) name##_meta_detail::TABLE,
#define METADATA_TABLE_COUNT(name, num, fields, rows, psd, columnar, xor_delta) std::size(name##_meta_detail::TABLE),

namespace FeatureMetadataRegistry {
ALL_LEVELS(GENERATE_METADATA_TABLE)
inline constexpr const FeatureMetadata *FEATURES[LEVEL_COUNT] = {ALL_LEVELS(METADATA_TABLE_PTR)};
inline constexpr size_t COUNTS[LEVEL_COUNT] = {ALL_LEVELS(METADATA_TABLE_COUNT)};
} // namespace FeatureMetadataRegistry

#undef GENERATE_METADATA
#undef GENERATE_METADATA_TABLE
#undef METADATA_TABLE_PTR
#undef METADATA_TABLE_COUNT

// ============================================================================
// Feature Data Structure (for SharedData)
// ============================================================================

struct Feature {
  // ==========================================================================
  // Feature Metadata (compile-time, read-only)
  // ==========================================================================

  struct Metadata {
    std::vector<FeatureMetadata> features[LEVEL_COUNT]; // [level] (0=L0, 1=L1)
    std::vector<std::string> deps[LEVEL_COUNT];         // [level][i] = 该特征直接依赖的字段 code (分号分隔), 与 features 平行
    void init_from_compile_time();                      // Copy from constexpr arrays + resolve deps

    // 字段 code → 该层列下标; 字段表里找不到 = 字段表与消费方脱节, assert
    size_t col_of(size_t level, const char *code) const;
  };
  Metadata metadata;

  // ==========================================================================
  // User Selection State
  // ==========================================================================

  struct Selection {
    int selected_level = 1; // 0=L0, 1=L1 (GUI 只在这两层选特征, 默认 L1)

    // Filter states
    std::set<FeatureDataType> filter_data_type;
    std::set<std::string_view> filter_cat_l1;
    std::set<std::string_view> filter_cat_l2;
    std::set<NormMethod> filter_norm_method;

    // Selected features (multi-selection; primary = 首个, 供需要单选的消费方)
    std::set<int> selected_features;

    // 单选视图: 多选的首个, 无选时 -1
    int primary_feature_idx() const { return selected_features.empty() ? -1 : *selected_features.begin(); }

    void clear();
  };
  Selection selection;

  // ==========================================================================
  // Methods
  // ==========================================================================

  // Constructor: Initialize metadata from compile-time arrays
  Feature();

  void clear();
};
