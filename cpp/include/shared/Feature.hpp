#pragma once

#include "features/Backend/FeatureStoreConfig.hpp" // 字段表 + LEVELS (width / valid / psd 由此推出)
#include "features/Method/CS.hpp"                  // CS 行 Tf / Method (SRC 推出, GUI "CS Norm" 列); TS 侧经 FeatureStoreConfig 带入
#include <array>
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
  ts::TfId ts_tf;            // 由 SRC 推出: OP(..., Tf, Method) → 时序归一化 (Tf 已落盘套用, Method 占位); 非 OP 行 None
  ts::MethodId ts_method;
  cs::TfId cs_tf; // 由 SRC 推出: CS(..., Tf, Method) → 截面归一化; 非 CS 行 None
  cs::MethodId cs_method;
  const char *formula;     // "(r-μ)/σ, W=50"
  const char *name_en;     // "Tick Return Z-score"
  const char *name_cn;     // "微小对数收益"
  const char *description; // "滚动窗口标准化..."
  uint8_t level;           // 0=L0, 1=L1
};

// ============================================================================
// Compile-time Metadata Generation: 每层一张表 (字段表行 + 层信息)
// ============================================================================
#define GENERATE_METADATA(code, cat_l1, cat_l2, name_en, name_cn, description, formula, src)                                        \
  {#code, SRC_WIDTH_##src, SRC_VALID_##src, SRC_KIND_##src, cat_l1, #cat_l2, SRC_TS_TF_##src, SRC_TS_METHOD_##src, SRC_CS_TF_##src, \
   SRC_CS_METHOD_##src, formula, name_en, name_cn, description, kLevel},
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

// ts:: / cs:: 归一化枚举的 GUI 适配 (与 FeaturesDefine 的 to_string / *_ALL 同形, 供 TabFeature 过滤下拉 / 两列显示)
inline constexpr EnumStr to_string(ts::TfId t) { return {ts::TF_TOKENS[static_cast<size_t>(t)], ts::TF_NAMES[static_cast<size_t>(t)]}; }
inline constexpr EnumStr to_string(ts::MethodId m) { return {ts::METHOD_TOKENS[static_cast<size_t>(m)], ts::METHOD_NAMES[static_cast<size_t>(m)]}; }
inline constexpr EnumStr to_string(cs::TfId t) { return {cs::TF_TOKENS[static_cast<size_t>(t)], cs::TF_NAMES[static_cast<size_t>(t)]}; }
inline constexpr EnumStr to_string(cs::MethodId m) { return {cs::METHOD_TOKENS[static_cast<size_t>(m)], cs::METHOD_NAMES[static_cast<size_t>(m)]}; }
template <class E>
constexpr auto norm_enum_all() {
  std::array<E, static_cast<size_t>(E::kCount)> a{};
  for (size_t i = 0; i < a.size(); ++i)
    a[i] = static_cast<E>(i);
  return a;
}
inline constexpr auto ts_MethodId_ALL = norm_enum_all<ts::MethodId>();
inline constexpr auto cs_MethodId_ALL = norm_enum_all<cs::MethodId>();

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
    std::set<ts::MethodId> filter_ts_method; // TS Norm 列按 Method 过滤 (Tf 只显示)
    std::set<cs::MethodId> filter_cs_method; // CS Norm 列同

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
