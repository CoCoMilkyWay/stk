#pragma once

#include "features/FeatureMeta.hpp" // FeatureMetadata / feature_meta:: (稳定头, 不依赖字段表)
#include <array>
#include <cassert>
#include <set>
#include <string>
#include <string_view>
#include <vector>

// ts:: / cs:: 归一化枚举的 GUI 适配 (与 FeaturesDefine 的 to_string / *_ALL 同形, 供 TabTable 过滤下拉 / 两列显示)
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
    TierId tier = TierId::Lob;
    std::vector<FeatureMetadata> features[LEVEL_COUNT]; // [level] (FeatureLevel; 档缺的层为空)
    std::vector<std::string> deps[LEVEL_COUNT];         // [level][i] = 该特征直接依赖的字段 code (分号分隔), 与 features 平行
    void init_from_compile_time(TierId t);              // 字段表拷贝 + 依赖 (feature_meta::)

    // 字段 code → 该层列下标; 字段表里找不到 = 字段表与消费方脱节, assert
    size_t col_of(size_t level, const char *code) const;
  };
  Metadata metadata[TIER_COUNT]; // [tier]

  // ==========================================================================
  // User Selection State
  // ==========================================================================

  struct Selection {
    PoolId pool = kAllPool;      // 当前池 (特征面板与因子页共用读写; 切档保留)
    TierId tier = TierId::Bar;   // 当前浏览的档 (TabTable 档单选)
    int selected_level = L_1min; // 当前浏览的层 (FeatureLevel: L_sec / L_1min / L_5min .. L_60min / L_1day), 默认 L_1min

    LibId lib() const { return lib_id(pool, tier); } // 当前库

    // Filter states
    std::set<FeatureDataType> filter_data_type;
    std::set<std::string_view> filter_cat_l1;
    std::set<std::string_view> filter_cat_l2;
    std::set<ts::MethodId> filter_ts_method; // TS Norm 列按 Method 过滤 (Tf 只显示)
    std::set<cs::MethodId> filter_cs_method; // CS Norm 列同

    // 选中列 (跨库跨层多选; 选中时绑定当时的当前库, 切库 / 切层保留; 同列在不同库各选一次; primary = 首个, 供需要单选的消费方)
    std::set<ColRef> picked;

    const ColRef *primary() const { return picked.empty() ? nullptr : &*picked.begin(); }

    void clear_filters();
    void clear(); // 过滤 + 选中
  };
  Selection selection;

  // ==========================================================================
  // Methods
  // ==========================================================================

  // Constructor: Initialize metadata from compile-time arrays
  Feature();

  const Metadata &meta(TierId t) const { return metadata[static_cast<size_t>(t)]; }
  const Metadata &meta() const { return meta(selection.tier); } // 当前档
  const FeatureMetadata &meta(const ColRef &c) const {
    const auto &list = meta(c.tier()).features[c.level];
    assert(c.col < list.size());
    return list[c.col];
  }
  // (库, 层, 字段 code) → 列; 字段表里找不到 = assert (Metadata::col_of)
  ColRef ref(LibId lib, size_t level, const char *code) const {
    return {lib, static_cast<uint8_t>(level), static_cast<uint16_t>(meta(lib_tier(lib)).col_of(level, code))};
  }

  void clear();
};
