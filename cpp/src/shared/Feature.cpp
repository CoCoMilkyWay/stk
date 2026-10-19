#include "shared/Feature.hpp"
#include "shared/FeatureLib.hpp" // lib_id

#include <cassert>
#include <cstring>

// ============================================================================
// Feature Constructor
// ============================================================================
// init_from_compile_time 的定义在 src/features/FeatureMeta.cpp (唯一依赖生成字段表的
// 元数据 TU) —— 本文件与字段表解耦, 增删改特征不重编.

Feature::Feature() {
  // Initialize metadata from compile-time arrays on construction (每档一份)
  for (size_t t = 0; t < TIER_COUNT; ++t)
    metadata[t].init_from_compile_time(static_cast<TierId>(t));
}

// ============================================================================
// Feature::Metadata Implementation
// ============================================================================

size_t Feature::Metadata::col_of(size_t level, const char *code) const {
  assert(level < LEVEL_COUNT);
  const auto &list = features[level];
  for (size_t i = 0; i < list.size(); ++i)
    if (std::strcmp(list[i].code, code) == 0)
      return i;
  assert(false && "字段表里找不到该列");
  return 0;
}

// ============================================================================
// Feature::Selection Implementation
// ============================================================================

const std::string &Feature::Selection::lib_of(TierId t) const {
  assert(!libs[t].empty() && "档的当前库未定 (TaskSettings 启动时按 default_lib 填)");
  return libs[t];
}

LibId Feature::Selection::cur_lib(TierId t) const { return lib_id(lib_of(t)); }

void Feature::Selection::clear_filters() {
  filter_data_type.clear();
  filter_cat_l1.clear();
  filter_cat_l2.clear();
  filter_ts_method.clear();
  filter_cs_method.clear();
}

void Feature::Selection::clear() {
  clear_filters();
  picked.clear();
}

// ============================================================================
// Feature Implementation
// ============================================================================

void Feature::clear() { selection.clear(); }
