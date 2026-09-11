#include "shared/Feature.hpp"

#include <cassert>
#include <cstring>

// ============================================================================
// Feature Constructor
// ============================================================================
// init_from_compile_time 的定义在 src/features/FeatureMeta.cpp (唯一依赖生成字段表的
// 元数据 TU) —— 本文件与字段表解耦, 增删改特征不重编.

Feature::Feature() {
  // Initialize metadata from compile-time arrays on construction
  metadata.init_from_compile_time();
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

void Feature::Selection::clear() {
  filter_data_type.clear();
  filter_cat_l1.clear();
  filter_cat_l2.clear();
  filter_ts_method.clear();
  filter_cs_method.clear();
  selected_features.clear();
}

// ============================================================================
// Feature Implementation
// ============================================================================

void Feature::clear() { selection.clear(); }
