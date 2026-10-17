#include "shared/Feature.hpp"
#include "shared/Config.hpp"

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

std::string Feature::Selection::lib_of(TierId t) const {
  assert(t == TierId::Bar || !pool.empty());
  return t == TierId::Bar ? Config::kAllTierDir : pool;
}

std::vector<int> Feature::Selection::picked_at(TierId t, size_t level) const {
  std::vector<int> out;
  for (const ColRef &c : picked)
    if (c.tier == t && c.level == level)
      out.push_back(c.col);
  return out;
}

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

TierId Feature::main_tier(const std::vector<ColRef> &cols) {
  for (const ColRef &c : cols)
    if (c.tier == TierId::Lob)
      return TierId::Lob;
  return TierId::Bar;
}

void Feature::clear() { selection.clear(); }
