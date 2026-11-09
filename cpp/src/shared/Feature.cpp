#include "shared/Feature.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>

// ============================================================================
// Feature Constructor
// ============================================================================
Feature::Feature() {
  // Initialize metadata from compile-time arrays on construction (每档一份)
  for (size_t t = 0; t < TIER_COUNT; ++t)
    metadata[t].init_from_compile_time(static_cast<TierId>(t));
}

// ============================================================================
// Feature::Metadata Implementation
// ============================================================================

void Feature::Metadata::init_from_compile_time(TierId t) {
  tier = t;
  for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
    const auto f = feature_meta::features(t, lvl); // 档缺的层为空
    features[lvl].assign(f.begin(), f.end());
  }
  auto d = feature_meta::deps(t);
  std::move(d.begin(), d.end(), deps);
}

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
