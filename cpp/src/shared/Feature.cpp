#include "shared/Feature.hpp"

// ============================================================================
// Feature Constructor
// ============================================================================

Feature::Feature() {
  // Initialize metadata from compile-time arrays on construction
  metadata.init_from_compile_time();
}

// ============================================================================
// Feature::Metadata Implementation
// ============================================================================

void Feature::Metadata::init_from_compile_time() {
  // Copy from constexpr arrays to runtime vectors (for filtering/sorting)
  for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
    features[lvl].assign(FeatureMetadataRegistry::FEATURES[lvl],
                         FeatureMetadataRegistry::FEATURES[lvl] + FeatureMetadataRegistry::COUNTS[lvl]);
}

// ============================================================================
// Feature::Selection Implementation
// ============================================================================

void Feature::Selection::clear() {
  filter_data_type.clear();
  filter_cat_l1.clear();
  filter_cat_l2.clear();
  filter_norm_method.clear();
  primary_feature_idx = -1;
  secondary_features.clear();
}

// ============================================================================
// Feature Implementation
// ============================================================================

void Feature::clear() {
  selection.clear();
  // analysis留空,以后扩展
}
