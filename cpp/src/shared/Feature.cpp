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
  features_l0.assign(FeatureMetadataRegistry::FEATURES_L0,
                     FeatureMetadataRegistry::FEATURES_L0 + FeatureMetadataRegistry::COUNT_L0);

  features_l1.assign(FeatureMetadataRegistry::FEATURES_L1,
                     FeatureMetadataRegistry::FEATURES_L1 + FeatureMetadataRegistry::COUNT_L1);

  features_l2.assign(FeatureMetadataRegistry::FEATURES_L2,
                     FeatureMetadataRegistry::FEATURES_L2 + FeatureMetadataRegistry::COUNT_L2);

  // Mark level for each feature
  for (auto &f : features_l0)
    f.level = 0;
  for (auto &f : features_l1)
    f.level = 1;
  for (auto &f : features_l2)
    f.level = 2;
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
  // analysis留空，以后扩展
}
