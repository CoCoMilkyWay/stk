#pragma once

#include "features/FeaturesDefine.hpp"
#include <set>
#include <vector>

// ============================================================================
// Feature Metadata Structure (for UI display)
// ============================================================================

struct FeatureMetadata {
  const char *code;          // tick_ret_z
  uint8_t width;             // 1 or LOB_DEPTH
  L2::ValidType valid_type;  // ALL/DATA/DEPTH (valid粒度)
  FeatureDataType data_type; // TS/CS/LB/SH/META
  FeatureCategoryL1 cat_l1;  // MOMENTUM, IMBALANCE, etc.
  FeatureCategoryL2 cat_l2;  // NORMALIZED, OSCILLATOR, etc.
  NormMethod norm_method;    // ZSCORE, CLIP, etc.
  const char *psd;           // "100/00/00" - Power Spectral Density
  const char *formula;       // "(r-μ)/σ, W=50"
  const char *name_en;       // "Tick Return Z-score"
  const char *name_cn;       // "微小对数收益"
  const char *description;   // "滚动窗口标准化..."
  uint8_t level;             // 0=L0, 1=L1
};

// ============================================================================
// Compile-time Metadata Generation from FeaturesDefine.hpp
// ============================================================================

// Macro to expand LEVEL_X_FIELDS into FeatureMetadata array
// Note: Parameter order matches FeaturesDefine.hpp: ...psd, name_en, name_cn, description, formula
//       But struct field order is: ...psd, formula, name_en, name_cn, description
#define GENERATE_METADATA(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, psd, name_en, name_cn, description, formula, src) \
  {#code, width, L2::ValidType::valid_type, FeatureDataType::data_type, FeatureCategoryL1::cat_l1, FeatureCategoryL2::cat_l2, NormMethod::norm_method, psd, formula, name_en, name_cn, description, 255},

// Compile-time generated constexpr arrays
namespace FeatureMetadataRegistry {
constexpr FeatureMetadata FEATURES_L0[] = {LEVEL_0_FIELDS(GENERATE_METADATA)};
constexpr FeatureMetadata FEATURES_L1[] = {LEVEL_1_FIELDS(GENERATE_METADATA)};

constexpr size_t COUNT_L0 = sizeof(FEATURES_L0) / sizeof(FeatureMetadata);
constexpr size_t COUNT_L1 = sizeof(FEATURES_L1) / sizeof(FeatureMetadata);
} // namespace FeatureMetadataRegistry

#undef GENERATE_METADATA

// ============================================================================
// Feature Data Structure (for SharedData)
// ============================================================================

struct Feature {
  // ==========================================================================
  // Feature Metadata (compile-time, read-only)
  // ==========================================================================

  struct Metadata {
    std::vector<FeatureMetadata> features_l0;
    std::vector<FeatureMetadata> features_l1;

    void init_from_compile_time(); // Copy from constexpr arrays
  };
  Metadata metadata;

  // ==========================================================================
  // User Selection State
  // ==========================================================================

  struct Selection {
    int selected_level = 0; // 0=L0, 1=L1

    // Filter states
    std::set<FeatureDataType> filter_data_type;
    std::set<FeatureCategoryL1> filter_cat_l1;
    std::set<FeatureCategoryL2> filter_cat_l2;
    std::set<NormMethod> filter_norm_method;

    // Selected features
    int primary_feature_idx = -1;      // Primary feature (single selection)
    std::set<int> secondary_features;  // Other features (multi-selection)

    void clear();
  };
  Selection selection;

  // ==========================================================================
  // Analysis Results (reserved for future expansion)
  // ==========================================================================

  struct AnalysisResults {
    // Distribution statistics
    struct Distribution {
      // mean, std, min, max, quantiles, histogram, etc.
      // TODO: expand in the future
    };

    // Time series visualization
    struct TimeSeries {
      // plot data, rolling stats, etc.
      // TODO: expand in the future
    };

    // Correlation analysis
    struct Correlation {
      // correlation matrix, heatmap data, etc.
      // TODO: expand in the future
    };

    // More analysis types can be added here...
  };
  AnalysisResults analysis;

  // ==========================================================================
  // Methods
  // ==========================================================================

  // Constructor: Initialize metadata from compile-time arrays
  Feature();

  void clear();
};
