// Tab Feature - Feature Selection and Analysis Panel
// Allows users to:
//   1. Select feature level (L0/L1/L2)
//   2. Filter features by multiple dimensions
//   3. Select primary feature and secondary features for analysis
//   4. (Future) View analysis results
#pragma once

#include <vector>

struct SharedData;

namespace GUI::Features {

// ============================================================================
// Feature Tab UI State
// ============================================================================

struct FeatureUIState {
  // Filter dropdown states
  bool show_filter_data_type = false;
  bool show_filter_cat_l1 = false;
  bool show_filter_cat_l2 = false;
  bool show_filter_ts_method = false;
  bool show_filter_cs_method = false;

  // Table display
  int sort_column = -1;
  bool sort_ascending = true;

  // 聚类排序缓存 (凝聚聚类较重, 只在 level / 过滤集变化时重算)
  int cluster_cache_level = -1;
  std::vector<int> cluster_cache_key; // cat_l1 稳定排序后的 filtered_indices
  std::vector<int> cluster_cache_val; // 聚类排序结果

  // Search
  char search_buffer[256] = {0};
};

// ============================================================================
// Render Function
// ============================================================================

void RenderTabFeature(SharedData &data, FeatureUIState &ui_state);

// 特征表落地 JSON: <FeatureUniverseDir>/features.json (两层全部行, 元数据下标序, 不受过滤/排序影响;
// 表格各列 + 预览账目/值域/PDF/PSD (仅 L1 有预览的行)). 预览跑完 (Building → Done) 时由 TaskFeatures 调
void SaveFeatureTableJson(SharedData &data);

} // namespace GUI::Features
