// Tab Feature - Feature Selection and Analysis Panel
// Allows users to:
//   1. Select feature level (L0/L1/L2)
//   2. Filter features by multiple dimensions
//   3. Select primary feature and secondary features for analysis
//   4. (Future) View analysis results
#pragma once

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
  bool show_filter_norm_method = false;

  // Table display
  int sort_column = -1;
  bool sort_ascending = true;

  // Search
  char search_buffer[256] = {0};
};

// ============================================================================
// Render Function
// ============================================================================

void RenderTabFeature(SharedData &data, FeatureUIState &ui_state);

} // namespace GUI::Features
