// Tab Feature - Feature Selection and Analysis Panel
// Allows users to:
//   1. Select feature level (L0/L1/L2)
//   2. Filter features by multiple dimensions
//   3. Select primary feature and secondary features for analysis
//   4. (Future) View analysis results
#pragma once

#include <cstdint>
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

  // 列宽贴合: FixedFit 只在列首次出现那几帧量内容, 之后宽度冻结; 且 ScrollX 下
  // 视野外的列被跳过提交, 根本量不到 → 内容变了必须显式 TableSetColumnWidthAutoAll
  // (它顺带置 CannotSkipItemsQueue 强制这批列提交), 收敛要 2 帧, 故连发几帧.
  int fit_frames = 0;             // >0 = 本帧请求贴合 (每帧递减)
  int fit_level = -1;             // 快照: 层
  uint64_t fit_rows_hash = 0;     // 快照: 过滤后行集
  uint64_t fit_preview_epoch = 0; // 快照: preview 发布代 (Stat/Range/Dist/PSD 列内容)

  // Search
  char search_buffer[256] = {0};
};

// ============================================================================
// Render Function
// ============================================================================

void RenderTabFeature(SharedData &data, FeatureUIState &ui_state);

// 特征表落地 JSON (给人看, 一行一特征): <FeatureUniverseDir>/features.json (两层全部行, 元数据下标序,
// 不受过滤/排序影响; 表格可见列 + Stat / Range (仅 L1 有预览的行), 不落 Dist / PSD 曲线).
// 预览跑完 (Building → Done) 时由 TaskFeatures 调
void SaveFeatureTableJson(SharedData &data);

} // namespace GUI::Features
