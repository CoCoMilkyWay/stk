// TabTable — 特征表 (Table 页)
//   1. Scope: 池 / 档 / 层下拉 (Common.hpp RenderScope, 与 NodeTime 页同一份 Feature::Selection, Features 自持)
//   2. Filters: 多维过滤
//   3. Features: 选主特征与对比特征 (Distribution / Transform 的输入); Stat / Range / Dist / PSD 列 = Preview 成品
#pragma once

#include "features/Storage/FeatureLevels.hpp" // TierId

#include <cstddef>
#include <cstdint>
#include <vector>

struct Feature;
struct SharedData;

namespace GUI::Features {

struct Preview;

// ============================================================================
// Table Tab UI State
// ============================================================================

struct TableUIState {
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
  // (它顺带置 CannotSkipItemsQueue 强制这批列提交). 行侧平时走 clipper 只提交视野内
  // 行, 贴合请求若用 clipper 帧的窄测量会先缩后弹 (抽搐) → 贴合窗全程全行提交,
  // 首帧只喂测量, 中段两帧才发请求, 尾帧落定 (见 TabTable.cpp 贴合窗).
  int fit_frames = 0;             // >0 = 贴合窗剩余帧数 (每帧递减, 窗内全行提交)
  int fit_level = -1;             // 快照: 层
  uint64_t fit_rows_hash = 0;     // 快照: 过滤后行集
  uint64_t fit_preview_epoch = 0; // 快照: preview 发布代 (Stat/Range/Dist/PSD 列内容)
  double fit_last_time = 0.0;     // 上次 epoch 触发贴合的时刻 (构建期发布密, 限速 0.5s)

  // Search
  char search_buffer[256] = {0};
};

// ============================================================================
// Render Function
// ============================================================================

void RenderTabTable(SharedData &data, const Preview &pv, TableUIState &ui_state);

// 该 (档, 层) 每行有效 Cat2 (值域探测 or 字段表人工覆盖, 与 Table 页同口径; 无预览 = "?") + 预览抽样全局值域.
// cat2[i] 指向静态串或字段表串 (稳定); 无预览的行 (含非预览档) val_min > val_max (空账目 ±inf).
// 内部短锁读 preview cells. OrderFlow legend 标注 / flag 轴缩放用
struct Cat2Snapshot {
  std::vector<const char *> cat2;
  std::vector<float> val_min, val_max;
};
void EffectiveCat2Snapshot(const Feature &feature, const Preview &pv, TierId tier, size_t level, Cat2Snapshot &out);

// 当前层过滤后的行集 (metadata 下标, 升序; 与表格 "Showing n / m" 同一口径, 不含排序).
// 内部短锁读 preview cells (Cat2 过滤要有效值). Correlation 热图取同一行集
void FilteredFeatureIndices(const Feature &feature, const Preview &pv, std::vector<int> &out);

// 特征表落地 JSON (给人看, 一行一特征): <当前库目录>/features.json (各层全部行, 元数据下标序,
// 不受过滤/排序影响; 表格可见列 + Stat / Range (有预览的行), 不落 Dist / PSD 曲线).
// 预览跑完 (Building → Done) 时由 TaskFeatures 调
void SaveFeatureTableJson(const SharedData &data, const Preview &pv);

} // namespace GUI::Features
