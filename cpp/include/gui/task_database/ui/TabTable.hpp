// Tab Table - L2 Database Asset Table View with Enhanced Filtering
// Shows asset table with board classification and ST filtering

#pragma once

#include "gui/task_database/models/SharedTypes.hpp"
#include "shared/Asset.hpp"
#include "shared/AssetInfo.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Database {

// 参与截面着色的列 (列号与 RenderDataTable 的 19 列一致):
// Listed / PE / PB / PS / PCF / DY1 / DY3 / DY5 / Cap
//
// 口径统一成"好 = 绿": 在市够久, 估值够低, 分红够多, 市值够大.
inline constexpr int kColoredColumns[] = {6, 8, 9, 10, 11, 12, 13, 14, 15};
inline constexpr int kColoredColumnCount = 9;

// 列号 → cs_colors 槽位; 不着色的列返回 -1
inline int ColoredColumnSlot(int col) {
  for (int i = 0; i < kColoredColumnCount; ++i)
    if (kColoredColumns[i] == col)
      return i;
  return -1;
}

// 表格视图 = 过滤 + 排序后的 asset_id 列表 (同 TabEncode 的 AssetTableView).
//
// 逐帧重建的代价: 每资产一次 stock_info 查找 (std::map, 还要先拼 "sh.600000"),
// 加上几千行的排序; 而 Orders / Order% / Miss_O 三列的行统计要遍历该资产的
// date_info —— 资产数 × 交易日数是五百万量级, 逐帧重算会把帧时间拖到几百
// 毫秒 (见 Asset::AssetStats 处的同一个坑). 行统计因此取自 Asset::asset_stats,
// 顺序取自本视图, 两者都只在扫描/过滤/排序变化时重算.
//
// 注意: 这里只缓存顺序, 不缓存 StockInfo 的值和指针 —— 基本面可能整体重载
// (指针会失效), 所以显示时每帧按 asset_id 重新查活数据.
struct TableView {
  std::vector<size_t> rows; // asset_id, 已过滤已排序

  // 截面渐进色 (红→黄→绿), 每个着色列一份, 与 rows 同序同长.
  // 0 = 该行在该列没有可比的值 (显示 "-", 退回默认色).
  //
  // 分位是"过滤后 pool 内"的名次, 所以只能跟 rows 一起在 RebuildView 里算:
  // 逐帧给几千行做 9 列排序和 rows 本身一样是帧时间杀手.
  std::vector<uint32_t> cs_colors[kColoredColumnCount];

  bool built = false;

  // 失效判据快照
  uint64_t generation = 0;     // 对应的 Asset::asset_stats_generation
  size_t asset_count = 0;      // 对应的 Asset::items.size()
  size_t stock_info_count = 0; // 对应的 stock_info.size() (基本面首次载入)
  int sort_column = -1;        // 对应的排序列
  bool sort_ascending = true;  // 对应的排序方向
  bool filter_st_only = false;
  bool filter_listed_only = false;
  BoardType board_filter = BoardType::All;
  std::string search_query;
  std::string industry_filter;
};

// Table state
struct TableState {
  // Filters
  //
  // 这里曾有 filter_missing_only / filter_no_missing 两个"缺口"过滤器, 但
  // 缺口是按天而不是按资产的概念 (见 AssetItem 的说明), 它们筛的那个量恒为
  // 0 —— 一个永远筛空, 一个永远等于不筛.
  bool filter_st_only = false;
  bool filter_listed_only = false; // Only show listed stocks (outDate is empty)
  BoardType board_filter = BoardType::All;
  std::string search_query;
  std::string industry_filter; // Filter by industry code (e.g. "C26")

  // Selection and sorting
  int selected_asset_idx = -1;
  int selected_column_idx_for_highlight = -1; // Column to highlight (from body click)
  int sort_column = -1;
  bool sort_ascending = true;

  // Cross-section analysis panel
  int selected_column_idx = -1;         // Selected column for analysis (-1 = none)
  bool show_cross_section_panel = true; // Show right panel
  float table_split_ratio = 0.65f;      // Left/right split ratio (0-1)

  // 过滤 + 排序结果的缓存 (表格与横截面面板共用)
  TableView view;
};

// Render the table tab
void RenderTabTable(
    Asset &asset,
    const StockInfoMap &stock_info,
    TableState &table_state);

} // namespace GUI::Database
