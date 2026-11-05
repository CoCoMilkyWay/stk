// Tab Assets - 资产表 (列定义一张表: 取值 / 排序 / 截面色 / 截面分析同源) + 右栏 Quotes / Statistics

#pragma once

#include "gui/task_database/models/SharedTypes.hpp"
#include "gui/task_database/ui/QuotesPanel.hpp"
#include "shared/Asset.hpp"
#include "shared/AssetInfo.hpp"

#include <array>
#include <cstdint>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Database {

// 过滤条件: 多选, 空集合 = 不过滤 (全选)
struct AssetsFilter {
  std::set<int> st;               // 0=正常 1=ST 2=*ST (GetStLevel 口径)
  std::set<int> listed{0};        // 0=在市 1=退市 (out_date 是否为 0); 默认在市
  std::set<BoardType> board;      // BoardType (Unknown ~ BSE, 不含 All 哨兵)
  std::set<std::string> industry; // 申万一级行业代码 (ind_code)
  std::string search;             // 代码 / 简称子串
  bool operator==(const AssetsFilter &) const = default;
};

// 视图的输入: 任一变 → 重建
struct AssetsViewKey {
  uint64_t generation = 0; // Asset::generation
  size_t assets = 0;       // Asset::items.size()
  size_t infos = 0;        // stock_info.size() (基本面首次载入)
  int sort_column = -1;
  bool sort_ascending = true;
  AssetsFilter filter;
  bool operator==(const AssetsViewKey &) const = default;
};

// 表格视图 = 过滤 + 排序后的 asset_id 列表 + 截面色 (表格与横截面面板共用).
//
// 逐帧重建的代价: 几千行的排序; 而 Orders / Order% / Miss_O 三列的行统计要遍历该资产在
// 帧目录里的整列 —— 资产数 × 交易日数是五百万量级, 逐帧重算会把帧时间拖到几百
// 毫秒 (见 Asset::AssetStats 处的同一个坑). 行统计因此取自 Asset::asset_stats,
// 顺序取自本视图, 两者都只在 key 变化时重算.
//
// 注意: 这里只缓存顺序, 不缓存 StockInfo 的值和指针 —— 基本面可能整体重载
// (指针会失效), 所以显示时每帧按 asset_id 重新查活数据.
struct AssetsView {
  bool built = false;
  AssetsViewKey key;
  std::vector<size_t> rows; // asset_id, 已过滤已排序
  // [列][rows 位置] 截面渐进色 ImU32 (红→黄→绿); 0 = 该行在该列没有可比的值 (退回默认色); 不着色的列空.
  // 分位是"过滤后 pool 内"的名次, 所以只能跟 rows 一起重建: 逐帧给几千行做 9 列排序和 rows 本身一样是帧时间杀手.
  std::vector<std::vector<uint32_t>> cs;
};

struct AssetsState {
  AssetsFilter filter;
  std::array<char, 256> search_buf{};                          // 搜索框
  std::vector<std::pair<std::string, std::string>> industries; // 行业下拉 (ind_code, 名): 从 stock_info 扫出
  size_t industries_of = SIZE_MAX;                             // 扫 industries 时的 stock_info.size() (基本面晚到 → 重扫)

  int selected_asset = -1; // 点中的行 (asset_id), Quotes 页看它
  int sort_column = -1;
  bool sort_ascending = true;

  // 右栏: Quotes (选中标的行情) / Statistics (选中列的截面分析); 点格子在 Statistics 页才切列
  bool quotes_page = true;
  int selected_column_idx = -1; // 截面分析的列 (-1 = 无)
  bool show_right_panel = true;
  float table_split_ratio = 0.65f; // 左右分栏比 (0-1)
  StockState stock;

  AssetsView view;
};

void RenderTabAssets(
    Asset &asset,
    const StockInfos &stock_info,
    AssetsState &assets_state,
    Market &market);

} // namespace GUI::Database
