// Tab Asset - 资产表 (列定义一张表: 格 / 排序 / 名次底色 / 截面分析同源) + 右栏 Quotes / Statistics
//   近况列 (涨幅 / Δ5日线) = Lines 右表同一核 RecentOf: 新浪当日接上价格库的日脊末段; 快照 / 价格库发布只重填这两列 (行与其余列不动)
//   列 / 格 / 名次底色 / 表头排序 = 各表同一套 (gui/util/Sheet.hpp, GUI::rank_by / sort_less); 估值列 (PE / PB / PS / PCF) 显示倍数, 键 = 倒数的收益率 (同股息高为好)

#pragma once

#include "gui/task_market/ui/QuotesPanel.hpp"
#include "gui/util/Board.hpp"
#include "gui/util/Sheet.hpp"
#include "gui/util/Stage.hpp"
#include "shared/Asset.hpp"
#include "shared/AssetInfo.hpp"

#include <array>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Market {

// 过滤条件: 多选, 空集合 = 不过滤 (全选)
struct AssetFilter {
  std::set<int> st;               // 0=正常 1=ST 2=*ST (GetStLevel 口径)
  std::set<int> listed{0};        // 0=在市 1=退市 (out_date 是否为 0); 默认在市
  std::set<BoardType> board;      // BoardType (Unknown ~ BSE, 不含 All 哨兵)
  std::set<std::string> industry; // 申万一级行业代码 (ind_code)
  std::string search;             // 代码 / 简称子串
  bool operator==(const AssetFilter &) const = default;
};

// 格的输入: 任一变 → 整表重填
struct AssetCellsKey {
  uint64_t generation = 0; // Asset::generation
  size_t assets = 0;       // Asset::items.size()
  size_t infos = 0;        // stock_info.size() (基本面首次载入)
  AssetFilter filter;
  float font = 0.f; // 字号 (格宽)
  uint32_t ink = 0; // 字色 (主题)
  bool operator==(const AssetCellsKey &) const = default;
};
// 行情列 (近况) 的输入: 变 → 只重填行情列 (行与其余列不动)
struct AssetQuoteKey {
  std::shared_ptr<const Tails> tails; // 个股的日脊末段 (价格库变才换)
  uint64_t today = 0;                 // 当日日根的版本 = 行情日 · 快照时刻 (随快照变; 补分钟根不动日根)
  bool operator==(const AssetQuoteKey &) const = default;
};
// 过滤后的行与全部格 (表格与截面分析共用; 每帧只画可见行): 名次 = 过滤后 pool 内, 只能跟行一起重填.
//   逐帧重填是帧时间杀手: 几千行 × 二十列的格 + 九列名次排序; 行统计 (交易日 / 逐笔) 要遍历帧目录整列,
//   取自扫描一次算好的 Asset::asset_stats. 只缓存格的文本 / 键, 不缓存 StockInfo 指针 (基本面可能整体重载)
struct AssetCells {
  std::vector<size_t> ids;      // [行] asset_id (过滤后, 资产序)
  std::vector<SheetCell> cells; // [行][列]
  std::vector<float> cw;        // [列] 各格 (含表头) 最宽
  AssetQuoteKey quote;          // 行情列所填的源
  uint64_t quote_gen = 0;       // 行情列重填次数
};
// 显示序的输入: 格 (Stage gen + 行情列重填次数) + 排序列
struct AssetOrderKey {
  uint64_t cells = 0, quote = 0;
  int sort_column = -1;
  bool sort_ascending = true;
  bool operator==(const AssetOrderKey &) const = default;
};

struct AssetState {
  AssetFilter filter;
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

  Joint quotes;                                    // 本帧的个股行情 (近况列的源)
  Stage<AssetCellsKey, AssetCells> cells;          // 格
  Stage<AssetOrderKey, std::vector<size_t>> order; // [显示位] 行 (cells 的行下标)
};

void RenderTabAsset(
    Asset &asset,
    const StockInfos &stock_info,
    AssetState &asset_state,
    QuoteHub &mk);

} // namespace GUI::Market