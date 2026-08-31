// Tab Table - L2 Database Asset Table View Implementation
// 18-column table with enhanced filtering and cross-section analysis panel

#include "gui/task_database/ui/TabTable.hpp"
#include "gui/task_database/models/SharedTypes.hpp"
#include "gui/task_database/ui/CrossSectionAnalysis.hpp"
#include "imgui.h"
#include "implot.h"
#include <chrono>
#include <cmath>
#include <cstdio>
#include <limits>
#include <numeric>

namespace GUI::Database {

// Color constants
constexpr ImVec4 COLOR_SH = ImVec4(0.0f, 0.4f, 0.8f, 1.0f);
constexpr ImVec4 COLOR_SZ = ImVec4(0.0f, 0.6f, 0.5f, 1.0f);
constexpr ImVec4 COLOR_BJ = ImVec4(0.8f, 0.5f, 0.1f, 1.0f);
constexpr ImVec4 COLOR_GREEN = ImVec4(0.3f, 0.95f, 0.4f, 1.0f);
constexpr ImVec4 COLOR_YELLOW = ImVec4(1.0f, 0.95f, 0.3f, 1.0f);
constexpr ImVec4 COLOR_RED = ImVec4(0.95f, 0.3f, 0.3f, 1.0f);
constexpr ImVec4 COLOR_GRAY = ImVec4(0.6f, 0.6f, 0.6f, 1.0f);

// ============================================================================
// Helper: 在市时长 (ipoDate → outDate 或今天), 分解成 年/月/日
// ============================================================================

struct ListedSpan {
  int years = 0;
  int months = 0;
  int days = 0;
  int total_days = 0;
  bool valid = false;
};

ListedSpan CalculateListedSpan(const StockInfo &info) {
  ListedSpan span;
  if (info.ipoDate.length() != 10)
    return span;

  using namespace std::chrono;

  auto parse = [](const std::string &s) {
    return year_month_day{year{std::stoi(s.substr(0, 4))},
                          month{static_cast<unsigned>(std::stoi(s.substr(5, 2)))},
                          day{static_cast<unsigned>(std::stoi(s.substr(8, 2)))}};
  };

  const year_month_day ipo = parse(info.ipoDate);
  // 退市股的在市时长截到退市日, 不跟着今天一起涨
  const year_month_day end = info.outDate.length() == 10
                                 ? parse(info.outDate)
                                 : year_month_day{floor<days>(system_clock::now())};
  if (!ipo.ok() || !end.ok() || sys_days(end) < sys_days(ipo))
    return span;

  span.total_days = static_cast<int>((sys_days(end) - sys_days(ipo)).count());

  int y = static_cast<int>(end.year()) - static_cast<int>(ipo.year());
  int m = static_cast<int>(static_cast<unsigned>(end.month())) -
          static_cast<int>(static_cast<unsigned>(ipo.month()));
  int d = static_cast<int>(static_cast<unsigned>(end.day())) -
          static_cast<int>(static_cast<unsigned>(ipo.day()));
  if (d < 0) {
    --m;
    // 借上一个月的天数 (相对 end)
    const year_month prev = year_month{end.year(), end.month()} - months{1};
    d += static_cast<int>(static_cast<unsigned>((prev / last).day()));
  }
  if (m < 0) {
    --y;
    m += 12;
  }

  span.years = y;
  span.months = m;
  span.days = d;
  span.valid = true;
  return span;
}

// ============================================================================
// Helper: Calculate market cap (billion yuan)
// ============================================================================

float CalculateMarketCap(const StockInfo &info) {
  // Market cap (billion) = amount (yuan) x 100 / turn (%) / 1e8
  if (info.amount.empty() || info.turn.empty())
    return 0.0;

  try {
    float amount = std::stod(info.amount);
    float turn = std::stod(info.turn);

    if (turn <= 0)
      return 0.0;

    return amount * 100.0 / turn / 1e8;
  } catch (...) {
    return 0.0;
  }
}

// ============================================================================
// Helper: ST level from StockInfo (isST = cn_stock_status.st_status 原值)
// ============================================================================

// 0 = 正常 (含无基本面), 1 = ST, 2 = *ST (退市风险警示)
int GetStLevel(const StockInfo *info) {
  if (!info)
    return 0;
  if (info->isST == "2")
    return 2;
  if (info->isST == "1")
    return 1;
  return 0;
}

const char *GetStLabel(int level) {
  return level == 2 ? "*ST" : (level == 1 ? "ST" : "-");
}

// 行业展示名: 优先申万一级名, 缺名回落到代码
const std::string &GetIndustryDisplay(const StockInfo &info) {
  return info.ind_name.empty() ? info.ind_code : info.ind_name;
}

// ============================================================================
// Helper: Check if asset should be shown based on filters
// ============================================================================

bool ShouldShowAsset(
    const AssetItem &asset,
    const TableState &state,
    const StockInfoMap &stock_info) {

  // Convert to lowercase format: sh.600000
  std::string exchange_lower = asset.exchange;
  std::transform(exchange_lower.begin(), exchange_lower.end(),
                 exchange_lower.begin(), ::tolower);
  std::string full_code = exchange_lower + "." + asset.asset_code;
  const StockInfo *info = nullptr;
  auto it = stock_info.find(full_code);
  if (it != stock_info.end()) {
    info = &it->second;
  }

  // Filter: missing only
  if (state.filter_missing_only && asset.get_missing_count() == 0) {
    return false;
  }

  // Filter: no missing (opposite of above)
  if (state.filter_no_missing && asset.get_missing_count() > 0) {
    return false;
  }

  // Filter: ST only (ST 与 *ST 都算)
  if (state.filter_st_only) {
    if (GetStLevel(info) == 0) {
      return false;
    }
  }

  // Filter: listed only (outDate is empty)
  if (state.filter_listed_only) {
    if (!info || !info->outDate.empty()) {
      return false;
    }
  }

  // Filter: board
  if (state.board_filter != BoardType::All) {
    BoardType asset_board = GetBoardType(asset.asset_code);
    if (asset_board != state.board_filter) {
      return false;
    }
  }

  // Filter: industry
  if (!state.industry_filter.empty()) {
    if (!info || info->ind_code != state.industry_filter) {
      return false;
    }
  }

  // Filter: search query
  if (!state.search_query.empty()) {
    if (asset.asset_code.find(state.search_query) != std::string::npos) {
      return true;
    }
    if (info && info->name.find(state.search_query) != std::string::npos) {
      return true;
    }
    return false;
  }

  return true;
}

// ============================================================================
// Helper: Render filter bar
// ============================================================================

void RenderFilterBar(
    TableState &state,
    size_t visible_count,
    size_t total_count,
    const std::vector<AssetItem> &assets,
    const StockInfoMap &stock_info) {

  // Search box
  static char search_buf[256] = "";
  ImGui::SetNextItemWidth(250.0f);
  if (ImGui::InputTextWithHint("##Search", "Search code/name...",
                               search_buf, sizeof(search_buf))) {
    state.search_query = search_buf;
  }

  ImGui::SameLine();
  ImGui::Checkbox("ST", &state.filter_st_only);

  ImGui::SameLine();
  ImGui::Checkbox("Listed", &state.filter_listed_only);

  ImGui::SameLine();
  ImGui::Checkbox("No Missing", &state.filter_no_missing);

  // Board filter dropdown
  ImGui::SameLine();
  ImGui::SetNextItemWidth(100.0f);
  const char *board_names[] = {"All", "Unknown", "沪主板", "深主板", "科创板", "创业板", "北交所"};
  int current_board = static_cast<int>(state.board_filter);
  if (ImGui::Combo("Board##BoardFilter", &current_board, board_names, 7)) {
    state.board_filter = static_cast<BoardType>(current_board);
  }

  // Industry filter - collect all unique industries
  static std::vector<std::pair<std::string, std::string>> industries; // code, name
  static bool industries_cached = false;

  if (!industries_cached) {
    std::map<std::string, std::string> ind_map; // code -> name
    for (const auto &asset : assets) {
      std::string exchange_lower = asset.exchange;
      std::transform(exchange_lower.begin(), exchange_lower.end(),
                     exchange_lower.begin(), ::tolower);
      std::string full_code = exchange_lower + "." + asset.asset_code;
      auto it = stock_info.find(full_code);
      if (it != stock_info.end() && !it->second.ind_code.empty()) {
        ind_map[it->second.ind_code] = it->second.ind_name;
      }
    }
    industries.clear();
    industries.emplace_back("", "All Industries");
    for (const auto &[code, name] : ind_map) {
      industries.emplace_back(code, name.empty() ? code : name + " (" + code + ")");
    }
    industries_cached = true;
  }

  // 预览文本用行业名, 不用裸代码
  const char *ind_preview = "All";
  for (const auto &[code, display] : industries) {
    if (code == state.industry_filter) {
      ind_preview = state.industry_filter.empty() ? "All" : display.c_str();
      break;
    }
  }

  ImGui::SameLine();
  ImGui::SetNextItemWidth(150.0f);
  if (ImGui::BeginCombo("Industry##IndFilter", ind_preview)) {
    for (const auto &[code, display] : industries) {
      bool is_selected = (state.industry_filter == code);
      if (ImGui::Selectable(display.c_str(), is_selected)) {
        state.industry_filter = code;
      }
      if (is_selected) {
        ImGui::SetItemDefaultFocus();
      }
    }
    ImGui::EndCombo();
  }

  // Count & panel toggle
  ImGui::SameLine();
  ImGui::Text("Showing:%zu/%zu", visible_count, total_count);
  ImGui::SameLine();
  if (ImGui::SmallButton(state.show_cross_section_panel ? "Hide" : "Show")) {
    state.show_cross_section_panel = !state.show_cross_section_panel;
  }
}

// ============================================================================
// Helper: Render data table (18 columns)
// ============================================================================

void RenderDataTable(
    const std::vector<AssetItem> &assets,
    const StockInfoMap &stock_info,
    TableState &table_state) {

  ImGuiTableFlags flags = ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                          ImGuiTableFlags_Sortable | ImGuiTableFlags_ScrollX | ImGuiTableFlags_ScrollY |
                          ImGuiTableFlags_Resizable | ImGuiTableFlags_Reorderable |
                          ImGuiTableFlags_SizingFixedFit;

  if (!ImGui::BeginTable("AssetsTable", 17, flags)) {
    return;
  }

  // Setup columns (17 columns) - use auto width (default)
  ImGui::TableSetupColumn("Code", ImGuiTableColumnFlags_DefaultSort | ImGuiTableColumnFlags_PreferSortAscending);
  ImGui::TableSetupColumn("Name");
  ImGui::TableSetupColumn("Exch");
  ImGui::TableSetupColumn("Board");
  ImGui::TableSetupColumn("ST");
  ImGui::TableSetupColumn("DL");
  ImGui::TableSetupColumn("Listed");
  ImGui::TableSetupColumn("Ind");
  ImGui::TableSetupColumn("PE");
  ImGui::TableSetupColumn("PB");
  ImGui::TableSetupColumn("PS");
  ImGui::TableSetupColumn("PCF");
  ImGui::TableSetupColumn("Cap");
  ImGui::TableSetupColumn("Days");
  ImGui::TableSetupColumn("Orders");
  ImGui::TableSetupColumn("Order%");
  ImGui::TableSetupColumn("Miss_O");

  ImGui::TableSetupScrollFreeze(0, 1);

  // Custom headers with tooltips
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  const char *header_labels[] = {"Code", "Name", "Exch", "Board", "ST", "DL", "Listed", "Ind",
                                 "PE", "PB", "PS", "PCF", "Cap", "Days", "Orders", "Order%", "Miss_O"};
  const char *header_tooltips[] = {
      "证券代码 (Code)\n股票的唯一标识符\n格式:6位数字(如600000、000001、688001)",

      "股票名称 (Name)\n公司在交易所的简称 (逐日 PIT)\n来源: cn_stock_instruments 最新交易日\n\n退市股回落到 cn_stock_basic_info 的最后简称",

      "交易所 (Exchange)\nSH = 上海证券交易所 (Shanghai Stock Exchange)\nSZ = 深圳证券交易所 (Shenzhen Stock Exchange)\nBJ = 北京证券交易所 (Beijing Stock Exchange)",

      "板块 (Board)\n市场分类:\n- 沪市主板 (600/601/603/605)\n- 深市主板 (000/001/002/003/004)\n- 科创板 (688/689)\n- 创业板 (300/301/302/309)\n- 北交所 (43/83/87/88/92)",

      "ST股 (Special Treatment)\n来源: cn_stock_status.st_status (逐日)\n\nST  = 特别处理 (连续两年亏损等), 涨跌幅限制 ±5%\n*ST = 退市风险警示 (风险更高一档)\n-   = 正常\n\n与 Name 列同源同日 (简称前缀 ↔ 本列取值 严格一致)",

      "退市 (Delisted)\n是否已退市或处于退市状态\nDL = 已退市\noutDate字段记录退市日期",

      "在市时间 (Listed Duration)\n紧凑格式 年/月/日 — 如 11/02/06 = 11年2个月6天\n\n区间: ipoDate → outDate (退市股截到退市日) 或今天\nhover 单元格可看起止日期与在市总天数\n排序与横截面分析口径为在市总天数",

      "行业 (Industry)\n申万一级行业名称 (industry_level1_name)\n来源: cn_stock_industry_component 最新月度快照\n\nhover 单元格可看行业代码 (ind_code)",

      "滚动市盈率 (PE TTM)\npeTTM = Trailing Twelve Months P/E Ratio\n= 股票收盘价 / 每股盈余TTM\n= (收盘价 x 总股本) / 归属母公司股东净利润TTM\n\nTTM = 过去12个月滚动数据\n反映公司盈利能力,数值越低估值越便宜",

      "市净率 (PB MRQ)\npbMRQ = Price-to-Book Ratio (Most Recent Quarter)\n= 股票收盘价 / 每股净资产\n= 总市值 / (归属母公司股东权益 - 其他权益工具)\n\nMRQ = 最近季度数据\n反映账面价值,通常>1,<1可能破净",

      "滚动市销率 (PS TTM)\npsTTM = Price-to-Sales Ratio (TTM)\n= 股票收盘价 / 每股销售额\n= (收盘价 x 总股本) / 营业总收入TTM\n\n反映每单位营收对应的市值\n适用于尚未盈利但有营收的公司",

      "滚动市现率 (PCF TTM)\npcfNcfTTM = Price-to-Cash-Flow Ratio (TTM)\n= 股票收盘价 / 每股现金流TTM\n= (收盘价 x 总股本) / 现金及现金等价物净增加额TTM\n\n反映现金流创造能力\n比PE更难以通过会计手段操纵",

      "总市值 (Market Cap)\n计算公式:市值(亿元) = 成交额 x 100 / 换手率 / 1亿\n= amount x 100 / turn / 1e8\n\n说明:\n- amount = 成交额(元)\n- turn = 换手率(%)\n- 通过成交额和换手率反推流通市值\n- 换手率 = 成交量/流通股数x100%\n- 流通市值 = 成交额/换手率x100",

      "交易日数 (Trading Days)\n该股票在数据库中有数据的总交易日数\n= date_info.size()\n可用于判断数据完整性",

      "逐笔总数 (Total Orders)\n所有交易日的逐笔记录总数量 (委托+成交合并后)\n= Σ order_count (累加所有日期)\n\n说明:\n- 单位:条记录\n- 显示格式:>1M用M(百万), >1K用K(千)\n- 条数由文件头 original_size 推出, 不来自文件名",

      "逐笔覆盖率 (Order Coverage %)\n= (有编码逐笔数据的天数 / 总交易日数) x 100%\n= orders_encoded_count / total_trading_days x 100%\n\n说明:\n- ≥95%为优秀(绿色), ≥90%为良好(黄色), <90%需关注(红色)\n- 快照不再编码, 所以这就是 L2 数据完整性的唯一口径",

      "逐笔缺失天数 (Missing Order Days)\n在交易日范围内缺失逐笔数据的天数\n= 数据库总交易日数 - 有 .bin 的天数\n\n说明:\n- 数值越大说明数据缺失越严重\n- 需要补充编码或检查archive源文件"};

  for (int col = 0; col < 17; col++) {
    ImGui::TableSetColumnIndex(col);
    ImGui::PushID(col);
    ImGui::TableHeader(header_labels[col]);
    if (ImGui::IsItemHovered()) {
      ImGui::BeginTooltip();
      ImGui::TextUnformatted(header_tooltips[col]);
      ImGui::EndTooltip();
    }
    ImGui::PopID();
  }

  // Build filtered asset list for sorting
  struct AssetRow {
    const AssetItem *asset;
    const StockInfo *info;
    std::string full_code;
  };

  std::vector<AssetRow> filtered_rows;
  filtered_rows.reserve(assets.size());

  for (const auto &asset : assets) {
    if (!ShouldShowAsset(asset, table_state, stock_info))
      continue;

    std::string exchange_lower = asset.exchange;
    std::transform(exchange_lower.begin(), exchange_lower.end(),
                   exchange_lower.begin(), ::tolower);
    std::string full_code = exchange_lower + "." + asset.asset_code;
    const StockInfo *info = nullptr;
    auto it = stock_info.find(full_code);
    if (it != stock_info.end()) {
      info = &it->second;
    }
    filtered_rows.push_back({&asset, info, full_code});
  }

  // Safe string to float conversion with extra safety for sorting
  auto safe_stod = [](const std::string &s, float default_val = -1e9) -> float {
    if (s.empty())
      return default_val;
    try {
      float val = std::stod(s);
      // Extra safety: check for NaN explicitly before isfinite
      if (val != val)
        return default_val; // NaN check
      if (!std::isfinite(val))
        return default_val;
      return val;
    } catch (const std::invalid_argument &) {
      return default_val;
    } catch (const std::out_of_range &) {
      return default_val;
    } catch (...) {
      return default_val;
    }
  };

  // Apply sorting (restore from table_state if needed)
  if (ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs()) {
    if (sort_specs->SpecsDirty || table_state.sort_column >= 0) {
      int col = table_state.sort_column;
      bool ascending = table_state.sort_ascending;

      // Update from ImGui if dirty
      if (sort_specs->SpecsDirty && sort_specs->SpecsCount > 0) {
        const auto &spec = sort_specs->Specs[0];
        col = spec.ColumnIndex;
        ascending = spec.SortDirection == ImGuiSortDirection_Ascending;
        table_state.sort_column = col;
        table_state.sort_ascending = ascending;
      }

      if (col >= 0) {
        std::stable_sort(filtered_rows.begin(), filtered_rows.end(),
                         [col, ascending, &safe_stod](const AssetRow &a, const AssetRow &b) -> bool {
                           try {
                             bool result = false;

                             switch (col) {
                             case 0:
                               result = a.asset->asset_code < b.asset->asset_code;
                               break;  // Code
                             case 1: { // Name
                               std::string name_a = a.info && !a.info->name.empty() ? a.info->name : a.asset->asset_code;
                               std::string name_b = b.info && !b.info->name.empty() ? b.info->name : b.asset->asset_code;
                               result = name_a < name_b;
                               break;
                             }
                             case 2:
                               result = a.asset->exchange < b.asset->exchange;
                               break; // Exchange
                             case 3:
                               result = (int)GetBoardType(a.asset->asset_code) < (int)GetBoardType(b.asset->asset_code);
                               break; // Board
                             case 4:  // ST: 正常 < ST < *ST
                               result = GetStLevel(a.info) < GetStLevel(b.info);
                               break;
                             case 5: { // DL (Delisted)
                               bool a_dl = a.info && a.info->outDate != "" && a.info->outDate != "0";
                               bool b_dl = b.info && b.info->outDate != "" && b.info->outDate != "0";
                               result = a_dl < b_dl;
                               break;
                             }
                             case 6: { // Listed: 在市总天数
                               int a_days = a.info ? CalculateListedSpan(*a.info).total_days : 0;
                               int b_days = b.info ? CalculateListedSpan(*b.info).total_days : 0;
                               result = a_days < b_days;
                               break;
                             }
                             case 7: { // Industry (按展示名排, 与列内容一致)
                               std::string a_ind = a.info ? GetIndustryDisplay(*a.info) : "";
                               std::string b_ind = b.info ? GetIndustryDisplay(*b.info) : "";
                               result = a_ind < b_ind;
                               break;
                             }
                             case 8: { // PE
                               // Valuation sorting: positive (cheap to expensive) -> negative (high risk) -> invalid
                               float a_val = a.info ? safe_stod(a.info->peTTM) : -1e9;
                               float b_val = b.info ? safe_stod(b.info->peTTM) : -1e9;
                               int a_tier = (a_val > 0) ? 0 : (a_val < 0) ? 1
                                                                          : 2;
                               int b_tier = (b_val > 0) ? 0 : (b_val < 0) ? 1
                                                                          : 2;
                               result = (a_tier != b_tier) ? (a_tier < b_tier) : (a_val < b_val);
                               break;
                             }
                             case 9: { // PB
                               // Valuation sorting: positive (cheap to expensive) -> negative (high risk) -> invalid
                               float a_val = a.info ? safe_stod(a.info->pbMRQ) : -1e9;
                               float b_val = b.info ? safe_stod(b.info->pbMRQ) : -1e9;
                               int a_tier = (a_val > 0) ? 0 : (a_val < 0) ? 1
                                                                          : 2;
                               int b_tier = (b_val > 0) ? 0 : (b_val < 0) ? 1
                                                                          : 2;
                               result = (a_tier != b_tier) ? (a_tier < b_tier) : (a_val < b_val);
                               break;
                             }
                             case 10: { // PS
                               // Valuation sorting: positive (cheap to expensive) -> negative (high risk) -> invalid
                               float a_val = a.info ? safe_stod(a.info->psTTM) : -1e9;
                               float b_val = b.info ? safe_stod(b.info->psTTM) : -1e9;
                               int a_tier = (a_val > 0) ? 0 : (a_val < 0) ? 1
                                                                          : 2;
                               int b_tier = (b_val > 0) ? 0 : (b_val < 0) ? 1
                                                                          : 2;
                               result = (a_tier != b_tier) ? (a_tier < b_tier) : (a_val < b_val);
                               break;
                             }
                             case 11: { // PCF
                               // Valuation sorting: positive (cheap to expensive) -> negative (high risk) -> invalid
                               float a_val = a.info ? safe_stod(a.info->pcfNcfTTM) : -1e9;
                               float b_val = b.info ? safe_stod(b.info->pcfNcfTTM) : -1e9;
                               int a_tier = (a_val > 0) ? 0 : (a_val < 0) ? 1
                                                                          : 2;
                               int b_tier = (b_val > 0) ? 0 : (b_val < 0) ? 1
                                                                          : 2;
                               result = (a_tier != b_tier) ? (a_tier < b_tier) : (a_val < b_val);
                               break;
                             }
                             case 12: { // Market Cap
                               float a_cap = a.info ? CalculateMarketCap(*a.info) : 0;
                               float b_cap = b.info ? CalculateMarketCap(*b.info) : 0;
                               result = a_cap < b_cap;
                               break;
                             }
                             case 13:
                               result = a.asset->get_total_trading_days() < b.asset->get_total_trading_days();
                               break; // Days
                             case 14:
                               result = a.asset->get_total_order_count() < b.asset->get_total_order_count();
                               break;   // Orders
                             case 15: { // Order%
                               float a_pct = a.asset->get_total_trading_days() > 0 ? (float)a.asset->get_orders_encoded_count() / a.asset->get_total_trading_days() : 0;
                               float b_pct = b.asset->get_total_trading_days() > 0 ? (float)b.asset->get_orders_encoded_count() / b.asset->get_total_trading_days() : 0;
                               result = a_pct < b_pct;
                               break;
                             }
                             case 16: { // Miss_O
                               size_t a_miss = a.asset->get_total_trading_days() - a.asset->get_orders_encoded_count();
                               size_t b_miss = b.asset->get_total_trading_days() - b.asset->get_orders_encoded_count();
                               result = a_miss < b_miss;
                               break;
                             }
                             }

                             return ascending ? result : !result;
                           } catch (...) {
                             // If comparison fails, maintain consistent ordering by comparing addresses
                             return &a < &b;
                           }
                         });
      }
      sort_specs->SpecsDirty = false;
    }
  }

  // Helper lambda to handle column highlight and click (left-click to trigger analysis)
  auto handle_column_click = [&table_state](int col_idx) {
    if (ImGui::IsItemClicked(ImGuiMouseButton_Left)) {
      if (table_state.selected_column_idx == col_idx) {
        table_state.selected_column_idx = -1;
      } else {
        table_state.selected_column_idx = col_idx;
        table_state.show_cross_section_panel = true;
      }
    }
  };

  // Get hovered column for highlight
  int hovered_col = ImGui::TableGetHoveredColumn();

  // Render rows
  int row_idx = 0;
  for (const auto &row : filtered_rows) {
    const AssetItem &asset = *row.asset;
    const StockInfo *info = row.info;

    ImGui::TableNextRow();
    ImGui::PushID(row_idx);
    bool is_row_selected = (table_state.selected_asset_idx == row_idx);

    // Col 0: Code
    ImGui::TableSetColumnIndex(0);
    if (hovered_col == 0) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.26f, 0.59f, 0.98f, 0.35f)));
    }
    // Use Text instead of Selectable to allow column click
    if (is_row_selected) {
      ImGui::TextColored(ImVec4(0.3f, 0.8f, 1.0f, 1.0f), "%s", asset.asset_code.c_str());
    } else {
      ImGui::Text("%s", asset.asset_code.c_str());
    }
    handle_column_click(0);

    // Col 1: Name
    ImGui::TableSetColumnIndex(1);
    if (hovered_col == 1) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->name.empty()) {
      ImGui::Text("%s", info->name.c_str());
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(1);

    // Col 2: Exchange
    ImGui::TableSetColumnIndex(2);
    if (hovered_col == 2) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    ImGui::TextColored(asset.exchange == "SH"   ? COLOR_SH
                       : asset.exchange == "SZ" ? COLOR_SZ
                                                : COLOR_BJ,
                       "%s", asset.exchange.c_str());
    handle_column_click(2);

    // Col 3: Board
    ImGui::TableSetColumnIndex(3);
    if (hovered_col == 3) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    BoardType board = GetBoardType(asset.asset_code);
    ImGui::Text("%s", GetBoardName(board));
    handle_column_click(3);

    // Col 4: ST
    ImGui::TableSetColumnIndex(4);
    if (hovered_col == 4) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    int st_level = GetStLevel(info);
    if (st_level > 0) {
      ImGui::TextColored(COLOR_RED, "%s", GetStLabel(st_level));
    } else {
      ImGui::Text("-");
    }
    handle_column_click(4);

    // Col 5: DL (Delisted - 退市)
    ImGui::TableSetColumnIndex(5);
    if (hovered_col == 5) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->outDate.empty() && info->outDate != "0") {
      ImGui::TextColored(COLOR_GRAY, "DL");
      if (ImGui::IsItemHovered()) {
        ImGui::SetTooltip("Delisted: %s", info->outDate.c_str());
      }
    } else {
      ImGui::Text("-");
    }
    handle_column_click(5);

    // Col 6: Listed (days)
    ImGui::TableSetColumnIndex(6);
    if (hovered_col == 6) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    ListedSpan span = info ? CalculateListedSpan(*info) : ListedSpan{};
    if (span.valid) {
      ImGui::Text("%02d/%02d/%02d", span.years, span.months, span.days);
      if (ImGui::IsItemHovered()) {
        ImGui::SetTooltip("上市 %s → %s\n在市 %d 年 %d 月 %d 天 (共 %d 天)",
                          info->ipoDate.c_str(),
                          info->outDate.empty() ? "至今" : info->outDate.c_str(),
                          span.years, span.months, span.days, span.total_days);
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(6);

    // Col 7: Industry
    ImGui::TableSetColumnIndex(7);
    if (hovered_col == 7) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !GetIndustryDisplay(*info).empty()) {
      ImGui::Text("%s", GetIndustryDisplay(*info).c_str());
      if (ImGui::IsItemHovered() && !info->ind_code.empty()) {
        ImGui::SetTooltip("%s", info->ind_code.c_str());
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(7);

    // Col 8: PE(TTM)
    ImGui::TableSetColumnIndex(8);
    if (hovered_col == 8) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->peTTM.empty()) {
      try {
        float pe = std::stod(info->peTTM);
        if (std::isfinite(pe)) {
          ImGui::Text("%.1f", pe);
        } else {
          ImGui::TextColored(COLOR_GRAY, "-");
        }
      } catch (...) {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(8);

    // Col 9: PB(MRQ)
    ImGui::TableSetColumnIndex(9);
    if (hovered_col == 9) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->pbMRQ.empty()) {
      try {
        float pb = std::stod(info->pbMRQ);
        if (std::isfinite(pb)) {
          ImGui::Text("%.2f", pb);
        } else {
          ImGui::TextColored(COLOR_GRAY, "-");
        }
      } catch (...) {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(9);

    // Col 10: PS(TTM)
    ImGui::TableSetColumnIndex(10);
    if (hovered_col == 10) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->psTTM.empty()) {
      try {
        float ps = std::stod(info->psTTM);
        if (std::isfinite(ps)) {
          ImGui::Text("%.2f", ps);
        } else {
          ImGui::TextColored(COLOR_GRAY, "-");
        }
      } catch (...) {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(10);

    // Col 11: PCF
    ImGui::TableSetColumnIndex(11);
    if (hovered_col == 11) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info && !info->pcfNcfTTM.empty()) {
      try {
        float pcf = std::stod(info->pcfNcfTTM);
        if (std::isfinite(pcf)) {
          ImGui::Text("%.1f", pcf);
        } else {
          ImGui::TextColored(COLOR_GRAY, "-");
        }
      } catch (...) {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(11);

    // Col 12: Market Cap (billion yuan)
    ImGui::TableSetColumnIndex(12);
    if (hovered_col == 12) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info) {
      float cap = CalculateMarketCap(*info);
      if (cap > 0) {
        ImGui::Text("%.1f", cap);
      } else {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(12);

    // Col 13: Trading Days
    ImGui::TableSetColumnIndex(13);
    if (hovered_col == 13) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t total_days = asset.get_total_trading_days();
    ImGui::Text("%zu", total_days);
    handle_column_click(13);

    // Col 14: Total Orders
    ImGui::TableSetColumnIndex(14);
    if (hovered_col == 14) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t total_orders = asset.get_total_order_count();
    if (total_orders > 1000000) {
      ImGui::Text("%.2fM", total_orders / 1000000.0);
    } else if (total_orders > 1000) {
      ImGui::Text("%.1fK", total_orders / 1000.0);
    } else {
      ImGui::Text("%zu", total_orders);
    }
    handle_column_click(14);

    // Col 15: Orders Encoded %
    ImGui::TableSetColumnIndex(15);
    if (hovered_col == 15) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t ord_encoded = asset.get_orders_encoded_count();
    float ord_pct = total_days > 0 ? (float)ord_encoded / total_days * 100.0 : 0.0;
    ImVec4 ord_color = ord_pct >= 95.0 ? COLOR_GREEN : (ord_pct >= 90.0 ? COLOR_YELLOW : COLOR_RED);
    ImGui::TextColored(ord_color, "%.1f%%", ord_pct);
    handle_column_click(15);

    // Col 16: Missing Order Days
    ImGui::TableSetColumnIndex(16);
    if (hovered_col == 16) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t ord_missing = total_days - ord_encoded;
    ImGui::TextColored(ord_missing > 0 ? COLOR_YELLOW : COLOR_GREEN, "%zu", ord_missing);
    handle_column_click(16);

    ImGui::PopID();
    row_idx++;
  }

  ImGui::EndTable();
}

// ============================================================================
// Forward declarations
// ============================================================================

static void RenderNumericAnalysis(
    const std::vector<AssetItem> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name);

static void RenderCategoricalAnalysis(
    const std::vector<AssetItem> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name);

// ============================================================================
// Helper: Determine column data type
// ============================================================================

static ColumnDataType GetColumnDataType(int col_idx) {
  // Categorical: Board(3), ST(4), DL(5), Industry(7)
  if (col_idx == 3 || col_idx == 4 || col_idx == 5 || col_idx == 7) {
    return ColumnDataType::Categorical;
  }
  // Numeric: Listed Days(6), PE(8), PB(9), PS(10), PCF(11), Market Cap(12),
  //          Trading Days(13), Total Orders(14), Order%(15), Miss_O(16)
  if (col_idx >= 6 && col_idx <= 16) {
    return ColumnDataType::Numeric;
  }
  // Others (Code, Name, Exchange) not analyzable
  return ColumnDataType::Categorical; // Default
}

// ============================================================================
// Helper: Render cross-section analysis panel
// ============================================================================

void RenderCrossSectionPanel(
    const std::vector<AssetItem> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state) {

  if (table_state.selected_column_idx < 0) {
    ImGui::TextWrapped("Click on any cell to view cross-section analysis.");
    return;
  }

  // Column names for display
  const char *col_names[] = {
      "Code", "Name", "Exchange", "Board", "ST", "DL", "Listed Days (在市总天数)", "Industry",
      "PE(TTM)", "PB(MRQ)", "PS(TTM)", "PCF", "Market Cap", "Trading Days",
      "Total Orders", "Order %", "Missing Order Days"};

  int col_idx = table_state.selected_column_idx;
  if (col_idx >= 17) {
    ImGui::Text("Invalid column index");
    return;
  }

  ImGui::Text("Column: %s", col_names[col_idx]);
  ImGui::Separator();

  ColumnDataType data_type = GetColumnDataType(col_idx);

  if (data_type == ColumnDataType::Categorical) {
    RenderCategoricalAnalysis(assets, stock_info, table_state, col_idx, col_names[col_idx]);
  } else {
    RenderNumericAnalysis(assets, stock_info, table_state, col_idx, col_names[col_idx]);
  }
}

// ============================================================================
// Numeric Column Analysis
// ============================================================================

static void RenderNumericAnalysis(
    const std::vector<AssetItem> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name) {
  (void)col_name; // Unused

  // Extract numeric data (only for filtered assets)
  std::vector<std::string> names;
  std::vector<float> values;
  std::vector<std::string> codes;

  for (const auto &asset : assets) {
    if (!ShouldShowAsset(asset, table_state, stock_info))
      continue;

    std::string exchange_lower = asset.exchange;
    std::transform(exchange_lower.begin(), exchange_lower.end(),
                   exchange_lower.begin(), ::tolower);
    std::string full_code = exchange_lower + "." + asset.asset_code;
    const StockInfo *info = nullptr;
    auto it = stock_info.find(full_code);
    if (it != stock_info.end()) {
      info = &it->second;
    }

    std::string display_name = info && !info->name.empty() ? info->name : asset.asset_code;
    float value = std::numeric_limits<float>::quiet_NaN();
    bool is_valid = false;

    switch (col_idx) {
    case 6: // Listed (在市总天数)
      if (info) {
        ListedSpan span = CalculateListedSpan(*info);
        value = span.total_days;
        is_valid = span.valid;
      }
      break;
    case 8: // PE
      if (info && !info->peTTM.empty()) {
        try {
          value = std::stod(info->peTTM);
          is_valid = std::isfinite(value);
        } catch (...) {
        }
      }
      break;
    case 9: // PB
      if (info && !info->pbMRQ.empty()) {
        try {
          value = std::stod(info->pbMRQ);
          is_valid = std::isfinite(value);
        } catch (...) {
        }
      }
      break;
    case 10: // PS
      if (info && !info->psTTM.empty()) {
        try {
          value = std::stod(info->psTTM);
          is_valid = std::isfinite(value);
        } catch (...) {
        }
      }
      break;
    case 11: // PCF
      if (info && !info->pcfNcfTTM.empty()) {
        try {
          value = std::stod(info->pcfNcfTTM);
          is_valid = std::isfinite(value);
        } catch (...) {
        }
      }
      break;
    case 12: // Market Cap
      if (info) {
        value = CalculateMarketCap(*info);
        is_valid = (value > 0);
      }
      break;
    case 13: // Trading Days
      value = asset.get_total_trading_days();
      is_valid = true;
      break;
    case 14: // Total Orders
      value = asset.get_total_order_count();
      is_valid = true;
      break;
    case 15: // Order %
      value = asset.get_total_trading_days() > 0 ? (float)asset.get_orders_encoded_count() / asset.get_total_trading_days() * 100.0 : 0.0;
      is_valid = true;
      break;
    case 16: // Miss_O
      value = asset.get_total_trading_days() - asset.get_orders_encoded_count();
      is_valid = true;
      break;
    default:
      break;
    }

    if (is_valid) {
      names.push_back(display_name);
      values.push_back(value);
      codes.push_back(asset.asset_code);
    }
  }

  if (values.empty()) {
    ImGui::Text("No valid data");
    return;
  }

  // === 1. Board Statistics Table (Compact) ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Board Statistics");
  auto board_stats = GroupNumericByBoard(codes, values);

  if (ImGui::BeginTable("BoardStatsTable", 5, ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit)) {
    ImGui::TableSetupColumn("Board");
    ImGui::TableSetupColumn("Mean");
    ImGui::TableSetupColumn("Median");
    ImGui::TableSetupColumn("StdDev");
    ImGui::TableSetupColumn("Count");
    ImGui::TableHeadersRow();

    for (const auto &bs : board_stats) {
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("%s", bs.board_name.c_str());
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("%.2f", bs.mean);
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("%.2f", bs.median);
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("%.2f", bs.std_dev);
      ImGui::TableSetColumnIndex(4);
      ImGui::Text("%zu", bs.count);
    }
    ImGui::EndTable();
  }

  ImGui::Spacing();

  // === 2. Distribution Plot (Remove top/bottom 5% outliers) ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Distribution (Outliers Removed)");
  auto filtered_values = RemoveOutliers(values, 5.0);

  if (!filtered_values.empty()) {
    const int num_bins = 100;

    // Calculate statistics for Gaussian fit
    auto minmax = std::minmax_element(filtered_values.begin(), filtered_values.end());
    float min_val = *minmax.first;
    float max_val = *minmax.second;
    float range = max_val - min_val;
    float bin_width = range / num_bins;

    float sum = std::accumulate(filtered_values.begin(), filtered_values.end(), 0.0);
    float mean = sum / filtered_values.size();

    float sq_sum = 0.0;
    for (float v : filtered_values) {
      sq_sum += (v - mean) * (v - mean);
    }
    float std_dev = std::sqrt(sq_sum / filtered_values.size());

    // Calculate histogram bins
    std::vector<float> hist_bins(num_bins, 0.0);
    for (float v : filtered_values) {
      int bin_idx = static_cast<int>((v - min_val) / bin_width);
      if (bin_idx >= num_bins)
        bin_idx = num_bins - 1;
      if (bin_idx < 0)
        bin_idx = 0;
      hist_bins[bin_idx] += 1.0;
    }

    // Normalize histogram to density
    float n = filtered_values.size();
    std::vector<float> hist_density(num_bins);
    for (int i = 0; i < num_bins; ++i) {
      hist_density[i] = hist_bins[i] / (n * bin_width);
    }

    // Prepare X axis positions for histogram
    std::vector<float> x_positions(num_bins);
    for (int i = 0; i < num_bins; ++i) {
      x_positions[i] = min_val + (i + 0.5) * bin_width;
    }

    // Generate fitted Gaussian PDF (200 points for smooth curve)
    const int pdf_points = 200;
    std::vector<float> pdf_x(pdf_points);
    std::vector<float> pdf_y(pdf_points);
    const float pi = 3.14159265358979323846;

    for (int i = 0; i < pdf_points; ++i) {
      float x = min_val + (i * range) / (pdf_points - 1);
      pdf_x[i] = x;
      float z = (x - mean) / std_dev;
      pdf_y[i] = (1.0 / (std_dev * std::sqrt(2.0 * pi))) * std::exp(-0.5 * z * z);
    }

    // Calculate empirical CDF
    std::vector<float> sorted_vals = filtered_values;
    std::sort(sorted_vals.begin(), sorted_vals.end());
    std::vector<float> cdf_x(pdf_points);
    std::vector<float> cdf_y(pdf_points);

    for (int i = 0; i < pdf_points; ++i) {
      float x = min_val + (i * range) / (pdf_points - 1);
      cdf_x[i] = x;
      auto it = std::upper_bound(sorted_vals.begin(), sorted_vals.end(), x);
      cdf_y[i] = (float)std::distance(sorted_vals.begin(), it) / sorted_vals.size();
    }

    // Find max density for Y axis
    float max_hist_density = *std::max_element(hist_density.begin(), hist_density.end());
    float max_pdf = *std::max_element(pdf_y.begin(), pdf_y.end());
    float y_max = std::max(max_hist_density, max_pdf) * 1.15;

    if (ImPlot::BeginPlot("##Distribution", ImVec2(-1, 350))) {
      ImPlot::SetupAxes("Value", "Density");
      ImPlot::SetupAxisLimits(ImAxis_X1, min_val, max_val, ImPlotCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, y_max, ImPlotCond_Always);
      ImPlot::SetupAxis(ImAxis_Y2, "CDF", ImPlotAxisFlags_AuxDefault);
      ImPlot::SetupAxisLimits(ImAxis_Y2, 0, 1.05, ImPlotCond_Always);

      // Plot histogram bars
      ImPlot::SetNextFillStyle(ImVec4(0.5f, 0.7f, 1.0f, 0.4f));
      ImPlot::PlotBars("Histogram", x_positions.data(), hist_density.data(), num_bins, bin_width * 0.9);

      // Plot fitted Gaussian PDF
      ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.3f, 0.3f, 1.0f), 2.5f);
      ImPlot::PlotLine("Fitted PDF", pdf_x.data(), pdf_y.data(), pdf_points);

      // Plot empirical CDF on secondary Y axis
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
      ImPlot::SetNextLineStyle(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), 2.5f);
      ImPlot::PlotLine("Empirical CDF", cdf_x.data(), cdf_y.data(), pdf_points);

      ImPlot::EndPlot();
    }
  }

  ImGui::Spacing();

  // === 3. Rankings (Top 10 / Bottom 10) ===
  float half_width = ImGui::GetContentRegionAvail().x * 0.48f;

  // Top 10
  ImGui::BeginChild("Top10", ImVec2(half_width, 250), true);
  ImGui::TextColored(ImVec4(0.4f, 1.0f, 0.4f, 1.0f), "Top 10");
  auto top10 = GetTopN(names, values, 10, true);
  for (size_t i = 0; i < top10.size(); ++i) {
    ImGui::Text("%zu. %s: %.2f", i + 1, top10[i].first.c_str(), top10[i].second);
  }
  ImGui::EndChild();

  ImGui::SameLine();

  // Bottom 10
  ImGui::BeginChild("Bottom10", ImVec2(half_width, 250), true);
  ImGui::TextColored(ImVec4(1.0f, 0.4f, 0.4f, 1.0f), "Bottom 10");
  auto bottom10 = GetTopN(names, values, 10, false);
  for (size_t i = 0; i < bottom10.size(); ++i) {
    ImGui::Text("%zu. %s: %.2f", i + 1, bottom10[i].first.c_str(), bottom10[i].second);
  }
  ImGui::EndChild();
}

// ============================================================================
// Categorical Column Analysis
// ============================================================================

static void RenderCategoricalAnalysis(
    const std::vector<AssetItem> &assets,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name) {
  (void)col_name; // Unused

  // Extract categorical data
  std::vector<std::string> categories;
  std::vector<std::string> codes;

  for (const auto &asset : assets) {
    if (!ShouldShowAsset(asset, table_state, stock_info))
      continue;

    std::string exchange_lower = asset.exchange;
    std::transform(exchange_lower.begin(), exchange_lower.end(),
                   exchange_lower.begin(), ::tolower);
    std::string full_code = exchange_lower + "." + asset.asset_code;
    const StockInfo *info = nullptr;
    auto it = stock_info.find(full_code);
    if (it != stock_info.end()) {
      info = &it->second;
    }

    std::string category;
    switch (col_idx) {
    case 3: // Board
      category = GetBoardName(GetBoardType(asset.asset_code));
      break;
    case 4: { // ST
      int level = GetStLevel(info);
      category = level == 2 ? "*ST" : (level == 1 ? "ST" : "Normal");
      break;
    }
    case 5: // DL
      category = (info && !info->outDate.empty()) ? "Delisted" : "Active";
      break;
    case 7: // Industry
      category = (info && !GetIndustryDisplay(*info).empty())
                     ? GetIndustryDisplay(*info)
                     : "Unknown";
      break;
    default:
      break;
    }

    if (!category.empty()) {
      categories.push_back(category);
      codes.push_back(asset.asset_code);
    }
  }

  if (categories.empty()) {
    ImGui::Text("No valid data");
    return;
  }

  // === 1. Overall Pie Chart ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Overall Distribution");
  auto overall_counts = CountCategories(categories);

  if (!overall_counts.empty() && ImPlot::BeginPlot("##OverallPie", ImVec2(-1, 250))) {
    std::vector<const char *> labels;
    std::vector<float> counts;
    for (const auto &cc : overall_counts) {
      labels.push_back(cc.label.c_str());
      counts.push_back((float)cc.count);
    }
    ImPlot::PlotPieChart(labels.data(), counts.data(), (int)counts.size(), 0.5, 0.5, 0.4);
    ImPlot::EndPlot();
  }

  ImGui::Spacing();

  // === 2. Board Breakdown Pie Charts (Multi-column) ===
  ImGui::TextColored(ImVec4(0.4f, 0.7f, 1.0f, 1.0f), "Board Breakdown");
  auto board_breakdown = GroupCategoricalByBoard(codes, categories);

  int charts_per_row = 2;
  float chart_width = ImGui::GetContentRegionAvail().x / charts_per_row - 10;

  for (size_t i = 0; i < board_breakdown.size(); ++i) {
    const auto &breakdown = board_breakdown[i];

    if (i % charts_per_row != 0) {
      ImGui::SameLine();
    }

    ImGui::BeginChild(("BoardPie_" + std::to_string(i)).c_str(), ImVec2(chart_width, 220), true);
    ImGui::Text("%s", breakdown.board_name.c_str());

    if (!breakdown.categories.empty() && ImPlot::BeginPlot("##BoardPie", ImVec2(-1, 180))) {
      std::vector<const char *> labels;
      std::vector<float> counts;
      for (const auto &cc : breakdown.categories) {
        labels.push_back(cc.label.c_str());
        counts.push_back((float)cc.count);
      }
      ImPlot::PlotPieChart(labels.data(), counts.data(), (int)counts.size(), 0.5, 0.5, 0.35);
      ImPlot::EndPlot();
    }

    ImGui::EndChild();
  }
}

// ============================================================================
// Main TabTable Render Function
// ============================================================================

void RenderTabTable(
    const std::vector<AssetItem> &assets,
    const StockInfoMap &stock_info,
    TableState &table_state) {

  // Count visible assets
  size_t visible_count = 0;
  for (const auto &asset : assets) {
    if (ShouldShowAsset(asset, table_state, stock_info)) {
      visible_count++;
    }
  }

  // Render filter bar
  RenderFilterBar(table_state, visible_count, assets.size(), assets, stock_info);
  ImGui::Spacing();

  // Get window dimensions
  float window_width = ImGui::GetContentRegionAvail().x;
  float window_height = ImGui::GetContentRegionAvail().y;

  // Calculate left table width
  float left_width = table_state.show_cross_section_panel ? window_width * table_state.table_split_ratio : window_width;

  // Left: Data Table
  ImGui::BeginChild("LeftTable", ImVec2(left_width, window_height), true,
                    ImGuiWindowFlags_HorizontalScrollbar);
  RenderDataTable(assets, stock_info, table_state);
  ImGui::EndChild();

  // Right: Cross-section Analysis Panel
  if (table_state.show_cross_section_panel) {
    ImGui::SameLine();
    ImGui::BeginChild("RightPanel", ImVec2(0, window_height), true);
    RenderCrossSectionPanel(assets, stock_info, table_state);
    ImGui::EndChild();
  }
}

} // namespace GUI::Database
