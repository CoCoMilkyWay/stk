// Tab Table - L2 Database Asset Table View Implementation
// 18-column table with enhanced filtering and cross-section analysis panel

#include "gui/task_database/ui/TabTable.hpp"
#include "gui/task_database/models/SharedTypes.hpp"
#include "gui/task_database/ui/CrossSectionAnalysis.hpp"
#include "imgui.h"
#include "implot.h"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>

namespace GUI::Database {

// Color constants
constexpr ImVec4 COLOR_SH = ImVec4(0.0f, 0.4f, 0.8f, 1.0f);
constexpr ImVec4 COLOR_SZ = ImVec4(0.0f, 0.6f, 0.5f, 1.0f);
constexpr ImVec4 COLOR_BJ = ImVec4(0.8f, 0.5f, 0.1f, 1.0f);
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
// Helper: 总市值 [亿元] — FundamentalService 已按 close × total_shares 算好
// ============================================================================

float CalculateMarketCap(const StockInfo &info) {
  if (info.mcap.empty())
    return 0.0;
  return std::stod(info.mcap);
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
// Helper: "sh.600000" — stock_info 的键
// ============================================================================

std::string MakeStockKey(const AssetItem &asset) {
  std::string exchange_lower = asset.exchange;
  std::transform(exchange_lower.begin(), exchange_lower.end(),
                 exchange_lower.begin(), ::tolower);
  return exchange_lower + "." + asset.asset_code;
}

const StockInfo *FindStockInfo(const AssetItem &asset, const StockInfoMap &stock_info) {
  auto it = stock_info.find(MakeStockKey(asset));
  return it != stock_info.end() ? &it->second : nullptr;
}

// ============================================================================
// Helper: Check if asset should be shown based on filters
// ============================================================================

bool ShouldShowAsset(
    const AssetItem &asset,
    const StockInfo *info,
    const TableState &state) {

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
// 视图缓存: 过滤 + 排序
// ============================================================================

// 估值列 (PE/PB/PS/PCF) 的统一序: 便宜 → 贵 → 亏得少 → 亏得多
//   0.1 → 10000 → -10000 → -0.1
// 负值的绝对值越大 = 亏损相对市值越小, 所以负数区间照样升序; 0 (无意义) 垫底.
// 排序与截面着色共用这一个口径.
bool ValuationLess(float va, float vb) {
  const int ta = (va > 0) ? 0 : (va < 0) ? 1
                                         : 2;
  const int tb = (vb > 0) ? 0 : (vb < 0) ? 1
                                         : 2;
  return (ta != tb) ? (ta < tb) : (va < vb);
}

// 基本面字段是 parquet 里的字符串, 可能是空/NaN/超范围 — 转不出来就归到
// 排序的最低档 (default_val), 不是错误处理
float SafeStod(const std::string &s, float default_val = -1e9) {
  if (s.empty())
    return default_val;
  try {
    float val = std::stod(s);
    if (val != val) // NaN
      return default_val;
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
}

// ============================================================================
// 截面着色: 过滤后 pool 内的名次分位 → 红 → 黄 → 绿
// ============================================================================

// 该列该行的截面取值; 返回 false = 没有可比的值 (空/NaN/无基本面), 不着色
bool GetCrossSectionValue(int col, const StockInfo *info, float &out) {
  if (!info)
    return false;

  // 与 SafeStod 同样的判据, 只是把"转不出来"报成 false 而不是垫底值 ——
  // 缺值的行不参与截面, 不能挤占最差那一档
  auto parse_finite = [&out](const std::string &s) {
    if (s.empty())
      return false;
    try {
      const float v = std::stof(s);
      if (!std::isfinite(v))
        return false;
      out = v;
      return true;
    } catch (...) {
      return false;
    }
  };

  switch (col) {
  case 6: { // Listed: 在市总天数
    const ListedSpan span = CalculateListedSpan(*info);
    out = static_cast<float>(span.total_days);
    return span.valid;
  }
  case 8:
    return parse_finite(info->peTTM);
  case 9:
    return parse_finite(info->pbMRQ);
  case 10:
    return parse_finite(info->psTTM);
  case 11:
    return parse_finite(info->pcfNcfTTM);
  case 12:
    return parse_finite(info->dy1y);
  case 13:
    return parse_finite(info->dy3y);
  case 14:
    return parse_finite(info->dy5y);
  case 15: // Cap: 0 = 缺收盘价或股本, 不是"市值为零"
    out = CalculateMarketCap(*info);
    return out > 0;
  default:
    return false;
  }
}

// 估值列越低越好, 其余 (Listed/DY/Cap) 越高越好
bool IsValuationColumn(int col) { return col >= 8 && col <= 11; }

// 分位 → 颜色. p = 1 最好 (绿), p = 0 最差 (红), 中间过黄.
// 直接过渡红→绿会在中段压成暗棕, 所以走黄这个中继.
ImVec4 RampColorVec4(float p) {
  constexpr ImVec4 kWorst = ImVec4(0.95f, 0.35f, 0.30f, 1.0f);
  constexpr ImVec4 kMid = ImVec4(0.95f, 0.90f, 0.35f, 1.0f);
  constexpr ImVec4 kBest = ImVec4(0.40f, 0.95f, 0.45f, 1.0f);

  p = std::clamp(p, 0.0f, 1.0f);
  const ImVec4 &a = (p < 0.5f) ? kWorst : kMid;
  const ImVec4 &b = (p < 0.5f) ? kMid : kBest;
  const float t = (p < 0.5f) ? p * 2.0f : (p - 0.5f) * 2.0f;
  return ImVec4(a.x + (b.x - a.x) * t,
                a.y + (b.y - a.y) * t,
                a.z + (b.z - a.z) * t,
                1.0f);
}

// 0 是 cs_colors 里"无有效值"的哨兵, 所以这里必须给足 alpha (不能返回 0)
ImU32 RampColor(float p) { return ImGui::GetColorU32(RampColorVec4(p)); }

// 用名次分位而不是数值归一化: 估值和市值都是重尾分布, 按 (v-min)/(max-min)
// 染色的话几个极端值就把其余几千个标的全压成同一个颜色.
void RebuildCrossSectionColors(TableView &view,
                               const std::vector<const StockInfo *> &infos) {
  const size_t n = view.rows.size();

  std::vector<std::pair<float, size_t>> keys; // 取值, 在 rows 里的位置
  for (int slot = 0; slot < kColoredColumnCount; ++slot) {
    const int col = kColoredColumns[slot];
    std::vector<uint32_t> &colors = view.cs_colors[slot];
    colors.assign(n, 0);

    keys.clear();
    keys.reserve(n);
    for (size_t pos = 0; pos < n; ++pos) {
      float value = 0.0f;
      if (GetCrossSectionValue(col, infos[view.rows[pos]], value))
        keys.emplace_back(value, pos);
    }
    if (keys.empty())
      continue;

    // 排完序 keys[0] 最好
    const bool valuation = IsValuationColumn(col);
    std::sort(keys.begin(), keys.end(),
              [valuation](const std::pair<float, size_t> &a,
                          const std::pair<float, size_t> &b) {
                return valuation ? ValuationLess(a.first, b.first)
                                 : a.first > b.first;
              });

    // 并列同值取平均名次 —— 否则 DY 里成片的 0.00 会被摊成一整条渐变,
    // 看着像有高低之分
    const size_t m = keys.size();
    for (size_t i = 0; i < m;) {
      size_t j = i + 1;
      while (j < m && keys[j].first == keys[i].first)
        ++j;

      const float rank = 0.5f * static_cast<float>(i + j - 1);
      const float p = (m > 1) ? 1.0f - rank / static_cast<float>(m - 1) : 0.5f;
      const ImU32 color = RampColor(p);
      for (size_t k = i; k < j; ++k)
        colors[keys[k].second] = color;

      i = j;
    }
  }
}

bool ViewIsStale(const TableView &view, const Asset &asset,
                 const StockInfoMap &stock_info, const TableState &state) {
  return !view.built ||
         view.generation != asset.asset_stats_generation ||
         view.asset_count != asset.items.size() ||
         view.stock_info_count != stock_info.size() ||
         view.sort_column != state.sort_column ||
         view.sort_ascending != state.sort_ascending ||
         view.filter_st_only != state.filter_st_only ||
         view.filter_listed_only != state.filter_listed_only ||
         view.board_filter != state.board_filter ||
         view.search_query != state.search_query ||
         view.industry_filter != state.industry_filter;
}

void RebuildView(TableView &view, const Asset &asset,
                 const StockInfoMap &stock_info, const TableState &state) {
  const size_t count = asset.items.size();
  assert(asset.asset_stats.size() == count);

  // StockInfo 指针只在本次重建期间用 (基本面整体重载会让它们失效, 所以
  // 不进 view). 每资产一次 map 查找, 排序时几万次比较都直接吃这份.
  std::vector<const StockInfo *> infos(count, nullptr);

  view.rows.clear();
  view.rows.reserve(count);
  for (size_t id = 0; id < count; ++id) {
    infos[id] = FindStockInfo(asset.items[id], stock_info);
    if (ShouldShowAsset(asset.items[id], infos[id], state))
      view.rows.push_back(id);
  }

  if (state.sort_column >= 0) {
    const int col = state.sort_column;

    // 严格弱序: 降序 = 交换实参, 不是对结果取反 (取反在相等时会同时声称
    // a<b 与 b<a, 那是 UB)
    auto less = [&](size_t ia, size_t ib) -> bool {
      const AssetItem &aa = asset.items[ia];
      const AssetItem &ab = asset.items[ib];
      const StockInfo *na = infos[ia];
      const StockInfo *nb = infos[ib];
      const Asset::AssetStats &sa = asset.asset_stats[ia];
      const Asset::AssetStats &sb = asset.asset_stats[ib];

      switch (col) {
      case 0: // Code
        return aa.asset_code < ab.asset_code;
      case 1: { // Name
        const std::string &name_a = (na && !na->name.empty()) ? na->name : aa.asset_code;
        const std::string &name_b = (nb && !nb->name.empty()) ? nb->name : ab.asset_code;
        return name_a < name_b;
      }
      case 2: // Exchange
        return aa.exchange < ab.exchange;
      case 3: // Board
        return (int)GetBoardType(aa.asset_code) < (int)GetBoardType(ab.asset_code);
      case 4: // ST: 正常 < ST < *ST
        return GetStLevel(na) < GetStLevel(nb);
      case 5: { // DL (Delisted)
        const bool a_dl = na && na->outDate != "" && na->outDate != "0";
        const bool b_dl = nb && nb->outDate != "" && nb->outDate != "0";
        return a_dl < b_dl;
      }
      case 6: { // Listed: 在市总天数
        const int a_days = na ? CalculateListedSpan(*na).total_days : 0;
        const int b_days = nb ? CalculateListedSpan(*nb).total_days : 0;
        return a_days < b_days;
      }
      case 7: { // Industry (按展示名排, 与列内容一致)
        static const std::string kEmpty;
        const std::string &a_ind = na ? GetIndustryDisplay(*na) : kEmpty;
        const std::string &b_ind = nb ? GetIndustryDisplay(*nb) : kEmpty;
        return a_ind < b_ind;
      }
      case 8: // PE
        return ValuationLess(na ? SafeStod(na->peTTM) : -1e9,
                             nb ? SafeStod(nb->peTTM) : -1e9);
      case 9: // PB
        return ValuationLess(na ? SafeStod(na->pbMRQ) : -1e9,
                             nb ? SafeStod(nb->pbMRQ) : -1e9);
      case 10: // PS
        return ValuationLess(na ? SafeStod(na->psTTM) : -1e9,
                             nb ? SafeStod(nb->psTTM) : -1e9);
      case 11: // PCF
        return ValuationLess(na ? SafeStod(na->pcfNcfTTM) : -1e9,
                             nb ? SafeStod(nb->pcfNcfTTM) : -1e9);
      case 12:   // DY1
      case 13:   // DY3
      case 14: { // DY5
        // 无分红 = 0 排在最低; 缺基本面 / 上市不足一季 = -1 更低
        std::string StockInfo::*field =
            col == 12 ? &StockInfo::dy1y
                      : (col == 13 ? &StockInfo::dy3y : &StockInfo::dy5y);
        return (na ? SafeStod(na->*field, -1.0f) : -1.0f) <
               (nb ? SafeStod(nb->*field, -1.0f) : -1.0f);
      }
      case 15: // Market Cap
        return (na ? CalculateMarketCap(*na) : 0) < (nb ? CalculateMarketCap(*nb) : 0);
      case 16: // Days
        return sa.total_days < sb.total_days;
      case 17: // Orders
        return sa.total_orders < sb.total_orders;
      case 18: { // Orders% — 无分母的 (北交所/未上市) 恒排在最后
        const float pa = sa.expected_days > 0 ? sa.orders_coverage_percent() : -1.0f;
        const float pb = sb.expected_days > 0 ? sb.orders_coverage_percent() : -1.0f;
        return pa < pb;
      }
      default:
        return false;
      }
    };

    std::stable_sort(view.rows.begin(), view.rows.end(),
                     [&](size_t ia, size_t ib) {
                       return state.sort_ascending ? less(ia, ib) : less(ib, ia);
                     });
  }

  RebuildCrossSectionColors(view, infos);

  view.built = true;
  view.generation = asset.asset_stats_generation;
  view.asset_count = count;
  view.stock_info_count = stock_info.size();
  view.sort_column = state.sort_column;
  view.sort_ascending = state.sort_ascending;
  view.filter_st_only = state.filter_st_only;
  view.filter_listed_only = state.filter_listed_only;
  view.board_filter = state.board_filter;
  view.search_query = state.search_query;
  view.industry_filter = state.industry_filter;
}

// 视图过期就重建. 表格与横截面面板都先调它, 之后共用 state.view.rows.
void SyncView(TableState &state, const Asset &asset, const StockInfoMap &stock_info) {
  if (ViewIsStale(state.view, asset, stock_info, state))
    RebuildView(state.view, asset, stock_info, state);
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

  // Board filter dropdown
  ImGui::SameLine();
  ImGui::SetNextItemWidth(100.0f);
  const char *board_names[] = {"All", "Unknown", "沪主板", "深主板", "科创板", "创业板", "北交所"};
  int current_board = static_cast<int>(state.board_filter);
  if (ImGui::Combo("Board##BoardFilter", &current_board, board_names, 7)) {
    state.board_filter = static_cast<BoardType>(current_board);
  }

  // Industry filter - collect all unique industries
  // 基本面可能在本页首次渲染之后才载入完, 所以缓存要跟着 stock_info 规模失效,
  // 否则行业下拉框会永久停在空列表
  static std::vector<std::pair<std::string, std::string>> industries; // code, name
  static size_t industries_cached_count = static_cast<size_t>(-1);

  if (industries_cached_count != stock_info.size()) {
    std::map<std::string, std::string> ind_map; // code -> name
    for (const auto &asset : assets) {
      const StockInfo *info = FindStockInfo(asset, stock_info);
      if (info && !info->ind_code.empty()) {
        ind_map[info->ind_code] = info->ind_name;
      }
    }
    industries.clear();
    industries.emplace_back("", "All Industries");
    for (const auto &[code, name] : ind_map) {
      industries.emplace_back(code, name.empty() ? code : name + " (" + code + ")");
    }
    industries_cached_count = stock_info.size();
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
  ImGui::BeginGroup();
  ImGui::TextColored(RampColorVec4(1.0f), "■");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(RampColorVec4(0.5f), "■");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(RampColorVec4(0.0f), "■");
  ImGui::EndGroup();
  if (ImGui::IsItemHovered()) {
    ImGui::SetTooltip(
        "截面色 (Listed / PE / PB / PS / PCF / DY1 / DY3 / DY5 / Cap)\n"
        "绿 = 好, 红 = 差; 取的是当前 filter 后 pool 内的名次分位\n"
        "  Listed: 在市越久越绿\n"
        "  PE/PB/PS/PCF: 越低越绿 (亏损的负值排在最差一端)\n"
        "  DY1/DY3/DY5: 分红越多越绿\n"
        "  Cap: 市值越大越绿\n\n"
        "用名次分位而非数值归一化: 估值/市值是重尾分布,\n"
        "按 min-max 染色会被几个极端值压成一片同色");
  }

  ImGui::SameLine();
  if (ImGui::SmallButton(state.show_cross_section_panel ? "Hide" : "Show")) {
    state.show_cross_section_panel = !state.show_cross_section_panel;
  }
}

// ============================================================================
// Helper: Render data table (18 columns)
// ============================================================================

void RenderDataTable(
    const Asset &asset_data,
    const StockInfoMap &stock_info,
    TableState &table_state) {

  ImGuiTableFlags flags = ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg |
                          ImGuiTableFlags_Sortable | ImGuiTableFlags_ScrollX | ImGuiTableFlags_ScrollY |
                          ImGuiTableFlags_Resizable | ImGuiTableFlags_Reorderable |
                          ImGuiTableFlags_SizingFixedFit;

  if (!ImGui::BeginTable("AssetsTable", 19, flags)) {
    return;
  }

  // Setup columns (19 columns) - use auto width (default)
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
  ImGui::TableSetupColumn("DY1");
  ImGui::TableSetupColumn("DY3");
  ImGui::TableSetupColumn("DY5");
  ImGui::TableSetupColumn("Cap");
  ImGui::TableSetupColumn("Days");
  ImGui::TableSetupColumn("Orders");
  ImGui::TableSetupColumn("Orders%");

  ImGui::TableSetupScrollFreeze(0, 1);

  // Custom headers with tooltips
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  const char *header_labels[] = {"Code", "Name", "Exch", "Board", "ST", "DL", "Listed", "Ind",
                                 "PE", "PB", "PS", "PCF", "DY1", "DY3", "DY5", "Cap", "Days",
                                 "Orders", "Orders%"};
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

      "市净率 (PB MRQ)\npbMRQ = Price-to-Book Ratio (Most Recent Quarter)\n= 总市值 / 归属母公司股东权益MRQ\n= (收盘价 x 总股本) / total_equity_to_parent_shareholders\n\nMRQ = 最新报告期 (max report_date 的最新可见行)\n反映账面价值,通常>1,<1可能破净",

      "滚动市销率 (PS TTM)\npsTTM = Price-to-Sales Ratio (TTM)\n= 股票收盘价 / 每股销售额\n= (收盘价 x 总股本) / 营业总收入TTM\n\n反映每单位营收对应的市值\n适用于尚未盈利但有营收的公司",

      "滚动市现率 (PCF TTM)\npcfNcfTTM = Price-to-Cash-Flow Ratio (TTM)\n= 总市值 / 经营活动现金流量净额TTM\n= (收盘价 x 总股本) / net_cffoa_ttm\n\n注意分母是经营现金流, 不是现金及现金等价物净增加额\n烧钱(经营现金流为负) 保留负值, 不置空\n\n反映现金流创造能力\n比PE更难以通过会计手段操纵",

      "股息率 近1年 (Dividend Yield, 年化)\n= 近365日税前分红总额 / 总市值 x 100%\n\n说明:\n- 单位: %\n- 窗口以分红公告日 (publish_date) 锚定, 非除权日\n- 口径与 L1 特征 dy_raw 一致\n- 0.00 = 窗口内确实没有分红公告 (非缺失)",

      "股息率 近3年 (年化平均)\n= 近3年(1095日)税前分红总额 / 年数 / 总市值 x 100%\n\n说明:\n- 已年化, 可与 DY1 / DY5 直接横向比较\n- 上市不足3年的按实际上市年数年化, 不被系统性摊薄\n- 上市不足一个季度 → 留空 (年化无意义)\n- DY3 明显低于 DY1 = 近年才开始分红",

      "股息率 近5年 (年化平均)\n= 近5年(1825日)税前分红总额 / 年数 / 总市值 x 100%\n\n说明:\n- 已年化, 可与 DY1 / DY3 直接横向比较\n- 上市不足5年的按实际上市年数年化\n- DY1 ≈ DY3 ≈ DY5 = 长期稳定分红",

      "总市值 (Market Cap)\n= 收盘价 x 总股本 / 1亿\n= close x total_shares (不复权真价)\n\n说明:\n- 单位: 亿元\n- 与 PE/PB/PS/PCF/DY 的分子同源\n- 数据来自 cn_stock_real_bar1d.close + cn_stock_shares.total_shares",

      "交易日数 (Trading Days)\n该股票在数据库中有数据的总交易日数\n= date_info.size()\n可用于判断数据完整性",

      "逐笔总数 (Total Orders)\n所有交易日的逐笔记录总数量 (委托+成交合并后)\n= Σ order_count (累加所有日期)\n\n说明:\n- 单位:条记录\n- 显示格式:>1M用M(百万), >1K用K(千)\n- 条数由文件头 raw_size 推出, 扫描时一并读到",

      "逐笔完整性 (Orders Coverage)\n回测区间内已编码的交易日占比\n= (应有天数 - 缺失天数) / 应有天数\n\n分母是该标的\"本该有逐笔\"的交易日:\n已上市未退市, 且排除当日全天停牌\n(北交所不在 L2 覆盖范围, 整体留空)\n\n与 Browser 页的完整性、Encode 页的缺失表同源\nhover 单元格可看缺失天数"};

  for (int col = 0; col < 19; col++) {
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

  // 排序规则的变化由 ImGui 的 SpecsDirty 告知; 真正的重排在 RebuildView 里,
  // 只在规则/过滤/数据代数变化时做一次 (见 TableView).
  if (ImGuiTableSortSpecs *sort_specs = ImGui::TableGetSortSpecs()) {
    if (sort_specs->SpecsDirty && sort_specs->SpecsCount > 0) {
      const auto &spec = sort_specs->Specs[0];
      table_state.sort_column = spec.ColumnIndex;
      table_state.sort_ascending = spec.SortDirection == ImGuiSortDirection_Ascending;
    }
    sort_specs->SpecsDirty = false;
  }

  SyncView(table_state, asset_data, stock_info);

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

  // 截面色: cs_colors 与 view.rows 同序, 直接按行位置取 (见 TableView).
  // 返回 true = 已 PushStyleColor, 调用方渲完要 Pop.
  auto push_cs_color = [&table_state](int col, int row) {
    const int slot = ColoredColumnSlot(col);
    assert(slot >= 0);
    const std::vector<uint32_t> &colors = table_state.view.cs_colors[slot];
    assert(colors.size() == table_state.view.rows.size());
    if (colors[row] == 0)
      return false;
    ImGui::PushStyleColor(ImGuiCol_Text, colors[row]);
    return true;
  };

  // Render rows
  int row_idx = 0;
  for (const size_t id : table_state.view.rows) {
    const AssetItem &asset = asset_data.items[id];
    const Asset::AssetStats &stats = asset_data.asset_stats[id];
    // StockInfo 每帧现查 (基本面可能整体重载, 缓存指针会失效)
    const StockInfo *info = FindStockInfo(asset, stock_info);

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
      const bool cs = push_cs_color(6, row_idx);
      ImGui::Text("%02d/%02d/%02d", span.years, span.months, span.days);
      if (cs)
        ImGui::PopStyleColor();
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
          const bool cs = push_cs_color(8, row_idx);
          ImGui::Text("%.1f", pe);
          if (cs)
            ImGui::PopStyleColor();
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
          const bool cs = push_cs_color(9, row_idx);
          ImGui::Text("%.2f", pb);
          if (cs)
            ImGui::PopStyleColor();
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
          const bool cs = push_cs_color(10, row_idx);
          ImGui::Text("%.2f", ps);
          if (cs)
            ImGui::PopStyleColor();
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
          const bool cs = push_cs_color(11, row_idx);
          ImGui::Text("%.1f", pcf);
          if (cs)
            ImGui::PopStyleColor();
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

    // Col 12/13/14: DY 1y/3y/5y (%), 三列同构 — 颜色走截面分位 (分红越多越绿)
    auto render_dy = [&](int col, const std::string *val) {
      ImGui::TableSetColumnIndex(col);
      if (hovered_col == col) {
        ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
      }
      if (val && !val->empty()) {
        float dy = std::stod(*val);
        const bool cs = push_cs_color(col, row_idx);
        ImGui::Text("%.2f", dy);
        if (cs)
          ImGui::PopStyleColor();
      } else {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
      handle_column_click(col);
    };
    render_dy(12, info ? &info->dy1y : nullptr);
    render_dy(13, info ? &info->dy3y : nullptr);
    render_dy(14, info ? &info->dy5y : nullptr);

    // Col 15: Market Cap (亿元)
    ImGui::TableSetColumnIndex(15);
    if (hovered_col == 15) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (info) {
      float cap = CalculateMarketCap(*info);
      if (cap > 0) {
        const bool cs = push_cs_color(15, row_idx);
        ImGui::Text("%.1f", cap);
        if (cs)
          ImGui::PopStyleColor();
      } else {
        ImGui::TextColored(COLOR_GRAY, "-");
      }
    } else {
      ImGui::TextColored(COLOR_GRAY, "-");
    }
    handle_column_click(15);

    // Col 16: Trading Days
    // 以下三列的统计都取自 Asset::asset_stats — 逐帧遍历 date_info 会把
    // 帧时间拖到几百毫秒 (见 TableView 处的说明)
    ImGui::TableSetColumnIndex(16);
    if (hovered_col == 16) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t total_days = stats.total_days;
    ImGui::Text("%zu", total_days);
    handle_column_click(16);

    // Col 17: Total Orders
    ImGui::TableSetColumnIndex(17);
    if (hovered_col == 17) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    size_t total_orders = stats.total_orders;
    if (total_orders > 1000000) {
      ImGui::Text("%.2fM", total_orders / 1000000.0);
    } else if (total_orders > 1000) {
      ImGui::Text("%.1fK", total_orders / 1000.0);
    } else {
      ImGui::Text("%zu", total_orders);
    }
    handle_column_click(17);

    // Col 18: Orders Coverage
    // expected_days == 0 = 该标的在回测区间内本就不该有数据 (北交所 / 尚未
    // 上市 / 已退市), 那不是缺口, 留空而不是 0%.
    ImGui::TableSetColumnIndex(18);
    if (hovered_col == 18) {
      ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::GetColorU32(ImVec4(0.3f, 0.3f, 0.4f, 0.3f)));
    }
    if (stats.expected_days == 0) {
      ImGui::TextColored(COLOR_GRAY, "-");
    } else {
      const float pct = stats.orders_coverage_percent();
      const ImVec4 color = pct >= 99.9f   ? ImVec4(0.3f, 0.95f, 0.4f, 1.0f)
                           : pct >= 95.0f ? ImVec4(1.0f, 1.0f, 0.0f, 1.0f)
                                          : ImVec4(1.0f, 0.5f, 0.0f, 1.0f);
      ImGui::TextColored(color, "%.1f%%", pct);
      if (ImGui::IsItemHovered()) {
        ImGui::SetTooltip("Missing %zu of %zu trading days", stats.orders_missing, stats.expected_days);
      }
    }
    handle_column_click(18);

    ImGui::PopID();
    row_idx++;
  }

  ImGui::EndTable();
}

// ============================================================================
// Forward declarations
// ============================================================================

static void RenderNumericAnalysis(
    const Asset &asset_data,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name);

static void RenderCategoricalAnalysis(
    const Asset &asset_data,
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
  // Numeric: Listed Days(6), PE(8), PB(9), PS(10), PCF(11), DY1/3/5(12,13,14),
  //          Market Cap(15), Trading Days(16), Total Orders(17), Orders%(18)
  if (col_idx >= 6 && col_idx <= 18) {
    return ColumnDataType::Numeric;
  }
  // Others (Code, Name, Exchange) not analyzable
  return ColumnDataType::Categorical; // Default
}

// ============================================================================
// Helper: Render cross-section analysis panel
// ============================================================================

void RenderCrossSectionPanel(
    const Asset &asset_data,
    const StockInfoMap &stock_info,
    const TableState &table_state) {

  if (table_state.selected_column_idx < 0) {
    ImGui::TextWrapped("Click on any cell to view cross-section analysis.");
    return;
  }

  // Column names for display
  const char *col_names[] = {
      "Code", "Name", "Exchange", "Board", "ST", "DL", "Listed Days (在市总天数)", "Industry",
      "PE(TTM)", "PB(MRQ)", "PS(TTM)", "PCF", "DY 近1年 (年化 %)", "DY 近3年 (年化 %)",
      "DY 近5年 (年化 %)", "Market Cap (亿元)", "Trading Days",
      "Total Orders", "Orders % (回测区间完整性)"};

  int col_idx = table_state.selected_column_idx;
  if (col_idx >= 19) {
    ImGui::Text("Invalid column index");
    return;
  }

  ImGui::Text("Column: %s", col_names[col_idx]);
  ImGui::Separator();

  ColumnDataType data_type = GetColumnDataType(col_idx);

  if (data_type == ColumnDataType::Categorical) {
    RenderCategoricalAnalysis(asset_data, stock_info, table_state, col_idx, col_names[col_idx]);
  } else {
    RenderNumericAnalysis(asset_data, stock_info, table_state, col_idx, col_names[col_idx]);
  }
}

// ============================================================================
// Numeric Column Analysis
// ============================================================================

static void RenderNumericAnalysis(
    const Asset &asset_data,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name) {
  (void)col_name; // Unused

  // Extract numeric data (only for filtered assets)
  std::vector<std::string> names;
  std::vector<float> values;
  std::vector<std::string> codes;

  // 过滤结果直接用表格的缓存视图 (见 TableView)
  for (const size_t id : table_state.view.rows) {
    const AssetItem &asset = asset_data.items[id];
    const Asset::AssetStats &stats = asset_data.asset_stats[id];
    const StockInfo *info = FindStockInfo(asset, stock_info);

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
    case 12:   // DY1
    case 13:   // DY3
    case 14: { // DY5
      const std::string *dy =
          !info ? nullptr
                : (col_idx == 12 ? &info->dy1y
                                 : (col_idx == 13 ? &info->dy3y : &info->dy5y));
      if (dy && !dy->empty()) {
        value = std::stod(*dy);
        is_valid = true;
      }
      break;
    }
    case 15: // Market Cap
      if (info) {
        value = CalculateMarketCap(*info);
        is_valid = (value > 0);
      }
      break;
    case 16: // Trading Days
      value = stats.total_days;
      is_valid = true;
      break;
    case 17: // Total Orders
      value = stats.total_orders;
      is_valid = true;
      break;
    case 18: // Orders% — 没有分母的标的不参与横截面
      value = stats.orders_coverage_percent();
      is_valid = stats.expected_days > 0;
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
    const Asset &asset_data,
    const StockInfoMap &stock_info,
    const TableState &table_state,
    int col_idx,
    const char *col_name) {
  (void)col_name; // Unused

  // Extract categorical data
  std::vector<std::string> categories;
  std::vector<std::string> codes;

  // 过滤结果直接用表格的缓存视图 (见 TableView)
  for (const size_t id : table_state.view.rows) {
    const AssetItem &asset = asset_data.items[id];
    const StockInfo *info = FindStockInfo(asset, stock_info);

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
    Asset &asset,
    const StockInfoMap &stock_info,
    TableState &table_state) {

  const std::vector<AssetItem> &assets = asset.items;

  // 每资产统计由扫描末尾一次算好 (见 ScanService Phase 5), 这里只是等它到位
  if (asset.asset_stats.size() != assets.size()) {
    ImGui::TextDisabled("Waiting for database scan...");
    return;
  }

  // 过滤 + 排序结果的缓存, 顺便给下面的 Showing:x/y 供数
  SyncView(table_state, asset, stock_info);

  // Render filter bar
  RenderFilterBar(table_state, table_state.view.rows.size(), assets.size(), assets, stock_info);
  ImGui::Spacing();

  // Get window dimensions
  float window_width = ImGui::GetContentRegionAvail().x;
  float window_height = ImGui::GetContentRegionAvail().y;

  // Calculate left table width
  float left_width = table_state.show_cross_section_panel ? window_width * table_state.table_split_ratio : window_width;

  // Left: Data Table
  ImGui::BeginChild("LeftTable", ImVec2(left_width, window_height), true,
                    ImGuiWindowFlags_HorizontalScrollbar);
  RenderDataTable(asset, stock_info, table_state);
  ImGui::EndChild();

  // Right: Cross-section Analysis Panel
  if (table_state.show_cross_section_panel) {
    ImGui::SameLine();
    ImGui::BeginChild("RightPanel", ImVec2(0, window_height), true);
    RenderCrossSectionPanel(asset, stock_info, table_state);
    ImGui::EndChild();
  }
}

} // namespace GUI::Database
