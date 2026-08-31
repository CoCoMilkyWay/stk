// Tab Browser - Calendar Grid View for Trading Days
// Displays dense calendar grid based on stock_days.json

#pragma once

#include "shared/Asset.hpp"
#include "shared/AssetInfo.hpp"
#include <map>
#include <string>
#include <vector>

namespace GUI::Database {

// 单只标的的一次除权除息 (复权因子变点)
struct DividendEvent {
  std::string code;  // "sz.000001"
  std::string name;  // 简称, 基本面查不到时为空
  float ratio = 0.0; // factor_curr / factor_prev
};

// Daily statistics aggregated for a single date.
//
// 早先这里按"快照 / 逐笔 / 两者都有"分三种视角看覆盖率. 快照不再编码后, L2
// 覆盖率只剩一个口径 (逐笔), 三种视角合成一种, 视图模式的枚举也就没了.
struct DailyStats {
  std::string date_str; // YYYYMMDD format
  bool is_trading_day = false;
  bool is_holiday = false; // Non-weekend non-trading day
  bool is_in_backtest_range = false;

  // L2 data coverage
  size_t total_assets = 0; // Total stocks listed on this date
  size_t assets_with_orders = 0;

  // Dividend/split events (按代码升序; 数量即 .size())
  std::vector<DividendEvent> dividend_events;

  // Completeness metric (0.0 - 1.0)
  float completeness_orders() const {
    return total_assets > 0 ? (float)assets_with_orders / total_assets : 0.0f;
  }
};

// Layer visibility toggle state
struct LayerVisibility {
  bool show_dividend_split = false; // Layer 1: Yellow (默认关: 会盖住其他层)
  bool show_holiday = true;         // Layer 2: Purple
  bool show_backtest_range = true;  // Layer 3: Green
  bool show_l2_data = true;         // Layer 4: Blue
  bool show_completeness = true;    // Border: Green/Yellow/Red
};

// Browser state
struct BrowserState {
  int selected_year = -1;
  int selected_month = -1;
  int selected_day = -1;
  std::string hover_date;
  std::map<std::string, DailyStats> daily_stats_cache; // date -> stats
  LayerVisibility layers;
};

// Render the browser tab showing calendar grid
void RenderTabBrowser(
    const StockDaysVec &stock_days,
    const StockFactorMap &stock_factors,
    const StockInfoMap &stock_info,
    const Asset &asset_data,
    const std::string &backtest_start,
    const std::string &backtest_end,
    BrowserState &browser_state);

} // namespace GUI::Database
