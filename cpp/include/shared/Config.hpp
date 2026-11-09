#pragma once
#include "misc/date.hpp"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <functional>
#include <string>

struct Config {

  // 回测日历起点 (config.json 里存 "YYYY-MM-DD"); 末日由数据定 (Asset::Backtest = 末个 L2 日, Lob 库 = 末个已编码日)
  misc::Ymd start_date = 20250101;

  // Paths
  std::string archive_dir = "/mnt/dev/sde/A_stock/L2";
  std::string orders_dir = "../../../../output/orders";
  std::string feature_dir = "../../../../output/features";
  std::string factor_dir = "../../../../output/factors";
  std::string portfolio_dir = "../../../../output/portfolio";
  std::string export_dir = "../../../../output/export";
  std::string log_dir = "../../../../output/log";
  std::string config_dir = "../../../../config";

  // L2 Raw Data
  std::string csv_market_data = "行情.csv";
  std::string csv_market_trade = "逐笔成交.csv";
  std::string csv_market_order = "逐笔委托.csv";

  // 联网同步开关 (两项独立; 全关 = 只本地构建 AssetInfo, 零网络): 本地 parquet 照常消费.
  //   sync_fundamental: 基本面 (BigQuant 日频表 + Tushare)
  //   sync_bar1m      : 分钟线 bar1m (基本面之后)
  bool sync_fundamental = true;
  bool sync_bar1m = true;

  // 交易税佣 (比率, 不分日期, 统一按现行标准): 标签 (LabelBar) 是毛收益不含它, 消费端事后扣 (Inspect 图 1 按钮).
  //   一次往返 = 2 × commission + stamp (印花税只卖出单边)
  double commission = 0.0001; // 佣金 万 1, 双边各一次
  double stamp = 0.0005;      // 印花税 万 5, 卖出单边
  // 固定冲击 (比率, 模拟值, 不按金额变): 标签 exit 是分钟 VWAP (不含冲击), Lob 库建仓冲击按深度精确算成特征 (lb_cost_*),
  //   平仓侧统一扣这一常数 (做多卖出); 固定段 (因子没用 lob 叶时唯一的段) 建仓也按它. 默认 20bp
  double fixed_impact = 0.0020;
  // 建仓买入冲击封顶 (比率): lb_cost_buy_* 装载时 min(·, max_impact) 再进组均 (gui/engine/Load). 深度吃穿 / 涨停成交的格冲击可到百分之几,
  //   原值进均值把期限结构净线整体砸穿 (过罚); 封顶后那些格最多扣这么多, 可执行性另议. 卖出侧不封. 默认 1%
  double max_impact = 0.01;

  // Config file path
  std::string filepath = "../../../../config/config.json";

  // String buffers for GUI (max 512 chars for path)
  char start_date_buf[64] = "";
  char archive_dir_buf[512] = "";
  char orders_dir_buf[512] = "";
  char feature_dir_buf[512] = "";
  char factor_dir_buf[512] = "";
  char portfolio_dir_buf[512] = "";
  char export_dir_buf[512] = "";
  char log_dir_buf[512] = "";
  char config_dir_buf[512] = "";
  char csv_market_data_buf[128] = "";
  char csv_market_trade_buf[128] = "";
  char csv_market_order_buf[128] = "";

  // Auto-sync state
  bool dirty = false;
  std::chrono::steady_clock::time_point last_modified;
  std::filesystem::file_time_type last_file_time;

  // Log callback
  std::function<void(const std::string &)> log_callback;

  // Reinit callback (triggered after config save)
  std::function<void()> reinit_callback;

  // Initialize config (load or create default)
  void Initialize();

  // Mark config as modified (will auto-save after debounce)
  void MarkDirty();

  // Sync string buffers from config
  void SyncStringBuffers();

  // Auto-sync: check file changes and debounced save
  void AutoSync();

private:
  // Load config from JSON file
  bool LoadFromFile();

  // Save config to JSON file
  bool SaveToFile();
};
