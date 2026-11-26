#pragma once
#include "define/Fee.hpp"
#include "misc/date.hpp"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <functional>
#include <string>

struct Config {

  // 回测日历起点 (settings.json 里存 "YYYY-MM-DD"); 末日由数据定 (Asset::Backtest = 末个 L2 日, Lob 库 = 末个已编码日)
  misc::Ymd start_date = 20250101;

  // L2 原始归档 (仓库外, 本机路径); 仓库内 output/ 的路径一律出自 disk/Disk.hpp
  std::string archive_dir = "/mnt/dev/sde/A_stock/L2";

  // L2 Raw Data
  std::string csv_market_data = "行情.csv";
  std::string csv_market_trade = "逐笔成交.csv";
  std::string csv_market_order = "逐笔委托.csv";

  // 联网同步开关 (两项独立; 全关 = 只本地构建 AssetInfo, 零网络): 本地 parquet 照常消费.
  //   sync_fundamental: 基本面 (BigQuant 日频表 + Tushare)
  //   sync_bar1m      : 分钟线 bar1m (基本面之后)
  bool sync_fundamental = true;
  bool sync_bar1m = true;

  // 交易费率 (define/Fee.hpp; 全仓唯一一份: 因子换手成本 / 组合净线 / CTA / 实盘调仓试算 / 账户日内回推同用):
  //   佣金双边 + 过户费双边 + 印花税卖出单边 + 固定冲击 (无盘口冲击列时两侧都按它; 标签是毛收益不含费)
  Fee fee;
  // 冲击封顶 (比率): lb_cost_buy / sell_* 装载时 min(·, max_impact) 再进组均 (gui/engine/Load). 深度吃穿 / 涨跌停成交的格冲击可到百分之几,
  //   原值进均值把期限结构净线整体砸穿 (过罚); 封顶后那些格最多扣这么多, 可执行性另议. 两侧同封. 默认 1%
  double max_impact = 0.01;
  // 成本口径: false = 固定段 (两侧量 × Fee::impact); true = 盘口段 (Lob 库 L_1min 的买 / 卖冲击列, 分钟层各层按行末分钟取; 评估域并入 Lob 库)
  bool lob_cost = false;

  // Config file path (output/config/settings/settings.json)
  std::string filepath;

  // String buffers for GUI (max 512 chars for path)
  char start_date_buf[64] = "";
  char archive_dir_buf[512] = "";
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
