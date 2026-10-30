#pragma once
#include "misc/date.hpp"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>

struct Config {

  // 回测日历起点 (config.json 里存 "YYYY-MM-DD"); 末日由数据定 (Asset::Backtest = 末个 L2 日, Lob 库 = 末个已编码日)
  misc::Ymd start_date = 20250101;

  // Paths
  std::string archive_dir = "/mnt/dev/sde/A_stock/L2";
  std::string orders_dir = "../../../../output/orders";
  std::string feature_dir = "../../../../output/features";
  std::string factor_dir = "../../../../output/factors";
  std::string portfolio_dir = "../../../../output/portfolio";
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

  // 交易税佣 (比率, 不分日期, 统一按现行标准): 标签 (LabelLob) 是毛收益不含它, 消费端事后扣 (Inspect 图 1 按钮).
  //   一次往返 = 2 × commission + stamp (印花税只卖出单边)
  double commission = 0.0001; // 佣金 万 1, 双边各一次
  double stamp = 0.0005;      // 印花税 万 5, 卖出单边
  // 固定冲击 (比率, 模拟值, 不按金额变): 标签 exit 是盘口中间价 (不含冲击), Lob 建仓冲击按深度精确算成特征 (lb_cost_*),
  //   平仓侧没有对应盘口, 选择扣冲击时统一扣这一常数 (做多卖出); Bar 档无冲击特征, 建仓也按它. 默认 20bp
  double fixed_impact = 0.0020;
  // 建仓买入冲击封顶 (比率): lb_cost_buy_* 装载时 min(·, max_impact) 再进组均 (FactorsLoad). 深度吃穿 / 涨停成交的格冲击可到百分之几,
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
  char log_dir_buf[512] = "";
  char config_dir_buf[512] = "";
  char csv_market_data_buf[128] = "";
  char csv_tick_trade_buf[128] = "";
  char csv_tick_order_buf[128] = "";

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

  // 特征库目录按池名分片: <feature_dir>/<pool>. 各池并存, 重算一个不清别人的池; 写读两端唯一的路径入口 (池名见 FeatureLib.hpp)
  std::string FeatureLibDir(std::string_view pool) const { return feature_dir + "/" + std::string(pool); }
  // Bar 档 (All: 分钟 K + 基本面, 全市场 A 轴) 的池名; 其余池名 = 池名 (Lob 档)
  static constexpr const char *kAllTierDir = "all";

private:
  // Load config from JSON file
  bool LoadFromFile();

  // Save config to JSON file
  bool SaveToFile();
};

// ============================================================================
// 编译期常量 (不进 config.json — 从未在运行时被改过, GUI 暴露成"可配置项"
//   反而误导; 基本面 sync 凭据/端点/流水线参数在 api 层深处以 constexpr 消费)
// ============================================================================
namespace config {

// ---- L2 归档/二进制数据库 (与 unrar 语法绑定, 非真正可插拔) ----
//   misc/archive.cpp 的列表/流式解压命令行硬编码 unrar 语法 ("vt" 技术列表 /
//   "p -inul" 抽取到 stdout) —— 换成 7z/unzip 会拼出错误命令, ARCHIVE_TOOL
//   本质是常量而非运行时可切换的选项.
inline constexpr const char *ARCHIVE_EXTENSION = ".rar"; // 归档压缩包扩展名
inline constexpr const char *ARCHIVE_TOOL = "unrar";     // 解压工具 (语法已绑死, 见上)
inline constexpr const char *BINARY_EXTENSION = ".bin";  // L2 二进制产物扩展名

// ---- 代码变更 (同一家公司换了证券代码) ----
// A 股换代码极罕见 —— 重大资产重组通常也不换 —— 全库实测只有中航电测一例 (300114.SZ → 302132.SZ)
struct CodeChange {
  const char *code;   // 现行代码 (资产轴上的那个)
  const char *former; // 变更前代码 (归档目录名)
  misc::Ymd until;    // 新代码生效日; 早于这天的归档里叫 former
};

inline constexpr CodeChange CODE_CHANGES[] = {
    {"302132.SZ", "300114.SZ", 20250217}, // 中航电测 → 中航成飞
};

// 现行代码 → 归档里的目录名. 无变更记录时原样返回.
inline std::string archive_code(const std::string &code, misc::Ymd date) {
  for (const auto &c : CODE_CHANGES) {
    if (code == c.code && date < c.until)
      return c.former;
  }
  return code;
}

// archive_code 的逆向: 归档里读到的目录名 → 现行代码.
inline std::string current_code(const std::string &archived, misc::Ymd date) {
  for (const auto &c : CODE_CHANGES) {
    if (archived == c.former && date < c.until)
      return c.code;
  }
  return archived;
}

// ---- 数据源凭据 (与官方 CLI / 控制台 token 同源) ----
//   BigQuant DAI: AK = Flight Basic Token 用户名; SK = 密码, 永不回传
//   Tushare pro token; *_vip 接口需 5000+ 积分
inline constexpr const char *BIGQUANT_AK = "6dS0GYgxocXL";
inline constexpr const char *BIGQUANT_SK = "bKDM141Hz2etbj3QTLf9GA6aGmEZRu68MJuvaJBJyPgq22E3fNNDRehFCbgComTQ";
inline constexpr const char *TUSHARE_TOKEN = "6b5ea435a4626b1eeedefb2115bcf9e84fc64a0d212d21cf2be03d54";

// ---- 数据源端点 (host / port / 超时 / 重试) ----
//   BigQuant 数据面: Flight 17010 — 明文 gRPC + Arrow IPC RecordBatch, 零拷贝
//   Tushare:         HTTP   80    — 明文 JSON POST, 三张事件表
//   超时 = 单次连接+读写整体时长; 重试 = RETRY_MAX 次额外重试, 线性间隔
inline constexpr const char *BIGQUANT_FLIGHT_URI = "grpc+tcp://bigquant.com:17010";
inline constexpr int BIGQUANT_FLIGHT_GRPC_MAX_METADATA_SIZE = 16 * 1024 * 1024; // SDK 默认 8KB 会被 JWT 撑爆
// 周配额 preflight: 剩余比例 < 该值时 BigQuant 表 (基本面 / 分钟线) 同步前弹窗确认 (BigQuant sync 不自己判, 由 GUI 决定)
inline constexpr double BIGQUANT_QUOTA_CONFIRM_RATIO = 0.3;

inline constexpr const char *TUSHARE_HTTP_HOST = "api.tushare.pro";
inline constexpr const char *TUSHARE_HTTP_PORT = "80";  // 走 80, 省掉 SSL 依赖
inline constexpr int TUSHARE_HTTP_TIMEOUT_SECONDS = 60; // range 接口序列化耗时可达 20s
inline constexpr int TUSHARE_HTTP_RETRY_MAX = 4;        // 共 5 次尝试
inline constexpr int TUSHARE_HTTP_RETRY_INTERVAL_SECONDS = 30;

// ---- 抓取流水线 (落地 output/fundamental/YYYY-MM/*.parquet, 调度 fundamental::sync::plan) ----
//   PIPELINE_START_DATE: 数据同步起点 (A 股财报电子化从 2015 起逐渐完整).
//     与 Config::start_date (回测日历起点) 语义不同, 不合并 — 基本面历史
//     要为后续特征表全周期服务, 不随回测日历收窄.
//   LOOKBACK_DAYS: 月末仍在该窗口内的月视为开放月 (兜服务端回填修订)
//   DEDUP_WINDOW_SECONDS: parquet mtime 距今 < 该值则本表跳过 (连跑零网络)
inline constexpr misc::Ymd PIPELINE_START_DATE = 20150101;
inline constexpr int PIPELINE_LOOKBACK_DAYS = 7;
inline constexpr int PIPELINE_DEDUP_WINDOW_SECONDS = 60 * 60;

// ---- 特征计算 perf 采样落地 (PROFILE 模式, 见 worker/feature_workers.hpp FeatureProfile) ----
inline constexpr const char *PROFILE_DIR = "../../../../output/profile";

} // namespace config