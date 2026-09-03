#pragma once
#include <chrono>
#include <filesystem>
#include <functional>
#include <string>

struct Config {

  // Backtest/Analysis Period
  std::string start_date = "2025-01-01";
  std::string end_date = "2025-02-01";

  // Paths
  std::string archive_dir = "/mnt/dev/sde/A_stock/L2";
  std::string orders_dir = "../../../../output/orders";
  std::string feature_dir = "../../../../output/features";
  std::string factor_dir = "../../../../output/factors";
  std::string log_dir = "../../../../output/log";
  std::string config_dir = "../../../../config";

  // L2 Raw Data
  std::string csv_market_data = "行情.csv";
  std::string csv_market_trade = "逐笔成交.csv";
  std::string csv_market_order = "逐笔委托.csv";

  // Config file path
  std::string filepath = "../../../../config/config.json";

  // String buffers for GUI (max 512 chars for path)
  char start_date_buf[64] = "";
  char end_date_buf[64] = "";
  char archive_dir_buf[512] = "";
  char orders_dir_buf[512] = "";
  char feature_dir_buf[512] = "";
  char factor_dir_buf[512] = "";
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
  const char *until;  // YYYYMMDD, 新代码生效日; 早于这天的归档里叫 former
};

inline constexpr CodeChange CODE_CHANGES[] = {
    {"302132.SZ", "300114.SZ", "20250217"}, // 中航电测 → 中航成飞
};

// 现行代码 → 归档里的目录名. 无变更记录时原样返回.
inline std::string archive_code(const std::string &code, const std::string &date) {
  for (const auto &c : CODE_CHANGES) {
    if (code == c.code && date < c.until)
      return c.former;
  }
  return code;
}

// archive_code 的逆向: 归档里读到的目录名 → 现行代码.
inline std::string current_code(const std::string &archived, const std::string &date) {
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

inline constexpr const char *TUSHARE_HTTP_HOST = "api.tushare.pro";
inline constexpr const char *TUSHARE_HTTP_PORT = "80";  // 走 80, 省掉 SSL 依赖
inline constexpr int TUSHARE_HTTP_TIMEOUT_SECONDS = 60; // range 接口序列化耗时可达 20s
inline constexpr int TUSHARE_HTTP_RETRY_MAX = 4;        // 共 5 次尝试
inline constexpr int TUSHARE_HTTP_RETRY_INTERVAL_SECONDS = 30;

// ---- 抓取流水线 (落地 output/fundamental/YYYY-MM/*.parquet, 调度 misc::plan_months) ----
//   PIPELINE_START_DATE: 数据同步起点 (A 股财报电子化从 2015 起逐渐完整).
//     与 Config::start_date (回测/分析窗口) 语义不同, 不合并 — 基本面历史
//     要为后续特征表全周期服务, 不随回测窗口收窄.
//   LOOKBACK_DAYS: 月末仍在该窗口内的月视为开放月 (兜服务端回填修订)
//   DEDUP_WINDOW_SECONDS: parquet mtime 距今 < 该值则本表跳过 (连跑零网络)
inline constexpr const char *PIPELINE_START_DATE = "20150101";
inline constexpr int PIPELINE_LOOKBACK_DAYS = 7;
inline constexpr int PIPELINE_DEDUP_WINDOW_SECONDS = 60 * 60;

} // namespace config