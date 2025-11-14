#include "worker/crosssectional_worker.hpp"
#include "worker/encoding_worker.hpp"
#include "worker/io_worker.hpp"
#include "worker/sequential_worker.hpp"
#include "worker/shared_state.hpp"

#include "codec/json_config.hpp"
#include "features/backend/FeatureStore.hpp"
#include "misc/affinity.hpp"
#include "misc/file_check.hpp"
#include "misc/logging.hpp"
#include "misc/progress_parallel.hpp"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

// ============================================================================
// L2数据处理架构 - 两阶段并行处理
// ============================================================================
//
// 【核心设计】
//   数据结构: SharedState 统一管理所有共享状态
//     - assets[]: 所有资产信息(元数据 + 每日统计 + 文件路径 + 状态位图)
//     - all_dates[]: 全局交易日序列(用于横截面因子同步)
//
//   Phase 1 (Encoding): Asset并行，Date乱序
//     - Worker领取asset，shuffle日期顺序以分散RAR访问压力
//     - RAR锁(阻塞模式):确保同一压缩包不被并发解压
//     - 记录统计信息到 asset.date_info[]，零额外扫描
//
//   Phase 2 (Analysis): Date-first遍历，横截面同步
//     - 所有worker按 all_dates[] 顺序同步推进
//     - 每个date处理完成后可插入横截面因子计算
//     - 无锁读取:所有路径/统计信息已在Phase 1缓存
//
// 【数据流】
//   RAR → CSV → L2结构 → Zstd压缩 → Binary文件
//                ↓ (统计信息记录到 date_info)
//   Binary文件 → 解压解码 → Order Book → 特征提取 → FeatureStore
//
// 【Key Insights】
//   1. 零重复扫描: Encoding时记录order_count/文件路径，Analysis直接读取
//   2. 路径缓存: 所有路径初始化时生成一次，存储在 date_info.database_dir
//   3. 类型缓存: exchange_type 构造时推导一次，避免字符串解析
//   4. 状态位图: date_info.encoded/analyzed 支持断点续传和精确追踪
//   5. 负载均衡: 使用encoding时累积的order_count，无需预扫描
//   6. 横截面准备: all_dates[] 全局同步，预留 cross_sectional_cache
//
// 【线程安全】
//   - Encoding: 每个worker只写自己的asset，relaxed无锁
//   - Analysis: 只读共享状态，零锁开销
//   - RAR解压: 按archive_path加锁，细粒度并发
//
// 【性能优化】
//   - 跳过已编码文件 + 断点续传支持
//   - 日期shuffle分散RAR访问热点
//   - CPU亲和性减少cache miss
//   - Zstd解压速度 1300+ MB/s
//
// ============================================================================
// CONFIGURATION SECTION
// ============================================================================
// Modify these constants to adapt to different environments or data suppliers
namespace Config {
// Archive settings - modify these for different compression formats
const char *ARCHIVE_EXTENSION = ".rar"; // Archive file extension (.rar/.7z/.zip)
const char *ARCHIVE_TOOL = "unrar";     // Archive extraction tool (unrar/7z/unzip)
const char *ARCHIVE_EXTRACT_CMD = "x";  // Extract command (x for unrar, x for 7z)

// File extensions and names - standard CSV filenames from data supplier
const char *BIN_EXTENSION = ".bin";
// CSV filenames defined here for documentation and potential future use
// Currently the encoder auto-detects these files, but explicit names reserved for future API changes
[[maybe_unused]] constexpr const char *CSV_MARKET_DATA = "行情.csv";    // Market snapshot CSV filename
[[maybe_unused]] constexpr const char *CSV_TICK_TRADE = "逐笔成交.csv"; // Tick-by-tick trade CSV filename
[[maybe_unused]] constexpr const char *CSV_TICK_ORDER = "逐笔委托.csv"; // Tick-by-tick order CSV filename

// Path settings - modify these for your environment
constexpr const char *DEFAULT_CONFIG_FILE = "../../../../config/config.json";
constexpr const char *DEFAULT_STOCK_INFO_FILE = "../../../../config/daily_holding/asset_list.json";

constexpr const char *DEFAULT_L2_ARCHIVE_BASE = "/I/AM/A/FAKE/PATH/TO/SKIP/ARCHIVE/CHECK";
// constexpr const char *DEFAULT_L2_ARCHIVE_BASE = "/mnt/dev/sde/A_stock/L2";
// constexpr const char *DEFAULT_L2_ARCHIVE_BASE = "/media/chuyin/48ac8067-d3b7-4332-b652-45e367a1ebcc/A_stock/L2";

constexpr const char *DEFAULT_DATABASE_DIR = "../../../../output/database";
constexpr const char *DEFAULT_FEATURE_DIR = "../../../../output/features";
constexpr const char *DEFAULT_LOG_DIR = "../../../../output/log";

// Processing settings - modify for different behaviors
const bool CLEANUP_AFTER_PROCESSING = false; // Clean up temp files after processing (saves disk space)
const bool SKIP_EXISTING_BINARIES = true;    // Skip extraction/encoding if binary files exist (faster rerun)

} // namespace Config

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

int main() {
  // Load configuration paths first
  const std::string config_file = Config::DEFAULT_CONFIG_FILE;
  const std::string stock_info_file = Config::DEFAULT_STOCK_INFO_FILE;
  const std::string l2_archive_base = Config::DEFAULT_L2_ARCHIVE_BASE;
  const std::string database_dir = Config::DEFAULT_DATABASE_DIR;
  const std::string feature_dir = Config::DEFAULT_FEATURE_DIR;
  const std::string log_dir = Config::DEFAULT_LOG_DIR;

  try {
    std::cout << "=== L2 Data Processor (CSV Mode) ===" << "\n";

    const JsonConfig::AppConfig app_config = JsonConfig::ParseAppConfig(config_file);
    auto stock_info_map = JsonConfig::ParseStockInfo(stock_info_file);

    // Adjust stock dates based on config
    const std::chrono::year_month config_start{app_config.start_date.year(), app_config.start_date.month()};
    const std::chrono::year_month config_end{app_config.end_date.year(), app_config.end_date.month()};
    for (auto &[code, info] : stock_info_map) {
      if (info.start_date < config_start) {
        info.start_date = config_start;
      }
      if (!info.is_delisted) {
        info.end_date = config_end;
      }
    }

    // Print summary
    std::cout << "Configuration:" << "\n";
    std::cout << "  Archive: " << Config::ARCHIVE_TOOL << " (" << Config::ARCHIVE_EXTENSION << ")\n";
    std::cout << "  L2 base: " << l2_archive_base << "\n";
    std::cout << "  Temp dir: " << database_dir << "\n";
    std::cout << "  Period: " << JsonConfig::FormatYearMonthDay(app_config.start_date)
              << " → " << JsonConfig::FormatYearMonthDay(app_config.end_date) << "\n";
    std::cout << "  Assets: " << stock_info_map.size() << "\n";
    std::cout << "  Skip existing: " << (Config::SKIP_EXISTING_BINARIES ? "Yes" : "No") << "\n";
    std::cout << "  Auto cleanup: " << (Config::CLEANUP_AFTER_PROCESSING ? "Yes" : "No") << "\n\n";

    std::filesystem::create_directories(database_dir);
    Logger::init(log_dir);

    const unsigned int num_threads = misc::Affinity::core_count();
    const unsigned int num_workers = std::min(num_threads, static_cast<unsigned int>(stock_info_map.size()));

    std::cout << "Threads: " << num_threads;
    if (misc::Affinity::supported()) {
      std::cout << " (CPU affinity enabled)";
    }
    std::cout << "\n";
    std::cout << "Workers: " << num_workers << " (processing " << stock_info_map.size() << " assets)\n\n";

    // ========================================================================
    // STAGE 0: ARCHIVE FORMAT VALIDATION AND CONVERSION
    // ========================================================================
    if (!FileCheck::check_src_archives(l2_archive_base)) {
      return 1;
    }

    // ========================================================================
    // PHASE 1: ENCODING (can be out-of-order, uses RAR locks)
    // ========================================================================
    std::cout << "=== Phase 1: Encoding (generate binaries to output/database, run only once) ===" << "\n";

    // Build shared state
    SharedState state;

    // Step 1: Initialize global trading dates (filtered by config date range)
    const std::string config_start_str = JsonConfig::FormatYearMonthDay(app_config.start_date);
    const std::string config_end_str = JsonConfig::FormatYearMonthDay(app_config.end_date);
    state.init_dates(l2_archive_base, database_dir, config_start_str, config_end_str);

    // Step 2: Build assets with their date ranges
    for (const auto &[asset_code, stock_info] : stock_info_map) {
      const auto effective_start = std::max(std::chrono::year_month_day{stock_info.start_date / std::chrono::day{1}}, app_config.start_date);
      const auto effective_end = std::min(std::chrono::year_month_day{stock_info.end_date / std::chrono::last}, app_config.end_date);

      const std::string start_str = JsonConfig::FormatYearMonthDay(effective_start);
      const std::string end_str = JsonConfig::FormatYearMonthDay(effective_end);

      state.assets.emplace_back(state.assets.size(), asset_code, stock_info.name, start_str, end_str);
    }

    // Step 3: Initialize paths and scan existing binaries
    state.init_paths(database_dir);
    state.scan_all_existing_binaries();

    std::cout << "Global date range: " << state.all_dates.front() << " → " << state.all_dates.back()
              << " (" << state.all_dates.size() << " unique trading days)\n\n";

    std::cout << "Asset summary (可能是停牌):\n";
    for (const auto &asset : state.assets) {
      if (asset.get_missing_count() > 0) {

        std::cout << "  " << asset.asset_code << " (" << asset.asset_name << "): "
                  << asset.start_date << " → " << asset.end_date
                  << " | Total: " << asset.get_total_trading_days()
                  << ", Encoded: " << asset.get_encoded_count()
                  << ", Missing: " << asset.get_missing_count();

        const auto missing_dates = asset.get_missing_dates();
        std::cout << " [";
        const size_t show_count = std::min(size_t(5), missing_dates.size());
        for (size_t i = 0; i < show_count; ++i) {
          if (i > 0)
            std::cout << ", ";
          std::cout << missing_dates[i];
        }
        if (missing_dates.size() > show_count) {
          std::cout << ", ...";
        }
        std::cout << "]";
        std::cout << "\n";
      }
    }
    std::cout << "\n";

    // Build asset ID queue for work distribution
    std::vector<size_t> asset_id_queue;
    for (size_t i = 0; i < state.assets.size(); ++i) {
      asset_id_queue.push_back(i);
    }

    std::mutex queue_mutex;
    auto encoding_progress = std::make_shared<misc::ParallelProgress>(num_workers);
    std::vector<std::future<void>> encoding_workers;

    for (unsigned int i = 0; i < num_workers; ++i) {
      encoding_workers.push_back(
          std::async(std::launch::async, [&state, &asset_id_queue, &queue_mutex, &l2_archive_base, &database_dir, i, encoding_progress]() {
            encoding_worker(state, asset_id_queue, queue_mutex, l2_archive_base, database_dir, i, encoding_progress->get_handle(static_cast<int>(i)));
          }));
    }

    for (auto &worker : encoding_workers) {
      worker.wait();
    }
    encoding_progress->stop();

    std::cout << "\nEncoding complete:\n";
    std::cout << "  Assets: " << state.assets.size() << "\n";
    std::cout << "  Total trading days: " << state.total_trading_days() << "\n";
    std::cout << "  Encoded: " << state.total_encoded_dates()
              << " (" << (state.total_trading_days() > 0 ? 100.0 * state.total_encoded_dates() / state.total_trading_days() : 0) << "%)\n";
    std::cout << "  Missing: " << state.total_missing_dates() << "\n\n";

    // ========================================================================
    // PHASE 2: ANALYSIS
    // ========================================================================
    std::cout << "=== Phase 2: Analysis ===" << "\n";

    // Initialize global feature store
    // Analysis phase: (N-2) TS workers + 1 CS worker + 1 Flush IO worker = N total workers
    const unsigned int num_ts_workers = num_workers - 2;
    const unsigned int cs_worker_core = num_workers - 2; // Second-to-last core for CS
    const unsigned int io_worker_core = num_workers - 1; // Last core for Flush IO
    const size_t num_assets = state.assets.size();

    // Tensor pool size: small fixed size (10-20), recycled through flush_and_recycle
    // Rule of thumb: ~2x number of TS workers to allow pipeline overlap
    const size_t total_dates = state.all_dates.size();

    // Load balancing: sort assets by order count (already collected during encoding!)
    std::vector<std::pair<size_t, size_t>> asset_workloads; // (asset_id, order_count)
    asset_workloads.reserve(state.assets.size());

    for (size_t i = 0; i < state.assets.size(); ++i) {
      asset_workloads.push_back({i, state.assets[i].get_total_order_count()});
    }

    std::sort(asset_workloads.begin(), asset_workloads.end(),
              [](const auto &a, const auto &b) { return a.second > b.second; });

    // Greedy assignment: each asset goes to TS worker with minimum current load
    std::vector<size_t> worker_loads(num_ts_workers, 0);

    for (const auto &[asset_id, order_count] : asset_workloads) {
      size_t min_worker = std::min_element(worker_loads.begin(), worker_loads.end()) - worker_loads.begin();
      state.assets[asset_id].assigned_worker_id = min_worker;
      worker_loads[min_worker] += order_count;
    }

    GlobalFeatureStore feature_store(num_assets, num_ts_workers, feature_dir, static_cast<int>(cs_worker_core), static_cast<int>(io_worker_core));

    // Launch (N-2) TS workers + 1 CS worker + 1 IO worker = N total workers
    auto analysis_progress = std::make_shared<misc::ParallelProgress>(num_workers);
    std::vector<std::future<void>> workers;

    // IO worker (core N-1, last core)
    workers.push_back(std::async(std::launch::async, [&feature_store, analysis_progress, io_worker_core, total_dates]() {
      if (misc::Affinity::supported()) {
        misc::Affinity::pin_to_core(io_worker_core);
      }
      io_worker(&feature_store, analysis_progress->get_handle(static_cast<int>(io_worker_core)), total_dates, static_cast<int>(io_worker_core));
    }));

    // TS workers (cores 0 to N-3)
    for (unsigned int i = 0; i < num_ts_workers; ++i) {
      workers.push_back(std::async(std::launch::async, [&state, i, &feature_store, analysis_progress]() {
        if (misc::Affinity::supported()) {
          misc::Affinity::pin_to_core(i);
        }
        sequential_worker(state, static_cast<int>(i), &feature_store, analysis_progress->get_handle(static_cast<int>(i)));
      }));
    }

    // CS worker (core N-2, second-to-last core)
    workers.push_back(std::async(std::launch::async, [&state, &feature_store, analysis_progress, cs_worker_core]() {
      if (misc::Affinity::supported()) {
        misc::Affinity::pin_to_core(cs_worker_core);
      }
      crosssectional_worker(state, &feature_store, static_cast<int>(cs_worker_core), analysis_progress->get_handle(static_cast<int>(cs_worker_core)));
    }));

    // Wait for all workers
    for (auto &worker : workers) {
      worker.wait();
    }
    analysis_progress->stop();

    Logger::close();

    if (Config::CLEANUP_AFTER_PROCESSING) {
      if (std::filesystem::exists(database_dir)) {
        std::filesystem::remove_all(database_dir);
      }
    }

    std::cout << "\n=== Processing Complete ===" << "\n";
    return 0;

  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << "\n";
    std::cerr << "Check: config files, " << Config::ARCHIVE_TOOL << " availability, archive path\n";
    std::cerr << "Format: " << Config::DEFAULT_L2_ARCHIVE_BASE << "/YYYY/YYYYMM/YYYYMMDD" << Config::ARCHIVE_EXTENSION << "\n";
    return 1;
  }
}
