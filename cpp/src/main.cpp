#include "worker/crosssectional_worker.hpp"
#include "worker/encoding_worker.hpp"
#include "worker/io_worker.hpp"
#include "worker/sequential_worker.hpp"
#include "shared/SharedData.hpp"

#include "codec/json_config.hpp"
#include "features/backend/FeatureStore.hpp"
#include "gui/Gui.hpp"
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
// L2数据处理架构
// ============================================================================
//
//   - 统一管理所有共享状态的 SharedState:
//       · 资产信息 assets[]:存储所有资产的元数据、每日统计、文件路径及状态位图,支持断点续传和精确状态追踪(date_info.encoded / date_info.analyzed)
//
//   - Phase 1 (Encoding):
//       · 资产并行处理,日期顺序打乱(shuffle)以分散RAR压缩包访问压力,实现负载均衡(利用已累积的order_count,无需预扫描)
//       · 按archive_path加锁保证RAR解压的细粒度并发,阻塞等待锁避免同一压缩包并发解压冲突
//       · 零重复扫描:在Encoding阶段直接统计order_count及文件路径,缓存到 asset.date_info[],Analysis阶段无须额外扫描
//       · 路径缓存与类型缓存:所有文件路径初始化时生成并缓存,exchange_type推导一次,避免字符串重复解析
//       · 每个worker只写所属asset,使用relaxed无锁操作,保证线程安全且无锁开销
//       · 支持跳过已编码文件,提升断点续传效率
//       · 日期shuffle结合CPU亲和性减少缓存未命中,提升处理性能
//
//   - Phase 2 (Analysis):
//       · 以全局日期顺序(all_dates[])为主线,所有worker同步推进,方便横截面因子计算和缓存共享
//       · 无锁读取共享状态,所有路径及统计信息在Phase 1已缓存,避免重复IO和扫描
//       · 处理流程:Binary文件 → 解压解码(Zstd解压速度1300+ MB/s)→ Order Book还原 → 特征提取 → 写入FeatureStore
//
//   - 线程安全设计细节:
//       · Encoding阶段为写隔离(每线程写自己asset),无锁relaxed操作
//       · Analysis阶段为只读共享,零锁访问
//       · RAR解压采用基于archive_path的细粒度加锁,最大化并行度
//

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

int main() {
  GUI::RunGUI();
/*
  std::cout << "=== L2 Data Processor (CSV Mode) ===" << "\n";

  const JsonConfig::AppConfig app_config = JsonConfig::ParseAppConfig("../../../../config/config.json");
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
  std::cout << "=== Phase 1: Encoding ===" << "\n";

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

  std::cout << "Encoding: 二进制数据库创建中, 下次运行可直接快速读取...\n";

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
  return 0;
  */
}
