// Encoding Service Implementation
#include "gui/task_database/services/EncodingService.hpp"
#include "misc/cross_platform.hpp"
#include "misc/logging.hpp"
#include "misc/progress_parallel.hpp"
#include "shared/SharedData.hpp"
#include "worker/encoding_worker.hpp"

#include <cassert>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <vector>

namespace GUI::Database {

EncodingService::EncodingService(SharedData &data)
    : data_(data) {
  Start();
}

// 异常不捕获 (fail fast). 取消也 post 交接: 日文件可能已被 rename 换掉, 须重扫
bool EncodingService::run(EncodeRequest &req) {
  assert(req.workers > 0 && "encoding: workers 须 > 0");
  const auto start_time = std::chrono::steady_clock::now();
  const size_t target_dates = data_.asset.backtest.days.size();
  assert(target_dates > 0 && "编码前必须先完成回测区间覆盖扫描");
  // 大内存假设 (不为小机器妥协): 各 worker 并发解压+解码的工作集要装得下.
  assert(physical_ram_bytes() >= (size_t(16) << 30) &&
         "encoding 需要 ≥16GB 物理内存");

  // Enable High Performance Mode: GUI low-refresh, workers own cores
  data_.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI low-refresh\n"
            << std::endl;

  // 编码会 rename 换掉日文件, 帧目录快照里的偏移随之作废: 先撤下, 收尾交接后的重扫描再发布新的.
  data_.asset.publish_orders(std::make_shared<const OrdersCatalog>());

  begin(Phase::Running, target_dates);

  std::cout << "\n=== Encoding Started ===\n"
            << "Workers: " << req.workers << " | Assets: " << data_.asset.items.size()
            << " | Backtest dates: " << target_dates << "\n"
            << std::endl;

  // Initialize logger for all encoding workers (shared log file)
  Logger::init(data_.config.log_dir);
  Logger::reg("encoding");

  std::cout << "Encoding: 逐笔二进制生成中 (按天流水: 列举 → 并行解码)...\n"
            << std::endl;

  // ------------------------------------------------------------------
  // 流水线: producer 按天 [列举 → 推元数据批], worker 每批自己 unrar +
  // 解码. 解压在 worker 侧并行 — 单条 unrar 流喂不满几十个核, 归档在
  // NVMe 上多路并发直读也没有寻道代价 (见 encoding_worker.hpp).
  // ------------------------------------------------------------------
  // 容量即流水深度: ~85 批/天, 256 ≈ 领先 2-3 天. 批是纯元数据, 队列本身
  // 不占什么内存.
  BatchQueue queue(256);
  // 汇总行以天为单位, 总量 = 回测区间交易日;
  // 附注显示最老在编天的资产进度, 由 producer/worker 维护.
  misc::ParallelProgress progress(req.workers, 100, "days");
  progress.set_summary_total(target_dates, true);

  EncodeStats stats;

  std::vector<std::future<void>> workers;
  workers.reserve(req.workers);
  for (int i = 0; i < req.workers; ++i) {
    workers.push_back(std::async(std::launch::async, [this, i, &queue, &stats, &progress]() {
      encoding_worker(data_, queue, &cancel_, stats, i, progress.get_handle(i));
    }));
  }

  auto producer = std::async(std::launch::async, [this, &req, &queue, &stats, &progress]() {
    encoding_producer(data_, queue, &cancel_, req.skip_existing, stats,
                      static_cast<size_t>(req.workers), &progress);
  });

  // 盯流水: 汇总天数镜像进进度面; 见取消即 close 队列 (唤醒被背压堵住的 producer 与等批的 worker)
  auto mirror = [this, &progress]() { publish(progress.summary_done() - done.load(std::memory_order_relaxed)); };
  bool closed = false;
  while (producer.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready) {
    mirror();
    if (!closed && cancel_.load(std::memory_order_relaxed)) {
      queue.close();
      closed = true;
      std::cout << "[Encoding] Cancelling..." << std::endl;
    }
  }
  queue.close();

  for (auto &worker : workers)
    worker.wait();
  progress.stop();
  mirror();

  // Finalize
  const size_t pairs = stats.pairs_listed.load();
  const size_t skipped = stats.pairs_skipped.load();
  const size_t days_skipped = stats.days_skipped.load();
  const bool completed = !cancel_.load(std::memory_order_relaxed);

  const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::steady_clock::now() - start_time)
                           .count();

  std::cout << "\n=== Encoding " << (completed ? "Complete" : "Cancelled") << " ===\n"
            << "Encoded: " << pairs << " (asset, date) pairs across "
            << stats.assets_with_work.size() << " assets in " << elapsed << "s"
            << (skipped > 0 ? " (" + std::to_string(skipped) + " pairs already encoded, skipped)" : "")
            << (days_skipped > 0 ? " (" + std::to_string(days_skipped) + " days skipped via completion marker)" : "")
            << std::endl;

  // 源损坏的都被跳过留在了日志里 (从不 abort): 醒目提示一下, 修完源文件
  // 直接重跑增量即可 —— 这些 (资产, 日期) 既没有产物也没有完成标记.
  const size_t pairs_corrupt = stats.run.count(L2::Outcome::Corrupt);
  const size_t days_corrupt = stats.days_corrupt.load();
  if (pairs_corrupt > 0 || days_corrupt > 0) {
    std::cout << "CORRUPT SOURCE: " << pairs_corrupt << " (asset, date) pairs + " << days_corrupt
              << " whole days skipped — grep '[CORRUPT' in " << data_.config.log_dir
              << "/encoding.log, repair, then re-run incremental\n"
              << std::endl;
  }

  // 准入校验未过的同理: 逐笔流本身缺片 (见 codec/L2_Validator.hpp), 数据源
  // 侧的问题, 编码器无从修复. 核查/换源之后重跑增量即可.
  const size_t pairs_invalid = stats.run.count(L2::Outcome::Invalid);
  if (pairs_invalid > 0) {
    std::cout << "INVALID DATA: " << pairs_invalid << " (asset, date) pairs failed validation — grep '[INVALID' in "
              << data_.config.log_dir << "/encoding.log, check source, then re-run incremental\n"
              << std::endl;
  }

  // Disable High Performance Mode: GUI full-refresh
  data_.DisableHighPerformanceMode();
  std::cout << "\n[High Performance Mode] Disabled - GUI full-refresh\n"
            << std::endl;

  // 交接 → StateManager 重扫 (扫描整体重建, 每天重读一遍日文件目录, 不用在这里告诉它动过哪些天)
  handoff.post(req.seq);
  return completed;
}

bool FileCheckService::run(std::string &archive_base_dir) {
  std::cout << "========================================\n"
            << "[File Check] Starting Archive Validation\n"
            << "========================================\n"
            << "[File Check] Archive path: " << archive_base_dir << "\n"
            << std::endl;

  // Step 1: Check directory exists
  std::cout << "[File Check] Step 1: Checking archive directory..." << std::endl;

  // Each probe (unrar lb) does O(entries) scattered read+lseek pairs across
  // the whole archive to walk its header chain (measured via strace: ~6500
  // read+lseek pairs for a 3270-entry / 3.9GB archive). The archive store is
  // a single-actuator spinning disk, so probes run sequentially -- running
  // several concurrently thrashes the disk head (measured 359x slowdown).
  auto progress = [this](size_t n_done, size_t n_total, const std::string &path) {
    if (n_done == 1)
      begin(Phase::Scanning, n_total);
    publish(1);
    std::cout << "[File Check]   (" << n_done << "/" << n_total << ") " << path << std::endl;
  };

  FileCheck::FileCheckResult local_result =
      FileCheck::check_src_archives(archive_base_dir, progress);

  if (!local_result.archive_dir_exists) {
    std::cout << "[File Check] ✗ Archive directory does not exist\n"
              << "[File Check] Will use built binaries instead\n"
              << "========================================" << std::endl;
    handoff.post(std::move(local_result));
    return true;
  }

  std::cout << "[File Check] ✓ Archive directory exists\n"
            << std::endl;

  // Step 2: Check required commands
  std::cout << "[File Check] Step 2: Checking required commands (unrar, 7z, rar, gdb)..." << std::endl;
  if (!local_result.commands_available) {
    std::cout << "[File Check] ✗ Some required commands are missing\n"
              << "[File Check] Please install: unrar, 7z, rar, gdb\n"
              << "========================================" << std::endl;
    handoff.post(std::move(local_result));
    return true;
  }
  std::cout << "[File Check] ✓ All required commands available\n"
            << std::endl;

  // Step 3: Scan archives
  std::cout << "[File Check] Step 3: Scanning archive files...\n"
            << "[File Check] Total archives found: " << local_result.total_archives << "\n"
            << std::endl;

  // Step 4-7: Validate naming, format, structure, ZIP files
  auto print_errors = [](const std::string &step, const std::string &desc, size_t count,
                         const std::vector<std::string> &files, const std::string &fix = "") {
    std::cout << "[File Check] " << step << ": " << desc << "..." << std::endl;
    if (count > 0) {
      std::cout << "[File Check] ✗ Found " << count << " error(s)" << std::endl;
      if (!fix.empty())
        std::cout << "[File Check]   Fix: " << fix << std::endl;
      for (const auto &file : files) {
        std::cout << "[File Check]   - " << file << std::endl;
      }
    } else {
      std::cout << "[File Check] ✓ All correct" << std::endl;
    }
    std::cout << std::endl;
  };

  print_errors("Step 4", "Checking archive naming (YYYY/YYYYMM/YYYYMMDD.rar)",
               local_result.naming_errors, local_result.naming_error_files);

  print_errors("Step 5", "Checking archive format (RAR non-solid)",
               local_result.format_errors, local_result.format_error_files,
               "Run py/app/FileRepair/fix_to_rar.py or fix_solid_to_nonsolid.py");

  print_errors("Step 6", "Checking internal structure (YYYYMMDD/asset_code/*.csv)",
               local_result.structure_errors, local_result.structure_error_files,
               "Run py/app/FileRepair/fix_archive_structure.py");

  print_errors("Step 6b", "Checking archive integrity (truncated / corrupt headers)",
               local_result.integrity_errors, local_result.integrity_error_files,
               "Re-download or re-create the archive");

  print_errors("Step 7", "Checking for ZIP files (should be RAR)",
               local_result.zip_files, local_result.zip_error_files,
               "Run py/app/FileRepair/fix_to_rar.py");

  // Summary
  std::cout << "========================================" << std::endl;
  if (local_result.passed) {
    std::cout << "[File Check] ✓ ALL CHECKS PASSED\n"
              << "[File Check] Valid archives: " << local_result.valid_archives << std::endl;
  } else {
    std::cout << "[File Check] ✗ SOME CHECKS FAILED\n"
              << "[File Check] Valid: " << local_result.valid_archives
              << " / Total: " << local_result.total_archives << "\n"
              << "[File Check] Total errors: " << (local_result.naming_errors + local_result.format_errors + local_result.structure_errors + local_result.integrity_errors + local_result.zip_files)
              << std::endl;
  }
  std::cout << "========================================" << std::endl;

  // Publish result once, atomically, at the end.
  handoff.post(std::move(local_result));
  return true;
}

} // namespace GUI::Database
