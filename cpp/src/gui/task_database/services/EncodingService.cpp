// Encoding Service Implementation
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "misc/cross_platform.hpp"
#include "misc/logging.hpp"
#include "shared/SharedData.hpp"
#include "worker/encoding_worker.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cassert>
#include <iostream>

namespace GUI::Database {

EncodingService::EncodingService(SharedData &data, TaskTerminal *term)
    : data_(data), terminal_(term) {
  // Scan operations now in Asset class
}

EncodingService::~EncodingService() = default;

void EncodingService::start_encoding(int num_workers, bool skip_existing) {
  if (status_ == EncodingStatus::Running)
    return;

  status_ = EncodingStatus::Running;
  cancel_flag_.store(false);
  num_workers_ = num_workers;
  skip_existing_ = skip_existing;
  start_time_ = std::chrono::steady_clock::now();

  // Enable High Performance Mode: GUI sleeps, all CPU for encoding
  data_.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI thread sleeping\n"
            << std::endl;

  // 大内存假设 (不为小机器妥协): 各 worker 并发解压+解码的工作集要装得下.
  assert(physical_ram_bytes() >= (size_t(16) << 30) &&
         "encoding 需要 ≥16GB 物理内存");

  // 容量即流水深度: ~85 批/天, 256 ≈ 领先 2-3 天. 批是纯元数据, 队列本身
  // 不占什么内存.
  queue_ = std::make_unique<BatchQueue>(256);

  // Launch encoding in background thread
  encoding_thread_ = std::async(std::launch::async, [this]() {
    std::cout << "\n=== Encoding Started ===\n"
              << "Workers: " << num_workers_ << " | Assets: " << data_.asset.items.size()
              << " | Dates: " << data_.asset.all_dates.size() << "\n"
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
    // 汇总行以天为单位, 总量 = 全部日期数 (开跑即精确);
    // 附注显示最老在编天的资产进度, 由 producer/worker 维护.
    progress_ = std::make_shared<misc::ParallelProgress>(num_workers_, 100, "days");
    progress_->set_summary_total(data_.asset.all_dates.size(), true);

    EncodeStats stats;

    workers_.clear();
    workers_.reserve(num_workers_);
    for (int i = 0; i < num_workers_; ++i) {
      workers_.push_back(std::async(std::launch::async, [this, i, &stats]() {
        encoding_worker(data_, *queue_, &cancel_flag_, stats, i, progress_->get_handle(i));
      }));
    }

    auto producer = std::async(std::launch::async, [this, &stats]() {
      encoding_producer(data_, *queue_, &cancel_flag_, skip_existing_, stats,
                        static_cast<size_t>(num_workers_), progress_.get());
    });

    producer.wait();
    queue_->close();

    for (auto &worker : workers_)
      worker.wait();
    progress_->stop();
    workers_.clear();

    // Finalize
    const size_t pairs = stats.pairs_listed.load();
    const size_t skipped = stats.pairs_skipped.load();
    const size_t days_skipped = stats.days_skipped.load();
    status_ = cancel_flag_.load() ? EncodingStatus::Cancelled : EncodingStatus::Completed;

    const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::steady_clock::now() - start_time_)
                             .count();

    std::cout << "\n=== Encoding " << (status_ == EncodingStatus::Completed ? "Complete" : "Cancelled") << " ===\n"
              << "Encoded: " << pairs << " (asset, date) pairs across "
              << stats.assets_with_work.size() << " assets in " << elapsed << "s"
              << (skipped > 0 ? " (" + std::to_string(skipped) + " pairs already encoded, skipped)" : "")
              << (days_skipped > 0 ? " (" + std::to_string(days_skipped) + " days skipped via completion marker)" : "")
              << std::endl;

    // 源损坏的都被跳过留在了日志里 (从不 abort): 醒目提示一下, 修完源文件
    // 直接重跑增量即可 —— 这些 (资产, 日期) 既没有产物也没有完成标记.
    const size_t pairs_corrupt = stats.pairs_corrupt.load();
    const size_t days_corrupt = stats.days_corrupt.load();
    if (pairs_corrupt > 0 || days_corrupt > 0) {
      std::cout << "CORRUPT SOURCE: " << pairs_corrupt << " (asset, date) pairs + " << days_corrupt
                << " whole days skipped — grep '[CORRUPT' in " << data_.config.log_dir
                << "/encoding.log, repair, then re-run incremental\n"
                << std::endl;
    }

    // 准入校验未过的同理: 逐笔流本身缺片 (见 codec/L2_Validator.hpp), 数据源
    // 侧的问题, 编码器无从修复. 核查/换源之后重跑增量即可.
    const size_t pairs_invalid = stats.pairs_invalid.load();
    if (pairs_invalid > 0) {
      std::cout << "INVALID DATA: " << pairs_invalid << " (asset, date) pairs failed validation — grep '[INVALID' in "
                << data_.config.log_dir << "/encoding.log, check source, then re-run incremental\n"
                << std::endl;
    }

    // 把本轮动过的天交给增量扫描 (见 Asset::binary.dirty_dates)
    {
      std::lock_guard<std::mutex> lock(stats.days_mutex);
      data_.asset.binary.dirty_dates.insert(stats.days_touched.begin(),
                                            stats.days_touched.end());
    }

    // Trigger scan callback after encoding completion
    if (scan_callback_) {
      scan_callback_();
    }

    // Disable High Performance Mode: GUI resumes
    data_.DisableHighPerformanceMode();
    std::cout << "\n[High Performance Mode] Disabled - GUI thread resumed\n"
              << std::endl;
  });
}

void EncodingService::stop_encoding() {
  if (status_ != EncodingStatus::Running) {
    return;
  }

  cancel_flag_.store(true);
  if (queue_)
    queue_->close(); // 唤醒被背压堵住的 producer 与等批的 worker
  std::cout << "[Encoding] Cancelling..." << std::endl;

  // Wait for encoding thread to finish (which will also wait for workers)
  if (encoding_thread_.valid()) {
    encoding_thread_.wait();
  }

  // Clear any remaining worker futures (though they should already be cleared in the encoding thread)
  workers_.clear();
}

EncodingProgress EncodingService::get_progress() const {
  EncodingProgress prog;
  prog.total_assets = data_.asset.items.size();
  prog.total_dates = data_.asset.all_dates.size();
  prog.encoded_dates = data_.asset.binary.dates.size();
  prog.completed_assets = 0;

  // 扫描时已累加好 (见 coro_scan_binary_database). 这里每帧都会被 Encode 页
  // 调一次, 逐帧遍历 items[].date_info (资产数 × 交易日数, 五百万量级) 会把
  // 帧时间拖到几百毫秒 —— 与 Asset::asset_stats 同一个坑.
  prog.total_orders = data_.asset.binary.total_orders;

  if (status_ == EncodingStatus::Running) {
    prog.elapsed_seconds = std::chrono::duration<float>(
                               std::chrono::steady_clock::now() - start_time_)
                               .count();
    prog.encoding_rate = prog.elapsed_seconds > 0 ? prog.completed_assets / prog.elapsed_seconds : 0;
  }

  return prog;
}

void EncodingService::run_file_check(const std::string &archive_base_dir) {
  if (!terminal_)
    return;

  if (file_check_running_.load()) {
    terminal_->AddLine("[File Check] Already running, please wait...", Color::Yellow());
    return;
  }

  file_check_running_.store(true);
  file_check_thread_ = std::async(std::launch::async, [this, archive_base_dir]() {
    run_file_check_async(archive_base_dir);
    file_check_running_.store(false);
  });
}

void EncodingService::run_file_check_async(const std::string &archive_base_dir) {
  terminal_->AddLine("========================================");
  terminal_->AddLine("[File Check] Starting Archive Validation");
  terminal_->AddLine("========================================");
  terminal_->AddLine("[File Check] Archive path: " + archive_base_dir);
  terminal_->AddLine("");

  // Step 1: Check directory exists
  terminal_->AddLine("[File Check] Step 1: Checking archive directory...");

  // Each probe (unrar lb) does O(entries) scattered read+lseek pairs across
  // the whole archive to walk its header chain (measured via strace: ~6500
  // read+lseek pairs for a 3270-entry / 3.9GB archive). The archive store is
  // a single-actuator spinning disk, so probes run sequentially -- running
  // several concurrently thrashes the disk head (measured 359x slowdown).
  auto progress = [this](size_t done, size_t total, const std::string &path) {
    terminal_->AddLine("[File Check]   (" + std::to_string(done) + "/" +
                       std::to_string(total) + ") " + path);
  };

  FileCheck::FileCheckResult local_result =
      FileCheck::check_src_archives(archive_base_dir, progress);

  if (!local_result.archive_dir_exists) {
    terminal_->AddLine("[File Check] ✗ Archive directory does not exist", Color::Yellow());
    terminal_->AddLine("[File Check] Will use built binaries instead");
    terminal_->AddLine("========================================");
    file_check_result_ = local_result;
    return;
  }

  terminal_->AddLine("[File Check] ✓ Archive directory exists", Color::Green());
  terminal_->AddLine("");

  // Step 2: Check required commands
  terminal_->AddLine("[File Check] Step 2: Checking required commands (unrar, 7z, rar, gdb)...");
  if (!local_result.commands_available) {
    terminal_->AddLine("[File Check] ✗ Some required commands are missing", Color::Red());
    terminal_->AddLine("[File Check] Please install: unrar, 7z, rar, gdb");
    terminal_->AddLine("========================================");
    file_check_result_ = local_result;
    return;
  }
  terminal_->AddLine("[File Check] ✓ All required commands available", Color::Green());
  terminal_->AddLine("");

  // Step 3: Scan archives
  terminal_->AddLine("[File Check] Step 3: Scanning archive files...");
  terminal_->AddLine("[File Check] Total archives found: " + std::to_string(local_result.total_archives), Color::Green());
  terminal_->AddLine("");

  // Step 4-7: Validate naming, format, structure, ZIP files
  auto print_errors = [this](const std::string &step, const std::string &desc, size_t count,
                             const std::vector<std::string> &files, const std::string &fix = "") {
    terminal_->AddLine("[File Check] " + step + ": " + desc + "...");
    if (count > 0) {
      terminal_->AddLine("[File Check] ✗ Found " + std::to_string(count) + " error(s)", Color::Red());
      if (!fix.empty())
        terminal_->AddLine("[File Check]   Fix: " + fix);
      for (const auto &file : files) {
        terminal_->AddLine("[File Check]   - " + file, Color::Yellow());
      }
    } else {
      terminal_->AddLine("[File Check] ✓ All correct", Color::Green());
    }
    terminal_->AddLine("");
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
  terminal_->AddLine("========================================");
  if (local_result.passed) {
    terminal_->AddLine("[File Check] ✓ ALL CHECKS PASSED", Color::Green());
    terminal_->AddLine("[File Check] Valid archives: " + std::to_string(local_result.valid_archives));
  } else {
    terminal_->AddLine("[File Check] ✗ SOME CHECKS FAILED", Color::Red());
    terminal_->AddLine("[File Check] Valid: " + std::to_string(local_result.valid_archives) +
                       " / Total: " + std::to_string(local_result.total_archives));
    terminal_->AddLine("[File Check] Total errors: " + std::to_string(
                                                           local_result.naming_errors + local_result.format_errors +
                                                           local_result.structure_errors + local_result.integrity_errors +
                                                           local_result.zip_files));
  }
  terminal_->AddLine("========================================");

  // Publish result once, atomically, at the end.
  file_check_result_ = local_result;
}

} // namespace GUI::Database
