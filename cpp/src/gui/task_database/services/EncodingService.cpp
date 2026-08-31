// Encoding Service Implementation
#include "gui/task_database/services/EncodingService.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "misc/archive.hpp"
#include "misc/logging.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/SharedData.hpp"
#include "worker/encoding_worker.hpp"

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <unordered_map>

namespace GUI::Database {

// 一批多少个资产 — 权衡两件事:
//   摊薄 unrar 固定开销 (进程启动 + 三万条目包头扫描): 批越大越省, 实测 20 个
//   资产一次调用比 20 次单独调用快 3.3x, 200 个模式一次调用也正常;
//   负载均衡: 批是 worker 的调度粒度, 批太大会在收尾时让部分 worker 空转.
// 内存不参与权衡 — 批内一次只持有一个文件的字节 (见 stream_archive_files).
static constexpr size_t kAssetsPerBatch = 64;

EncodingService::EncodingService(SharedData &data, TaskTerminal *term)
    : data_(data), terminal_(term) {
  // Scan operations now in Asset class
}

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

  // Launch encoding in background thread
  encoding_thread_ = std::async(std::launch::async, [this]() {
    std::cout << "\n=== Encoding Started ===\n"
              << "Workers: " << num_workers_ << " | Assets: " << data_.asset.items.size()
              << " | Dates: " << data_.asset.all_dates.size() << "\n"
              << std::endl;

    // Initialize logger for all encoding workers (shared log file)
    Logger::init(data_.config.log_dir);
    Logger::reg("encoding");

    // ------------------------------------------------------------------
    // 阶段一: 列举 — 每日包里"实际有哪些资产、每个文件多大"
    // ------------------------------------------------------------------
    //
    // 全市场下不能拿 ipo/退市区间去盲试: 那会对当天包里没有的资产各发一次
    // unrar (每次重走三万条目的包头). 先列举 (单包 ~0.4s) 拿到当天真实
    // universe, 再按 A 轴过滤掉 ETF/基金 (轴只含股票).
    //
    // 尺寸在这一步就要拿到 —— 阶段二靠它在 unrar p 的输出流上切分文件边界.
    const AssetAxis &axis = asset_axis();
    assert(data_.asset.items.size() == axis.size() &&
           "items 未与 A 轴对齐 (AssetLoader::load 没跑?)");

    std::vector<EncodeBatch> batches;
    size_t pairs = 0;
    size_t skipped = 0;
    std::unordered_map<size_t, std::string> last_date_of_asset;

    for (const auto &date_str : data_.asset.all_dates) {
      if (cancel_flag_.load())
        break;

      const std::string archive_path = Utils::generate_archive_path(
          data_.config.archive_dir, date_str, data_.config.archive_extension);
      const auto entries = misc::list_archive(archive_path, data_.config.archive_tool);
      if (entries.empty())
        continue;

      // 同一资产的委托/成交条目在包里是分开的两条, 先按资产归并
      struct Pending {
        size_t order_index = 0, trade_index = 0;
        size_t order_size = 0, trade_size = 0;
      };
      std::unordered_map<size_t, Pending> by_asset;

      for (const auto &entry : entries) {
        // "20260803/000001.SZ/逐笔委托.csv" → code_ex, filename
        const size_t first = entry.path.find('/');
        if (first == std::string::npos)
          continue;
        const size_t second = entry.path.find('/', first + 1);
        if (second == std::string::npos)
          continue;

        const std::string code_ex = entry.path.substr(first + 1, second - first - 1);
        const std::string filename = entry.path.substr(second + 1);

        const bool is_order = (filename == data_.config.csv_market_order);
        const bool is_trade = (filename == data_.config.csv_market_trade);
        if (!is_order && !is_trade)
          continue; // 行情.csv (快照) 不再编码

        const size_t asset_id = axis.find(code_ex);
        if (asset_id == axis.size())
          continue; // 非股票 (ETF/基金) 或轴外代码

        Pending &p = by_asset[asset_id];
        if (is_order) {
          p.order_index = entry.index;
          p.order_size = entry.size;
        } else {
          p.trade_index = entry.index;
          p.trade_size = entry.size;
        }
      }

      // 断点续跑: 目标文件已存在就跳过. 文件名不带条数, 所以这是一次
      // exists 而不是通配符匹配.
      std::vector<EncodeTask> tasks;
      tasks.reserve(by_asset.size());
      for (const auto &[asset_id, p] : by_asset) {
        if (p.order_size == 0)
          continue; // 没有委托文件, 无从重建盘口

        const AssetItem &asset = data_.asset.items[asset_id];
        if (skip_existing_ &&
            std::filesystem::exists(Utils::generate_orders_path(
                data_.config.orders_dir, date_str, asset.asset_code, asset.exchange,
                data_.config.binary_extension))) {
          ++skipped;
          continue;
        }

        tasks.push_back({asset_id, p.order_index, p.trade_index, p.order_size, p.trade_size,
                         /*last_for_asset=*/false});
        last_date_of_asset[asset_id] = date_str;
        ++pairs;
      }
      if (tasks.empty())
        continue;

      // 按归档序排, 再切成批 (见 encoding_worker.hpp: 一批一次 unrar p)
      std::sort(tasks.begin(), tasks.end(),
                [](const EncodeTask &a, const EncodeTask &b) { return a.order_index < b.order_index; });

      for (size_t i = 0; i < tasks.size(); i += kAssetsPerBatch) {
        EncodeBatch batch;
        batch.date = date_str;
        batch.archive_path = archive_path;
        batch.tasks.assign(tasks.begin() + static_cast<long>(i),
                           tasks.begin() + static_cast<long>(std::min(i + kAssetsPerBatch, tasks.size())));
        batches.push_back(std::move(batch));
      }
    }

    // 标记每个资产的末日任务 — 进度条上的资产计数靠它推进
    size_t assets_with_work = last_date_of_asset.size();
    for (auto &batch : batches)
      for (auto &task : batch.tasks)
        if (last_date_of_asset[task.asset_id] == batch.date)
          task.last_for_asset = true;

    std::cout << "Archive universe: " << assets_with_work << " assets / " << pairs
              << " (asset, date) pairs to encode"
              << (skipped > 0 ? " (" + std::to_string(skipped) + " already encoded, skipped)" : "")
              << "\n"
              << "Batches: " << batches.size() << " (<=" << kAssetsPerBatch << " assets each)\n"
              << std::endl;

    if (pairs == 0) {
      status_ = EncodingStatus::Completed;
      std::cout << "Nothing to encode." << std::endl;
      if (scan_callback_)
        scan_callback_();
      data_.DisableHighPerformanceMode();
      return;
    }

    std::cout << "Encoding: 逐笔二进制生成中...\n"
              << std::endl;

    // ------------------------------------------------------------------
    // 阶段二: 编码 — worker 消费批
    // ------------------------------------------------------------------
    progress_ = std::make_shared<misc::ParallelProgress>(num_workers_, 100, pairs, "pairs");
    progress_->set_summary_secondary(assets_with_work, "assets");

    BatchQueue queue(static_cast<size_t>(num_workers_) * 4);
    workers_.clear();
    workers_.reserve(num_workers_);

    for (int i = 0; i < num_workers_; ++i) {
      workers_.push_back(std::async(std::launch::async, [this, i, &queue]() {
        encoding_worker(data_, queue, &cancel_flag_, i, progress_->get_handle(i));
      }));
    }

    for (auto &batch : batches) {
      if (cancel_flag_.load())
        break;
      if (!queue.push(std::move(batch)))
        break;
    }
    queue.close();

    // Wait for completion
    for (auto &worker : workers_)
      worker.wait();
    progress_->stop();
    workers_.clear();

    // Finalize
    status_ = cancel_flag_.load() ? EncodingStatus::Cancelled : EncodingStatus::Completed;

    const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::steady_clock::now() - start_time_)
                             .count();

    std::cout << "\n=== Encoding " << (status_ == EncodingStatus::Completed ? "Complete" : "Cancelled") << " ===\n"
              << "Encoded: " << pairs << " (asset, date) pairs across " << assets_with_work
              << " assets in " << elapsed << "s" << std::endl;

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

  // Calculate total orders
  for (const auto &item : data_.asset.items) {
    prog.total_orders += item.get_total_order_count();
  }

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
