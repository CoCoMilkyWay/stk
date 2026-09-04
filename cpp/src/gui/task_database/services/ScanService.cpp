// Scan Service Implementation
#include "gui/task_database/services/ScanService.hpp"
#include "gui/coro/CoroManager.hpp"
#include "gui/task_database/infrastructure/ScanThreadPool.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <cassert>
#include <filesystem>

namespace GUI::Database {

namespace {
// 见 Phase 3 处的实测数据
constexpr unsigned kScanThreads = 24;

unsigned scan_threads() {
  const unsigned cores = std::thread::hardware_concurrency();
  return (cores > 0 && cores < kScanThreads) ? cores : kScanThreads;
}
} // namespace

ScanService::ScanService(SharedData &data, io_context &io, TaskTerminal *term)
    : data_(data), io_(io), terminal_(term) {}

void ScanService::trigger_scan(ScanMode mode) {
  // Atomic check: if already scanning, ignore request
  bool expected = false;
  if (!is_scanning_.compare_exchange_strong(expected, true)) {
    return; // Ignore concurrent requests
  }

  if (mode == ScanMode::RescanStorage) {
    // Reset scanned flags to force rescan
    data_.asset.binary.scanned = false;
    data_.asset.archive.scanned = false;
  }
  data_.asset.date_stats.clear();
  data_.asset.date_gaps.clear();
  data_.asset.asset_stats.clear();

  // Clear old result, update status
  last_check_ = DatabaseCheckResult{};
  status_ = ScanStatus::InitializingCheck;
  // 对外的"扫描进行中": UI 靠它压住重复触发. 与 is_scanning_ 同起同落 ——
  // 后者是本服务的锁, 前者是界面/任务状态看得见的那一份.
  data_.taskstate.database.l2_scan_inflight = true;

  // Launch coroutine.
  // 交给 co_spawn 的是 lambda 本身而不是它的调用结果: asio 的 awaitable 是
  // 惰性的 (initial_suspend = suspend_always), 就地调用会让协程体在闭包临时
  // 对象析构之后才首次执行 —— 捕获的 this/mode 读的是已经弹掉的栈.
  boost::asio::co_spawn(
      io_,
      [this, mode]() -> awaitable<void> {
        co_await coro_scan(mode);
        data_.taskstate.database.l2_scan_inflight = false;
        is_scanning_.store(false); // Release lock after completion
        // Notify completion
        if (on_complete_callback_) {
          on_complete_callback_();
        }
      },
      boost::asio::detached);
}

awaitable<void> ScanService::coro_scan(ScanMode mode) {
  namespace fs = std::filesystem;

  DatabaseCheckResult result;

  // ========================================
  // Phase 0: Initialization - yield immediately, let GUI render
  // ========================================
  co_await Coro::Yield(io_);

  // ========================================
  // Phase 1: Validate config (before any FS operations)
  // ========================================

  // Input: backtest_start, backtest_end
  std::string backtest_start = data_.config.start_date;
  std::string backtest_end = data_.config.end_date;

  // Convert YYYY-MM-DD to YYYYMMDD
  backtest_start.erase(std::remove(backtest_start.begin(), backtest_start.end(), '-'), backtest_start.end());
  backtest_end.erase(std::remove(backtest_end.begin(), backtest_end.end(), '-'), backtest_end.end());

  if (backtest_start.empty() || backtest_end.empty()) {
    result.status = DatabaseStatus::Error;
    result.error_message = "Backtest period not configured";
    last_check_ = result;
    status_ = ScanStatus::Error;
    co_return;
  }

  // ========================================
  // Phase 2: Check file system - update status, yield, then check
  // ========================================

  status_ = ScanStatus::CheckingFileSystem;
  co_await Coro::Yield(io_);

  bool binary_exists = fs::exists(data_.config.orders_dir) &&
                       !fs::is_empty(data_.config.orders_dir);
  bool archive_exists = fs::exists(data_.config.archive_dir) &&
                        !fs::is_empty(data_.config.archive_dir);

  // Case 3: binary_exists == false && archive_exists == false -> NoData
  if (!binary_exists && !archive_exists) {
    result.status = DatabaseStatus::NoData;
    result.error_message = "No binary or archive database found";
    last_check_ = result;
    status_ = ScanStatus::Idle;
    co_return;
  }

  if (mode == ScanMode::RecomputeCoverage) {
    assert((data_.asset.binary.scanned || data_.asset.archive.scanned) &&
           "coverage-only scan 前必须已有底层 binary/archive 扫描缓存");
  }

  // ========================================
  // Phase 3: Scan binary database - update status, yield, then scan
  // ========================================

  if (mode == ScanMode::RescanStorage) {
    status_ = ScanStatus::ScanningBinary;
    co_await Coro::Yield(io_);

    // 线程数不跟核数走: 读头的成本压在内核的路径解析上, 加线程并不摊薄.
    // 451 万文件实测 8/24/72 线程分别是 4.50 / 3.27 / 4.09 秒 —— 超过二十
    // 几个之后争用反而吃掉收益.
    auto scan_pool = std::make_shared<ScanThreadPool>(scan_threads());
    co_await data_.asset.coro_scan_binary_database(io_, data_.config.orders_dir,
                                                   config::BINARY_EXTENSION, scan_pool);
  }

  // ========================================
  // Phase 4: Scan archive database - update status, yield, then scan
  // ========================================

  if (mode == ScanMode::RescanStorage) {
    status_ = ScanStatus::ScanningArchive;
    co_await Coro::Yield(io_);

    auto scan_pool = std::make_shared<ScanThreadPool>(scan_threads());
    co_await data_.asset.coro_scan_archive_database(io_, data_.config.archive_dir,
                                                    config::ARCHIVE_EXTENSION, scan_pool);
  }

  // ========================================
  // Phase 5: Compute backtest coverage - update status, yield, then compute
  // ========================================

  status_ = ScanStatus::ComputingCoverage;
  co_await Coro::Yield(io_);

  // required_dates 的 ground truth = 基本面交易日历 (调用方保证基本面 Ready:
  // StateManager::initialize / TriggerRefreshFlow 只在 Ready 后触发扫描)
  assert(!data_.assetinfo.get_stock_days().empty() && "基本面日历未就绪, 不应触发扫描");
  co_await data_.asset.coro_compute_backtest_coverage(io_, backtest_start, backtest_end,
                                                      data_.assetinfo);

  // Encode 的缺失表、Table 的 Orders%、Browser 的完整性是同一份统计, 在这里
  // 一次算完. 放在扫描里而不是各页首帧惰性算: 那是 885 天 × 5800 资产的双重
  // 遍历, 摊在渲染帧上会直接卡住一次交互.
  co_await data_.asset.coro_compute_coverage_statistics(io_, data_.assetinfo);

  // ========================================
  // Phase 6: Analyze and determine status - update status, yield, then analyze
  // ========================================

  status_ = ScanStatus::AnalyzingStatus;
  co_await Coro::Yield(io_);

  // Populate result from Asset data
  result.binary.exists = data_.asset.binary.exists;
  result.binary.path = data_.asset.binary.path;

  result.archive.exists = data_.asset.archive.exists;
  result.archive.path = data_.asset.archive.path;

  result.required_dates = data_.asset.backtest.required_dates.size();
  result.binary_coverage = data_.asset.backtest.covered_dates.size();
  for (const auto &date : data_.asset.backtest.required_dates) {
    if (data_.asset.binary.dates.count(date))
      result.binary.available_dates.push_back(date);
    if (data_.asset.archive.dates.count(date))
      result.archive.available_dates.push_back(date);
  }
  result.binary.total_dates = result.binary.available_dates.size();
  result.archive.total_dates = result.archive.available_dates.size();

  result.missing_dates.assign(data_.asset.backtest.missing_dates.begin(),
                              data_.asset.backtest.missing_dates.end());
  result.missing_can_encode.assign(data_.asset.backtest.can_encode.begin(),
                                   data_.asset.backtest.can_encode.end());
  result.missing_no_archive.assign(data_.asset.backtest.need_download.begin(),
                                   data_.asset.backtest.need_download.end());

  // Decision Tree (覆盖判定以交易日历为 ground truth, 不再看 archive/binary 的 min/max 区间)
  assert(result.required_dates > 0 && "回测区间内无交易日 (config 日期错误?)");
  if (result.missing_dates.empty()) {
    // binary 覆盖全部交易日
    result.status = DatabaseStatus::Pass;
  } else if (!result.missing_no_archive.empty()) {
    // 有交易日既无 binary 也无 archive — 日历为准, archive 自身缺日在此暴露
    result.status = DatabaseStatus::NeedArchive;
    result.error_message = std::to_string(result.missing_no_archive.size()) +
                           " trading days have no archive";
  } else if (binary_exists) {
    // 缺失日全部可从 archive encode
    result.status = DatabaseStatus::Incomplete;
  } else {
    // binary 尚不存在, archive 覆盖完整
    result.status = DatabaseStatus::NotEncoded;
  }

  // Cache result and finalize
  last_check_ = result;
  status_ = ScanStatus::Completed;
  co_return;
}

const char *ScanService::get_status_string() const {
  // 两个扫描阶段带进度. 缓冲区是成员而非局部, 因为返回的是裸指针.
  auto with_progress = [this](const char *what) -> const char * {
    const size_t done = data_.asset.scan_days_done.load(std::memory_order_relaxed);
    const size_t total = data_.asset.scan_days_total.load(std::memory_order_relaxed);
    if (total == 0)
      return what;
    snprintf(this->status_buf_, sizeof(this->status_buf_), "%s (%zu/%zu)", what, done, total);
    return this->status_buf_;
  };

  switch (status_) {
  case ScanStatus::Idle:
    return "Idle";
  case ScanStatus::InitializingCheck:
    return "Initializing check...";
  case ScanStatus::CheckingFileSystem:
    return "Checking filesystem...";
  case ScanStatus::ScanningBinary:
    return with_progress("Scanning binary database");
  case ScanStatus::ScanningArchive:
    return with_progress("Scanning archive database");
  case ScanStatus::ComputingCoverage:
    return "Computing coverage...";
  case ScanStatus::AnalyzingStatus:
    return "Analyzing status...";
  case ScanStatus::Completed:
    return "Completed";
  case ScanStatus::Error:
    return "Error";
  default:
    return "Unknown";
  }
}

} // namespace GUI::Database
