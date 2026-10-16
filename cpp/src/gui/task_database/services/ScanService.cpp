// Scan Service Implementation
#include "gui/task_database/services/ScanService.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <thread>

namespace GUI::Database {

namespace {
// 扫盘: 一天 / 一月一个 task, 每个只是一次 open + 两次 pread (头 + 尾部目录) 或一次 readdir, 冷缓存下
// 是几百个随机读, 有几个线程把盘的队列喂满就够了 (实测 24 封顶)
constexpr unsigned kScanThreads = 24;

int hw_threads() { return static_cast<int>(std::max(1u, std::thread::hardware_concurrency())); }
} // namespace

ScanService::ScanService(SharedData &data)
    : data_(data) {
  Start();
}

bool ScanService::run(ScanRequest &req) {
  namespace fs = std::filesystem;

  ScanResult r;
  r.seq = req.seq;
  DatabaseCheckResult &result = r.check;
  const Asset &asset = data_.asset;

  // ========================================
  // Phase 1: Validate config (before any FS operations)
  // ========================================
  const misc::Ymd backtest_start = data_.config.start_date;
  const misc::Ymd backtest_end = data_.config.end_date;
  assert(backtest_start && backtest_end && "回测区间未配置 (Config::Load 已校验)");

  // ========================================
  // Phase 2: Check file system
  // ========================================
  const bool binary_exists = fs::exists(data_.config.orders_dir) &&
                             !fs::is_empty(data_.config.orders_dir);
  const bool archive_exists = fs::exists(data_.config.archive_dir) &&
                              !fs::is_empty(data_.config.archive_dir);

  if (!binary_exists && !archive_exists) {
    result.status = DatabaseStatus::NoData;
    result.error_message = "No binary or archive database found";
    handoff.post(std::move(r));
    return true;
  }

  // ========================================
  // Phase 3-4: 扫盘 (RescanStorage) 或沿用上一版两源缓存 (RecomputeCoverage)
  // ========================================
  AssetScan &s = r.scan.emplace();
  if (req.mode == ScanMode::RescanStorage) {
    misc::exec::ForkJoin &io = misc::exec::ensure(io_ex_, std::min<int>(hw_threads(), kScanThreads));
    asset.scan_binary(s, data_.config.orders_dir, io, *this);
    Asset::scan_archive(s, data_.config.archive_dir, config::ARCHIVE_EXTENSION, io, *this);
  } else {
    assert((asset.binary.scanned || asset.archive.scanned) &&
           "coverage-only scan 前必须已有底层 binary/archive 扫描缓存");
    s.binary = asset.binary;
    s.archive = asset.archive;
    s.orders = asset.orders();
  }

  // ========================================
  // Phase 5: Compute backtest coverage
  // ========================================
  // 回测日历的 ground truth = 基本面交易日历 (StateManager 只在基本面交接 ok 后请求扫描)
  assert(!data_.assetinfo.get_stock_days().empty() && "基本面日历未就绪, 不应触发扫描");
  Asset::compute_backtest_coverage(s, backtest_start, backtest_end, data_.assetinfo);

  // Encode 的缺失表、Table 的 Orders%、Browser 的完整性是同一份统计, 在这里
  // 一次算完. 放在扫描里而不是各页首帧惰性算: 那是 885 天 × 5800 资产的双重
  // 遍历, 摊在渲染帧上会直接卡住一次交互. 纯 CPU, 线程数取核数.
  asset.compute_coverage_statistics(s, data_.assetinfo, misc::exec::ensure(cpu_ex_, hw_threads()), *this);

  // ========================================
  // Phase 6: Analyze and determine status
  // ========================================
  result.binary.exists = s.binary.exists;
  result.binary.path = s.binary.path;

  result.archive.exists = s.archive.exists;
  result.archive.path = s.archive.path;

  const auto &days = s.backtest.days;
  result.required_dates = days.size();
  for (const Asset::BacktestDay &day : days) {
    if (day.binary)
      result.binary.available_dates.push_back(day.date);
    if (day.archive)
      result.archive.available_dates.push_back(day.date);
    if (day.missing())
      result.missing_dates.push_back(day.date);
    if (day.can_encode())
      result.missing_can_encode.push_back(day.date);
    if (day.need_download())
      result.missing_no_archive.push_back(day.date);
  }
  result.binary_coverage = result.binary.available_dates.size();
  result.binary.total_dates = result.binary.available_dates.size();
  result.archive.total_dates = result.archive.available_dates.size();

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

  handoff.post(std::move(r));
  return true;
}

} // namespace GUI::Database
