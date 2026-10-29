// Scan Service Implementation
#include "gui/task_database/services/ScanService.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <filesystem>

namespace GUI::Database {

namespace {
// 扫盘: 一天 / 一月一个 task, 每个只是一次 open + 两次 pread (头 + 尾部目录) 或一次 readdir, 冷缓存下
// 是几百个随机读, 有几个线程把盘的队列喂满就够了 (实测 24 封顶)
constexpr unsigned kScanThreads = 24;
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
  assert(backtest_start && "回测起点未配置 (Config::Load 已校验)");

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
    misc::exec::ForkJoin &io = misc::exec::ensure(io_ex_, misc::exec::fit(kScanThreads));
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
  Asset::compute_backtest_coverage(s, backtest_start, data_.assetinfo);

  // Encode 的缺失表、Table 的 Orders%、Browser 的完整性是同一份统计, 在这里
  // 一次算完. 放在扫描里而不是各页首帧惰性算: 那是 885 天 × 5800 资产的双重
  // 遍历, 摊在渲染帧上会直接卡住一次交互. 纯 CPU, 线程数取核数.
  asset.compute_coverage_statistics(s, data_.assetinfo, misc::exec::ensure(cpu_ex_, misc::exec::hw()), *this);

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
    if (day.pending())
      result.pending.push_back(day.date);
    if (day.gap())
      result.gaps.push_back(day.date);
  }
  result.binary_coverage = result.binary.available_dates.size();
  result.binary.total_dates = result.binary.available_dates.size();
  result.archive.total_dates = result.archive.available_dates.size();

  // 交易日历为 ground truth (archive 自身缺日在 gaps 里暴露, 只提示不判状态)
  assert(result.required_dates > 0 && "start_date 之后无 L2 交易日 (start_date 晚于末个 L2 日?)");
  result.status = result.pending.empty() ? DatabaseStatus::Done : DatabaseStatus::Pending;

  handoff.post(std::move(r));
  return true;
}

} // namespace GUI::Database
