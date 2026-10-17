// Scan Service - L2 binary / archive 扫盘 + 回测区间覆盖检查 (单 worker)
//
// 线程模型 (gui/util/Service.hpp, 随对象):
//   - GUI 线程 (StateManager): Request({seq, mode}); 帧首 handoff.take() → Asset::adopt 换进 + 留存覆盖结论
//   - worker:   run = [扫 binary → 扫 archive] (RescanStorage) → 回测日历 → 覆盖统计 → 判定, 产物整件 post
//               只读 data_.asset / data_.assetinfo (StateManager 保证扫描在飞期间 GUI 不换它们)
//   - UI:       进度面 = 自身 (Scanning: 扫盘 天 / 月; Running: 覆盖统计 交易日)
#pragma once

#include "gui/util/Service.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "shared/Asset.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// Forward declarations
struct SharedData;

namespace GUI::Database {

enum class ScanMode {
  RecomputeCoverage, // 只用已有 binary/archive 扫描缓存重算 backtest coverage
  RescanStorage      // 底层 binary/archive 可能变了, 重新逐日扫盘
};

// ============================================================================
// Database Check Types
// ============================================================================

enum class DatabaseStatus {
  Unchecked,   // Not checked yet (initial state)
  Pass,        // Binary fully covers backtest period
  Incomplete,  // Binary incomplete but can encode from archive
  NeedArchive, // Binary incomplete, missing dates have no archive
  NotEncoded,  // Binary doesn't exist, needs encoding
  NoData,      // Both binary and archive don't exist
  Error        // Configuration error or other exception
};

struct BinaryDatabaseInfo {
  bool exists = false;
  std::string path;
  size_t total_dates = 0;
  std::vector<misc::Ymd> available_dates;
};

struct ArchiveDatabaseInfo {
  bool exists = false;
  std::string path;
  size_t total_dates = 0;
  std::vector<misc::Ymd> available_dates;
};

struct DatabaseCheckResult {
  DatabaseStatus status = DatabaseStatus::Unchecked;
  std::string error_message;

  // Binary database
  BinaryDatabaseInfo binary;

  // Archive database
  ArchiveDatabaseInfo archive;

  // Coverage analysis
  size_t required_dates = 0;  // Required trading days in backtest period
  size_t binary_coverage = 0; // Binary covered dates

  // Missing details
  std::vector<misc::Ymd> missing_dates;      // All missing dates
  std::vector<misc::Ymd> missing_can_encode; // Missing but has archive
  std::vector<misc::Ymd> missing_no_archive; // Missing without archive

  // Helper: L2 覆盖检查通过 (binary 覆盖回测区间内全部交易日)
  bool is_pass() const {
    return status == DatabaseStatus::Pass;
  }

  const char *get_status_string() const {
    switch (status) {
    case DatabaseStatus::Unchecked:
      return "Not checked";
    case DatabaseStatus::Pass:
      return "Pass";
    case DatabaseStatus::Incomplete:
      return "Incomplete";
    case DatabaseStatus::NeedArchive:
      return "NeedArchive";
    case DatabaseStatus::NotEncoded:
      return "NotEncoded";
    case DatabaseStatus::NoData:
      return "NoData";
    case DatabaseStatus::Error:
      return "ERROR";
    }
    return "UNKNOWN";
  }
};

// ============================================================================
// Scan Service
// ============================================================================

struct ScanRequest {
  uint64_t seq = 0; // 请求代 (StateManager 发), 交接原样带回
  ScanMode mode = ScanMode::RecomputeCoverage;
};

// 交接件: scan 空 = 两库皆无 (NoData), Asset 不动
struct ScanResult {
  uint64_t seq = 0;
  std::optional<AssetScan> scan;
  DatabaseCheckResult check;
};

class ScanService : public Worker<ScanService, ScanRequest>, public Progress {
  friend Worker<ScanService, ScanRequest>;

public:
  static constexpr const char *kName = "ScanWorker";
  explicit ScanService(SharedData &data); // 末尾 Start
  ~ScanService() { Stop(); }

  Mailbox<ScanResult> handoff; // worker 每轮收尾 post, StateManager 帧首 take

private:
  static constexpr Phase kStart = Phase::Scanning;
  Progress &board() { return *this; }
  bool run(ScanRequest &req);

  SharedData &data_;
  // ---- worker 私有: 扫盘 (内核路径解析, 线程数按盘队列定) / 覆盖统计 (纯 CPU, 核数) 两个常驻执行器 ----
  std::unique_ptr<misc::exec::ForkJoin> io_ex_, cpu_ex_;
};

} // namespace GUI::Database
