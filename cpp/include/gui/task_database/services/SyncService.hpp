// SyncService — 基本面 / 分钟线 / 概念数据集的统一后台服务: 同步 (Sync) · 核对 (Audit) · 修复 (Repair) + AssetInfo 构建
//   数据落地 = output/data/fundamental/{YYYY-MM, _meta}/<表>.parquet + YYYY-MM/bar1m/YYYY-MM-DD.parquet;
//   核对账本 = output/data/fundamental/_audit/<表>.tsv.
//   同步 / 核对 / 修复天然互斥, 共用同一 worker、同一份 Round 本地视图、同一份周配额与逐表快照.
#pragma once

#include "gui/util/Service.hpp"
#include "misc/date.hpp"
#include "shared/AssetInfo.hpp"

#include <cstddef>
#include <cstdint>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

namespace fundamental::audit {
class Book;
}

namespace GUI::Database {

std::string short_time(std::time_t t); // "MM-DD HH:MM" (本地时区)

enum class SyncOp : std::uint8_t { Sync,
                                   Audit,
                                   Repair };

struct SyncRequest {
  std::uint64_t seq = 0;    // 请求代 (StateManager 发), 交接原样带回
  SyncOp op = SyncOp::Sync; // 操作类型
  bool fundamental = false; // Sync: 联网同步基本面 (BigQuant 日频/静态表 + Tushare 事件/概念)
  bool bar1m = false;       // Sync: 联网补齐分钟线 bar1m
  std::string table;        // Audit (空 = 全部表) / Repair (目标表)
  bool incremental = false; // Audit: 跳过仍有效的已核月
};

// 同步三道 = 数据源 × 配置开关: BigQuant 轨 (bigquant → bigquant.bar1m) 与 Tushare 轨 (tushare) 并行
enum Lane : std::size_t { kLaneBq,
                          kLaneTu,
                          kLaneBar1m,
                          kLanes };
struct LaneDef {
  const char *tag;       // 日志 tag / Sync 页道名
  bool SyncRequest::*on; // 由请求的哪个开关启用
};
inline constexpr LaneDef kLaneDef[kLanes] = {{"bigquant", &SyncRequest::fundamental},
                                             {"tushare", &SyncRequest::fundamental},
                                             {"bigquant.bar1m", &SyncRequest::bar1m}};

enum class LaneRun : std::uint8_t { Idle,
                                    Queued,
                                    Syncing,
                                    Done,
                                    Cancelled };

struct LaneStat {
  LaneRun run = LaneRun::Idle;
  std::size_t total = 0, done = 0; // 计划项 / 已完成项; total 0 = 已到水位
  std::int64_t rows = 0;           // 落盘行数
  std::string current;             // 当前在拉的表名 (Sync 页整行高亮)
  std::string time;                // 收尾时刻 "MM-DD HH:MM"
};

// 单表核对统计 (仅 fundamental::audit::audited(spec) 表有效; 分母只计已核月)
struct AuditStat {
  std::int64_t local_days = 0;
  std::int64_t local_rows = 0;
  std::int64_t remote_only_days = 0;
  std::int64_t added = 0;
  std::int64_t removed = 0;
  std::size_t bad_days = 0;
  std::size_t bad_months = 0;
  std::int64_t repair_cells = 0;
  std::int64_t full_cells = 0;
  std::size_t audited_months = 0;
  std::size_t frozen_months = 0;
  std::string updated; // 最近一次核对时刻 "YYYY-MM-DD HH:MM"; 空 = 从未核对

  bool audited() const { return audited_months > 0; }
  double bad_pct() const {
    const std::int64_t den = local_days + remote_only_days;
    return den > 0 ? 100.0 * static_cast<double>(bad_days) / static_cast<double>(den) : 0.0;
  }
  double row_pct() const {
    return local_rows > 0 ? 100.0 * static_cast<double>(added + removed) / static_cast<double>(local_rows) : 0.0;
  }
};

// 单表完整状态 = 本地分片统计 + 核对统计; SyncState::tables 与 fundamental::SPECS 等长同序 (O(1) 下标对齐)
struct TableStat {
  bool present = false;
  std::size_t shards = 0; // Month: 月分片数; Daily: 日文件数; Meta: 1
  misc::Ymd last_at = 0;  // Month: YYYYMM00; Daily: YYYYMMDD; Meta: 0
  std::uint64_t bytes = 0;
  std::string mtime; // 最后落盘时刻 "MM-DD HH:MM"
  AuditStat audit;
};

struct SyncState {
  SyncOp op = SyncOp::Sync;
  std::string table; // Audit / Repair 目标表 (空 = 全部表)
  bool incremental = false;
  std::string message;     // 当前阶段 / 当前步说明
  std::string last_update; // 最近一次 AssetInfo 构建成功时刻

  // BigQuant 周配额 (单位 cell; Sync / Audit / Repair 共用同一份最新读数)
  std::int64_t quota_weekly = 0;
  std::int64_t quota_used = 0;
  std::int64_t quota_start = -1; // 最近一轮联网操作起点 used (-1 = 未取)
  std::string quota_time;        // 读数时刻 "YYYY-MM-DD HH:MM:SS"; 空 = 从未取到

  // AssetInfo 构建结果统计
  std::size_t stock_count = 0;
  std::size_t factor_stock_count = 0;
  std::size_t trading_days_count = 0;
  misc::Ymd date_range_start = 0;
  misc::Ymd date_range_end = 0;

  std::vector<TableStat> tables; // 空 = 首轮尚未扫完; 非空则长度 == SPECS.size()
  LaneStat lanes[kLanes];
};

// 交接件: rebuilt = false (只读 Audit) 时不触发 AssetInfo 替换与 L2 重扫
struct SyncResult {
  std::uint64_t seq = 0;
  bool rebuilt = false;
  bool ok = false;
  AssetInfo asset_info;
};

class SyncService : public Service<SyncService, SyncRequest>, public Published {
  friend Service<SyncService, SyncRequest>;

public:
  static constexpr const char *kName = "SyncWorker";
  SyncService();
  ~SyncService();

  SyncState state;             // UI 持 mutex 读
  Mailbox<SyncResult> handoff; // worker 每轮收尾 post, StateManager 帧首 take

private:
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return *this; }
  bool run(SyncRequest &req);
  bool run_sync(const SyncRequest &req);
  bool run_audit(const SyncRequest &req);
  bool run_repair(const SyncRequest &req);
  SyncResult build_and_publish(std::uint64_t seq, const fundamental::Round &r);

  std::unique_ptr<fundamental::audit::Book> book_; // worker 独占, 首次 run 载入
};

} // namespace GUI::Database
