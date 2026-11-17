// Sync Service - 基本面 + 分钟线数据同步 (BigQuant DAI + Tushare HTTP) + 概念快照 (Tushare 东财板块接口)
// 数据落地 = output/data/fundamental/YYYY-MM/*.parquet + YYYY-MM/bar1m/YYYY-MM-DD.parquet; 概念 = output/data/concept/YYYYMMDD.tsv (shared/Concept.hpp)
// (fundamental/ 月度分片 + _meta 单文件 + Daily 日文件, 水位增量, 常量见 fundamental/Spec.hpp)
// AssetInfo 从 parquet 构建 (按全局 id 稠密, 见 shared/AssetInfo.hpp).
#pragma once

#include "gui/util/Service.hpp"
#include "misc/date.hpp"
#include "shared/AssetInfo.hpp"

#include <cstddef>
#include <cstdint>
#include <ctime>
#include <string>
#include <vector>

namespace GUI::Database {

// 单表本地落盘状态 — 工作线程由 fundamental::Catalog 得到, 渲染帧只读不做 IO.
// 静态元描述 (增量键 / 抓法 / 就绪时点 / 中文说明) 不在这里重复: Sync 页直接
// 遍历 fundamental::SPECS, 按 name 关联本结构.
struct TableFileStat {
  std::string name;        // = parquet 文件名 = spec.name
  std::size_t months = 0;  // 已落月度分片数 (Daily: 日文件数); _meta 单文件表恒 0
  misc::Ym last_month = 0; // 最新分片; _meta 单文件表为 0
  misc::Ymd last_day = 0;  // Daily: 最新日文件; 其余 0
  std::uint64_t bytes = 0;
  std::string mtime; // 最后落盘时刻 "MM-DD HH:MM" (short_time)
};

std::string short_time(std::time_t t); // "MM-DD HH:MM" (本地时区)

struct SyncRequest {
  uint64_t seq = 0;         // 请求代 (StateManager 发), 交接原样带回
  bool fundamental = false; // 联网同步基本面 (BigQuant 日频表 + Tushare) + 概念快照
  bool bar1m = false;       // 联网补齐分钟线 (基本面之后)
};

// 同步道 = 一个接口的一类数据 (两源 × 两类); 道序即 Update 的执行序
//   Sync 页各源分表的表头逐道列出 (BigQuant: 日频表 / 分钟线; Tushare: 事件表 / 概念)
enum Lane : std::size_t { kLaneBq,
                          kLaneTu,
                          kLaneConcept,
                          kLaneBar1m,
                          kLanes };
struct LaneDef {
  const char *tag;       // 日志 tag / Sync 页道名
  bool SyncRequest::*on; // 由请求的哪个开关开
};
inline constexpr LaneDef kLaneDef[kLanes] = {{"bigquant", &SyncRequest::fundamental},
                                             {"tushare", &SyncRequest::fundamental},
                                             {"tushare.concept", &SyncRequest::fundamental},
                                             {"bigquant.bar1m", &SyncRequest::bar1m}};
inline constexpr const char *kConceptTable = "concept"; // 概念快照在 Tushare 分表里的表名

// 一道最近一次联网同步: Idle = 本进程未联网同步过; Queued = 本轮排在前道之后
enum class LaneRun : std::uint8_t { Idle,
                                    Queued,
                                    Syncing,
                                    Done,
                                    Cancelled };

struct LaneStat {
  LaneRun run = LaneRun::Idle;
  std::size_t total = 0, done = 0; // 计划项 / 已完成项; total 0 = 已到水位
  std::int64_t rows = 0;           // 落盘行数 (概念: 收进快照的概念数)
  std::string time;                // 收尾时刻 "MM-DD HH:MM"
};

// 展示字段 (worker 持 mutex 写, UI 持 mutex 读); 阶段与计步在进度面上
struct SyncState {
  std::string message;     // 当前阶段说明 (工作线程实时更新)
  std::string last_update; // 最近一次构建成功时刻

  // BigQuant 周配额 (单位 cell; Sync 页常显)
  //   每轮联网同步开头 preflight 一次, 收尾再取一次; 未联网的轮次沿用上次
  std::int64_t quota_weekly = 0;
  std::int64_t quota_used = 0;
  std::string quota_time; // 读数时刻 "YYYY-MM-DD HH:MM:SS"; 空 = 从未取到

  // 构建结果统计 (最近一次构建成功的)
  std::size_t stock_count = 0;        // cn_stock_basic_info 收录的股票数 (StockInfo::known)
  std::size_t factor_stock_count = 0; // 有复权因子序列的股票数
  std::size_t trading_days_count = 0; // 交易日历天数
  misc::Ymd date_range_start = 0;     // D 轴范围
  misc::Ymd date_range_end = 0;

  // 逐表落盘状态 (构建失败也填 — 正是要看清哪张表缺); 概念快照记作表 kConceptTable (一份 = 一个日分片)
  std::vector<TableFileStat> tables;
  LaneStat lanes[kLanes];
  std::string syncing; // 在拉的表 (spec.name / kConceptTable); 空 = 无

  // tables 里 name == 的那条; 缺失返回 nullptr
  const TableFileStat *find_table(const std::string &name) const;
};

// 交接件: ok = false ⇒ 本地 parquet 缺失 (需先联网同步), asset_info 空
struct SyncResult {
  uint64_t seq = 0;
  bool ok = false;
  AssetInfo asset_info;
};

// ============================================================================
// SyncService — 四道数据源同步 + AssetInfo 构建 (单 worker; 阻塞网络 + parquet IO)
//
// 线程模型 (gui/util/Service.hpp, 随对象):
//   - GUI 线程 (StateManager): Request; 帧首 handoff.take() → ok 则换进 SharedData::asset_info (只在 GUI 线程替换)
//   - worker:   run = 本轮视图 Round → 开道的计划 (纯本地)
//               → 四道依次 drive [bigquant → tushare → tushare.concept → bigquant.bar1m] → 构建 (扫描 → 落地) → post
//               (两开关皆关 ⇒ 零网络直接本地构建). 取消 = 当前任务的查询跑完即停, 余下同步任务全部跳过, 构建照常
//               各道进退写 state.lanes (持 mutex)
//   - UI:       进度面 = 自身 (Published): Loading 同步 / Running 构建, done/total = 任务 / 分片;
//               state 持 mutex 读
// ============================================================================
class SyncService : public Service<SyncService, SyncRequest>, public Published {
  friend Service<SyncService, SyncRequest>;

public:
  static constexpr const char *kName = "SyncWorker";
  SyncService(); // 末尾 Start
  ~SyncService() { Stop(); }

  SyncState state;             // UI 持 mutex 读
  Mailbox<SyncResult> handoff; // worker 每轮收尾 post, StateManager 帧首 take

private:
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return *this; }
  bool run(SyncRequest &req);
};

} // namespace GUI::Database
