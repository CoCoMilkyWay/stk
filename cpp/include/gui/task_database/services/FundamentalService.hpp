// Fundamental Service - 基本面 + 分钟线数据 sync (BigQuant DAI + Tushare HTTP) + 概念快照 (东财 HTTP)
// 数据落地 = output/fundamental/YYYY-MM/*.parquet + YYYY-MM/bar1m/YYYY-MM-DD.parquet; 概念 = output/concept/YYYYMMDD.tsv (shared/Concept.hpp)
// (fundamental/ 月度分片 + _meta 单文件 + Daily 日文件, 水位增量, 常量见 shared/Config.hpp)
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
// 静态元描述 (增量键 / 抓法 / 就绪时点 / 中文说明) 不在这里重复: Fundamental 页直接
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

// 同步道 = 一个接口的一类数据; 道序即 Update 的执行序, 日志 tag = kLaneTag
//   Fundamental 页各源分表的表头逐道列出 (BigQuant 两道: 基本面 / 分钟线)
enum Lane : std::size_t { kLaneBq,
                          kLaneTu,
                          kLaneConcept,
                          kLaneBar1m,
                          kLanes };
inline constexpr const char *kLaneTag[kLanes] = {"bigquant", "tushare", "eastmoney.concept", "bigquant.bar1m"};
inline constexpr const char *kConceptTable = "concept"; // 东财分表的表名 (概念快照)

// 一道最近一次联网同步: Idle = 本进程未联网同步过; Queued = 本轮排在前道之后
enum class LaneRun : std::uint8_t { Idle,
                                    Queued,
                                    Syncing,
                                    Done,
                                    Cancelled };

struct LaneStat {
  LaneRun run = LaneRun::Idle;
  std::size_t total = 0, done = 0; // 计划项 / 已完成项; total 0 = 已到水位 (概念: 当日已有)
  std::int64_t rows = 0;           // 落盘行数 (概念: 收进快照的概念数)
  std::string time;                // 收尾时刻 "MM-DD HH:MM"
};

// 展示字段 (worker 持 mutex 写, UI 持 mutex 读); 阶段与计步在进度面上
struct FundamentalState {
  std::string message;     // 当前阶段说明 (工作线程实时更新)
  std::string last_update; // 最近一次构建成功时刻

  // BigQuant 周配额 (preflight 结果, 单位 cell; Fundamental 页常显)
  //   每轮联网同步开头 preflight 一次, 每个 BigQuant 任务后再取; 未联网的轮次沿用上次
  std::int64_t quota_weekly = 0;
  std::int64_t quota_used = 0;
  std::string quota_time; // 读数时刻 "YYYY-MM-DD HH:MM:SS"; 空 = 从未取到

  // 构建结果统计 (最近一次构建成功的)
  std::size_t stock_count = 0;        // cn_stock_basic_info 收录的股票数 (StockInfo::known)
  std::size_t factor_stock_count = 0; // 有复权因子序列的股票数
  std::size_t trading_days_count = 0; // 交易日历天数
  misc::Ymd date_range_start = 0;     // D 轴范围
  misc::Ymd date_range_end = 0;

  // 逐表落盘状态 (构建失败也填 — 正是要看清哪张表缺)
  std::vector<TableFileStat> tables;
  TableFileStat concept_files; // 概念快照 (output/concept, 一份 = 一个日分片); 无快照 → months 0
  LaneStat lanes[kLanes];
  std::string syncing; // 在拉的表 (spec.name / kConceptTable); 空 = 无

  // tables 里 name == 的那条; 缺失返回 nullptr
  const TableFileStat *find_table(const std::string &name) const;
};

struct FundamentalRequest {
  uint64_t seq = 0;         // 请求代 (StateManager 发), 交接原样带回
  bool fundamental = false; // 联网同步基本面 (BigQuant 日频表 + Tushare) + 概念快照
  bool bar1m = false;       // 联网补齐分钟线 (基本面之后)
};

// 交接件: ok = false ⇒ 本地 parquet 缺失 (需先联网同步), assetinfo 空
struct FundamentalResult {
  uint64_t seq = 0;
  bool ok = false;
  AssetInfo assetinfo;
};

// ============================================================================
// FundamentalService — 基本面 sync + AssetInfo 构建 (单 worker; 阻塞网络 + parquet IO)
//
// 线程模型 (gui/util/Service.hpp, 随对象):
//   - GUI 线程 (StateManager): Request; 帧首 handoff.take() → ok 则换进 SharedData::assetinfo (只在 GUI 线程替换)
//   - worker:   run = 外来写者 parquet 重写 (renative) → 四道计划 (Round, 纯本地) → 四道依次 drive
//               [bigquant → tushare → eastmoney.concept → bigquant.bar1m] → 构建 (扫描 → 落地) → post
//               (两开关皆关 ⇒ 零网络直接本地构建). 取消 = 当前任务的查询跑完即停, 余下同步任务全部跳过, 构建照常
//               各道进退写 state.lanes (持 mutex)
//   - UI:       进度面 = 自身 (Published): Scanning 重写 / Loading 同步 / Running 构建, done/total = 文件 / 任务 / 分片;
//               state 持 mutex 读
// ============================================================================
class FundamentalService : public Service<FundamentalService, FundamentalRequest>, public Published {
  friend Service<FundamentalService, FundamentalRequest>;

public:
  static constexpr const char *kName = "FundamentalWorker";
  FundamentalService(); // 末尾 Start
  ~FundamentalService() { Stop(); }

  FundamentalState state;             // UI 持 mutex 读
  Mailbox<FundamentalResult> handoff; // worker 每轮收尾 post, StateManager 帧首 take

private:
  static constexpr Phase kStart = Phase::Scanning;
  Progress &board() { return *this; }
  bool run(FundamentalRequest &req);
};

} // namespace GUI::Database
