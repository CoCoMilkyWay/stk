// Fundamental Service - 基本面 + 分钟线数据 sync (BigQuant DAI + Tushare HTTP)
// 数据落地 = output/fundamental/YYYY-MM/*.parquet + YYYY-MM/bar1m/YYYY-MM-DD.parquet
// (fundamental/ 月度分片 + _meta 单文件 + Daily 日文件, 水位增量, 常量见 shared/Config.hpp)
// AssetInfo 从 parquet 构建内存结构.
#pragma once

#include "gui/util/Service.hpp"
#include "misc/date.hpp"
#include "shared/AssetInfo.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Database {

// 单表本地落盘状态 — 工作线程由 fundamental::Catalog 得到, 渲染帧只读不做 IO.
// 静态元描述 (增量键 / 抓法 / 就绪时点 / 中文说明) 不在这里重复: Overview 直接
// 遍历 fundamental::SPECS, 按 name 关联本结构.
struct TableFileStat {
  std::string name;        // = parquet 文件名 = spec.name
  std::size_t months = 0;  // 已落月度分片数 (Daily: 日文件数); _meta 单文件表恒 0
  misc::Ym last_month = 0; // 最新分片; _meta 单文件表为 0
  misc::Ymd last_day = 0;  // Daily: 最新日文件; 其余 0
  std::uint64_t bytes = 0;
  std::string mtime; // 最后落盘时刻 "MM-DD HH:MM"
};

// 展示字段 (worker 持 mutex 写, UI 持 mutex 读); 阶段与计步在进度面上
struct FundamentalState {
  std::string message;     // 当前阶段说明 (工作线程实时更新)
  std::string last_update; // 最近一次构建成功时刻

  // BigQuant 周配额 (preflight 结果, 单位 cell; Overview 常显 + 待确认弹窗)
  //   每轮联网同步开头 preflight 一次, 每个 BigQuant 任务后再取; 未联网的轮次沿用上次
  std::int64_t quota_weekly = 0;
  std::int64_t quota_used = 0;
  std::string quota_time; // 读数时刻 "YYYY-MM-DD HH:MM:SS"; 空 = 从未取到

  // 构建结果统计 (最近一次构建成功的)
  std::size_t stock_count = 0;        // stock_info 条数
  std::size_t factor_stock_count = 0; // 有复权因子序列的股票数
  std::size_t trading_days_count = 0; // 交易日历天数
  misc::Ymd date_range_start = 0;     // D 轴范围
  misc::Ymd date_range_end = 0;

  // 逐表落盘状态 (构建失败也填 — 正是要看清哪张表缺)
  std::vector<TableFileStat> tables;

  // tables 里 name == 的那条; 缺失返回 nullptr
  const TableFileStat *find_table(const std::string &name) const;
};

struct FundamentalRequest {
  uint64_t seq = 0;         // 请求代 (StateManager 发), 交接原样带回
  bool fundamental = false; // 联网同步基本面 (BigQuant 日频表 + Tushare)
  bool bar1m = false;       // 联网补齐分钟线 (基本面之后)
};

// worker 挂起等弹窗的事由 (GUI 线程按事由画模态, 回答经 confirm)
//   Quota: BigQuant 表 (基本面 / 分钟线) 待拉且周配额剩余 < BIGQUANT_QUOTA_CONFIRM_RATIO
enum class Ask { None,
                 Quota };

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
//   - worker:   run = 外来写者 parquet 重写 (renative) → sync::plan<Bq> / plan<Tu> (Round)
//               → [Ask::Quota] → 基本面 [drive: bq → ts] → 分钟线 [drive bq.bar1m] → 构建 → post
//               (两开关皆关 ⇒ 零网络直接本地构建). 取消 = 当前任务的查询跑完即停, 余下同步任务全部跳过, 构建照常
//   - UI:       进度面 = 自身 (Published): Scanning 重写 / Loading 同步 / Running 构建, done/total = 文件 / 任务 / 分片;
//               state 持 mutex 读; asking = 待确认事由 (worker 挂起等 confirm)
// ============================================================================
class FundamentalService : public Service<FundamentalService, FundamentalRequest>, public Published {
  friend Service<FundamentalService, FundamentalRequest>;

public:
  static constexpr const char *kName = "FundamentalWorker";
  FundamentalService(); // 末尾 Start
  ~FundamentalService() { Stop(); }

  // 待确认的弹窗回答: proceed=true 照常同步该道, false 跳过该道 (其余道 / 本地构建照常).
  // 只在 asking != None 时可调, 每问只回答一次.
  void confirm(bool proceed);

  FundamentalState state;             // UI 持 mutex 读
  std::atomic<Ask> asking{Ask::None}; // worker 挂起等弹窗的事由
  Mailbox<FundamentalResult> handoff; // worker 每轮收尾 post, StateManager 帧首 take

private:
  static constexpr Phase kStart = Phase::Scanning;
  Progress &board() { return *this; }
  bool run(FundamentalRequest &req);

  std::atomic<int> decision_{0}; // 弹窗决定 → worker: 0 = 未答, 1 = 继续, 2 = 跳过该道
};

} // namespace GUI::Database
