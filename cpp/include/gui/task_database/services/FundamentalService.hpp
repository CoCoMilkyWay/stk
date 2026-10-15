// Fundamental Service - 基本面数据 sync (BigQuant DAI + Tushare HTTP)
// 数据落地 = output/fundamental/YYYY-MM/*.parquet
// (fundamental/ 月度分片 + _meta 单文件, 水位增量, 常量见 shared/Config.hpp)
// AssetInfo 从 parquet 构建内存结构.
#pragma once

#include "misc/date.hpp"

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Database {

using boost::asio::awaitable;

enum class FundamentalStatus {
  Idle,     // 未加载
  Updating, // 联网同步 parquet 中 (bigquant + tushare)
  Confirm,  // BigQuant 周配额剩余 < BIGQUANT_QUOTA_CONFIRM_RATIO, 工作线程挂起等弹窗决定
  Building, // parquet → AssetInfo 构建中
  Ready,    // AssetInfo 就绪
  Error     // 本地 parquet 缺失 (需先联网同步)
};

inline const char *GetFundamentalStatusName(FundamentalStatus s) {
  switch (s) {
  case FundamentalStatus::Idle:
    return "Idle";
  case FundamentalStatus::Updating:
    return "Updating";
  case FundamentalStatus::Confirm:
    return "Confirm";
  case FundamentalStatus::Building:
    return "Building";
  case FundamentalStatus::Ready:
    return "Ready";
  case FundamentalStatus::Error:
    return "Error";
  }
  return "Unknown";
}

// 单表本地落盘状态 — 工作线程由 fundamental::Catalog 得到, 渲染帧只读不做 IO.
// 静态元描述 (增量键 / 抓法 / 就绪时点 / 中文说明) 不在这里重复: Overview 直接
// 遍历 fundamental::SPECS, 按 name 关联本结构.
struct TableFileStat {
  std::string name;        // = parquet 文件名 = spec.name
  std::size_t months = 0;  // 已落月度分片数; _meta 单文件表恒 0
  misc::Ym last_month = 0; // 最新分片; _meta 单文件表为 0
  std::uint64_t bytes = 0;
  std::string mtime; // 最后落盘时刻 "MM-DD HH:MM"
};

struct FundamentalState {
  FundamentalStatus status = FundamentalStatus::Idle;
  std::string message;     // 当前阶段说明 (工作线程实时更新)
  std::string last_update; // 最近一次构建完成时刻

  // BigQuant 周配额 (preflight 结果, 单位 cell; Overview 常显 + Confirm 态弹窗)
  //   每轮联网同步开头取一次, BigQuant 同步后再取一次; 未联网的轮次沿用上次
  std::int64_t quota_weekly = 0;
  std::int64_t quota_used = 0;
  std::string quota_time; // 读数时刻 "YYYY-MM-DD HH:MM:SS"; 空 = 从未取到

  // 构建结果统计 (Ready 后有效)
  std::size_t stock_count = 0;        // stock_info 条数
  std::size_t factor_stock_count = 0; // 有复权因子序列的股票数
  std::size_t trading_days_count = 0; // 交易日历天数
  misc::Ymd date_range_start = 0;     // D 轴范围
  misc::Ymd date_range_end = 0;

  // 逐表落盘状态 (Error 时也填 — 正是要看清哪张表缺)
  std::vector<TableFileStat> tables;

  // tables 里 name == 的那条; 缺失返回 nullptr
  const TableFileStat *find_table(const std::string &name) const;
};

// ============================================================================
// FundamentalService — GUI 协程侧薄壳: 抓取/构建全在独立工作线程 (阻塞网络 +
//   parquet IO), 协程轮询进度; SharedData::assetinfo 的替换只发生在 io 线程.
// ============================================================================
class FundamentalService {
public:
  FundamentalService(boost::asio::io_context &io, SharedData &data)
      : io_(io), data_(data) {}

  // 外来写者 parquet 重写 (renative) → sync::plan<Bq> / plan<Tu> → [逐任务 sync::run (bq) → (ts)] → 重建 AssetInfo
  // (两份计划皆空 ⇒ 零网络直接本地构建; 启动与手动 Update 共用)
  awaitable<void> update_all();

  // Confirm 态的弹窗回答: proceed=true 照常同步 BigQuant, false 跳过 BigQuant
  // (Tushare / 本地构建照常). 只在 Confirm 态可调, 每轮只回答一次.
  void confirm(bool proceed);

  // 当前任务的查询跑完即停, 余下同步任务全部跳过 (本地构建照常); 与 AuditService::cancel 同义
  void cancel();

  const FundamentalState &get_state() const { return state_; }
  bool is_ready() const { return state_.status == FundamentalStatus::Ready; }
  bool is_busy() const { return busy_; }

private:
  awaitable<void> run(bool with_network);

  boost::asio::io_context &io_;
  SharedData &data_;
  FundamentalState state_;
  std::atomic<bool> busy_ = false;
  // 弹窗决定 → 工作线程: 0 = 未答, 1 = 继续, 2 = 跳过 BigQuant.
  // shared_ptr 随 detached 线程存活 (与 Job 同理), 每轮 run 新建.
  std::shared_ptr<std::atomic<int>> decision_;
  std::shared_ptr<std::atomic<bool>> cancel_; // 同上, 每轮 run 新建
};

} // namespace GUI::Database
