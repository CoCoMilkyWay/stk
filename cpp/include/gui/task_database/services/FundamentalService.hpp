// Fundamental Service - 基本面数据 sync (BigQuant DAI + Tushare HTTP)
// 数据落地 = output/fundamental/YYYY-MM/*.parquet
// (api/bigquant + api/tushare 月度分片 + _meta 单文件, 水位增量, 常量见 shared/Config.hpp)
// AssetInfo 从 parquet 构建内存结构.
#pragma once

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <string>

struct SharedData;

namespace GUI::Database {

using boost::asio::awaitable;

enum class FundamentalStatus {
  Idle,     // 未加载
  Updating, // 联网同步 parquet 中 (bigquant + tushare)
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
  case FundamentalStatus::Building:
    return "Building";
  case FundamentalStatus::Ready:
    return "Ready";
  case FundamentalStatus::Error:
    return "Error";
  }
  return "Unknown";
}

struct FundamentalState {
  FundamentalStatus status = FundamentalStatus::Idle;
  std::string message;     // 当前阶段说明 (工作线程实时更新)
  std::string last_update; // 最近一次构建完成时刻

  // 构建结果统计 (Ready 后有效)
  std::size_t stock_count = 0;        // stock_info 条数
  std::size_t factor_stock_count = 0; // 有复权因子序列的股票数
  std::size_t trading_days_count = 0; // 交易日历天数
  std::string date_range_start;       // D 轴范围 "YYYY-MM-DD"
  std::string date_range_end;
};

// ============================================================================
// FundamentalService — GUI 协程侧薄壳: 抓取/构建全在独立工作线程 (阻塞网络 +
//   parquet IO), 协程轮询进度; SharedData::assetinfo 的替换只发生在 io 线程.
// ============================================================================
class FundamentalService {
public:
  FundamentalService(boost::asio::io_context &io, SharedData &data)
      : io_(io), data_(data) {}

  // pending 判定 → [bigquant::update → tushare::update] → 重建 AssetInfo
  // (pending 全 fresh ⇒ 零网络直接本地构建; 启动与手动 Update 共用)
  awaitable<void> update_all();

  const FundamentalState &get_state() const { return state_; }
  bool is_ready() const { return state_.status == FundamentalStatus::Ready; }
  bool is_busy() const { return busy_; }

private:
  awaitable<void> run(bool with_network);

  boost::asio::io_context &io_;
  SharedData &data_;
  FundamentalState state_;
  std::atomic<bool> busy_ = false;
};

} // namespace GUI::Database
