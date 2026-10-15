// Audit Service - BigQuant 远端 vs 本地冻结月分片一致性核对 (只报告, 不改盘)
// 口径见 api/bigquant/audit.hpp; 网络 + parquet IO 全在独立工作线程, 协程轮询进度.
#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Database {

// 每表汇总 = f(差异日志, 本地冻结月分片清点); 启动读上次日志与在跑时实时刷新同一条路径.
// 逐格明细只在 AuditState::log_path.
//   问题天数占比 = bad_days / (local_days + remote_only_days)
//   行差占比     = (added + removed) / local_rows
//   流量 (cell = 行 × 列, 与 fetch 整月 SELECT * 同口径; 远端行 = 本地行 + 行差):
//     repair_cells = 只重拉有差异的月;  full_cells = 重拉全部冻结月
struct AuditTableStat {
  std::string table;
  std::int64_t local_days = 0; // 本地冻结月出现过的 visible_date 天数
  std::int64_t local_rows = 0;
  std::int64_t remote_only_days = 0; // 远端有、本地整天没有
  std::int64_t added = 0;            // Σ max(0, 远端 n − 本地 n)
  std::int64_t removed = 0;          // Σ max(0, 本地 n − 远端 n)
  std::size_t bad_days = 0;          // rows / content 任一不一致的日 (去重)
  std::size_t row_days = 0;          // 行数不一致的日
  std::size_t content_days = 0;      // 行数一致而内容不一致的日
  std::string top_columns;           // 改值涉及天数最多的列 "col 天数, ..."
  std::size_t bad_months = 0;
  std::int64_t repair_cells = 0;
  std::int64_t full_cells = 0;

  double bad_pct() const {
    const std::int64_t den = local_days + remote_only_days;
    return den > 0 ? 100.0 * static_cast<double>(bad_days) / static_cast<double>(den) : 0.0;
  }
  double row_pct() const {
    return local_rows > 0 ? 100.0 * static_cast<double>(added + removed) / static_cast<double>(local_rows) : 0.0;
  }
};

struct AuditState {
  bool running = false;
  bool loading = false;   // 启动时读上次日志 + 清点本地分片中
  bool cancelled = false; // 上一轮被取消 (统计只覆盖已跑单元)
  std::size_t done = 0;   // 已跑单元数
  std::size_t total = 0;  // 0 = 尚未规划完
  std::string current;    // 当前单元 (audit_label)
  // BigQuant 周配额 (单位 cell): 开跑时 / 最近一单元后的 used; -1 = 未取
  std::int64_t quota_start = -1;
  std::int64_t quota_used = -1;
  std::int64_t quota_weekly = 0;
  std::vector<AuditTableStat> tables; // 表名升序; 空 = 从未核对过
  // 差异落盘 output/log/audit_YYYYMMDD_HHMMSS.tsv (每单元追加 + flush, 末行 "# done|cancelled k/n"):
  //   一行一个不一致格, 含本地 / 远端原值
  std::string log_path;
  std::string log_status; // "2026-10-15 11:45 完成 612/612" 等

  const AuditTableStat *find_table(const std::string &name) const;
};

class AuditService {
public:
  explicit AuditService(boost::asio::io_context &io) : io_(io) {}

  // 启动调一次: 最新日志 + 本地清点 → 统计 (零网络); 无日志则 tables 留空
  boost::asio::awaitable<void> load_last();
  // audit_plan → 逐单元 audit_unit; 忙则直接返回
  boost::asio::awaitable<void> run();
  // 当前单元的查询跑完即停 (在飞的 DoGet 不可中断)
  void cancel();

  const AuditState &get_state() const { return state_; }
  bool is_running() const { return state_.running; }
  bool is_busy() const { return state_.running || state_.loading; }

private:
  struct Job;

  // 轮询 job 直到 done, 期间把快照同步进 state_
  boost::asio::awaitable<void> follow(std::shared_ptr<Job> job);

  boost::asio::io_context &io_;
  AuditState state_;
  std::shared_ptr<Job> job_; // 随 detached 线程存活, 每轮新建
};

} // namespace GUI::Database
