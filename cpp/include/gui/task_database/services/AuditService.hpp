// Audit Service - BigQuant 远端 vs 本地冻结月分片一致性核对 + 逐表修复
// 口径见 fundamental/Audit.hpp; 网络 + parquet IO 全在独立工作线程, 协程轮询进度.
//   run    : 全量核对, 只报告不改盘
//   repair : 一表的差异月整月重拉 (Replace) → 只复核这些月
// 结果是一份 "当前状态" 缓存 (不是变更日志): 每个 (表, 月, 轮) 只留最近一次核对的结论.
#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Database {

// 每表汇总 = f(缓存里的差异格, 已核月的本地清点); 启动读缓存与在跑时实时刷新同一条路径.
// 逐格明细只在 AuditState::log_path. 分母只含已核月 (清点取自核对当时的分片).
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
  bool running = false;  // 全量核对中
  bool loading = true;   // 构造即 true: load 读完缓存前不可核对 / 修复
  std::string repairing; // 修复中的表; 空 = 无
  std::size_t done = 0;  // 已跑步数 (核对单元; 修复 = 重拉月 + 复核单元)
  std::size_t total = 0; // 0 = 尚未规划完
  std::string current;   // 当前步 (audit::label / sync::label)
  // BigQuant 周配额 (单位 cell): 开跑时 / 最近一步后的 used; -1 = 未取
  std::int64_t quota_start = -1;
  std::int64_t quota_used = -1;
  std::int64_t quota_weekly = 0;
  std::vector<AuditTableStat> tables; // 表名升序; 空 = 从未核对过
  // 核对缓存 output/log/audit.tsv (每步后整写, tmp + rename): 已核月清点 + 不一致格 (含本地 / 远端原值)
  std::string log_path;
  std::string updated;            // 缓存最后写入 "YYYY-MM-DD HH:MM"; 空 = 无缓存
  std::size_t rows_months = 0;    // 已跑 Rows 轮的 (表, 月)
  std::size_t content_months = 0; // 已跑 Content 轮的 (表, 月)
  std::size_t frozen_months = 0;  // 当前冻结的 (表, 月), 覆盖率分母

  const AuditTableStat *find_table(const std::string &name) const;
};

class AuditService {
public:
  explicit AuditService(boost::asio::io_context &io) : io_(io) {}

  // 启动调一次: 读缓存 → 统计 (零网络, 零 parquet IO); 无缓存则 tables 留空
  boost::asio::awaitable<void> load();
  // audit::plan → 逐单元 audit::run, 每单元覆盖缓存里该 (表, 月, 轮); 忙则直接返回
  boost::asio::awaitable<void> run();
  // table 的差异月: 整月重拉 (sync::run, Replace) → 旧结论作废 → plan(keep = 这些月) 两轮复核.
  // 不可取消; 忙则直接返回. 改了盘, 调用方随后须重建 AssetInfo
  boost::asio::awaitable<void> repair(std::string table);
  // 全量核对: 当前单元的查询跑完即停 (在飞的 DoGet 不可中断)
  void cancel();

  const AuditState &get_state() const { return state_; }
  bool is_running() const { return state_.running; }
  bool is_repairing() const { return !state_.repairing.empty(); }
  bool is_busy() const { return state_.running || state_.loading || is_repairing(); }

private:
  struct Job;

  // 轮询 job 直到 done, 期间把快照同步进 state_
  boost::asio::awaitable<void> follow(std::shared_ptr<Job> job);
  // 新一轮的 job: 承接上一轮的缓存内容
  std::shared_ptr<Job> next_job();

  boost::asio::io_context &io_;
  AuditState state_;
  std::shared_ptr<Job> job_; // 最近一轮 (随 detached 线程存活); 下一轮从它承接缓存
};

} // namespace GUI::Database
