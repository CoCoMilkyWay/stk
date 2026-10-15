// Audit Service - BigQuant 远端 vs 本地冻结月分片一致性核对 + 逐表修复
// 口径见 fundamental/Audit.hpp; 网络 + parquet IO 全在独立工作线程, 协程轮询进度.
//   run    : 一表 / 全部表核对, 只报告不改盘
//   repair : 一表的差异 → audit::repair 任务 (按日 Splice; Snap::Day / 只定位到月 → 整月 Replace) → sync::run → 只复核这些月
// 结果是逐表的 "当前状态" (不是变更日志) output/log/audit/<表>.tsv:
//   每个 (月, 轮) 只留最近一次核对的结论; 每单元后只整写该表一个文件 (tmp + rename).
#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Database {

// 每表汇总 = f(该表状态文件里的差异格, 已核月的本地清点); 启动读状态与在跑时实时刷新同一条路径.
// 逐格明细只在状态文件 (AuditState::log_dir). 分母只含已核月 (清点取自核对当时的分片).
// SPECS 里每张 DAI·Month 表恒有一条 (从未核对 = 已核月数 0).
//   问题天数占比 = bad_days / (local_days + remote_only_days)
//   行差占比     = (added + removed) / local_rows
//   流量 (cell = 行 × 列, 与 fetch 整月 SELECT * 同口径; 远端行 = 本地行 + 行差):
//     repair_cells = repair 同口径 (audit::fix_day): 有差异的日 Σ 远端行 × 列 (只定位到月 / Snap::Day 表 → 整月);
//     full_cells   = 重拉全部已核月
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
  std::size_t whole_months = 0; // 须整月重拉的差异月 (其余按日修补)
  std::int64_t repair_cells = 0;
  std::int64_t full_cells = 0;
  std::size_t rows_months = 0;    // 已跑 Rows 轮的月
  std::size_t content_months = 0; // 已跑 Content 轮的月
  std::size_t frozen_months = 0;  // 当前冻结的月 (覆盖率分母)
  std::string updated;            // 最近一次核对时刻 "YYYY-MM-DD HH:MM"; 空 = 从未核对

  bool audited() const { return rows_months > 0 || content_months > 0; }
  double bad_pct() const {
    const std::int64_t den = local_days + remote_only_days;
    return den > 0 ? 100.0 * static_cast<double>(bad_days) / static_cast<double>(den) : 0.0;
  }
  double row_pct() const {
    return local_rows > 0 ? 100.0 * static_cast<double>(added + removed) / static_cast<double>(local_rows) : 0.0;
  }
};

enum class AuditOp { None,
                     Audit,
                     Repair };

struct AuditState {
  bool loading = true; // 构造即 true: load 读完状态前不可核对 / 修复
  AuditOp op = AuditOp::None;
  std::string table;        // op 的对象表; 空 = 全部表 (仅 Audit)
  bool incremental = false; // Audit: 跳过已核的 (月, 轮)
  std::size_t done = 0;     // 已跑步数 (核对单元; 修复 = 重拉月 + 复核单元)
  std::size_t total = 0;    // 0 = 尚未规划完
  std::string current;      // 当前步 (audit::label / sync::label)
  // BigQuant 周配额 (单位 cell): 开跑时 / 最近一步后的 used; -1 = 未取
  std::int64_t quota_start = -1;
  std::int64_t quota_used = -1;
  std::int64_t quota_weekly = 0;
  std::vector<AuditTableStat> tables; // 表名升序; 空 = 尚未 load
  std::string log_dir;                // output/log/audit/ (逐表状态, 含本地 / 远端原值)

  const AuditTableStat *find_table(const std::string &name) const;
};

class AuditService {
public:
  explicit AuditService(boost::asio::io_context &io) : io_(io) {}

  // 启动调一次: 读逐表状态 → 统计 (零网络, 零 parquet IO)
  boost::asio::awaitable<void> load();
  // audit::plan (table 空 = 全部表, 否则只该表; incremental = 跳过状态里已核的 (月, 轮))
  //   → 逐单元 audit::run, 每单元覆盖该 (表, 月, 轮); 忙则直接返回
  boost::asio::awaitable<void> run(std::string table, bool incremental);
  // table 的差异: audit::repair → 逐任务 sync::run (Splice 按日 / Replace 整月) → 这些月旧结论作废 → plan(keep = 这些月) 两轮复核.
  // 不可取消; 忙则直接返回. 改了盘, 调用方随后须重建 AssetInfo
  boost::asio::awaitable<void> repair(std::string table);
  // 核对 (一表 / 全部): 当前单元的查询跑完即停 (在飞的 DoGet 不可中断)
  void cancel();

  const AuditState &get_state() const { return state_; }
  bool is_running() const { return state_.op == AuditOp::Audit; }
  bool is_repairing() const { return state_.op == AuditOp::Repair; }
  bool is_busy() const { return state_.loading || state_.op != AuditOp::None; }

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
