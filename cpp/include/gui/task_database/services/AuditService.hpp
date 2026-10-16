// Audit Service - BigQuant 远端 vs 本地冻结月分片一致性核对 + 逐表修复 (单 worker; 网络 + parquet IO)
// 口径见 fundamental/Audit.hpp. 请求三种 (AuditOp), 同一 worker 串行:
//   Load   : 读逐表状态 → 统计 (零网络, 零 parquet IO); StateManager 首个请求
//   Audit  : 一表 / 全部表核对 (月扫描 → 不一致的月逐级定位到 日 / 资产), 只报告不改盘;
//            取消在单元间生效 (在飞的 DoGet 不可中断)
//   Repair : 一表的定位键 → audit::repair 任务 (日·资产 Splice; 只定位到月 → 整月 Replace) → sync::run → 只复核这些月
//            UI 不给取消 (Stop 时在步间退出); 改了盘, StateManager 收到交接后重建 AssetInfo
// 结果是逐表的 "当前状态" (不是变更日志) output/fundamental/_audit/<表>.tsv:
//   每月只留最近一次核对的结论; 每单元后只整写该表一个文件 (tmp + rename).
// 线程模型 (gui/util/Worker.hpp, 随对象): 进度面 = 自身 (Published, done/total = 步), state 持 mutex 读;
//   逐表状态 (book) worker 独占, 跨轮承接; 每轮收尾 handoff.post(请求代)
#pragma once

#include "gui/util/Worker.hpp"

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
//   流量 (cell = 行 × 列, 与 fetch SELECT * 同口径):
//     repair_cells = repair 同口径: 每个定位组 Σ 远端行 × 列 (Month 组 = 整月);
//     full_cells   = 重拉全部已核月 (远端行 = 本地行 + 行差)
struct AuditTableStat {
  std::string table;
  std::int64_t local_days = 0; // 本地冻结月出现过的 visible_date 天数
  std::int64_t local_rows = 0;
  std::int64_t remote_only_days = 0; // 远端有、本地整天没有 (定位到 Day 且本地 0 行)
  std::int64_t added = 0;            // Σ max(0, 远端 n − 本地 n)
  std::int64_t removed = 0;          // Σ max(0, 本地 n − 远端 n)
  std::size_t bad_days = 0;          // 有定位组的日 (去重; 只定位到月的按月计)
  std::size_t row_days = 0;          // 有行数不一致组的日
  std::size_t content_days = 0;      // 有行数一致而内容不一致组的日
  std::string top_columns;           // 改值涉及天数最多的列 "col 天数, ..."
  std::size_t bad_months = 0;
  std::size_t whole_months = 0; // 只定位到月、须整月重拉的差异月 (其余按 日 / 资产 修补)
  std::int64_t repair_cells = 0;
  std::int64_t full_cells = 0;
  std::size_t audited_months = 0; // 已核的月
  std::size_t frozen_months = 0;  // 当前冻结的月 (覆盖率分母)
  std::string updated;            // 最近一次核对时刻 "YYYY-MM-DD HH:MM"; 空 = 从未核对

  bool audited() const { return audited_months > 0; }
  double bad_pct() const {
    const std::int64_t den = local_days + remote_only_days;
    return den > 0 ? 100.0 * static_cast<double>(bad_days) / static_cast<double>(den) : 0.0;
  }
  double row_pct() const {
    return local_rows > 0 ? 100.0 * static_cast<double>(added + removed) / static_cast<double>(local_rows) : 0.0;
  }
};

enum class AuditOp { Load,
                     Audit,
                     Repair };

struct AuditRequest {
  uint64_t seq = 0; // 请求代 (StateManager 发), 交接原样带回
  AuditOp op = AuditOp::Load;
  std::string table;        // op 的对象表; 空 = 全部表 (仅 Audit)
  bool incremental = false; // Audit: 跳过已核的月
};

// 展示字段 (worker 持 mutex 写, UI 持 mutex 读); 步数在进度面 (done/total, total = 0 ⇒ 尚未规划完)
struct AuditState {
  AuditOp op = AuditOp::Load; // 在跑 / 最近一轮的请求; 构造即 Load (StateManager 首个请求)
  std::string table;
  bool incremental = false;
  std::string current; // 当前步 (audit::label / sync::label)
  // BigQuant 周配额 (单位 cell): 开跑时 / 最近一步后的 used; -1 = 未取
  std::int64_t quota_start = -1;
  std::int64_t quota_used = -1;
  std::int64_t quota_weekly = 0;
  std::vector<AuditTableStat> tables; // 表名升序; 空 = 尚未 load
  std::string log_dir;                // output/fundamental/_audit/ (逐表状态, 含本地 / 远端原值)

  const AuditTableStat *find_table(const std::string &name) const;
};

class AuditService : public Worker<AuditService, AuditRequest>, public Published {
  friend Worker<AuditService, AuditRequest>;

public:
  static constexpr const char *kName = "AuditWorker";
  AuditService(); // 两者都在 .cpp: Book 不完整; 构造末尾 Start
  ~AuditService();

  AuditState state;          // UI 持 mutex 读
  Mailbox<uint64_t> handoff; // worker 每轮收尾 post 请求代, StateManager 帧首 take

private:
  struct Book; // 逐表核对状态 + 冻结月数 (worker 独占, 跨轮承接)

  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return *this; }
  bool run(AuditRequest &req);
  bool load();
  bool audit(const std::string &table, bool incremental);
  bool repair(const std::string &table);

  std::unique_ptr<Book> book_;
};

} // namespace GUI::Database
