// Tab Fundamental - 基本面数据源 (BigQuant + Tushare) 状态与同步入口
#pragma once

#include "gui/Tasks.hpp"
#include "gui/task_database/services/AuditService.hpp"
#include "gui/task_database/services/FundamentalService.hpp"

#include <cstdint>
#include <string>

namespace GUI::Database {

struct FundamentalTabState {
  // 逐表清单列宽贴合: 两服务进度面发布代变了 (限速) → 连发几帧 TableSetColumnWidthAutoAll
  uint64_t fit_epoch = ~0ull;
  double fit_last_time = 0.0;
  int fit_frames = 0;
};

// 渲染 Fundamental: 基本面数据状态卡 + Update 按钮 + Audit (远端核对) 区
// state / audit: 调用方持各自 mutex 传入; progress / audit_progress: 两服务的进度面 (原子)
// audit_table / repair_table: 点了某表的 audit / sync 小按钮 → 写入表名 (与 Audit 同一使能 disable_audit)
void RenderTabFundamental(
    const FundamentalState &state,
    const Progress &progress,
    const TaskStatus &status, // 基本面状态标签
    bool *update_clicked,
    bool *update_cancel_clicked,
    bool disable_update_controls,
    const AuditState &audit,
    const Progress &audit_progress,
    bool *audit_clicked,      // Audit all
    bool *audit_incr_clicked, // Audit incre (跳过已核的 (月, 轮))
    bool *audit_cancel_clicked,
    bool disable_audit,
    std::string *audit_table,
    std::string *repair_table,
    FundamentalTabState &ui);

} // namespace GUI::Database
