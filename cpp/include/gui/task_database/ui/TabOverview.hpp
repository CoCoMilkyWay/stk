// Tab Overview - 基本面数据源 (BigQuant + Tushare) 状态与同步入口
#pragma once

#include "gui/task_database/services/AuditService.hpp"
#include "gui/task_database/services/FundamentalService.hpp"

#include <string>

namespace GUI::Database {

// 渲染 Overview: 基本面数据状态卡 + Update 按钮 + Audit (远端核对) 区
// repair_table: 点了某表的 sync 小按钮 → 写入表名 (与 Audit 同一使能 disable_audit)
void RenderTabOverview(
    const FundamentalState &state,
    bool *update_clicked,
    bool *update_cancel_clicked,
    bool disable_update_controls,
    const AuditState &audit,
    bool *audit_clicked,
    bool *audit_cancel_clicked,
    bool disable_audit,
    std::string *repair_table);

} // namespace GUI::Database
