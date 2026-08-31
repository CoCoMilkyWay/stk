// Tab Overview - 基本面数据源 (BigQuant + Tushare) 状态与同步入口
#pragma once

#include "gui/task_database/services/FundamentalService.hpp"

namespace GUI::Database {

// 渲染 Overview: 基本面数据状态卡 + Update / Refresh 按钮
void RenderTabOverview(
    const FundamentalState &state,
    bool *update_clicked,
    bool *refresh_scan_clicked,
    bool disable_update_controls,
    bool disable_scan_controls);

} // namespace GUI::Database
