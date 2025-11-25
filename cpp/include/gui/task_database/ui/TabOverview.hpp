// Tab Overview - JSON File Maintenance and Crawler Status
// Header for overview tab rendering

#pragma once

#include "gui/task_database/models/BaostockData.hpp"
#include <cstddef>

namespace GUI::Database {

// Render the overview tab showing JSON file status and crawler progress
void RenderTabOverview(
    const JsonFileState &stock_factor_state,
    const JsonFileState &stock_info_state,
    const JsonFileState &stock_days_state,
    const CrawlerState &crawler_state,
    bool *stock_factor_update,
    bool *stock_factor_remove,
    bool *stock_factor_view,
    bool *stock_info_update,
    bool *stock_info_remove,
    bool *stock_info_view,
    bool *stock_days_update,
    bool *stock_days_remove,
    bool *stock_days_view,
    bool *update_all_clicked,
    bool *check_integrity_clicked,
    bool *refresh_scan_clicked,
    size_t l2_asset_count,
    size_t l2_encoded_count,
    size_t l2_missing_count,
    double l2_coverage_pct,
    double l2_disk_usage_gb,
    bool disable_update_controls,
    bool disable_scan_controls);

} // namespace GUI::Database
