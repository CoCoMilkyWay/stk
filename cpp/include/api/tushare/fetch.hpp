#pragma once

#include "api/tushare/http.hpp"
#include "fundamental/Spec.hpp"

#include <arrow/table.h>

#include <memory>

namespace tushare {

// 一次抓取 (与 bigquant::fetch 对仗): 区间接口 1 次调用 / 逐日接口窗口内逐日 × day_params;
// 全部响应合并转一张 arrow::Table (0 行窗口 → 0 行表). 增量 (w.inc != 0): 下界抬到 inc.
std::shared_ptr<arrow::Table> fetch(Http &http, const fundamental::Spec &s,
                                    const fundamental::Tu &src, const fundamental::Window &w);

} // namespace tushare
