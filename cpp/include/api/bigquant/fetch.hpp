#pragma once

#include "api/bigquant/dai.hpp"
#include "fundamental/Spec.hpp"

#include <arrow/table.h>

#include <memory>
#include <string>

namespace bigquant {

// ============================================================================
// SQL 层 — fetch 与 audit 共用同一个窗口谓词, 两者只差 SELECT 什么:
//   fetch = "SELECT *"      + where(s, lo, hi) [+ AND vd >= inc]
//   audit = "SELECT 指纹 .." + where(s, lo, hi) + GROUP BY
//
// where: vd ∈ [lo, hi]; Pick::First / Last 再只留每个分片窗口内 MIN / MAX(vd) 那天
//   (Month 分片 = 自然月, 子查询按月分组; Meta 分片 = 整窗). vd 空 (Meta·All) → "".
// prune: 分区表的服务端裁剪 filters {"date": [lo, hi]} (DAI 对分区表必须带, 否则拒绝); 非分区表空.
// 日期: Ymd (内部 dash 成 DAI 接受的 "YYYY-MM-DD").
// ============================================================================
std::string where(const fundamental::Spec &s, fundamental::Ymd lo, fundamental::Ymd hi);
DaiFilters prune(const fundamental::Spec &s, fundamental::Ymd lo, fundamental::Ymd hi);

// 一次抓取. 增量 (w.inc != 0): Pick::All 直接把窗口下界抬到 inc; First / Last 的 MIN / MAX
//   须看整窗 (快照日语义不受增量影响), 只在外层叠 vd >= inc ⇒ 快照已入盘时 0 行 (配额按 cell 计 = 免费).
std::shared_ptr<arrow::Table> fetch(DaiClient &client, const fundamental::Spec &s,
                                    const fundamental::Dai &src, const fundamental::Window &w);

} // namespace bigquant
