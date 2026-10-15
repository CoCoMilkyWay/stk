#pragma once

#include "api/bigquant/client.hpp"
#include "api/tushare/client.hpp"
#include "fundamental/Store.hpp"

#include <arrow/table.h>

#include <memory>
#include <string>
#include <string_view>

namespace fundamental {

// ============================================================================
// Remote — 一次拉取任务 → 远端查询 → Table (Store::put 的上游). 两源同骨架, 按 client 类型重载:
//   queries(s, src, w): Spec × Window → Query 列表 (纯函数, .cpp 内)
//                       Bq 恒 1 条 (= select(s, "*", w)); Tu 区间接口 1 条 / 逐日接口窗口内逐日 × day_params
//   fetch(client, t)  : 逐条 query → 按序拼接 → 剥 Spec::drop
//   Sync 的循环体因而只有一句 put(t, fetch(client, t)).
//
// 增量 (t.append): 窗口下界已由 plan 抬到水位 + 1; snap 表只整段覆盖 (MIN / MAX 须看整窗, fetch 断言).
// ============================================================================
std::shared_ptr<arrow::Table> fetch(bigquant::Client &client, const Task &t);
std::shared_ptr<arrow::Table> fetch(tushare::Client &client, const Task &t);

// ============================================================================
// DAI SQL 唯一构造点 — fetch 与 Audit 只差 SELECT 什么、尾部接什么:
//   fetch  = select(s, "*", w)
//   schema = select(s, "*", w, " LIMIT 0")
//   指纹   = select(s, "strftime(..) AS g, ..", w, " GROUP BY 1")
//
// 窗口谓词: vd ∈ [w.lo, w.hi]; snap 再只留每个分片窗口内 MIN (Month, 子查询按月分组) / MAX (Meta, 整窗) 那天.
//   vd 空 → 无 WHERE (w 须为 {0, 0}).
// 分区表附服务端裁剪 filters {"date": [lo, hi]} (DAI 对分区表必须带, 否则拒绝); 非分区表空.
// 日期: Ymd (内部 dash 成 DAI 接受的 "YYYY-MM-DD").
// ============================================================================
bigquant::Query select(const Spec &s, std::string_view cols, Window w, std::string_view tail = {});

// SQL 标识符加双引号 (select 与 Audit 指纹列共用)
std::string ident(std::string_view c);

} // namespace fundamental
