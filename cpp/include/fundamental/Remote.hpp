#pragma once

#include "api/bigquant/client.hpp"
#include "api/tushare/client.hpp"
#include "fundamental/Spec.hpp"

#include <arrow/table.h>

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>

namespace fundamental {

// ============================================================================
// Remote — 远端传输面 (Store 的对偶上游): Clients 聚合两源连接 + 远端表 schema 缓存
//   fetch(clients, task) : Task → 远端查询 → Table → 剥 Spec::drop; Tushare 远端故障 → nullptr (该任务跳过; 同一 Clients 其后的 Tushare 任务也不再发, 见 tushare::Client::query)
//   schema(clients, s, w): BigQuant 表 LIMIT 0 schema (同 Clients 实例内每表查一次)
//
// 落盘行序 (Bq): Month / Meta 按 vd (按日成块); Daily 按 (资产键, vd) (日文件内逐资产成段); vd 空不定
// 配额: Bq 每条查询收成一格 (bigquant::Client::query), 1 cell / 条, 与行列数无关
// ============================================================================
struct Clients {
  bigquant::Client bq;
  tushare::Client tu;
  std::map<std::string, std::shared_ptr<arrow::Schema>, std::less<>> schemas;
};

std::shared_ptr<arrow::Table> fetch(Clients &c, const Task &t);

std::shared_ptr<arrow::Schema> schema(Clients &c, const Spec &s, Window w);

// ============================================================================
// DAI SQL 唯一构造点 — fetch 与 audit 只差 SELECT 什么、尾部接什么:
//   fetch  = select(s, "*", t.w)
//   schema = select(s, "*", w, " LIMIT 0")
//   指纹   = select(s, group_expr(g) AS g, .., w, " GROUP BY 1")
//
// 窗口谓词: vd ∈ [w.lo, w.hi]; Shape::Daily (vd 为分钟时间戳) 为 [w.lo, w.hi + 1);
//   Snap::Day 再只留每个分片窗口内 MIN (Month, 子查询按月分组) / MAX (Meta, 整窗) 那天.
// Prune::Partition 附服务端裁剪 filters {"date": [lo, hi]}; Where 空.
// ============================================================================
bigquant::Query select(const Spec &s, std::string_view cols, Window w, std::string_view tail = {});

// SQL 标识符加双引号 (select 与 audit 指纹列共用)
std::string ident(std::string_view c);

// g 级组键的 SQL 表达式 (与本地 group_key 逐字同形)
std::string group_expr(const Spec &s, Grain g);

} // namespace fundamental
