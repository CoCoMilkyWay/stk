#pragma once

#include "api/bigquant/client.hpp"
#include "fundamental/Spec.hpp"

#include <arrow/table.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace fundamental {

// ============================================================================
// Remote — Task → 远端查询 → Table (Store::put 的对偶上游). 两源同骨架, 按 Src 分派:
//   queries(s, src, t)    : Task → Query 列表 (纯函数, .cpp 内)
//                           Bq 恒 1 条 (= select(s, "*", t.w); Splice 另接 key_filter(s, t.keys),
//                              与 audit 定位同一谓词); Tu 区间接口 1 条 / 逐日接口窗口内逐日 × day_params
//   fetch<Src>(client, t) : 逐条 query → 按序拼接 → 剥 Spec::drop
//   Daily 走打包 (见下), 拆回与 select(s, "*", ..) 同形 (列序 / 列型), 一日一 chunk, 日升序 × 资产序
//   sync::run 因而只有一句 put(t, fetch<Src>(client, t)).
//
// Daily 打包: 配额只按返回顶层 cell 计, LIST / STRUCT 内的元素不计 ⇒
//   SELECT 键, LIST({其余列} ORDER BY vd) .. WHERE 窗口 AND 组键 ∈ t.keys GROUP BY 键 ORDER BY 键
//   一资产一行, 配额 = 资产数 × kPackCols, 与日数 / 分钟行数无关; 列清单取 LIMIT 0 schema (0 cell)
// ============================================================================
inline constexpr int kPackCols = 2;

template <class Src>
std::shared_ptr<arrow::Table> fetch(typename Src::Client &client, const Task &t);

// 一份 Daily 计划逐任务的打包响应行数 (= 任务内资产数) —— 与 fetch 实付同口径.
//   一条查询: 任务窗口互不相交且升序 → CASE 按窗口归桶, COUNT(DISTINCT 键) GROUP BY 桶 (2 cell / 任务)
std::vector<std::int64_t> pack_rows(bigquant::Client &client, const std::vector<Task> &tasks);

// ============================================================================
// DAI SQL 唯一构造点 — fetch 与 Audit 只差 SELECT 什么、尾部接什么:
//   fetch  = select(s, "*", t.w)
//   Splice = select(s, "*", 槽整窗, key_filter(s, keys))
//   schema = select(s, "*", w, " LIMIT 0")
//   打包   = select(s, 键, LIST(..), t.w, key_filter(s, t.keys) GROUP BY 键)
//   指纹   = select(s, group_expr(g) AS g, .., w, [key_filter(s, 父键)] GROUP BY 1)
//
// 窗口谓词: vd ∈ [w.lo, w.hi]; Shape::Daily (vd 为分钟时间戳) 为 [w.lo, w.hi + 1);
//   Snap::Day 再只留每个分片窗口内 MIN (Month, 子查询按月分组) / MAX (Meta, 整窗) 那天.
// FROM = Bq::table (空 = Spec::name).
//   vd 空 → 无 WHERE (w 须为 {0, 0}).
// Prune::Partition 附服务端裁剪 filters {"date": [lo, hi]} (DAI 对分区表必须带, 否则拒绝); Where 空.
// 日期: Ymd (内部 dash 成 DAI 接受的 "YYYY-MM-DD").
// ============================================================================
bigquant::Query select(const Spec &s, std::string_view cols, Window w, std::string_view tail = {});

// SQL 标识符加双引号 (select 与 Audit 指纹列共用)
std::string ident(std::string_view c);

// g 级组键的 SQL 表达式 (与本地 group_key 逐字同形; Asset = 日 || '|' || 资产键列)
std::string group_expr(const Spec &s, Grain g);

// " AND (组键 ∈ keys)": keys 按粒度分组, 各组 group_expr IN (..) 以 OR 相连 (keys 空 → assert).
//   audit 定位 (父键收窄) 与 Splice 修复 (替换键) 共用 —— 定位到哪些行, 就只拉哪些行
std::string key_filter(const Spec &s, const std::vector<std::string> &keys);

} // namespace fundamental
