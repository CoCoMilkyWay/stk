#pragma once

#include "api/bigquant/dai.hpp"

#include <arrow/table.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace bigquant {

// ============================================================================
// TableSpec — api.md 中"需要支持"的 26 张表的元描述
// 与 doc/bigquant/fetch.py::TABLES 同构, 是 DAI 查询的最小调度单元.
// ============================================================================

// 抓取策略 — 决定 SQL 写法和 filters 是否启用.
//   Static    : 无 date 维度, 全量 SELECT *.        (basic_info)
//   Partition : date 列是分区列, 也是 visible_date. 用 filters={"date":[s,e]}
//               做服务端分区裁剪, 性能最佳.
//   Where     : visible_date 不是分区列. 用 SQL WHERE 过滤
//               (publish_date / end_date 等事件列).
//   Snapshot  : 有 date 列, 但仅取 [s,e] 内 MAX(<vd>) 一天的全量快照, 落
//               data/_meta/<name>.parquet 单文件 (像 Static 一样不走月度分片).
//               用于"真盘前"表 (cn_stock_static_data 09:00) 给 PIT overlay
//               最后一天 row D 用. 与 MonthFirst 对仗 (MIN vs MAX).
enum class FetchKind { Static,
                       Partition,
                       Where,
                       Snapshot };

// 抓取频率 — 当前仅一张表 (industry_component) 用 MonthFirst.
//   Day        : 在 [start, end] 闭区间内全量拉.
//   MonthFirst : 在 [start, end] 内最早一天的全量行 (月度快照).
enum class FetchFreq { Day,
                       MonthFirst };

struct TableSpec {
  std::string name;         // DAI 表名 = SQL FROM 子句
  std::string visible_date; // 因果安全可见日列名; Static 为空 ""
  FetchKind kind;
  FetchFreq freq;
  int avail_hour;   // day X 数据于 X 日该小时后完整 (misc::plan_months 水位增量用):
                    //   0 = 排程提前入库, 随时可拉 (all_trading_days / holidays)
                    //   9/10 = 真盘前 (static_data / margin)
                    //   20 = 盘后批 (api.md 17:00~20:00 统一记 20)
  std::string desc; // 表内容一句话 (GUI Database→Overview 逐表列出; 与 spec 同处
                    // 一地免得漏配. 列数取自实际 parquet schema)
};

// api.md 中"需要支持"的全部表 (26 张), 顺序与 api.md 自上而下一致.
extern const std::vector<TableSpec> SPECS;

// ============================================================================
// fetch — 自动按 (kind, freq) 选 SQL 模板; 一步式 DAI 查询入口.
//   Static                : "SELECT * FROM <name>",  filters={}, start/end 忽略
//   Partition + Day       : "SELECT * FROM <name>",  filters={"date":[start,end]}
//   Partition + MonthFirst: WHERE <vd>=(SELECT MIN(<vd>) ... WHERE <vd> BETWEEN s AND e),
//                           filters={"date":[start,end]}
//   Where + Day           : "SELECT * FROM <name> WHERE <vd>>=start AND <vd><=end",
//                           filters={}
//   Snapshot              : WHERE <vd>=(SELECT MAX(<vd>) ... WHERE <vd> BETWEEN s AND e),
//                           filters={"date":[start,end]}
// 日期格式: "YYYYMMDD" (内部 dash 成 DAI 接受的 "YYYY-MM-DD").
//
// min_vd 非空 = 增量下界 (含): 只取 vd >= min_vd 的行 (开放月水位增量).
//   Partition Day / Where: 直接抬升窗口下界; MonthFirst: MIN 子查询窗口保持
//   整月不变 (快照日语义不受增量影响), 仅外层叠 vd >= min_vd ⇒ 快照已入盘
//   时返回 0 行 (配额按 cell 计 = 免费).
// ============================================================================
std::shared_ptr<arrow::Table> fetch(DaiClient &client, const TableSpec &spec,
                                    std::string_view start,
                                    std::string_view end,
                                    std::string_view min_vd = {});

} // namespace bigquant
