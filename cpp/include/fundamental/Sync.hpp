#pragma once

#include "fundamental/Store.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// ============================================================================
// sync — 写侧 (与 audit 逐项对仗: Task ↔ Unit, plan ↔ plan, run ↔ run, label ↔ label)
//   调用方循环: for t ∈ plan<Src>(round): run<Src>(client, t)
//   Src = Bq (BigQuant) / Tu (Tushare); 客户端类型 = Src::Client.
//
// plan<Src>: 纯本地判定 (零网络) = SPECS 里来自 Src 的表逐张 ledger → todo (表序 = SPECS 序, 表内 ym 升序):
//   已存在的分片 mtime 距 now.at < PIPELINE_DEDUP_WINDOW_SECONDS → 本轮不动 (节流); keep 再滤; 其余槽的动作全由 todo 定
//   (见 Store.hpp). 空 ⇒ 本轮无事可做.
//   run 只执行这份计划, 不重判 ⇒ "判定" 与 "执行" 结构上就是同一份.
// round 是本轮开头的本地视图: 每表只写自己的分片, 表间互不影响, 故一轮内无需重扫.
// client 由调用方持有 (BigQuant 配额探针与同步共用一条认证连接).
// Task 另一来源 = audit::repair (差异键 → 整槽 Replace / Splice 日·资产), 同走 run.
// ============================================================================
namespace fundamental::sync {

template <class Src>
std::vector<Task> plan(const Round &r, const Keep &keep = {});

// put(t, fetch(client, t)); 返回远端行数
template <class Src>
std::int64_t run(typename Src::Client &client, const Task &t);

// "cn_stock_status 2024-05 +[20240510..]" / "... ~[1 日 + 4 资产]" (进度展示)
std::string label(const Task &t);

// 一份 Daily 计划 (单表, 整日 Replace, 日升序) 的精确配额: cells = Σ 各日远端行数 × 返回列数 —— 与 run 实付同口径
struct Budget {
  std::size_t days = 0;
  Ymd lo = 0, hi = 0; // 首 / 末缺日
  std::int64_t rows = 0;
  int cols = 0;
  std::int64_t cells = 0;
};
// 列数 = LIMIT 0 schema (0 cell); 行数 = 缺日键 GROUP BY 计数 (2 cell / 日). 计划日远端 0 行 → assert
Budget budget(bigquant::Client &client, const std::vector<Task> &tasks);

} // namespace fundamental::sync
