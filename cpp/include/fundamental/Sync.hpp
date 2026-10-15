#pragma once

#include "fundamental/Store.hpp"

#include <cstdint>
#include <string>
#include <vector>

// ============================================================================
// sync — 写侧 (与 audit 逐项对仗: Task ↔ Unit, plan ↔ plan, run ↔ run, label ↔ label)
//   调用方循环: for t ∈ plan<Src>(cat, now): run<Src>(client, t)
//   Src = Bq (BigQuant) / Tu (Tushare); 客户端类型 = Src::Client.
//
// plan<Src>: 纯本地判定 (零网络) = SPECS 里来自 Src 的表逐张 ledger 过滤 (表序 = SPECS 序, 表内 ym 升序):
//   Frozen → 不动; 已存在的分片 mtime 距 now.at < PIPELINE_DEDUP_WINDOW_SECONDS → 本轮不动 (节流);
//   Open → 水位增量 (见 Store.hpp); 其余 → 整段覆盖. 空 ⇒ 本轮无事可做.
//   run 只执行这份计划, 不重判 ⇒ "判定" 与 "执行" 结构上就是同一份.
// cat 是本轮开头的清单: 每表只写自己的分片, 表间互不影响, 故一轮内无需重扫.
// client 由调用方持有 (BigQuant 配额探针与同步共用一条认证连接).
// ============================================================================
namespace fundamental::sync {

// 一次拉取任务 = 表 + 分片 + 窗口 + 落盘方式
//   Write::Append: 水位增量, w.lo 已抬到水位 + 1; Snap::Day 表只整段覆盖 (MIN / MAX 须看整窗)
struct Task {
  const Spec *spec = nullptr; // 指向 SPECS 的一行
  Ym ym = 0;                  // 0 = _meta
  Window w;
  Write how = Write::Replace;
};

template <class Src>
std::vector<Task> plan(const Catalog &cat, const Now &now);

// put(fetch(t)); 返回落盘行数
template <class Src>
std::int64_t run(typename Src::Client &client, const Task &t);

// "cn_stock_status 2024-05 +[20240510..]" (进度展示)
std::string label(const Task &t);

} // namespace fundamental::sync
