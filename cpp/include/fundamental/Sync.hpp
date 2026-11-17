#pragma once

#include "fundamental/Store.hpp"

#include <cstdint>
#include <string>
#include <vector>

// ============================================================================
// sync — 写侧 (与 audit 逐项对仗: Task ↔ Unit, plan ↔ plan, run ↔ run, label ↔ label, Store ↔ Book)
//   plan(s, round)      : 一张表的纯本地判定 (零网络) → Task 列表 (at 升序; 空 = 已到水位)
//   run(clients, task)  : put(task, fetch(clients, task)), 返回远端行数
//   Task 另一来源 = audit::repair (差异组键 → 分片槽 Replace 任务), 同走 fetch / put.
// ============================================================================
namespace fundamental {

struct Clients;

namespace sync {

std::vector<Task> plan(const Spec &s, const Round &r);

std::int64_t run(Clients &c, const Task &t);

} // namespace sync

// "cn_stock_status 2024-05 +[20240510..]" / "cn_stock_basic_info _meta" / "bar1m 2026-10-09"
std::string label(const Task &t);

} // namespace fundamental
