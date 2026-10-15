#pragma once

#include "fundamental/Store.hpp"

#include <vector>

namespace bigquant {
class Client;
}
namespace tushare {
class Client;
}

namespace fundamental {

// ============================================================================
// Sync — 写侧唯一循环 (两源同一份): for task ∈ plan<Src>: put(task, fetch(client, task))
//   Src = Bq (BigQuant) / Tu (Tushare); fetch 按 client 类型重载 (Remote.hpp).
//
// plan<Src>: 纯本地判定 (零网络), SPECS 里来自 Src 的表逐张 plan 拼接 (表序 = SPECS 序, 表内 ym 升序);
//   空 ⇒ 本轮无事可做. sync 只执行这份计划, 不重判 ⇒ "判定" 与 "执行" 结构上就是同一份.
// cat 是本轮开头的清单: 每表只写自己的分片, 表间互不影响, 故一轮内无需重扫.
// client 由调用方持有 (BigQuant 配额探针与同步共用一条认证连接).
// ============================================================================
template <class Src>
std::vector<Task> plan(const Catalog &cat, const Now &now);

void sync(bigquant::Client &client, const std::vector<Task> &tasks);
void sync(tushare::Client &client, const std::vector<Task> &tasks);

} // namespace fundamental
