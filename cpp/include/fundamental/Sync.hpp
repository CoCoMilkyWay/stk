#pragma once

#include "fundamental/Store.hpp"

namespace fundamental {

// ============================================================================
// Sync — 写侧唯一循环 (两源同一份): for spec ∈ SPECS 且来自 Src: for task ∈ plan: put(task, fetch(task))
//   Src = Dai (BigQuant) / Tu (Tushare); 客户端按 Src 在函数内建一份, fetch 按 spec.src 分派.
//
// pending: 纯本地判定 (零网络), 与 sync 同一个 plan ⇒ false 时 sync 必然无事可做.
// cat 是本轮开头的清单: 每表只写自己的分片, 表间互不影响, 故一轮内无需重扫.
// ============================================================================
template <class Src>
bool pending(const Catalog &cat, Ymd today);

template <class Src>
void sync(const Catalog &cat, Ymd today);

} // namespace fundamental
