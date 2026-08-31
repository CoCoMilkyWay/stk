#pragma once

#include "api/tushare/spec.hpp"
#include <string_view>
#include <vector>

namespace tushare {

// Tushare 月度流水线 (与 bigquant::update 完全对仗):
//   misc::plan_months (关月冻结不动 / 开放月水位增量) →
//   fetch_month → data/YYYY-MM/<name>.parquet (整段覆盖 / 增量 append).
// lookback_days: 月末仍在该窗口内的月视为开放月; 关月时整月重拉一次后冻结.
void update(std::string_view start, std::string_view end,
            const std::vector<InterfaceSpec> &specs, int lookback_days);

// 纯本地判定 (零网络), 语义同 bigquant::pending.
bool pending(std::string_view start, std::string_view end,
             const std::vector<InterfaceSpec> &specs, int lookback_days);

// preflight 探针 — 证明 token 积分 ≥ 5000 (SPECS 里 *_vip 接口的门槛).
// 传一个必然无数据的 period: 服务端先判权限再查数据 ⇒ 有权限返回 code=0 + 0 行
// (最小响应); 积分不足则 code != 0, Http::call 直接 assert fail.
void probe();

} // namespace tushare
