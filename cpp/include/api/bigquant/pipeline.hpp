#pragma once

#include "api/bigquant/spec.hpp"

#include <string_view>
#include <vector>

namespace bigquant {

// ============================================================================
// BigQuant DAI 月度流水线 (与 tushare::update 完全对仗)
//   - Static / Snapshot → data/_meta/<name>.parquet 单文件整刷 (meta_fresh 判定)
//   - 其余              → misc::plan_months (关月冻结不动 / 开放月水位增量) →
//                         fetch(月) → data/YYYY-MM/<name>.parquet
//                         (整段覆盖 / 增量 append, 见 misc/schedule.hpp)
// 落盘 = 服务端响应原样 (单文件 tmp+rename 原子; 0 行月也落 0 行文件).
// ============================================================================
void update(std::string_view start, std::string_view end,
            const std::vector<TableSpec> &specs, int lookback_days);

// 纯本地判定 (零网络): 任一表存在待拉计划 (关月缺失 / 开放月水位未到且出 dedup
// 窗 / _meta 不新鲜) → true. false ⇒ update 必然无事可做, 调用方可整体跳过
// preflight + 联网 (与 update 内部用同一套 plan_months / meta_fresh, 判定一致).
bool pending(std::string_view start, std::string_view end,
             const std::vector<TableSpec> &specs, int lookback_days);

} // namespace bigquant
