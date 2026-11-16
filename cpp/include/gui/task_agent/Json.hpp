#pragma once

// =============================================================================
// 落盘 JSON 的字段清单: 作用域 / 门槛 / 预算 (Mine 页的 ui.json 与会话的 session.json 同一份)
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Mine.hpp"
#include "nlohmann/json.hpp"

namespace GUI::Agent {

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Open, all, on)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Scope, pool, level, hold, lob, years, leaves, ops)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Gate, mono, ric)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Budget, rounds, minutes, ktokens)

} // namespace GUI::Agent
