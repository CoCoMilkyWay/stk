#pragma once

// =============================================================================
// All — 工具全表的类型面: 各类的结果 (tools/<类>.hpp) 齐 + 清单的结果类型列 → ResOf<T> / As<T>
// =============================================================================
//   As<T>(act)  一步的调用是 T 时它的结构化结果 (别的工具 / 还没有结果 = 空); 只给展示与工具自己用, 底座 (Rig / 账本) 不认具体工具
// =============================================================================

#include "gui/task_agent/tools/Doc.hpp"   // IWYU pragma: export
#include "gui/task_agent/tools/Look.hpp"  // IWYU pragma: export
#include "gui/task_agent/tools/Misc.hpp"  // IWYU pragma: export
#include "gui/task_agent/tools/Note.hpp"  // IWYU pragma: export
#include "gui/task_agent/tools/Query.hpp" // IWYU pragma: export
#include "gui/task_agent/tools/Trial.hpp" // IWYU pragma: export

namespace GUI::Agent {

template <Tool T>
struct ResFor;
#define AGENT_TOOL_RES(T, id, cn, F, C, keep, R) \
  template <>                                    \
  struct ResFor<Tool::T> {                       \
    using type = R;                              \
  };
AGENT_TOOLS(AGENT_TOOL_RES)
#undef AGENT_TOOL_RES

template <Tool T>
using ResOf = typename ResFor<T>::type;

template <Tool T>
ResOf<T> *As(Act &a) {
  return a.call.tool == T ? static_cast<ResOf<T> *>(a.out.res.get()) : nullptr;
}
template <Tool T>
const ResOf<T> *As(const Act &a) {
  return a.call.tool == T ? static_cast<const ResOf<T> *>(a.out.res.get()) : nullptr;
}

} // namespace GUI::Agent
