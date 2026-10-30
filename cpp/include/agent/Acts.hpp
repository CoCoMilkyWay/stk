#pragma once

// =============================================================================
// Acts — 一个调用的执行 (按 Run 分派; 状态机见 agent/Mine)
// =============================================================================
//   一生: begin → (等 Lab: take 每步 / 等人: answer) → Mine::ended
//   Now    当场: search_leaves (列出的叶记进 Desk::seen) / list_tried / list_library / submit_factor / note_issue / finish
//   Human  ask_human: 起问题 → 等人按 Start
//   Eval   eval_factor: 式子里的叶都在 Desk::seen → 落文件 → 等 Lab (Search / Run / Settle, 无显著另有代表点的 Look); 没落成当场做完
//   Look   观察: 查参数 → 等 Lab (Look); 参数不成当场做完
//   Docs   研报库: 当场
//   调用读写会话只经 Desk (两个 worker / 设定 / 日志 / 计数)
// =============================================================================

#include "agent/Bridge.hpp"
#include "agent/Tools.hpp"

#include <cstdint>

namespace agent {

struct Desk;

enum class Wait : uint8_t { None,    // 当场做完
                            Lab,     // 等实验台到 End
                            Human }; // 等人按 Start
Wait begin(Act &a, Desk &d);
void take(Act &a, Lab::Tick &&t, Desk &d); // 实验台交回的一步 (End 除外)
void answer(Act &a, Desk &d);              // 人按 Start

} // namespace agent
