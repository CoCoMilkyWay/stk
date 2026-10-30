#pragma once

// =============================================================================
// Acts — 一个调用的执行 (按 Run 分派; 状态机见 agent/Mine)
// =============================================================================
//   一生: begin → (等 Lab: take 每步 / 等人: answer) → Mine::ended
//   Now    当场: search_leaves (列出的叶记进 Desk::seen) / list_tried / list_library / list_notes / submit_factor /
//                request_feature · request_tool · note_insight · report_issue (记进 Desk::notes; ref 对不上同类已有的一条则不记) /
//                hypothesize (记上 Desk::board) / judge (证据的调用编号须成立才改判定) / finish
//   Human  ask_human: 起问题 → 等人按 Start
//   Eval   eval_factor: 叶都在 Desk::seen, 假设判为支持且有配额, base 可见, 改动归类不退回, 功能块覆盖模板 → 落文件 → 等 Lab
//          (Search / Run / Settle, 无显著另有代表点的 Look); 没落成当场做完
//   Look   观察: 查参数 → 等 Lab (Look); 参数不成当场做完
//   Docs   研报库: 当场
//   调用读写会话只经 Desk (两个 worker / 设定 / 日志 / 全局记录 / 假设板 / 计数)
// =============================================================================

#include "agent/Bridge.hpp"
#include "agent/Tools.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace agent {

struct Desk;

enum class Wait : uint8_t { None,    // 当场做完
                            Lab,     // 等实验台到 End
                            Human }; // 等人按 Start
Wait begin(Act &a, Desk &d);
void take(Act &a, Lab::Tick &&t, Desk &d); // 实验台交回的一步 (End 除外)
void answer(Act &a, Desk &d);              // 人按 Start
// 一条合协议的回复是否守住会话约束: 上一条执行了的回复有回测 → 这条不能回测; 假设板上待判的 → 这条须各有一个 judge. 守住 = 空, 否则 = 原因 (经 retry 反馈)
std::string Owed(const std::vector<Call> &cs, const Desk &d);

} // namespace agent
