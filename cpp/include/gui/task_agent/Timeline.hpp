#pragma once

// =============================================================================
// Timeline — 时间线 (全会话唯一真相) 与它的纯函数; 发给 agent 的上下文 TurnsOf 同是它的纯函数, 见 task_agent/Tree
// =============================================================================
//   一轮 = 一条发出的消息: 原文 / 随它的插话 / 时段 / 回复 / 回复里每个调用的执行 (Act)
//   State  = 各轮收尾了的调用依次 Apply (唯一改状态处): 假设板 / 查过的叶 / 入库;
//            运行中每个调用收尾即 Apply 一次 (Mine 持有, 与 StateOf 同一条路), 开会话 / 载入 / 回退照 StateOf 重算
//   Tally  = 计数: 调用开始即计其工具; 做完的计失败 / 回测结局 / 入库; 用时含在走的那段 (算到 now)
//   Flags  = 流程旗:
//     evaled    末个有调用的轮里有落了文件的回测 (下一条回复不能回测)
//     asked     已有 ask_human 调用 (整段会话只问一次)
//     reviewed  发过复盘 (首次 finish / 本段预算用完); 自检会话视同复盘过
//     done      末轮的 finish 做完且复盘过: 本条做完即停
// =============================================================================

#include "gui/task_agent/Board.hpp"
#include "gui/task_agent/Llm.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Tools.hpp"

#include <array>
#include <set>
#include <span>
#include <string>
#include <vector>

namespace GUI::Agent {

// 一轮 = 一条发出的消息. span = 发出 → 收齐 (未开 = 待发), 其内 think / write = 首个思考增量 / 首个正文增量起 (无思考则 think 未开); acts = 回复里的调用, 按序执行
struct Round {
  Msg kind = Msg::First;
  std::string user; // 系统生成的消息原文
  std::string memo; // 随它发出的研究员插话 (空 = 无)
  Span span, think, write;
  llm::Reply reply;  // 收齐后有效
  std::string error; // reply.ok() 时 = 不合协议的原因; 否则 = 回复没收成的原因
  std::vector<Act> acts;
};
using Rounds = std::span<const Round>;

struct State {
  Board board;
  std::set<std::string> seen;    // search_leaves 列出过的叶组里的叶 (eval_factor 的式子只能用它们)
  std::set<std::string> adopted; // submit_factor 做成的英文名 (自检会话 = 达门槛, 不拷)
};
void Apply(State &s, const Act &a, Ref at); // a = 收尾了的调用, at = 它的编号
State StateOf(Rounds rs);

struct Tally {
  std::array<int, kToolCount> calls{}, fails{}; // 各工具开始执行的次数 / 其中没做成的
  std::array<int, kNum<Verdict>> verdicts{};    // eval_factor 的结局 (没落文件 = Unstaged)
  int adopted = 0;                              // submit_factor 做成的
  double llm_s = 0, act_s = 0;                  // 各轮发出 → 收齐 / 各调用开始 → 做完
  llm::Usage usage;
  int n(Tool t) const { return calls[static_cast<size_t>(t)]; }
  int n(Verdict v) const { return verdicts[static_cast<size_t>(v)]; }
  int n(Run r) const; // 该执行方的工具之和
  int n(Cat c) const; // 该用途类的工具之和
  int all() const;
  int failed(Tool t) const { return fails[static_cast<size_t>(t)]; }
  int failed(Cat c) const;
  int failed() const;
  int staged() const { return n(Tool::EvalFactor) - n(Verdict::Unstaged); }
};
Tally TallyOf(Rounds rs, double now);

struct Flags {
  bool evaled = false, asked = false, reviewed = false, done = false;
};
Flags FlagsOf(Rounds rs, bool test);

} // namespace GUI::Agent
