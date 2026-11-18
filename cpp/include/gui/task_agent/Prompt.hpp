#pragma once

// =============================================================================
// Prompt — 提示词模板、工具结果成文、时间线状态折叠与对话上下文纯函数 TurnsOf
// =============================================================================
//   模板 = output/config/agent/prompt/<名>.md ({{键}} 占位, 未知键立即 assert):
//     system  静态前缀 (会话内逐字节不变): 作用域 / 行与时刻 / 语法与口径 / 入库门槛 / 做法 / 协议 / 工具表 / 特征族卡与叶表 / 算子表
//     first   首条 user: 已试候选 / 正式库 / 全局记录规模 + 能否提问 + 开工 (+ 本会话要求)
//     test    自检会话首条: 把每个工具调一遍, 问题记 report_issue, 末了总评再 finish (+ 本会话要求)
//     round   每条回复各调用的结果 (带调用编号 R<轮>#<序>) + 假设板 + 下一步; 原文只在本条给一次, 此后收进研究树
//     retry   回复不合协议: 原因 + 重发要求
//   时间线 Round 是全会话唯一真相: StateOf(Rounds) 唯一折叠出假设板 Board / 已落候选 / 已入库名, TurnsOf(Rounds) 纯函数折叠出完整上下文
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Llm.hpp"
#include "gui/task_agent/Notes.hpp"
#include "gui/task_agent/Tools.hpp"

#include <cstdint>
#include <set>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace GUI::Agent {

// ---- 时段、动作与时间线 ----
struct Span {
  double t0 = -1.0, t1 = -1.0;
  bool began() const { return t0 >= 0.0; }
  bool ended() const { return t1 >= 0.0; }
  bool running() const { return began() && !ended(); }
  double len(double now) const { return !began() ? 0.0 : (ended() ? t1 : now) - t0; }
  void begin(double t) {
    if (!began())
      t0 = t;
  }
  void end(double t) {
    if (running())
      t1 = t;
  }
};

struct Trial {
  Staged st;
  Span search, run;
};

using Extra = std::variant<std::monostate, Trial, Asked>;

struct Act {
  Call call;
  Span span;
  ToolOut out; // 结构化结果 res 只供工作台展示, 不存盘
  Extra extra;
};

enum class Msg : uint8_t { System,
                           First,
                           Round,
                           Retry,
                           Test };
struct MsgDef {
  const char *id, *cn;
};
inline constexpr MsgDef kMsgs[] = {{"system", "系统提示"}, {"first", "首轮"}, {"round", "结果"}, {"retry", "重发"}, {"test", "自检"}};
AGENT_ROWS(Msg, kMsgs, Test);

struct Message {
  Msg kind;
  std::string text;
};

struct Round {
  Msg kind = Msg::First;
  std::string user;
  std::string memo;
  Span span, think, write;
  llm::Reply reply;
  std::string error;
  std::vector<Act> acts;
};
using Rounds = std::span<const Round>;

// ---- 时间线唯一状态折叠 ----
struct State {
  Board board;
  std::set<std::string> staged;  // 本会话落了文件的候选文件名 (*.json)
  std::set<std::string> adopted; // submit_factor 做成的英文名
};
void Apply(State &s, const Act &a, Ref at);
State StateOf(Rounds rs);

// ---- 模板读写 ----
std::string LoadPrompt(Msg m);
void SavePrompt(Msg m, const std::string &text);

// ---- 结局与工具成文 ----
std::string Text(const Stat &s);
std::string Text(const Weight &w);
std::string Text(const Top &t);
std::string Brief(const Result &r);
std::string Detail(const Result &r);

std::string Fills(const std::string &tmpl, const std::string &expr); // 具体式相对模板只写自由参数的取值
std::string GateLine(const Result &r, const Gate &g);                // 有权重的候选: 是否达入库门槛一行
std::string ListTail(int all, int skip, int n);                      // 分页列表末尾: 共几个 / 这是第几到第几 / 接着看的 skip

std::string CallTitle(const Act &a, Ref r);
std::string AnswerText(const Asked &a);

// ---- 消息组装与上下文纯函数 ----
Message SystemMessage(const Context &c, const Gate &g, bool ask);
Message FirstMessage(const Context &c, int tried, const std::string &notes, bool ask, const std::string &remark);
Message TestMessage(bool ask, const std::string &remark);
Message RoundMessage(const std::vector<Act> &acts, int round, const Board &board, bool evaled, bool test);
Message RetryMessage(const std::string &error);
std::string MemoText(const std::string &memo);
std::string TextOf(const Round &rd);
llm::Turns TurnsOf(Rounds rounds);

} // namespace GUI::Agent
