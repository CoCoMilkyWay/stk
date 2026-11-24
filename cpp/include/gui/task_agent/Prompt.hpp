#pragma once

// =============================================================================
// Prompt — 提示词模板、工具结果成文与对话上下文纯函数 TurnsOf (纯成文层, 不持会话数据)
// =============================================================================
//   模板 = output/config/agent/prompt/<名>.md ({{键}} 占位, 未知键立即 assert):
//     system  静态前缀 (会话内逐字节不变): 作用域 / 行与时刻 / 语法与口径 / 入库门槛 / 做法 / 协议 / 工具表 / 特征族卡与叶表 / 算子表
//     first   首条 user: 已试候选 / 正式库 / 全局记录规模 + 能否提问 + 开工 (+ 本会话要求)
//     test    自检会话首条: 把每个工具调一遍, 问题记 report_issue, 末了总评再 finish (+ 本会话要求)
//     round   每条回复各调用的结果 (带调用编号 R<轮>#<序>) + 假设板 + 下一步; 原文只在本条给一次, 此后收进研究树
//     retry   回复不合协议: 原因 + 重发要求
//   时间线与状态账本见 tools/Kit.hpp; TurnsOf(Rounds) 纯函数线性分桶折叠出完整对话上下文
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Llm.hpp"
#include "gui/task_agent/Notes.hpp"
#include "gui/task_agent/Tools.hpp"
#include "gui/task_agent/tools/Kit.hpp"

#include <string>
#include <vector>

namespace GUI::Agent {

// ---- 模板读写 ----
std::string LoadPrompt(Msg m);
void SavePrompt(Msg m, const std::string &text);

// ---- 结局与工具成文 ----
inline const char *SignCn(int sign) { return sign < 0 ? "负" : "正"; } // 权重符号: 负 = 因子已整体取反
std::string Text(const Weight &w);
std::string SignLine(const Result &r); // "符号: 正 / 负" (最佳权重; 无权重 → 空)
std::string Brief(const Result &r);
std::string Detail(const Result &r); // 有权重: 先一行符号, 再逐权重一行 (与最佳权重符号不同的才另标)

std::string Fills(const std::string &tmpl, const std::string &expr); // 具体式相对模板只写自由参数的取值
std::string GateLine(const Result &r, const Gate &g);                // 有权重的候选: 是否达入库门槛一行
std::string ListTail(int all, int skip, int n);                      // 分页列表末尾: 共几个 / 这是第几到第几 / 接着看的 skip

std::string CallTitle(const Act &a, Ref r);
std::string ResultsText(const std::vector<Act> &acts, int round); // 一轮各调用的结果原文 (标题 = 调用编号 + 调用; round 消息的结果节)
std::string AnswerText(const Asked &a);

// ---- 消息组装与上下文纯函数 ----
Message SystemMessage(const Context &c, const Gate &g, bool ask);
Message FirstMessage(const Context &c, int tried, const std::string &notes, bool ask, const std::string &remark);
Message TestMessage(bool ask, const std::string &remark);
Message RoundMessage(const std::vector<Act> &acts, int round, const Board &board, bool evaled, bool test);
Message RetryMessage(const std::string &error);
std::string MemoText(const std::string &memo);
std::string TextOf(const Round &rd);
std::string TreeText(Rounds rounds); // 末轮之前各轮的研究树 (TurnsOf 首条后附的那份; 没有可收的步骤 → 空)
llm::Turns TurnsOf(Rounds rounds);

} // namespace GUI::Agent
