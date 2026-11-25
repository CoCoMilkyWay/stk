#pragma once

// =============================================================================
// Prompt — 提示词模板、工具结果成文与对话上下文纯函数 TurnsOf (纯成文层, 不持会话数据)
// =============================================================================
//   模板 = output/config/agent/prompt/<名>.md ({{键}} 占位, 未知键立即 assert):
//     system  静态前缀 (会话内逐字节不变): 作用域 / 行与时刻 / 语法与口径 / 入库门槛 / 做法 / 协议 / 工具表 / 特征族卡与叶表 / 算子表
//     first   首条 user: 已试候选 / 正式库 / 全局记录规模 + 能否提问 + 开工 (+ 本会话要求)
//     test    自检会话首条: 把每个工具调一遍, 问题记 report_issue, 末了总评再 finish (+ 本会话要求)
//     result  上一个调用的结果 (标题带调用编号 #<步>) + 下一步
//     retry   上一条回复不合协议: 逐条列出不合处, 要求改正重交
//   时间线与状态账本见 tools/Kit.hpp; TurnsOf(Steps) 纯函数给出只追加的完整对话 (每步 = 消息 + 回复):
//     原样留着的近处超 kCtxChars 字, 即把更早的结果整块折成结论 (FoldOf; 折叠点 kFoldSteps 步一跳, 其间前缀不变)
//   分层: 底座 tools/Kit.hpp < 成文 Prompt < 各工具 (工具的原文借这里的候选结局成文; 这里不认具体工具)
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Llm.hpp"
#include "gui/task_agent/Notes.hpp"
#include "gui/task_agent/Tools.hpp"
#include "gui/task_agent/tools/Kit.hpp"

#include <string>

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

std::string CallTitle(const Act &a, int step);
std::string StageOf(const Act &a, double frac); // 在跑的调用此刻在做什么: eval_factor 写阶段 (Search / Run), 后台的带进度
std::string ResultText(const Act &a, int step); // 一个调用的结果原文 (标题 = 调用编号 + 调用; result 消息的结果节)
std::string BriefOf(const Act &a, int step);    // 折叠后留的结论 (回测留模板与结局, 观察留关键数, 记录 / 问答留原文, 查询只留调用一行)
std::string AnswerText(const Asked &a);
std::string NextStep(bool test); // result 消息末尾的 "下一步" 提示

// ---- 消息组装与上下文纯函数 ----
Message SystemMessage(const Context &c, const Gate &g, bool ask);
Message FirstMessage(const Context &c, int tried, const std::string &notes, bool ask, const std::string &remark);
Message TestMessage(bool ask, const std::string &remark);
Message ResultMessage(const Act &a, int step, bool test);
Message RetryMessage(const std::string &error); // 上一条回复不合协议的重交要求 (error 一行一处)
std::string MemoText(const std::string &memo);
std::string TextOf(const Step &sp);
size_t FoldOf(Steps ss);      // 第 2..fold 条消息 (下标 [1, fold)) 折成结论; 0 = 不折
llm::Turns TurnsOf(Steps ss); // 完整对话 (内置无状态后端每步用; 有状态的大脑只在新开一段对话时用, 其后只收本条)

} // namespace GUI::Agent
