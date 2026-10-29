#pragma once

// =============================================================================
// 提示词: 模板填充 / 回复解析 / 反馈 / 入库门槛
// =============================================================================
//   模板 = kPromptDir/<名>.md (agent/Config.hpp), {{键}} 占位; 模板里出现未给的键 → assert (模板与代码脱节立刻暴露)
//     system.md  静态前缀 (一段对话内逐字节不变): 作用域 / 语法与口径 / 入库门槛 / 做法 (一段会话精修一个因子) / 两阶段回复协议 / 特征叶 / 算子表 / 正式因子库
//     first.md   首条 user: 本作用域已试过的候选 (可作起点) + 消化规则、提出拿不准的问题
//     answer.md  人对各题的选择 (含 "其他" 自写意见; 没照推荐的附上推荐) + 附言 + 开挖 (此后不得提问)
//     round.md   每轮反馈: 上次提交的入库结果 + 本批各变体的结局 (落文件失败原因 / Search 结论 / 各权重统计) + 对照最好版本续改
//     retry.md   回复解析失败: 原因 + 按协议重发
//   回复协议 (JSON 对象, 外层可包 ```json 代码块):
//     澄清 (一次)  {"questions": [{"prompt", "options": [..], "multiple", "recommended": [下标]}]} (单选推荐恰 1 项, 多选 ≥ 1 项)
//     挖掘 (每轮)  {"factors": [{"name", "name_cn", "expr", "note"}], "submit": [英文名], "issues": [文本], "done"}
// =============================================================================

#include "agent/Bridge.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace agent {

inline constexpr const char *kPrompts[] = {"system", "first", "answer", "round", "retry"};

// 入库门槛: 一个权重 mono / rIC 均值都不低于下限才算达标 (多空不设门槛, 只比大小)
struct Gate {
  double mono = 0, ric = 0;
};
// 达标 → 空; 否则 = 差在哪 (取 score 最高的权重说)
std::string GateMiss(const Gate &g, const Result &r);

// 模板原文 (文件须在)
std::string LoadPrompt(std::string_view name);
void SavePrompt(std::string_view name, const std::string &text);

std::string SystemPrompt(const Context &c, const Gate &g);
std::string FirstMessage(const Context &c, const std::vector<Result> &tried);

// ---- 澄清 ----
struct Question {
  std::string prompt;
  std::vector<std::string> options;
  std::vector<bool> recommended; // 与 options 同长
  bool multiple = false;
};
// 人对一题的答复: on[j] = 选了第 j 项; other = 选了 "其他" 自己写意见 text (单选: 与选项互斥; 多选: 与选项并列)
struct Pick {
  std::vector<bool> on;
  bool other = false;
  std::string text;
};
// mask 选中的选项, "; " 连接
std::string Picked(const Question &q, const std::vector<bool> &mask);
// 选中的选项 + "其他: 意见" (other 且 text 非空), "; " 连接
std::string Answered(const Question &q, const Pick &p);
// 与推荐不同的题附上推荐
std::string AnswerMessage(const std::vector<Question> &qs, const std::vector<Pick> &picks, const std::string &note);

// ---- 挖掘 ----
struct MineReply {
  std::vector<Cand> factors;
  std::vector<std::string> submit, issues;
  bool done = false;
};
// 一个候选在本轮的结局: 落文件失败 (err) 或已落 (file → 评估结果)
struct Staged {
  Cand cand;
  std::string file, err;
};
// 一个提交的结局: err 空 = 已入库
struct Submitted {
  std::string name, err;
};
std::string RoundMessage(const Context &c, const std::vector<Submitted> &subs, const std::vector<Staged> &batch, const std::vector<Result> &results);
// 回复解析失败时的重发要求
std::string RetryMessage(const std::string &err);

// 回复 → 结构; 不合协议 → false + err (原样反馈给模型)
bool ParseQuestions(std::string_view text, std::vector<Question> &out, std::string &err);
bool ParseMine(std::string_view text, MineReply &out, std::string &err);

} // namespace agent
