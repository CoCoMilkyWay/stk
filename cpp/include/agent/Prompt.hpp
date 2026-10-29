#pragma once

// =============================================================================
// 提示词: 模板填充 / 回复解析 / 反馈 / 入库门槛
// =============================================================================
//   模板 = kPromptDir/<名>.md (agent/Config.hpp), {{键}} 占位; 模板里出现未给的键 → assert (模板与代码脱节立刻暴露)
//     system.md  静态前缀 (一段对话内逐字节不变): 作用域 / 语法与口径 / 入库门槛 / 两阶段回复协议 / 特征叶 / 算子表 / 正式因子库
//     first.md   首条 user: 本作用域已试过的候选 (避免重复) + 消化规则、提出要人定的问题
//     answer.md  人对各题的选择 + 附言 + 开挖 (此后不得提问)
//     round.md   每轮反馈: 上次提交的入库结果 + 本批各候选的结局 (落文件失败原因 / Search 结论 / 各权重统计) + 续挖指令
//     retry.md   回复解析失败: 原因 + 按协议重发
//   回复协议 (JSON 对象, 外层可包 ```json 代码块):
//     澄清 (一次)  {"questions": [{"prompt", "options": [..], "multiple"}]}
//     挖掘 (每轮)  {"factors": [{"name", "name_cn", "expr", "note"}], "submit": [英文名], "issues": [文本], "done"}
// =============================================================================

#include "agent/Bridge.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace agent {

inline constexpr const char *kPrompts[] = {"system", "first", "answer", "round", "retry"};

// 入库门槛: 一个权重 score / t / mono / 池内有效率全部不低于下限才算达标
struct Gate {
  double score = 0, t = 0, mono = 0, valid = 0;
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
  bool multiple = false;
};
// picks[i][j] = 第 i 题选了第 j 项
std::string AnswerMessage(const std::vector<Question> &qs, const std::vector<std::vector<bool>> &picks, const std::string &note);

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
