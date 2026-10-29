#pragma once

// =============================================================================
// 提示词: 模板填充 / 回复解析 / 反馈
// =============================================================================
//   模板 = kPromptDir/<名>.md (agent/Config.hpp), {{键}} 占位; 模板里出现未给的键 → assert (模板与代码脱节立刻暴露)
//     system.md  静态前缀 (一段对话内逐字节不变): 作用域 / 语法与口径 / 特征叶 / 算子表 / 正式因子库 / 回复协议
//     first.md   首条 user: 本作用域已试过的候选 (避免重复) + 开工指令
//     round.md   每轮反馈: 本批各候选的结局 (落文件失败原因 / Search 结论 / 各权重统计) + 续挖指令
//     retry.md   回复解析失败: 原因 + 按协议重发
//   回复协议 (JSON 对象): {"factors": [{"name", "name_cn", "expr", "note"}, …]}; 外层可包 ```json 代码块
// =============================================================================

#include "agent/Bridge.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace agent {

inline constexpr const char *kPrompts[] = {"system", "first", "round", "retry"};

// 模板原文 (文件须在)
std::string LoadPrompt(std::string_view name);
void SavePrompt(std::string_view name, const std::string &text);

std::string SystemPrompt(const Context &c);
std::string FirstMessage(const Context &c, const std::vector<Result> &tried);

// 一个候选在本轮的结局: 落文件失败 (err) 或已落 (file → 评估结果)
struct Staged {
  Cand cand;
  std::string file, err;
};
std::string RoundMessage(const Context &c, const std::vector<Staged> &batch, const std::vector<Result> &results);
// 回复解析失败时的重发要求
std::string RetryMessage(const std::string &err);

// 回复 → 候选; 不是 JSON 对象 / 无 factors 数组 / 项缺字段 → false + err (原样反馈给模型)
bool ParseReply(std::string_view text, std::vector<Cand> &out, std::string &err);

} // namespace agent
