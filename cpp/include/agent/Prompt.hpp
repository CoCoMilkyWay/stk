#pragma once

// =============================================================================
// 提示词: 发 (系统 → agent, 模板填充) ↔ 收 (agent → 系统, 回复解析)
// =============================================================================
//   发: 模板 = kPromptDir/<名>.md (agent/Config.hpp), {{键}} 占位; 模板里出现未给的键 → assert (模板与代码脱节立刻暴露)
//     system  静态前缀 (一段对话内逐字节不变): 作用域 / 语法与口径 / 入库门槛 / 做法 (一段会话精修一个因子) / 两阶段回复协议 / 特征叶 / 算子表 / 正式因子库
//     first   首条 user: 本作用域已试过的候选 (可作起点) + 消化规则、提出拿不准的问题
//     answer  人对各题的选择 (含 "其他" 自写意见; 没照推荐的附上推荐) + 附言 + 开挖 (此后不得提问)
//     round   每轮反馈: 上次提交的入库结果 + 本批各变体的结局 (落文件失败原因 / Search 结论 / 各权重统计) + 对照最好版本续改
//     retry   回复解析失败: 原因 + 按协议重发
//   收: 回复 = JSON 对象 (外层可包 ```json 代码块); 不合协议 → false + err (首个不合处, 带字段路径; 原样经 retry 反馈给模型)
//     澄清 (一次)  {"questions": [{"prompt", "options": [..], "multiple", "recommended": [下标]}]} (单选推荐恰 1 项, 多选 ≥ 1 项) → Questions
//     挖掘 (每轮)  {"factors": [{"name", "name_cn", "expr", "note"}], "submit": [英文名], "issues": [文本], "done"}               → Proposal
//   两阶段对仗: 澄清 Questions ─人选→ Asked ─→ answer;  挖掘 Proposal ─系统评→ Mined ─→ round
// =============================================================================

#include "agent/Bridge.hpp"

#include <cstdint>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

namespace agent {

enum class Msg : uint8_t { System,
                           First,
                           Answer,
                           Round,
                           Retry };
// 消息表 (下标 = Msg): id = 模板名 = 流水里的 kind; cn = 界面名
struct MsgDef {
  const char *id, *cn;
};
inline constexpr MsgDef kMsgs[] = {{"system", "系统提示"}, {"first", "首轮"}, {"answer", "答案"}, {"round", "反馈"}, {"retry", "重发"}};
inline constexpr int kMsgCount = static_cast<int>(std::size(kMsgs));
inline const MsgDef &def(Msg m) { return kMsgs[static_cast<int>(m)]; }
struct Message {
  Msg kind;
  std::string text;
};

// 模板原文 (文件须在)
std::string LoadPrompt(Msg m);
void SavePrompt(Msg m, const std::string &text);

// ---- 澄清: agent 的问题 → 人的选择 ----
struct Question {
  std::string prompt;
  std::vector<std::string> options;
  std::vector<bool> recommended; // 与 options 同长
  bool multiple = false;
};
using Questions = std::vector<Question>;
// 人对一题的答复: on[j] = 选了第 j 项; other = 选了 "其他" 自己写意见 text (单选: 与选项互斥; 多选: 与选项并列)
struct Pick {
  std::vector<bool> on;
  bool other = false;
  std::string text;
};
struct Asked {
  Questions qs;
  std::vector<Pick> picks; // 与 qs 对齐, 初值 = 推荐
  std::string note;        // 附言
};
// mask 选中的选项, "; " 连接
std::string Picked(const Question &q, const std::vector<bool> &mask);
// 选中的选项 + "其他: 意见" (other 且 text 非空), "; " 连接
std::string Answered(const Question &q, const Pick &p);

// ---- 挖掘: agent 的一批 → 系统的处理 ----
struct Proposal {
  std::vector<Cand> factors;
  std::vector<std::string> submit, issues;
  bool done = false;
};
// 一个候选: 落文件 → 回测. 时刻 = 会话起算秒, −1 = 未到 (队列里直接跳过的只有 t1)
struct Trial {
  Staged st;
  double t0 = -1, t_run = -1, t1 = -1; // 开评 / 转 Run / 评完
};
// factors → batch, submit → submitted
struct Mined {
  std::vector<Trial> batch;
  std::vector<Submitted> submitted;
  std::vector<std::string> issues;
  bool done = false;
  double t_eval0 = -1, t_eval1 = -1; // 开评 / 评完
};

// ---- 结局文本 (反馈 / 流水 / 界面同一份) ----
std::string Text(const Stat &s);    // mono · 每期毛多空 · rIC
std::string Text(const Weight &w);  // + score · 有效率; 无本持有期统计另说
std::string Text(const Top &t);     // + 目标 ± 标准误 (+ 取反)
std::string Brief(const Result &r); // 一行: 结局 + 最佳权重 / 目标最高的候选

// ---- 发 ----
Message SystemMessage(const Context &c, const Gate &g);
Message FirstMessage(const Context &c, const std::vector<Result> &tried);
Message AnswerMessage(const Asked &a); // 与推荐不同的题附上推荐
Message RoundMessage(const Context &c, const Mined &m);
Message RetryMessage(const std::string &err);

// ---- 收 ----
bool Parse(std::string_view text, Questions &out, std::string &err);
bool Parse(std::string_view text, Proposal &out, std::string &err);

} // namespace agent
