#pragma once

// =============================================================================
// 提示词: 系统 → agent 的消息 (模板填充) + 工具结果的文本
// =============================================================================
//   模板 = kPromptDir/<名>.md (agent/Config.hpp), {{键}} 占位; 模板里出现未给的键 → assert (模板与代码脱节立刻暴露)
//     system  静态前缀 (一段对话内逐字节不变): 作用域 / 语法与口径 / 入库门槛 / 做法 (先看后改, 一段会话精修一个因子) / 调用协议 / 工具表 / 特征叶 / 算子表
//     first   首条 user: 已试候选与正式库的规模 (用 list_tried / list_library 看) + 能否提问 + 开工
//     round   每条回复的调用结果, 按调用序 (调用一行 + 结果)
//     retry   回复不合协议: 原因 + 按协议重发
//   回复的解析 / 工具表见 agent/Tools.hpp; 观察工具的成文见 agent/Views.hpp
//   结局文本两档: Brief 一行 (流水 / 界面标题) ↔ Detail 多行 (工具结果 / 界面展开); 五种结局 (Verdict) 一处写全
// =============================================================================

#include "agent/Bridge.hpp"
#include "agent/Tools.hpp"

#include <cstdint>
#include <iterator>
#include <string>
#include <vector>

namespace agent {

enum class Msg : uint8_t { System,
                           First,
                           Round,
                           Retry };
// 消息表 (下标 = Msg): id = 模板名 = 流水里的 kind; cn = 界面名
struct MsgDef {
  const char *id, *cn;
};
inline constexpr MsgDef kMsgs[] = {{"system", "系统提示"}, {"first", "首轮"}, {"round", "结果"}, {"retry", "重发"}};
inline constexpr int kMsgCount = static_cast<int>(std::size(kMsgs));
inline const MsgDef &def(Msg m) { return kMsgs[static_cast<int>(m)]; }
struct Message {
  Msg kind;
  std::string text;
};

// 模板原文 (文件须在)
std::string LoadPrompt(Msg m);
void SavePrompt(Msg m, const std::string &text);

// ---- 结局文本 ----
std::string Text(const Stat &s);     // mono · 每期毛多空 · rIC
std::string Text(const Weight &w);   // + score ± 标准误 (t) · 有效率; 无本持有期统计另说
std::string Text(const Top &t);      // + 目标 ± 标准误 (t) (+ 曲面估计) (+ 取反)
std::string Brief(const Result &r);  // 一行: 结局 + 原因 / 最佳权重 / 代表点
std::string Detail(const Result &r); // 多行 ("- " 起): 结局 + 原因 / 每个权重 / 落选原因与代表点; 不可见的只说不可比

// ---- 工具结果 (当场 / 回测 / 问答; 观察的在 agent/Views) ----
std::string TriedText(const std::vector<Result> &tried, int top);  // list_tried
std::string LibraryText(const std::vector<Result> &lib, int top);  // list_library
std::string TrialText(const Staged &st);                           // eval_factor: 名 + 模板 + 结局
std::string AnswerText(const Asked &a);                            // ask_human: 每题的选择 (没照推荐的附上推荐) + 附言

// ---- 发 ----
Message SystemMessage(const Context &c, const Gate &g, bool ask);
Message FirstMessage(const Context &c, int tried, bool ask); // tried = 可见的已试候选数
Message RoundMessage(const std::vector<Act> &acts);          // 一条回复的全部调用, 按序
Message RetryMessage(const std::string &err);

} // namespace agent
