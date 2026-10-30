#pragma once

// =============================================================================
// 提示词: 系统 → agent 的消息 (模板填充) + 工具结果的文本
// =============================================================================
//   模板 = kPromptDir/<名>.md (agent/Config.hpp), {{键}} 占位; 模板里出现未给的键 → assert (模板与代码脱节立刻暴露)
//     system  静态前缀 (一段对话内逐字节不变): 作用域 / 本层的行与时刻 / 语法与口径 / 入库门槛 / 做法 (先看后改, 一段会话精修一个因子) / 调用协议 /
//             工具表 / 特征叶 (按族分节: 族卡 (kFamilyFile) + 按 档.层 分组的 列名 + 中文名 + 落盘变换, 同组叶 (LeafKin) 合一行写成花括号式; 公式与说明经 search_leaves) / 算子表
//     first   首条 user: 已试候选 / 正式库 / 全局记录的规模 (用 list_tried / list_library / list_notes 看) + 能否提问 + 开工
//     test    自检会话的首条 (代替 first): 把每个工具都调一遍, 对照说明查结果, 问题记 report_issue, 末了总评再 finish
//     round   每条回复的调用结果, 按调用序 (调用编号 + 调用一行 + 结果) + 假设板 (不折叠) + 下一步 (挖掘 / 自检各一种); 较早的在会话历史里成批换成折叠版 (FoldedText)
//     retry   回复不合协议: 原因 + 按协议重发
//   回复的解析 / 工具表见 agent/Tools.hpp; 观察工具的成文见 agent/Views.hpp
//   结局文本两档: Brief 一行 (流水 / 界面标题) ↔ Detail 多行 (工具结果 / 界面展开); 五种结局 (Verdict) 一处写全
// =============================================================================

#include "agent/Board.hpp"
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
                           Retry,
                           Test };
// 消息表 (下标 = Msg): id = 模板名 = 流水里的 kind; cn = 界面名
struct MsgDef {
  const char *id, *cn;
};
inline constexpr MsgDef kMsgs[] = {{"system", "系统提示"}, {"first", "首轮"}, {"round", "结果"}, {"retry", "重发"}, {"test", "自检"}};
inline constexpr int kMsgCount = static_cast<int>(std::size(kMsgs));
static_assert(kMsgCount == static_cast<int>(Msg::Test) + 1);
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
std::string Text(const Top &t);      // + 目标 ± 标准误 (+ 曲面估计) (+ 取反)
std::string Brief(const Result &r);  // 一行: 结局 + 原因 / 最佳权重 / 代表点
std::string Detail(const Result &r); // 多行 ("- " 起): 结局 + 原因 / 每个权重 (自由参数的取值) / 落选原因与代表点; 不可见的只说不可比

// ---- 工具结果 (当场 / 回测 / 问答; 观察的在 agent/Views) ----
// 列表跳过可见的前 skip 个, 列 top 个; 没列完的末尾给总数与接着看的 skip
std::string TriedText(const std::vector<Result> &tried, int skip, int top, const Gate &g); // list_tried: 每个另标是否达入库门槛
std::string LibraryText(const std::vector<Result> &lib, int skip, int top);                // list_library: 只列本持有期有统计的, 其余报个数
// eval_factor: 名 + 模板 + 改动 + 结局 + 是否达入库门槛 (+ rep = 代表点的观察摘要, agent/Views RepText) (+ 最佳权重的 20 组 / 参数曲面)
std::string TrialText(const Trial &k, const Gate &g, const std::string &rep = "");
std::string AnswerText(const Asked &a); // ask_human: 每题的选择 (没照推荐的附上推荐) + 附言
// search_leaves: 命中的叶所在的同组 (LeafKin, 落盘变换也同) 前 n 组, 每组一行 (叶 / 中文名 / 公式 / 说明 / 落盘值, 按组内首个命中的写); shown 追加这几组的全部叶名
std::string LeavesText(const std::vector<Leaf> &leaves, const std::string &query, int n, std::vector<std::string> &shown);

// ---- 发 ----
Message SystemMessage(const Context &c, const Gate &g, bool ask, bool fold);           // fold = 会话历史折叠旧结果 (agent/Mine)
Message FirstMessage(const Context &c, int tried, const std::string &notes, bool ask); // tried = 可见的已试候选数; notes = 全局记录的条数 (NotesBrief)
Message TestMessage(bool ask);
// 一条回复的全部调用, 按序 (标题带调用编号 R<round>#<序>) + 假设板 + 下一步; evaled = 这条回复有落了文件的回测 (下一条不能回测); test = 自检会话
Message RoundMessage(const std::vector<Act> &acts, int round, const Board &board, bool evaled, bool test);
Message RetryMessage(const std::string &error);
// round 的折叠版 (会话历史里换掉原文): 观察 / 查询只留标题, 回测留一行结局, 其余 (提交 / 问答 / 判定 / 没做成的原因) 原样
std::string FoldedText(const std::vector<Act> &acts, int round);

} // namespace agent
