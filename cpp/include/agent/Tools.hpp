#pragma once

// =============================================================================
// Tools — agent 的工具箱 (类 MCP): 一张工具表, 系统提示的 "工具" 节 / 回复解析 / 参数校验 / 界面名都从它出
// =============================================================================
//   回复 = 一串调用 {"calls": [{"tool": 名, "args": {...}}, ...]}; 系统按序执行, 下条消息按序给回各自的结果 (Prompt round)
//   工具 = 名 + 执行方 + 参数表 (键 / 类型 / 缺省 / 取值范围 / 说明) + 做什么 / 返回什么; 加工具 = 表里加一行 + 执行方里接一个分支
//   执行方 Run: Now 当场 (GUI 线程, 读写会话状态) / Human 等人 / Eval 回测 (Lab eval) / Look 观察 (Lab look → agent/Views 成文) / Docs 研报库 (GUI 线程当场查, agent/Docs 成文)
//   校验两层: 解析时按参数表 (类型 / 必填 / 范围 / 可选值; 不合 → 整条回复重发); 执行时按数据 (因子 / 特征 / 资产 / 日期 / 流派 / 研报不成 → 该调用的结果写原因, 其余照做)
//   回复级约束: 1..kMaxCalls 个调用; eval_factor 至多 kBatch 个, 有 eval_factor 就至少带一个观察; finish 只能在末尾; ask_human 只在会话允许提问且还没问过时可用
//   执行记录 Act: 一个调用的时段 / 交给 agent 的结果 / 带状态调用的过程 (eval_factor 的回测 Trial, ask_human 的问答 Asked); 执行见 agent/Acts.hpp
// =============================================================================

#include "agent/Bridge.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace agent {

enum class Tool : uint8_t { ViewFeature,
                            ViewLayers,
                            ViewTrades,
                            ViewHolders,
                            ViewResponse,
                            ViewExplain,
                            ViewStockDay,
                            ViewStockDays,
                            SearchLeaves,
                            SearchDocs,
                            SearchFactors,
                            ReadDoc,
                            EvalFactor,
                            ListTried,
                            ListLibrary,
                            SubmitFactor,
                            NoteIssue,
                            Finish,
                            AskHuman };
inline constexpr size_t kToolCount = static_cast<size_t>(Tool::AskHuman) + 1;
enum class Run : uint8_t { Now,
                           Human,
                           Eval,
                           Look,
                           Docs };
inline constexpr size_t kRunCount = static_cast<size_t>(Run::Docs) + 1;
enum class Arg : uint8_t { Str,
                           Int,
                           Strs,
                           Questions };
struct ArgDef {
  const char *key;
  Arg type;
  const char *def;     // 缺省值 (JSON 字面量); nullptr = 必填
  int lo, hi;          // Int 的取值 / Strs 的个数 (闭区间)
  const char *choices; // Str 的可选值 "a|b|c" (缺省为 "" 的另可写空串 = 不限); nullptr = 任意串 (必填的须非空)
  const char *desc;
};
struct ToolDef {
  Tool id;
  const char *name, *cn; // 调用名 / 界面名
  Run run;
  std::span<const ArgDef> args;
  const char *desc, *out; // 做什么 (何时用) / 返回什么
};
std::span<const ToolDef> Tools(); // 下标 = Tool
const ToolDef &def(Tool t);
std::string ToolText(const char *s);  // 工具说明 (desc / out) 的占位填实: {batch} → kBatch, {stock_days} → kStockDays
std::string ArgType(const ArgDef &a); // 参数的类型与取值 (可选值 / 范围 / 个数)
std::string ArgText(const ArgDef &a); // 参数一行: `键` (类型, 缺省 / 必填): 说明 (工具节与界面同一份)

// 一个调用 (已按参数表校验, 缺省已补, 键按参数表序)
struct Call {
  Tool tool;
  nlohmann::ordered_json args;
  std::string str(const char *k) const;
  int num(const char *k) const;
  std::vector<std::string> strs(const char *k) const;
  std::string text(size_t cap = 0) const; // 一行: 名(键=值, …) (流水 / 界面); cap > 0 = 每个值至多 cap 字 (结果标题)
};
// ask = 本条回复可用 ask_human; 不合协议 → false + err (首个不合处, 带字段路径; 原样经 retry 反馈给模型)
bool ParseCalls(std::string_view reply, bool ask, std::vector<Call> &out, std::string &err);
// 系统提示的工具节: 每个工具一节 (做什么 / 参数 / 返回); ask = 列不列 ask_human
std::string ToolsDoc(bool ask);

// ---- ask_human: agent 的问题 → 人的选择 ----
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
  Asked() = default;
  explicit Asked(Questions q);
};
Questions QuestionsOf(const Call &c); // ask_human 的问题 (解析时已校验)
// mask 选中的选项, "; " 连接
std::string Picked(const Question &q, const std::vector<bool> &mask);
// 选中的选项 + "其他: 意见" (other 且 text 非空), "; " 连接
std::string Answered(const Question &q, const Pick &p);

// ---- 执行记录 ----
// 时段: 会话起算秒; t0 < 0 = 未开, t1 < 0 = 在走
struct Span {
  double t0 = -1, t1 = -1;
  bool began() const { return t0 >= 0; }
  bool ended() const { return t1 >= 0; }
  bool running() const { return began() && !ended(); }
  double len(double now) const { return began() ? (ended() ? t1 : now) - t0 : 0.0; }
  void begin(double t) {
    if (!began())
      t0 = t;
  }
  void end(double t) {
    if (running())
      t1 = t;
  }
};
// eval_factor 的候选: 落文件 → 回测, 其内 search / run 两段
struct Trial {
  Staged st;
  Span search, run;
};
// 一个调用的执行: span = 开始 → 做完; out = 交给 agent 的结果; ok = 做成了 (否则 out 是原因)
struct Act {
  Call call;
  Span span;
  std::string out;
  bool ok = true;
  std::variant<std::monostate, Trial, Asked> got;
};

} // namespace agent
