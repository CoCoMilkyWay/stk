#pragma once

// =============================================================================
// Tools — agent 的工具箱 (类 MCP): 一张工具表, 系统提示的 "工具" 节 / 回复解析 / 参数校验 / 界面名都从它出
// =============================================================================
//   回复 = 一串调用 {"calls": [{"tool": 名, "args": {...}}, ...]}; 系统按序执行, 下条消息按序给回各自的结果 (Prompt round)
//   工具 = 名 + 执行方 + 用途类 + 研究树里留不留原样 + 参数表 (键 / 类型 / 缺省 / 取值范围 / 说明) + 做什么 / 返回什么; 加工具 = 表里加一行 + 执行方里接一个分支
//   执行方 Run: Now 当场 (GUI 线程, 只读会话状态) / Human 等人 / Eval 回测 (Lab 跑 EvalSpec) / Look 观察 (Lab 跑 LookSpec 或 BookSpec; 取参与成文在 task_agent/Views) /
//              Docs 研报库 (GUI 线程当场查, task_agent/Docs 成文)
//   校验两层: 解析时按参数表 (类型 / 必填 / 范围 / 可选值; 不合 → 整条回复重发); 执行时按数据 (因子 / 特征 / 资产 / 日期 / 流派 / 研报不成 → 该调用的结果写原因, 其余照做)
//   回复级约束: 1..kMaxCalls 个调用; eval_factor 至多 kBatch 个; finish 只能在末尾; ask_human 只在会话允许提问且还没问过时可用;
//              随会话状态的 (上条有回测则本条不能回测 / 待判的假设须 judge) 见 task_agent/Acts RuleMiss
//   执行记录 Act: 一个调用的时段 / 交给 agent 的结果 / 落进会话状态要的那份 (eval_factor 的回测 Trial, ask_human 的问答 Asked, search_leaves 列出的叶 Shown;
//              另有观察的关键数 Seen, 只给研究树);
//              执行见 gui/task_agent/Acts.hpp, 落进状态见 gui/task_agent/Timeline.hpp Apply
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Enum.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <initializer_list>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace GUI::Agent {

enum class Tool : uint8_t { ViewFeature,
                            ViewLayers,
                            ViewTrades,
                            ViewHolders,
                            ViewResponse,
                            ViewExplain,
                            ViewSnapshot,
                            ViewStockDay,
                            ViewStockDays,
                            ViewBook,
                            SearchLeaves,
                            SearchDocs,
                            SearchFactors,
                            ReadDoc,
                            Hypothesize,
                            Judge,
                            EvalFactor,
                            ListTried,
                            ListLibrary,
                            ListNotes,
                            SubmitFactor,
                            RequestFeature,
                            RequestTool,
                            NoteInsight,
                            ReportIssue,
                            Finish,
                            AskHuman };
inline constexpr size_t kToolCount = static_cast<size_t>(Tool::AskHuman) + 1;
// 执行方 (下标 = Run): cn = 界面名
enum class Run : uint8_t { Now,
                           Human,
                           Eval,
                           Look,
                           Docs };
struct RunDef {
  const char *cn;
};
inline constexpr RunDef kRuns[] = {{"当场"}, {"等人"}, {"回测"}, {"观察"}, {"研报库"}};
AGENT_ROWS(Run, kRuns, Docs);
// 用途类 (下标 = Cat): 界面分组 / 计数; judge 判支持须引用的观察 = Cat::Look
enum class Cat : uint8_t { Look,  // view_*
                           Hyp,   // 提 / 判假设
                           Trial, // 回测 / 提交
                           Query, // 查特征叶 / 研报库 / 已试 / 正式库 / 全局记录
                           Note,  // 跨会话记录
                           Misc };
struct CatDef {
  const char *cn;
};
inline constexpr CatDef kCats[] = {{"观察"}, {"假设"}, {"回测"}, {"检索"}, {"记录"}, {"其他"}};
AGENT_ROWS(Cat, kCats, Misc);
enum class Arg : uint8_t { Str,
                           Int,
                           Strs,
                           Questions,
                           Parts };
struct ArgDef {
  const char *key, *cn; // 调用里的键 / 界面与报告里的标签
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
  Cat cat;
  bool keep; // 原文交回之后研究树 (task_agent/Tree) 里留结论原样; 否则只留调用一行 (回测另留一行结局); 没做成的一律留调用一行 + 原因首行
  std::span<const ArgDef> args;
  const char *desc, *out; // 做什么 (何时用) / 返回什么
};
std::span<const ToolDef> Tools(); // 下标 = Tool
const ToolDef &def(Tool t);
Tool ToolOf(std::string_view name);   // 调用名 → 工具 (须在表里)
std::string ToolText(const char *s);  // 工具说明 (desc / out) 的常量占位 {{键}} 填实 (Config kConsts, 与提示词模板同一张表)
std::string ArgType(const ArgDef &a); // 参数的类型与取值 (可选值 / 范围 / 个数)
std::string ArgText(const ArgDef &a); // 参数一行: `键` (类型, 缺省 / 必填): 说明 (工具节与界面同一份)
// 工具 t 的参数 args 里的文本字段 (字符串 / 字符串数组, 后者 ", " 连), 一行一个 "bullet标签: 值", 按参数表序; skip 里的键不列 (界面 / 报告 / 全局记录同一份)
std::string FieldsText(Tool t, const nlohmann::ordered_json &args, std::initializer_list<std::string_view> skip, std::string_view bullet = "");

// 一个调用 (已按参数表校验, 缺省已补, 键按参数表序)
struct Call {
  Tool tool;
  nlohmann::ordered_json args;
  std::string str(const char *k) const;
  int num(const char *k) const;
  std::vector<std::string> strs(const char *k) const;
  std::string text(size_t cap = 0) const; // 一行: 名(键=值, …) (流水 / 界面); cap > 0 = 每个值至多 cap 字 (结果标题)
};
// ask = 本条回复可用 ask_human; 不合协议 → false + error (全部不合处, 一行一处, 带字段路径; 经 retry 逐行反馈给模型)
bool ParseCalls(std::string_view reply, bool ask, std::vector<Call> &out, std::string &error);
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
Questions QuestionsOf(const Call &c);     // ask_human 的问题 (解析时已校验)
std::vector<Part> PartsOf(const Call &c); // eval_factor 的功能块 (解析时已校验形)
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
// eval_factor 的候选: 落文件 → 回测, 其内 search / run 两段; change = 相对 base 的改动一行 (没落 = 空); knob = 落了文件的是旋钮类改动 (取消时退回配额)
struct Trial {
  Staged st;
  std::string change;
  bool knob = false;
  Span search, run;
};
// search_leaves 列出的组里的全部叶
struct Shown {
  std::vector<std::string> leaves;
};
// 观察的关键数一行 (原文收起后研究树里留它, 见 task_agent/Views Render)
struct Seen {
  std::string gist;
};
// 一个调用的执行: span = 开始 → 做完; out = 交给 agent 的结果; ok = 做成了 (否则 out 是原因)
struct Act {
  Call call;
  Span span;
  std::string out;
  bool ok = true;
  std::variant<std::monostate, Trial, Asked, Shown, Seen> got;
};

} // namespace GUI::Agent
