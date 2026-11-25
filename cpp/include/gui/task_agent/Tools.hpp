#pragma once

// =============================================================================
// Tools — agent 的工具真相表: 系统提示的 "工具" 节 / 回复解析 / 参数校验 / 界面都从同一张表出
// =============================================================================
//   回复 = 一个调用 {"tool": 名, "args": {...}}; 系统执行后, 下一条消息交回它的结果
//   执行方式 Exec (唯一分派轴):
//     Sync   GUI 线程当场做完 (查特征叶 / 列候选与正式库 / 全局记录 / 提交 / 结束)
//     Lab    交右翼 worker 后台执行 (回测 eval_factor / 观察 view_* / 盘口重放 view_book / 研报库 search_docs · search_factors · read_doc)
//     Human  等研究员点选答案 (ask_human)
//   校验两层:
//     协议层 (ParseCall): JSON 结构 / 工具名 / 参数类型与范围 → 不合则不执行, 记成一步错, 下一条 = 重交要求
//     数据层 (执行期): 因子 / 特征 / 代码 / 日期 / 研报 id 不成 → 该调用 ok=false 写原因
//   工具清单 AGENT_TOOLS (唯一顺序 = Tool 的值): 一行一个工具 (枚举, id, 中文名, 执行, 类, 折叠后留原文);
//     Tool 挂入 AGENT_ROWS (Named + Tabled): def(t) 编译期取元数据, IdOf / FindId / JSON 序列化与其他枚举同一套
//     说明表 doc(t) (Tools.cpp) 存编译期类型化参数表 ArgDef 与成文说明; 实现表 kSpecs (tools/Kit.cpp) 按清单拼名并编译期核签名
//   每个工具四面对仗: 输入 Call → 结构化结果 Res (唯一载体, tools/<类>.hpp) → 原文 text()/gist() (tools/<类>/<工具>.cpp) ↔ 展示 draw() (ui/<类>/<工具>.cpp)
//   参数表 ArgDef 同时描述外部 agent 接口工具 (open / run, Remote.cpp) 的参数: 校验 ArgsOf 与 inputSchema SchemaOf 同表出
// =============================================================================

#include "gui/task_agent/Enum.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace GUI::Agent {

enum class Exec : uint8_t { Sync,
                            Lab,
                            Human };
struct ExecDef {
  const char *cn;
};
inline constexpr ExecDef kExecs[] = {{"当场"}, {"后台"}, {"等人"}};
AGENT_ROWS(Exec, kExecs, Human);

enum class Cat : uint8_t { Look,   // view_*
                           Trial,  // eval_factor / submit_factor
                           Query,  // search_* / read_doc / list_*
                           Note,   // request_* / note_insight / report_issue
                           Misc }; // finish / ask_human
struct CatDef {
  const char *en, *cn; // en = 主栏 Tools 下的组名
};
inline constexpr CatDef kCats[] = {{"Look", "观察"}, {"Trial", "回测"}, {"Query", "检索"}, {"Note", "记录"}, {"Misc", "其他"}};
AGENT_ROWS(Cat, kCats, Misc);

// 工具清单: X(枚举, id, 中文名, 执行, 类, 上下文折叠后仍留原文)
#define AGENT_TOOLS(X)                                                \
  X(ViewFeature, "view_feature", "看特征", Lab, Look, false)          \
  X(ViewLayers, "view_layers", "看分层", Lab, Look, false)            \
  X(ViewTrades, "view_trades", "看交割单", Lab, Look, false)          \
  X(ViewHolders, "view_holders", "看常客", Lab, Look, false)          \
  X(ViewResponse, "view_response", "看响应", Lab, Look, false)        \
  X(ViewExplain, "view_explain", "看成分", Lab, Look, false)          \
  X(ViewSnapshot, "view_snapshot", "看截面快照", Lab, Look, false)    \
  X(ViewStockDay, "view_stock_day", "看个股一天", Lab, Look, false)   \
  X(ViewStockDays, "view_stock_days", "看个股多日", Lab, Look, false) \
  X(ViewBook, "view_book", "看盘口重放", Lab, Look, false)            \
  X(SearchLeaves, "search_leaves", "查特征叶", Sync, Query, false)    \
  X(SearchDocs, "search_docs", "查研报", Lab, Query, false)           \
  X(SearchFactors, "search_factors", "查研报因子", Lab, Query, false) \
  X(ReadDoc, "read_doc", "读研报", Lab, Query, false)                 \
  X(EvalFactor, "eval_factor", "回测", Lab, Trial, false)             \
  X(ListTried, "list_tried", "列已试", Sync, Query, false)            \
  X(ListLibrary, "list_library", "列正式库", Sync, Query, false)      \
  X(ListNotes, "list_notes", "查全局记录", Sync, Query, false)        \
  X(SubmitFactor, "submit_factor", "提交入库", Sync, Trial, true)     \
  X(RequestFeature, "request_feature", "申请特征", Sync, Note, true)  \
  X(RequestTool, "request_tool", "申请工具", Sync, Note, true)        \
  X(NoteInsight, "note_insight", "记发现", Sync, Note, true)          \
  X(ReportIssue, "report_issue", "报问题", Sync, Note, true)          \
  X(Finish, "finish", "结束", Sync, Misc, true)                       \
  X(AskHuman, "ask_human", "问研究员", Human, Misc, true)

#define AGENT_TOOL_ID(T, id, cn, E, C, keep) T,
enum class Tool : uint8_t { AGENT_TOOLS(AGENT_TOOL_ID) };
#undef AGENT_TOOL_ID

struct ToolMeta {
  const char *id, *cn;
  Exec exec;
  Cat cat;
  bool keep;
};
#define AGENT_TOOL_META(T, id, cn, E, C, keep) {id, cn, Exec::E, Cat::C, keep},
inline constexpr ToolMeta kTools[] = {AGENT_TOOLS(AGENT_TOOL_META)};
#undef AGENT_TOOL_META
AGENT_ROWS(Tool, kTools, AskHuman);

enum class Arg : uint8_t { Str,
                           Int,
                           Bool,
                           Strs,
                           Obj,
                           Questions };
struct EmptyArr {};
struct Unset {};                                                                           // 可不给: 不给即不填 (由收参的一方定)
using ArgVal = std::variant<std::monostate, std::string_view, int, bool, EmptyArr, Unset>; // monostate = 必填
using Choices = std::span<const std::string_view>;

struct ArgDef {
  const char *key, *cn;
  Arg type;
  ArgVal def; // 缺省值; monostate = 必填, Unset = 可不给
  int lo = 0, hi = 0;
  Choices choices; // Str 可选集 (缺省为 "" 时另许空串); 空 = 任意串
  const char *desc = "";
  constexpr bool required() const { return std::holds_alternative<std::monostate>(def); }
  constexpr bool unset() const { return std::holds_alternative<Unset>(def); }
  constexpr bool empty_ok() const {
    const auto *s = std::get_if<std::string_view>(&def);
    return s && s->empty();
  }
};
using ArgDefs = std::span<const ArgDef>;

struct ToolDoc {
  Tool id;
  ArgDefs args;
  const char *desc = "", *out = "";
};

const ToolDoc &doc(Tool t);
std::string ToolText(const char *s);
std::string ArgType(const ArgDef &a);
std::string ArgText(const ArgDef &a);
nlohmann::ordered_json DefaultJson(const ArgDef &a);
nlohmann::ordered_json SeedJson(const ArgDef &a);
bool IsDefault(const ArgDef &a, const nlohmann::ordered_json &v);
std::string FieldsText(Tool t, const nlohmann::ordered_json &args, std::initializer_list<std::string_view> skip, std::string_view bullet = "");

// 一个已通过参数表校验的调用 (缺省已补齐, 键按参数表序)
struct Call {
  Tool tool = Tool::ViewFeature;
  nlohmann::ordered_json args;
  std::string str(const char *k) const;
  int num(const char *k) const;
  std::vector<std::string> strs(const char *k) const;
  std::string text(size_t cap = 0) const;
};

// 一组参数按参数表校验并补缺省 (owner = 报错里参数表的属主, path = 参数的前缀; 键按参数表序); 不合的各条进 errs
bool ArgsOf(ArgDefs spec, std::string_view owner, const nlohmann::ordered_json &args, const std::string &path, nlohmann::ordered_json &out, std::vector<std::string> &errs);
bool CallOf(Tool t, const nlohmann::ordered_json &args, const std::string &path, Call &out, std::vector<std::string> &errs);
nlohmann::ordered_json SchemaOf(ArgDefs spec); // 参数表的 JSON Schema (外部 agent 接口的 inputSchema)

// 大脑的一条回复 ↔ 一个调用 (协议层): 解析 (不合 = error 一行一处) / 写回协议形
struct CallCheck {
  Call call;
  std::string error;
  bool ok() const { return error.empty(); }
};
CallCheck ParseCall(std::string_view reply, bool ask);
std::string CallJson(const Call &c);
std::string ToolsDoc(bool ask);

// ---- 工具的结构化结果 (唯一载体: Mine 时间线与 Bench 工作台共用) ----
struct Res {
  virtual ~Res() = default;
  virtual std::string text() const = 0;            // 交给 agent 的原文
  virtual std::string gist() const { return {}; }  // 上下文折叠后留的一行关键数 (无则空)
  virtual std::string title() const { return {}; } // 时间线上这个调用一行里代替调用原文的一句 (无则空)
  virtual void draw() const = 0;                   // 结构化展示
};
using ResPtr = std::shared_ptr<Res>;

struct ToolOut {
  bool ok = true;
  std::string text, gist;
  ResPtr res;
};
template <class R>
ToolOut Done(std::shared_ptr<R> r, bool ok = true) {
  std::string text = r->text(), gist = r->gist();
  return {ok, std::move(text), std::move(gist), std::move(r)};
}
inline ToolOut Fail(std::string why) { return {false, std::move(why), {}, {}}; }

// ---- ask_human: 问题与答复 ----
struct Question {
  std::string prompt;
  std::vector<std::string> options;
  std::vector<bool> recommended;
  bool multiple = false;
};
using Questions = std::vector<Question>;

struct Pick {
  std::vector<bool> on;
  bool other = false;
  std::string text;
};
struct Asked {
  Questions qs;
  std::vector<Pick> picks;
  std::string note;
  Asked() = default;
  explicit Asked(Questions q);
};
Questions QuestionsOf(const Call &c);
std::string Picked(const Question &q, const std::vector<bool> &mask);
std::string Answered(const Question &q, const Pick &p);

} // namespace GUI::Agent
