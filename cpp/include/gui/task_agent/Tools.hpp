#pragma once

// =============================================================================
// Tools — agent 的工具真相表: 系统提示的 "工具" 节 / 回复解析 / 参数校验 / 界面都从同一张表出
// =============================================================================
//   回复 = {"calls": [{"tool": 名, "args": {...}}, ...]}; 系统按序执行, 下一条消息按序交回结果
//   执行方式 Exec (唯一分派轴):
//     Sync   GUI 线程当场做完 (查特征叶 / 列候选与正式库 / 全局记录 / 假设与判定 / 提交 / 结束)
//     Lab    交右翼 worker 后台执行 (回测 eval_factor / 观察 view_* / 盘口重放 view_book / 研报库 search_docs · search_factors · read_doc)
//     Human  等研究员点选答案 (ask_human)
//   校验两层:
//     协议层 (ParseCalls + RuleMiss): JSON 结构 / 工具名 / 参数类型与范围 / 回复级约束 → 不合则整条退回重发
//     数据层 (执行期): 因子 / 特征 / 代码 / 日期 / 假设 / 证据 / 研报 id 不成 → 仅该调用 ok=false 写原因, 其余照做
//   每个工具三部分对仗: 输入 Call → 结果 Res (该工具独有的结构, tools/<类>.hpp) → 交给 agent 的原文 text() (tools/<类>/<工具>.cpp) ↔ 工作台的展示 draw() (ui/<类>/<工具>.cpp)
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

enum class Exec : uint8_t { Sync,
                            Lab,
                            Human };
struct ExecDef {
  const char *cn;
};
inline constexpr ExecDef kExecs[] = {{"当场"}, {"后台"}, {"等人"}};
AGENT_ROWS(Exec, kExecs, Human);

enum class Cat : uint8_t { Look,  // view_*
                           Hyp,   // hypothesize / judge
                           Trial, // eval_factor / submit_factor
                           Query, // search_* / read_doc / list_*
                           Note,  // request_* / note_insight / report_issue
                           Misc };
struct CatDef {
  const char *cn;
};
inline constexpr CatDef kCats[] = {{"观察"}, {"假设"}, {"回测"}, {"检索"}, {"记录"}, {"其他"}};
AGENT_ROWS(Cat, kCats, Misc);

enum class Arg : uint8_t { Str,
                           Int,
                           Strs,
                           Questions };
struct ArgDef {
  const char *key, *cn;
  Arg type;
  const char *def;     // 缺省值 (JSON 字面量); nullptr = 必填
  int lo, hi;          // Int 的取值范围 / Strs 与 Questions 的元素个数 (闭区间)
  const char *choices; // Str 的可选值 "a|b|c" (缺省为 "" 的另许空串); nullptr = 任意串
  const char *desc;
};

struct ToolDef {
  Tool id;
  const char *name, *cn;
  Exec exec;
  Cat cat;
  bool keep; // 研究树里留结论原文 (提交 / 全局记录 / 问答 / finish)
  std::span<const ArgDef> args;
  const char *desc, *out;
};

std::span<const ToolDef> Tools();
const ToolDef &def(Tool t);
Tool ToolOf(std::string_view name);
std::string ToolText(const char *s);
std::string ArgType(const ArgDef &a);
std::string ArgText(const ArgDef &a);
std::string FieldsText(Tool t, const nlohmann::ordered_json &args, std::initializer_list<std::string_view> skip, std::string_view bullet = "");

// 一个已通过参数表校验的调用 (缺省已补齐, 键按参数表序)
struct Call {
  Tool tool;
  nlohmann::ordered_json args;
  std::string str(const char *k) const;
  int num(const char *k) const;
  std::vector<std::string> strs(const char *k) const;
  std::string text(size_t cap = 0) const;
};

// 一个调用的参数按参数表校验并补缺省 (path = 报错里参数的前缀); 不合的各条进 errs
bool CallOf(Tool t, const nlohmann::ordered_json &args, const std::string &path, Call &out, std::vector<std::string> &errs);
bool ParseCalls(std::string_view reply, bool ask, std::vector<Call> &out, std::string &error);
std::string ToolsDoc(bool ask);

// ---- 工具的结果 ----
struct Res {
  virtual ~Res() = default;
  virtual std::string text() const = 0;           // 交给 agent 的原文
  virtual std::string gist() const { return {}; } // 收进研究树后留的一行关键数 (无则空)
  virtual void draw() const = 0;                  // 工作台的展示
};
using ResPtr = std::shared_ptr<const Res>;

// 一个调用的产出: ok = 做成了; out = 交给 agent 的原文 (有 res 即 res->text()); 没做成而无结构的, out 写原因
struct ToolOut {
  bool ok = false;
  std::string out, gist;
  ResPtr res;
};
template <class R>
ToolOut Done(std::shared_ptr<R> r, bool ok = true) {
  std::string out = r->text(), gist = r->gist();
  return {ok, std::move(out), std::move(gist), std::move(r)};
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
