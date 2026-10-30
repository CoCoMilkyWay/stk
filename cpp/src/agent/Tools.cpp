// Tools — 见头文件
#include "agent/Tools.hpp"
#include "agent/Config.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <string>
#include <utility>

namespace agent {

namespace {

using json = nlohmann::ordered_json;

// ---- 工具表 ----
// 因子目标 (观察工具共用一个写法)
constexpr const char *kFactorDesc = "候选英文名 (取它在本持有期的最佳权重, 含方向) 或具体根式 (根须是归一算子, 参数写数字; 按原方向看)";
constexpr const char *kSideDesc = "看哪端: top = 做多端 (最高组 G20) / bottom = 最低组 G1";
constexpr const char *kExprsDesc = "特征式: 任意不含自由参数的子式, 不必套根, 单个叶也行";

constexpr ArgDef kFeatureArgs[] = {{"exprs", Arg::Strs, nullptr, 1, 4, nullptr, kExprsDesc}};
constexpr ArgDef kLayersArgs[] = {{"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
                                  {"split", Arg::Str, "\"none\"", 0, 0, "none|tod|month", "切开看: none 不切 / tod 按日内时段 / month 按月"}};
constexpr ArgDef kTradesArgs[] = {
    {"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
    {"side", Arg::Str, "\"top\"", 0, 0, "top|bottom", kSideDesc},
    {"sort", Arg::Str, "\"worst\"", 0, 0, "worst|best|random", "明细挑哪些段: worst 超额最低 / best 最高 / random 随机 (固定种子)"},
    {"n", Arg::Int, "12", 1, 30, nullptr, "明细段数"},
    {"merge", Arg::Str, "\"run\"", 0, 0, "none|run|day", "合并成段: none 每行一笔 / run 同一资产同日相邻行连成一段 / day 同一资产同日并成一段"}};
constexpr ArgDef kHoldersArgs[] = {{"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
                                   {"side", Arg::Str, "\"top\"", 0, 0, "top|bottom", kSideDesc},
                                   {"n", Arg::Int, "15", 1, 30, nullptr, "列几只"}};
constexpr ArgDef kStockDayArgs[] = {{"code", Arg::Str, nullptr, 0, 0, nullptr, "6 位代码或 代码.交易所 (如 000001 / 000001.SZ)"},
                                    {"date", Arg::Str, nullptr, 0, 0, nullptr, "交易日 YYYY-MM-DD (须在评估期内)"},
                                    {"factor", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 叠加看的因子, 写法同其他观察工具的 factor"},
                                    {"exprs", Arg::Strs, "[]", 0, 4, nullptr, "可选: 叠加看的特征式 (写法同 view_feature)"}};
constexpr ArgDef kStockDaysArgs[] = {{"code", Arg::Str, nullptr, 0, 0, nullptr, "6 位代码或 代码.交易所"},
                                     {"from", Arg::Str, "\"\"", 0, 0, nullptr, "首日 YYYY-MM-DD; 空 = 末日往前共 20 个交易日"},
                                     {"to", Arg::Str, "\"\"", 0, 0, nullptr, "末日 YYYY-MM-DD; 空 = 评估期末日"},
                                     {"factor", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 叠加看的因子"},
                                     {"exprs", Arg::Strs, "[]", 0, 4, nullptr, "可选: 叠加看的特征式 (各日取收盘行的值)"}};
constexpr ArgDef kEvalArgs[] = {{"name", Arg::Str, nullptr, 0, 0, nullptr, "英文名: 仅 [A-Za-z0-9_], ≤ 64, 作文件名, 不能与已有的重复"},
                                {"name_cn", Arg::Str, nullptr, 0, 0, nullptr, "中文名: 1..10 个汉字, 不含字母数字标点"},
                                {"expr", Arg::Str, nullptr, 0, 0, nullptr, "模板 (可含自由参数)"},
                                {"note", Arg::Str, "\"\"", 0, 0, nullptr, "相对哪个版本改了什么、想验证什么; 由观察促成的写明是哪个调用看到了什么"}};
constexpr ArgDef kTopArgs[] = {{"top", Arg::Int, "20", 1, kListTop, nullptr, "列前几个"}};
constexpr ArgDef kNameArgs[] = {{"name", Arg::Str, nullptr, 0, 0, nullptr, "已评候选的英文名 (本作用域历次会话的都可以)"}};
constexpr ArgDef kIssueArgs[] = {{"text", Arg::Str, nullptr, 0, 0, nullptr, "疑问 / 建议"}};
constexpr ArgDef kAskArgs[] = {{"questions", Arg::Questions, nullptr, 1, 8, nullptr,
                                "问题数组, 每题 {\"prompt\": 题干, \"options\": [2..5 个字符串], \"multiple\": 可多选 (缺省 false), \"recommended\": [推荐的选项下标]}"}};

constexpr ToolDef kTools[] = {
    {Tool::ViewFeature, "view_feature", "看特征", Run::Look, kFeatureArgs,
     "看特征长什么样: 池内分布 (分位) / 日内轮廓 (各行截面中位数) / 方差来自哪 (资产间 · 日间 · 日内) / 持续性 (同一资产的自相关) / 与本持有期收益的截面 rIC; "
     "给多个时附两两截面相关. 造新特征前先看原料, 造完看它是否如你所想",
     "每个式子一节统计 (带火花线) + 多个时的相关矩阵"},
    {Tool::ViewLayers, "view_layers", "看分层", Run::Look, kLayersArgs,
     "看因子的 20 组分层: 每组每期超额 / mono / 多空 / rIC, 做多端与最低组各贡献多少; 切开看分层在哪些时段 / 月份成立、哪些失效",
     "全期 20 组 + 切分时每段一行 (行数 / mono / 多空 / rIC / 首中末组)"},
    {Tool::ViewTrades, "view_trades", "看交割单", Run::Look, kTradesArgs,
     "看因子一端真持有过的仓位 (与分层回测同一份持仓判据, 建不了仓的不算): 段收益的分布与集中度, 按开仓时段 / 标签 (涨跌停附近 · 当日大涨大跌 · 日内高低位 · 放量 · "
     "竞价行) 的占比提升与段收益, 再列最差 / 最好 / 随机若干段的明细 (代码与日期可接 view_stock_day). 用来找收益从哪来、亏在哪类情形",
     "汇总 + 时段表 + 标签表 + 明细表"},
    {Tool::ViewHolders, "view_holders", "看常客", Run::Look, kHoldersArgs,
     "看哪些资产最常落在因子一端: 本端行数 / 占其在池行的比例 / 涉及天数 / 平均连续行数 / 本端持仓平均超额, 附集中度. 用来看因子是否只在少数票上起作用、在挑哪类票",
     "集中度一行 + 资产表"},
    {Tool::ViewStockDay, "view_stock_day", "看个股一天", Run::Look, kStockDayArgs,
     "看一只股票一天的逐行走势: 价 / 量 / 因子分位 / 特征的火花线, 加分段表 (竞价行 + 每段约 30 分钟: 涨跌 / 段内高低 / 量占比 / 因子分位 / 后续持有期超额 / 特征段末值). "
     "用来对照盘面现象与因子何时高、何时低",
     "当日概况 + 火花线 + 分段表"},
    {Tool::ViewStockDays, "view_stock_days", "看个股多日", Run::Look, kStockDaysArgs,
     "看一只股票连续多日 (至多 {stock_days} 日) 的日级概况: 每日一行 (涨跌 / 振幅 / 成交额 / 在池 / 因子日均与收盘分位 / 两端行数 / 收盘开仓的后续超额 / 特征收盘值), 附日级火花线",
     "火花线 + 逐日表"},
    {Tool::EvalFactor, "eval_factor", "回测", Run::Eval, kEvalArgs,
     "把一个模板落成候选文件并回测: Search (定方向 + 搜自由参数) → 各权重全量分层回测. 一条回复至多 {batch} 个, 都应是当前在做那个因子的变体",
     "结局: 未落文件的原因 / BROKEN / 无显著 (落选原因 + 代表点) / 各权重的统计"},
    {Tool::ListTried, "list_tried", "列已试", Run::Now, kTopArgs, "列本作用域已评过的候选 (历次会话累积, 只列同一评估期的), 按 score 降序; 可作起点接着做",
     "每个候选: 模板 + 结局"},
    {Tool::ListLibrary, "list_library", "列正式库", Run::Now, kTopArgs,
     "列正式因子库里同一评估期且算子不超上限的因子在本持有期的成绩, 按 score 降序; 可作起点接着做", "每个因子: 模板 + 思路 + 最佳权重"},
    {Tool::SubmitFactor, "submit_factor", "提交入库", Run::Now, kNameArgs, "把一个已评候选提交入库 (至少一个权重过入库门槛才入); 一般只在最后交最好的版本",
     "入库 / 未入库的原因"},
    {Tool::NoteIssue, "note_issue", "记 issue", Run::Now, kIssueArgs, "记下想问研究员的疑问、对规则或系统的建议; 不阻塞, 会话结束后一起交给研究员", "已记下"},
    {Tool::Finish, "finish", "结束", Run::Now, {}, "宣布挖完: 须是本条回复的最后一个调用, 前面的调用照常执行后会话结束", "—"},
    {Tool::AskHuman, "ask_human", "问研究员", Run::Human, kAskArgs,
     "向研究员提问 (每题带推荐), 等他选答; 整个会话只有一次机会. 只问研究员才能定的事 (从哪个因子起步、往哪个方向挖); 系统提示已定的不问也不确认. "
     "研究员每题都能选 \"其他\" 自己写, 还能附言, 所以选项不必穷举, 不要列 \"其他\" / \"由你决定\" 之类的兜底项",
     "每题的选择 (没照推荐的附上推荐) + 附言"},
};
static_assert(std::size(kTools) == static_cast<size_t>(Tool::AskHuman) + 1);
constexpr bool tools_in_order() {
  for (size_t i = 0; i < std::size(kTools); ++i)
    if (static_cast<size_t>(kTools[i].id) != i)
      return false;
  return true;
}
static_assert(tools_in_order(), "kTools 下标 = Tool");

const ToolDef *find(std::string_view name) {
  const auto it = std::find_if(std::begin(kTools), std::end(kTools), [&](const ToolDef &d) { return name == d.name; });
  return it == std::end(kTools) ? nullptr : &*it;
}

// 工具说明里的占位: {batch} → kBatch, {stock_days} → kStockDays
std::string text_of(const char *s) {
  std::string t = s;
  for (const auto &[key, v] : {std::pair<std::string_view, int>{"{batch}", kBatch}, {"{stock_days}", kStockDays}})
    if (const size_t p = t.find(key); p != std::string::npos)
      t.replace(p, key.size(), std::to_string(v));
  return t;
}

// ---- 回复解析 ----
// 回复 → JSON 对象 (取首个 '{' 到末个 '}', 容忍外包代码块)
bool object_of(std::string_view text, json &j, std::string &err) {
  const size_t a = text.find('{'), b = text.rfind('}');
  if (a == std::string_view::npos || b == std::string_view::npos || b < a) {
    err = "回复里没有 JSON 对象";
    return false;
  }
  j = json::parse(text.substr(a, b - a + 1), nullptr, false);
  if (j.is_discarded() || !j.is_object()) {
    err = "回复不是合法的 JSON 对象";
    return false;
  }
  return true;
}

std::string at(const std::string &k, size_t i) { return k + "[" + std::to_string(i) + "]"; }

// 一个参数值按参数表查; 不合 → false + err
bool check(const ArgDef &a, const json &v, const std::string &path, std::string &err) {
  const auto fail = [&](std::string why) {
    err = path + " " + std::move(why);
    return false;
  };
  switch (a.type) {
  case Arg::Str: {
    if (!v.is_string())
      return fail("须是字符串");
    const std::string s = v.get<std::string>();
    if (a.choices) {
      const std::string all = std::string("|") + a.choices + "|";
      if (s.empty() || all.find("|" + s + "|") == std::string::npos)
        return fail(std::string("须是 ") + a.choices + " 之一");
    } else if (s.empty() && !a.def) {
      return fail("不能为空");
    }
    return true;
  }
  case Arg::Int:
    if (!v.is_number_integer() || v.get<int64_t>() < a.lo || v.get<int64_t>() > a.hi)
      return fail("须是 " + std::to_string(a.lo) + ".." + std::to_string(a.hi) + " 的整数");
    return true;
  case Arg::Strs:
    if (!v.is_array() || v.size() < static_cast<size_t>(a.lo) || v.size() > static_cast<size_t>(a.hi))
      return fail("须是 " + std::to_string(a.lo) + ".." + std::to_string(a.hi) + " 个字符串的数组");
    for (const json &x : v)
      if (!x.is_string() || x.get<std::string>().empty())
        return fail("的每项须是非空字符串");
    return true;
  case Arg::Questions:
    if (!v.is_array() || v.size() < static_cast<size_t>(a.lo) || v.size() > static_cast<size_t>(a.hi))
      return fail("须是 " + std::to_string(a.lo) + ".." + std::to_string(a.hi) + " 个问题的数组");
    return true;
  }
  return true;
}

// ask_human 的问题数组 (形已由 check 查过) → Questions; 不合 → false + err
bool questions_of(const json &v, const std::string &path, Questions &out, std::string &err) {
  out.clear();
  for (size_t i = 0; i < v.size(); ++i) {
    const std::string p = at(path, i);
    const json &q = v[i];
    if (!q.is_object()) {
      err = p + " 须是 JSON 对象";
      return false;
    }
    Question x;
    if (!q.contains("prompt") || !q["prompt"].is_string() || q["prompt"].get<std::string>().empty()) {
      err = p + ".prompt 须是非空字符串";
      return false;
    }
    x.prompt = q["prompt"].get<std::string>();
    if (!q.contains("options") || !q["options"].is_array() || q["options"].size() < 2 || q["options"].size() > 5 ||
        !std::all_of(q["options"].begin(), q["options"].end(), [](const json &o) { return o.is_string() && !o.get<std::string>().empty(); })) {
      err = p + ".options 须是 2..5 个非空字符串的数组 (每项一句话写完这个决定, 不是对象)";
      return false;
    }
    for (const json &o : q["options"])
      x.options.push_back(o.get<std::string>());
    if (q.contains("multiple")) {
      if (!q["multiple"].is_boolean()) {
        err = p + ".multiple 须是布尔";
        return false;
      }
      x.multiple = q["multiple"].get<bool>();
    }
    x.recommended.assign(x.options.size(), false);
    bool ok = q.contains("recommended") && q["recommended"].is_array();
    if (ok) {
      const json &r = q["recommended"];
      ok = x.multiple ? !r.empty() : r.size() == 1;
      for (const json &k : r) {
        ok = ok && k.is_number_integer() && k.get<int64_t>() >= 0 && k.get<int64_t>() < static_cast<int64_t>(x.options.size()) &&
             !x.recommended[k.get<size_t>()];
        if (ok)
          x.recommended[k.get<size_t>()] = true;
      }
    }
    if (!ok) {
      err = p + ".recommended 须是选项下标数组 (从 0 起, 不重复; 单选恰 1 个, 多选至少 1 个)";
      return false;
    }
    out.push_back(std::move(x));
  }
  return true;
}

// 参数值的一行写法 (字符串不加引号, 数组用 [a, b])
std::string brief(const json &v) {
  if (v.is_string())
    return v.get<std::string>();
  if (v.is_array() && std::all_of(v.begin(), v.end(), [](const json &x) { return x.is_string(); })) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i)
      s += (i ? ", " : "") + v[i].get<std::string>();
    return s + "]";
  }
  return v.dump();
}

} // namespace

std::span<const ToolDef> Tools() { return kTools; }
const ToolDef &def(Tool t) { return kTools[static_cast<size_t>(t)]; }

std::string Call::str(const char *k) const { return args.at(k).get<std::string>(); }
int Call::num(const char *k) const { return args.at(k).get<int>(); }
std::vector<std::string> Call::strs(const char *k) const { return args.at(k).get<std::vector<std::string>>(); }

std::string Call::text() const {
  std::string s = std::string(def(tool).name) + "(";
  bool first = true;
  for (const ArgDef &a : def(tool).args) {
    const json &v = args.at(a.key);
    if (a.type == Arg::Questions) {
      s += (first ? "" : ", ") + std::to_string(v.size()) + " 个问题";
    } else {
      if (a.def && v == json::parse(a.def)) // 缺省值不写
        continue;
      s += (first ? "" : ", ") + std::string(a.key) + "=" + brief(v);
    }
    first = false;
  }
  return s + ")";
}

bool ParseCalls(std::string_view reply, bool ask, std::vector<Call> &out, std::string &err) {
  out.clear(), err.clear();
  json j;
  if (!object_of(reply, j, err))
    return false;
  if (!j.contains("calls") || !j["calls"].is_array()) {
    err = "缺数组 calls (每次回复 = 一串工具调用)";
    return false;
  }
  const json &cs = j["calls"];
  if (cs.empty() || cs.size() > static_cast<size_t>(kMaxCalls)) {
    err = "calls 须有 1.." + std::to_string(kMaxCalls) + " 个调用";
    return false;
  }
  int evals = 0;
  for (size_t i = 0; i < cs.size(); ++i) {
    const std::string p = at("calls", i);
    const json &c = cs[i];
    if (!c.is_object() || !c.contains("tool") || !c["tool"].is_string()) {
      err = p + " 须是 {\"tool\": 工具名, \"args\": {...}}";
      return false;
    }
    const std::string name = c["tool"].get<std::string>();
    const ToolDef *d = find(name);
    if (!d || (d->id == Tool::AskHuman && !ask)) {
      err = p + ".tool: " + (d ? "ask_human 本会话不可用 (研究员没开提问, 或已问过一次; 一条回复也只能有一个)" : "没有工具 " + name);
      return false;
    }
    if (d->id == Tool::AskHuman)
      ask = false;
    if (d->id == Tool::Finish && i + 1 != cs.size()) {
      err = p + ": finish 须是最后一个调用";
      return false;
    }
    if (d->id == Tool::EvalFactor && ++evals > kBatch) {
      err = "一条回复至多 " + std::to_string(kBatch) + " 个 eval_factor";
      return false;
    }
    const json args = c.contains("args") ? c["args"] : json::object();
    if (!args.is_object()) {
      err = p + ".args 须是 JSON 对象";
      return false;
    }
    for (const auto &[k, v] : args.items())
      if (std::none_of(d->args.begin(), d->args.end(), [&](const ArgDef &a) { return k == a.key; })) {
        err = p + ".args." + k + ": " + name + " 没有这个参数";
        return false;
      }
    Call call{d->id, json::object()};
    for (const ArgDef &a : d->args) {
      const std::string ap = p + ".args." + a.key;
      if (!args.contains(a.key)) {
        if (!a.def) {
          err = "缺参数 " + ap;
          return false;
        }
        call.args[a.key] = json::parse(a.def);
        continue;
      }
      if (!check(a, args[a.key], ap, err))
        return false;
      Questions qs;
      if (a.type == Arg::Questions && !questions_of(args[a.key], ap, qs, err))
        return false;
      call.args[a.key] = args[a.key];
    }
    out.push_back(std::move(call));
  }
  return true;
}

std::string ToolsDoc(bool ask) {
  std::string s;
  for (const ToolDef &d : kTools) {
    if (d.id == Tool::AskHuman && !ask)
      continue;
    s += "## " + std::string(d.name) + " — " + d.cn + "\n" + text_of(d.desc) + "\n\n";
    if (d.args.empty()) {
      s += "参数: 无\n";
    } else {
      s += "参数:\n";
      for (const ArgDef &a : d.args) {
        std::string t;
        switch (a.type) {
        case Arg::Str:
          t = a.choices ? std::string("取 ") + a.choices : "字符串";
          break;
        case Arg::Int:
          t = "整数 " + std::to_string(a.lo) + ".." + std::to_string(a.hi);
          break;
        case Arg::Strs:
          t = "字符串数组, " + std::to_string(a.lo) + ".." + std::to_string(a.hi) + " 个";
          break;
        case Arg::Questions:
          t = "数组, " + std::to_string(a.lo) + ".." + std::to_string(a.hi) + " 题";
          break;
        }
        s += "- `" + std::string(a.key) + "` (" + t + (a.def ? ", 缺省 " + std::string(a.def) : std::string(", 必填")) + "): " + a.desc + "\n";
      }
    }
    s += "返回: " + std::string(d.out) + "\n\n";
  }
  return s;
}

Asked::Asked(Questions q) : qs(std::move(q)) {
  for (const Question &x : qs)
    picks.push_back({x.recommended, false, {}});
}

Questions QuestionsOf(const Call &c) {
  assert(c.tool == Tool::AskHuman);
  Questions qs;
  std::string err;
  [[maybe_unused]] const bool ok = questions_of(c.args.at("questions"), "questions", qs, err);
  assert(ok && "解析时已校验");
  return qs;
}

std::string Picked(const Question &q, const std::vector<bool> &mask) {
  assert(mask.size() == q.options.size());
  std::string s;
  for (size_t j = 0; j < q.options.size(); ++j)
    if (mask[j])
      s += (s.empty() ? "" : "; ") + q.options[j];
  return s;
}

std::string Answered(const Question &q, const Pick &p) {
  std::string s = Picked(q, p.on);
  if (p.other && !p.text.empty())
    s += (s.empty() ? "" : "; ") + ("其他: " + p.text);
  return s;
}

} // namespace agent
