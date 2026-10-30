// Tools — 见头文件
#include "agent/Tools.hpp"
#include "agent/Config.hpp"
#include "agent/Text.hpp"

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
constexpr const char *kFactorDesc =
    "候选英文名 (取它在本持有期的最佳权重, 含方向; 无显著的取其代表点) 或具体根式 (根须是归一算子, 参数写数字; 按原方向看)";
constexpr const char *kSideDesc = "看哪端: top = 做多端 (最高组 G20) / bottom = 最低组 G1";
constexpr const char *kExprsDesc = "特征式: 任意不含自由参数的子式, 不必套根, 单个叶也行";

constexpr ArgDef kFeatureArgs[] = {{"exprs", Arg::Strs, nullptr, 1, 4, nullptr, kExprsDesc}};
constexpr ArgDef kLayersArgs[] = {{"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
                                  {"split", Arg::Str, "\"none\"", 0, 0, "none|tod|month", "切开看: none 不切 / tod 按日内时段 / month 按月"}};
constexpr ArgDef kTradesArgs[] = {
    {"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
    {"side", Arg::Str, "\"top\"", 0, 0, "top|bottom", kSideDesc},
    {"sort", Arg::Str, "\"random\"", 0, 0, "random|worst|best", "明细挑哪些段: random 随机 (固定种子, 看典型持仓) / worst 超额最低 / best 最高 (两端是尾部个例)"},
    {"n", Arg::Int, "12", 1, 30, nullptr, "明细段数"},
    {"merge", Arg::Str, "\"run\"", 0, 0, "none|run|day", "合并成段: none 每行一笔 / run 同一资产同日相邻行连成一段 / day 同一资产同日并成一段"}};
constexpr ArgDef kHoldersArgs[] = {{"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
                                   {"side", Arg::Str, "\"top\"", 0, 0, "top|bottom", kSideDesc},
                                   {"n", Arg::Int, "15", 1, 30, nullptr, "列几只"}};
constexpr ArgDef kResponseArgs[] = {{"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
                                    {"side", Arg::Str, "\"top\"", 0, 0, "top|bottom", kSideDesc},
                                    {"split", Arg::Str, "\"none\"", 0, 0, "none|tod", "切开看: none 不切 / tod 按开仓时段 (日层无时段, 同 none)"}};
constexpr ArgDef kStockDayArgs[] = {{"code", Arg::Str, nullptr, 0, 0, nullptr, "6 位代码或 代码.交易所 (如 000001 / 000001.SZ)"},
                                    {"date", Arg::Str, nullptr, 0, 0, nullptr, "交易日 YYYY-MM-DD (须在评估期内)"},
                                    {"factor", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 叠加看的因子, 写法同其他观察工具的 factor"},
                                    {"exprs", Arg::Strs, "[]", 0, 4, nullptr, "可选: 叠加看的特征式 (写法同 view_feature)"}};
constexpr ArgDef kStockDaysArgs[] = {{"code", Arg::Str, nullptr, 0, 0, nullptr, "6 位代码或 代码.交易所"},
                                     {"from", Arg::Str, "\"\"", 0, 0, nullptr, "首日 YYYY-MM-DD; 空 = 末日往前共 20 个交易日"},
                                     {"to", Arg::Str, "\"\"", 0, 0, nullptr, "末日 YYYY-MM-DD; 空 = 评估期末日"},
                                     {"factor", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 叠加看的因子"},
                                     {"exprs", Arg::Strs, "[]", 0, 4, nullptr, "可选: 叠加看的特征式 (各日取收盘行的值)"}};
constexpr ArgDef kLeavesArgs[] = {{"query", Arg::Str, nullptr, 0, 0, nullptr, "关键词, 查叶名 / 中文名 / 公式 / 说明; 空格隔开的几个须同时出现 (英文不分大小写)"},
                                  {"n", Arg::Int, "30", 1, 60, nullptr, "列几个"}};
// 研报库的词表 (标注体系的 code)
constexpr const char *kStages = "L0_data|L1_factor|L2_process|L3_alpha|L4_risk|L5_portfolio|L6_evaluation|L7_execution|L8_timing";
constexpr const char *kAssets = "stock_cn|stock_hk|stock_us|future_index|future_commodity|future_bond|bond|option|convertible|fund|multi";
constexpr const char *kFamilies =
    "size|value|growth|quality|dividend|carry|analyst|momentum|reversal|volatility|liquidity|microstructure|sentiment|relation|institution|event|composite|other";
constexpr const char *kPeriods = "second|intraday|daily|weekly|monthly|quarterly|event|na";
constexpr const char *kQueryDesc = "关键词, 空格隔开的几个须同时出现 (英文不分大小写); 空 = 不限";

constexpr ArgDef kDocsArgs[] = {
    {"query", Arg::Str, "\"\"", 0, 0, nullptr, kQueryDesc},
    {"scope", Arg::Str, "\"tag\"", 0, 0, "tag|text", "query 查哪: tag 标注 (id 里的日期·机构·系列·标题 / 结论 / 因子名) / text 全文 (须给 query; 另给命中处的小节与片段)"},
    {"stage", Arg::Str, "\"\"", 0, 0, kStages, "涉及的流水线阶段; 空 = 不限"},
    {"approach", Arg::Str, "\"\"", 0, 0, nullptr, "流派 code (见分面的 阶段 → 流派); 空 = 不限"},
    {"asset", Arg::Str, "\"\"", 0, 0, kAssets, "资产; 空 = 不限"},
    {"freq", Arg::Str, "\"\"", 0, 0, "l1|l2|minute|daily|weekly|monthly|quarterly|mixed", "所用数据的频率 (l1 快照 / l2 逐笔 / minute 分钟 / mixed 混频); 空 = 不限"},
    {"genre", Arg::Str, "\"\"", 0, 0, "framework|research|handbook|review",
     "体裁: framework 体系搭建 / research 原创研究 / handbook 因子全集测试 / review 综述与文献解读; 空 = 不限"},
    {"n", Arg::Int, "10", 1, 40, nullptr, "列几篇"}};
constexpr ArgDef kFactorsArgs[] = {
    {"query", Arg::Str, "\"\"", 0, 0, nullptr, "关键词, 查因子名 / 公式 / 定义摘录; 空格隔开的几个须同时出现 (英文不分大小写); 空 = 不限"},
    {"family", Arg::Str, "\"\"", 0, 0, kFamilies, "风格桶 (microstructure = 订单流 / 订单簿 / 日内结构); 空 = 不限"},
    {"period", Arg::Str, "\"\"", 0, 0, kPeriods, "构造所用原始数据的周期含它 (second 逐笔 / 快照, intraday 分钟); 空 = 不限"},
    {"horizon", Arg::Str, "\"\"", 0, 0, kPeriods, "预测期; 空 = 不限"},
    {"approach", Arg::Str, "\"\"", 0, 0, nullptr, "出处研报的因子构造流派 (L1_factor 下的流派 code); 空 = 不限"},
    {"asset", Arg::Str, "\"\"", 0, 0, kAssets, "出处研报的资产; 空 = 不限"},
    {"sort", Arg::Str, "\"ic\"", 0, 0, "ic|icir|ls|new", "排序: ic = |RankIC| (没有则 |IC|) / icir = |ICIR| / ls = 多空年化 / new = 研报新到旧; 没有该统计的排后"},
    {"n", Arg::Int, "12", 1, 40, nullptr, "列几个"}};
constexpr ArgDef kReadArgs[] = {
    {"id", Arg::Str, nullptr, 0, 0, nullptr, "研报 id (检索结果里反引号中的那串; 能唯一对上的一段也行)"},
    {"part", Arg::Str, "\"card\"", 0, 0, "card|outline|text", "读哪部分: card 标注卡 / outline 目录 / text 原文"},
    {"from", Arg::Int, "0", 0, 999, nullptr, "text: 从第几节读起 (outline 的节号); card: 从第几个因子列起 (0 = 整卡; 因子多时卡末尾会给)"},
    {"skip", Arg::Int, "0", 0, 1000000, nullptr, "text: 跳过该节开头多少字 (接着读上次没读完的; 上次结果末尾会给)"}};
constexpr ArgDef kEvalArgs[] = {{"name", Arg::Str, nullptr, 0, 0, nullptr, "英文名: 仅 [A-Za-z0-9_], ≤ 64, 作文件名, 不能与已有的重复"},
                                {"name_cn", Arg::Str, nullptr, 0, 0, nullptr, "中文名: 1..10 个汉字, 不含字母数字标点"},
                                {"expr", Arg::Str, nullptr, 0, 0, nullptr, "模板 (可含自由参数)"},
                                {"note", Arg::Str, "\"\"", 0, 0, nullptr,
                                 "相对哪个版本改了什么、想验证什么; 由观察促成的写明是哪个调用看到了什么, 借鉴研报的写明研报 id 与借的是什么"}};
constexpr ArgDef kTopArgs[] = {{"top", Arg::Int, "20", 1, kListTop, nullptr, "列前几个"}};
constexpr ArgDef kNameArgs[] = {{"name", Arg::Str, nullptr, 0, 0, nullptr, "已评候选的英文名 (本作用域历次会话的都可以)"}};
constexpr ArgDef kIssueArgs[] = {{"text", Arg::Str, nullptr, 0, 0, nullptr, "疑问 / 建议"}};
constexpr ArgDef kAskArgs[] = {{"questions", Arg::Questions, nullptr, 1, 8, nullptr,
                                "问题数组, 每题 {\"prompt\": 题干, \"options\": [2..5 个字符串], \"multiple\": 可多选 (缺省 false), \"recommended\": [推荐的选项下标]}"}};

constexpr ToolDef kTools[] = {
    {Tool::ViewFeature, "view_feature", "看特征", Run::Look, kFeatureArgs,
     "看特征长什么样: 池内分布 (分位) / 日内轮廓 (各行截面中位数) / 方差来自哪 (资产间 · 日间 · 日内) / 持续性 (同一资产的自相关) / 与本持有期收益的截面 rIC; "
     "给多个时附两两截面相关. 造新特征前先看原料, 造完看它是否如你所想",
     "每个式子一节统计 (带火花线; 写错的式子在它那节给原因, 其余照算) + 多个时的相关矩阵"},
    {Tool::ViewLayers, "view_layers", "看分层", Run::Look, kLayersArgs,
     "看因子的 20 组分层: 每组每期超额 / mono / 多空 / rIC, 做多端与最低组各贡献多少; 切开看分层在哪些时段 / 月份成立、哪些失效",
     "全期 20 组 + 切分时每段一行 (行数 / mono / 多空 / rIC / 首中末组)"},
    {Tool::ViewTrades, "view_trades", "看交割单", Run::Look, kTradesArgs,
     "看因子一端真持有过的仓位 (与分层回测同一份持仓判据, 建不了仓的不算): 段收益的分布与集中度, 按开仓时段 / 标签 (涨跌停附近 · 当日大涨大跌 · 日内高低位 · 放量 · "
     "竞价段 · 尾盘) 的格占比提升 (本端真持有格对池内格) 与段收益, 再列若干段的明细 (代码与日期可接 view_stock_day). 汇总与两张表看收益从哪类情形来、亏在哪类情形; "
     "明细只用来挑票对着盘面看机制, 尾部个例 (worst / best) 多是个股事件或数据缺失, 不代表信号",
     "汇总 + 时段表 + 标签表 + 明细表"},
    {Tool::ViewHolders, "view_holders", "看常客", Run::Look, kHoldersArgs,
     "看哪些资产最常落在因子一端: 本端行数 / 占其在池行的比例 / 涉及天数 / 平均连续行数 / 本端持仓平均超额, 附集中度. 用来看因子是否只在少数票上起作用、在挑哪类票",
     "集中度一行 + 资产表"},
    {Tool::ViewResponse, "view_response", "看响应", Run::Look, kResponseArgs,
     "看因子一端开仓前后全池平均的价格路径 (事件研究, 与分层回测同一份持仓): 以真持有段的开仓行为零点, 开仓前若干时点到开仓的累计超额 (因子是不是过去价格的影子: 反转 / 动量) "
     "与开仓后到若干时点的累计超额 (收益何时到、多快衰减、持有期是否合适), 各时点的因子分位 (信号能持续多久), 附开仓行因子分位与此前收益的截面相关; 可按开仓时段切开",
     "说明两行 + 时点表 (段数 / 累计超额 ± 按日聚类标准误 / 为正占比 / 因子分位) + 与此前收益的相关一行 + 切分时的时段表"},
    {Tool::ViewStockDay, "view_stock_day", "看个股一天", Run::Look, kStockDayArgs,
     "看一只股票一天的逐行走势: 价 / 量 / 因子分位 / 特征的火花线, 加分段表 (竞价段 + 每段约 30 分钟: 涨跌 / 段内高低 / 量占比 / 因子分位 / 后续持有期超额 / 特征段末值). "
     "用来对照盘面现象与因子何时高、何时低",
     "当日概况 + 火花线 + 分段表"},
    {Tool::ViewStockDays, "view_stock_days", "看个股多日", Run::Look, kStockDaysArgs,
     "看一只股票连续多日 (至多 {stock_days} 日) 的日级概况: 每日一行 (涨跌 / 振幅 / 成交额 / 在池 / 因子日均与收盘分位 / 两端行数 / 收盘开仓的后续超额 / 特征收盘值), 附日级火花线",
     "火花线 + 逐日表"},
    {Tool::SearchLeaves, "search_leaves", "查特征叶", Run::Now, kLeavesArgs,
     "查特征叶的公式与说明 (系统提示的特征表只列叶名与中文名): 用一个叶之前先查清它怎么算、什么单位、在哪个时点可见、落盘值是不是原量",
     "命中数 + 叶行 (叶 / 中文名 / 公式 / 说明 / 落盘值); 零命中时给各词单独的命中数"},
    {Tool::SearchDocs, "search_docs", "查研报", Run::Docs, kDocsArgs,
     "查研报库 (券商金工研报的标注与全文): 先给命中研报的分面 —— 阶段 → 流派树 (篇数) / 机构 / 年份 / 价值 (我们对研报的判断: high 可直接复用 / medium 有参考 / low 有水分), "
     "再列研报. 不带条件 = 全库版图; 从上往下逐层加条件 (阶段 → 流派 → 关键词) 收窄到感兴趣的几篇, 再用 read_doc 读",
     "分面 + 研报行 (id / 价值 / 主阶段 / 流派 / 因子数 / 首条结论; 全文检索另给命中数与片段)"},
    {Tool::SearchFactors, "search_factors", "查研报因子", Run::Docs, kFactorsArgs,
     "查研报里提出过的因子: 先给命中因子的分面 (类别 / 出处流派 / 数据周期 × 预测期), 再列因子 (名 / 类别 / 方向 / 数据周期 → 预测期 / 原文统计 / LaTeX 公式). "
     "造新特征或换思路前先查别人怎么定义同类东西、背后讲的什么机制. 统计是原文在其自己股票池与预测期 (多为月度) 上的数, 只用来理解机制, 与本会话的回测不可比",
     "分面 + 因子行 (出处研报 id 可接 read_doc)"},
    {Tool::ReadDoc, "read_doc", "读研报", Run::Docs, kReadArgs,
     "读一篇研报: card 是标注卡 (各阶段的流派与设定 / 结论 / 库内前作与被引 / 因子与公式; 因子多读不完时末尾给接着读的 from), 先看它判断值不值得细读; 要细节看 outline (节号 · 标题 · 字数) 挑节, "
     "再用 text 从那节读原文 (图片已去, 表格与公式保留), 每次读到字数上限为止, 末尾给接着读的 from / skip",
     "标注卡 / 目录 / 原文一段"},
    {Tool::EvalFactor, "eval_factor", "回测", Run::Eval, kEvalArgs,
     "把一个模板落成候选文件并回测: Search (定方向 + 搜自由参数) → 各权重全量分层回测. 一条回复至多 {batch} 个, 都应是当前在做那个因子的变体; 与已有候选同式 (含只换截面根) 的不落",
     "结局: 未落文件的原因 / BROKEN / 无显著 (落选原因 + 代表点 + 代表点的 20 组与分时段 mono) / 各权重的统计 (+ 最佳权重的 20 组); 有自由参数的另附参数曲面 (每个参数按取值分段, 段内最好的点)"},
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
static_assert(std::size(kTools) == kToolCount);
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

// ---- 回复解析 ----
// 回复 → JSON 对象 (取首个 '{' 到末个 '}', 容忍外包代码块); 同一对象里键重复也不收 (解析器会静默丢掉一个)
bool object_of(std::string_view text, json &j, std::string &err) {
  const size_t a = text.find('{'), b = text.rfind('}');
  if (a == std::string_view::npos || b == std::string_view::npos || b < a) {
    err = "回复里没有 JSON 对象";
    return false;
  }
  // 在开的各层容器: 数组记在第几项, 对象记在哪个键与已见的键; 键重复时按它们拼出所在对象的路径
  struct Open {
    bool arr = false;
    int n = -1;
    std::string key;
    std::vector<std::string> keys;
  };
  std::vector<Open> st;
  std::string dup, where;
  const json::parser_callback_t seen = [&](int, json::parse_event_t ev, json &v) {
    using E = json::parse_event_t;
    if (ev == E::object_start || ev == E::array_start) {
      if (!st.empty() && st.back().arr)
        ++st.back().n;
      st.push_back({.arr = ev == E::array_start});
    } else if (ev == E::object_end || ev == E::array_end) {
      st.pop_back();
    } else if (ev == E::value) {
      if (!st.empty() && st.back().arr)
        ++st.back().n;
    } else if (ev == E::key) {
      Open &o = st.back();
      o.key = v.get<std::string>();
      if (dup.empty() && std::find(o.keys.begin(), o.keys.end(), o.key) != o.keys.end()) {
        dup = o.key;
        for (size_t i = 0; i + 1 < st.size(); ++i)
          where += st[i].arr ? "[" + std::to_string(st[i].n) + "]" : (i ? "." : "") + st[i].key;
      }
      o.keys.push_back(o.key);
    }
    return true;
  };
  j = json::parse(text.substr(a, b - a + 1), seen, false);
  if (j.is_discarded() || !j.is_object()) {
    err = "回复不是合法的 JSON 对象";
    return false;
  }
  if (!dup.empty()) {
    err = (where.empty() ? "顶层对象" : where) + " 里键 \"" + dup + "\" 出现了两次 (常见于把两个调用写进一个 {...}: 每个调用单独一个 {\"tool\": ..., \"args\": {...}}, 之间用逗号隔开)";
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
    if (s.empty() && a.def && std::string_view(a.def) == "\"\"")
      return true;
    if (a.choices) {
      const std::string all = std::string("|") + a.choices + "|";
      if (s.empty() || all.find("|" + s + "|") == std::string::npos)
        return fail(std::string("须是 ") + a.choices + " 之一");
    } else if (s.empty()) {
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

std::string ToolText(const char *s) {
  std::string t = s;
  for (const auto &[key, v] : {std::pair<std::string_view, int>{"{batch}", kBatch}, {"{stock_days}", kStockDays}})
    if (const size_t p = t.find(key); p != std::string::npos)
      t.replace(p, key.size(), std::to_string(v));
  return t;
}

std::string ArgType(const ArgDef &a) {
  switch (a.type) {
  case Arg::Str:
    return a.choices ? std::string("取 ") + a.choices : "字符串";
  case Arg::Int:
    return "整数 " + std::to_string(a.lo) + ".." + std::to_string(a.hi);
  case Arg::Strs:
    return "字符串数组, " + std::to_string(a.lo) + ".." + std::to_string(a.hi) + " 个";
  case Arg::Questions:
    return "数组, " + std::to_string(a.lo) + ".." + std::to_string(a.hi) + " 题";
  }
  assert(false && "未知参数类型");
  return {};
}

std::string ArgText(const ArgDef &a) { return "`" + std::string(a.key) + "` (" + ArgType(a) + (a.def ? ", 缺省 " + std::string(a.def) : std::string(", 必填")) + "): " + a.desc; }

std::string Call::str(const char *k) const { return args.at(k).get<std::string>(); }
int Call::num(const char *k) const { return args.at(k).get<int>(); }
std::vector<std::string> Call::strs(const char *k) const { return args.at(k).get<std::vector<std::string>>(); }

std::string Call::text(size_t cap) const {
  std::string s = std::string(def(tool).name) + "(";
  bool first = true;
  for (const ArgDef &a : def(tool).args) {
    const json &v = args.at(a.key);
    if (a.type == Arg::Questions) {
      s += (first ? "" : ", ") + std::to_string(v.size()) + " 个问题";
    } else {
      if (a.def && v == json::parse(a.def)) // 缺省值不写
        continue;
      s += (first ? "" : ", ") + std::string(a.key) + "=" + (cap ? agent::text::head(brief(v), cap) : brief(v));
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
    err = "calls 须有 1.." + std::to_string(kMaxCalls) + " 个调用 (收到 " + std::to_string(cs.size()) + " 个; 多的留到下一条)";
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
      err = p + ": 一条回复至多 " + std::to_string(kBatch) + " 个 eval_factor (这是第 " + std::to_string(evals) + " 个)";
      return false;
    }
    const json args = c.contains("args") ? c["args"] : json::object();
    if (!args.is_object()) {
      err = p + ".args 须是 JSON 对象";
      return false;
    }
    for (const auto &[k, v] : args.items())
      if (std::none_of(d->args.begin(), d->args.end(), [&](const ArgDef &a) { return k == a.key; })) {
        std::string keys;
        for (const ArgDef &a : d->args)
          keys += (keys.empty() ? "" : " / ") + std::string(a.key);
        err = p + ".args." + k + ": " + name + " 没有这个参数 (它的参数: " + (keys.empty() ? "无" : keys) + ")";
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
    s += "## " + std::string(d.name) + " — " + d.cn + "\n" + ToolText(d.desc) + "\n\n";
    if (d.args.empty()) {
      s += "参数: 无\n";
    } else {
      s += "参数:\n";
      for (const ArgDef &a : d.args)
        s += "- " + ArgText(a) + "\n";
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
