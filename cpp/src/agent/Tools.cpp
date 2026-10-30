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
constexpr ArgDef kLayersArgs[] = {
    {"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
    {"split", Arg::Str, "\"none\"", 0, 0, "none|tod|month", "切开看: none 不切 / tod 按日内时段 / month 按月"},
    {"cond", Arg::Str, "\"\"", 0, 0, nullptr,
     "可选: 条件特征式 (写法同 view_feature 的一个式子); 给了就每行按它把池子截面三等分 (低 / 中 / 高), 各段内把因子重新分 20 组看分层 —— 验证 \"在某类盘面 / 某类票上更强\" 的机制"}};
constexpr ArgDef kTradesArgs[] = {
    {"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
    {"side", Arg::Str, "\"top\"", 0, 0, "top|bottom", kSideDesc},
    {"sort", Arg::Str, "\"random\"", 0, 0, "random|worst|best", "明细挑哪些段: random 随机 (固定种子, 看典型持仓) / worst 超额最低 / best 最高 (两端是尾部个例)"},
    {"n", Arg::Int, "12", 1, 30, nullptr, "明细段数"},
    {"merge", Arg::Str, "\"run\"", 0, 0, "none|run|day", "合并成段: none 每行一笔 / run 同一资产同日相邻行连成一段 / day 同一资产同日并成一段"},
    {"from", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 只看此日 (YYYY-MM-DD) 及之后的行"},
    {"to", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 只看此日及之前的行"},
    {"tod", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 只看行末时刻在 HH:MM-HH:MM 内 (含两端) 的行 (日层不可用); 持仓格与对照的池内格同窗, 跨窗的段被窗口截断"}};
constexpr ArgDef kSnapshotArgs[] = {
    {"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
    {"date", Arg::Str, nullptr, 0, 0, nullptr, "交易日 YYYY-MM-DD (须在评估期内)"},
    {"time", Arg::Str, "\"\"", 0, 0, nullptr, "日内层: 哪一行, 写该行的行末时刻 HH:MM (如 10:30); 日层不填"},
    {"n", Arg::Int, "10", 1, 20, nullptr, "两端各列几只"},
    {"exprs", Arg::Strs, "[]", 0, 3, nullptr, "可选: 一并列出的特征式 (写法同 view_feature)"}};
constexpr ArgDef kHoldersArgs[] = {{"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
                                   {"side", Arg::Str, "\"top\"", 0, 0, "top|bottom", kSideDesc},
                                   {"n", Arg::Int, "15", 1, 30, nullptr, "列几只"}};
constexpr ArgDef kResponseArgs[] = {{"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
                                    {"side", Arg::Str, "\"top\"", 0, 0, "top|bottom", kSideDesc},
                                    {"split", Arg::Str, "\"none\"", 0, 0, "none|tod", "切开看: none 不切 / tod 按开仓时段 (日层无时段, 同 none)"}};
constexpr ArgDef kStockDayArgs[] = {{"code", Arg::Str, nullptr, 0, 0, nullptr, "6 位代码或 代码.交易所 (如 000001 / 000001.SZ)"},
                                    {"date", Arg::Str, nullptr, 0, 0, nullptr, "交易日 YYYY-MM-DD (须在评估期内)"},
                                    {"factor", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 叠加看的因子, 写法同其他观察工具的 factor"},
                                    {"exprs", Arg::Strs, "[]", 0, 4, nullptr, "可选: 叠加看的特征式 (写法同 view_feature)"},
                                    {"rows", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 逐行细看的时段 HH:MM-HH:MM (按行末时刻, 含两端, 至多 {stock_rows} 行); 给了另附逐行表"}};
constexpr ArgDef kExplainArgs[] = {{"factor", Arg::Str, nullptr, 0, 0, nullptr, kFactorDesc},
                                   {"exprs", Arg::Strs, "[]", 0, 4, nullptr, "可选: 另加的基准特征式 (写法同 view_feature; 例: 主动买卖失衡、价差、市值), 与内置基准一起剥离"}};
constexpr ArgDef kStockDaysArgs[] = {{"code", Arg::Str, nullptr, 0, 0, nullptr, "6 位代码或 代码.交易所"},
                                     {"from", Arg::Str, "\"\"", 0, 0, nullptr, "首日 YYYY-MM-DD; 空 = 末日往前共 20 个交易日"},
                                     {"to", Arg::Str, "\"\"", 0, 0, nullptr, "末日 YYYY-MM-DD; 空 = 评估期末日"},
                                     {"factor", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 叠加看的因子"},
                                     {"exprs", Arg::Strs, "[]", 0, 4, nullptr, "可选: 叠加看的特征式 (各日取收盘行的值)"}};
constexpr ArgDef kBookArgs[] = {
    {"code", Arg::Str, nullptr, 0, 0, nullptr, "6 位代码或 代码.交易所"},
    {"date", Arg::Str, nullptr, 0, 0, nullptr, "交易日 YYYY-MM-DD (有逐笔数据的日子, 不限评估期)"},
    {"time", Arg::Str, nullptr, 0, 0, nullptr,
     "重放窗口 HH:MM-HH:MM 或 HH:MM:SS-HH:MM:SS (左闭右开), 在 09:15–11:30 / 13:00–15:00 内 (可跨午休), 至多 {book_mins} 分钟交易时间; 含 09:15–09:25 / 14:57–15:00 即看开盘 / 收盘集合竞价"},
    {"step", Arg::Int, "0", 0, 600, nullptr, "逐时表每行几秒; 0 = 自动 (窗口分成不超过 {book_rows} 行的整齐秒数)"},
    {"at", Arg::Str, "\"\"", 0, 0, nullptr, "可选: 全簿剖面的时刻 HH:MM:SS (取该秒末的簿, 须在窗口内); 空 = 窗口最后一秒"},
    {"big", Arg::Int, "0", 0, 100000, nullptr, "大单门槛 (万元, 按委托额); 0 = 当日全部新挂单额的 99 分位"},
    {"n", Arg::Int, "20", 1, 40, nullptr, "大单事件列几条 (按额取最大的, 再按时序列)"}};
constexpr ArgDef kLeavesArgs[] = {{"query", Arg::Str, nullptr, 0, 0, nullptr, "关键词, 查叶名 / 中文名 / 公式 / 说明; 空格隔开的几个须同时出现 (英文不分大小写)"},
                                  {"n", Arg::Int, "30", 1, 60, nullptr, "列几组 (同组 = 只差计量 / 阈值的叶)"}};
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
constexpr ArgDef kHypArgs[] = {
    {"title", Arg::Str, nullptr, 0, 0, nullptr, "一句话: 什么机制 (例: 秒级吃光新挂卖单的买方在不推价地吸筹)"},
    {"actor", Arg::Str, nullptr, 0, 0, nullptr, "因子高 = 谁在做什么: 主动吃单方还是被动挂单方、哪一侧、大单还是小单、成交还是挂撤, 在什么盘面下"},
    {"why", Arg::Str, nullptr, 0, 0, nullptr, "为什么这预示本持有期的收益 (信息 / 流动性冲击 / 行为偏差 …), 收益何时兑现; 借鉴研报的写研报 id 与借的是什么"},
    {"expect", Arg::Str, nullptr, 0, 0, nullptr, "若假设成立, 哪个观察 (工具 + 参数) 会看到什么: 一个方向, 写出量级 (例: view_layers cond=价差 的高段 mono 比低段高 0.1 以上)"},
    {"refute", Arg::Str, nullptr, 0, 0, nullptr, "看到什么就算落空 (同一观察的反面, 或另一个观察); 不能写成 \"若…则升, 若…则降\" 两头都说"}};
constexpr ArgDef kJudgeArgs[] = {
    {"hyp", Arg::Str, nullptr, 0, 0, nullptr, "假设编号 (假设板上的, 如 H3)"},
    {"verdict", Arg::Str, nullptr, 0, 0, "supported|refuted|unclear", "supported 支持 (证据里须有观察调用) / refuted 落空 / unclear 不清 (证据不够; next 写还差什么)"},
    {"evidence", Arg::Strs, nullptr, 1, kEvidence, nullptr, "证据: 调用编号 (结果标题里的 R<轮>#<序>), 须是该假设提出之后做成的调用; 再判时至少一个在上次判定之后"},
    {"result", Arg::Str, nullptr, 0, 0, nullptr, "测出来是什么: 对照 expect / refute 写看到的数 (哪个调用、哪一行); 判变体时写各变体相对 base 的变化"},
    {"why", Arg::Str, nullptr, 0, 0, nullptr, "为什么是这个结果: 机制上怎么解释; 落空的说清错在假设的哪一环 (谁 / 什么行为 / 什么盘面 / 何时兑现)"},
    {"insight", Arg::Str, nullptr, 0, 0, nullptr, "得到了什么认识 (对这个因子、这类叶、这个市场); 值得跨会话留下的另用 note_insight 记"},
    {"better_feature", Arg::Str, nullptr, 0, 0, nullptr,
     "这个现象有没有更直接捕捉它的特征: 现有叶里哪个更贴切 (查过的叶名, 为什么), 或缺什么样的量 (缺的用 request_feature 申请)"},
    {"next", Arg::Str, nullptr, 0, 0, nullptr, "下一步: 用哪个工具 (参数) 查什么, 想确认或排除什么; 或据此挂什么变体、提什么新假设"}};
constexpr ArgDef kEvalArgs[] = {
    {"name", Arg::Str, nullptr, 0, 0, nullptr, "英文名: 仅 [A-Za-z0-9_], ≤ 64, 作文件名, 不能与已有的重复"},
    {"name_cn", Arg::Str, nullptr, 0, 0, nullptr, "中文名: 1..10 个汉字, 不含字母数字标点"},
    {"expr", Arg::Str, nullptr, 0, 0, nullptr, "模板 (可含自由参数)"},
    {"hyp", Arg::Str, nullptr, 0, 0, nullptr, "挂在哪个假设上 (假设板上判为支持的, 如 H3)"},
    {"base", Arg::Str, "\"\"", 0, 0, nullptr,
     "相对哪个已试候选改 (英文名, 本作用域历次会话的都可以); 空 = 新起. 系统比对两式归类改动: 只改参数 / 改了多处 / 同式直接退回; "
     "换平滑 / 换压缩 / 换口径 只在 base 达入库门槛时可用, 每个假设至多 {hyp_knobs} 个"},
    {"change", Arg::Str, nullptr, 0, 0, nullptr, "这一处改动想修哪个现象 (哪个调用看到了什么), 改的是哪一处"},
    {"expect", Arg::Str, nullptr, 0, 0, nullptr, "预期: 相对 base 会看到什么变化 (mono / 多空 / rIC 哪个往哪个方向, 或哪个观察里的现象消失); 一个方向"},
    {"parts", Arg::Parts, nullptr, 1, kParts, nullptr,
     "功能块 [{\"expr\": 模板里逐字抄的一段子式, \"means\": 它衡量什么 (≤ {part_chars} 字的短句, 不计空格标点)}, …]; 须覆盖根的输入 (整个因子衡量什么) 与每个多元算子 "
     "(TsAdd / TsSub / TsMul / TsImb / TsMask …) 的每个非叶元 (各成分各衡量什么; 叶的组合如 TsAdd(叶, 叶) 作了别的多元算子的一元, 也要一块)"}};
constexpr ArgDef kTopArgs[] = {{"top", Arg::Int, "20", 1, kListTop, nullptr, "列几个"},
                               {"skip", Arg::Int, "0", 0, 9999, nullptr, "跳过排在前面的几个 (接着看上次没列到的; 结果末尾会给)"}};
constexpr ArgDef kNameArgs[] = {{"name", Arg::Str, nullptr, 0, 0, nullptr, "已评候选的英文名 (本作用域历次会话的都可以)"}};
// 全局记录 (agent/Notes)
constexpr const char *kRefDesc = "可选: 同类已有一条的编号 (list_notes 里的, 如 F3): 附议或补充它, 只写新的部分; 空 = 新提";
constexpr ArgDef kNotesArgs[] = {
    {"kind", Arg::Str, "\"\"", 0, 0, "feature|tool|insight|issue",
     "哪类: feature 特征申请 / tool 工具申请 / insight 发现 / issue 建议与 bug; 空 = 各类的条数与最近几条的标题"},
    {"scope", Arg::Str, "\"all\"", 0, 0, "all|here", "all = 全部 / here = 只看本作用域 (池 / 层 / 持有期) 提的"},
    {"query", Arg::Str, "\"\"", 0, 0, nullptr, "关键词, 查标题与正文; 空格隔开的几个须同时出现 (英文不分大小写); 空 = 不限"},
    {"n", Arg::Int, "15", 1, kNotesTop, nullptr, "列几条 (新到旧)"}};
constexpr ArgDef kFeatureReqArgs[] = {
    {"title", Arg::Str, nullptr, 0, 0, nullptr, "一句话: 要的是什么量 (例: 撤单前挂单存续时长的分布)"},
    {"define", Arg::Str, nullptr, 0, 0, nullptr, "怎么算: 原料 (逐笔成交 / 委托 / 撤单 / 快照)、谁的哪一侧、窗口与归一、在哪个时点可见"},
    {"mechanism", Arg::Str, nullptr, 0, 0, nullptr, "它记的是谁的什么行为, 为什么可能预示后续收益; 你打算拿它验证哪个假设"},
    {"gap", Arg::Str, nullptr, 0, 0, nullptr, "现有的叶为什么不够: 读过哪些族卡、search_leaves 查过哪些叶, 差在哪 (口径 / 维度 / 时点 / 根本没有)"},
    {"ref", Arg::Str, "\"\"", 0, 0, nullptr, kRefDesc}};
constexpr ArgDef kToolReqArgs[] = {
    {"title", Arg::Str, nullptr, 0, 0, nullptr, "一句话: 要个什么工具"},
    {"want", Arg::Str, nullptr, 0, 0, nullptr, "想给什么参数、看到什么 (哪几张表 / 哪些统计 / 怎么切)"},
    {"purpose", Arg::Str, nullptr, 0, 0, nullptr, "拿它验证什么假设、决定什么改动"},
    {"gap", Arg::Str, nullptr, 0, 0, nullptr, "现有工具哪里看不到: 试过哪些调用, 差在哪"},
    {"ref", Arg::Str, "\"\"", 0, 0, nullptr, kRefDesc}};
constexpr ArgDef kInsightArgs[] = {
    {"title", Arg::Str, nullptr, 0, 0, nullptr, "一句话结论 (被证实或被证伪的都算; 例: 尾盘 14:30 后主动买大单的信号在次日开盘前兑现完)"},
    {"evidence", Arg::Str, nullptr, 0, 0, nullptr, "证据: 哪个调用 (工具 + 因子 / 参数) 看到了哪些数; 有对照的写对照"},
    {"reach", Arg::Str, nullptr, 0, 0, nullptr, "适用范围: 在什么条件下成立 (本作用域 / 某类票 / 某时段 / 某类盘面), 哪里没验证过"},
    {"use", Arg::Str, nullptr, 0, 0, nullptr, "对挖因子意味着什么: 该做什么、别做什么"},
    {"ref", Arg::Str, "\"\"", 0, 0, nullptr, kRefDesc}};
constexpr ArgDef kIssueArgs[] = {
    {"type", Arg::Str, nullptr, 0, 0, "bug|idea|question", "bug 工具结果不对 / idea 对规则或系统的改进建议 / question 想问研究员的疑问"},
    {"title", Arg::Str, nullptr, 0, 0, nullptr, "一句话"},
    {"detail", Arg::Str, nullptr, 0, 0, nullptr, "bug: 工具 + 参数 + 看到了什么 + 预期是什么; idea: 现在怎样、改成怎样、为什么; question: 问什么、卡在哪"},
    {"ref", Arg::Str, "\"\"", 0, 0, nullptr, kRefDesc}};
constexpr ArgDef kAskArgs[] = {{"questions", Arg::Questions, nullptr, 1, 8, nullptr,
                                "问题数组, 每题 {\"prompt\": 题干, \"options\": [2..5 个字符串], \"multiple\": 可多选 (缺省 false), \"recommended\": [推荐的选项下标]}"}};

constexpr ToolDef kTools[] = {
    {Tool::ViewFeature, "view_feature", "看特征", Run::Look, Cat::Look, Fold::Title, kFeatureArgs,
     "看特征长什么样: 池内分布 (分位) / 日内轮廓 (各行截面中位数) / 方差来自哪 (资产间 · 日间 · 日内) / 持续性 (同一资产的自相关) / 与本持有期收益的截面 rIC; "
     "给多个时附两两截面相关. 造新特征前先看原料, 造完看它是否如你所想",
     "每个式子一节统计 (带火花线; 写错的式子在它那节给原因, 其余照算) + 多个时的相关矩阵"},
    {Tool::ViewLayers, "view_layers", "看分层", Run::Look, Cat::Look, Fold::Title, kLayersArgs,
     "看因子的 20 组分层: 每组每期超额 / mono / 多空 / rIC, 做多端与最低组各贡献多少; 两端组内再截最极端的 50% / 20% / 5% 看尾部 (越极端越强 = 信号集中在极值, "
     "可加强截面根的区分度; 尾部反而回落 = 极值多是事件 / 数据噪声); 切开看分层在哪些时段 / 月份成立、哪些失效; 给 cond 再按条件三等分看各段的分层",
     "全期 20 组 + 尾部表 (各截取宽度两端的每期超额 / 只数 / 多空) + 切分时每段一行 (行数 / mono / 多空 / rIC / 首中末组) + 给 cond 时条件三段各一行; "
     "切分或给 cond 时全期部分照给, 不必另调一次不切的"},
    {Tool::ViewTrades, "view_trades", "看交割单", Run::Look, Cat::Look, Fold::Title, kTradesArgs,
     "看因子一端真持有过的仓位 (与分层回测同一份持仓判据, 建不了仓的不算): 段收益的分布与集中度, 按开仓时段 / 标签 (涨跌停附近 · 当日大涨大跌 · 日内高低位 · 放量 · "
     "竞价段 · 尾盘) 的格占比提升 (本端真持有格对池内格) 与段收益, 再列若干段的明细 (代码与日期可接 view_stock_day). 汇总与两张表看收益从哪类情形来、亏在哪类情形; "
     "明细只用来挑票对着盘面看机制, 尾部个例 (worst / best) 多是个股事件或数据缺失, 不代表信号",
     "汇总 + 时段表 + 标签表 + 明细表; 给 from / to / tod 只看该日段 / 时段 (看收益集中在哪段行情、开盘还是尾盘)"},
    {Tool::ViewHolders, "view_holders", "看常客", Run::Look, Cat::Look, Fold::Title, kHoldersArgs,
     "看哪些资产最常落在因子一端: 本端行数 / 占其在池行的比例 / 涉及天数 / 平均连续行数 / 本端持仓平均超额, 附集中度. 用来看因子是否只在少数票上起作用、在挑哪类票",
     "集中度一行 + 资产表"},
    {Tool::ViewResponse, "view_response", "看响应", Run::Look, Cat::Look, Fold::Title, kResponseArgs,
     "看因子一端开仓前后全池平均的价格路径 (事件研究, 与分层回测同一份持仓): 以真持有段的开仓行为零点, 开仓前若干时点到开仓的累计超额 (因子是不是过去价格的影子: 反转 / 动量) "
     "与开仓后到若干时点的累计超额 (收益何时到、多快衰减、持有期是否合适), 各时点的因子分位 (信号能持续多久), 附开仓行因子分位与此前收益的截面相关; 可按开仓时段切开",
     "说明两行 + 时点表 (段数 / 累计超额 ± 按日聚类标准误 / 为正占比 / 因子分位) + 与此前收益的相关一行 + 切分时的时段表"},
    {Tool::ViewExplain, "view_explain", "看成分", Run::Look, Cat::Look, Fold::Title, kExplainArgs,
     "看因子是不是已知量的影子: 内置基准 (日内层: 近 5 / 30 行收益、当日至今收益、前 1 / 5 日收益、当日至今成交额、当日至今振幅、日内位置、价格; 日层: 当日 / 5 日 / 20 日收益、成交额、振幅、价格) "
     "加你给的特征式, 每个给与因子的截面秩相关、它自身的 rIC、只剥掉它之后因子剩下的 rIC; 再把全部基准一起剥掉 (每行截面回归取残差), 给剩余部分与原因子的 rIC / mono / 多空 对照. "
     "新方向起步、每次显著改进后都看一次: 剥掉之后还剩下的才是新信息, 被哪个基准吃掉就说明它是谁的影子",
     "基准表 (与因子的相关 / 基准自身 rIC / 只剥它后的因子 rIC) + 全剥离对照 (原因子与剩余部分的 rIC · mono · 多空 · 20 组火花线)"},
    {Tool::ViewSnapshot, "view_snapshot", "看截面快照", Run::Look, Cat::Look, Fold::Title, kSnapshotArgs,
     "看某一天某一行的截面: 因子分位最高 / 最低的各 n 只 (分位 · 组 · 是否真持有 · 当日涨跌 · 本行涨跌 · 日内位置 · 当日至此成交额 · 后续超额 · 盘面标签 · 特征值), 中间夹一行池中位作对照, "
     "附各列与因子分位的截面秩相关. 用来看同一时刻两端各是什么样的票 (同涨同跌的一批? 刚异动的? 小票?), 与 view_trades 的明细互补: 那里是一只票的一段, 这里是一个时刻的全池",
     "本行概况 + 截面秩相关一行 + 两端表 (中夹池中位)"},
    {Tool::ViewStockDay, "view_stock_day", "看个股一天", Run::Look, Cat::Look, Fold::Title, kStockDayArgs,
     "看一只股票一天的逐行走势: 价 / 量 / 因子分位 / 特征的火花线, 加分段表 (竞价段 + 每段约 30 分钟: 涨跌 / 段内高低 / 量占比 / 因子分位 / 后续持有期超额 / 特征段末值). "
     "给 rows 另附该时段逐行表 (价 / 涨跌 / 高低 / 量额 / 因子分位与组 / 后续超额 / 特征值), 对着分钟看机制里说的那个行为是否真的发生. 用来对照盘面现象与因子何时高、何时低",
     "当日概况 + 火花线 + 分段表 (+ 逐行表)"},
    {Tool::ViewStockDays, "view_stock_days", "看个股多日", Run::Look, Cat::Look, Fold::Title, kStockDaysArgs,
     "看一只股票连续多日 (至多 {stock_days} 日) 的日级概况: 每日一行 (涨跌 / 振幅 / 成交额 / 在池 / 因子日均与收盘分位 / 两端行数 / 收盘开仓的后续超额 / 特征收盘值), 附日级火花线",
     "火花线 + 逐日表"},
    {Tool::ViewBook, "view_book", "看盘口重放", Run::Book, Cat::Look, Fold::Title, kBookArgs,
     "逐笔重放一只股票一天里的一段 (至多 {book_mins} 分钟; 不经特征库, 任何有逐笔数据的日子都能看): 逐时表 (每行 step 秒: 末买一卖一 / 价差 / 末价 / 主动买卖额 / 新挂与撤单额 (分买卖侧) / 前五档挂单额, "
     "竞价段另有预撮合参考价与未匹配额), 大单事件 (≥ 门槛的新挂 / 撤单 / 被吃完, 带距盘口几档与挂了多久; 一个主动单连吃多个价位的扫单), 某一秒的全簿剖面 (盘口两侧各若干价位的挂单额). "
     "用来亲眼看特征叶背后的行为: 大单是挂在盘口还是远处、挂多久撤、被谁吃; 撤单是不是集中在价格变动之前; 主动扫单之后盘口怎么补; 竞价里谁在挂撤. "
     "挑 view_trades / view_snapshot 里的票与时刻来看",
     "当日与窗口概况 + 逐时表 + 大单事件 (计数 + 明细) + 全簿剖面"},
    {Tool::SearchLeaves, "search_leaves", "查特征叶", Run::Now, Cat::Query, Fold::Title, kLeavesArgs,
     "查特征叶的公式与说明 (系统提示的特征表只列族卡、叶名与中文名): 用一个叶之前先查清它怎么算、什么单位、在哪个时点可见、落盘值是不是原量. "
     "只差计量 (额 / 量 / 笔) 或阈值 / 窗口的同组叶合成一行, 公式与说明按组内首个写. 这里列出过的组, 组内的叶才能写进 eval_factor 的式子 (本会话内有效; 观察工具不限)",
     "命中的叶数与组数 + 组行 (叶 (同组写成花括号式) / 中文名 / 公式 / 说明 / 落盘值); 零命中时给各词单独的命中数"},
    {Tool::SearchDocs, "search_docs", "查研报", Run::Docs, Cat::Query, Fold::Title, kDocsArgs,
     "查研报库 (券商金工研报的标注与全文): 先给命中研报的分面 —— 阶段 → 流派树 (篇数) / 机构 / 年份 / 价值 (我们对研报的判断: high 可直接复用 / medium 有参考 / low 有水分), "
     "再列研报. 不带条件 = 全库版图; 从上往下逐层加条件 (阶段 → 流派 → 关键词) 收窄到感兴趣的几篇, 再用 read_doc 读",
     "分面 + 研报行 (id / 价值 / 主阶段 / 流派 / 因子数 / 首条结论; 全文检索另给命中数与片段)"},
    {Tool::SearchFactors, "search_factors", "查研报因子", Run::Docs, Cat::Query, Fold::Title, kFactorsArgs,
     "查研报里提出过的因子: 先给命中因子的分面 (类别 / 出处流派 / 数据周期 × 预测期), 再列因子 (名 / 类别 / 方向 / 数据周期 → 预测期 / 原文统计 / LaTeX 公式). "
     "造新特征或换思路前先查别人怎么定义同类东西、背后讲的什么机制. 统计是原文在其自己股票池与预测期 (多为月度) 上的数, 只用来理解机制, 与本会话的回测不可比",
     "分面 + 因子行 (出处研报 id 可接 read_doc)"},
    {Tool::ReadDoc, "read_doc", "读研报", Run::Docs, Cat::Query, Fold::Title, kReadArgs,
     "读一篇研报: card 是标注卡 (各阶段的流派与设定 / 结论 / 库内前作与被引 / 因子与公式; 因子多读不完时末尾给接着读的 from), 先看它判断值不值得细读; 要细节看 outline (节号 · 标题 · 字数) 挑节, "
     "再用 text 从那节读原文 (图片已去, 表格与公式保留), 每次读到字数上限为止, 末尾给接着读的 from / skip",
     "标注卡 / 目录 / 原文一段"},
    {Tool::Hypothesize, "hypothesize", "提假设", Run::Now, Cat::Hyp, Fold::Keep, kHypArgs,
     "写下一个机制假设与它可证伪的预测, 记上假设板 (编号 H1, H2 …, 待检验). 之后用观察检验它, 再 judge; 只有判为支持的假设才能挂 eval_factor 的变体 (每个至多 {hyp_evals} 个). "
     "观察到新现象、上一个假设落空或变体用完、想换方向时提新的",
     "编号"},
    {Tool::Judge, "judge", "判假设", Run::Now, Cat::Hyp, Fold::Keep, kJudgeArgs,
     "对照假设的 expect / refute 给它下判定, 证据写调用编号. 两个时点: 检验它的观察做完后 (判为支持才能挂变体); 它的变体回测完的下一条回复 (系统强制: 待判的不判, 整条退回). "
     "判支持须引用观察: 同一条回复里把观察排在 judge 前面, 就能引用它们. "
     "每次都写清测出来是什么、为什么、得到什么认识、有没有更好的特征、下一步用什么工具查什么",
     "判定后的假设板一行; 证据不成 (编号不存在 / 在提出之前 / 没做成 / 判支持却没有观察 / 再判没有新证据) 写原因, 不改判定"},
    {Tool::EvalFactor, "eval_factor", "回测", Run::Eval, Cat::Trial, Fold::Brief, kEvalArgs,
     "把一个模板落成候选文件并回测: Search (定方向 + 搜自由参数) → 各权重全量分层回测. 一条回复至多 {batch} 个, 都应是当前在做那个因子的变体; "
     "须挂在判为支持的假设上 (每个至多 {hyp_evals} 个, 有变体回测过还没判的先 judge), 写明相对哪个 base 改了哪一处、功能块各衡量什么; 上一条回复有落了文件的回测时本条不能回测. "
     "与已有候选同式 (含只换截面根) 的不落, 用了本会话没经 search_leaves 查过的叶的不落; 同一条回复里先 search_leaves 再回测用它列出的叶也行",
     "改动归类 (相对 base) + 结局: 未落文件的原因 / BROKEN / 无显著 (落选原因 + 代表点 + 代表点的 20 组与分时段 mono) / 各权重的统计 (只写自由参数的取值) + 是否达入库门槛 "
     "(+ 最佳权重的 20 组); 有自由参数的另附参数曲面 (每个参数按取值分段, 段内最好的点)"},
    {Tool::ListTried, "list_tried", "列已试", Run::Now, Cat::Query, Fold::Title, kTopArgs, "列本作用域已评过的候选 (历次会话累积, 只列同一评估期的), 按 score 降序; 可作起点接着做",
     "每个候选: 模板 + 结局 (权重只写自由参数的取值) + 是否达入库门槛; 没列完时末尾给总数与接着看的 skip"},
    {Tool::ListLibrary, "list_library", "列正式库", Run::Now, Cat::Query, Fold::Title, kTopArgs,
     "列正式因子库里同一评估期且算子不超上限、在本持有期有统计的因子, 按 score 降序; 可作起点接着做",
     "每个因子: 模板 + 思路 + 最佳权重; 本持有期无统计的只报个数; 没列完时末尾给总数与接着看的 skip"},
    {Tool::ListNotes, "list_notes", "查全局记录", Run::Now, Cat::Query, Fold::Title, kNotesArgs,
     "查历次会话 (各作用域) 留下的全局记录: 特征申请 / 工具申请 / 发现 / 建议与 bug. 起步时看前人的发现 (别重走证伪过的路); 提交记录前先查同类, 已有的用 ref 附议或补充, 不要重提",
     "不给 kind: 各类条数 + 每类最近几条的标题; 给 kind: 逐条全文 (编号 / 时间 / 作用域 / 附议关系 / 各字段)"},
    {Tool::SubmitFactor, "submit_factor", "提交入库", Run::Now, Cat::Trial, Fold::Keep, kNameArgs, "把一个已评候选提交入库 (至少一个权重过入库门槛才入); 一般只在最后交最好的版本",
     "入库 / 未入库的原因"},
    {Tool::RequestFeature, "request_feature", "申请特征", Run::Now, Cat::Note, Fold::Keep, kFeatureReqArgs,
     "申请一个新特征叶: 要表达的行为在现有叶里做不出来 (读过族卡、search_leaves 查清之后). 全局记录, 研究员会看, 不阻塞; 本会话照常用现有的叶接着做",
     "编号"},
    {Tool::RequestTool, "request_tool", "申请工具", Run::Now, Cat::Note, Fold::Keep, kToolReqArgs,
     "申请一个新工具或给现有工具加功能: 验证假设要看的东西现有工具看不到. 全局记录, 研究员会看, 不阻塞", "编号"},
    {Tool::NoteInsight, "note_insight", "记发现", Run::Now, Cat::Note, Fold::Keep, kInsightArgs,
     "记下一条被观察 / 回测证实或证伪的规律 (市场微观结构、某类叶或算子的脾性、哪条路走不通), 对别的因子与后续会话也有用的才记. 全局记录, 后续会话的 agent 经 list_notes 看得到",
     "编号"},
    {Tool::ReportIssue, "report_issue", "报问题", Run::Now, Cat::Note, Fold::Keep, kIssueArgs,
     "报工具的 bug、对规则或系统的建议、想问研究员的疑问. 全局记录, 研究员会看, 不阻塞", "编号"},
    {Tool::Finish, "finish", "结束", Run::Now, Cat::Misc, Fold::Keep, {}, "宣布挖完: 须是本条回复的最后一个调用, 前面的调用照常执行后会话结束", "—"},
    {Tool::AskHuman, "ask_human", "问研究员", Run::Human, Cat::Misc, Fold::Keep, kAskArgs,
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
bool object_of(std::string_view text, json &j, std::string &error) {
  const size_t a = text.find('{'), b = text.rfind('}');
  if (a == std::string_view::npos || b == std::string_view::npos || b < a) {
    error = "回复里没有 JSON 对象";
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
    error = "回复不是合法的 JSON 对象";
    return false;
  }
  if (!dup.empty()) {
    error = (where.empty() ? "顶层对象" : where) + " 里键 \"" + dup + "\" 出现了两次 (常见于把两个调用写进一个 {...}: 每个调用单独一个 {\"tool\": ..., \"args\": {...}}, 之间用逗号隔开)";
    return false;
  }
  return true;
}

std::string at(const std::string &k, size_t i) { return k + "[" + std::to_string(i) + "]"; }

// 一个参数值按参数表查; 不合 → false + error
bool check(const ArgDef &a, const json &v, const std::string &path, std::string &error) {
  const auto fail = [&](std::string why) {
    error = path + " " + std::move(why);
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
  case Arg::Parts:
    if (!v.is_array() || v.size() < static_cast<size_t>(a.lo) || v.size() > static_cast<size_t>(a.hi))
      return fail("须是 " + std::to_string(a.lo) + ".." + std::to_string(a.hi) + " 个功能块的数组");
    for (size_t i = 0; i < v.size(); ++i) {
      const json &x = v[i];
      std::string why;
      if (!x.is_object() || x.size() != 2 || !x.contains("expr") || !x.contains("means") || !x["expr"].is_string() || !x["means"].is_string())
        why = " 须是 {\"expr\": 子式, \"means\": 短句}";
      else if (x["expr"].get<std::string>().empty())
        why = ".expr 不能为空";
      else if (const size_t n = text::solid(x["means"].get<std::string>()); n == 0 || n > static_cast<size_t>(kPartChars))
        why = ".means 须是 1.." + std::to_string(kPartChars) + " 字的短句 (不计空格标点; 收到 " + std::to_string(n) + " 字)";
      if (!why.empty()) {
        error = at(path, i) + why;
        return false;
      }
    }
    return true;
  }
  return true;
}

// ask_human 的问题数组 (形已由 check 查过) → Questions; 不合 → false + error
bool questions_of(const json &v, const std::string &path, Questions &out, std::string &error) {
  out.clear();
  for (size_t i = 0; i < v.size(); ++i) {
    const std::string p = at(path, i);
    const json &q = v[i];
    if (!q.is_object()) {
      error = p + " 须是 JSON 对象";
      return false;
    }
    Question x;
    if (!q.contains("prompt") || !q["prompt"].is_string() || q["prompt"].get<std::string>().empty()) {
      error = p + ".prompt 须是非空字符串";
      return false;
    }
    x.prompt = q["prompt"].get<std::string>();
    if (!q.contains("options") || !q["options"].is_array() || q["options"].size() < 2 || q["options"].size() > 5 ||
        !std::all_of(q["options"].begin(), q["options"].end(), [](const json &o) { return o.is_string() && !o.get<std::string>().empty(); })) {
      error = p + ".options 须是 2..5 个非空字符串的数组 (每项一句话写完这个决定, 不是对象)";
      return false;
    }
    for (const json &o : q["options"])
      x.options.push_back(o.get<std::string>());
    if (q.contains("multiple")) {
      if (!q["multiple"].is_boolean()) {
        error = p + ".multiple 须是布尔";
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
      error = p + ".recommended 须是选项下标数组 (从 0 起, 不重复; 单选恰 1 个, 多选至少 1 个)";
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
  for (const auto &[key, v] : {std::pair<std::string_view, int>{"{batch}", kBatch},
                               {"{hyp_evals}", kHypEvals},
                               {"{hyp_knobs}", kHypKnobs},
                               {"{part_chars}", kPartChars},
                               {"{stock_days}", kStockDays},
                               {"{stock_rows}", kStockRows},
                               {"{book_rows}", kBookRows},
                               {"{book_mins}", kBookMins}})
    for (size_t p = t.find(key); p != std::string::npos; p = t.find(key, p))
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
  case Arg::Parts:
    return "数组, " + std::to_string(a.lo) + ".." + std::to_string(a.hi) + " 块";
  }
  assert(false && "未知参数类型");
  return {};
}

std::string ArgText(const ArgDef &a) {
  return "`" + std::string(a.key) + "` (" + ArgType(a) + (a.def ? ", 缺省 " + std::string(a.def) : std::string(", 必填")) + "): " + ToolText(a.desc);
}

std::string Call::str(const char *k) const { return args.at(k).get<std::string>(); }
int Call::num(const char *k) const { return args.at(k).get<int>(); }
std::vector<std::string> Call::strs(const char *k) const { return args.at(k).get<std::vector<std::string>>(); }

std::string Call::text(size_t cap) const {
  std::string s = std::string(def(tool).name) + "(";
  bool first = true;
  for (const ArgDef &a : def(tool).args) {
    const json &v = args.at(a.key);
    if (a.type == Arg::Questions || a.type == Arg::Parts) {
      s += (first ? "" : ", ") + std::to_string(v.size()) + (a.type == Arg::Questions ? " 个问题" : " 个功能块");
    } else {
      if (a.def && v == json::parse(a.def)) // 缺省值不写
        continue;
      s += (first ? "" : ", ") + std::string(a.key) + "=" + (cap ? agent::text::head(brief(v), cap) : brief(v));
    }
    first = false;
  }
  return s + ")";
}

bool ParseCalls(std::string_view reply, bool ask, std::vector<Call> &out, std::string &error) {
  out.clear(), error.clear();
  json j;
  if (!object_of(reply, j, error))
    return false;
  if (!j.contains("calls") || !j["calls"].is_array()) {
    error = "缺数组 calls (每次回复 = 一串工具调用)";
    return false;
  }
  const json &cs = j["calls"];
  if (cs.empty() || cs.size() > static_cast<size_t>(kMaxCalls)) {
    error = "calls 须有 1.." + std::to_string(kMaxCalls) + " 个调用 (收到 " + std::to_string(cs.size()) + " 个; 多的留到下一条)";
    return false;
  }
  int evals = 0;
  for (size_t i = 0; i < cs.size(); ++i) {
    const std::string p = at("calls", i);
    const json &c = cs[i];
    if (!c.is_object() || !c.contains("tool") || !c["tool"].is_string()) {
      error = p + " 须是 {\"tool\": 工具名, \"args\": {...}}";
      return false;
    }
    const std::string name = c["tool"].get<std::string>();
    const ToolDef *d = find(name);
    if (!d || (d->id == Tool::AskHuman && !ask)) {
      error = p + ".tool: " + (d ? "ask_human 本会话不可用 (研究员没开提问, 或已问过一次; 一条回复也只能有一个)" : "没有工具 " + name);
      return false;
    }
    if (d->id == Tool::AskHuman)
      ask = false;
    if (d->id == Tool::Finish && i + 1 != cs.size()) {
      error = p + ": finish 须是最后一个调用";
      return false;
    }
    if (d->id == Tool::EvalFactor && ++evals > kBatch) {
      error = p + ": 一条回复至多 " + std::to_string(kBatch) + " 个 eval_factor (这是第 " + std::to_string(evals) + " 个)";
      return false;
    }
    const json args = c.contains("args") ? c["args"] : json::object();
    if (!args.is_object()) {
      error = p + ".args 须是 JSON 对象";
      return false;
    }
    for (const auto &[k, v] : args.items())
      if (std::none_of(d->args.begin(), d->args.end(), [&](const ArgDef &a) { return k == a.key; })) {
        std::string keys;
        for (const ArgDef &a : d->args)
          keys += (keys.empty() ? "" : " / ") + std::string(a.key);
        error = p + ".args." + k + ": " + name + " 没有这个参数 (它的参数: " + (keys.empty() ? "无" : keys) + ")";
        return false;
      }
    Call call{d->id, json::object()};
    for (const ArgDef &a : d->args) {
      const std::string ap = p + ".args." + a.key;
      if (!args.contains(a.key)) {
        if (!a.def) {
          error = "缺参数 " + ap;
          return false;
        }
        call.args[a.key] = json::parse(a.def);
        continue;
      }
      if (!check(a, args[a.key], ap, error))
        return false;
      Questions qs;
      if (a.type == Arg::Questions && !questions_of(args[a.key], ap, qs, error))
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
  std::string error;
  [[maybe_unused]] const bool ok = questions_of(c.args.at("questions"), "questions", qs, error);
  assert(ok && "解析时已校验");
  return qs;
}

std::vector<Part> PartsOf(const Call &c) {
  assert(c.tool == Tool::EvalFactor);
  std::vector<Part> out;
  for (const json &x : c.args.at("parts"))
    out.push_back({x.at("expr").get<std::string>(), x.at("means").get<std::string>()});
  return out;
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
