// Factors→Portfolio: 因子组合页 + Factors 页 1. 区右侧的两个组合小框.
//   小框 1: 选组合 (<portfolio_dir>/<name>.json) / New / Rename / Del;  小框 2: 当前组合的条目 "(池, 持有期): 名", Add = 加入 Factors 表里高光的
//   权重 (持有期 = Factors 页 Hold), Del = 删选中条目; 失效条目 (因子文件里找不到该规范串等) 红. 组合选择在内存, 内容即文件.
//   Portfolio 页 (对仗 OrderFlow 图 2 的横轴: x = 日下标 · 255 + 行末分钟, 刻度只到日 YY/MM/DD; 不画个股):
//   NAV 右表 = 顶部类别条选一组看 (指数 / 行业 / 概念 / 因子 / 策略, 五组同形: 一行一对线, 见 NavGroup), 点行 = 选中 (同 VAL 行高亮; 跨组保留) = 主图画该行两条线, 主图例默认只显理想列;
//     默认不选 (空图). 列 = 中文名 (线色; 悬停看名) | 当日涨幅 | Δ5日线 | 只数 | 总市值 | 换手 | 资金流入 | 可成交年化 | 理想年化 | 夏普 (可成交列) | 年换手;
//     近况两列 (Database::Recent; 篮子行 = Assets 篮子的日线 + 新浪当日, 因子 / 策略 = 净值日末点) 按涨跌着色, 不随区间;
//     只数 / 总市值 (只篮子行) = 当日成员表 (Tape::members / weight): 只数取同切片等权线, 总市值取同切片加权线 (当日接上 × 当日涨跌), 不着色;
//     换手 / 资金流入 (只篮子行) = 新浪当日 成交额占总市值 / 净流入占成交额 (Day::flow, 分钟涨跌法), 流入按正负着色, 悬停格看分子分母金额;
//     篮子组 (指数 / 行业 / 概念) 表上一行过滤条 (BasketFilter, 三页共用): 只数 / 总市值区间 + |流入| 下限, 选中行恒列;
//     三页右表同一骨架 (sheet), 列宽按内容最紧 (数值右对齐不补宽, 表头取短名, 名超 kNameCells 格截断补 …), 右栏宽 = 表格与表上工具行之大者;
//     篮子行可展开 (名前 ▸, 三页同一份展开集): 下挂成分行 = 历史末日收盘定的成员表 (Tape::cons, 权降序; 表头排序时按同列), 列与篮子行同一张表,
//       能算的填 (近况 / 总市值 / 换手 / 流入 = 个股新浪当日 + 基本面; VAL 各口 = 该股当前收益率; FLOW 只当日), 其余 "-"; 名悬停看权占比 / 行业 / 估值;
//       点成分行 = 叠加 / 取消该股到日内栏 / FLOW 图 (个股历史 + 新浪当日, 线色按 id 定色相);
//     表头悬停 = 该列说明 (同一格式: 列名 = 全称 + 算法 / 数据 / 显示 / 适用 四行);
//     其余数值列底色 = 同组同列截面名次 (好绿 → 差红: 年化 / 夏普高好, 年换手低好; 同 VAL 配色); 表头点排序 (组内, 升 → 降 → 原序; 名列按色相; VAL 表同)
//   NAV 日内栏 (类别条旁 "日内" 开关, 默认开): 主图上方占左栏 1/3 高 (日内 : 主图 = 1 : 2), 选中的篮子行同主图线色叠加日内收盘线, 点中的成分股一并叠加
//     (Database::RenderBasketIntraday: Assets 篮子同一份, 当日由新浪快照合成; 对首日前收的涨跌幅, 横网格 (含 0 轴) 涨跌幅小字内标在右壁,
//      线名中文小字贴线尾, 读数看十字光标); 只一条 = 同一张图改画分钟蜡烛 + 现价十字 (标涨跌幅 / 时刻)
//   主图 / 日内栏贴边 (窄内边距, 不标轴名), 图区让给线
//   主图例只列画出的线 (同列, 补空格对齐; 悬停一条看名), 点图例单独隐 / 显一条
//     组合策略线 (有冲击 / 无冲击两条) = 各因子独立回测 (PortfolioService 的账 PortfolioBook,
//     各自的层, 净值引擎 fold_book: 每行开 1/h̄ 一批 (全日平均满仓), 收益在平仓行实现; 可成交 / 顺延在标签里) 的逐行增量等权聚合: 每个交易日在当日有数据的 n 个因子间等分 (按实现日),
//     因子增量 = 毛收益 − 建仓冲击 − 固定冲击份数 · fixed_impact (占净值), 记在行末 (收益 = 平仓行, 成本 = 建仓行;
//       已实现口径, 与池指数对齐); 净值 = 同一 x 的增量和逐点复利连乘. 单因子线同式 (权 1) ⇒ 单因子线 ≡ 只含它的组合.
//       只算 G20 多头 (无冲击 = 不扣). 成本按 Δw (Stat 契约【换手成本】), 冲击段 = 各因子自己的主段, 不可选:
//       用了 lob.* 叶 = 增仓 × 最小金额段建仓冲击 + 平仓量 × Config::fixed_impact; 否则固定段, 增仓量 / 平仓量都按 fixed_impact;
//       年换手 = 自首次建仓起的单边 (倍/年: Σ 增仓量 / 其后交易日数 × 每年交易日; 组合 = 各因子增仓量按当日份额加权, 不计因子间对冲)
//     篮子线 (指数 / 行业 / 概念) = 篮子目录三组 (shared/Basket.hpp basket_group: 各池整体, all 池两条 = 全A.加 (总市值) / 全A.等, 其余池等权
//       / SW2021 一级, 总市值加权 / 东财题材概念 (全A 内切, 成分按最近一份快照倒推全史), 总市值加权):
//       只画因子日并集轴范围内: 范围首日开盘前一点 (前收) + 每日收盘一点; 每条成对 = 可成交 (T+1C 标签: 封板不建仓 / 平仓顺延) + 理想 (价格指数, 不看可成交:
//       Assets 篮子同一份), 两者之差 = 可成交拖累; 年换手 = 篮子账的增仓量同式 (成分再平衡 + 进出; 指数线本身不扣成本)
//     单因子线: 该因子单独的 G20 多头净值, 成对 = 有冲击 (它自己的冲击段) + 无冲击, 年换手 = 它自己的
//     y = 净值 (线性下限钉 0; 可切 log, log 下正常缩放); 底部区间条选日段 [d0, d1) (两边竖把手 / 段内平移, 拖动中吸附到月初即生效, 把手旁标起止日), 各线在区间起点归一 (起点 = 1), 年化按区间复利折算;
//     年化 / 夏普只数该线在区间内的活跃日 (日末净值, 活跃日内无点的日收益 0): 夏普 = 日收益 mean / sd × √每年交易日;
//     主图内容 (新结果到 / 区间 / VAL 选中 / 滚动窗) / 页 / log / 主图例显隐 / 右表选中 任一变 → 重新 fit (PortfolioFitKey); 每条可见线在视野内的末点右侧标小字名
//   页: NAV = 上面的资金曲线; VAL = 池估值 (同一布局, 区间 / log 共用; 行 = NAV 篮子三组的同一份): 右侧类别条 (指数 / 行业 / 概念) 选一组, 表格单选一个池 / 申万一级行业
//     (全A 内按上一行 ind_l1 切) / 概念 (再点 = 取消); 默认不选 (空图); 列 = 中文名 (悬停看名) | 只数 | 总市值 | 换手 | 资金流入 (同 NAV) | 区间末 EP / BP / SP / CP / DY 的 N 年估值分位,
//     底色 = 该分位在同组同口内的截面名次, 便宜绿 → 贵红),
//     主图 = 该篮子指数 P (全A.加 / 行业 / 概念 = 总市值加权, 全A.等 / 其余池 = 等权; 区间起点 = 1, 与 NAV 同一切片) + 同权重的滚动 EP / BP / SP / CP / DY Band
//     (默认只开 BP): 价格 band y_t = P_t · v_t / q_j(t), v = 收益率 (高 = 便宜), q_j(t) = 过去 N 年窗内 v 按贵度 (−v) 的 kBandQs 分位
//     (N = 右图例滑条, 即时重算; 正值窗内 ≡ 倍数口径 P · m_q / m); band 间填色 (最便宜档以下绿 → 最贵档以上红, 低 alpha 渐变), 指数线在上;
//     不可算 / v ≤ 0 / 窗内有 v ≤ 0 的点断开, q_j ≤ 0 的档单独断开;
//     绝对估值等值线 (默认关; 很淡的灰, 不进 fit): 倍数 m = 1 / v (PE / PB / PS / PCF / P/D), y_t = P_t · v_t · m,
//     档 = 该篮子全史倍数区间 (两头剔极端分位) 20 档等分, 不随区间 / 视野变; v ≤ 0 断开; 悬停最近一条高亮 + 提示该档倍数 / 当日倍数 / 指数到该线的涨跌;
//     主图例 = 左上自绘矩阵 (对仗 Inspect 期限结构): 行 = 口, 列 = 当前收益率 | 当前倍数 | N 年分位 | Band | 等值线, 点色块 / 行头 / 列头开关, 悬停高亮
//   页 FLOW = 篮子日内资金流 (只这一张图, 不看区间): 左 = 选中篮子行 (NAV 同一份选中) + 点中的成分股的累计净流入叠加 (Database::RenderBasketFlow:
//     日轴 = 行情日及其前 n − 1 个交易日, n = 档 日内 / 2日 … 5日; 自首日开盘逐分钟累计, 跨日接续; y = 占近 5 日日均成交额 % (默认; 分母盘前已定) / 金额 亿元;
//     开收盘开关 (默认关) = 计入首根 (开盘集合竞价并入) / 末根 (收盘集合竞价), 图与右表同一份; 当日随新浪快照动);
//     右表 = 类别条 (指数 / 行业 / 概念) + 过滤条 + 点行选中; 列 = 中文名 | 只数 | 总市值 | 换手 | 资金流入 (同 NAV) | 日轴各日净流入占当日成交额 | 档内合计净流入 | 合计占比 (按正负着色, 悬停格看金额)
#pragma once

#include "gui/task_database/services/Market.hpp"
#include "gui/task_database/ui/QuotesPanel.hpp"      // BasketState / Recent / Flow (NAV 日内栏 + 近况列 / FLOW 页)
#include "gui/task_factors/services/FactorsFile.hpp" // FactorRow
#include "gui/task_factors/services/PortfolioService.hpp"
#include "gui/task_factors/ui/PortfolioView.hpp" // 派生 → 滚动分位 / 末点分位 → 视图
#include "imgui.h"                               // ImVec4 (右表格)
#include "shared/Basket.hpp"                     // kGroupPool / kBasketGroups

#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace GUI::Factors {

// 主图 fit 键: 主图画的全部内容 (视图 gen 已含派生 / 滚动分位 / 区间) + 页 + log + NAV 右表选中 + 上帧主图例各线显隐; 与上次 fit 的不同 → 本帧 fit
struct PortfolioFitKey {
  uint64_t view = 0; // 视图 gen
  bool nav = true, log = false;
  std::string rows;  // NAV 各行选中 (VAL 空)
  std::string shown; // 上帧主图例各线显隐
  bool operator==(const PortfolioFitKey &) const = default;
};

// 区间条: [d0, d1) (der.dates 下标; 两边在月初 / 末尾; 日轴变 → 全段) + 拖动态
struct PortfolioRange {
  int d0 = 0, d1 = 0;
  int drag = 0;                 // 拖动中: 0 无 / 1 左把手 / 2 右把手 / 3 段内平移
  double grab_day = 0.0;        // 按下时鼠标所在日 (平移按与它的差算)
  int grab_k0 = 0, grab_k1 = 0; // 按下时两边的月初序号 (平移保持 k1 − k0)
};

// 页签 (同一个右图例位; 图例宽 / 类别条按页各一份; 页表 kPageDefs 见 TabPortfolio.cpp)
enum PortfolioPage : int { kPageNav,
                           kPageVal,
                           kPageFlow };
inline constexpr int kPages = 3;
// 右表行的选中方式 (页表按页指定; 同方式的页共用一份选中)
enum PickMode : int { kPickMany,  // 多选 (NAV / FLOW)
                      kPickOne }; // 单选, 再点 = 取消 (VAL)
inline constexpr int kPicks = 2;

// 篮子组 (指数 / 行业 / 概念) 右表的行过滤 (三页共用, 按组各一份): 只数 / 总市值 [亿元] ∈ [lo, hi] (hi 0 = 不限), |资金流入| ≥ flow (%; 0 = 不限);
// 设了的条件缺值 (未到 / 无新浪行情日) 即不过; 选中行恒列
struct BasketFilter {
  int n_lo = 0, n_hi = 0;
  float cap_lo = 0.f, cap_hi = 0.f;
  float flow = 0.f;
};

// 右表格数值格 (格式串不补宽): 先量出各列最宽 (含表头), 再右对齐到该宽 ⇒ 列宽恒 = 内容, 可缩
//   (不能对齐到列宽本身: 列宽是上帧的, 对齐后每格都顶满, 列只涨不缩)
struct NumCell {
  char s[24] = "-";
  ImVec4 col;
  float w = 0.f; // 文本宽 (填格时的字号)
};
// 一格
struct SheetCell {
  NumCell text;
  double key = std::numeric_limits<double>::quiet_NaN();  // 排序键 (不可算 = NaN, 恒末)
  double rank = std::numeric_limits<double>::quiet_NaN(); // 底色 = 同组同列名次 (Ranked::rank: 0 = 好 绿 → 1 = 差 红); NaN = 不铺
};
// 当日四列 (三张右表同一份, 不随区间)
enum TodayCol : int { kTodayMembers,
                      kTodayCap,
                      kTodayTurn,
                      kTodayFlow };
inline constexpr int kTodays = 4;
// 篮子行 / 成分行的当日: [TodayCol] + 成交额 [亿元] (换手 / 流入悬停的分子分母); 因子 / 策略行 / 未到 → NaN; 成分行无只数
struct SheetToday {
  static constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
  double v[kTodays] = {kNaN, kNaN, kNaN, kNaN};
  double amount = kNaN;
};
// 展开行的成分在新浪当日的根 (成分行的当日列): 成分集 / 行情日 · 快照时刻变了才重取
struct PortfolioConsToday {
  std::vector<uint32_t> ids;                             // 各展开行成分之并 (全局 id, 升序)
  std::shared_ptr<const std::vector<Database::Day>> day; // [ids] (QuoteService::today 个股; 当日无成交 → candle.date 0)
  int64_t at = -1;                                       // 行情日 · 快照时刻
};
// 一页右表一组的格 (每帧只过滤 / 排序 / 画): 键 = 格的全部来源, 变了才整组重填 (过滤 / 排序 / 选中不在键里)
struct PortfolioSheetKey {
  int g = -1;                                              // 组
  uint64_t view = 0;                                       // 视图 gen (含派生: 行 / NAV 数值列 / VAL 估值分位)
  std::shared_ptr<const Database::Tape> data;              // 篮子历史在画的发布 (近况 / 只数 / 总市值 / 资金流历史日 / 成分)
  std::shared_ptr<const std::vector<Database::Day>> today; // 篮子当日 (近况 / 当日四列 / 资金流当日)
  std::set<std::string> open;                              // 展开的行
  std::shared_ptr<const std::vector<Database::Day>> cons;  // 成分当日 (PortfolioConsToday::day)
  size_t infos = 0;                                        // 基本面档案数 (首次载入)
  std::vector<misc::Ymd> days;                             // FLOW 日轴 (其余页空)
  bool ends = false;                                       // FLOW 计入开收盘
  float font = 0.f;                                        // 字号 (格宽)
  uint32_t ink = 0;                                        // 字色 (主题)
  bool operator==(const PortfolioSheetKey &) const = default;
};
// 成分行: 展开的篮子行下的一只
struct PortfolioKid {
  uint32_t id = 0;   // 全局 id
  std::string name;  // 简称 (无档案 → 代码)
  std::string hover; // 悬停 (代码 + 权占比 + 行业 / 估值)
};
struct PortfolioSheet {
  size_t nc = 0;                                    // 名之后的列数
  std::vector<SheetCell> cells;                     // [行][nc]; 行 = 组内行 [0, R) + 成分行 [R, R + 成分数)
  std::vector<SheetToday> today;                    // [行] 当日 (过滤条 / 悬停)
  std::vector<std::vector<Database::Flow>> flow;    // FLOW: [行][日轴各日 + 合计] (悬停)
  std::vector<uint32_t> kid_at{0};                  // [R + 1]: 组内行 r 的成分 = kids [kid_at[r], kid_at[r + 1]) (未展开 = 空)
  std::vector<PortfolioKid> kids;                   // 成分行 R + j ↔ kids[j] (各展开行内权降序)
  size_t rows() const { return kid_at.size() - 1; } // R
};

// 右表行选中 (PortfolioRow::id; 默认空): 属于组合 of, 换组合 → 整份清空
struct PortfolioPick {
  std::string of;
  std::set<std::string> rows[kPicks]; // [PickMode] 选中行 (kPickOne 至多一个)
  std::set<std::string> open;         // 展开的篮子行 (三页同一份)
  std::set<uint32_t> stocks;          // 点中的成分股 (全局 id; 叠加到日内栏 / FLOW 图; 至多 StockHistory::kKeep 只)
};

struct PortfolioUIState {
  // ---- 组合 (小框) ----
  std::string dir;                     // names 对应的目录 (config.portfolio_dir 变 → 重列)
  std::vector<std::string> names;      // 目录下的组合 (主干名)
  std::string name;                    // 当前组合; 空 = 无
  std::vector<PortfolioEntry> entries; // 当前组合内容 (= 文件)
  std::vector<PortfolioItem> items;    // 解析结果 (TaskFactors 在内容变 / 因子重扫后重解析; 与 entries 同序, 可能落后一帧)
  uint64_t content_gen = 0;            // 换组合 / 改内容 +1
  uint64_t items_gen = 0;              // items 每改一次 +1 (解析 / 建请求时标失效)
  int sel = -1;                        // 小框 2 选中条目
  char name_buf[64] = "";              // New / Rename 的名字
  std::string msg;                     // 上次操作结果
  bool confirm_delete = false;
  // ---- Portfolio 页 ----
  PortfolioStage<PortfolioDerKey, PortfolioDerived> der;
  PortfolioStage<PortfolioRollKey, std::array<PortfolioRoll, kPoolVals>> roll; // [PoolVal] 选中篮子的
  PortfolioStage<PortfolioPctKey, PortfolioPct> pct;                           // VAL 表格各格的末点
  PortfolioStage<PortfolioViewKey, PortfolioView> view;
  PortfolioStage<PortfolioSheetKey, PortfolioSheet> sheet[kPages]; // [PortfolioPage] 右表格
  PortfolioRange range;
  PortfolioPick pick;
  int band_years = kBandYearsDef; // VAL 滚动窗 N (年; 右图例滑条)
  bool log_y = false;
  std::set<std::string> reveal;                                              // 待在主图例里回默认显隐的线 ID (行刚选中; 画到时消费; 理想列显, 可成交列隐)
  std::string shown_key;                                                     // 上帧主图各线显隐 (进 fit 键)
  PortfolioFitKey fit_key;                                                   // 上次 fit 的键 (BeginPlot 没起来不记: 下帧照 fit)
  float legend_w[kPages] = {400.0f, 380.0f, 380.0f};                         // [PortfolioPage] 右图例表格不截断所需的宽 (上帧 EndTable 量的; 本帧右图例按它定宽)
  int cat[kPages] = {kNavPool, kGroupPool, kGroupPool};                      // [PortfolioPage] 右表类别条选中组 (NAV = NavGroup; VAL / FLOW = BasketGroup)
  BasketFilter filter[kBasketGroups] = {{}, {}, {.n_hi = 500, .flow = 5.f}}; // [BasketGroup] 默认: 指数 / 行业不滤, 概念 只数 ≤ 500 且 |流入| ≥ 5%
  bool intraday = true;                                                      // NAV 日内栏
  Database::BasketState basket;                                              // 篮子历史 + 当日 + 叠加的个股 (日内栏 / FLOW / 篮子行近况 / 成分)
  PortfolioConsToday cons;                                                   // 成分行的新浪当日
};

// GUI 线程每帧 (两页都调): 目录变 → 重列, 当前组合不在了 → 选第一个 (载入内容, content_gen +1)
void SyncPortfolioDir(PortfolioUIState &ui, const std::string &dir);
// Factors 页 1. 区右侧两框. view_row = Factors 表高光行 (nullptr = 无); (pool, lv, hold) = Factors 页当前池 / 层 / Hold
void RenderPortfolioBoxes(PortfolioUIState &ui, const FactorRow *view_row, const std::string &pool, LevelId lv, int hold);
// mk = 行情 (已起, ui.basket 本帧已 Sync; nullptr = AssetInfo 未建, 日内栏 / 指数行业近况 / 成分行当日空着); infos = 基本面档案 (成分行名 / 近况 / 估值).
// 返回值: 1 = Recompute (弃缓存重算), -1 = Cancel, 0 = 无
int RenderTabPortfolio(PortfolioService &svc, PortfolioUIState &ui, double fixed_impact, const Database::Market *mk, const StockInfos &infos);

} // namespace GUI::Factors
