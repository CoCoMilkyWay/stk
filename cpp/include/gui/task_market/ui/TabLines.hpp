// Market 的行情页 Lines (NAV / VAL / FLOW / CTA, 左栏子页选): 一行一对线 (Line), 四页同一份派生 / 选中 / 区间.
//   组合 (因子 / 策略两组的来源) 由 PortfolioBoxes.hpp 编辑; 篮子账不依赖组合 (all 标签库建好即算): 无组合时 NAV 只少因子 / 策略两组, 其余各页照常
//   行情页 (对仗 OrderFlow 图 2 的横轴: x = 日下标 · 255 + 行末分钟, 刻度只到日 YY/MM/DD):
//   NAV 右表 = 顶部类别条选一组看 (指数 / 行业 / 概念 / 自选 / 因子 / 策略, 六组同形: 一行一对线, 见 RowGroup), 点行 = 选中 (同 VAL 行高亮; 跨组保留) = 主图画该行两条线, 主图例默认只显理想列;
//     默认不选 (空图). 列 = 中文名 (线色; 悬停看名) | 当日涨幅 | Δ5日线 | 只数 | 总市值 | 换手 | 资金流入 | 可成交年化 | 理想年化 | 夏普 (可成交列) | 年换手;
//     近况两列 (Market::Recent; 篮子行 = Market 篮子历史的日线 + 新浪当日, 因子 / 策略 = 净值日末点) 按涨跌着色, 不随区间;
//     只数 / 总市值 (只篮子行) = 当日成员表 (Tape::members / cap): 成员只数 / 末日收盘成员总市值 (加权线当日接上 × 当日涨跌), 不着色;
//     换手 / 资金流入 (只篮子行) = 新浪当日 成交额占总市值 / 净流入占成交额 (Day::flow, 分钟涨跌法), 流入按正负着色, 悬停格看分子分母金额;
//     篮子组 (指数 / 行业 / 概念) 表上一行过滤条 (BasketFilter, 各页共用): 只数 / 总市值区间 + |流入| 下限, 一项一个按钮 (名 + 当前条件摘要, 设了 = 按下色),
//       点开 = 预设 (点即设) + 精确输入; 选中 / 展开行恒列;
//     右栏定宽 (各页同宽, 不随内容): 各页右表同一骨架 (sheet), 列宽按内容最紧 (数值右对齐不补宽, 表头取短名, 名超 kNameCells 格截断补 …),
//       名列伸满余宽, 内容超宽 → 横滚 (名列钉住);
//     篮子行可展开 (名前 ▸, 至多一行, 各页同一份): 展开行钉在表头下 (不随滚动), 其下先列成分行 = 历史末日收盘定的成员表 (Tape::cons, 默认权降序,
//       表头排序时按同列), 再列其余行; 成分行与篮子行同一种行 (LinesRow: 一对线 = 后复权收盘, 两条同一条; 行情键 = 个股; 估值腿 = 个股账),
//       列同一张表, 能算的填 (近况 / 总市值 / 换手 / 流入 = 个股新浪当日 + 基本面; NAV 数值列 / VAL 各口名次在成分之间;
//       FLOW = 与篮子行同一查法 Market::SourceOf: 选中的有个股历史各日齐, 其余只当日), 其余 "-";
//       名悬停看权占比 / 行业 / 估值; 点成分行 = 同篮子行的选中 (NAV 主图画线 + 日内栏 / FLOW 图叠加, 线色按 id 定色相; VAL 选为标的), 收起后选中的照画;
//     自选 (各页类别条都有, 排在概念后): 篮子行 / 成分行可加, 全局一份 (output/config/market/watch.json, 自选序); 各页右表首列 = 星列 (点星 = 加入 / 移出),
//       名格右键同; 自选组的行 = 原行的副本 (选中 / 展开同一份, 篮子行照样 ▸ 展开), 数值列名次在自选之间; 找不到的键 (概念换快照没了) 在表上一行计数, 可一键移出;
//     表头悬停 = 该列说明 (同一格式: 列名 = 全称 + 算法 / 数据 / 显示 / 适用 四行);
//     其余数值列底色 = 同组同列截面名次 (好绿 → 差红: 年化 / 夏普高好, 年换手低好; 同 VAL 配色); 表头点排序 (组内, 升 → 降 → 原序; 名列按色相; VAL 表同);
//     原序下子池 / 自选行左键 / 中键拖动调序 (子池 = 改池表序 pools.json, 即进程内重启; 全A 两行钉首)
//   NAV 日内栏 (类别条旁 "日内" 开关, 默认开): 主图上方占左栏 1/3 高 (日内 : 主图 = 1 : 2), 选中的篮子行 / 成分行同主图线色叠加日内收盘线
//     (Market::RenderIntraday: Market 篮子历史同一份, 当日由新浪快照合成; 对首日前收的涨跌幅, 横网格 (含 0 轴) 涨跌幅小字内标在右壁,
//      线名中文小字贴线尾, 读数看十字光标); 只一条 = 同一张图改画分钟蜡烛 + 现价十字 (标涨跌幅 / 时刻);
//     底部 1/4 = 委比副图 (当日各根五档委比红绿柱, 篮子按成员权平均; 只选一条看它, 否则看全A.等)
//   主图 / 日内栏贴边 (窄内边距, 不标轴名), 图区让给线
//   悬停联动 (NAV 主图 / 日内栏 / FLOW 图 ↔ 右表, 同 Beta→Style: gui/util/HoverSync.hpp): 图上悬停最近一条线 = 右表该行底色亮并滚到居中
//     (悬停止回原位; 主图另出行的悬停提示); 右表悬停一行 = 图上该行的线白粗线画在最上; 主图与日内栏互相跟着亮
//   主图例 = 左上自绘矩阵 (同 VAL): 行 = 选中行 (名 = 线色), 列 = 可成交 | 夏普 | 理想 | 夏普 | 年换手 (因子 / 策略行 = 有冲击 / 无冲击; 年化 / 夏普按线随区间);
//     线格 = 色块 + 区间年化, 点色块单独隐 / 显一条, 点名 / 列头开关整行 / 整列; 悬停一行 = 同图上悬停的联动, 名格看名
//     组合策略线 (有冲击 / 无冲击两条) = 各因子独立回测 (PortfolioService 的账 PortfolioBook,
//     各自的层, 净值引擎 fold_book: 每行开 1/h̄ 一批 (全日平均满仓), 收益在平仓行实现; 可成交 / 顺延在标签里) 的逐行增量等权聚合: 每个交易日在当日有数据的 n 个因子间等分 (按实现日),
//     因子增量 = 毛收益 − 建仓冲击 − 固定冲击份数 · fixed_impact (占净值), 记在行末 (收益 = 平仓行, 成本 = 建仓行;
//       已实现口径, 与池指数对齐); 净值 = 同一格 (日, 行末分钟) 内各因子的增量 (同因子格内先复利) 按份额加和, 逐格复利连乘.
//       单因子线 / 篮子可成交线同式 (一腿) ⇒ 单因子线 ≡ 只含它的组合.
//       只算 G20 多头 (无冲击 = 不扣). 成本按 Δw (Stat 契约【换手成本】), 冲击段 = 各因子自己的主段, 不可选:
//       用了 lob.* 叶 = 增仓 × 最小金额段建仓冲击 + 平仓量 × Config::fixed_impact; 否则固定段, 增仓量 / 平仓量都按 fixed_impact;
//       年换手 = 自首次建仓起的单边 (倍/年: Σ 增仓量 / 其后交易日数 × 每年交易日; 组合 = 各因子增仓量按当日份额加权, 不计因子间对冲)
//     篮子线 (指数 / 行业 / 概念) = 篮子表三段 (shared/Basket.hpp BasketTable: 各子池 (全A.等 / 全A.加 起头), 按池参数的权重
//       / SW2021 一级, 总市值加权 / 东财题材概念 (全A 内切, 成分按最近一份快照倒推全史), 总市值加权):
//       日轴 = 篮子账日 ∪ 因子日: 首日开盘前一点 (前收) + 每日收盘一点; 每条成对 = 可成交 (T+1C 标签: 封板不建仓 / 平仓顺延) + 理想 (价格指数, 不看可成交:
//       Market 篮子历史同一份), 两者之差 = 可成交拖累; 年换手 = 篮子账的增仓量同式 (成分再平衡 + 进出; 指数线本身不扣成本)
//     单因子线: 该因子单独的 G20 多头净值, 成对 = 有冲击 (它自己的冲击段) + 无冲击, 年换手 = 它自己的
//     y = 净值 (线性下限钉 0; 可切 log, log 下正常缩放); 底部区间条选日段 [d0, d1) (两边竖把手吸附到月初 / 段内按日平移, 拖动中即生效, 把手旁标起止日; 双击段内回全段;
//       主图左上角档位 1周 / 1月 / 1年 / 10年 (对仗日内栏档位, 图例矩阵在其下) = 末日往前推该段, 当前区间正是某档则该钮按下), 各线在区间起点归一 (起点 = 1), 年化按区间复利折算;
//     年化 / 夏普只数该线在区间内的活跃日 (日末净值, 活跃日内无点的日收益 0): 夏普 = 日收益 mean / sd × √每年交易日;
//     主图内容 (新结果到 / 区间 / VAL 选中 / 滚动窗) / 页 / log / 主图例显隐 / 右表选中 任一变 → 重新 fit (LinesFitKey); 每条可见线在视野内的末点右侧标小字名;
//     只显示一条线时 (VAL = 价格线) 同日内栏画现价十字: 过视野内末点的横 / 竖虚线 (区间涨跌色), 右壁标区间涨跌, 底壁标日期
//   页: NAV = 上面的资金曲线; VAL = 估值 (同一布局, 区间 / log 共用; 行 = NAV 篮子三组 + 自选的同一份): 右侧类别条 (指数 / 行业 / 概念 / 自选) 选一组, 表格单选一个标的 = 池 / 申万一级行业
//     (全A 内按上一行 ind_l1 切) / 概念 / 展开行的成分股 (再点 = 取消); 默认不选 (空图); 列 = 中文名 (悬停看名) | 只数 | 总市值 | 可成交 | 理想 | 夏普 | 年换手 (同 NAV)
//     | 区间末 EP / BP / SP / CP / DY 的 N 年估值分位,
//     底色 = 该分位在同组同口内的截面名次, 便宜绿 → 贵红),
//     主图 = 标的可成交线 P (篮子 = 可成交指数: 行业 / 概念 = 总市值加权, 子池 (含全A) = 池参数的权重; 成分股 = 后复权收盘; 区间起点 = 1, 与 NAV 同一切片)
//     + 同口径的滚动 EP / BP / SP / CP / DY Band (篮子 = 同权重, 成分股 = 本只; 篮子与个股同一套: 两本账同一作业出, 见 LinesView.hpp)
//     (默认只开 BP): 价格 band y_t = P_t · v_t / q_j(t), v = 收益率 (高 = 便宜), q_j(t) = 过去 N 年窗内 v 按贵度 (−v) 的 kBandQs 分位
//     (N = 右图例滑条, 即时重算; 正值窗内 ≡ 倍数口径 P · m_q / m); band 间填色 (最便宜档以下绿 → 最贵档以上红, 低 alpha 渐变), 指数线在上;
//     不可算 / v ≤ 0 / 窗内有 v ≤ 0 的点断开, q_j ≤ 0 的档单独断开;
//     绝对估值等值线 (默认关; 很淡的灰, 不进 fit): 倍数 m = 1 / v (PE / PB / PS / PCF / P/D), y_t = P_t · v_t · m,
//     档 = 该标的全史倍数区间 (两头剔极端分位) 20 档等分, 不随区间 / 视野变; v ≤ 0 断开; 悬停最近一条高亮 + 提示该档倍数 / 当日倍数 / 指数到该线的涨跌;
//     主图例 = 左上自绘矩阵 (对仗 Inspect 期限结构): 行 = 口, 列 = 当前收益率 | 当前倍数 | N 年分位 | Band | 等值线, 点色块 / 行头 / 列头开关, 悬停高亮;
//     主图恒贴合: x = 区间, y 只看价格线 (上限 = 最高点 + 20%, band 再高也不撑); 不平移 / 缩放, 左键横向框选 = 区间改成该段 (任意日, 区间条同一份), 双击 = 回全段
//   页 FLOW = 篮子日内资金流 (只这一张图, 不看区间): 左 = 选中的篮子行 / 成分行 (NAV 同一份选中) 的累计净流入叠加 (Market::RenderFlow:
//     日轴 = 行情日及其前 n − 1 个交易日, n = 档 日内 / 2日 … 5日; 自首日开盘逐分钟累计, 跨日接续; y = 占近 5 日日均成交额 % (默认; 分母盘前已定) / 金额 亿元;
//     开收盘开关 (默认关) = 计入首根 (开盘集合竞价并入) / 末根 (收盘集合竞价), 图与右表同一份; 当日随新浪快照动);
//     右表 = 类别条 (指数 / 行业 / 概念 / 自选) + 过滤条 + 点行选中; 列 = 中文名 | 只数 | 总市值 | 换手 | 资金流入 (同 NAV) | 日轴各日净流入占当日成交额 | 档内合计净流入 | 合计占比 (按正负着色, 悬停格看金额)
//   页 CTA = 单标的多空 band 模型的滚动调参回测 (CtaService; 不看区间条): 标的 = 右表单选 (与 VAL 同一份选中: 篮子行 / 成分行), 价格 = all.bar 库合成落盘的 1 分钟价;
//     右栏 = 中栏 + 右表各半; 右表列 = 中文名 | 理想 | 夏普 (同 NAV 数值列);
//     中栏 = 设定 (优化目标与三组部件各选模型 / 滚动窗 / 费率 / 目标波动 / 不交易带; 改了即重算) + 光标处所用参数 + 全期指标 (策略 vs 买入持有);
//     左 = 上下两图共一条 x (有效根下标, 无隔夜空档; 刻度标日, 放大到几日内标时刻; 每像素取末根): 上 = 仓位色 (多 = 收盘线以上 / 空 = 以下, 越满越浓) + 收盘 + 上线 / 下线 (带间填色)
//     + 中线 + 分层线 (等差 / 等比) + 逐根时的买 / 卖标记; 下 = 策略 / 持有净值 (log); 悬停 = 光标 (中栏参数跟着走)
#pragma once

#include "gui/engine/services/PortfolioService.hpp"
#include "gui/engine/ui/PortfolioBoxes.hpp" // PortfolioUIState (当前组合)
#include "gui/quote/QuoteHub.hpp"
#include "gui/task_market/services/CtaService.hpp" // CtaSpec / CtaRequest (CTA 页)
#include "gui/task_market/ui/LinesView.hpp"        // 派生 → 滚动分位 / 末点分位 → 视图
#include "gui/task_market/ui/QuotesPanel.hpp"      // OverlayState / Source / Recent / Flow (NAV 日内栏 + 近况列 / FLOW 页)
#include "gui/util/Chart.hpp"                      // DayRange (区间条)
#include "gui/util/HoverSync.hpp"                  // 图 ↔ 右表悬停联动
#include "gui/util/Reorder.hpp"                    // Move (右表拖动调序)
#include "imgui.h"                                 // ImVec4 (右表格)
#include "shared/Basket.hpp"                       // kGroupPool / kBasketGroups

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace GUI::Market {

using namespace Engine;
using namespace Quote;

// 主图 fit 键: 主图画的全部内容 (视图 gen 已含派生 / 滚动分位 / 区间) + 页 + log + NAV 右表选中 + 上帧主图例各线显隐; 与上次 fit 的不同 → 本帧 fit
struct LinesFitKey {
  uint64_t view = 0; // 视图 gen
  bool nav = true, log = false;
  std::string rows;  // NAV 各行选中 (VAL 空)
  std::string shown; // 上帧主图例各线显隐
  bool operator==(const LinesFitKey &) const = default;
};

// 区间条: DayRange [d0, d1) (der.dates 下标; 把手吸附月初 / 末尾, VAL 主图框选任意日, 主图左上角档位 = 末日往前推一档; 日轴变 → 全段) + VAL 框选态
struct LinesRange : DayRange {
  XZoom zoom; // VAL 主图横向框选
};

// 行情页 (= Market 左栏 ASSET 之后的四个子页; 同一个右图例位; 类别条按页各一份; 页表 kPageDefs 见 LinesCommon.hpp)
enum LinesPage : int { kPageNav,
                       kPageVal,
                       kPageFlow,
                       kPageCta,
                       kPages };
// 右表行的选中方式 (页表按页指定; 同方式的页共用一份选中)
enum PickMode : int { kPickMany,  // 多选 (NAV / FLOW)
                      kPickOne }; // 单选, 再点 = 取消 (VAL / CTA)
inline constexpr int kPicks = 2;

// CTA 页: 设定 (进请求; 标的 = kPickOne 那一行) + 两图同一 x 视野 + 光标
struct CtaUIState {
  CtaSpec spec;
  CtaRequest sent;                 // 上次发出的 (gen 0 = 未发)
  bool editing = false;            // 数值框编辑中 (spec 未定, 不发请求)
  Quote::Ref fit;                  // x 视野所属的标的: 换标的即回全段, 只改设定保留缩放
  double x0 = 0, x1 = 1;           // 两图 x 视野 (有效根下标)
  bool fit_y = true;               // 价格轴跟随视野内数据 (框选放大后 = 用框的 y)
  bool boxing = false;             // 价格图上框选进行中
  XZoom zoom;                      // 净值图横向框选
  int cursor = -1;                 // 光标 (两图上可拖的竖线) 所在有效根 (−1 = 未放; 换标的重放)
  bool cloud_open = false;         // 参数点云展开 (光标只此时出现并生效, 否则按末根)
  bool nav_log = true;             // 净值图 y 轴: log / 绝对
  float yaw = 0.6f, pitch = 0.35f; // 参数点云视角 (弧度; 左键拖动旋转)
  bool cloud_fit = true;           // 参数点云着色: 拟合 / 实际
  uint32_t cloud_gen = 0;          // 参数点云坐标所属: 结果 (gen) + 日 (坐标随日)
  size_t cloud_day = SIZE_MAX;
  cta::Cloud cloud;
};

// 篮子组 (指数 / 行业 / 概念) 右表的行过滤 (各页共用, 按组各一份): 只数 / 总市值 [亿元] ∈ [lo, hi] (hi 0 = 不限), |资金流入| ≥ flow (%; 0 = 不限);
// 设了的条件缺值 (未到 / 无新浪行情日) 即不过; 选中 / 展开行恒列
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
// 当日四列 (NAV / FLOW 右表全四列, VAL 只数 / 总市值; 不随区间)
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
// 一页右表一组的格 (每帧只过滤 / 排序 / 画): 键 = 格的全部来源, 变了才整组重填 (过滤 / 排序 / 选中不在键里)
struct LinesSheetKey {
  int g = -1;                                                                   // 组
  uint64_t view = 0;                                                            // 视图 gen (含派生: 行 / NAV 数值列 / VAL 估值分位)
  std::shared_ptr<const Quote::Tape> baskets, stocks;                           // 两路历史在画的发布 (Market::OverlayState; 近况 / 只数 / 总市值 / 资金流历史日 / 成分)
  std::shared_ptr<const std::vector<Quote::Day>> baskets_day, stocks_day, cons; // 三份当日 (近况 / 当日四列 / 资金流当日)
  size_t infos = 0;                                                             // 基本面档案数 (首次载入)
  std::vector<misc::Ymd> days;                                                  // FLOW 日轴 (其余页空)
  bool ends = false;                                                            // FLOW 计入开收盘
  float font = 0.f;                                                             // 字号 (格宽)
  uint32_t ink = 0;                                                             // 字色 (主题)
  bool operator==(const LinesSheetKey &) const = default;
};
struct LinesSheet {
  size_t nc = 0, R = 0;                        // 名之后的列数 / 组内行数
  std::vector<LinesAt> at;                     // [表行] 行: 组内行 [0, R) + 展开行在本组时其下的成分行 (成分组前 n_cons 行)
  std::vector<SheetCell> cells;                // [表行][nc]
  std::vector<SheetToday> today;               // [表行] 当日 (过滤条 / 悬停)
  std::vector<std::vector<Market::Flow>> flow; // FLOW: [表行][日轴各日 + 合计] (悬停)
};

// 行选中 (LinesRow::id → 行情键; 默认空): 属于组合 of, 换组合 → 整份清空
struct LinesPick {
  std::string of;
  std::map<std::string, Quote::Ref> rows[kPicks]; // [PickMode] 选中行 (kPickOne 至多一个; 多选的个股至多 StockHistory::kKeep 只)
  std::string open;                               // 展开的篮子行 (至多一行, 各页同一份; 空 = 无)
  // 方式 m (kPicks = 全部) 选中的个股 (全局 id, 升序)
  std::vector<uint32_t> stocks(int m) const {
    std::vector<uint32_t> o;
    for (int k = 0; k < kPicks; ++k)
      if (m == kPicks || m == k)
        for (const auto &[id, ref] : rows[k])
          if (ref.is(Quote::kStock))
            o.push_back(ref.key);
    std::ranges::sort(o);
    o.erase(std::unique(o.begin(), o.end()), o.end());
    return o;
  }
};

// 行情页 (四页共用) 的 UI 态: 三级缓存 + 选中 / 区间 / 显隐 + 自选 + 篮子行情 + CTA 设定
struct LinesUIState {
  LinesStage<LinesDerKey, LinesDerived> der;
  LinesStage<LinesRollKey, LinesRolls> roll; // VAL 标的的滚动分位
  LinesStage<LinesPctKey, LinesPct> pct;     // VAL 表格各格的末点
  LinesStage<LinesViewKey, LinesView> view;
  LinesStage<LinesSheetKey, LinesSheet> sheet[kPages]; // [LinesPage] 右表格
  LinesRange range;
  LinesPick pick;
  HoverSync<std::string> hover;       // NAV 主图 / 日内栏 / FLOW 图 ↔ 右表的悬停联动 (键 = LinesRow::id)
  bool watch_loaded = false;          // 读过自选文件
  std::vector<std::string> watch;     // 自选键 (= 文件, 自选序; 改了即存)
  std::optional<GUI::Move> pool_move; // 右表指数组拖定的池表移动 (pool_order 的位置; 由 TaskMarket 交 move_pool)
  int band_years = kBandYearsDef;     // VAL 滚动窗 N (年; 右图例滑条)
  bool log_y = false;
  std::set<std::string> reveal;                                              // 待在主图例里回默认显隐的线 ID (行刚选中; 画到时消费; 理想列显, 可成交列隐)
  std::string shown_key;                                                     // 上帧主图各线显隐 (进 fit 键)
  LinesFitKey fit_key;                                                       // 上次 fit 的键 (BeginPlot 没起来不记: 下帧照 fit)
  int cat[kPages] = {kGroupPool, kGroupPool, kGroupPool, kGroupPool};        // [LinesPage] 右表类别条选中组 (RowGroup)
  BasketFilter filter[kBasketGroups] = {{}, {}, {.n_hi = 500, .flow = 5.f}}; // [BasketGroup] 默认: 指数 / 行业不滤, 概念 只数 ≤ 500 且 |流入| ≥ 5%
  bool intraday = true;                                                      // NAV 日内栏
  Market::OverlayState overlay;                                              // 篮子 + 选中的个股 + 成分当日 (日内栏 / FLOW / 近况 / 当日四列 / 成分)
  CtaUIState cta;
};

// 自选文件 output/config/market/watch.json (GUI 线程读写; NAV 右表星列 / 右键增删): { "watch": [ "sw01.加", "BK0493.加", "600512.SH", … ] }
//   条目 = 自选键 (LinesRow::watch), 文件序 = 自选序 (新加的排末尾, 右表自选组原序下可拖动调); 与组合无关, 全局一份
std::vector<std::string> LoadWatch();                  // 文件不存在 = 空; 格式不对 → assert
void SaveWatch(const std::vector<std::string> &watch); // 原子替换

// GUI 线程每帧: 首次读自选文件
void SyncLinesWatch(LinesUIState &ui);
// 行情页 page (LinesPage). pf = 当前组合 (名 / 解析结果); mk = 行情 (已起, ui.overlay 两路 Feed 本帧已 Sync; nullptr = AssetInfo 未建,
// 日内栏 / 指数行业近况 / 成分行当日空着); infos = 基本面档案 (成分行名 / 近况 / 估值); cta = CTA 页的服务.
// 返回值: 1 = Recompute (弃缓存重算), -1 = Cancel, 0 = 无
int RenderTabLines(int page, PortfolioService &svc, CtaService &cta, const PortfolioUIState &pf, LinesUIState &ui, double fixed_impact,
                   const Quote::QuoteHub *mk, const StockInfos &infos);

} // namespace GUI::Market