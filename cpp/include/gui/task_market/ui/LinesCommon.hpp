// Market 行情页 (NAV / VAL / FLOW / CTA) 的内部件 (TabLines / LinesPlot / LinesSheet / LinesCta 共用):
//   页表 (页的一切差异: 右表段 / 页件 / 布局 / 头 / 状态) / 行色 / 估值口表 / 选中 + 图与右表的入口
//   TabLines = 头 / 布局 / 三级缓存; LinesPlot = 主图 (NAV ↔ VAL) + 区间条; LinesSheet = 右表 (各页同一骨架); LinesCta = CTA 页的图 + 中栏
//   图与右表只读 ui 的缓存末级 (ui.der.val / ui.view.val), 写 ui 的交互态 (选中 / 展开 / 区间 / 显隐)
#pragma once

#include "gui/task_market/ui/TabLines.hpp"
#include "gui/util/Chart.hpp" // band_col / oklch / kStratCol / kHead / sheet_width
#include "gui/util/PoolTip.hpp"
#include "gui/util/Tip.hpp"
#include "imgui.h"

#include <cmath>
#include <limits>
#include <string>
#include <vector>

namespace GUI::Market {

inline constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

// 右表段 (各页由段拼成, 同一段各页同一份; LinesSheet.cpp): 近况 | 当日四列的前 arg 列 | NAV 数值列里位集 arg 的列 | 各口估值分位 | 资金流 (FLOW 日轴各日 + 合计)
enum SegKind : int { kSegNone = -1,
                     kSegRecent,
                     kSegToday,
                     kSegNum,
                     kSegVal,
                     kSegFlow };
struct SegDef {
  SegKind kind = kSegNone;
  int arg = 0;
};
inline constexpr int kSegsMax = 3, kNumAll = (1 << kNavNums) - 1;
// 页的左栏布局: 主图 + 区间条 (上方可叠日内栏) / 只一张叠加图 / CTA 两图 + 中栏
enum PageLayout : int { kLayoutPlot,
                        kLayoutOverlay,
                        kLayoutCta };
// 类别条旁的页件: 无 / 日内栏开关 / Band 滚动窗滑条
enum PageTool : int { kToolNone,
                      kToolIntraday,
                      kToolBandYears };
// 页的 worker (Tab 状态)
enum PageWork : int { kWorkPortfolio,
                      kWorkMinute,
                      kWorkCta };

// 页表 [LinesPage]: 页的一切差异只在这里
struct PageDef {
  const char *name, *tip; // 页名 / 页说明 (头行 (?) 悬停)
  int groups;             // 右表类别条的组数 (前 groups 个 RowGroup)
  PickMode pick;          // 行选中方式
  const char *table;      // 右表 ID
  const char *hint;       // 类别条旁选中方式 (点行 = 画 / 单选) 的悬停
  const char *name_tip;   // 右表名列说明
  SegDef segs[kSegsMax];  // 右表名列之后的段
  PageTool tool;          // 类别条旁的页件
  PageLayout layout;      // 左栏布局
  OverlayKind overlay;    // 叠加图: kLayoutPlot = 日内栏, kLayoutOverlay = 整张
  bool log, folio;        // 头: log 开关 / 组合 (名 + 失效 / 失败条目)
  PageWork work, rest;    // Tab 状态: work 在忙 → 它, 否则 rest; 页发 work 的请求
};
inline constexpr PageDef kPageDefs[kPages] = {
    {"NAV",
     "NAV = 资金曲线 (Net Asset Value, 净值)\n"
     "• 橙线 = 本组合 G20 多头: 各因子独立回测 (分批 1/h̄, 全日平均满仓; 能成交才成交) 的逐行增量等权聚合\n"
     "• 池指数线 = 各子池指数 (全A.等 / 全A.加 起头, 按池参数的权重); 行业线 = 申万一级 / 概念线 = 东财题材概念 (全A 内切, 总市值加权)\n"
     "• 单因子线 = 各自的 G20 多头净值\n"
     "• 每条成对: 可成交 (篮子 = 标签指数, 收盘封板不建仓 / 平仓顺延; 因子 / 策略 = 扣换手冲击) + 理想 (篮子 = 后复权收盘价指数; 因子 / 策略 = 无冲击; 同色淡线)\n"
     "• 成分股 (篮子行 ▸ 展开, 权降序): 后复权收盘, 两条同一条; 与篮子行同一套 (点选中 = 画线 + 日内叠加; 收起后选中的照画)\n"
     "• 默认不画 (空图); 右侧类别条选一组 (指数 / 行业 / 概念 / 自选 / 因子 / 策略), 点行选中 = 画该行两条, 默认只显理想; 显隐变了自动重新 fit\n"
     "• 选中的指数 / 行业 / 概念 / 成分股的两条线都画到分钟 (全史 1 分钟价, 与 CTA 同一份库; 各日以收盘对齐日线, 年化 / 夏普照日末算); 首次选中时合成库\n"
     "• 主图左上图例矩阵 (同 VAL): 行 = 选中行, 列 = 可成交 / 夏普 / 理想 / 夏普 (区间年化 / 夏普) / 年换手; 点色块开关一条, 点名 / 列头开关整行 / 整列\n"
     "• 自选 = 星列 / 右键加的篮子与成分股 (全局一份, 各页同一份)\n"
     "• 日内 (右侧开关, 默认开): 主图上方画选中的指数 / 行业 / 概念 / 成分股的日内行情 (多行 = 同主图线色叠加; 一行 = 蜡烛 + 现价十字)\n"
     "• 日内栏 0 轴上 = 每分钟净流入半透明红绿柱 (同 FLOW: 分钟涨跌法, 篮子 = 成分金额之和); 只选一行看它, 否则看全A.等;\n"
     "  历史日 = 分钟 K, 当日 = 新浪快照合成 (当前分钟随快照动, 走完即与分钟 K 同值); 柱高自成比例, 首末两根 (集合竞价) 截短\n"
     "• 只显示一条线: 视野内末点画现价十字 (同日内栏; 右壁标区间涨跌, 底壁标日期)\n"
     "• 悬停联动 (同 Beta → Style): 主图 / 日内栏上悬停最近一条线 (或图例矩阵一行) = 右表该行底色亮并滚到居中 (悬停止回原位); 悬停右表一行 = 两图上该行的线白粗线\n"
     "• 底部区间条选日段 (主图左上角档位 1周 / 1月 / 1年 / 10年 = 末日往前推该段, 区间条同步): 各线在区间起点归一 = 1, 年化按区间复利",
     kGroupKid,
     kPickMany,
     "##pf_nav_tbl",
     "点行 = 选中 / 取消, 选中 = 主图画该行两条线, 主图例默认只显理想列, 可成交列点图例显 (显隐变了自动重新 fit); 默认不选; 选中换组保留\n"
     "• 指数 = 各子池指数 (全A.等 / 全A.加 起头, 按池参数的权重); 行业 = 申万一级 (全A 内切, 总市值加权); 概念 = 东财题材概念 (全A 内切, 总市值加权):\n"
     "    ◦ 可成交 = 标签指数 (收盘封板不建仓 / 平仓顺延, 与因子回测同口径)\n"
     "    ◦ 理想 = 价格指数 (Market 篮子历史同一份: 成员 / 权重取前一日收盘, 点位按前收合成, 不看可成交, 同中证 / 万得口径); 两者之差 = 可成交拖累\n"
     "• 因子 = 单因子各自的 G20 多头净值: 可成交列 = 有冲击 (它自己的冲击段: Lob 最小金额段 / Bar 固定冲击), 理想列 = 无冲击\n"
     "• 策略 = 本组合 G20 多头: 可成交列 = 有冲击, 理想列 = 无冲击\n"
     "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
     "• 自选 = 首列星 (或名格右键) 加入的篮子行 / 成分行: 全局一份, 各页同一份, 与原行同一份选中; 名次在自选之间\n"
     "• 指数 / 行业 / 概念表上过滤条 (各页同一份): 按只数 / 总市值 / |资金流入| 筛本组的行, 已画 / 展开的行恒列\n"
     "• 指数 / 行业 / 概念行名前 ▸ = 展开成分股 (至多一行, 各页同一份): 该行钉在表头下, 成分接在其下 (列同篮子行, 不能算的 -);\n"
     "  成分行 = 后复权收盘 (两条同一条), 与组内行同一套: 点 = 选中 (主图 + 日内栏 / FLOW 图; 至多 64 只), 收起后选中的照画\n"
     "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)\n"
     "• 原序下子池 / 自选行左键 / 中键拖动调序 (子池 = 改池表序 pools.json, 即进程内重启; 全A 两行钉首)",
     "名 = 行名\n"
     "• 算法: 篮子 = 切片名 + .等 (等权) / .加 (总市值加权); 因子 / 策略 = 中文名; 成分行 = 简称\n"
     "• 数据: 切片 = 池 (全A 起头) / 申万一级行业 / 东财题材概念\n"
     "• 显示: 字色 = 主图线色 (四组一起按 max(成员重合, 剔全A.加的残差相关) 定色相: 色差 ∝ 不相似; 没算完全灰); 点本列表头 = 按颜色排 (相似的挨着; 没算完按名); 超长截断补 …, 悬停看全名\n"
     "• 成分行: 篮子名前 ▸ 展开 (至多一行), 该行钉在表头下, 其下缩进列出成分 (默认权降序, 点表头在本篮子内排, 名列按简称);\n"
     "  与组内行同一套: 点 = 选中 (主图画线 + 日内叠加), 字色 = 线色\n"
     "• 适用: 全部组; 成分行只指数 / 行业 / 概念 / 自选 (篮子行展开)",
     {{kSegRecent}, {kSegToday, kTodays}, {kSegNum, kNumAll}},
     kToolIntraday,
     kLayoutPlot,
     kOverlayIntra,
     true,
     true,
     kWorkMinute,
     kWorkPortfolio},
    {"VAL",
     "VAL = 估值 (Valuation)\n"
     "• 右侧单选一个池 / 申万一级行业 / 概念 / 自选 / 成分股 (▸ 展开行): 主图 = 它的指数 (成分股 = 后复权收盘) + 估值 Band\n"
     "  (行业 / 概念 = 总市值加权, 子池 (含全A) = 池参数的权重; 成分股 = 本只); 指数同 NAV 画到分钟, Band 落在各日收盘点上\n"
     "• EP 盈利收益率 (1/PE) / BP 账面市值比 (1/PB) / SP 营收市值比 (1/PS) / CP 现金流收益率 (1/PCF) / DY 股息率\n"
     "  (收益率口径, 与指数同权重: 加权 = 整体法, 等权 = 个股均值, 成分股 = 本只分子 / 总市值; 高 = 便宜, 亏损照计为负, 过 0 连续)\n"
     "• 主图左上图例矩阵: 行 = 口, 列 = 当前收益率 / 倍数 / N 年分位 / Band / 等值线; 点色块开关, 点行头 / 列头开关整行 / 整列\n"
     "• Band = 把过去 N 年的估值分位换算成价格线: 指数落在哪档 = 当前估值在历史里的位置 (绿 = 便宜, 红 = 贵)\n"
     "• 等值线 = 绝对估值: 倍数 (PE / PB / PS / PCF / P/D = 1 / 收益率) 的价格线, 档 = 该标的全史倍数区间 (两头各剔 2% 极端) 20 档等分;\n"
     "  默认关, 悬停一条 = 高亮 + 看该档倍数 / 当日倍数 / 指数到该线还差多少\n"
     "• 线尾刻度 = 指数末点 +10% / -5% / -10% / -20% / -30% 的点位: 对照 Band 线看还要涨跌多少到哪档; 末点另画现价十字 (同 NAV)\n"
     "• y 只贴合指数 (区间内最低点 − 20% ~ 最高点 + 20%), 超出的 Band 不撑图\n"
     "• 主图左键横向框选 = 放大到该段 (区间条同步), 双击 = 回全段; 区间 (含左上角档位) / log 与 NAV 页共用",
     kGroupFactor,
     kPickOne,
     "##pf_val_tbl",
     "类别条选一组 (指数 = 各池, 行业 = 申万一级, 概念 = 东财题材概念), 点行 / 成分行选一个: 主图画它的价格 + EP / BP / SP / CP / DY Band; 选中换组保留\n"
     "• 价格: 行业 / 概念 = 总市值加权指数 (全A 内切); 子池 (含全A) = 池参数的权重 (同 NAV 池线); 成分股 = 后复权收盘\n"
     "• 行业成员 = 全A 成分按当日适用 (盘前已知) 的 SW2021 一级行业 (ind_l1) 切\n"
     "• 概念成员 = 全A 成分 ∩ 最近一份东财概念快照的成分 (全史同一份)\n"
     "• 估值与价格同权重: 个股收益率 = 分子 / 总市值; 加权 = 整体法 Σ分子 / Σ总市值, 等权 = 成分个股收益率的算术平均 (= 等权组合每元的分子); 成分股 = 本只\n"
     "  分子: EP = 归母净利TTM, BP = 归母普通股权益MRQ (扣永续债 / 优先股), SP = 营业总收入TTM, CP = 经营现金流TTM, DY = 近4季报告期税前分红 (亏损 / 负权益 / 负现金流照计为负)\n"
     "• 成分 = 当日持仓 (同池指数: 上一交易日收盘建仓的池); 市值 = 收盘价 × 当日适用总股本, 财务取当日适用 (无前视)\n"
     "• 估值分位 = 当日收益率在过去 N 年 (含当日; 类别条旁滑条) 窗内的加权分位 (窗尾降权, 见滑条提示), 收益率越低越贵 (高 = 贵; ≤ 0 排在最贵端), 逐日滚动; 估值史不满 1 年不画\n"
     "• Band = 价格 × 当日收益率 / q, q = 窗内 5/10/25/50/75/90/95% 贵度分位的收益率 (与倍数口径的 Band 同一条线);\n"
     "  当日 ≤ 0 或窗内收益率 > 0 的日子不足 80% 整段不画, 某档 q ≤ 0 该档线断开 (价格无解); 分位照常. 最便宜档以下绿 → 最贵档以上红\n"
     "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
     "• 表上过滤条 (各页同一份): 按只数 / 总市值 / |资金流入| 筛本组的行, 选中 / 展开行恒列\n"
     "• 行名前 ▸ = 展开成分股 (至多一行, 各页同一份; 该行钉在表头下): 各口 = 该股本只的估值分位 (底色 = 成分之间名次); 点成分行 = 选该股\n"
     "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)\n"
     "• 原序下子池 / 自选行左键 / 中键拖动调序 (子池 = 改池表序 pools.json, 即进程内重启; 全A 两行钉首)",
     "名 = 篮子名\n"
     "• 算法: 切片名 + .等 (等权) / .加 (总市值加权)\n"
     "• 数据: 切片 = 池 (全A 起头) / 申万一级行业 / 东财题材概念\n"
     "• 显示: 超长截断补 …, 悬停看全名; 选中行高亮 (主图画它)\n"
     "• 成分行: 篮子名前 ▸ 展开, 缩进列出成分 (同 NAV 页); 与篮子行同一个单选\n"
     "• 适用: 指数 / 行业 / 概念 / 自选",
     {{kSegToday, kTodayTurn}, {kSegNum, kNumAll}, {kSegVal}},
     kToolBandYears,
     kLayoutPlot,
     kOverlayNone,
     true,
     false,
     kWorkMinute,
     kWorkPortfolio},
    {"FLOW",
     "FLOW = 资金流 (Money Flow): 指数 / 行业 / 概念 / 自选的日内净流入, 当日随新浪快照实时动\n"
     "• 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟; 成分之和不加权, 分不出大单\n"
     "• 左图 = 右表选中行 (与 NAV 页同一份选中) 自日轴首日开盘起逐分钟累计的净流入, 跨日接续; 档位 日内 / 2日 … 5日 叠在图左上角\n"
     "• y 口径 (图左上角切): 占比 = 累计净流入 / 近 5 日日均成交额 (默认; 分母盘前已定, 开盘即稳, 跨篮子可比) / 金额 = 累计净流入 (亿元)\n"
     "• 开收盘 (图左上角开关, 默认关): 计入首根 09:31 (开盘集合竞价并入) / 末根 15:00 (收盘集合竞价); 关 = 两根的额与净流入都不计 (图与右表同一开关)\n"
     "• 右表: 日轴各日的净流入占当日成交额 + 档内合计; 过滤条各页同一份\n"
     "• 悬停联动 (同 NAV): 图上悬停最近一条线 = 右表该行底色亮并滚到居中 (悬停止回原位); 悬停右表一行 = 图上该行的线白粗线\n"
     "• 历史日 = all.bar 库 L_1min (其后至基本面末日 = bar1m) 合成, 当日 = 新浪快照合成; 不看区间条",
     kGroupFactor,
     kPickMany,
     "##pf_flow_tbl",
     "点行 = 选中 / 取消, 选中 = 左图画该篮子的累计净流入 (与 NAV 页同一份选中: 这里选的 NAV 主图 / 日内栏也画); 选中换组保留\n"
     "• 指数 = 各子池 (全A.等 / 全A.加 起头); 行业 = 申万一级; 概念 = 东财题材概念 (全A 内切)\n"
     "• 各列怎么算: 悬停表头 (算法 / 数据 / 显示 / 适用)\n"
     "• 表上过滤条 (各页同一份): 按只数 / 总市值 / |当日资金流入| 筛本组的行, 已画 / 展开的行恒列\n"
     "• 行名前 ▸ = 展开成分股 (至多一行, 各页同一份; 该行钉在表头下): 点成分行 = 选中, 左图画它 (个股历史 + 新浪当日, 跨日接续), 表里各日随之齐; 未选中的资金流只当日\n"
     "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序; 名列按颜色排 (相似的挨着)\n"
     "• 原序下子池 / 自选行左键 / 中键拖动调序 (子池 = 改池表序 pools.json, 即进程内重启; 全A 两行钉首)",
     "名 = 篮子名\n"
     "• 算法: 切片名 + .等 (等权) / .加 (总市值加权; 子池的权重是池参数的一维); 资金流不分权重, 只差权重的池同成分即同值\n"
     "• 数据: 切片 = 池 (全A 起头) / 申万一级行业 / 东财题材概念\n"
     "• 显示: 字色 = 线色 (同 NAV); 超长截断补 …, 悬停看全名\n"
     "• 成分行: 篮子名前 ▸ 展开, 缩进列出成分 (同 NAV 页); 与篮子行同一份选中\n"
     "• 适用: 指数 / 行业 / 概念 / 自选",
     {{kSegToday, kTodays}, {kSegFlow}},
     kToolNone,
     kLayoutOverlay,
     kOverlayFlow,
     false,
     false,
     kWorkPortfolio,
     kWorkPortfolio},
    {"CTA",
     "CTA = 单标的 band 择时 (方向 long / short / longshort), 参数滚动调优\n"
     "• 右侧单选一个指数 / 行业 / 概念 / 自选 / 成分股 (与 VAL 同一份选中); 价格 = all.bar 库合成落盘的 1 分钟价 (篮子 = 同 NAV 日内栏口径的点位, 个股 = 后复权价; 首次打开一趟备齐)\n"
     "• 模型 = 优化目标 (split) + 三组部件各选一个: 中线 (ema / kama / mid) · 带 (vol / atr / dev / rng: 上线 / 下线 = 中线 ± k · 半宽) · 分层 (arith / geo)\n"
     "• 分层 = 跟随网格: 中线 = 空仓, 中线到上线 / 下线各 N 层, 每越过一层线开 / 平一份 (浮盈加仓, 回撤减仓), 到上线满多 / 到下线满空 (方向之外的半边空仓); 层距 等差 = 同一价差 / 等比 = 同一涨幅\n"
     "• 只选模型, 参数滚动选出: 每日以此前至多 W 日 (不足 W 日 = 自首日起, expanding) 参数网格 (对数格点, 间距不小于参数的基本单位; 中心周围的配置数可配, 默认 50, 各参数轮流分格点) 的目标值拟岭回归曲面, 取最高点用到当日, 最高点在网格边上则以它为中心移网格再拟; 首日 / 带都未就绪 = 买入持有 (与基准同口径)\n"
     "• 仓位按本根收盘调到, 成本 = 佣金 + 印花税 + 冲击 + 震荡 + 跳空; 可选目标年化波动定仓 / 不交易带\n"
     "• 中栏 = 设定 + 参数点云 + 光标处所用参数 + 全期指标 (策略 vs 买入持有); 左图 上 = 价格与带 (底色 = 仓位, 越满越浓), 下 = 复利净值 + 叠在策略净值之上的各项成本带 (悬停看是哪项); 不看区间条\n"
     "• 左键框选放大 / 右键拖动平移 / 滚轮缩放 / 双击回全段; 换标的回全段, 只改设定保留缩放\n"
     "• 光标 = 橙色竖线: 参数点云展开时才出现 (初放视野正中), 左键拖动, 松手停住; 中栏参数与参数点云都看光标所在日 (收起 = 末根)",
     kGroupFactor,
     kPickOne,
     "##pf_cta_tbl",
     "类别条选一组 (指数 = 各池, 行业 = 申万一级, 概念 = 东财题材概念), 点行 / 成分行选一个 = 左图回测它 (与 VAL 页同一份选中); 再点 = 取消\n"
     "• 篮子价格 = all.bar 库逐日合成 (成员 / 权重 / 基准取前一日收盘, 同 Market 篮子历史); 成分股价格 = 库存原价 × 后复权因子\n"
     "• 两者首次打开本页一趟合成, 存在库目录 cache/minute/ 下 (与 NAV 分钟线同一份), 之后只补新日\n"
     "• 理想 / 夏普 = NAV 页同名列 (随 NAV 区间), 供挑标的参考; 悬停表头看算法\n"
     "• 表上过滤条 (各页同一份): 按只数 / 总市值 / |资金流入| 筛本组的行, 选中 / 展开行恒列\n"
     "• 列宽按内容最紧, 超出栏宽横滚 (名列不动); 名超长截断补 …, 悬停中文名看全名; 点表头排序 (组内): 升序 → 降序 → 原序\n"
     "• 原序下子池 / 自选行左键 / 中键拖动调序 (子池 = 改池表序 pools.json, 即进程内重启; 全A 两行钉首)",
     "名 = 篮子名\n"
     "• 算法: 切片名 + .等 (等权) / .加 (总市值加权)\n"
     "• 数据: 切片 = 池 (全A 起头) / 申万一级行业 / 东财题材概念; 回测价格 = all.bar 库合成的 1 分钟点位 (同 Market 篮子历史口径)\n"
     "• 显示: 超长截断补 …, 悬停看全名; 选中行高亮 (左图回测它)\n"
     "• 成分行: 篮子名前 ▸ 展开, 缩进列出成分 (同 NAV 页); 点成分行 = 回测该股 (后复权 1 分钟价)\n"
     "• 适用: 指数 / 行业 / 概念 / 自选",
     {{kSegNum, (1 << kNumIdeal) | (1 << kNumSharpe)}},
     kToolNone,
     kLayoutCta,
     kOverlayNone,
     false,
     false,
     kWorkCta,
     kWorkCta},
};

// 线 ID 后缀 [PairLine]: 线的 ImGui ID = 行 id + 后缀 (主图例显隐 / 行刚选中回默认显隐同一份)
inline constexpr const char *kPairId[kPairLines] = {"", "_ideal"};
inline std::string line_id(const LinesRow &r, int l) { return r.id + kPairId[l]; }

// VAL 估值口表 [PoolVal]: 名 / 倍数名 (1 / 收益率, 等值线) / 百分比小数位 (五口同为收益率, 显示 %)
struct ValMeta {
  const char *name, *inv;
  int prec;
};
inline constexpr ValMeta kVal[kPoolVals] = {{"EP", "PE", 2}, {"BP", "PB", 1}, {"SP", "PS", 1}, {"CP", "PCF", 2}, {"DY", "P/D", 2}};
// 组 g 行 r 线 l 的色 (主图 / 右表 / 日内栏 / FLOW 图同一份): 强调组 (kGroupDefs::accent) = kStratCol, 其余 = 行色相 (LinesRow::hue) 的定亮定艳色, 未定色 = 同亮度灰; 理想列 = 同色淡
inline constexpr float kIdealAlpha = 0.5f;
inline ImVec4 row_color(const LinesDerived &d, int g, size_t r, int l) {
  const float hue = d.rows[g][r].hue;
  ImVec4 c = kGroupDefs[g].accent ? kStratCol : std::isnan(hue) ? oklch(kRowL, 0.0f, 0.0f)
                                                                : oklch(kRowL, kRowC, hue);
  c.w = l == kIdeal ? kIdealAlpha : 1.0f;
  return c;
}
inline ImVec4 row_color(const LinesDerived &d, const LinesAt &a, int l) { return row_color(d, a.g, a.r, l); }

// 页 page 的行选中 (按页表的选中方式; NAV / FLOW 同一份)
inline bool picked(const LinesState &ui, int page, const LinesRow &r) { return ui.pick.rows[kPageDefs[page].pick].contains(r.id); }

// 行的悬停 (右表名格 / 主图例同一份): 子池篮子 = hover 起头 + 逐维拆解, 其余 = hover
inline void row_tip(const LinesRow &r) {
  if (r.pool != kAllPool)
    pool_tip(pool_def(r.pool).spec, r.hover);
  else
    tip("%s", r.hover.c_str());
}

// ---- 图 (LinesPlot.cpp) ----
// 默认区间 = 主图左上角档位 1月 (日轴变 / Clear 回它); 日轴空 → 空段
LinesRange DefaultLinesRange(const std::vector<misc::Ymd> &dates);
// 主图 (size 贴边占满): nav → NAV 资金曲线 (选中行), 否则 VAL 标的 pin 的理想线 + 估值 band; 内容 / 显隐变 → 重新 fit; 线尾小字 / 刻度 / 现价十字
void RenderLinesPlot(LinesState &ui, const LinesAt &pin, bool nav, const ImVec2 &size);
// 区间条 (size): 全段策略线 (nav) / VAL 标的理想线上拖选 ui.range
void RenderLinesRange(LinesState &ui, const LinesAt &pin, bool nav, const ImVec2 &size);

// ---- CTA 页 (LinesCta.cpp) ----
// 标的 (kPickOne) / 设定变 → 发请求; 画两图 (plots_size) + 其右中栏 (宽 panel_w, 同高: 设定 + 光标处参数 + 指标); name = 标的行名 (nullptr = 未选)
void RenderLinesCta(CtaService &svc, LinesState &ui, const char *name, const ImVec2 &plots_size, float panel_w);

// ---- 右表 (LinesSheet.cpp) ----
// 页 page 的右图例 (各页同一条, 按页表): 类别条 + 页件 + 表 (名 + 页的段); mk = 行情 (资金流段的日轴; nullptr = 未起); infos = 成分行的近况 / 当日
void RenderLinesSheet(LinesState &ui, int page, const Quote::QuoteHub *mk, const StockInfos &infos);
// 成分行的当日 (ui.overlay.cons, Today): 成分集 / 个股当日版本变了才重取
void SyncLinesCons(LinesState &ui, const Quote::QuoteService &q);

} // namespace GUI::Market