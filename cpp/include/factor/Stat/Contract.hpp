#pragma once

// =============================================================================
// Stat: 因子评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 无流式后端 —— 评估只在挖掘侧)
// =============================================================================
//   输入  因子平面 x [T][A] (SoA: float + 有效位) + 口径 Frame (见下) + H 组常驻标签 (每组一个持有期键 hold, 见下【持有期键】):
//           v[kLegs] 做多 lv / 做空 sv 价格收益 (features 层 LabelLob, 毛口径; fp16 位, 与落盘同格式, 无精度损失. 冲击不进标签: 超额对每 t 去均值,
//                    成本进来只剩截面相对项 —— 消费端在二级上按组池化成本扣). 分钟型 entry / exit 同为盘口中间价 (行 m 的 exit = 行 m+h 的
//                    entry), 相邻行精确伸缩: 留仓成员逐期标签之和 = 连续持有收益, 【换手成本】的"留仓不付"才成立; 价差只在成本列
//           m        标签有效位 (long / short 同一快照, 共用一张)
//           ry       预处理: 做多标签逐行截面 rank (r16), 与因子无关, 常驻期算一次 (prep_label)
//
//   【腿 Leg】kLong = 顶组 (G20) · 做多标签 lv · 建仓买入冲击; kShort = 底组 (G1) · 做空标签 sv · 建仓卖出冲击.
//     两腿成对的量 (Label::v / Row::mkt / LegSum / Cost::imp / CostAcc::leg) 一律按 [kLegs] 下标, 不各写一份; 腿 → 组号 leg_group.
//   输出  两级:
//     一级 Row[H][T]  每 (持有期, 时刻) 一行沿 A 轴归约的统计 (两后端各算, 对拍在此级)
//     二级 HoldStat   每持有期 沿 T 轴汇总成十几个标量 (Accum 一遍可合并累加器 → finish; 主机 double, 两后端共用同一份代码)
//
//   【口径 Frame】alpha 因子分两种坐标系, 由因子表达式的根算子决定 (Expr.hpp root_frame). 标签**统一超额语义**:
//     e = lv − mean_J lv (做空侧 sv − mean_J sv), 每 t 的共同分量 (市场水平 / 市场择时) 被扔掉, 评的都是"相对市场";
//     一套代码只在两处分叉:
//     CS  截面: 根是截面归一算子 (CsRank / CsNormRank / CsZ). 每 t 沿 A 轴 rank x (排序) → 20 组 = 截面分位组;
//         因子值 = 仓位 (每 t 居中 → 多空对冲, β 自然隔离; 超额对 rank 类统计是平移不变的). 大块并列会把相邻组挤空.
//     TS  时序: 根是 TsRankRoll(d = ts_norm_d(seg), 整数个交易日), 输出已是每资产对自身滚动历史的分位 ∈ [0,1]
//         (整数天窗跨日 deseason). 不排序, 直接量化 r16 = round(x · kRankMax) (r16_quant) → 20 组 = 自身历史分位组;
//         评的是"个股处于自身高位时是否跑赢市场", β 由超额隔离, 样本间不再因共同分量相关.
//     两口径之后逐字共用: grp_of / rIC / 超额 / 持仓口径 / ls / mkt / rAC / 二级汇总. 组允许空 (那一刻没有资产在该组), 空组 / 空腿收益 0.
//
//   【持仓口径】每组 (含两腿) 资金固定为 1, 满仓 cap = Labels::cap 只 (调用方给 n_full = 池内平均有效资产数 / kGroups, 与因子无关), 每只 1/cap:
//     触发 n 只 → 持仓 min(n, cap) / cap, 超过 cap 满仓在 n 只里均分 → 组 / 腿收益 = Σe / max(n, cap) (book_den); 未触发 → 0.
//     触发不足 (稀疏 / 并列挤空) 与集中度直接进收益, 不另设活跃度项.
//
//   【rank 口径】精确并列均秩 (排序, 不用桶近似 —— 原始因子重尾时一个离群点会把整截面压进同一桶, IC 归零):
//     r16 = round(pct · kRankMax) ∈ [0, kRankMax], 全整数公式 (r16_of), 两后端逐位一致; kRankNone = 无效.
//     行有效样本 n < 2 或全并列 (spread 为假) → 整行 kRankNone.
//     rank(x) 只看 x 的有效集, rank(y) 只看 y 的有效集 (与因子无关才能常驻); 两者的联合集 J 上算统计.
//     TS 的 r16_quant 走 double (float × 65534 在 double 内精确, 与 FMA 无关), 两后端逐位一致; x ∉ [0,1] 断言.
//
//   【一级 Row (每 h, t)】J = {a : rx ≠ None ∧ ry ≠ None}; e = 超额标签 (lv − mkt[kLong]; 空腿 sv − mkt[kShort])
//     ric    rank IC = Spearman = Pearson(r16_x, r16_y) over J. 五个和都是整数 (pair_add → pearson_int), 两后端逐位一致.
//     grp[k] 第 k 组 (k = grp_of(r16_x), 等分 kGroups 组) 做多超额 e 的**持仓收益** (Σe / max(cnt, cap), 代数上 Σlv − cnt·mkt 再除, 一遍 A 轴);
//            cnt[k] = 组样本数. 二级沿 t 取均值 (Σ / n_all, 见【二级】).
//     ls     顶底多空 LS(tp-dn) = 两腿持仓收益之和 = 多腿 (顶组) 做多超额 + 空腿 (底组) 做空超额, 各按【持仓口径】—— 相对市场; 腿空 → 该项 0.
//            二级 HoldStat 的 LS 系 (Moments ls / ls_annual / sharpe_of / beta) 全部基于它. 无信号时 ≈ 0 (原值口径会 ≈ −市场往返成本).
//     mkt    [kLegs] J 上做多 / 做空标签原值均值 = 两腿的超额基准 (grp 与二级回归 beta 用 mkt[kLong]).
//     ok     标签侧有效: 非段末尾部 (见下) ∧ ric 可算 (n ≥ 2, 两侧方差 > 0). 组可空 (不再整行作废).
//     rac    rank AC: Pearson(r16_x(t), r16_x(t−h)) over 两行都有效的资产 —— lag = h 行 (hold_rows) 才对应"一个持有期换多少仓".
//     ok_rac t ≥ h ∧ n ≥ 2 ∧ 两侧方差 > 0. 与标签无关, 段末尾部也算.
//     段末尾部 (分钟型 / 收盘型): 行 t 的标签 = 该行末分钟 m = minute(t_seg) (Contract【层几何】) 的 L_1min 标签, entry 锚点 = 分钟 m+1 起始;
//       最后一个可成交锚点是收盘集合竞价 (14:57 起, 段末 kCloseAuction 分钟) 前一分钟的起始 = L_1min 行 kLastAnchorRow 的锚点.
//       分钟型 exit = 分钟 m+n 的锚点, m + n 越过它就"持有到收盘", 持有期缩短 → 掩掉 minute(t_seg) + n > kLastAnchorRow 的行 (tail_masked);
//       收盘型 m 越过它 = 收盘价建仓即平 (标签恒 0) → 同式 n 取 0. 日级型 (T+N 开盘 / 收盘) exit 在次日之后, 无尾部 (该行按收盘价建仓).
//       尾部行不开仓: 换手成本 / 净值同一判据.
//     ok / ok_rac 为假时对应字段全 0 (与 Contract 的 valid=false ⇒ v=0 同约).
//     可成交 / 平仓顺延全在标签里 (features LabelHold): 建仓不可两向成交的格标签 NaN → 有效位 0, 不进 J; 本契约不另读可成交.
//
//   【持有期键 hold】int, 与 features 层 LabelHold 的列名同源 (lb_<side>_<name>); 键的单位是**分钟**, 与因子所在层无关.
//     层上合法 (hold_ok): 分钟型 n 是行长 N 的整数倍且首行 minute(0) + n ≤ kLastAnchorRow (尾部不吞整段); close 要求 S > 1
//       (日层的行就在收盘, 即建即平); 日级型各层皆可.
//     折算 / lag / 年化用的有效行数 h = hold_rows(hold, geo): 分钟型 = n / N; close 与 T+1 = S (同日各行共一个 exit →
//     每日一个独立样本); T+N = N · S (相邻 N 日的窗口重叠). L_1min 上 = 分钟数.
//
//   【协议表 1: 持有期 → 开仓 / 清仓 / 价】(features 标签 / Stat / 净值 / UI hold_desc 同一份; 改口径先改这里, 标签 kLabelSemver +1)
//     价点: E    = 行末分钟价点. Lob 层 (min) = 锚点 (分钟末) + LABEL_DELAY_SECONDS 的可成交盘口中间价;
//                  Bar 层 (5min..60min) = 之后首个有成交连续竞价分钟的 VWAP; 锚点后再无连续竞价 (≥ 14:57) → C(D)
//           C(D) = D 日收盘集合竞价撮合价 (末笔成交);  O(D) = D 日 09:25 开盘撮合价 (无撮合 → 首个有成交连续竞价 bar 的 open)
//     | 显示 (hold_name) | 列名 token | 键            | 适用层            | 开仓                     | 清仓                       | h (hold_rows) |
//     |------------------|------------|---------------|-------------------|--------------------------|----------------------------|---------------|
//     | 1m               | 1m         | 1             | min               | C(行) @ E                | C(行)+1m @ E               | n / N         |
//     | 5m 15m 30m 60m   | <n>m       | n             | 行长 ≤ n 的分钟层 | C(行) @ E                | C(行)+n @ E (过 14:57 掩)  | n / N         |
//     | close            | close      | kHoldClose    | 非 day            | C(行) @ E                | C(D) @ 收盘撮合            | S             |
//     | T+1O             | t1o        | hold_open(1)  | 全部              | C(行) @ E; day 层 C(D)   | O(D+1) @ 开盘撮合          | S             |
//     | T+1C T+2C T+3C   | t<N>c      | hold_close(N) | 全部              | C(行) @ E; day 层 C(D)   | C(D+N) @ 收盘撮合          | N · S         |
//
//   【协议表 2: 开关仓策略】(可成交 / 作废 / 顺延全在 features 标签里定, 本契约与全部消费端只读标签, 不另读可成交列)
//     | 环节       | 规则                                                                                                      | 落在          |
//     |------------|-----------------------------------------------------------------------------------------------------------|---------------|
//     | 可成交判据 | Lob 盘口: 可买 = 卖一侧有挂单, 可卖 = 买一侧有挂单; Bar 分钟 (Tradable): 可买 = low < 涨停 − ε,           | LabelHold     |
//     |            |   可卖 = high > 跌停 + ε; 开盘 / 收盘撮合价: 价 < 涨停 − ε 可买, 价 > 跌停 + ε 可卖; 涨跌停 NaN → 可成交  |   LabelLob/Bar|
//     | 开仓       | E 价点非两向可成交 → 该行全部持有期两腿 NaN (作废: 不进截面, 不建仓); 不顺延开仓                          | 标签          |
//     | 平仓       | 名义价点该腿不可成交 (多腿卖不出 / 空腿买不回) 或名义日停牌 → 顺延到之后首个该腿可成交价点                | 标签          |
//     |            |   (分钟级 / 日级同一规则, 可跨日); 行所在日起 8 个交易日 (PEND_DAYS − 1) 仍未平 → 最近收盘价结清          |               |
//     | 尾部       | 分钟型 / 收盘型 minute(t) + n > kLastAnchorRow 的行不开仓 (tail_masked)                                   | Stat          |
//     | 仓位       | 每行一批, 资金 1/h, 批内每只 1 / max(只数, cap) (【持仓口径】); 目标仓位 w_t = (1/h) Σ_{k<h} s_{t−k}      | Stat / 净值   |
//     | 换手       | Δw 逐格 (微调照计); 只在行 t 建仓与行 t−h 平仓同一价点时交接 (hold_nets: 只交易差额), 否则整建整平        | 【换手成本】  |
//     | 成本       | 增仓 Δw⁺ × 建仓冲击 (Lob 金额段, 买入封顶 max_impact) + 平仓量 × fixed_impact; 无金额段 (Bar) 增仓量也付  | 【换手成本】  |
//     |            |   fixed_impact (Portfolio); 只在扣净线时扣, 标签恒毛                                                      |               |
//     | 净值       | 行 t 的批收益在 hold_exit_row 实现 (顺延不改实现行) × 1/h, 日内求和, 跨日复利; 显示的 LS / SR 取 HoldStat | 【二级】净值  |
//     | 复权       | 标签价 × 当日复权比: 跨日收益不含除权缺口                                                                 | 标签          |
//
//   【二级 HoldStat (每 hold)】只用 ok 行 (n) / ok_rac 行 (n_rac); n_all = 攒过的全部行 (含尾部 / 不 ok, 整张量 = T):
//     ric / rac / ls  三个逐行量沿 t 的同一套矩 (Moments, 同一个累加器 MomAcc; ric / ls 用 ok 行, rac 用 ok_rac 行):
//       mean / sd (ddof=1) / ir = mean/sd / pos = >0 占比 / skew, kurt (总体矩, 超额峰度)
//       t = ir · √(n/h): 持有期重叠 (相邻 h 行是同一段收益 / 同一段 lag, h = hold_rows), 有效样本按 n/h 折算, 不然 t 虚高 √h 倍
//     真实收益 (派生, 不落 HoldStat): 不 ok 的行记 0 补满 n_all 行 —— ls_period = Σls / n_all, ls_annual = × 每年期数,
//       sharpe_of = 补 0 后的 mean / sd × √每年期数 (由 n / ls.mean / ls.sd 还原 Σ / Σ², ann_sharpe). 因子表 / Inspect / Portfolio / Search 同一份
//     净值 (Inspect / Portfolio 曲线, 逐行结果按日折叠): 目标仓位 w_t = (1/h) Σ_{k<h} s_{t−k} (s = 行 t 的信号篮子, 每批资金 1/h),
//       行 t 的批收益在其平仓行实现 (hold_exit_row): 日收益 R_D = (1/h) Σ_{exit_day(t) = D} r_t (r = Row 的 ls / grp, 不 ok 记 0),
//       日内求和, 跨日复利. 沿全期 Σ R_D = Σr / h, 算术年化 = ls_annual (同一份收益, 只差落日); 显示的 LS / SR 一律取 HoldStat.
//     beta   ls 对 mkt[kLong] 的 OLS 斜率 (alpha 略: beta ≈ 0 时 alpha ≈ ls.mean)
//     grp[k] 各组每行持仓收益沿 t 的均值 (Σ grp / n_all, 不 ok 行记 0); mono = Spearman(组号, grp[k])
//     n_trig 触发行数 = ok 行里至少一腿非空; trigger_of = n_trig / n (诊断)
//     leg_n  平均每触发行一腿成员数 = (Σ cnt[0] + Σ cnt[K−1]) / (2 n_trig); util_of = leg_n / cap, 不封顶 (> 1 = 并列把腿塞过满仓; 诊断)
//     n < 3 → 标签侧 (ric / ls / grp / mono / leg_n) 全 0; n_rac < 3 → rac 全 0 (消费端先看对应计数)
//     累加器 Accum = Head (三个 MomAcc + beta 幂和 + n_all / n_trig) + Grp[kGroups] (组收益和 / 组计数), 可合并: 行区间并行各攒一份 merge; 超大张量沿 T
//     分块流式时跨块续攒, 不必落 Row. 拆两半是给 GPU 一 warp 一候选: lane 0 攒 Head, lane k 攒 Grp[k]; CPU 整行 add = 两半串起来
//
//   【换手成本 (每 hold)】Row / HoldStat 全是毛口径; 净多空由消费端 (Inspect 期限结构净线) 在二级上扣 (Search 不看成本, 只评排序):
//     交易的只是两腿的组 (多腿 = 顶组, 空腿 = 底组). 成本只看**腿码** lc = leg_code(r16) ∈ {kLcLong, kLcShort, kLcMid, kLcVoid} (u8 平面,
//     两后端由 rx 逐格派生; GPU 只回传它, 不回 u16 rx) ∧ 该持有期的标签有效位 m (腿成员 = 真建了仓的格, 与 Row 的组计数同一集合).
//     行 t 的批 (资金 1, 【持仓口径】) 每只权 w_t(a) = [a ∈ 腿] / max(腿样本数, cap). 换手 = 目标仓位之差 Δw (无限细粒度, 微调照计):
//     【交接】行 t 建仓与行 t−h 平仓同一价点 (hold_nets: 分钟型恒是; T+N 收盘型只在行 t 按收盘价建仓时 (日层恒是); 收盘型 / 开盘型从不)
//       → 两批在同一时刻对冲, 只交易差额: Δ = w_t − w_{t−h} 逐格, 增仓 Δ⁺ 付建仓冲击, 减仓 Δ⁻ 付平仓固定冲击;
//       不交接 → 本批整建 (w_t 全付建仓冲击), 前一批的平仓由它自己记 (见下).
//     本批的平仓若不与下一批交接 (hold_nets(t + h) 假: 下一批尾部不开仓 / 非同一价点) → 建仓时一并记平仓量 w_t (付固定冲击).
//     建仓冲击 imp[腿][a] (多腿买入 / 空腿卖出, 单一金额段, 买入已按 Config::max_impact 封顶; 腿成员格恒有限, 装载层断言) + 平仓固定冲击 fixed_impact.
//     每 h 一份 CostAcc 沿 t 求和; 每期成本 cost(腿) = (Σ Δ⁺·imp + Σ 平仓量 · fixed_impact) / n_all (与 ls_period 同分母). 尾部行不攒.
//     净多空 net_ls_period = ls_period − cost(kLong) − cost(kShort); 年化 net_ls = × periods_per_year(h).
//
//   【视图】两后端 / 全部调用方同一组参数形状 (宿主或设备指针, 由后端定): Plane (因子平面) · Batch (C 个同形候选, Stat = 1) ·
//     Labels (持有期键 hd 与其标签 l[] 同处, 带满仓 cap) · Span (块形 T × A + 层每段行数 seg + 分块流式 t0_abs / t_lo; 整张量两者为 0).
//     流程出口统一是二级累加器 Accum[H] (两后端同), HoldStat 由 finish_all 现算; 一级 Row 是可选的 sink.
//
//   【数值】整数和精确 (r16 ≤ 65534, A ≤ kMaxA → n·Σx² < 2^58, 有符号 64 位不溢); 浮点和 (标签值) 两后端
//   求和序不同, 走对拍容差. 全程 branchless 无 NaN: 标签 fp16 → float 是精确转换, 出口不产 inf.
//   【一份公式】契约公式 (ord / r16 / grp_of / tail / pair_add / pearson_int / finish_row / Accum::add, merge) 标 FACTOR_HD: CUDA 编译器下是
//   __host__ __device__, 纯 C++ 下为空 —— CPU / GPU 两后端直接调用这一份, 不再各自复刻; 后端只剩访存与并行.
//   【precise-math】依赖受控浮点, 编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Contract.hpp"

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace factor::stat {

// ---- 常量 ----
inline constexpr int kGroups = 20;                                 // 分组数 (等分 pct rank)
inline constexpr int kMaxHold = 12;                                // 同时评估的持有期数上限
inline constexpr int kMaxCost = 8;                                 // 同时攒的换手成本金额段数上限
inline constexpr int kMaxA = 8192;                                 // 资产轴上限 (GPU 一行一 block 的片上排序容量)
inline constexpr int kCloseAuction = 3;                            // 段末收盘集合竞价分钟数 (14:57–15:00), 标签 exit 锚点不能落入
inline constexpr int kLastAnchorRow = kSegMax - kCloseAuction - 2; // 最后一个 entry 锚点可成交的 L_1min 行 (= 分钟号; 锚点 = 下一分钟起始, 须在 14:57 前)
inline constexpr int kDaysPerYear = 242;                           // 年化用交易日数
inline constexpr uint16_t kRankNone = 0xFFFF;                      // r16 无效
inline constexpr int kRankMax = 65534;                             // r16 = round(pct · kRankMax) ∈ [0, kRankMax]
inline constexpr unsigned kNoKey = 0xFFFFFFFFu;                    // 排序键的"无效"哨兵 (有限 float 的保序键 ≤ 0xFF800000, 不冲突)
// TS 口径根 TsRankRoll 的窗长 (行) = ≤ kMaxD 行的最大整交易日数 (整数天窗跨日 deseason; ≤ kMaxD 故 Search carry 盖得住)
constexpr int ts_norm_d(int seg) { return kMaxD / seg * seg; }

// ---- 腿 (见文件头【腿 Leg】) ----
enum Leg : int { kLong = 0,
                 kShort = 1 };
inline constexpr int kLegs = 2;
FACTOR_HD inline constexpr int leg_group(int l) { return l == kLong ? kGroups - 1 : 0; } // 腿 → 组号 (顶 / 底)
inline constexpr double leg_sign(int l) { return l == kLong ? 1.0 : -1.0; }              // 腿在做多超额口径下的方向 (多 +1 / 空 −1)

// ---- 口径 (见文件头【口径 Frame】) ----
enum class Frame : uint8_t { CS,
                             TS };
inline constexpr const char *frame_name(Frame f) { return f == Frame::CS ? "CS" : "TS"; }

// ---- 持有期键 (见文件头【持有期键】) ----
inline constexpr int kHoldDayBase = 1000;                             // ≥ 此值为日级型 (含 close)
inline constexpr int kHoldClose = kHoldDayBase;                       // 持有到当日收盘
inline constexpr int kHoldCloseBase = 2000;                           // T+N 收盘型的基
inline constexpr int kHoldDaysMax = 30;                               // T+N 的 N 上限 (键值域 / 断言用)
inline constexpr int hold_open(int n) { return kHoldDayBase + n; }    // T+n 开盘平仓
inline constexpr int hold_close(int n) { return kHoldCloseBase + n; } // T+n 收盘平仓
FACTOR_HD inline constexpr bool hold_intraday(int hold) { return hold < kHoldDayBase; }
FACTOR_HD inline constexpr bool hold_is_open(int hold) { return hold > kHoldDayBase && hold < kHoldCloseBase; }
FACTOR_HD inline constexpr bool hold_is_close_n(int hold) { return hold > kHoldCloseBase; }
// T+N 的 N (日级型平仓日偏移; 分钟型 / close 为 0)
FACTOR_HD inline constexpr int hold_days(int hold) { return hold_is_open(hold) ? hold - kHoldDayBase : hold_is_close_n(hold) ? hold - kHoldCloseBase
                                                                                                                             : 0; }
// 键在值域内 (与层无关)
inline constexpr bool hold_key_ok(int hold) {
  return (hold >= 1 && hold <= kLastAnchorRow) || hold == kHoldClose ||
         ((hold_is_open(hold) || hold_is_close_n(hold)) && hold_days(hold) <= kHoldDaysMax);
}
// 持有期在层 g 上是否合法 (见文件头【持有期键】)
inline constexpr bool hold_ok(int hold, const Geo &g) {
  if (hold_intraday(hold))
    return hold >= 1 && hold % g.N == 0 && g.minute(0) + hold <= kLastAnchorRow;
  return hold != kHoldClose || g.S > 1;
}
// 折算 / lag / 年化用的有效行数 (层 g 上): 分钟型 = n / N; close / T+1 = 一段 S 行; T+N = N 段
FACTOR_HD inline constexpr int hold_rows(int hold, const Geo &g) {
  if (hold_intraday(hold))
    return hold / g.N;
  const int n = hold_days(hold);
  return g.S * (n < 1 ? 1 : n);
}
// 列名 token → 键: "<n>m" / "close" / "t<N>o" / "t<N>c" (LabelGroups LABEL_GROUPS 的 name); 不识别 → 断言
inline int hold_from_name(std::string_view name) {
  assert(!name.empty());
  if (name == "close")
    return kHoldClose;
  const bool day = name.front() == 't';
  const char tail = name.back();
  assert((day ? tail == 'o' || tail == 'c' : tail == 'm') && "持有期 token 不合 <n>m / close / t<N>o / t<N>c");
  const std::string_view digits = day ? name.substr(1, name.size() - 2) : name.substr(0, name.size() - 1);
  assert(!digits.empty());
  int n = 0;
  for (const char c : digits) {
    assert(c >= '0' && c <= '9' && "持有期 token 数字部分非数字");
    n = n * 10 + (c - '0');
  }
  return !day ? n : tail == 'o' ? hold_open(n)
                                : hold_close(n);
}
// 键 → 显示名: "5m" / "close" / "T+1O" / "T+2C"
inline std::string hold_name(int hold) {
  if (hold_intraday(hold))
    return std::to_string(hold) + "m";
  if (hold == kHoldClose)
    return "close";
  return "T+" + std::to_string(hold_days(hold)) + (hold_is_open(hold) ? "O" : "C");
}

// ---- 参数: 持有期键列表. 每个 h 对应一组标签 ----
struct Holds {
  int n = 0;
  int h[kMaxHold] = {};
};

inline void assert_holds(const Holds &hd, int seg) {
  assert(hd.n >= 1 && hd.n <= kMaxHold);
  for (int i = 0; i < hd.n; ++i) {
    const int h = hd.h[i];
    assert(hold_key_ok(h) && "持有期键越界 (close = kHoldClose, T+N = hold_open(N) / hold_close(N))");
    assert(hold_ok(h, geo_of(seg)) && "持有期在本层不合法 (分钟型须是行长整数倍且尾部不吞整段; 日层无 close)");
    (void)h;
  }
}

// ---- 输入: 一组标签 = 四张 [T][A] 平面的指针 (宿主或设备, 由后端定; 每个 hold 一组, 与 Holds 同序) ----
struct Label {
  const uint16_t *v[kLegs] = {};       // 两腿收益 fp16 位 (毛): 做多 lv / 做空 sv
  const uint8_t *m = nullptr;          // 有效位 (两腿共用)
  const uint16_t *ry = nullptr;        // 做多标签逐行 r16 (后端 prep_label 填; 与因子无关, 常驻期一次)
  FACTOR_HD Label at(size_t i) const { // 偏到第 i 格 (行首 = t·A)
    assert(ry && "标签未预处理 (prep_label)");
    return {{v[kLong] + i, v[kShort] + i}, m + i, ry + i};
  }
};

// ---- 视图 (见文件头【视图】) ----
inline constexpr int kMaxBatch = 128; // 一批候选上限 (GPU 走 kernel 参数区: 128 × 16 B = 2 KB)
struct Plane {                        // 因子平面 [T][A]: 值 + 有效位
  const float *v = nullptr;
  const uint8_t *m = nullptr;
};
struct Batch { // C 个同形候选 (同池 g / 同口径 / 同标签集); Stat = 1
  int C = 0;
  Plane x[kMaxBatch];
};
struct Labels { // l[i] ↔ hd.h[i]
  Holds hd;
  Label l[kMaxHold];
  double cap = 0.0; // 满仓一腿只数 (【持仓口径】; 调用方给 n_full)
};
struct Span { // 块形 + 层几何 + 分块流式 (Search 契约【分块流式】); 整张量 t0_abs = t_lo = 0
  int T = 0, A = 0;
  Geo g; // 因子所在层 (Contract【层几何】): tail_masked / hold_rows 用
  int t0_abs = 0, t_lo = 0;
  FACTOR_HD size_t n() const { return static_cast<size_t>(T) * A; }
};

inline void assert_span(const Span &s) {
  assert(s.T >= 1 && s.A >= 1 && s.A <= kMaxA && static_cast<long long>(s.T) * s.A < (1LL << 31));
  assert(is_seg(s.g.S) && geo_of(s.g.S).N == s.g.N && geo_of(s.g.S).M0 == s.g.M0 && "Span::g 须是 kGeos 之一");
  assert(s.t0_abs >= 0 && s.t_lo >= 0 && s.t_lo < s.T);
}
// prepped = ry 已由 prep_label 填 (评估入口); 假 = 只要两腿 + 掩码 (GPU 会话的宿主输入). seg = 因子所在层 (持有期按层校验)
inline void assert_labels(const Labels &L, int seg, bool prepped) {
  assert_holds(L.hd, seg);
  for (int i = 0; i < L.hd.n; ++i)
    assert(L.l[i].v[kLong] && L.l[i].v[kShort] && L.l[i].m && (!prepped || L.l[i].ry) && "标签不全 / 未预处理 (prep_label)");
  assert(L.cap > 0.0 && "满仓 cap 未给 (Labels::cap = n_full)");
}

// ---- 一级输出: 每 (持有期, 时刻) 一行; 布局 rows[h_idx * T + t] ----
struct Row {
  float ric = 0.f, rac = 0.f, ls = 0.f; // ls = LS(tp-dn)
  float mkt[kLegs] = {};                // J 上做多 / 做空标签原值均值 (两腿的超额基准)
  float grp[kGroups] = {};              // 组内做多超额 e 的持仓收益 Σe / max(cnt, cap)
  uint16_t cnt[kGroups] = {};           // 组样本数 (A ≤ kMaxA < 65536)
  uint8_t ok = 0, ok_rac = 0;
};

// ---- 二级输出: 每持有期一份 ----
// 一个逐行量 (rIC / rAC / LS) 沿 t 的矩 (见文件头【二级 HoldStat】)
struct Moments {
  float mean = 0.f, sd = 0.f, ir = 0.f, t = 0.f, pos = 0.f, skew = 0.f, kurt = 0.f;
};
struct HoldStat {
  int seg = 0; // 因子所在层的每段行数 (finish 填; 不落文件, 载入时由因子目录的层给): 年化 / 换手折算用
  int hold = 0, n = 0, n_rac = 0;
  int n_all = 0, n_trig = 0; // 攒过的全部行 (真实收益的分母) / 触发行 (ok 且至少一腿非空)
  Moments ric, rac, ls;      // 三个逐行量 (ric / ls: ok 行; rac: ok_rac 行)
  float beta = 0.f, mono = 0.f;
  float leg_n = 0.f;       // 平均每触发行一腿成员数 (两腿组计数的平均; util_of 的分子)
  float grp[kGroups] = {}; // 组每行持仓收益的均值 (Σ / n_all)
};
// 每年持有期数 = kDaysPerYear · S / h (一年 242 个交易日, 一天 S 行, h = hold_rows): 年化 (sharpe / 换手 / 均值超额) 统一走这一个
inline constexpr double periods_per_year(int hold, int seg) { return static_cast<double>(kDaysPerYear) * seg / hold_rows(hold, geo_of(seg)); }
inline double periods_per_year(const HoldStat &s) { return periods_per_year(s.hold, s.seg); }
// 真实收益 (见文件头【二级 HoldStat】): m = n 个 ok 行的矩, 其余 n_all − n 行记 0. 年化均值 = Σ / n_all × 每年期数 ppy
inline double ann_mean(const Moments &m, int n, int n_all, double ppy) { return n_all > 0 ? static_cast<double>(m.mean) * n / n_all * ppy : 0.0; }
// 补 0 后的年化 Sharpe: 由 (n, mean, sd) 还原 Σ = n·mean, Σ² = (n−1)·sd² + n·mean², 在 n_all 行上重算 mean / sd (ddof=1)
inline double ann_sharpe(const Moments &m, int n, int n_all, double ppy) {
  if (n < 3 || n_all < 2)
    return 0.0;
  const double nn = n, N = n_all, mu = m.mean, sd = m.sd;
  const double s1 = nn * mu, s2 = (nn - 1.0) * sd * sd + nn * mu * mu;
  const double M = s1 / N, v = (s2 - N * M * M) / (N - 1.0);
  return v > 0.0 ? M / std::sqrt(v) * std::sqrt(ppy) : 0.0;
}
// LS 每期真实收益 / 年化 / 年化 Sharpe. 派生量, 不落 HoldStat / 文件
inline double ls_period(const HoldStat &s) { return s.n_all > 0 ? static_cast<double>(s.ls.mean) * s.n / s.n_all : 0.0; }
inline double ls_annual(const HoldStat &s) { return ann_mean(s.ls, s.n, s.n_all, periods_per_year(s)); }
inline double sharpe_of(const HoldStat &s) { return ann_sharpe(s.ls, s.n, s.n_all, periods_per_year(s)); }
// 触发率 = 触发行 / ok 行; n = 0 → 0 (诊断)
inline double trigger_of(const HoldStat &s) { return s.n > 0 ? static_cast<double>(s.n_trig) / s.n : 0.0; }
// 两腿利用率 = 平均每触发行一腿成员数 / 满仓 cap, 不封顶 (> 1 = 腿塞过满仓; 诊断)
inline double util_of(const HoldStat &s, double cap) {
  assert(cap > 0.0);
  return static_cast<double>(s.leg_n) / cap;
}
// 年化换手 (倍/年) = 每持有期换手 (1 − rAC 均值, 秩重排占比) × 每年持有期数.
// rAC 的 lag = h 随持有期变, 本身不可跨期比; 折到同一时间轴后可比, 越低越好. 派生量, 不落 HoldStat / 文件
inline double turnover_annual(const HoldStat &s) { return (1.0 - s.rac.mean) * periods_per_year(s); }

// ---- 共享公式 (FACTOR_HD: 两后端同一份) ----

// float → 保序无符号键 (a < b ⟺ ord(a) < ord(b)); ±0 先归一为 +0
FACTOR_HD inline unsigned ord(float f) {
  const unsigned u = __builtin_bit_cast(unsigned, f + 0.f);
  return (u & 0x80000000u) ? ~u : (u | 0x80000000u);
}

// 并列均秩 r16: pct = (avg_rank − 1)/(n − 1), avg_rank = less + (eq + 1)/2, r16 = round(pct · kRankMax)
//   全整数: num = (2·less + eq − 1)·kRankMax, den = 2·(n − 1), r16 = ⌊(2·num + den) / (2·den)⌋
FACTOR_HD inline uint16_t r16_of(int less, int eq, int n) {
  assert(n >= 2 && eq >= 1 && less >= 0 && less + eq <= n);
  const long long num = (2LL * less + eq - 1) * kRankMax;
  const long long den = 2LL * (n - 1);
  return static_cast<uint16_t>((2 * num + den) / (2 * den));
}

// TS 口径: 分位 x ∈ [0,1] 直接量化 r16 = round(x · kRankMax). double 内 float × 65534 精确, 与 FMA / 求值序无关
FACTOR_HD inline uint16_t r16_quant(float x) {
  assert(x >= 0.f && x <= 1.f && "TS 口径的因子值须是分位 ∈ [0,1] (根 TsRankRoll)");
  return static_cast<uint16_t>(static_cast<double>(x) * kRankMax + 0.5);
}

// 组号: r16 等分 kGroups 组
FACTOR_HD inline int grp_of(unsigned r16) {
  const int g = static_cast<int>(r16 * kGroups / (kRankMax + 1));
  return g < kGroups - 1 ? g : kGroups - 1;
}

// 段末尾部 (见文件头【Row】段末尾部; 不开仓): 分钟型 exit 锚点越过最后可成交锚点 (持有期缩短) / 收盘型 entry 锚点越过 (收盘价建仓即平);
// 日级型无尾部. t = 绝对行号
FACTOR_HD inline bool tail_masked(int t, int hold, const Geo &g) {
  return hold <= kHoldClose && g.minute(t % g.S) + (hold_intraday(hold) ? hold : 0) > kLastAnchorRow;
}
// 交接 (见文件头【换手成本】): 行 t 建仓与行 t − h 平仓是同一价点 (两批同刻对冲, 只交易差额). t = 绝对行号;
// 分钟型 = 行 t−h 的 exit 就是行 t 的 entry; T+N 收盘型 = 行 t 按收盘价建仓 (行末分钟越过最后可成交锚点; 日层恒是); 收盘型 / 开盘型从不
inline bool hold_nets(int t, int hold, const Geo &g) {
  const int h = hold_rows(hold, g);
  if (t < h || tail_masked(t, hold, g) || tail_masked(t - h, hold, g))
    return false;
  return hold_intraday(hold) || (hold_is_close_n(hold) && g.minute(t % g.S) > kLastAnchorRow);
}
// 平仓行 (见文件头【净值】): 行 t 这一批的收益在哪一行实现. t = 绝对行号; 分钟型 = t + h; 收盘型 = 当日末行;
// T+N 开盘型 = D+N 日首行; T+N 收盘型 = D+N 日末行 (日层 S = 1: 两者都是 t + N). 顺延不改实现行 (按名义平仓日记)
inline int hold_exit_row(int t, int hold, const Geo &g) {
  if (hold_intraday(hold))
    return t + hold_rows(hold, g);
  const int d = t / g.S + hold_days(hold);
  return hold_is_open(hold) ? d * g.S : d * g.S + g.S - 1;
}

// ---- 换手成本 (见文件头【换手成本】; Run (stat.costs) / Inspect 期限结构净线共用) ----

// 单一金额段的建仓冲击: 两腿冲击, [T][A] 平面的指针 (宿主或设备, 由后端定); 标签有效格恒有限 (装载层断言)
struct Cost {
  const uint16_t *imp[kLegs] = {};                                                  // 建仓冲击 fp16 位: 多腿买入 / 空腿卖出
  FACTOR_HD Cost at(size_t i) const { return {{imp[kLong] + i, imp[kShort] + i}}; } // 偏到第 i 格 (行首 = t·A)
};

// 腿码 (两后端 / Inspect 同一份): r16 → 顶组 kLcLong / 底组 kLcShort / 其他组 kLcMid / rank 无效 kLcVoid. 前两个与 Leg 同值:
// 腿成员 = lc ∈ {kLong, kShort}; 腿 l 的新进者 = 前一批腿码 ≠ l (含当时 rank 无效; 没有前一批 = 全 kLcVoid)
enum LegCode : uint8_t { kLcLong = kLong,
                         kLcShort = kShort,
                         kLcMid,
                         kLcVoid };
FACTOR_HD inline uint8_t leg_code(unsigned x) {
  const int g = x != kRankNone ? grp_of(x) : -1;
  return g < 0 ? kLcVoid : g == leg_group(kLong) ? kLcLong
                       : g == leg_group(kShort)  ? kLcShort
                                                 : kLcMid;
}

// 一腿的换手成本和 (沿行求和; 量按【持仓口径】的权, 每批资金 1)
struct CostLeg {
  double imp = 0.0;  // Σ Δw⁺ · 建仓冲击
  double buy = 0.0;  // Σ Δw⁺ (增仓量)
  double sell = 0.0; // Σ 平仓量 (交接的 Δw⁻ + 不交接批的整批平仓)
  double w = 0.0;    // Σ w (持仓量)
  long long n = 0;   // 腿样本数 (腿成员格数)
};
// 一个持有期的换手成本累加器 (两腿); 可合并
struct CostAcc {
  CostLeg leg[kLegs];
  long long rows = 0; // 攒过的行数 (非尾部行各计一次)
  FACTOR_HD void merge(const CostAcc &o) {
    for (int l = 0; l < kLegs; ++l) {
      leg[l].imp += o.leg[l].imp, leg[l].buy += o.leg[l].buy, leg[l].sell += o.leg[l].sell;
      leg[l].w += o.leg[l].w, leg[l].n += o.leg[l].n;
    }
    rows += o.rows;
  }
  // 每期真实成本 (腿 l): (Σ Δw⁺ · 建仓冲击 + Σ 平仓量 · 固定冲击) / n_all (与 ls_period 同分母)
  double cost(int l, double fixed_impact, int n_all) const {
    const CostLeg &c = leg[l];
    return n_all > 0 ? (c.imp + c.sell * fixed_impact) / static_cast<double>(n_all) : 0.0;
  }
  // 换手 (每批增仓占持仓) / 增仓的平均建仓冲击 (显示用)
  double turn(int l) const { return leg[l].w > 0.0 ? leg[l].buy / leg[l].w : 0.0; }
  double entry(int l) const { return leg[l].buy > 0.0 ? leg[l].imp / leg[l].buy : 0.0; }
};

// 每期净多空 = ls_period − 两腿每期成本; 年化 = × periods_per_year (s.hold 由 finish 填)
inline double net_ls_period(const HoldStat &s, const CostAcc &c, double fixed_impact) {
  return ls_period(s) - c.cost(kLong, fixed_impact, s.n_all) - c.cost(kShort, fixed_impact, s.n_all);
}
inline double net_ls(const HoldStat &s, const CostAcc &c, double fixed_impact) { return net_ls_period(s, c, fixed_impact) * periods_per_year(s); }

// 整数五和 + 计数 (rIC / rAC 的 Pearson 原料), 下标 s[kSn..kSxy]; 两后端同一份累加 (r16 ≤ 65534: 积 < 2³², 32 位乘后再加进 64 位)
enum : int { kSn,
             kSx,
             kSy,
             kSxx,
             kSyy,
             kSxy,
             kSums };
FACTOR_HD inline void pair_add(unsigned long long *s, unsigned x, unsigned y, bool ok) {
  s[kSn] += ok;
  s[kSx] += ok ? x : 0u;
  s[kSy] += ok ? y : 0u;
  s[kSxx] += ok ? x * x : 0u;
  s[kSyy] += ok ? y * y : 0u;
  s[kSxy] += ok ? x * y : 0u;
}

// 整数五和的 Pearson: vx = n·Σx² − (Σx)² 等全在 64 位整数内精确, 只有最后一步除法/开方是浮点 (IEEE 正确舍入,
// 两后端逐位一致). ok = n ≥ 2 ∧ vx > 0 ∧ vy > 0
FACTOR_HD inline float pearson_int(const unsigned long long *s, bool &ok) {
  const unsigned long long n = s[kSn];
  if (n < 2) {
    ok = false;
    return 0.f;
  }
  const long long vx = static_cast<long long>(n * s[kSxx]) - static_cast<long long>(s[kSx] * s[kSx]);
  const long long vy = static_cast<long long>(n * s[kSyy]) - static_cast<long long>(s[kSy] * s[kSy]);
  const long long cxy = static_cast<long long>(n * s[kSxy]) - static_cast<long long>(s[kSx] * s[kSy]);
  ok = vx > 0 && vy > 0;
  return ok ? static_cast<float>(static_cast<double>(cxy) / std::sqrt(static_cast<double>(vx) * static_cast<double>(vy))) : 0.f;
}

// 多空的一腿 (一个组): 样本数 n + 该腿标签和 s (kLong: 顶组 Σlv / kShort: 底组 Σsv)
struct LegSum {
  double n = 0.0, s = 0.0;
};
// 持仓分母 (【持仓口径】) = max(只数, cap): 收益 = Σe / book_den, 不足满仓按只数 / cap 建仓, 超过满仓均分
FACTOR_HD inline double book_den(double n, double cap) { return n > cap ? n : cap; }
// 一腿持仓收益 = Σ(标签 − 该腿基准) / max(只数, cap); 腿空 → 0
FACTOR_HD inline double leg_excess(const LegSum &l, double base, double cap) { return (l.s - l.n * base) / book_den(l.n, cap); }
// 多空 = 两腿持仓收益之和 (leg / mkt 按 [kLegs])
FACTOR_HD inline double ls_of(const LegSum *leg, const double *mkt, double cap) {
  return leg_excess(leg[kLong], mkt[kLong], cap) + leg_excess(leg[kShort], mkt[kShort], cap);
}

// 一行标签侧的收尾 (两后端同一份): J 上整数五和 s → ric; 组和 / 组计数 → grp (持仓收益) / cnt; 两腿 → ls; 两腿基准 → mkt.
//   sm[kLegs] = J 上 Σlv / Σsv; gs[k] / gc[k] = 组 k 的 Σlv / 计数; leg[kLegs] = 两腿的组 (见 LegSum); cap = 满仓. 不 ok → r 标签侧字段保持 0
FACTOR_HD inline void finish_row(const unsigned long long *s, const double *sm, const double *gs, const int *gc, const LegSum *leg, double cap, Row &r) {
  bool icok = false;
  const float ric = pearson_int(s, icok);
  if (!icok)
    return;
  const double n = static_cast<double>(s[kSn]);
  const double mkt[kLegs] = {sm[kLong] / n, sm[kShort] / n};
  r.ok = 1;
  r.ric = ric;
  for (int l = 0; l < kLegs; ++l)
    r.mkt[l] = static_cast<float>(mkt[l]);
  for (int k = 0; k < kGroups; ++k) { // 超额和 Σ(lv − mkt) = Σlv − cnt·mkt, 按持仓分母折成收益
    r.cnt[k] = static_cast<uint16_t>(gc[k]);
    r.grp[k] = static_cast<float>((gs[k] - gc[k] * mkt[kLong]) / book_den(gc[k], cap));
  }
  r.ls = static_cast<float>(ls_of(leg, mkt, cap));
}

// ---- 二级汇总 (主机, double; 两后端共用) ----

// 组号 0..K−1 与 g[k] 的 Spearman (g 并列均秩); g 全并列 → 0
inline double mono_of(const double *g, int K) {
  double rk[kGroups];
  for (int i = 0; i < K; ++i) {
    int less = 0, eq = 0;
    for (int j = 0; j < K; ++j) {
      less += g[j] < g[i];
      eq += g[j] == g[i];
    }
    rk[i] = less + (eq + 1) * 0.5; // 1-based 均秩
  }
  const double mu = (K + 1) * 0.5; // 0..K−1 的秩均值 (1-based) = 组号均值 + 1
  double cxy = 0.0, vx = 0.0, vy = 0.0;
  for (int i = 0; i < K; ++i) {
    const double dx = (i + 1) - mu, dy = rk[i] - mu;
    cxy += dx * dy;
    vx += dx * dx;
    vy += dy * dy;
  }
  return vy > 0.0 ? cxy / std::sqrt(vx * vy) : 0.0;
}

// 二级累加器: 一遍消费 Row, 可合并 (t 切块并行各攒一份再 merge; 沿 T 分块流式跨块续攒), 不留 Row.
//   幂和 double, 原点 0: rIC / rAC ∈ [−1,1], 标签收益 ~1e-3 —— 相消损失 ≈ (μ/σ)^k 量级 (< 1e-2 相对), 远在 float 出口精度内;
//   中心矩相消出负由 fmax 钳 0. 两后端共用同一份 (对拍一致性在 Row 级); add / merge 是 FACTOR_HD, 设备侧也原样攒.

// 一个逐行量 (rIC / rAC / LS 同一份) 的幂和 → Moments; n < 3 → 全 0
struct MomAcc {
  int n = 0, npos = 0; // 行数 / > 0 行数
  double s1 = 0.0, s2 = 0.0, s3 = 0.0, s4 = 0.0;
  FACTOR_HD void add(float v) {
    ++n;
    npos += v > 0.f;
    const double x = v, x2 = x * x;
    s1 += x, s2 += x2, s3 += x2 * x, s4 += x2 * x2;
  }
  FACTOR_HD void merge(const MomAcc &o) {
    n += o.n, npos += o.npos;
    s1 += o.s1, s2 += o.s2, s3 += o.s3, s4 += o.s4;
  }
  // h = 持有期折成的行数 (hold_rows; t 值的重叠折算 n/h)
  Moments finish(int h) const {
    Moments m;
    if (n < 3)
      return m;
    const double nn = n, mu = s1 / nn;
    const double m2 = std::fmax(s2 - nn * mu * mu, 0.0); // 中心矩
    const double m3 = s3 - 3.0 * mu * s2 + 2.0 * nn * mu * mu * mu;
    const double m4 = std::fmax(s4 - 4.0 * mu * s3 + 6.0 * mu * mu * s2 - 3.0 * nn * mu * mu * mu * mu, 0.0);
    const double sd = std::sqrt(m2 / (nn - 1));
    m.mean = static_cast<float>(mu);
    m.sd = static_cast<float>(sd);
    m.pos = static_cast<float>(npos / nn);
    if (sd > 0.0) {
      m.ir = static_cast<float>(mu / sd);
      m.t = static_cast<float>(mu / sd * std::sqrt(nn / h));
      const double v = m2 / nn;
      m.skew = static_cast<float>((m3 / nn) / (v * std::sqrt(v)));
      m.kurt = static_cast<float>((m4 / nn) / (v * v) - 3.0);
    }
    return m;
  }
};

struct Accum {
  // 标量头: 三个逐行量的矩 + beta 的幂和 + 行计数 (GPU 上 lane 0 攒)
  struct Head {
    MomAcc ric, rac, ls;                     // ric.n = ls.n = ok 行数 (标签侧全部统计的样本), rac.n = ok_rac 行数
    double smk = 0.0, smk2 = 0.0, slm = 0.0; // mkt[kLong] 幂和 + ls·mkt 交叉 (beta)
    int n_all = 0, n_trig = 0;               // 全部行 / 触发行 (ok 且至少一腿非空)
    FACTOR_HD void add(const Row &w) {
      ++n_all;
      n_trig += w.ok && (w.cnt[leg_group(kLong)] || w.cnt[leg_group(kShort)]);
      if (w.ok) {
        ric.add(w.ric);
        ls.add(w.ls);
        const double v = w.ls, mk = w.mkt[kLong];
        smk += mk, smk2 += mk * mk, slm += v * mk;
      }
      if (w.ok_rac)
        rac.add(w.rac);
    }
    FACTOR_HD void merge(const Head &o) {
      ric.merge(o.ric), rac.merge(o.rac), ls.merge(o.ls);
      smk += o.smk, smk2 += o.smk2, slm += o.slm;
      n_all += o.n_all, n_trig += o.n_trig;
    }
  };
  // 一个组: 组持仓收益 / 组样本数 沿 t 的和 (GPU 上 lane k 攒组 k; 只收 ok 行, 由调用方判)
  struct Grp {
    double s = 0.0;
    long long c = 0;
    FACTOR_HD void add(float g, unsigned n) { s += g, c += n; }
    FACTOR_HD void merge(const Grp &o) { s += o.s, c += o.c; }
  };
  Head head;
  Grp grp[kGroups];

  FACTOR_HD void add(const Row &w) {
    head.add(w);
    if (w.ok)
      for (int k = 0; k < kGroups; ++k)
        grp[k].add(w.grp[k], w.cnt[k]);
  }
  FACTOR_HD void merge(const Accum &o) {
    head.merge(o.head);
    for (int k = 0; k < kGroups; ++k)
      grp[k].merge(o.grp[k]);
  }
  // hold = 持有期键, seg = 因子所在层的每段行数 (折算 / 年化); n < 3 → 标签侧全 0, rac 照算 (看 n_rac)
  HoldStat finish(int hold, int seg) const {
    assert(hold >= 1);
    const int h = hold_rows(hold, geo_of(seg));
    assert(h >= 1);
    HoldStat s;
    s.seg = seg;
    s.hold = hold;
    s.n = head.ric.n;
    s.n_rac = head.rac.n;
    s.n_all = head.n_all;
    s.n_trig = head.n_trig;
    s.ric = head.ric.finish(h);
    s.rac = head.rac.finish(h);
    s.ls = head.ls.finish(h);
    if (s.n < 3)
      return s;
    double g[kGroups];
    for (int k = 0; k < kGroups; ++k)
      g[k] = grp[k].s / static_cast<double>(s.n_all); // 不 ok 行记 0 (n_all ≥ n ≥ 3)
    const double nn = s.n, mmk = head.smk / nn, mls = head.ls.s1 / nn;
    const double vmk = std::fmax(head.smk2 - nn * mmk * mmk, 0.0), cov = head.slm - nn * mls * mmk;
    if (vmk > 0.0)
      s.beta = static_cast<float>(cov / vmk);
    for (int k = 0; k < kGroups; ++k)
      s.grp[k] = static_cast<float>(g[k]);
    s.mono = static_cast<float>(mono_of(g, kGroups));
    s.leg_n = s.n_trig > 0 ? static_cast<float>(static_cast<double>(grp[leg_group(kLong)].c + grp[leg_group(kShort)].c) / (2.0 * s.n_trig)) : 0.f;
    return s;
  }
};

// 流程出口 Accum[hd.n] → HoldStat[hd.n] (两后端同一份; 见文件头【视图】)
inline void finish_all(const Accum *acc, const Holds &hd, int seg, HoldStat *out) {
  assert(acc && out);
  for (int i = 0; i < hd.n; ++i)
    out[i] = acc[i].finish(hd.h[i], seg);
}

} // namespace factor::stat
