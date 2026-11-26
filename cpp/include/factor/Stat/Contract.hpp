#pragma once

// =============================================================================
// Stat: 因子评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 无流式后端 —— 评估只在挖掘侧)
// =============================================================================
//   输入  因子平面 x [T][A] (SoA: float + 有效位) + 口径 Frame + 方向 Dir + H 组常驻标签 (每组一个持有期 HoldId, define/Label.hpp):
//           lv       做多收益 (features LabelBar lb_<h>, 毛口径, 平仓顺延; fp16 位, 与落盘同格式). 冲击不进标签: 超额对每 t 去均值,
//                    成本进来只剩截面相对项 —— 消费端在二级上按账扣. 分钟型 entry / exit 同为分钟 VWAP (行 m 的 exit = 行 m+h 的
//                    entry), 相邻行精确伸缩: 留仓成员逐期标签之和 = 连续持有收益, 【换手成本】的"留仓不付"才成立
//           nv       名义收益 (lb_nom_<h>, 只偏移型): 名义价点即平不顺延 —— 交接续持的估值 (【仓位感知】); 锚点型无此列 (空指针)
//           m        标签有效位
//           eo       建仓可成交码 kEoBuy | kEoSell (features lb_entry_ok; 各持有期共用一张): 见【仓位感知】
//           ry       预处理: 做多标签逐行截面 rank (r16), 与因子无关, 常驻期算一次 (prep_label)
//
//   【只做多】一切组合 (组 / 尾部 / 多空两端 / 池篮子 / 单只) 都是做多账本: 收益取 lv (续持取名义值), 建仓要价点可买, 冲击取买入冲击.
//   【多空 LS】两端 End = 最高组 G20 (kTop) 与最低组 G1 (kBot), 各是一本做多账本 (就是该组的 grp, 同一套建仓 / 续持 / 换手);
//     多空 = ½ (G20 − G1): 资金 1 两本各半, 做多 G20 的收益减做多 G1 的收益, 不做空. 分层与多空同一组账本, 多空只是两端组之差.
//   【方向 Dir】方向是评估参数 (因子行的 sign), 不改写表达式, 在源头一次定: rank_x 出的 r16 已定向 (反向 = kRankMax − r16, 高 = 做多端),
//     其后 Row / Day / Accum / 端码 / 尾部全部只按"高端做多"读. rIC 精确变号; 组边界上 grp_of(kRankMax − r) 与 K−1−grp_of(r) 至多差一个 r16 取值.
//     Search 只正向评一遍, 由 oriented(Accum) 改读反向的二级 (定向打分只看 rIC, 精确).
//   输出  三级:
//     一级 Row[H][T]  每 (持有期, 时刻) 一行沿 A 轴归约的统计 (两后端各算, 对拍在此级); 只在要逐行时落
//     二级 Day[H][D]  每 (持有期, 统计日) 的 ok 行数 / Σ rIC / Σ 多空 (Roll 滚动窗目标与分年的原料); 恒出
//     三级 HoldStat   每持有期沿 T 轴汇总成十几个标量 (Accum 一遍可合并累加器 → finish; 主机 double, 两后端共用同一份代码); 恒出
//
//   【口径 Frame】alpha 因子分两种坐标系, 由因子表达式的根算子决定 (Expr.hpp root_frame). 标签**统一超额语义**:
//     e = lv − mean_J lv, 每 t 的共同分量 (市场水平 / 市场择时) 被扔掉, 评的都是"相对市场";
//     CS  截面: 根是截面归一算子 (CsRank / CsNormRank / CsZ). 每 t 沿 A 轴 rank x (排序) → 20 组 = 截面分位组;
//     TS  时序: 根是 TsRankRoll(d = ts_norm_d(seg), 整数个交易日), 输出已是每资产对自身滚动历史的分位 ∈ [0,1];
//         不排序, 直接量化 r16 = round(x · kRankMax) (r16_quant) → 20 组 = 自身历史分位组.
//     两口径之后逐字共用: grp_of / rIC / 超额 / 持仓口径 / 尾部 / ls / mkt / rAC / 二级汇总. 组允许空 (空组收益 0).
//
//   【持仓口径】每组资金固定为 1, 满仓 cap = Labels::cap 只 (调用方给 n_full = 池内平均有效资产数 / kGroups, 与因子无关), 每只 1/cap:
//     触发 n 只 → 持仓 min(n, cap) / cap, 超过 cap 满仓在 n 只里均分 → 组收益 = Σe / max(n, cap) (book_den); 未触发 → 0.
//
//   【rank 口径】精确并列均秩 (排序, 不用桶近似): r16 = round(pct · kRankMax) ∈ [0, kRankMax], 全整数公式 (r16_of), 两后端逐位一致;
//     kRankNone = 无效. 行有效样本 n < 2 或全并列 → 整行 kRankNone. rank(x) 只看 x 的有效集, rank(y) 只看 y 的有效集; 两者的联合集 J 上算统计.
//
//   【一级 Row (每 h, t)】J = {a : rx ≠ None ∧ ry ≠ None}; e = 超额标签 lv − mkt
//     ric    rank IC = Pearson(r16_x, r16_y) over J (整数五和 → pearson_int, 两后端逐位一致)
//     grp[k] 第 k 组 (k = grp_of(r16_x)) 的持仓收益 Σe / max(cnt, cap); cnt[k] = 组真持有只数 (【仓位感知】)
//     tail   两端组内更极端的一截 (【尾部】), 同口径 Σe / max(n, cap · tail_frac(j))
//     ls_row 多空 = ½ (grp[K−1] − grp[0]); mkt = J 上 lv 均值 (超额基准, 二级回归 beta 用)
//     ok     标签侧有效: 非段末尾部 ∧ ric 可算 (n ≥ 2, 两侧方差 > 0). rac = Pearson(r16_x(t), r16_x(t−h)) (lag = h 行 = hold_rows), ok_rac 同理.
//     段末尾部 (分钟型 / 收盘型, tail_masked): 分钟型 exit 锚点越过最后可成交锚点 kLastAnchorRow (持有期缩短), 收盘型 entry 越过 (即建即平) → 不开仓.
//     ok / ok_rac 为假时对应字段全 0. J / ric / mkt 与可成交无关; grp / cnt / tail 只收真持有的格.
//
//   【尾部】两端组 (顶 / 底) 内更极端的一截, 占该组 tail_frac(j) (= 整截面 2.5% / 1% / 0.25%), j 越大越极端; 阈值按 r16 定 (tail_thr),
//     与 grp_of 的组边界同口径; 成员判据 in_tail, 真持有 / 续持与组同一套 (【仓位感知】). 不进二级.
//
//   【仓位感知】一个组合 (组 / 尾部 / 池篮子 / 单只; 成员判据 in 由调用方给) 每行一批 (做多), 格 a 在行 t 的批里:
//     真持有 held: 行 t 价点可买 (eo & kEoBuy), 或 与前一批交接 (hold_nets) 且前一批同组合 (m ∧ in) 并真持有 —— 续持者不必成交, 封板也照持.
//       递归沿链回走到可成交为止, 前一批至多在 t 前 kChainDays 个交易日 (= 平仓结清窗; 更早视为断链), 所以每格只看
//       [t − kChainDays 日 − h, t + h] 的行: 分块带够前后带即与整张量一致 (【分块流式】); 块外视为断链.
//     收益 hold_ret: 下一批 (t + h) 交接且同组合 (continues) → 不真平仓, 按名义价点估值: 名义价点可卖 → lv 即名义值, 否则 = nv;
//       不续持 → lv (含顺延). 非交接持有期 (收盘型 / 开盘型) 无链, 自然退化为"建仓须可买, 平仓顺延".
//
//   【协议表 1: 持有期 → 开仓 / 清仓 / 价】(features 标签 / Stat / 净值 / UI hold_points 同一份; 改口径先改这里, 标签 kLabelSemver +1)
//     价点: E    = 行末分钟价点 = 之后首个有成交连续竞价分钟的 VWAP; 锚点后再无连续竞价 (≥ 14:57) → C(D)
//           C(D) = D 日收盘集合竞价撮合价 (末笔成交);  O(D) = D 日 09:25 开盘撮合价 (无撮合 → 首个有成交连续竞价 bar 的 open)
//     | 显示   | token | 适用层            | 开仓                   | 清仓                        | h (hold_rows) | h̄ (hold_span; J = 日内开仓行数) |
//     |--------|-------|-------------------|------------------------|-----------------------------|---------------|---------------------------------|
//     | <n>m   | <n>m  | 行长 ≤ n 的分钟层 | C(行) @ E              | C(行)+n @ E (过 14:57 掩)   | n / N         | n / N · J / S                   |
//     | close  | close | 非 day            | C(行) @ E              | C(D) @ 收盘撮合             | S             | Σ_{j<J} (S − 1 − j) / S         |
//     | T+1O   | t1o   | 全部              | C(行) @ E; day 层 C(D) | O(D+1) @ 开盘撮合           | S             | (S + 1) / 2                     |
//     | T+N    | t<N>  | 全部              | C(行) @ E; day 层 C(D) | D+N 日同一行 @ E (day: C)   | N · S         | N · S                           |
//
//   【协议表 2: 开关仓策略】
//     | 环节       | 规则                                                                                                      | 落在          |
//     |------------|-----------------------------------------------------------------------------------------------------------|---------------|
//     | 可成交判据 | 分钟价点 (Tradable): 可买 = low < 涨停 − ε, 可卖 = high > 跌停 + ε; 开盘 / 收盘撮合价同式; 涨跌停 NaN → 可成交 | LabelBar      |
//     | 开仓       | 组合只做多: E 价点可买才新建; 交接的续持者不必成交 (held); 不顺延开仓                                    | 【仓位感知】  |
//     | 续持       | 下一批交接且同组合 → 不真平仓, 名义价点估值 (lv 或 nv)                                                    | 【仓位感知】  |
//     | 平仓       | 名义价点卖不出或名义日停牌 → 顺延到之后首个可卖价点; 行所在日起 kPendDays − 1 个交易日仍未平 → 最近收盘价结清 | 标签          |
//     | 尾部       | 分钟型 / 收盘型 minute(t) + n > kLastAnchorRow 的行不开仓 (tail_masked)                                   | Stat          |
//     | 仓位       | 每行一批, 资金 1/h̄, 批内每只 1 / max(只数, cap); 仓位 = 在持各批之和, 全日平均恰为 1                      | Stat / 净值   |
//     | 换手       | 前一批的权随价漂移到交接点, 与本批目标权逐格之差 (只在交接时; 否则整建整平, 平仓量亦随价漂移)            | 【换手成本】  |
//     | 成本       | 两侧逐格费率: 税佣 (Fee::tax) + 冲击 (盘口段 = 真实买 / 卖冲击封顶 max_impact; 固定段 = Fee::impact)     | 【换手成本】  |
//     | 净值       | 行 t 的批收益在 hold_exit_row 实现 × 1/h̄, 日内求和, 跨日复利; 成本在建仓行即付                            | 【二级】净值  |
//     | 复权       | 标签价 × 当日复权比: 跨日收益不含除权缺口                                                                 | 标签          |
//
//   【二级 Day / HoldStat (每 hold)】只用 ok 行 (n) / ok_rac 行 (n_rac); n_all = 攒过的全部行:
//     ric / rac / ls  逐行量沿 t 的矩 (Moments, MomAcc): mean / sd (ddof=1) / ir / pos / skew / kurt; t = ir · √(n/h) (重叠折算)
//     真实收益 (派生): 不 ok 的行记 0 补满 n_all 行 —— ls_period = Σls / n_all, ls_annual = × span_per_year (满仓一年),
//       sharpe_of = 补 0 后 mean / sd × √periods_per_year. 净值 (逐行结果按日折叠): 每批资金 1/h̄, 收益在平仓行实现, 日内求和, 跨日复利.
//     beta = ls 对 mkt 的 OLS 斜率; grp[k] = Σ grp / n_all; mono = Spearman(组号, grp[k]); n_trig / end_n 诊断.
//     n < 3 → 标签侧全 0; n_rac < 3 → rac 全 0. Accum = Head + Grp[kGroups] 可合并 (GPU 一 warp 一候选: lane 0 攒 Head, lane k 攒 Grp[k]).
//     Day 逐统计日: ok 行数 / Σ ric / Σ ls_row (行序累加, 两后端逐位同); 块内统计行按整日对齐.
//
//   【换手成本 (每 hold)】Row / HoldStat 全是毛口径; 净多空由消费端在二级上扣 (Search 不看成本). 交易的是两端两本做多账本.
//     端码 ec = end_code(r16) ∈ {kEcTop, kEcBot, kEcMid, kEcVoid} (u8 平面; GPU 只回传它) ∧ 标签有效位 ∧ 真持有 (链上成员 = 同端码).
//     行 t 的批每只目标权 w_t(a) = [a ∈ 端] / max(端只数, 端满仓). 换手 = 与持仓之差:
//       交接 (hold_nets: 行 t 建仓与行 t−h 平仓同一价点) → 前一批持仓随价漂移到交接点 w̃(a) = w_{t−h}(a)(1 + r_a) / V, r_a = 名义收益
//         (hold_ret 续持口径), V = 1 + Σ w_{t−h} r (批资金含现金) → Δ = w_t − w̃ 逐格, 增仓 Δ⁺ 付买入 (本格不可买则 0), 减仓 Δ⁻ 付卖出;
//       不交接 → 本批整建 (w_t 全付买入). 本批平仓不与下一批交接 → 建仓时一并记平仓量 w_t (1 + lv) / V_t (随价漂移, 付卖出, 冲击取本格建仓时的卖出冲击).
//     费率 = Fee::tax (税佣) + 冲击 (盘口段 = min(imp[侧][a], top); 固定段 = Fee::impact); 每 h 一份 CostAcc (每侧: 量 / Σ 量·冲击 / 付固定冲击的量),
//     每期成本 cost(端, Fee) = Σ侧 / n_all (与 ls_period 同分母); 费率在 finish 端现乘 (UI 改费率不重算). 尾部行不攒.
//     净多空 net_ls_period = ls_period − ½ (cost(kTop) + cost(kBot)); 年化 net_ls = × span_per_year.
//
//   【分块流式】超大张量沿 T 切块 (Span): t0_abs 块首绝对行 (tail_masked / hold_nets 看绝对行); 统计行 [t_lo, T − t_back), 前带只给因子回看 /
//     rAC lag / 交接链 (≥ 回看 + hold_reach + kChainDays 日), 后带只给续持 (≥ hold_reach); 两者按整日对齐. 分块 ≡ 整张量, 跨块 Accum merge 续攒.
//   【视图】Plane (因子平面) · Batch (C 个同形候选; 交互 C = 1) · Labels (持有期 hd 与其标签 l[] 同处, 带满仓 cap) · Span.
//   【数值】整数和精确 (r16 ≤ 65534, A ≤ kMaxA); 浮点和两后端求和序不同, 走对拍容差. 全程 branchless 无 NaN.
//   【一份公式】契约公式标 FACTOR_HD (define/HostDev.hpp): CPU / GPU 两后端直接调用这一份, 后端只剩访存与并行.
//   【precise-math】依赖受控浮点, 编进 -fno-fast-math TU.
// =============================================================================

#include "define/Annual.hpp"
#include "define/Fee.hpp"
#include "define/Label.hpp"
#include "factor/Contract.hpp"

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>

namespace factor::stat {

using ::AmtId;
using ::Amts;
using ::AmtSet;
using ::ann_geo;
using ::Fee;
using ::HoldId;
using ::Holds;
using ::HoldSet;
using ::kAmtCount;
using ::kChainDays;
using ::kDaysPerYear;
using ::kHoldCount;
using ::kLastAnchorRow;

// ---- 常量 ----
inline constexpr int kGroups = 20;              // 分组数 (等分 pct rank)
inline constexpr int kMaxA = 8192;              // 资产轴上限 (GPU 一行一 block 的片上排序容量)
inline constexpr uint16_t kRankNone = 0xFFFF;   // r16 无效
inline constexpr int kRankMax = 65534;          // r16 = round(pct · kRankMax) ∈ [0, kRankMax]
inline constexpr unsigned kNoKey = 0xFFFFFFFFu; // 排序键的"无效"哨兵 (有限 float 的保序键 ≤ 0xFF800000, 不冲突)
static_assert(kLastAnchorRow + 5 == kSegMax, "末个可成交锚点 = 收盘集合竞价 (末 3 分钟) 前一分钟的起始");
// TS 口径根 TsRankRoll 的窗长 (行) = ≤ kMaxD 行的最大整交易日数
constexpr int ts_norm_d(int seg) { return kMaxD / seg * seg; }

// ---- 多空两端 / 方向 ----
enum End : int { kTop = 0,
                 kBot = 1 };
inline constexpr int kEnds = 2;
enum Dir : int { kFwd = 0,
                 kRev = 1 };
inline constexpr Dir dir_of(int sign) { return sign < 0 ? kRev : kFwd; } // 权重行的 sign (±1) → 方向
// 端 e 的组号 (G20 / G1)
FACTOR_HD inline constexpr int end_group(int e) { return e == kTop ? kGroups - 1 : 0; }
// 建仓可成交码 (features lb_entry_ok): 价点可买 | 可卖. 做多建仓要可买, 名义平仓要可卖
inline constexpr uint8_t kEoBuy = 1, kEoSell = 2;

// ---- 口径 ----
enum class Frame : uint8_t { CS,
                             TS };
inline constexpr const char *frame_name(Frame f) { return f == Frame::CS ? "CS" : "TS"; }

// ---- 持有期几何 (协议表 1; 族 / 天数见 define/Label.hpp) ----
// 持有期在层 g 上是否合法: 分钟型 n 是行长 N 的整数倍且首行 minute(0) + n ≤ kLastAnchorRow; close 要求 S > 1; 日级型各层皆可
FACTOR_HD inline constexpr bool hold_ok(HoldId h, const Geo &g) {
  switch (hold_kind(h)) {
  case HoldKind::Min:
    return hold_n(h) % g.N == 0 && g.minute(0) + hold_n(h) <= kLastAnchorRow;
  case HoldKind::Close:
    return g.S > 1;
  default:
    return true;
  }
}
// 独立样本口径的行数 h (lag / 交接 / 重叠折算 / Sharpe 与换手的年化): 分钟型 = n / N; close / T+1O = 一段 S 行; T+N 同刻 = N 段
FACTOR_HD inline constexpr int hold_rows(HoldId h, const Geo &g) {
  if (hold_kind(h) == HoldKind::Min)
    return hold_n(h) / g.N;
  const int n = hold_days(h);
  return g.S * (n < 1 ? 1 : n);
}
// 层 seg 上的合法持有期集合
inline HoldSet holds_on(int seg) {
  HoldSet s = 0;
  for (int i = 0; i < kHoldCount; ++i)
    if (hold_ok(static_cast<HoldId>(i), geo_of(seg)))
      s |= hold_bit(static_cast<HoldId>(i));
  return s;
}
inline void assert_holds(const Holds &hd, int seg) {
  assert(hd.n >= 1 && hd.n <= kHoldCount);
  for (int i = 0; i < hd.n; ++i)
    assert(hold_ok(hd.k[i], geo_of(seg)) && "持有期在本层不合法 (分钟型须是行长整数倍且尾部不吞整段; 日层无 close)");
  (void)seg;
}
// 最长持有期的行数 (rAC 的 lag / 续持前看 / 前一批的链; 分块前后带都按它)
inline int hold_reach(const Holds &hd, int seg) {
  int m = 0;
  for (int i = 0; i < hd.n; ++i)
    m = std::max(m, hold_rows(hd.k[i], geo_of(seg)));
  return m;
}

// ---- 输入: 一组标签 = [T][A] 平面的指针 (宿主或设备, 由后端定; 每个持有期一组, 与 Holds 同序) ----
struct Label {
  const uint16_t *lv = nullptr;        // 做多收益 fp16 位 (毛, 顺延)
  const uint16_t *nv = nullptr;        // 名义收益 fp16 位 (只偏移型; 锚点型空)
  const uint8_t *m = nullptr;          // 有效位
  const uint8_t *eo = nullptr;         // 建仓可成交码 (各持有期共用同一张)
  const uint16_t *ry = nullptr;        // 做多收益逐行 r16 (后端 prep_label 填; 与因子无关, 常驻期一次)
  FACTOR_HD Label at(size_t i) const { // 偏到第 i 格 (行首 = t·A)
    return {lv + i, nv ? nv + i : nullptr, m + i, eo + i, ry ? ry + i : nullptr};
  }
};

// ---- 视图 ----
inline constexpr int kMaxBatch = 128; // 一批候选上限 (GPU 走 kernel 参数区: 128 × 16 B = 2 KB)
struct Plane {                        // 因子平面 [T][A]: 值 + 有效位
  const float *v = nullptr;
  const uint8_t *m = nullptr;
};
struct Batch { // C 个同形候选 (同池 g / 同口径 / 同方向 / 同标签集); 交互 = 1
  int C = 0;
  Plane x[kMaxBatch];
};
struct Labels { // l[i] ↔ hd.k[i]
  Holds hd;
  Label l[kHoldCount];
  double cap = 0.0; // 满仓一组只数 (【持仓口径】; 调用方给 n_full)
};
struct Span { // 块形 + 层几何 + 分块流式; 整张量 t0_abs = t_lo = t_back = 0
  int T = 0, A = 0;
  Geo g;
  int t0_abs = 0, t_lo = 0, t_back = 0;
  FACTOR_HD size_t n() const { return static_cast<size_t>(T) * A; }
  FACTOR_HD int t_hi() const { return T - t_back; }            // 统计行 [t_lo, t_hi)
  FACTOR_HD int days() const { return (t_hi() - t_lo) / g.S; } // 统计日数 (二级 Day 的条数)
};

inline void assert_span(const Span &s) {
  assert(s.T >= 1 && s.A >= 1 && s.A <= kMaxA && static_cast<long long>(s.T) * s.A < (1LL << 31));
  assert(is_seg(s.g.S) && geo_of(s.g.S).N == s.g.N && geo_of(s.g.S).M0 == s.g.M0 && "Span::g 须是 kGeos 之一");
  assert(s.t0_abs >= 0 && s.t_lo >= 0 && s.t_back >= 0 && s.t_lo < s.t_hi());
  assert(s.t_lo % s.g.S == 0 && (s.t_hi() - s.t_lo) % s.g.S == 0 && s.t0_abs % s.g.S == 0 && "统计行按整日对齐");
}
// prepped = ry 已由 prep_label 填 (评估入口); 假 = 只要标签 + 掩码 (宿主输入). seg = 因子所在层
inline void assert_labels(const Labels &L, int seg, bool prepped) {
  assert_holds(L.hd, seg);
  for (int i = 0; i < L.hd.n; ++i)
    assert(L.l[i].lv && (!hold_offset(L.hd.k[i]) || L.l[i].nv) && L.l[i].m && L.l[i].eo && (!prepped || L.l[i].ry) && "标签不全 / 未预处理 (prep_label)");
  assert(L.cap > 0.0 && "满仓 cap 未给 (Labels::cap = n_full)");
}

// ---- 尾部 (见文件头【尾部】) ----
inline constexpr int kTails = 3;
// 尾部 j 占端组的比例 (j 越大越极端)
FACTOR_HD inline constexpr float tail_frac(int j) { return j == 0 ? 0.5f : j == 1 ? 0.2f
                                                                                  : 0.05f; }
// r16 阈值: 顶端 r ≥ ⌈(1 − q)(kRankMax + 1)⌉, 底端 r < ⌈q (kRankMax + 1)⌉, q = tail_frac / kGroups (与 grp_of 组边界同口径)
FACTOR_HD inline constexpr int tail_thr(int e, int j) {
  const double q = static_cast<double>(tail_frac(j)) / kGroups, span = kRankMax + 1.0, x = (e == kTop ? 1.0 - q : q) * span;
  const int i = static_cast<int>(x);
  return i < x ? i + 1 : i;
}
FACTOR_HD inline constexpr bool in_tail(int e, unsigned r, int j) { return e == kTop ? r >= static_cast<unsigned>(tail_thr(kTop, j)) : r < static_cast<unsigned>(tail_thr(kBot, j)); }

// ---- 一级输出: 每 (持有期, 时刻) 一行; 布局 rows[h_idx * T + t] ----
struct Row {
  float ric = 0.f, rac = 0.f;
  float mkt = 0.f;                     // J 上做多收益均值 (超额基准)
  float grp[kGroups] = {};             // 组的超额持仓收益 Σe / max(cnt, cap)
  uint16_t cnt[kGroups] = {};          // 组真持有只数 (A ≤ kMaxA < 65536)
  float tail[kEnds][kTails] = {};      // 两端尾部的超额持仓收益 Σe / max(n, cap · tail_frac)
  uint16_t tail_n[kEnds][kTails] = {}; // 两端尾部真持有只数
  uint8_t ok = 0, ok_rac = 0;
};
// 多空 = ½ (G20 − G1)
FACTOR_HD inline float ls_row(const Row &r) { return 0.5f * (r.grp[end_group(kTop)] - r.grp[end_group(kBot)]); }
// r16 改读成方向 d: 反向 = kRankMax − x (高 = 做多端), 无效不动
FACTOR_HD inline uint16_t oriented(uint16_t x, int d) { return x == kRankNone || d == kFwd ? x : static_cast<uint16_t>(kRankMax - x); }

// ---- 二级输出 (逐日): 一个统计日的 ok 行数 / Σ rIC / Σ 多空 ----
struct Day {
  int n = 0;
  double ric = 0.0, ls = 0.0;
  FACTOR_HD void add(const Row &r) {
    if (r.ok)
      ++n, ric += r.ric, ls += ls_row(r);
  }
  void merge(const Day &o) { n += o.n, ric += o.ric, ls += o.ls; }
};

// ---- 三级输出: 每持有期一份 ----
// 口径戳: 字段不变但口径变时 +1, 落盘 stat 与之不符读取即作废 (下次 Run 重算)
inline constexpr int kStatSemver = 6;
struct Moments {
  float mean = 0.f, sd = 0.f, ir = 0.f, t = 0.f, pos = 0.f, skew = 0.f, kurt = 0.f;
};
struct HoldStat {
  int seg = 0; // 因子所在层的每段行数 (finish 填; 不落文件, 载入时由因子目录的层给): 年化 / 换手折算用
  HoldId hold = kHoldM1;
  int n = 0, n_rac = 0;
  int n_all = 0, n_trig = 0; // 攒过的全部行 (真实收益的分母) / 触发行 (ok 且两端至少一端非空)
  Moments ric, rac, ls;      // 逐行量 (ric / ls: ok 行; rac: ok_rac 行)
  float beta = 0.f, mono = 0.f;
  float end_n = 0.f;       // 平均每触发行一端成员数 (util_of 的分子)
  float grp[kGroups] = {}; // 组每行持仓收益的均值 (Σ / n_all)
};
// 每年独立期数 = kDaysPerYear · S / h: 独立样本口径的年化 (Sharpe 的 √期数) 走这一个
inline constexpr double periods_per_year(HoldId h, int seg) { return static_cast<double>(kDaysPerYear) * seg / hold_rows(h, geo_of(seg)); }
inline double periods_per_year(const HoldStat &s) { return periods_per_year(s.hold, s.seg); }
inline double hold_span(HoldId h, const Geo &g);
// 每年满仓轮数 = kDaysPerYear · S / h̄: 金额口径的年化 (均值超额 / 均摊成本 / 换手 = 满仓一年) 走这一个
inline double span_per_year(HoldId h, int seg) { return static_cast<double>(kDaysPerYear) * seg / hold_span(h, geo_of(seg)); }
inline double span_per_year(const HoldStat &s) { return span_per_year(s.hold, s.seg); }
// 真实收益: m = n 个 ok 行的矩, 其余 n_all − n 行记 0. 年化均值 = Σ / n_all × 每年期数 ppy
inline double ann_mean(const Moments &m, int n, int n_all, double ppy) { return n_all > 0 ? static_cast<double>(m.mean) * n / n_all * ppy : 0.0; }
// 补 0 后的年化 Sharpe: 由 (n, mean, sd) 还原 Σ / Σ², 在 n_all 行上重算 mean / sd (ddof=1)
inline double ann_sharpe(const Moments &m, int n, int n_all, double ppy) {
  if (n < 3 || n_all < 2)
    return 0.0;
  const double nn = n, N = n_all, mu = m.mean, sd = m.sd;
  const double s1 = nn * mu, s2 = (nn - 1.0) * sd * sd + nn * mu * mu;
  const double M = s1 / N, v = (s2 - N * M * M) / (N - 1.0);
  return v > 0.0 ? M / std::sqrt(v) * std::sqrt(ppy) : 0.0;
}
// LS 每期真实收益 / 年化 / 年化 Sharpe (派生量, 不落文件)
inline double ls_period(const HoldStat &s) { return s.n_all > 0 ? static_cast<double>(s.ls.mean) * s.n / s.n_all : 0.0; }
inline double ls_annual(const HoldStat &s) { return ann_mean(s.ls, s.n, s.n_all, span_per_year(s)); }
inline double sharpe_of(const HoldStat &s) { return ann_sharpe(s.ls, s.n, s.n_all, periods_per_year(s)); }
// 端 e 的每期真实收益 (超额; 与 ls_period 同分母)
inline double end_period(const HoldStat &s, int e) { return s.grp[end_group(e)]; }
// 触发率 = 触发行 / ok 行 (诊断)
inline double trigger_of(const HoldStat &s) { return s.n > 0 ? static_cast<double>(s.n_trig) / s.n : 0.0; }
// 两端利用率 = 平均每触发行一端成员数 / 满仓 cap, 不封顶 (诊断)
inline double util_of(const HoldStat &s, double cap) {
  assert(cap > 0.0);
  return static_cast<double>(s.end_n) / cap;
}

// ---- 共享公式 (FACTOR_HD: 两后端同一份) ----

// float → 保序无符号键 (a < b ⟺ ord(a) < ord(b)); ±0 先归一为 +0
FACTOR_HD inline unsigned ord(float f) {
  const unsigned u = __builtin_bit_cast(unsigned, f + 0.f);
  return (u & 0x80000000u) ? ~u : (u | 0x80000000u);
}

// 并列均秩 r16: pct = (avg_rank − 1)/(n − 1), avg_rank = less + (eq + 1)/2, r16 = round(pct · kRankMax) (全整数)
FACTOR_HD inline uint16_t r16_of(int less, int eq, int n) {
  assert(n >= 2 && eq >= 1 && less >= 0 && less + eq <= n);
  const long long num = (2LL * less + eq - 1) * kRankMax;
  const long long den = 2LL * (n - 1);
  return static_cast<uint16_t>((2 * num + den) / (2 * den));
}

// TS 口径: 分位 x ∈ [0,1] 直接量化 r16 = round(x · kRankMax). double 内精确
FACTOR_HD inline uint16_t r16_quant(float x) {
  assert(x >= 0.f && x <= 1.f && "TS 口径的因子值须是分位 ∈ [0,1] (根 TsRankRoll)");
  return static_cast<uint16_t>(static_cast<double>(x) * kRankMax + 0.5);
}

// 组号: r16 等分 kGroups 组
FACTOR_HD inline int grp_of(unsigned r16) {
  const int g = static_cast<int>(r16 * kGroups / (kRankMax + 1));
  return g < kGroups - 1 ? g : kGroups - 1;
}

// 段末尾部 (不开仓): 分钟型 exit 锚点越过最后可成交锚点 / 收盘型 entry 锚点越过; 日级型无尾部. t = 绝对行号
FACTOR_HD inline bool tail_masked(int t, HoldId h, const Geo &g) {
  const HoldKind k = hold_kind(h);
  return (k == HoldKind::Min || k == HoldKind::Close) && g.minute(t % g.S) + (k == HoldKind::Min ? hold_n(h) : 0) > kLastAnchorRow;
}
// 行 t 按收盘价建仓: 行末分钟越过最后可成交锚点, 价点 E 退化为 C(D) (日层每行都是). t = 绝对行号
FACTOR_HD inline bool close_entry(int t, const Geo &g) { return g.minute(t % g.S) > kLastAnchorRow; }
// 交接: 行 t 建仓与行 t − h 平仓是同一价点 (偏移型恒是, 锚点型从不). t = 绝对行号
FACTOR_HD inline bool hold_nets(int t, HoldId h, const Geo &g) {
  const int r = hold_rows(h, g);
  if (t < r || tail_masked(t, h, g) || tail_masked(t - r, h, g))
    return false;
  return hold_offset(h);
}

// ---- 仓位感知 (Row / 尾部 / 换手成本 / 池篮子 / 单只同一份) ----

// 行 t 的批沿交接链的几何: t = 块内行号, 交接按绝对行 t0_abs + u 判; 前一批越过 t 前 kChainDays 日 / 块外视为断链
struct Chain {
  int t = 0, T = 0, A = 0, h = 0, t0_abs = 0;
  HoldId hold = kHoldM1;
  Geo g;
  FACTOR_HD bool nets(int u) const { // 行 u 的批与行 u − h 的批交接
    return u >= h && u < T && t - (u - h) <= kChainDays * g.S && hold_nets(t0_abs + u, hold, g);
  }
};
FACTOR_HD inline Chain chain_of(int t, const Span &s, HoldId hold) { return {t, s.T, s.A, hold_rows(hold, s.g), s.t0_abs, hold, s.g}; }

// 格在行 t 的批里是否真持有: 行 t 价点可买 → 是; 否则须与前一批交接且前一批同一组合 (m ∧ in(code)) 并真持有, 沿链回走.
//   code / m / eo 已偏到行 t 的本格 (行 u 的格 = p[(u − t)·A]); 调用方保证本格在行 t 是成员
template <class Code, class In>
FACTOR_HD inline bool held(const Chain &c, const Code *code, const uint8_t *m, const uint8_t *eo, const In &in) {
  for (int u = c.t;; u -= c.h) {
    const ptrdiff_t o = static_cast<ptrdiff_t>(u - c.t) * c.A;
    if (eo[o] & kEoBuy)
      return true;
    if (!c.nets(u))
      return false;
    const ptrdiff_t p = o - static_cast<ptrdiff_t>(c.h) * c.A;
    if (!m[p] || !in(code[p]))
      return false;
  }
}
// 行 t 的批平仓时, 下一批 (t + h) 交接且同一组合接着持有 → 不真平仓
template <class Code, class In>
FACTOR_HD inline bool continues(const Chain &c, const Code *code, const uint8_t *m, const In &in) {
  if (!c.nets(c.t + c.h))
    return false;
  const ptrdiff_t o = static_cast<ptrdiff_t>(c.h) * c.A;
  return m[o] && in(code[o]);
}
// 做多一格的收益: 续持 → 名义价点估值 (下一批价点可卖 → lv 就是名义值; 否则 nv); 不续持 → lv (含平仓顺延). y = {lv, nv}
FACTOR_HD inline float hold_ret(bool cont, uint8_t eo_next, const float *y) { return !cont || (eo_next & kEoSell) ? y[0] : y[1]; }
// 一格的交接链视图 (Code = 成员码平面: rx / 端码 / 池位集 …; in(code) = 是否同一组合, 由调用方给)
template <class Code>
struct Cell {
  const Chain &c;
  const Code *code;      // 已偏到行 t 的本格
  const uint8_t *m, *eo; // 同上
  const float *y;        // 本格 {lv, nv}
  template <class In>
  FACTOR_HD bool held(const In &in) const { return stat::held(c, code, m, eo, in); }
  template <class In>
  FACTOR_HD bool continues(const In &in) const { return stat::continues(c, code, m, in); }
  FACTOR_HD float ret(bool cont) const { return hold_ret(cont, cont ? eo[static_cast<ptrdiff_t>(c.h) * c.A] : 0, y); }
};
// 本格两侧标签 {lv, nv} (fp16 位 → float; nv 空 = 0)
template <class H2F>
FACTOR_HD inline void label_cell(const Label &lb, ptrdiff_t a, const H2F &h2f, float (&y)[2]) {
  y[0] = h2f(lb.lv[a]);
  y[1] = lb.nv ? h2f(lb.nv[a]) : 0.f;
}
// 平仓行: 行 t 这一批的收益在哪一行实现. 偏移型 = t + h; 收盘型 = 当日末行; T+N 开盘型 = D+N 日首行. 顺延不改实现行
FACTOR_HD inline int hold_exit_row(int t, HoldId h, const Geo &g) {
  if (hold_offset(h))
    return t + hold_rows(h, g);
  const int d = t / g.S + hold_days(h);
  return hold_kind(h) == HoldKind::Open ? d * g.S : d * g.S + g.S - 1;
}
// 行 s 的标签在哪一行收盘时已实现 (N 元回归的样本行): 平仓行的价点成交即知; 分钟价点 E 至迟下一行收盘可知 (+1)
FACTOR_HD inline int label_known(int s, HoldId h, const Geo &g) {
  const bool at_e = hold_offset(h) && (hold_kind(h) == HoldKind::Min || !close_entry(s, g));
  return hold_exit_row(s, h, g) + (at_e ? 1 : 0);
}
// 行 t 可用的最末标签行 u(t) (无 → −1)
FACTOR_HD inline int label_last(int t, HoldId h, const Geo &g) {
  int u = t;
  while (u >= 0 && label_known(u, h, g) > t)
    --u;
  return u;
}
// t − u(t) 的上界 (行): N 元回归的回看 = d + 它
inline int label_reach(HoldId h, const Geo &g) {
  const int t0 = (hold_days(h) + 2) * g.S + hold_rows(h, g) + 1;
  int r = 0;
  for (int t = t0; t < t0 + g.S; ++t)
    r = std::max(r, t - label_last(t, h, g));
  return r;
}
// 资金口径行数 h̄: 一日各开仓行的在持行数 (建仓行起, hold_exit_row 止; 尾部行不开仓不计) 之和 / S
inline double hold_span(HoldId h, const Geo &g) {
  int s = 0;
  for (int j = 0; j < g.S; ++j)
    s += tail_masked(j, h, g) ? 0 : hold_exit_row(j, h, g) - j;
  assert(s > 0);
  return static_cast<double>(s) / g.S;
}

// ---- 换手成本 ----

// 成交两侧 (冲击列 / 成本账同一下标)
enum Side : int { kBuy = 0,
                  kSell = 1 };
inline constexpr int kSides = 2;
// 一个金额段: 盘口段 = 两侧冲击 [T][A] 平面的指针 (原值, 池内盘口缺 = +inf) + 封顶 top; 固定段 = 空
struct Cost {
  const uint16_t *imp[kSides] = {};
  float top = 0.f;
  FACTOR_HD bool flat() const { return !imp[kBuy]; }
  FACTOR_HD Cost at(size_t i) const { return flat() ? Cost{} : Cost{{imp[kBuy] + i, imp[kSell] + i}, top}; }
};

// 端码: r16 (已定向) → 高端组 kEcTop / 低端组 kEcBot / 其他组 kEcMid / rank 无效 kEcVoid. 前两个与 End 同值
enum EndCode : uint8_t { kEcTop = kTop,
                         kEcBot = kBot,
                         kEcMid,
                         kEcVoid };
FACTOR_HD inline uint8_t end_code(unsigned x) {
  const int g = x != kRankNone ? grp_of(x) : -1;
  return g < 0 ? kEcVoid : g == end_group(kTop) ? kEcTop
                       : g == end_group(kBot)   ? kEcBot
                                                : kEcMid;
}
// 尾部 j 的端码 (与 end_code 同形; 尾部换手)
FACTOR_HD inline uint8_t tail_code(unsigned x, int j) {
  return x == kRankNone ? kEcVoid : in_tail(kTop, x, j) ? kEcTop
                                : in_tail(kBot, x, j)   ? kEcBot
                                                        : kEcMid;
}

// 一本做多账本的换手成本和 (沿行求和; 量按【持仓口径】的权, 每批资金 1); 每侧 [kSides] 三和, 费率在 cost 时按 Fee 现乘
struct CostBook {
  double vol[kSides] = {}; // Σ 成交量: 买 = Δ⁺ (增仓), 卖 = 交接的 Δ⁻ + 不交接批的整批平仓
  double imp[kSides] = {}; // Σ 量 · 真实冲击 (盘口段; 固定段恒 0)
  double fix[kSides] = {}; // Σ 付固定冲击的量 (固定段 = vol; 盘口段 = 0)
  double w = 0.0;          // Σ w (持仓量)
  long long n = 0;         // 样本数 (成员格数)
  // 一侧的 Σ 量 · 费率
  double paid(int s, const Fee &f) const { return imp[s] + fix[s] * f.impact + vol[s] * f.tax(s == kSell); }
};
// 一个持有期的换手成本累加器 (两端各一本); 可合并
struct CostAcc {
  CostBook book[kEnds];
  long long rows = 0; // 攒过的行数 (非尾部行各计一次)
  FACTOR_HD void merge(const CostAcc &o) {
    for (int e = 0; e < kEnds; ++e) {
      for (int s = 0; s < kSides; ++s)
        book[e].vol[s] += o.book[e].vol[s], book[e].imp[s] += o.book[e].imp[s], book[e].fix[s] += o.book[e].fix[s];
      book[e].w += o.book[e].w, book[e].n += o.book[e].n;
    }
    rows += o.rows;
  }
  // 每期真实成本 (端 e): 两侧 Σ 量 · 费率 / n_all (与 ls_period 同分母)
  double cost(int e, const Fee &f, int n_all) const {
    const CostBook &c = book[e];
    return n_all > 0 ? (c.paid(kBuy, f) + c.paid(kSell, f)) / static_cast<double>(n_all) : 0.0;
  }
  // 一侧的平均费率 (显示用): Σ 量 · 费率 / Σ 量
  double rate(int e, int s, const Fee &f) const {
    const CostBook &c = book[e];
    return c.vol[s] > 0.0 ? c.paid(s, f) / c.vol[s] : 0.0;
  }
};

// 每期净多空 = ls_period − 两端每期成本的平均; 年化 = × span_per_year
inline double net_ls_period(const HoldStat &s, const CostAcc &c, const Fee &f) {
  return ls_period(s) - 0.5 * (c.cost(kTop, f, s.n_all) + c.cost(kBot, f, s.n_all));
}
inline double net_ls(const HoldStat &s, const CostAcc &c, const Fee &f) { return net_ls_period(s, c, f) * span_per_year(s); }
// 年化换手 (倍/年, 单边, 每本资金 1): 端 e = Σ Δ⁺ / n_all × span_per_year; 两端 = 两端均
inline double turn_annual(const HoldStat &s, const CostAcc &c, int e) {
  return s.n_all > 0 ? c.book[e].vol[kBuy] / static_cast<double>(s.n_all) * span_per_year(s) : 0.0;
}
inline double turn_annual(const HoldStat &s, const CostAcc &c) { return 0.5 * (turn_annual(s, c, kTop) + turn_annual(s, c, kBot)); }

// 整数五和 + 计数 (rIC / rAC 的 Pearson 原料)
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

// 整数五和的 Pearson: 方差 / 协方差全在 64 位整数内精确, 只有最后一步除法/开方是浮点 (两后端逐位一致). ok = n ≥ 2 ∧ vx > 0 ∧ vy > 0
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

// 持仓分母 = max(只数, cap)
FACTOR_HD inline double book_den(double n, double cap) { return n > cap ? n : cap; }
// 一本的超额持仓收益 = (Σlv − 只数 · 基准) / 分母; 空 → 0
FACTOR_HD inline double book_excess(double sum, double n, double base, double cap) { return (sum - n * base) / book_den(n, cap); }
// 超额持仓收益 → 绝对持仓收益
inline double book_abs(double excess, double n, double base, double cap) { return excess + n * base / book_den(n, cap); }

// 一行标签侧的收尾 (两后端同一份): J 上整数五和 s → ric; 组和 gs / gc + 尾部和 ts / tc → grp / cnt / tail / tail_n; 基准 → mkt.
//   sm = J 上 Σlv; cap = 满仓. 不 ok → r 标签侧字段保持 0
FACTOR_HD inline void finish_row(const unsigned long long *s, double sm, const double *gs, const int *gc, const double (*ts)[kTails], const int (*tc)[kTails],
                                 double cap, Row &r) {
  bool icok = false;
  const float ric = pearson_int(s, icok);
  if (!icok)
    return;
  const double mkt = sm / static_cast<double>(s[kSn]);
  r.ok = 1;
  r.ric = ric;
  r.mkt = static_cast<float>(mkt);
  for (int k = 0; k < kGroups; ++k) {
    r.cnt[k] = static_cast<uint16_t>(gc[k]);
    r.grp[k] = static_cast<float>(book_excess(gs[k], gc[k], mkt, cap));
  }
  for (int e = 0; e < kEnds; ++e)
    for (int j = 0; j < kTails; ++j) {
      r.tail_n[e][j] = static_cast<uint16_t>(tc[e][j]);
      r.tail[e][j] = static_cast<float>(book_excess(ts[e][j], tc[e][j], mkt, cap * tail_frac(j)));
    }
}

// ---- 三级汇总 (主机, double; 两后端共用) ----

// 组号 0..K−1 与 g[k] 的 Spearman (g 并列均秩); g 全并列 → 0
inline double mono_of(const double *g, int K) {
  double rk[kGroups];
  for (int i = 0; i < K; ++i) {
    int less = 0, eq = 0;
    for (int j = 0; j < K; ++j) {
      less += g[j] < g[i];
      eq += g[j] == g[i];
    }
    rk[i] = less + (eq + 1) * 0.5;
  }
  const double mu = (K + 1) * 0.5;
  double cxy = 0.0, vx = 0.0, vy = 0.0;
  for (int i = 0; i < K; ++i) {
    const double dx = (i + 1) - mu, dy = rk[i] - mu;
    cxy += dx * dy;
    vx += dx * dx;
    vy += dy * dy;
  }
  return vy > 0.0 ? cxy / std::sqrt(vx * vy) : 0.0;
}

// 一个逐行量 (rIC / rAC / LS 同一份) 的幂和 → Moments; n < 3 → 全 0
struct MomAcc {
  int n = 0, npos = 0, nneg = 0;
  double s1 = 0.0, s2 = 0.0, s3 = 0.0, s4 = 0.0;
  FACTOR_HD void add(float v) {
    ++n;
    npos += v > 0.f, nneg += v < 0.f;
    const double x = v, x2 = x * x;
    s1 += x, s2 += x2, s3 += x2 * x, s4 += x2 * x2;
  }
  FACTOR_HD void merge(const MomAcc &o) {
    n += o.n, npos += o.npos, nneg += o.nneg;
    s1 += o.s1, s2 += o.s2, s3 += o.s3, s4 += o.s4;
  }
  // 逐行量取负后的幂和 (奇次变号, 正负计数互换)
  MomAcc negated() const {
    MomAcc m = *this;
    m.npos = nneg, m.nneg = npos;
    m.s1 = -s1, m.s3 = -s3;
    return m;
  }
  // h = 持有期折成的行数 (hold_rows; t 值的重叠折算 n/h)
  Moments finish(int h) const {
    Moments m;
    if (n < 3)
      return m;
    const double nn = n, mu = s1 / nn;
    const double m2 = std::fmax(s2 - nn * mu * mu, 0.0);
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
  // 标量头: 逐行量的矩 + beta 的幂和 + 行计数 (GPU 上 lane 0 攒)
  struct Head {
    MomAcc ric, rac, ls;
    double smk = 0.0, smk2 = 0.0, slm = 0.0; // mkt 幂和 + ls·mkt 交叉 (beta)
    int n_all = 0, n_trig = 0;
    FACTOR_HD void add(const Row &w) {
      ++n_all;
      if (w.ok) {
        ric.add(w.ric);
        const double mk = w.mkt;
        smk += mk, smk2 += mk * mk;
        n_trig += w.cnt[end_group(kTop)] + w.cnt[end_group(kBot)] > 0;
        const float v = ls_row(w);
        ls.add(v);
        slm += static_cast<double>(v) * mk;
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
  // 一个组: 组持仓收益 / 组样本数 沿 t 的和 (GPU 上 lane k 攒组 k; 只收 ok 行)
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
  // seg = 因子所在层的每段行数 (折算 / 年化); n < 3 → 标签侧全 0, rac 照算
  HoldStat finish(HoldId hold, int seg) const {
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
      g[k] = grp[k].s / static_cast<double>(s.n_all);
    const double nn = s.n, mmk = head.smk / nn;
    const double vmk = std::fmax(head.smk2 - nn * mmk * mmk, 0.0);
    if (vmk > 0.0)
      s.beta = static_cast<float>((head.slm - head.ls.s1 * mmk) / vmk);
    for (int k = 0; k < kGroups; ++k)
      s.grp[k] = static_cast<float>(g[k]);
    s.mono = static_cast<float>(mono_of(g, kGroups));
    const long long ne = grp[end_group(kTop)].c + grp[end_group(kBot)].c;
    s.end_n = s.n_trig > 0 ? static_cast<float>(static_cast<double>(ne) / (2.0 * s.n_trig)) : 0.f;
    return s;
  }
};
// 正向攒的累加器改读成方向 d (Search 定向用): 反向 = rIC / LS 幂和取负, 组倒序
inline Accum oriented(Accum a, int d) {
  if (d == kFwd)
    return a;
  Accum::Head &h = a.head;
  h.ric = h.ric.negated();
  h.ls = h.ls.negated();
  h.slm = -h.slm;
  for (int k = 0; k < kGroups / 2; ++k)
    std::swap(a.grp[k], a.grp[kGroups - 1 - k]);
  return a;
}

// 流程出口 Accum[hd.n] → HoldStat[hd.n]
inline void finish_all(const Accum *acc, const Holds &hd, int seg, HoldStat *out) {
  assert(acc && out);
  for (int i = 0; i < hd.n; ++i)
    out[i] = acc[i].finish(hd.k[i], seg);
}

} // namespace factor::stat
