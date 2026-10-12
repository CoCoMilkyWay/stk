#pragma once

// =============================================================================
// Search: 参数搜索评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 建在 Stat/Contract.hpp 之上)
// =============================================================================
//   定位  同一因子表达式的一批参数候选 (C 张因子平面, 同池 g / 同口径 Frame / 同标签集), 每候选每持有期直接攒二级累加器 Accum,
//         不落一级 Row, 外加打分要的两格建仓冲击池化. 语义 = Stat: 候选 c 的 acc[h].finish(hold) 与 Stat 对同一平面算出的 HoldStat
//         逐字段一致 (整数派生量逐位, 浮点和只差求和序; 对拍 Search/Check.hpp). Stat 服务交互 (一因子一次, 要 Row), Search 服务搜索
//         (一因子上万次, 只要分数); 两者同一后端共用同一组行核 (Stat/Host.hpp / Stat/Dev.cuh), 只在"行怎么排 / 攒到哪"上分叉.
//
//   【分块流式】超大张量沿 T 切块: 一块 = 行 [0, T) 的平面 (因子 / 标签 / 成本 / 池 同块同形), 带两个绝对量:
//         t0_abs  块首行的绝对下标 (tail_masked 看 (t0_abs + t) % kSegLen)
//         t_lo    从这一行起才计入统计; [0, t_lo) 是上一块的尾巴 (carry), 只给 rAC 的 lag 行 (t − h) 用
//         块内 t − h < 0 → ok_rac = 0. 调用方保证 carry ≥ max_lag(hd) (首块除外: 整张量里 t < h 本就无 rAC) 则分块 ≡ 整张量;
//         跨块 Result::merge 续攒 (Accum 可合并). 一块内候选之间互不相干, 后端怎么并行由后端定.
//
//   【成本】= Stat 契约【换手成本】(Cost / CostAcc / cost_cell / net_ls 都定义在那边, Inspect 期限结构净线同一份): 顶 / 底组按真实换手扣,
//         每 (t, h) 行只有新进者 (t−h 不在该组) 付建仓冲击 + sell_impact. 本算子只加块语义: 只在有 lag 行 (块内 t ≥ h; carry 保证非首块全有)
//         的行上攒. 单一金额档 (如 5W) 由调用方选定.
//
//   【目标】量的是**固定资金上的年化收益**, 再乘 rIC (两者都要高):
//         ls_net(h) = net_ls = (ls_mean(h) − 顶组均摊成本 − 底组均摊成本) · periods_per_year(h)   每期每成员口径的年化净多空
//         util(h)   = CostAcc::util(n_full) = 平均每行一腿可交易成员数 / 满仓只数, 封顶 1          资金利用率: 闲置资金记 0
//         h* = argmax_{h ∈ [h_lo, h_hi]} ls_net(h) · util(h);  score = ls_net(h*) · util(h*) · ric.mean(h*)^ric_pow;  ls_net / rIC 任一 ≤ 0 → −∞.
//         · 年化系数随 h 变 1000 倍, 跨 h 比的是单位资金年收益, 短持有期靠周转补回 (与 Inspect plot_term 的净线同轴).
//         · util 是唯一的"活跃度"项, 不设样本数 / 显著性硬阈值: 稀疏触发 (TS 门控 / 极端阈值) 的候选 util → 0, 分数自然 → 0, 10 行里挑出来的
//           噪声 rIC 赢不了; 同时保住"分钟频 rIC 是上万行均值、本身显著"的前提. n_full 按池宽定 (池内平均有效票数 / kGroups), CS 因子恒 1.
//         · h* 只在 [h_lo, h_hi] (默认 30m ~ T+1) 里选: 更短的持有期每笔 bp 盖不过往返成本, 更长的不是日内范畴; 窗外照算只画期限结构.
//         有意不进目标的: mono / 换手 / 显著性 (ls_t, ric.ir) / 样本外 / 活跃度硬阈值 —— 搜索要快; 换手由成本定价, 活跃度由 util 定价.
//         同一份 score_of 两个入口: Search 吃 Result (Accum 先 finish), Stat 服务 / 因子看板吃 HoldStat[] + CostAcc[] (Run 每因子每金额档
//         都攒了 CostAcc 落 stat) —— 看板的 h* / util 列是 stat 算的, Search 只在内部用分数挑候选, 不往文件写结论.
// =============================================================================

#include "factor/Stat/Contract.hpp"

#include <cassert>
#include <cmath>
#include <limits>

namespace factor::search {

using factor::stat::Accum;
using factor::stat::Holds;
using factor::stat::HoldStat;
using factor::stat::kGroups;
using factor::stat::kMaxHold;

// rAC 需要的最大 lag 行数 (= carry 下限)
inline int max_lag(const Holds &hd) {
  int m = 0;
  for (int i = 0; i < hd.n; ++i)
    m = m > factor::stat::hold_minutes(hd.h[i]) ? m : factor::stat::hold_minutes(hd.h[i]);
  return m;
}

// 成本输入 / 累加器 / 净多空公式全在 Stat 契约 (【换手成本】), 与 Inspect 期限结构净线同一份; 这里只是别名
using factor::stat::Cost;
using factor::stat::CostAcc;
using factor::stat::net_ls;

// 一个候选的全部输出: 每持有期一份 Accum + 一份 CostAcc. 跨块 merge
struct Result {
  Accum acc[kMaxHold];
  CostAcc cost[kMaxHold];
  void merge(const Result &o, int H) {
    assert(H >= 1 && H <= kMaxHold);
    for (int i = 0; i < H; ++i) {
      acc[i].merge(o.acc[i]);
      cost[i].merge(o.cost[i]);
    }
  }
};

// ---- 目标 (主机, double; 见【目标】) ----
struct Objective {
  double sell_impact = 0.0;                         // 平仓固定冲击 (Config::sell_impact), 一腿一次
  double n_full = 1.0;                              // 满仓一腿只数 (util 的分母; Search = 池内平均有效票数 / kGroups)
  int h_lo = 30, h_hi = factor::stat::hold_open(1); // h* 只在 [h_lo, h_hi] (持有期键, 闭区间) 里选; 窗外持有期照算 (期限结构), 不进目标
  double ric_pow = 1.0;                             // score = ls_net · util · ric^ric_pow
};
inline bool in_window(const Objective &o, int hold) {
  return factor::stat::hold_minutes(hold) >= factor::stat::hold_minutes(o.h_lo) && factor::stat::hold_minutes(hold) <= factor::stat::hold_minutes(o.h_hi);
}

struct Score {
  int best = -1; // hd 下标 (窗内); < 0 = 窗内没有一个持有期 n ≥ 3
  double ls_net = 0.0, util = 0.0, ric = 0.0;
  double score = -std::numeric_limits<double>::infinity();
  HoldStat hs[kMaxHold];  // 期限结构 (毛二级)
  double net[kMaxHold]{}; // 期限结构 (年化净多空; n < 3 → −∞)
  double u[kMaxHold]{};   // 期限结构 (资金利用率)
};

// 单点分数 (同一式, score_of / score_at 共用): 年化净多空 × 资金利用率 × rIC^pow; n < 3 / 净多空 ≤ 0 / rIC ≤ 0 → −∞
inline double score_pt(const Score &s, int hi, const Objective &o) {
  constexpr double kNeg = -std::numeric_limits<double>::infinity();
  if (s.hs[hi].n < 3)
    return kNeg;
  const double ls = s.net[hi], ric = s.hs[hi].ric.mean;
  if (!(ls > 0.0 && ric > 0.0))
    return kNeg;
  return ls * s.u[hi] * (o.ric_pow == 1.0 ? ric : std::pow(ric, o.ric_pow));
}

// 二级入口 (Stat 服务 / 看板用: HoldStat 已 finish, CostAcc 与 hd 同序); Search 的 Result 入口在下面, 先 finish 再走这一份
inline Score score_of(const HoldStat *hs, const CostAcc *cost, const Holds &hd, const Objective &o) {
  factor::stat::assert_holds(hd);
  assert(hs && cost);
  assert(factor::stat::hold_minutes(o.h_lo) <= factor::stat::hold_minutes(o.h_hi));
  Score s;
  constexpr double kNeg = -std::numeric_limits<double>::infinity();
  for (int i = 0; i < hd.n; ++i) {
    assert(hs[i].hold == hd.h[i] && "HoldStat 与 hd 不同序");
    s.hs[i] = hs[i];
    s.net[i] = s.hs[i].n >= 3 ? net_ls(s.hs[i], cost[i], o.sell_impact) : kNeg;
    s.u[i] = cost[i].util(o.n_full);
  }
  // h* = 窗内 ls_net · util 最大者 (固定资金上的钱, 不看 rIC); 全是 −∞ 也取一个 (best ≥ 0 只要窗内有持有期), score 由 score_pt 判
  double vbest = kNeg;
  for (int i = 0; i < hd.n; ++i) {
    if (!in_window(o, hd.h[i]))
      continue;
    const double v = s.hs[i].n >= 3 ? s.net[i] * s.u[i] : kNeg;
    if (s.best < 0 || v > vbest)
      s.best = i, vbest = v;
  }
  if (s.best < 0 || s.hs[s.best].n < 3)
    return s;
  s.ls_net = s.net[s.best];
  s.util = s.u[s.best];
  s.ric = s.hs[s.best].ric.mean;
  s.score = score_pt(s, s.best, o);
  return s;
}
inline Score score_of(const Result &r, const Holds &hd, const Objective &o) {
  factor::stat::assert_holds(hd);
  HoldStat hs[kMaxHold];
  for (int i = 0; i < hd.n; ++i)
    hs[i] = r.acc[i].finish(hd.h[i]);
  return score_of(hs, r.cost, hd, o);
}

// 固定持有期 hi 的分数 (细化阶段 h 定在粗阶段的 h*, 不逐候选重选)
inline double score_at(const Score &s, int hi, const Objective &o) { return score_pt(s, hi, o); }

} // namespace factor::search
