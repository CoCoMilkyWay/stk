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
//         t_lo    从这一行起才计入统计; [0, t_lo) 是上一块的尾巴 (carry), 只给 rank-AC 的 lag 行 (t − h) 用
//         块内 t − h < 0 → ok_ac = 0. 调用方保证 carry ≥ max_lag(hd) (首块除外: 整张量里 t < h 本就无 AC) 则分块 ≡ 整张量;
//         跨块 Result::merge 续攒 (Accum 可合并). 一块内候选之间互不相干, 后端怎么并行由后端定.
//
//   【成本】= Stat 契约【换手成本】(Cost / CostAcc / cost_cell / net_ls 都定义在那边, Inspect 期限结构净线同一份): 顶 / 底组按真实换手扣,
//         每 (t, h) 行只有新进者 (t−h 不在该组) 付建仓冲击 + sell_impact. 本算子只加块语义: 只在有 lag 行 (块内 t ≥ h; carry 保证非首块全有)
//         的行上攒. 单一金额档 (如 5W) 由调用方选定.
//
//   【目标】只看 LS 与 IC, 两者都要高: 期限结构 = 各 hold 的**年化**净多空 ls_net(h) = net_ls = (ls_mean(h) − 顶组均摊成本 − 底组均摊成本) · periods_per_year(h)
//         (年化系数随 h 变 1000 倍, 跨 h 比的是单位资金年收益, 短持有期靠周转补回; 与 Inspect plot_term 的净线同轴);
//         h* = argmax_h ls_net(h); score = ls_net(h*) · ic_mean(h*)^ic_pow; 任一 ≤ 0 → −∞ (方向反 / 无效, 直接出局).
//         有意不进目标的: mono / 换手 / 显著性 (ls_t, icir) / 样本外 —— 分钟频 IC 是上万行的均值, 本身就有显著性; 搜索要快, 不做 holdout.
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

// rank-AC 需要的最大 lag 行数 (= carry 下限)
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

// ---- 目标 (主机, double) ----
struct Objective {
  double sell_impact = 0.0; // 平仓固定冲击 (Config::sell_impact), 一腿一次
  double ic_pow = 1.0;      // score = ls_net · ic^ic_pow
};

struct Score {
  int best = -1; // hd 下标; < 0 = 没有一个持有期 n ≥ 3
  double ls_net = 0.0, ic = 0.0;
  double score = -std::numeric_limits<double>::infinity();
  HoldStat hs[kMaxHold];  // 期限结构 (毛二级)
  double net[kMaxHold]{}; // 期限结构 (年化净多空; n < 3 → −∞)
};

inline Score score_of(const Result &r, const Holds &hd, const Objective &o) {
  factor::stat::assert_holds(hd);
  Score s;
  constexpr double kNeg = -std::numeric_limits<double>::infinity();
  for (int i = 0; i < hd.n; ++i) {
    s.hs[i] = r.acc[i].finish(hd.h[i]);
    s.net[i] = s.hs[i].n >= 3 ? net_ls(s.hs[i], r.cost[i], o.sell_impact) : kNeg;
    if (s.best < 0 || s.net[i] > s.net[s.best])
      s.best = i;
  }
  if (s.best < 0 || s.hs[s.best].n < 3)
    return s;
  s.ls_net = s.net[s.best];
  s.ic = s.hs[s.best].ic_mean;
  if (s.ls_net > 0.0 && s.ic > 0.0)
    s.score = s.ls_net * (o.ic_pow == 1.0 ? s.ic : std::pow(s.ic, o.ic_pow));
  return s;
}

// 固定持有期 hi 的分数 (细化阶段 h 定在粗阶段的 h*, 不逐候选重选): 同一式, n < 3 / 任一 ≤ 0 → −∞
inline double score_at(const Score &s, int hi, const Objective &o) {
  constexpr double kNeg = -std::numeric_limits<double>::infinity();
  if (s.hs[hi].n < 3)
    return kNeg;
  const double ls = s.net[hi], ic = s.hs[hi].ic_mean;
  if (!(ls > 0.0 && ic > 0.0))
    return kNeg;
  return ls * (o.ic_pow == 1.0 ? ic : std::pow(ic, o.ic_pow));
}

} // namespace factor::search
