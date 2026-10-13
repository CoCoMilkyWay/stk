#pragma once

// =============================================================================
// Search: 参数搜索评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 建在 Stat/Contract.hpp 之上)
// =============================================================================
//   定位  同一因子表达式的一批参数候选 (C 张因子平面, 同池 g / 同口径 Frame / 同标签集), 每候选每持有期直接攒二级累加器 Accum,
//         不落一级 Row. 语义 = Stat: 候选 c 的 acc[h].finish(hold) 与 Stat 对同一平面算出的 HoldStat 逐字段一致 (整数派生量逐位,
//         浮点和只差求和序; 对拍 Search/Check.hpp). Stat 服务交互 (一因子一次, 要 Row), Search 服务搜索 (一因子上万次, 只要分数);
//         两者同一后端共用同一组行核 (Stat/Host.hpp / Stat/Dev.cuh), 只在"行怎么排 / 攒到哪"上分叉.
//         算子对持有期数 H 不设限 (hd.n ≤ kMaxHold, 对拍多 h 同跑); Search 服务只传一个 (见【目标】), 标签 / 核都只走一组.
//
//   【分块流式】超大张量沿 T 切块: 一块 = 行 [0, T) 的平面 (因子 / 标签 / 池 同块同形), 带两个绝对量:
//         t0_abs  块首行的绝对下标 (tail_masked 看 (t0_abs + t) % kSegLen)
//         t_lo    从这一行起才计入统计; [0, t_lo) 是上一块的尾巴 (carry), 只给 rAC 的 lag 行 (t − h) 用
//         块内 t − h < 0 → ok_rac = 0. 调用方保证 carry ≥ max_lag(hd) (首块除外: 整张量里 t < h 本就无 rAC) 则分块 ≡ 整张量;
//         跨块 Result::merge 续攒 (Accum 可合并). 一块内候选之间互不相干, 后端怎么并行由后端定.
//
//   【目标】只评**排序能力**, 一次搜索针对一个固定持有期 h (调用方选), 不跨 h 比 (不同周期的每笔收益 / 执行方式不可公平折算), 不看成本
//         (h ≥ 30m 时换手 ≈ 100%, 建仓冲击对参数几乎是常数, 减常数不改变 argmax; 成本 / 执行在选出因子之后独立处理):
//         score = ls_mean · util · ric.mean^ric_pow;  ls_mean / rIC 任一 ≤ 0 或 n < 3 → −∞
//         · ls_mean  每期每成员毛多空 (HoldStat; 固定 h 下年化系数是常数, 不乘)
//         · util     = util_of(HoldStat, n_full) = 平均每行一腿成员数 / 满仓只数, 封顶 1. 唯一的"活跃度"项, 不设样本数 / 显著性硬阈值:
//                    稀疏触发 (TS 门控 / 极端阈值) 的候选 util → 0, 分数自然 → 0, 10 行里挑出来的噪声 rIC 赢不了; 同时保住"分钟频 rIC
//                    是上万行均值、本身显著"的前提. n_full 按池宽定 (池内平均有效票数 / kGroups), CS 因子恒 ≈ 1.
//         · rIC      正则项: 要求全截面排序也成立, 不只尾部
//         有意不进目标的: 成本 / 换手 / mono / 显著性 (ls_t, ric.ir) / 样本外 —— 搜索要快, 活跃度由 util 定价.
//         同一份 score_of 两个入口: Search 吃 Result (Accum 先 finish), 因子看板吃 stat 里该 h 的 HoldStat —— Search 只在内部用分数挑候选,
//         不往文件写结论.
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

// 一个候选的全部输出: 每持有期一份 Accum. 跨块 merge
struct Result {
  Accum acc[kMaxHold];
  void merge(const Result &o, int H) {
    assert(H >= 1 && H <= kMaxHold);
    for (int i = 0; i < H; ++i)
      acc[i].merge(o.acc[i]);
  }
};

// ---- 目标 (主机, double; 见【目标】) ----
struct Objective {
  double n_full = 1.0;  // 满仓一腿只数 (util 的分母; Search = 池内平均有效票数 / kGroups)
  double ric_pow = 1.0; // score = ls_mean · util · ric^ric_pow
};

struct Score {
  bool ok = false;                        // n ≥ 3 (假 → 其余字段 0, score −∞)
  double ls = 0.0, util = 0.0, ric = 0.0; // 毛多空 (每期每成员) / 资金利用率 / rIC 均值
  double score = -std::numeric_limits<double>::infinity();
};

// 二级入口 (看板用: 该 h 的 HoldStat 已 finish); Search 的 Result 入口在下面, 先 finish 再走这一份
inline Score score_of(const HoldStat &hs, const Objective &o) {
  Score s;
  if (hs.n < 3)
    return s;
  s.ok = true;
  s.ls = hs.ls_mean;
  s.util = factor::stat::util_of(hs, o.n_full);
  s.ric = hs.ric.mean;
  if (s.ls > 0.0 && s.ric > 0.0)
    s.score = s.ls * s.util * (o.ric_pow == 1.0 ? s.ric : std::pow(s.ric, o.ric_pow));
  return s;
}
// hi = hd 下标 (Search 服务 hd.n == 1, hi = 0)
inline Score score_of(const Result &r, const Holds &hd, int hi, const Objective &o) {
  factor::stat::assert_holds(hd);
  assert(hi >= 0 && hi < hd.n);
  return score_of(r.acc[hi].finish(hd.h[hi]), o);
}

} // namespace factor::search
