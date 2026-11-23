#pragma once

// =============================================================================
// Search: 参数搜索评估算子的语义契约 (CPU / GPU 两后端唯一共享件; 建在 Stat/Contract.hpp 之上)
// =============================================================================
//   定位  同一因子表达式的一批参数候选 (C 张因子平面, 同池 g / 同口径 Frame / 同标签集), 每候选每持有期直接攒二级累加器 Accum, 另出逐行 Tick,
//         不落一级 Row. 语义 = Stat: 候选 c 的 acc[h].finish(hold, seg) 与 Stat 对同一平面算出的 HoldStat 逐字段一致, Tick 与 Stat 一级 Row
//         的 ok / ric / ls_row 一致 (整数派生量逐位, 浮点和只差求和序; 对拍 Search/Check.hpp). Stat 服务交互 (一因子一次, 要 Row),
//         Search 服务搜索 (一因子几十个参数点, 只要 Accum 与 Tick); 两者同一后端共用同一组行核 (Stat/Host.hpp / Stat/Dev.cuh),
//         只在"行怎么排 / 攒到哪"上分叉. 算子对持有期数 H 不设限 (hd.n ≤ kMaxHold), 因子平面只算一遍, 多 h 只多行统计;
//         Search 服务只传上层给定的一个持有期 (搜哪个周期由上层定).
//
//   【分块流式】超大张量沿 T 切块: 一块 = 行 [0, T) 的平面 (因子 / 标签 / 池 同块同形), 带三个量 (Span):
//         t0_abs  块首行的绝对下标 (tail_masked / hold_nets 看绝对行)
//         t_lo    统计首行; 前带 [0, t_lo) 只给 因子回看 / rAC 的 lag 行 (t − h) / 交接链回走 (Stat 契约【仓位感知】: 至多 kChainDays 日,
//                 换手成本的前一批再早 h 行)
//         t_back  后带行数; 统计行 [t_lo, T − t_back), 后带只给续持 (行 t + h 的批) 用
//         块内 t − h < 0 → ok_rac = 0, 链越块视为断链. 调用方保证 前带 ≥ 因子回看 + hold_reach + kChainDays 日、后带 ≥ hold_reach
//         (评估域首 / 尾块除外: 整张量在那里同样断链) 则分块 ≡ 整张量; 跨块 Result::merge 续攒 (Accum 可合并), Tick 各块只出本块统计行.
//         一块内候选之间互不相干, 后端怎么并行由后端定.
//
//   【逐行 Tick】ticks[(c·H + h)·T + t] = 候选 c 持有期 h 在行 t 的 tick_of(Row): ok = Row::ok, ric = Row::ric, ls = ls_row (不 ok 全 0).
//         只写统计行 [t_lo, t_hi), 其余行不动. 恒按正向攒 (反向由 flipped 改读).
//
//   【目标】只评**排序能力**, 一次搜索针对一个固定持有期 h (调用方选, 限 Config kSearchHolds: search_hold), 不跨 h 比, 不看成本 / 多空 / 分层:
//         score = 中位 rIC (按方向, 见【方向】) = 评估期上全部滚动窗 rIC 的中位
//         · 逐日     Tick 按交易日折成 DaySum (ok 行数 / Σ rIC / Σ 多空); Run 的 Probe YearP 逐日攒同一份
//         · 滚动窗   窗长 W = round(D / kRollParts) 个交易日 (≥ 1; D = 评估期交易日数, 即窗长恒为评估期的 1/kRollParts), 以每个交易日
//                    收尾一个窗 (满 W 日才计); 窗 rIC = 窗内 ok 行 rIC 的均值, ok 行 < kRollMinRows 的窗不计.
//                    中位压得住少数窗的极端行情: 只有大多数时段都排得对的参数才高 (与 Beta 选池的累计中位夏普同一思路)
//         · 窗 rIC 线 收尾日 t 的值 = 该日收尾的窗 rIC (不计入的窗 NaN; 全线的中位 = 中位 rIC); 只作展示
//         · 年化多空 窗内 Σ 多空 / (W·S) × span_per_year (不 ok 行记 0, 与 ls_annual 同口径), 取各窗的中位 / 平均; 只作展示, 不进目标
//         不进目标的: 分层 mono / 多空 / 成本 / 换手 / 显著性 —— 分层能力就是 rIC, 尾部行为另由观察处理.
//   【方向】Search 自动定向: 方向 d = 正向中位 rIC 的号 (≥ 0 正向 kFwd, 否则反向 kRev; Stat 契约【方向 Dir】), sign = ±1. 反向 = 逐行 rIC /
//         多空变号, 中位与均值随之变号 (flipped), 定向后中位 rIC ≥ 0. sign 落进 weights, Run / Inspect / Portfolio 按 sign 的方向评同一个表达式;
//         Run 逐日攒的已是定向后的行, 同一份 roll_of 出同一个中位 rIC (因子表 score_of).
// =============================================================================

#include "factor/Search/Config.hpp"
#include "factor/Stat/Contract.hpp"
#include "misc/median.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <span>
#include <vector>

namespace factor::search {

using factor::stat::Accum;
using factor::stat::Holds;
using factor::stat::HoldStat;
using factor::stat::kGroups;
using factor::stat::kMaxHold;

// 可搜的持有期档 (见【目标】): Config kSearchHolds
inline constexpr bool search_hold(int hold) {
  for (const int h : kSearchHolds)
    if (h == hold)
      return true;
  return false;
}

// 最长持有期的行数 h (rAC 的 lag / 续持前看 / 前一批的链; 分块前后带都按它, 【分块流式】; seg = 因子所在层)
inline int hold_reach(const Holds &hd, int seg) {
  int m = 0;
  for (int i = 0; i < hd.n; ++i)
    m = std::max(m, factor::stat::hold_rows(hd.k[i], geo_of(seg)));
  return m;
}

// 一个候选的二级输出: 每持有期一份 Accum. 跨块 merge
struct Result {
  Accum acc[kMaxHold];
  void merge(const Result &o, int H) {
    assert(H >= 1 && H <= kMaxHold);
    for (int i = 0; i < H; ++i)
      acc[i].merge(o.acc[i]);
  }
};

// 一行的逐行量 (见【逐行 Tick】)
struct Tick {
  float ric = 0.f, ls = 0.f;
  uint8_t ok = 0;
};
FACTOR_HD inline Tick tick_of(const factor::stat::Row &r) {
  Tick k;
  if (r.ok)
    k.ric = r.ric, k.ls = factor::stat::ls_row(r), k.ok = 1;
  return k;
}

// ---- 目标 (主机, double; 见【目标】【方向】) ----
inline constexpr int kRollMinRows = 3; // 一个滚动窗至少的 ok 行数

// 一个交易日的 ok 行数 / Σ rIC / Σ 多空
struct DaySum {
  int n = 0;
  double ric = 0.0, ls = 0.0;
  void add(const Tick &k) {
    if (k.ok)
      ++n, ric += k.ric, ls += k.ls;
  }
};

// 评估期 days 个交易日上的窗长 (交易日)
inline int roll_win(int days) {
  assert(days >= 1);
  return std::max(1, static_cast<int>(std::lround(static_cast<double>(days) / kRollParts)));
}

// 滚动窗统计 (已按所评方向; n = 0 → 无窗, 其余为 0)
struct Roll {
  int win = 0, n = 0;                   // 窗长 (交易日) / 计入的窗数
  double ric_med = 0.0, ric_mean = 0.0; // 窗 rIC 的中位 (= score) / 平均
  double ann_med = 0.0, ann_mean = 0.0; // 窗年化多空的中位 / 平均
  bool ok() const { return n > 0; }
};

// 逐日和 d[D] → 滚动窗统计 (hold / seg = 持有期键 / 因子所在层); line 非空 → 填窗 rIC 线 [D] (不计入的窗 NaN)
inline Roll roll_of(std::span<const DaySum> d, int hold, int seg, std::vector<float> *line = nullptr) {
  const int D = static_cast<int>(d.size());
  Roll r;
  r.win = roll_win(D);
  if (line)
    line->assign(static_cast<size_t>(D), std::numeric_limits<float>::quiet_NaN());
  std::vector<double> N(static_cast<size_t>(D) + 1, 0.0), R(N), L(N); // 前缀和 (下标 = 日 + 1)
  for (int t = 0; t < D; ++t) {
    const DaySum &x = d[static_cast<size_t>(t)];
    N[static_cast<size_t>(t) + 1] = N[static_cast<size_t>(t)] + x.n;
    R[static_cast<size_t>(t) + 1] = R[static_cast<size_t>(t)] + x.ric;
    L[static_cast<size_t>(t) + 1] = L[static_cast<size_t>(t)] + x.ls;
  }
  const double ann = factor::stat::span_per_year(hold, seg) / (static_cast<double>(r.win) * seg);
  misc::RunMedian<double> ric_m, ann_m;
  double ric_s = 0.0, ann_s = 0.0;
  for (int t = r.win - 1; t < D; ++t) {
    const size_t a = static_cast<size_t>(t + 1 - r.win), b = static_cast<size_t>(t) + 1;
    const double n = N[b] - N[a];
    if (n >= kRollMinRows) {
      const double w_ric = (R[b] - R[a]) / n, w_ann = (L[b] - L[a]) * ann;
      ++r.n, ric_s += w_ric, ann_s += w_ann;
      ric_m.push(w_ric), ann_m.push(w_ann);
      if (line)
        (*line)[static_cast<size_t>(t)] = static_cast<float>(w_ric);
    }
  }
  if (r.n > 0) {
    r.ric_med = ric_m.mid(), r.ric_mean = ric_s / r.n;
    r.ann_med = ann_m.mid(), r.ann_mean = ann_s / r.n;
  }
  return r;
}

// 反向读 (见【方向】): 各量变号 (中位与取负可交换); 窗 rIC 线由调用方同样变号
inline Roll flipped(Roll r) {
  r.ric_med = -r.ric_med, r.ric_mean = -r.ric_mean;
  r.ann_med = -r.ann_med, r.ann_mean = -r.ann_mean;
  return r;
}

// 正向攒的滚动窗统计 → 所评方向 (+1 / −1)
inline int sign_of(const Roll &fwd) { return fwd.ric_med >= 0.0 ? 1 : -1; }

// 打分 (Search 挑候选 / 因子表排序同一份): 定向后的中位 rIC; 无窗 → −∞
inline double score_of(const Roll &r) { return r.ok() ? r.ric_med : -std::numeric_limits<double>::infinity(); }

} // namespace factor::search
