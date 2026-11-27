#pragma once

// =============================================================================
// Roll — 滚动窗目标 (Stat 二级逐日 Sum 的派生; Search 打分 / 因子表 score / Run 分年同一份)
// =============================================================================
//   一个固定持有期 h 上: score = 中位窗 mono (按所评方向) = 评估期上全部滚动窗的窗 mono 的中位
//     · 窗 mono  窗内各组 Σ grp 对组号的 Spearman (Fold mono_of; 各组分母同为窗内行数, 不影响秩) —— 看全截面分层是否处处单调
//     · 滚动窗   窗长 W = round(D / kRollParts) 个交易日 (≥ 1; D = 评估期交易日数), 以每个交易日收尾一个窗 (满 W 日才计);
//                ok 行 < kRollMinRows 的窗不计. 中位压得住少数窗的极端行情
//     · 窗 = 逐日 Sum 的前缀之差 (组和) 与日净值 LS (Fold ls_days: 钱按平仓日) 的 Path 之差 (窗年化多空, 取各窗的中位; 只作展示)
//   【方向】Search 自动定向: 方向 = 正向中位窗 mono 的号 (sign_of); 所评方向 sign 直接进 roll_of (逐日先 oriented, 再求窗;
//     窗 mono 精确变号, 复利年化随之精确). sign 落进 weights, Run 按 sign 评一致.
// =============================================================================

#include "factor/Stat/Fold.hpp"
#include "misc/median.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <span>
#include <vector>

namespace factor::stat {

inline constexpr const char *kTargetName = "mono";     // 窗目标名 (文案)
inline constexpr const char *kScoreName = "中位 mono"; // score 的名 (表头 / 图例)
inline constexpr int kRollParts = 10;                  // 滚动窗长 = 评估期交易日数 / kRollParts (10 年 → 1 年窗)
inline constexpr int kRollMinRows = 3;                 // 一个滚动窗至少的 ok 行数

// 评估期 days 个交易日上的窗长 (交易日)
inline int roll_win(int days) {
  assert(days >= 1);
  return std::max(1, static_cast<int>(std::lround(static_cast<double>(days) / kRollParts)));
}

// 滚动窗统计 (n = 0 → 无窗, 其余为 0)
struct Roll {
  int win = 0, n = 0;           // 窗长 (交易日) / 计入的窗数
  double med = 0.0, mean = 0.0; // 窗 mono 的中位 (= score) / 平均
  double ann_med = 0.0;         // 窗年化多空的中位
  bool ok() const { return n > 0; }
};

// 正向攒的逐日 d[D] 按方向 sign (±1) → 滚动窗统计 (hold / seg = 持有期 / 因子所在层); wins 非空 → 收尾日的窗 mono [D] (不计入的窗 NaN)
inline Roll roll_of(std::span<const Sum> d, HoldId hold, int seg, int sign = 1, std::vector<double> *wins = nullptr) {
  const int D = static_cast<int>(d.size());
  Roll r;
  r.win = roll_win(D);
  if (wins)
    wins->assign(static_cast<size_t>(D), std::numeric_limits<double>::quiet_NaN());
  const std::vector<Sum> o = oriented(d, sign);
  std::vector<Sum> P(static_cast<size_t>(D) + 1); // 前缀 (下标 = 日 + 1)
  Path L;                                         // 日净值 LS
  const std::vector<double> ls = ls_days(o, hold, seg);
  for (size_t i = 0; i < o.size(); ++i) {
    P[i + 1] = P[i];
    P[i + 1].merge(o[i]);
    L.add(ls[i]);
  }
  misc::RunMedian<double> med_m, ann_m;
  double med_s = 0.0;
  for (int t = r.win - 1; t < D; ++t) {
    const size_t a = static_cast<size_t>(t + 1 - r.win), b = static_cast<size_t>(t) + 1;
    const Sum wd = P[b] - P[a];
    if (wd.ok() < kRollMinRows)
      continue;
    const double w = mono_of(wd.grp, kGroups);
    ++r.n, med_s += w;
    med_m.push(w), ann_m.push(L.win(a, b).ann());
    if (wins)
      (*wins)[static_cast<size_t>(t)] = w;
  }
  if (r.n > 0)
    r.med = med_m.mid(), r.mean = med_s / r.n, r.ann_med = ann_m.mid();
  return r;
}

// 正向攒的滚动窗统计 → 所评方向 (+1 / −1)
inline int sign_of(const Roll &fwd) { return fwd.med >= 0.0 ? 1 : -1; }

// 打分 (Search 挑候选 / 因子表排序同一份): 定向后的中位窗 mono; 无窗 → −∞
inline double score_of(const Roll &r) { return r.ok() ? r.med : -std::numeric_limits<double>::infinity(); }

} // namespace factor::stat
