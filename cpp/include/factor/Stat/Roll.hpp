#pragma once

// =============================================================================
// Roll — 滚动窗目标 (Stat 二级 Day 的派生; Search 打分 / 因子表 score / Run 分年同一份)
// =============================================================================
//   只评**排序能力**, 一个固定持有期 h 上: score = 中位 rIC (按所评方向) = 评估期上全部滚动窗 rIC 的中位
//     · 逐日     Day (ok 行数 / Σ rIC / Σ 多空), 两后端恒出
//     · 滚动窗   窗长 W = round(D / kRollParts) 个交易日 (≥ 1; D = 评估期交易日数), 以每个交易日收尾一个窗 (满 W 日才计);
//                窗 rIC = 窗内 ok 行 rIC 的均值, ok 行 < kRollMinRows 的窗不计. 中位压得住少数窗的极端行情
//     · 窗中位 rIC 线 收尾日 t 的值 = 该窗内逐日 rIC (当日 ok 行 rIC 均值) 的中位 (不计入的窗 NaN); 只作展示
//     · 年化多空 窗内日净值 LS (逐日 Σ ls_row / h̄, 不 ok 行记 0) 的复利年化 (与 ls_annual 同口径), 取各窗的中位 / 平均; 只作展示
//   【方向】Search 自动定向: 方向 = 正向中位 rIC 的号 (sign_of); 反向读 = 各量变号 (flipped). sign 落进 weights, Run 按 sign 评一致.
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

inline constexpr int kRollParts = 10;  // 滚动窗长 = 评估期交易日数 / kRollParts (10 年 → 1 年窗)
inline constexpr int kRollMinRows = 3; // 一个滚动窗至少的 ok 行数

// 评估期 days 个交易日上的窗长 (交易日)
inline int roll_win(int days) {
  assert(days >= 1);
  return std::max(1, static_cast<int>(std::lround(static_cast<double>(days) / kRollParts)));
}

// 滚动窗统计 (n = 0 → 无窗, 其余为 0)
struct Roll {
  int win = 0, n = 0;                   // 窗长 (交易日) / 计入的窗数
  double ric_med = 0.0, ric_mean = 0.0; // 窗 rIC 的中位 (= score) / 平均
  double ann_med = 0.0, ann_mean = 0.0; // 窗年化多空的中位 / 平均
  bool ok() const { return n > 0; }
};

// 逐日 d[D] → 滚动窗统计 (hold / seg = 持有期 / 因子所在层); line 非空 → 窗中位 rIC 线 [D]; wins 非空 → 收尾日的窗 rIC [D] (都是不计入的窗 NaN)
inline Roll roll_of(std::span<const Day> d, HoldId hold, int seg, std::vector<float> *line = nullptr, std::vector<double> *wins = nullptr) {
  const int D = static_cast<int>(d.size());
  Roll r;
  r.win = roll_win(D);
  if (line)
    line->assign(static_cast<size_t>(D), std::numeric_limits<float>::quiet_NaN());
  if (wins)
    wins->assign(static_cast<size_t>(D), std::numeric_limits<double>::quiet_NaN());
  std::vector<double> N(static_cast<size_t>(D) + 1, 0.0), R(N); // 前缀和 (下标 = 日 + 1)
  std::vector<Daily> L(static_cast<size_t>(D) + 1);             // 日净值 LS 前缀
  const double span = hold_on(hold, geo_of(seg)).span();
  for (int t = 0; t < D; ++t) {
    const Day &x = d[static_cast<size_t>(t)];
    N[static_cast<size_t>(t) + 1] = N[static_cast<size_t>(t)] + x.n;
    R[static_cast<size_t>(t) + 1] = R[static_cast<size_t>(t)] + x.ric;
    L[static_cast<size_t>(t) + 1] = L[static_cast<size_t>(t)];
    L[static_cast<size_t>(t) + 1].add(x.ls / span);
  }
  const auto day_ric = [&](int t) { return d[static_cast<size_t>(t)].ric / d[static_cast<size_t>(t)].n; };
  misc::RunMedian<double> ric_m, ann_m;
  std::vector<double> in_win; // 窗内有 ok 行的日的逐日 rIC (升序; 只 line 用)
  double ric_s = 0.0, ann_s = 0.0;
  for (int t = 0; t < D; ++t) {
    if (line) {
      if (d[static_cast<size_t>(t)].n > 0)
        in_win.insert(std::lower_bound(in_win.begin(), in_win.end(), day_ric(t)), day_ric(t));
      if (const int o = t - r.win; o >= 0 && d[static_cast<size_t>(o)].n > 0)
        in_win.erase(std::lower_bound(in_win.begin(), in_win.end(), day_ric(o)));
    }
    if (t < r.win - 1)
      continue;
    const size_t a = static_cast<size_t>(t + 1 - r.win), b = static_cast<size_t>(t) + 1;
    const double n = N[b] - N[a];
    if (n >= kRollMinRows) {
      const double w_ric = (R[b] - R[a]) / n, w_ann = (L[b] - L[a]).ann();
      ++r.n, ric_s += w_ric, ann_s += w_ann;
      ric_m.push(w_ric), ann_m.push(w_ann);
      if (wins)
        (*wins)[static_cast<size_t>(t)] = w_ric;
      if (line) {
        const size_t m = in_win.size();
        (*line)[static_cast<size_t>(t)] = static_cast<float>(0.5 * (in_win[(m - 1) / 2] + in_win[m / 2]));
      }
    }
  }
  if (r.n > 0) {
    r.ric_med = ric_m.mid(), r.ric_mean = ric_s / r.n;
    r.ann_med = ann_m.mid(), r.ann_mean = ann_s / r.n;
  }
  return r;
}

// 反向读: 各量变号 (中位与取负可交换); 展示线由调用方同样变号
inline Roll flipped(Roll r) {
  r.ric_med = -r.ric_med, r.ric_mean = -r.ric_mean;
  r.ann_med = -r.ann_med, r.ann_mean = -r.ann_mean;
  return r;
}

// 正向攒的滚动窗统计 → 所评方向 (+1 / −1)
inline int sign_of(const Roll &fwd) { return fwd.ric_med >= 0.0 ? 1 : -1; }

// 打分 (Search 挑候选 / 因子表排序同一份): 定向后的中位 rIC; 无窗 → −∞
inline double score_of(const Roll &r) { return r.ok() ? r.ric_med : -std::numeric_limits<double>::infinity(); }

} // namespace factor::stat
