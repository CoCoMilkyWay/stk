#pragma once

// =============================================================================
// Roll — 滚动窗目标 (Stat 二级 Day 的派生; Search 打分 / 因子表 score / Run 分年同一份)
// =============================================================================
//   一个固定持有期 h 上: score = 中位窗目标 (按所评方向) = 评估期上全部滚动窗的窗目标的中位
//     · 目标     编译期二选一 kTarget (Search 打分 / 定向 / 因子表 score / 分年 / agent 门槛与展示同随它):
//                  mono  窗 mono = 窗内各组 Σ grp 对组号的 Spearman (Fold mono_of; 各组分母同为窗内行数, 不影响秩) —— 看全截面分层是否处处单调;
//                  rIC   窗 rIC = 窗内 ok 行 rIC 的均值
//     · 逐日     Day (ok 行数 / Σ rIC / Σ 多空 / 各组 Σ grp), 两后端恒出
//     · 滚动窗   窗长 W = round(D / kRollParts) 个交易日 (≥ 1; D = 评估期交易日数), 以每个交易日收尾一个窗 (满 W 日才计);
//                ok 行 < kRollMinRows 的窗不计. 中位压得住少数窗的极端行情
//     · 窗目标线 收尾日 t 的值 (不计入的窗 NaN); 只作展示: mono = 该窗的窗 mono; rIC = 该窗内逐日 rIC (当日 ok 行 rIC 均值) 的中位
//     · 年化多空 窗内日净值 LS (Fold LsDays: 钱按平仓日) 的复利年化 (与 ls_annual 同口径), 取各窗的中位 / 平均; 只作展示
//   【方向】Search 自动定向: 方向 = 正向中位窗目标的号 (sign_of); 所评方向 sign 直接进 roll_of (逐日 rIC / 多空先乘 sign, 组号倒读, 再求窗;
//     窗 mono 精确变号, 复利年化随之精确). sign 落进 weights, Run 按 sign 评一致.
// =============================================================================

#include "factor/Stat/Fold.hpp"
#include "misc/median.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <span>
#include <vector>

namespace factor::stat {

enum class Target : uint8_t { Ric,
                              Mono };
inline constexpr Target kTarget = Target::Mono;
inline constexpr bool kMonoTarget = kTarget == Target::Mono;
inline constexpr const char *kTargetName = kMonoTarget ? "mono" : "rIC";          // 文案里的目标名
inline constexpr const char *kScoreName = kMonoTarget ? "中位 mono" : "中位 rIC"; // score 的名 (表头 / 图例)

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
  double med = 0.0, mean = 0.0;         // 窗目标的中位 (= score) / 平均
  double ann_med = 0.0, ann_mean = 0.0; // 窗年化多空的中位 / 平均
  bool ok() const { return n > 0; }
};

// 正向攒的逐日 d[D] 按方向 sign (±1) → 滚动窗统计 (hold / seg = 持有期 / 因子所在层); line 非空 → 窗目标线 [D]; wins 非空 → 收尾日的窗目标 [D]
//   (都是不计入的窗 NaN). 窗 = 逐日 Day 的前缀之差 (组和 / rIC) 与日净值 LS 的 Path 之差 (年化)
inline Roll roll_of(std::span<const Day> d, HoldId hold, int seg, int sign = 1, std::vector<float> *line = nullptr, std::vector<double> *wins = nullptr) {
  assert(sign == 1 || sign == -1);
  const int D = static_cast<int>(d.size());
  Roll r;
  r.win = roll_win(D);
  if (line)
    line->assign(static_cast<size_t>(D), std::numeric_limits<float>::quiet_NaN());
  if (wins)
    wins->assign(static_cast<size_t>(D), std::numeric_limits<double>::quiet_NaN());
  std::vector<Day> o(d.begin(), d.end()); // 按所评方向改读
  if (sign < 0)
    for (Day &x : o)
      x = x.reversed();
  std::vector<Day> P(static_cast<size_t>(D) + 1); // 前缀 (下标 = 日 + 1)
  Path L;                                         // 日净值 LS
  const LsDays ls(o, hold, seg);
  for (size_t i = 0; i < o.size(); ++i) {
    P[i + 1] = P[i];
    P[i + 1].merge(o[i]);
    L.add(ls[i]);
  }
  const auto win_of = [](const Day &w) {
    if constexpr (kMonoTarget)
      return mono_of(w.grp, kGroups);
    else
      return w.ric / w.n;
  };
  const auto day_ric = [&](int t) { return o[static_cast<size_t>(t)].ric / o[static_cast<size_t>(t)].n; };
  misc::RunMedian<double> med_m, ann_m;
  std::vector<double> in_win; // 窗内有 ok 行的日的逐日 rIC (升序; 只 rIC 目标的 line 用)
  double med_s = 0.0, ann_s = 0.0;
  for (int t = 0; t < D; ++t) {
    if (!kMonoTarget && line) {
      if (o[static_cast<size_t>(t)].n > 0)
        in_win.insert(std::lower_bound(in_win.begin(), in_win.end(), day_ric(t)), day_ric(t));
      if (const int x = t - r.win; x >= 0 && o[static_cast<size_t>(x)].n > 0)
        in_win.erase(std::lower_bound(in_win.begin(), in_win.end(), day_ric(x)));
    }
    if (t < r.win - 1)
      continue;
    const size_t a = static_cast<size_t>(t + 1 - r.win), b = static_cast<size_t>(t) + 1;
    const Day wd = P[b] - P[a];
    if (wd.n >= kRollMinRows) {
      const double w = win_of(wd), w_ann = L.win(a, b).ann();
      ++r.n, med_s += w, ann_s += w_ann;
      med_m.push(w), ann_m.push(w_ann);
      if (wins)
        (*wins)[static_cast<size_t>(t)] = w;
      if (line) {
        if constexpr (kMonoTarget) {
          (*line)[static_cast<size_t>(t)] = static_cast<float>(w);
        } else {
          const size_t m = in_win.size();
          (*line)[static_cast<size_t>(t)] = static_cast<float>(0.5 * (in_win[(m - 1) / 2] + in_win[m / 2]));
        }
      }
    }
  }
  if (r.n > 0) {
    r.med = med_m.mid(), r.mean = med_s / r.n;
    r.ann_med = ann_m.mid(), r.ann_mean = ann_s / r.n;
  }
  return r;
}

// 正向攒的滚动窗统计 → 所评方向 (+1 / −1)
inline int sign_of(const Roll &fwd) { return fwd.med >= 0.0 ? 1 : -1; }

// 打分 (Search 挑候选 / 因子表排序同一份): 定向后的中位窗目标; 无窗 → −∞
inline double score_of(const Roll &r) { return r.ok() ? r.med : -std::numeric_limits<double>::infinity(); }

} // namespace factor::stat
