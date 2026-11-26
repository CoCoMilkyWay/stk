#pragma once

// 年化口径 (全仓唯一一份: 一年 = kDaysPerYear 个交易日; 因子评估 / 组合 / 行情 / Beta 选池 / Agent 同用)
#include <cassert>
#include <cmath>

inline constexpr int kDaysPerYear = 242;

// 逐日量之和 → 年化 (Σ / 日数 × 每年交易日; 换手 / 均值收益)
inline double per_year(double sum, double days) {
  assert(days > 0.0);
  return sum / days * kDaysPerYear;
}
// 逐日序列的年化 IR / Sharpe = mean / sd × √每年交易日; sd ≤ 0 → 0
inline double ann_ir(double mean, double sd) { return sd > 0.0 ? mean / sd * std::sqrt(static_cast<double>(kDaysPerYear)) : 0.0; }
// 净值几何年化: 净值 nav (起点 1) 历经 days 个交易日 → nav^(kDaysPerYear / days) − 1; nav ≤ 0 (亏光) → −1
inline double ann_geo(double nav, double days) {
  assert(days > 0.0);
  return nav > 0.0 ? std::pow(nav, static_cast<double>(kDaysPerYear) / days) - 1.0 : -1.0;
}
// 对数收益之和 → 年化 (对数空间按日数折算, 换回简单收益)
inline double ann_log(double log_sum, double days) {
  assert(days > 0.0);
  return std::expm1(log_sum / days * kDaysPerYear);
}
