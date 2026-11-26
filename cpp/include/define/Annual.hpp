#pragma once

// 年化口径 (全仓唯一一份: 一年 = kDaysPerYear 个交易日; 因子评估 / 组合 / 行情 / Beta 选池 / Agent 同用)
//   逐日收益序列一律走 Daily (可合并 / 前缀相减即窗): 复利年化 ann = expm1(Σ ln(1 + r) / 日数 × 每年交易日), Sharpe = mean / sd (ddof = 1) × √每年交易日
#include <cassert>
#include <cmath>
#include <limits>

inline constexpr int kDaysPerYear = 242;

// 逐日量之和 → 年化算术均值 (Σ / 日数 × 每年交易日; 换手 / 每期均值)
inline double per_year(double sum, double days) {
  assert(days > 0.0);
  return sum / days * kDaysPerYear;
}
// 逐日序列的年化 IR / Sharpe = mean / sd × √每年交易日; sd ≤ 0 → 0
inline double ann_ir(double mean, double sd) { return sd > 0.0 ? mean / sd * std::sqrt(static_cast<double>(kDaysPerYear)) : 0.0; }
// 对数收益之和 → 复利年化 (对数空间按日数折算, 换回简单收益); 爆仓 (−∞) → −1
inline double ann_log(double log_sum, double days) {
  assert(days > 0.0);
  return std::expm1(log_sum / days * kDaysPerYear);
}
// 净值几何年化: 净值 nav (起点 1) 历经 days 个交易日; nav ≤ 0 (亏光) → −1
inline double ann_geo(double nav, double days) { return nav > 0.0 ? ann_log(std::log(nav), days) : -1.0; }

// 逐日收益序列的可合并和: 日数 / Σr / Σr² / Σ ln(1 + r) (r ≤ −1 = 爆仓 → −∞); 前缀 b − 前缀 a = 窗 [a, b)
struct Daily {
  double n = 0.0, s1 = 0.0, s2 = 0.0, lg = 0.0;
  void add(double r) {
    n += 1.0, s1 += r, s2 += r * r;
    lg += r > -1.0 ? std::log1p(r) : -std::numeric_limits<double>::infinity();
  }
  void merge(const Daily &o) { n += o.n, s1 += o.s1, s2 += o.s2, lg += o.lg; }
  Daily operator-(const Daily &a) const { return {n - a.n, s1 - a.s1, s2 - a.s2, lg - a.lg}; }
  double mean() const { return n > 0.0 ? s1 / n : 0.0; }
  double sd() const { return n >= 2.0 ? std::sqrt(std::fmax(s2 - s1 * s1 / n, 0.0) / (n - 1.0)) : 0.0; }
  double log_ann() const { return n > 0.0 ? lg / n * kDaysPerYear : 0.0; } // 对数年化 (跨窗平均 / 取中位在对数空间)
  double ann() const { return n > 0.0 ? ann_log(lg, n) : 0.0; }           // 复利年化
  double sharpe() const { return ann_ir(mean(), sd()); }
};
