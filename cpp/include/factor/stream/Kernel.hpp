#pragma once

// =============================================================================
// 流式算子契约 + 公共件 (factor/stream/*.hpp 共用; 真相表见 factor/OpTable.hpp)
// =============================================================================
//   Param      运行期参数包 {d, k, k2}; 每个算子只读表里声明的字段.
//   ELEM       struct Op { static float apply(float x[, float y[, float z]], const Param &); }   无状态
//   CUM        struct Op { explicit Op(const Param &); void reset(); float push(float x[, float y]); }
//              日内 expanding: 每分钟 push 一次返回当前值, 日界 reset(). t = 本日已 push 次数 (含 NaN).
//   ROLL       struct Op { explicit Op(const Param &); void reset(); float push(float x[, float y]); }
//              D 窗: ring 存最近 d 期 (含 NaN), 窗未满 → NaN; 统计量在窗上重算 (与向量式 unfold 逐窗重算严格同义, 无递推漂移).
//   CS         struct Op { static void apply(const float *x[, const float *y[, const float *z]], float *out, size_t n, const Param &); }
//              一个时刻的截面, out 与输入不重叠.
//
//   语义契约 (流式 / 向量式 / 对拍 三方一致):
//     NaN 输入 = 跳过不计 n (Elem 除外: 逐值算术自然传播); n < min_n 或 分母 = 0 → NaN.
//     累加一律 double, 输出 float; 方差用中心化两遍 (Σ(x−μ)²), 不用 Σx²−nμ² (fp32 抵消, 见 Tod.hpp 注释).
//     rank 并列取均秩, pct = (avg_rank − 1)/(m − 1), m = 1 → 0.5 (与 cs::pct_rank 同).
//     矩: m_k = Σ(x−μ)^k / n (总体), skew = m3/m2^1.5, kurt = m4/m2² − 3; std/var/cov 用 ddof=1.
//   【precise-math 契约】本目录依赖 isfinite / NaN 比较, 只能编进 -fno-fast-math TU (与 Method/CS.cpp 同).
// =============================================================================

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <vector>

namespace factor {

struct Param {
  int d = 0;      // 窗口 (ROLL)
  float k = 0.f;  // 阈值 / 指数 / 桶数 / EMA 系数
  float k2 = 0.f; // 第二阈值 (TodMask 上界)
};

inline constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
inline bool ok(float v) { return std::isfinite(v); }

// ---- 一元矩累加器 (double): n, Σx, 中心矩两遍需要原值 → 这里只做 expanding (Welford 递推 M2..M4) ----
struct Moments {
  double n = 0, mean = 0, m2 = 0, m3 = 0, m4 = 0;
  void reset() { *this = {}; }
  void add(double x) { // Welford / Terriberry 一遍递推, 数值稳定
    const double n1 = n;
    n += 1;
    const double delta = x - mean, dn = delta / n, dn2 = dn * dn, term = delta * dn * n1;
    mean += dn;
    m4 += term * dn2 * (n * n - 3 * n + 3) + 6 * dn2 * m2 - 4 * dn * m3;
    m3 += term * dn * (n - 2) - 3 * dn * m2;
    m2 += term;
  }
  float var() const { return n < 2 ? kNaN : static_cast<float>(m2 / (n - 1)); }
  float std() const { return n < 2 ? kNaN : static_cast<float>(std::sqrt(m2 / (n - 1))); }
  float skew() const { return (n < 3 || m2 <= 0) ? kNaN : static_cast<float>(std::sqrt(n) * m3 / std::pow(m2, 1.5)); }
  float kurt() const { return (n < 4 || m2 <= 0) ? kNaN : static_cast<float>(n * m4 / (m2 * m2) - 3.0); }
};

// ---- 二元协方差累加器 (double, Welford 协方差): cxx/cyy/cxy = 中心叉积和 ----
struct CoStats {
  double n = 0, mx = 0, my = 0, cxx = 0, cyy = 0, cxy = 0;
  void reset() { *this = {}; }
  void add(double x, double y) {
    n += 1;
    const double dx = x - mx;
    const double dy = y - my;
    mx += dx / n;
    my += dy / n;
    cxx += dx * (x - mx);
    cyy += dy * (y - my);
    cxy += dx * (y - my);
  }
  float cov() const { return n < 2 ? kNaN : static_cast<float>(cxy / (n - 1)); }
  float corr() const { return (n < 2 || cxx <= 0 || cyy <= 0) ? kNaN : static_cast<float>(cxy / std::sqrt(cxx * cyy)); }
  float beta() const { return (n < 2 || cyy <= 0) ? kNaN : static_cast<float>(cxy / cyy); } // x 对 y 的斜率
  float resid(double x, double y) const { return (n < 2 || cyy <= 0) ? kNaN : static_cast<float>((x - mx) - cxy / cyy * (y - my)); }
};

// ---- 窗口 (ROLL): ring 存最近 d 期原值 (含 NaN), 满窗后按时间序遍历 ----
class Ring {
public:
  explicit Ring(int d) : d_(static_cast<size_t>(d)), buf_(d_, kNaN) {}
  void reset() {
    std::fill(buf_.begin(), buf_.end(), kNaN);
    head_ = 0;
    n_ = 0;
  }
  void push(float x) {
    buf_[head_] = x;
    head_ = (head_ + 1) % d_;
    if (n_ < d_)
      ++n_;
  }
  bool full() const { return n_ == d_; }
  size_t size() const { return d_; }
  float at(size_t i) const { return buf_[(head_ + i) % d_]; } // i = 0 最旧 … d−1 最新 (满窗时)
  float oldest() const { return at(0); }
  float newest() const { return at(d_ - 1); }

private:
  size_t d_, head_ = 0, n_ = 0;
  std::vector<float> buf_;
};

// ---- 有序数组统计 (CUM 的 * 类 / ROLL 重算共用) ----
namespace stat {

// 中位数: 上下中位均值 (与 cs::median_in_place 同). v 会被重排. 空 → NaN
inline float median(std::vector<float> &v) {
  if (v.empty())
    return kNaN;
  const size_t n = v.size();
  std::nth_element(v.begin(), v.begin() + n / 2, v.end());
  float m = v[n / 2];
  if (n % 2 == 0)
    m = (m + *std::max_element(v.begin(), v.begin() + n / 2)) * 0.5f;
  return m;
}

// x 在样本 v (含 x 自身) 中的 pct rank, 并列均秩. m = 1 → 0.5
inline float pct_rank(const std::vector<float> &v, float x) {
  size_t less = 0, eq = 0;
  for (float s : v) {
    if (s < x)
      ++less;
    else if (s == x)
      ++eq;
  }
  const size_t m = v.size();
  if (m <= 1)
    return 0.5f;
  const double avg_rank = static_cast<double>(less) + (static_cast<double>(eq) + 1.0) * 0.5; // 1-based
  return static_cast<float>((avg_rank - 1.0) / static_cast<double>(m - 1));
}

} // namespace stat

} // namespace factor
