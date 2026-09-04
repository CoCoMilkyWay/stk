#pragma once

#include <cassert>
#include <cmath>
#include <span>
#include <vector>

// ============================================================================
// Autocorrelation Function (ACF) and Partial ACF (PACF)
// ============================================================================
//
// ACF: 自相关函数，测量序列与其滞后版本的相关性
// PACF: 偏自相关函数，去除中间滞后的影响后的相关性
//
// 用于 ARMA 模型阶数识别:
//   - ACF 截尾 + PACF 拖尾 → AR(p) 模型
//   - ACF 拖尾 + PACF 截尾 → MA(q) 模型
//   - 两者都拖尾 → ARMA(p,q) 模型
//
// ============================================================================

namespace math::timeseries {

struct ACFResult {
  std::vector<float> acf;        // [max_lag+1], acf[0] = 1.0
  std::vector<float> pacf;       // [max_lag+1], pacf[0] = 1.0
  float confidence_bound = 0.0f; // 95% 置信区间
  int cutoff_lag_acf = 0;        // ACF 首次落入置信区间的滞后
  int cutoff_lag_pacf = 0;       // PACF 首次落入置信区间的滞后
  size_t n_obs = 0;
  bool is_white_noise = false; // 所有 ACF 都在置信区间内
  bool valid = false;
};

struct ACFWorkspace {
  std::vector<double> gamma; // 自协方差
  std::vector<double> phi;   // Durbin-Levinson 系数
  std::vector<double> phi_prev;

  void ensure(size_t max_lag) {
    if (gamma.size() < max_lag + 1) {
      gamma.resize(max_lag + 1);
      phi.resize(max_lag + 1);
      phi_prev.resize(max_lag + 1);
    }
  }
};

// 计算 ACF 和 PACF
inline ACFResult compute_acf_pacf(std::span<const float> y, int max_lag, ACFWorkspace &ws) {
  ACFResult result;
  const size_t n = y.size();

  if (n < 20 || max_lag < 1)
    return result;

  max_lag = std::min(max_lag, static_cast<int>(n / 4)); // 限制最大滞后
  ws.ensure(static_cast<size_t>(max_lag));

  result.n_obs = n;
  result.acf.resize(static_cast<size_t>(max_lag) + 1);
  result.pacf.resize(static_cast<size_t>(max_lag) + 1);

  // ========== 1. 计算均值 ==========
  double mean = 0.0;
  for (size_t t = 0; t < n; ++t)
    mean += y[t];
  mean /= static_cast<double>(n);

  // ========== 2. 计算自协方差 γ_k ==========
  // γ_k = (1/n) Σ_{t=k}^{n-1} (y_t - μ)(y_{t-k} - μ)
  for (int k = 0; k <= max_lag; ++k) {
    double gk = 0.0;
    for (size_t t = static_cast<size_t>(k); t < n; ++t) {
      gk += (y[t] - mean) * (y[t - static_cast<size_t>(k)] - mean);
    }
    ws.gamma[static_cast<size_t>(k)] = gk / static_cast<double>(n);
  }

  // ========== 3. 计算 ACF: ρ_k = γ_k / γ_0 ==========
  const double gamma0 = ws.gamma[0];
  if (gamma0 <= 0.0)
    return result;

  result.acf[0] = 1.0f;
  for (int k = 1; k <= max_lag; ++k) {
    result.acf[static_cast<size_t>(k)] = static_cast<float>(ws.gamma[static_cast<size_t>(k)] / gamma0);
  }

  // ========== 4. 计算 PACF: Durbin-Levinson 递推 ==========
  result.pacf[0] = 1.0f;

  // φ_{1,1} = ρ_1
  ws.phi[1] = ws.gamma[1] / gamma0;
  result.pacf[1] = static_cast<float>(ws.phi[1]);

  for (int k = 2; k <= max_lag; ++k) {
    // 保存上一步的 φ
    for (int j = 1; j < k; ++j) {
      ws.phi_prev[static_cast<size_t>(j)] = ws.phi[static_cast<size_t>(j)];
    }

    // 计算 φ_{k,k}
    double num = ws.gamma[static_cast<size_t>(k)];
    double den = gamma0;
    for (int j = 1; j < k; ++j) {
      num -= ws.phi_prev[static_cast<size_t>(j)] * ws.gamma[static_cast<size_t>(k - j)];
      den -= ws.phi_prev[static_cast<size_t>(j)] * ws.gamma[static_cast<size_t>(j)];
    }

    if (std::abs(den) < 1e-10)
      break;

    ws.phi[static_cast<size_t>(k)] = num / den;
    result.pacf[static_cast<size_t>(k)] = static_cast<float>(ws.phi[static_cast<size_t>(k)]);

    // 更新 φ_{k,j} for j < k
    for (int j = 1; j < k; ++j) {
      ws.phi[static_cast<size_t>(j)] = ws.phi_prev[static_cast<size_t>(j)] -
                                       ws.phi[static_cast<size_t>(k)] * ws.phi_prev[static_cast<size_t>(k - j)];
    }
  }

  // ========== 5. 计算 95% 置信区间 ==========
  // Bartlett 公式: ±1.96/√n
  result.confidence_bound = 1.96f / std::sqrt(static_cast<float>(n));

  // ========== 6. 检测截尾点 ==========
  result.cutoff_lag_acf = max_lag + 1;
  result.cutoff_lag_pacf = max_lag + 1;

  for (int k = 1; k <= max_lag; ++k) {
    if (std::abs(result.acf[static_cast<size_t>(k)]) < result.confidence_bound) {
      result.cutoff_lag_acf = k;
      break;
    }
  }

  for (int k = 1; k <= max_lag; ++k) {
    if (std::abs(result.pacf[static_cast<size_t>(k)]) < result.confidence_bound) {
      result.cutoff_lag_pacf = k;
      break;
    }
  }

  // ========== 7. 白噪声检验 ==========
  result.is_white_noise = (result.cutoff_lag_acf == 1);

  result.valid = true;
  return result;
}

} // namespace math::timeseries
