#pragma once

#include <cassert>
#include <cmath>
#include <numeric>
#include <vector>

/**
 * PCA1D - Project data onto first principal component
 *
 * Algorithm:
 *   1. Center each row (subtract row mean)
 *   2. Compute covariance matrix C = X * X^T / (N-1)
 *   3. Power iteration to find first eigenvector v
 *   4. Project: scores[i] = v^T * X[:,i]
 *
 * @param data  D×N matrix (data[d][n] = dimension d, sample n)
 * @return      N scores (projection onto first PC)
 */
inline std::vector<float> pca1d(const std::vector<std::vector<float>> &data) {
  assert(!data.empty());
  const size_t D = data.size();
  const size_t N = data[0].size();
  assert(N > 1);

  // 1. Center each row
  std::vector<std::vector<float>> centered(D, std::vector<float>(N));
  for (size_t d = 0; d < D; ++d) {
    float mean = std::accumulate(data[d].begin(), data[d].end(), 0.0f) / N;
    for (size_t n = 0; n < N; ++n) {
      centered[d][n] = data[d][n] - mean;
    }
  }

  // 2. Compute covariance matrix C = X * X^T / (N-1)
  std::vector<std::vector<float>> cov(D, std::vector<float>(D, 0.0f));
  for (size_t i = 0; i < D; ++i) {
    for (size_t j = i; j < D; ++j) {
      float sum = 0.0f;
      for (size_t n = 0; n < N; ++n) {
        sum += centered[i][n] * centered[j][n];
      }
      cov[i][j] = cov[j][i] = sum / (N - 1);
    }
  }

  // 3. Power iteration for first eigenvector
  std::vector<float> v(D, 1.0f / std::sqrt(static_cast<float>(D)));
  std::vector<float> v_new(D);

  constexpr int MAX_ITER = 100;
  constexpr float TOL = 1e-6f;

  for (int iter = 0; iter < MAX_ITER; ++iter) {
    // v_new = C * v
    for (size_t i = 0; i < D; ++i) {
      v_new[i] = 0.0f;
      for (size_t j = 0; j < D; ++j) {
        v_new[i] += cov[i][j] * v[j];
      }
    }

    // Normalize
    float norm = 0.0f;
    for (size_t i = 0; i < D; ++i) {
      norm += v_new[i] * v_new[i];
    }
    norm = std::sqrt(norm);
    assert(norm > 1e-10f);

    for (size_t i = 0; i < D; ++i) {
      v_new[i] /= norm;
    }

    // Check convergence
    float diff = 0.0f;
    for (size_t i = 0; i < D; ++i) {
      diff += (v_new[i] - v[i]) * (v_new[i] - v[i]);
    }
    v = v_new;
    if (diff < TOL)
      break;
  }

  // 4. Project: scores[n] = v^T * X[:,n]
  std::vector<float> scores(N);
  for (size_t n = 0; n < N; ++n) {
    float s = 0.0f;
    for (size_t d = 0; d < D; ++d) {
      s += v[d] * centered[d][n];
    }
    scores[n] = s;
  }

  return scores;
}

