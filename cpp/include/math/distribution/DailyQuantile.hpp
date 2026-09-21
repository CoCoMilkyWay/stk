#pragma once

// =============================================================================
// DailyQuantile - 前 N_DAYS 日分位阈值 (日更, 算子自持的短周期因果基准)
// =============================================================================
//   每日一只 KLL (log x), roll() 归档当日并以各日 ICDF 分位均值更新阈值.
//   用法: add(log x) 逐样本攒批, flush_batch() 每分钟入 sketch, reset 时 roll(thr) 取阈值 (无历史返回 false).
//   使用方: TradeSize (单笔成交额), OrderQuad (委托申报额).
// =============================================================================

#include "math/distribution/KLLcache.hpp"
#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

template <size_t NQ, size_t N_DAYS>
class DailyQuantile {
  static constexpr size_t KLL_K = 128;
  static constexpr size_t KLL_RECON = 128; // u 网格 1/127, P98 落在网格内插

public:
  explicit DailyQuantile(const float (&probs)[NQ]) : cur_(KLL_K, KLL_RECON) {
    for (size_t i = 0; i < NQ; ++i)
      probs_[i] = probs[i];
    for (auto &d : days_)
      d = KLLcache(KLL_K, KLL_RECON);
    buf_.reserve(1024);
  }

  inline void add(float x) { buf_.push_back(x); }

  inline void flush_batch() {
    if (!buf_.empty()) {
      cur_.addBatch(buf_);
      buf_.clear();
    }
  }

  // 跨日: 归档当日 (空 sketch 不入), 阈值 ← 历史各日分位均值; 返回是否有可用阈值
  bool roll(float (&thr)[NQ]) {
    flush_batch();
    if (!cur_.empty()) {
      days_[write_] = std::move(cur_);
      cur_ = KLLcache(KLL_K, KLL_RECON);
      write_ = (write_ + 1) % N_DAYS;
      if (n_days_ < N_DAYS)
        ++n_days_;
    }
    if (n_days_ == 0)
      return false;
    float sum[NQ] = {};
    for (size_t i = 0; i < n_days_; ++i) {
      const KLLcache::LinePtr icdf = days_[(write_ + N_DAYS - 1 - i) % N_DAYS].exportICDF();
      for (size_t j = 0; j < NQ; ++j)
        sum[j] += query(icdf, probs_[j]);
    }
    const float inv = 1.0f / static_cast<float>(n_days_);
    for (size_t j = 0; j < NQ; ++j)
      thr[j] = sum[j] * inv;
    return true;
  }

private:
  static float query(const KLLcache::LinePtr &icdf, float p) {
    const float u0 = icdf.x[0], u1 = icdf.x[icdf.n - 1];
    if (u1 <= u0)
      return icdf.y[0];
    float t = std::clamp((p - u0) / (u1 - u0) * static_cast<float>(icdf.n - 1), 0.0f, static_cast<float>(icdf.n - 1));
    const size_t i = static_cast<size_t>(t);
    if (i >= icdf.n - 1)
      return icdf.y[icdf.n - 1];
    const float frac = t - static_cast<float>(i);
    return icdf.y[i] + frac * (icdf.y[i + 1] - icdf.y[i]);
  }

  float probs_[NQ];
  KLLcache cur_;
  KLLcache days_[N_DAYS];
  size_t write_ = 0, n_days_ = 0;
  std::vector<float> buf_;
};
