#pragma once

// =============================================================================
// DailyQuantile - 前 N_DAYS 日分位阈值 (日更, 节点自持的短周期因果基准)
// =============================================================================
//   当日一只 KLL (log x); 归档 = 当日 ICDF 折成 NQ 个分位存日环 (sketch 换新), roll() 先归档再以各日分位均值更新阈值.
//   用法: add(log x) 逐样本攒批, flush_batch() 每分钟入 sketch, reset 时 roll(thr) 取阈值 (无历史返回 false).
//   续算点 (snap): 当日先归档 (早于次日 roll 归档, 结果相同), 记录只有日环分位 —— sketch 不入快照.
//   使用方: TradeSize (单笔成交额), OrderQuad (委托申报额).
// =============================================================================

#include "math/distribution/KLLcache.hpp"
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <vector>

template <size_t NQ, size_t N_DAYS>
class DailyQuantile {
  static constexpr size_t KLL_K = 128;
  static constexpr size_t KLL_RECON = 128; // u 网格 1/127, P98 落在网格内插

public:
  explicit DailyQuantile(const float (&probs)[NQ]) : cur_(KLL_K, KLL_RECON) {
    for (size_t i = 0; i < NQ; ++i)
      probs_[i] = probs[i];
    buf_.reserve(1024);
  }

  inline void add(float x) { buf_.push_back(x); }

  inline void flush_batch() {
    if (!buf_.empty()) {
      cur_.addBatch(buf_);
      buf_.clear();
    }
  }

  // 跨日: 归档当日, 阈值 ← 历史各日分位均值 (新 → 老累加); 返回是否有可用阈值
  bool roll(float (&thr)[NQ]) {
    archive();
    if (n_days_ == 0)
      return false;
    float sum[NQ] = {};
    for (size_t i = 0; i < n_days_; ++i) {
      const float (&q)[NQ] = q_[(write_ + N_DAYS - 1 - i) % N_DAYS];
      for (size_t j = 0; j < NQ; ++j)
        sum[j] += q[j];
    }
    const float inv = 1.0f / static_cast<float>(n_days_);
    for (size_t j = 0; j < NQ; ++j)
      thr[j] = sum[j] * inv;
    return true;
  }

  template <class Ar>
  void snap(Ar &ar) {
    if constexpr (!Ar::kLoad)
      archive();
    assert(cur_.empty() && buf_.empty() && "续算点: 当日 sketch 须已归档");
    ar(q_);
    ar(write_);
    ar(n_days_);
  }

private:
  // 当日 sketch (含未入批样本) 折成分位入日环, sketch 换新; 空 sketch 不入
  void archive() {
    flush_batch();
    if (cur_.empty())
      return;
    const KLLcache::LinePtr icdf = cur_.exportICDF();
    for (size_t j = 0; j < NQ; ++j)
      q_[write_][j] = query(icdf, probs_[j]);
    cur_ = KLLcache(KLL_K, KLL_RECON);
    write_ = (write_ + 1) % N_DAYS;
    if (n_days_ < N_DAYS)
      ++n_days_;
  }

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
  float q_[N_DAYS][NQ] = {}; // 日环: 各日 log 分位 (q_[write_ − 1] = 最近一日)
  size_t write_ = 0, n_days_ = 0;
  std::vector<float> buf_;
};
