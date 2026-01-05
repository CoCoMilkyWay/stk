#pragma once
#include "define/CBuffer.hpp"
#include <cassert>
#include <cmath>

// Welford 算法 + Population Variance（总体方差）(不用样本方差)

// =============================================================================
// RollingZScoreContinuous: 连续滑动窗口 Z-Score (固定 W 个数据点)
// - 每次 compute 时，buffer 刚好 push 了一个新数据
// - 增量更新 sum/sum_sq，O(1) 计算
// =============================================================================
template <typename T, size_t BufLen, size_t W>
class RollingZScoreContinuous {
  static_assert(W <= BufLen, "Window size must <= buffer capacity");

public:
  explicit RollingZScoreContinuous(CBuffer<T, BufLen> &data) : data_(data) {}

  inline T compute() noexcept {
    const size_t n = data_.size();
    const T x = data_.back();

    sum_ += x;
    sum_sq_ += x * x;

    // 移除窗口外的老点 (99% 场景 buffer 满)
    if (n > W) [[likely]] {
      const T old = data_[n - 1 - W];
      sum_ -= old;
      sum_sq_ -= old * old;
    }

    // 计算 z-score
    const size_t cnt = (n < W) ? n : W;
    const T mean = sum_ / cnt;
    const T var = sum_sq_ / cnt - mean * mean;
    const T std = std::sqrt(var > T(0) ? var : T(0));
    return (std > T(1e-12)) ? (x - mean) / std : T(0);
  }

  inline size_t get_count() const noexcept { return (data_.size() < W) ? data_.size() : W; }

private:
  CBuffer<T, BufLen> &data_;
  T sum_ = T(0);
  T sum_sq_ = T(0);
};

// =============================================================================
// RollingZScoreDiscrete: 离散滑动窗口 Z-Score (基于 index 的时间窗口 W)
// - index 是非连续但单调递增的时间索引 (单位: 秒)
// - W 是基于 index 的窗口大小 (单位: 秒)，窗口内实际数据量不固定
// - 每次 compute 时，buffer 刚好 push 了一个新数据和对应的 index
// =============================================================================
template <typename T, size_t BufLen, size_t W>
class RollingZScoreDiscrete {
  static_assert(W <= BufLen, "Window size must <= buffer capacity");

public:
  explicit RollingZScoreDiscrete(CBuffer<T, BufLen> &data, CBuffer<T, BufLen> &index)
      : data_(data), index_(index) {}

  inline T compute() noexcept {
    const size_t n = data_.size();
    const T x = data_.back();

    // buffer 溢出处理（上一次满 = 这次 push 覆盖了最老元素）
    if (prev_full_) [[likely]] {
      if (head_ > 0) [[likely]] {
        --head_; // 补偿索引滑动
      } else {
        // head_ == 0，被覆盖的元素还在窗口内，用保存的值减掉
        sum_ -= prev_front_;
        sum_sq_ -= prev_front_ * prev_front_;
        --count_;
      }
    }

    // 保存当前状态供下次覆盖检测
    prev_full_ = (n == BufLen);
    prev_front_ = data_.front();

    // 加入新点
    sum_ += x;
    sum_sq_ += x * x;
    ++count_;

    // 移除窗口外的老点
    const T threshold = index_.back() - static_cast<T>(W);
    while (head_ < n && index_[head_] < threshold) {
      const T old = data_[head_];
      sum_ -= old;
      sum_sq_ -= old * old;
      --count_;
      ++head_;
    }

    // 计算 z-score
    const T mean = sum_ / count_;
    const T var = sum_sq_ / count_ - mean * mean;
    const T std = std::sqrt(var > T(0) ? var : T(0));
    return (std > T(1e-12)) ? (x - mean) / std : T(0);
  }

  inline size_t get_count() const noexcept { return count_; }

private:
  CBuffer<T, BufLen> &data_;
  CBuffer<T, BufLen> &index_;
  T sum_ = T(0);
  T sum_sq_ = T(0);
  size_t count_ = 0;
  size_t head_ = 0;
  bool prev_full_ = false;
  T prev_front_ = T(0);
};