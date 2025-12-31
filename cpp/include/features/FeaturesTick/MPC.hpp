#pragma once

// =============================================================================
// MPC (Midpoint Price Change) - 中间价变化率因子
// =============================================================================
// Reference: 中信建投《高频订单失衡及价差因子》2021.01.29
//
// 核心思想:
//   中间价的短期百分比变化率，捕捉价格动量
//   MPC_{t,k} = (M_t - M_{t-k}) / M_{t-k}
//
// 研报发现:
//   - lag=1: 高频正向 (分钟IC 0.98%), 低频反转 (月频IC -5.36%)
//   - lag=5: 高频已负向 (分钟IC -0.26%), 低频更负 (月频IC -5.83%)
//   - MPC5_neut: IC -7.26%, 年化多空30.63%, 夏普2.88 (最佳)
//
// 日内聚合方式:
//   - mean: 均值 (基础版本)
//   - max: 最大值 (捕捉极端变动，主力操纵信号)
//   - skew: 偏度 (分布形态)
//
// 参数:
//   - LAG: 时间间隔 (1 或 5)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <algorithm>
#include <cmath>

template <size_t LAG = 5>
class MPC {
  static_assert(LAG >= 1, "LAG must be >= 1");

public:
  // 从共享CBuffer读取中间价
  MPC(const CBuffer<float, L2::BLEN> &mid_price,
      CBuffer<float, L2::BLEN> &buffer)
      : mid_price_(mid_price),
        buffer_(buffer) {}

  void compute() {
    size_t sz = mid_price_.size();

    // 计算MPC: (M_t - M_{t-LAG}) / M_{t-LAG}
    float mpc = 0.0f;
    if (sz > LAG) {
      float cur_mid = mid_price_[sz - 1];
      float prev_mid = mid_price_[sz - 1 - LAG];
      if (prev_mid > 1e-6f) {
        mpc = (cur_mid - prev_mid) / prev_mid;
      }
    }

    buffer_.push_back(mpc);

    // 更新日内统计
    update_daily_stats(mpc);
  }

  float back() const { return buffer_.back(); }

  // 获取日内均值
  float get_daily_mean() const {
    if (daily_count_ == 0) return 0.0f;
    return daily_sum_ / daily_count_;
  }

  // 获取日内最大值
  float get_daily_max() const {
    return daily_max_;
  }

  // 获取日内偏度 (使用增量计算)
  float get_daily_skew() const {
    if (daily_count_ < 3) return 0.0f;

    float mean = daily_sum_ / daily_count_;
    float variance = (daily_sum_sq_ / daily_count_) - (mean * mean);
    float stddev = std::sqrt(std::max(variance, 1e-10f));

    if (stddev < 1e-8f) return 0.0f;

    // 偏度 = E[(X-μ)³] / σ³
    float m3 = daily_sum_cube_ / daily_count_
               - 3.0f * mean * daily_sum_sq_ / daily_count_
               + 2.0f * mean * mean * mean;

    return m3 / (stddev * stddev * stddev);
  }

  // 重置日内统计 (每日开盘调用)
  void reset_daily() {
    daily_sum_ = 0.0f;
    daily_sum_sq_ = 0.0f;
    daily_sum_cube_ = 0.0f;
    daily_max_ = -1e10f;
    daily_count_ = 0;
  }

private:
  void update_daily_stats(float mpc) {
    daily_sum_ += mpc;
    daily_sum_sq_ += mpc * mpc;
    daily_sum_cube_ += mpc * mpc * mpc;
    daily_max_ = std::max(daily_max_, mpc);
    ++daily_count_;
  }

  const CBuffer<float, L2::BLEN> &mid_price_;
  CBuffer<float, L2::BLEN> &buffer_;

  // 日内统计 (增量计算)
  float daily_sum_ = 0.0f;
  float daily_sum_sq_ = 0.0f;
  float daily_sum_cube_ = 0.0f;
  float daily_max_ = -1e10f;
  size_t daily_count_ = 0;
};
