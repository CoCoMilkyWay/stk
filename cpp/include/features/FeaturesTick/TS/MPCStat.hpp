#pragma once

// =============================================================================
// MPCStat - MPC日内统计因子 (Max, Skew)
// =============================================================================
// Reference: 中信建投《高频订单失衡及价差因子》2021.01.29
//
// 核心思想:
//   MPC分布的极端值包含主力操纵信号
//   - MPCmax: 日内MPC最大值，捕捉极端价格变动
//   - MPCskew: 日内MPC偏度，捕捉分布不对称性
//
// 研报发现:
//   - MPC5_max: IC -9.39%, 年化多空24.01%, 夏普1.94
//   - MPC5_skew: IC -6.66%, 年化多空20.67%, 夏普3.07 (与常用因子相关性最低)
//   - MPC1_max: IC -8.10%, 年化多空20.02%, 夏普1.68
//   - MPC1_skew: IC -5.45%, 年化IR -4.08, 夏普2.89
//
// 逻辑:
//   大单交易造成中间价极端变动 => 可能是主力"对倒"
//   短期吸引散户追高 => 长期价格回落
//   因此 MPCmax/MPCskew 越大 => 未来收益越负
//
// 使用方式:
//   每tick调用 update(mpc_value)
//   每日开盘调用 reset()
//   随时调用 get_max() / get_skew() 获取日内统计
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <algorithm>
#include <cmath>

class MPCStat {
public:
  MPCStat() = default;

  // 更新日内统计 (每tick调用)
  void update(float mpc) {
    sum_ += mpc;
    sum_sq_ += mpc * mpc;
    sum_cube_ += mpc * mpc * mpc;
    max_ = std::max(max_, mpc);
    ++count_;
  }

  // 重置日内统计 (每日开盘调用)
  void reset() {
    sum_ = 0.0f;
    sum_sq_ = 0.0f;
    sum_cube_ = 0.0f;
    max_ = -1e10f;
    count_ = 0;
  }

  // 获取日内均值
  float get_mean() const {
    if (count_ == 0)
      return 0.0f;
    return sum_ / count_;
  }

  // 获取日内最大值
  float get_max() const { return count_ > 0 ? max_ : 0.0f; }

  // 获取日内偏度 (Fisher's skewness)
  float get_skew() const {
    if (count_ < 3)
      return 0.0f;

    float n = static_cast<float>(count_);
    float mean = sum_ / n;
    float variance = (sum_sq_ / n) - (mean * mean);
    float stddev = std::sqrt(std::max(variance, 1e-10f));

    if (stddev < 1e-8f)
      return 0.0f;

    // E[(X-μ)³] = E[X³] - 3μE[X²] + 2μ³
    float m3 = sum_cube_ / n - 3.0f * mean * sum_sq_ / n +
               2.0f * mean * mean * mean;

    return m3 / (stddev * stddev * stddev);
  }

  size_t count() const { return count_; }

private:
  float sum_ = 0.0f;
  float sum_sq_ = 0.0f;
  float sum_cube_ = 0.0f;
  float max_ = -1e10f;
  size_t count_ = 0;
};

// =============================================================================
// MPCWithStats - MPC因子 + 日内统计输出
// =============================================================================
// 整合MPC计算和日内统计，支持输出到多个CBuffer
//
// 输出:
//   - mpc_buffer: 逐tick的MPC值
//   - max_buffer: 日内累计最大值 (可选，每tick更新)
//   - skew_buffer: 日内累计偏度 (可选，每tick更新)
// =============================================================================

template <size_t LAG = 5>
class MPCWithStats {
  static_assert(LAG >= 1, "LAG must be >= 1");

public:
  // 基础版: 只输出MPC
  MPCWithStats(const CBuffer<float, L2::BLEN> &mid_price,
               CBuffer<float, L2::BLEN> &mpc_buffer)
      : mid_price_(mid_price), mpc_buffer_(mpc_buffer), max_buffer_(nullptr),
        skew_buffer_(nullptr) {}

  // 完整版: 输出MPC + max + skew
  MPCWithStats(const CBuffer<float, L2::BLEN> &mid_price,
               CBuffer<float, L2::BLEN> &mpc_buffer,
               CBuffer<float, L2::BLEN> &max_buffer,
               CBuffer<float, L2::BLEN> &skew_buffer)
      : mid_price_(mid_price), mpc_buffer_(mpc_buffer),
        max_buffer_(&max_buffer), skew_buffer_(&skew_buffer) {}

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

    mpc_buffer_.push_back(mpc);
    stats_.update(mpc);

    // 输出日内统计 (如果配置了对应buffer)
    if (max_buffer_) {
      max_buffer_->push_back(stats_.get_max());
    }
    if (skew_buffer_) {
      skew_buffer_->push_back(stats_.get_skew());
    }
  }

  void reset_daily() { stats_.reset(); }

  float back() const { return mpc_buffer_.back(); }
  float get_max() const { return stats_.get_max(); }
  float get_skew() const { return stats_.get_skew(); }
  float get_mean() const { return stats_.get_mean(); }

private:
  const CBuffer<float, L2::BLEN> &mid_price_;
  CBuffer<float, L2::BLEN> &mpc_buffer_;
  CBuffer<float, L2::BLEN> *max_buffer_;
  CBuffer<float, L2::BLEN> *skew_buffer_;

  MPCStat stats_;
};

