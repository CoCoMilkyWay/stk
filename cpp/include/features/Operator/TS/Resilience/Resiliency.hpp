#pragma once

// =============================================================================
// RESIL (Resiliency) - 韧性与恢复 (降频版): 每笔累计, 按秒结算, 分钟末输出
// =============================================================================
//   ratio_bid/ask = |O^M| / (|O^T| + |O^C|)   (韧性比, >1 深度增长)
//   imba = (R^B - R^A) / (R^B + R^A)          (韧性失衡)
//   dev_bid/ask = (D_t - D̄_W) / D̄_W          (深度偏离度, 60s 移动均值)
//   mr_bid/ask = d_t - d_{t-1}                (均值回归速度)
//   recovery_bid/ask = max(0, Δd) · 1_{d<0}   (恢复信号)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"

class Resiliency {
  static constexpr size_t DEPTH_WINDOW = 60; // 秒

public:
  enum Out : size_t { ratio_bid,
                      ratio_ask,
                      imba,
                      dev_bid,
                      dev_ask,
                      mr_bid,
                      mr_ask,
                      recovery_bid,
                      recovery_ask,
                      kCount };
  float y[kCount] = {}; // 秒结算写入, 分钟末由 Node 推出

  Resiliency(TickData &td,
             const DepthSeries &bid_qty,
             const DepthSeries &ask_qty)
      : td_(td), bid_qty_(bid_qty), ask_qty_(ask_qty) {
    reset();
  }

  inline void compute() {
    const uint32_t cur_sec = td_.l0_index;
    while (last_sec_ < cur_sec) {
      flush_second_();
      ++last_sec_;
    }

    const auto &lob = td_.lob;
    const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);
    const float vol = static_cast<float>(lob.volume);

    switch (lob.order_type) {
    case L2::OrderType::MAKER:
      (is_bid ? vol_maker_bid_ : vol_maker_ask_) += vol;
      break;
    case L2::OrderType::TAKER:
      (is_bid ? vol_taker_bid_ : vol_taker_ask_) += vol;
      break;
    case L2::OrderType::CANCEL:
      (is_bid ? vol_cancel_bid_ : vol_cancel_ask_) += vol;
      break;
    }
  }

  inline void reset() {
    vol_maker_bid_ = vol_maker_ask_ = 0.0f;
    vol_taker_bid_ = vol_taker_ask_ = 0.0f;
    vol_cancel_bid_ = vol_cancel_ask_ = 0.0f;
    for (size_t i = 0; i < DEPTH_WINDOW; ++i)
      depth_buf_bid_[i] = depth_buf_ask_[i] = 0.0f;
    depth_sum_bid_ = depth_sum_ask_ = 0.0f;
    buf_idx_ = buf_count_ = 0;
    prev_d_bid_ = prev_d_ask_ = 0.0f;
    last_sec_ = 0;
    for (size_t i = 0; i < kCount; ++i)
      y[i] = 0.0f;
    y[ratio_bid] = y[ratio_ask] = 1.0f; // 无消耗时视为韧性中性
  }

private:
  // 秒级聚合
  inline void flush_second_() {
    // 1. 当前总深度
    float depth_bid = 0.0f, depth_ask = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH; ++i) {
      depth_bid += bid_qty_[i].back();
      depth_ask += -ask_qty_[i].back(); // ask 存负值
    }

    // 2. 60 秒滚动均值
    depth_sum_bid_ += depth_bid - depth_buf_bid_[buf_idx_];
    depth_sum_ask_ += depth_ask - depth_buf_ask_[buf_idx_];
    depth_buf_bid_[buf_idx_] = depth_bid;
    depth_buf_ask_[buf_idx_] = depth_ask;
    buf_idx_ = (buf_idx_ + 1) % DEPTH_WINDOW;
    if (buf_count_ < DEPTH_WINDOW)
      ++buf_count_;
    float mean_bid = buf_count_ > 0 ? depth_sum_bid_ / buf_count_ : depth_bid;
    float mean_ask = buf_count_ > 0 ? depth_sum_ask_ / buf_count_ : depth_ask;

    // 3. 韧性比 (无消耗沿用上一值)
    float consume_bid = vol_taker_bid_ + vol_cancel_bid_;
    float consume_ask = vol_taker_ask_ + vol_cancel_ask_;
    y[ratio_bid] = consume_bid > 1e-6f ? vol_maker_bid_ / consume_bid : y[ratio_bid];
    y[ratio_ask] = consume_ask > 1e-6f ? vol_maker_ask_ / consume_ask : y[ratio_ask];

    // 4. 韧性失衡
    float sum_r = y[ratio_bid] + y[ratio_ask];
    y[imba] = sum_r > 1e-6f ? (y[ratio_bid] - y[ratio_ask]) / sum_r : 0.0f;

    // 5. 深度偏离度 (负 = 被冲击)
    float d_bid = mean_bid > 1e-6f ? (depth_bid - mean_bid) / mean_bid : 0.0f;
    float d_ask = mean_ask > 1e-6f ? (depth_ask - mean_ask) / mean_ask : 0.0f;
    y[dev_bid] = d_bid;
    y[dev_ask] = d_ask;

    // 6. 均值回归速度 (正 = 恢复中)
    y[mr_bid] = d_bid - prev_d_bid_;
    y[mr_ask] = d_ask - prev_d_ask_;

    // 7. 恢复信号: 冲击状态 (d<0) 下的正向回归
    y[recovery_bid] = d_bid < 0 ? std::max(0.0f, y[mr_bid]) : 0.0f;
    y[recovery_ask] = d_ask < 0 ? std::max(0.0f, y[mr_ask]) : 0.0f;

    prev_d_bid_ = d_bid;
    prev_d_ask_ = d_ask;

    vol_maker_bid_ = vol_maker_ask_ = 0.0f;
    vol_taker_bid_ = vol_taker_ask_ = 0.0f;
    vol_cancel_bid_ = vol_cancel_ask_ = 0.0f;
  }

  TickData &td_;
  const DepthSeries &bid_qty_;
  const DepthSeries &ask_qty_;

  // 秒内累计量
  float vol_maker_bid_ = 0.0f, vol_maker_ask_ = 0.0f;
  float vol_taker_bid_ = 0.0f, vol_taker_ask_ = 0.0f;
  float vol_cancel_bid_ = 0.0f, vol_cancel_ask_ = 0.0f;

  // 深度移动平均
  float depth_buf_bid_[DEPTH_WINDOW] = {};
  float depth_buf_ask_[DEPTH_WINDOW] = {};
  float depth_sum_bid_ = 0.0f, depth_sum_ask_ = 0.0f;
  size_t buf_idx_ = 0, buf_count_ = 0;

  float prev_d_bid_ = 0.0f, prev_d_ask_ = 0.0f;
  uint32_t last_sec_ = 0;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Resiliency(N) N(Resiliency, (Resiliency), (tick_data, DepthData.bid_qty, DepthData.ask_qty), onTick, onMinute)

#define FIELDS_L1_Resiliency(X)                                                                                                                                                                                                                                                                                                     \
  X(ratio_bid, RESILIENCE, RATIO, NONE, "Bid Resiliency Ratio", "买侧韧性比", "买侧挂单量/消耗量,>1深度增长(降频)", R"(\frac{|O_W^{M,B}|}{|O_W^{T,B}|+|O_W^{C,B}|})", OP(Resiliency, ratio_bid))                                                                                                                                    \
  X(ratio_ask, RESILIENCE, RATIO, NONE, "Ask Resiliency Ratio", "卖侧韧性比", "卖侧挂单量/消耗量,>1深度增长(降频)", R"(\frac{|O_W^{M,A}|}{|O_W^{T,A}|+|O_W^{C,A}|})", OP(Resiliency, ratio_ask))                                                                                                                                    \
  X(resil_imba, RESILIENCE, RATIO, NONE, "Resiliency Imbalance", "韧性失衡", "买卖韧性比差异(正=买侧恢复快)(降频)", R"(\frac{R^B-R^A}{R^B+R^A}, \quad R^s=\frac{|O_W^{M,s}|}{|O_W^{T,s}|+|O_W^{C,s}|})", OP(Resiliency, imba))                                                                                                      \
  X(dev_bid, RESILIENCE, RAW, NONE, "Bid Depth Deviation", "买侧深度偏离", "当前深度vs移动均值偏离度(负=被冲击)(降频)", R"(\frac{D_t^B-\bar{D}_W^B}{\bar{D}_W^B}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s}, \quad \bar{D}_W^s=\frac{1}{|W|}\sum_{\tau\in W}D_\tau^s)", OP(Resiliency, dev_bid))                                      \
  X(dev_ask, RESILIENCE, RAW, NONE, "Ask Depth Deviation", "卖侧深度偏离", "当前深度vs移动均值偏离度(负=被冲击)(降频)", R"(\frac{D_t^A-\bar{D}_W^A}{\bar{D}_W^A}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s}, \quad \bar{D}_W^s=\frac{1}{|W|}\sum_{\tau\in W}D_\tau^s)", OP(Resiliency, dev_ask))                                      \
  X(mr_bid, RESILIENCE, RAW, NONE, "Bid Mean-Reversion Speed", "买侧均值回归速度", "深度偏离度变化率(正=恢复中)(降频)", R"(d_t^B-d_{t-1}^B, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})", OP(Resiliency, mr_bid))                                                                   \
  X(mr_ask, RESILIENCE, RAW, NONE, "Ask Mean-Reversion Speed", "卖侧均值回归速度", "深度偏离度变化率(正=恢复中)(降频)", R"(d_t^A-d_{t-1}^A, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})", OP(Resiliency, mr_ask))                                                                   \
  X(recovery_bid, RESILIENCE, RAW, NONE, "Bid Recovery Signal", "买侧恢复信号", "冲击状态下的正向恢复强度(降频)", R"(\max(0,\Delta d_t^B)\cdot\mathbf{1}_{d_t^B<0}, \quad \Delta d_t^s=d_t^s-d_{t-1}^s, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})", OP(Resiliency, recovery_bid)) \
  X(recovery_ask, RESILIENCE, RAW, NONE, "Ask Recovery Signal", "卖侧恢复信号", "冲击状态下的正向恢复强度(降频)", R"(\max(0,\Delta d_t^A)\cdot\mathbf{1}_{d_t^A<0}, \quad \Delta d_t^s=d_t^s-d_{t-1}^s, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})", OP(Resiliency, recovery_ask))
