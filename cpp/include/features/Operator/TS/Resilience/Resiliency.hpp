#pragma once

// =============================================================================
// RESIL (Resiliency) - 韧性与恢复特征 (降频版)
// =============================================================================
// 计算市场深度的韧性和恢复能力
//
// 【公式定义】
//   ratio_bid/ask = |O^M| / (|O^T| + |O^C|)  (韧性比, >1表示深度增长)
//   imba_resil = (R^B - R^A) / (R^B + R^A)   (韧性失衡)
//   dev_bid/ask = (D_t - D̄_W) / D̄_W          (深度偏离度)
//   mr_bid/ask = d_t - d_{t-1}               (均值回归速度)
//   recovery_bid/ask = max(0, Δd) · 1_{d<0}  (恢复信号)
//
// 【触发域】
//   compute: onTaker / onMaker / onCancel (内部按秒推进)
//   flush:   onMinute
//
// 【输入输出】
//   输入: TickData.lob.{order_type, order_dir, volume, l0_index} (onTaker/onMaker/onCancel), bid_qty[0:29] (onDepth), ask_qty[0:29] (onDepth)
//   输出: ratio_bid, ratio_ask, imba, dev_bid, dev_ask, mr_bid, mr_ask, recovery_bid, recovery_ask (onMinute)
//
// 【备注】
//   - 使用60秒移动平均窗口
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class Resiliency {
  static constexpr size_t DEPTH_WINDOW = 60; // 60秒移动平均窗口

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

  Resiliency(TickData &td,
             const CBuffer<float, L2::BLEN> (&bid_qty)[L2::LOB_DEPTH],
             const CBuffer<float, L2::BLEN> (&ask_qty)[L2::LOB_DEPTH],
             CBuffer<float, L2::BLEN> (&out)[kCount])
      : td_(td), bid_qty_(bid_qty), ask_qty_(ask_qty),
        ratio_bid_(out[ratio_bid]), ratio_ask_(out[ratio_ask]), imba_(out[imba]),
        dev_bid_(out[dev_bid]), dev_ask_(out[dev_ask]),
        mr_bid_(out[mr_bid]), mr_ask_(out[mr_ask]),
        recovery_bid_(out[recovery_bid]), recovery_ask_(out[recovery_ask]) {}

  inline void compute() {
    const uint32_t cur_sec = td_.l0_index;

    // 按秒推进：当秒变化时，聚合上一秒的数据
    while (last_sec_ < cur_sec) {
      flush_second_();
      ++last_sec_;
    }

    // 每笔订单时，根据订单类型和方向累计交易量
    const auto &lob = td_.lob;
    const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);
    const float vol = static_cast<float>(lob.volume);

    // 分别统计买卖两侧的挂单、成交、撤单量
    switch (lob.order_type) {
    case L2::OrderType::MAKER: // 挂单量
      if (is_bid)
        vol_maker_bid_ += vol;
      else
        vol_maker_ask_ += vol;
      break;
    case L2::OrderType::TAKER: // 成交量
      if (is_bid)
        vol_taker_bid_ += vol;
      else
        vol_taker_ask_ += vol;
      break;
    case L2::OrderType::CANCEL: // 撤单量
      if (is_bid)
        vol_cancel_bid_ += vol;
      else
        vol_cancel_ask_ += vol;
      break;
    }
  }

  // 每分钟输出
  inline void flush() {
    ratio_bid_.push_back(out_ratio_bid_);
    ratio_ask_.push_back(out_ratio_ask_);
    imba_.push_back(out_imba_);
    dev_bid_.push_back(out_dev_bid_);
    dev_ask_.push_back(out_dev_ask_);
    mr_bid_.push_back(out_mr_bid_);
    mr_ask_.push_back(out_mr_ask_);
    recovery_bid_.push_back(out_recovery_bid_);
    recovery_ask_.push_back(out_recovery_ask_);
  }

  inline void reset() {
    // 重置秒内累计器
    vol_maker_bid_ = vol_maker_ask_ = 0.0f;
    vol_taker_bid_ = vol_taker_ask_ = 0.0f;
    vol_cancel_bid_ = vol_cancel_ask_ = 0.0f;

    // 清空深度移动平均缓冲区
    for (size_t i = 0; i < DEPTH_WINDOW; ++i) {
      depth_buf_bid_[i] = 0.0f;
      depth_buf_ask_[i] = 0.0f;
    }
    depth_sum_bid_ = 0.0f;
    depth_sum_ask_ = 0.0f;
    buf_idx_ = 0;
    buf_count_ = 0;

    // 重置偏离度
    prev_d_bid_ = 0.0f;
    prev_d_ask_ = 0.0f;

    // 重置秒推进状态
    last_sec_ = 0;

    // 重置输出缓存
    out_ratio_bid_ = out_ratio_ask_ = 1.0f;
    out_imba_ = 0.0f;
    out_dev_bid_ = out_dev_ask_ = 0.0f;
    out_mr_bid_ = out_mr_ask_ = 0.0f;
    out_recovery_bid_ = out_recovery_ask_ = 0.0f;
  }

private:
  // 秒级聚合（内部调用）
  inline void flush_second_() {
    // 1. 从BidQty和AskQty CBuffer计算当前总深度
    float depth_bid = 0.0f, depth_ask = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH; ++i) {
      depth_bid += bid_qty_[i].back();  // 买方总量
      depth_ask += -ask_qty_[i].back(); // 卖方总量（取反）
    }

    // 2. 更新深度移动平均（60秒滚动窗口）
    depth_sum_bid_ += depth_bid - depth_buf_bid_[buf_idx_]; // 移除旧值，加入新值
    depth_sum_ask_ += depth_ask - depth_buf_ask_[buf_idx_];
    depth_buf_bid_[buf_idx_] = depth_bid; // 保存当前值
    depth_buf_ask_[buf_idx_] = depth_ask;
    buf_idx_ = (buf_idx_ + 1) % DEPTH_WINDOW; // 循环移动指针
    if (buf_count_ < DEPTH_WINDOW)
      ++buf_count_; // 计数直到填满窗口

    float mean_bid = buf_count_ > 0 ? depth_sum_bid_ / buf_count_ : depth_bid;
    float mean_ask = buf_count_ > 0 ? depth_sum_ask_ / buf_count_ : depth_ask;

    // 3. 计算韧性比：挂单量 / (成交量 + 撤单量)
    // >1 表示深度增长快于消耗，市场韧性强
    float consume_bid = vol_taker_bid_ + vol_cancel_bid_; // 买方消耗量
    float consume_ask = vol_taker_ask_ + vol_cancel_ask_; // 卖方消耗量
    out_ratio_bid_ = consume_bid > 1e-6f ? vol_maker_bid_ / consume_bid : out_ratio_bid_;
    out_ratio_ask_ = consume_ask > 1e-6f ? vol_maker_ask_ / consume_ask : out_ratio_ask_;

    // 4. 计算韧性失衡：(买侧韧性 - 卖侧韧性) / (买侧 + 卖侧)
    float sum_r = out_ratio_bid_ + out_ratio_ask_;
    out_imba_ = sum_r > 1e-6f ? (out_ratio_bid_ - out_ratio_ask_) / sum_r : 0.0f;

    // 5. 深度偏离度：(当前深度 - 均值) / 均值
    // 负值表示当前深度低于历史均值（被冲击）
    float d_bid = mean_bid > 1e-6f ? (depth_bid - mean_bid) / mean_bid : 0.0f;
    float d_ask = mean_ask > 1e-6f ? (depth_ask - mean_ask) / mean_ask : 0.0f;
    out_dev_bid_ = d_bid;
    out_dev_ask_ = d_ask;

    // 6. 均值回归速度：偏离度变化率
    // 正值表示正在恢复（向均值靠近）
    out_mr_bid_ = d_bid - prev_d_bid_;
    out_mr_ask_ = d_ask - prev_d_ask_;

    // 7. 恢复信号：冲击状态下的正向恢复强度
    // 只有当前处于冲击状态（d<0）且正在恢复（mr>0）时才有信号
    out_recovery_bid_ = d_bid < 0 ? std::max(0.0f, out_mr_bid_) : 0.0f;
    out_recovery_ask_ = d_ask < 0 ? std::max(0.0f, out_mr_ask_) : 0.0f;

    prev_d_bid_ = d_bid;
    prev_d_ask_ = d_ask;

    // 重置秒内累计器
    vol_maker_bid_ = vol_maker_ask_ = 0.0f;
    vol_taker_bid_ = vol_taker_ask_ = 0.0f;
    vol_cancel_bid_ = vol_cancel_ask_ = 0.0f;
  }

  TickData &td_;
  const CBuffer<float, L2::BLEN> (&bid_qty_)[L2::LOB_DEPTH];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[L2::LOB_DEPTH];

  // 输出 CBuffer
  CBuffer<float, L2::BLEN> &ratio_bid_, &ratio_ask_, &imba_;
  CBuffer<float, L2::BLEN> &dev_bid_, &dev_ask_;
  CBuffer<float, L2::BLEN> &mr_bid_, &mr_ask_;
  CBuffer<float, L2::BLEN> &recovery_bid_, &recovery_ask_;

  // 秒内累计量
  float vol_maker_bid_ = 0.0f, vol_maker_ask_ = 0.0f;
  float vol_taker_bid_ = 0.0f, vol_taker_ask_ = 0.0f;
  float vol_cancel_bid_ = 0.0f, vol_cancel_ask_ = 0.0f;

  // 深度移动平均缓冲区
  float depth_buf_bid_[DEPTH_WINDOW] = {};
  float depth_buf_ask_[DEPTH_WINDOW] = {};
  float depth_sum_bid_ = 0.0f, depth_sum_ask_ = 0.0f;
  size_t buf_idx_ = 0, buf_count_ = 0;

  // 前一时刻偏离度
  float prev_d_bid_ = 0.0f, prev_d_ask_ = 0.0f;

  // 秒推进状态
  uint32_t last_sec_ = 0;

  // 输出缓存（分钟末输出）
  float out_ratio_bid_ = 1.0f, out_ratio_ask_ = 1.0f;
  float out_imba_ = 0.0f;
  float out_dev_bid_ = 0.0f, out_dev_ask_ = 0.0f;
  float out_mr_bid_ = 0.0f, out_mr_ask_ = 0.0f;
  float out_recovery_bid_ = 0.0f, out_recovery_ask_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Resiliency(N) N(Resiliency, (Resiliency), (tick_data, DepthData.bid_qty, DepthData.ask_qty), onTick, onMinute)

#define FIELDS_L1_Resiliency(X)                                                                                                                                                                                                                                                                                                                               \
  X(ratio_bid, 1, DATA, TS, RESILIENCE, RATIO, NONE, "00/100/00", "Bid Resiliency Ratio", "买侧韧性比", "买侧挂单量/消耗量,>1深度增长(降频)", R"(\frac{|O_W^{M,B}|}{|O_W^{T,B}|+|O_W^{C,B}|})", OP(Resiliency, ratio_bid))                                                                                                                                    \
  X(ratio_ask, 1, DATA, TS, RESILIENCE, RATIO, NONE, "00/100/00", "Ask Resiliency Ratio", "卖侧韧性比", "卖侧挂单量/消耗量,>1深度增长(降频)", R"(\frac{|O_W^{M,A}|}{|O_W^{T,A}|+|O_W^{C,A}|})", OP(Resiliency, ratio_ask))                                                                                                                                    \
  X(resil_imba, 1, DATA, TS, RESILIENCE, RATIO, NONE, "00/100/00", "Resiliency Imbalance", "韧性失衡", "买卖韧性比差异(正=买侧恢复快)(降频)", R"(\frac{R^B-R^A}{R^B+R^A}, \quad R^s=\frac{|O_W^{M,s}|}{|O_W^{T,s}|+|O_W^{C,s}|})", OP(Resiliency, imba))                                                                                                      \
  X(dev_bid, 1, DATA, TS, RESILIENCE, RAW, NONE, "00/100/00", "Bid Depth Deviation", "买侧深度偏离", "当前深度vs移动均值偏离度(负=被冲击)(降频)", R"(\frac{D_t^B-\bar{D}_W^B}{\bar{D}_W^B}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s}, \quad \bar{D}_W^s=\frac{1}{|W|}\sum_{\tau\in W}D_\tau^s)", OP(Resiliency, dev_bid))                                      \
  X(dev_ask, 1, DATA, TS, RESILIENCE, RAW, NONE, "00/100/00", "Ask Depth Deviation", "卖侧深度偏离", "当前深度vs移动均值偏离度(负=被冲击)(降频)", R"(\frac{D_t^A-\bar{D}_W^A}{\bar{D}_W^A}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s}, \quad \bar{D}_W^s=\frac{1}{|W|}\sum_{\tau\in W}D_\tau^s)", OP(Resiliency, dev_ask))                                      \
  X(mr_bid, 1, DATA, TS, RESILIENCE, RAW, NONE, "00/100/00", "Bid Mean-Reversion Speed", "买侧均值回归速度", "深度偏离度变化率(正=恢复中)(降频)", R"(d_t^B-d_{t-1}^B, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})", OP(Resiliency, mr_bid))                                                                   \
  X(mr_ask, 1, DATA, TS, RESILIENCE, RAW, NONE, "00/100/00", "Ask Mean-Reversion Speed", "卖侧均值回归速度", "深度偏离度变化率(正=恢复中)(降频)", R"(d_t^A-d_{t-1}^A, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})", OP(Resiliency, mr_ask))                                                                   \
  X(recovery_bid, 1, DATA, TS, RESILIENCE, RAW, NONE, "00/100/00", "Bid Recovery Signal", "买侧恢复信号", "冲击状态下的正向恢复强度(降频)", R"(\max(0,\Delta d_t^B)\cdot\mathbf{1}_{d_t^B<0}, \quad \Delta d_t^s=d_t^s-d_{t-1}^s, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})", OP(Resiliency, recovery_bid)) \
  X(recovery_ask, 1, DATA, TS, RESILIENCE, RAW, NONE, "00/100/00", "Ask Recovery Signal", "卖侧恢复信号", "冲击状态下的正向恢复强度(降频)", R"(\max(0,\Delta d_t^A)\cdot\mathbf{1}_{d_t^A<0}, \quad \Delta d_t^s=d_t^s-d_{t-1}^s, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})", OP(Resiliency, recovery_ask))
