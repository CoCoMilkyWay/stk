#pragma once

// =============================================================================
// RESIL (Resiliency) - 韧性与恢复特征
// =============================================================================
// 计算市场深度的韧性和恢复能力
//   ratio_bid/ask = |O^M| / (|O^T| + |O^C|)  (韧性比, >1表示深度增长)
//   imba_resil = (R^B - R^A) / (R^B + R^A)   (韧性失衡)
//   dev_bid/ask = (D_t - D̄_W) / D̄_W          (深度偏离度)
//   mr_bid/ask = d_t - d_{t-1}               (均值回归速度)
//   recovery_bid/ask = max(0, Δd) * 1_{d<0}  (恢复信号)
//
// 输入频率: PER_ORDER + ON_DEPTH
// 输出频率: per sec
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

// compute: 每笔订单时累计, flush: 按秒输出（读取深度）
class Resiliency {
  static constexpr size_t DEPTH_WINDOW = 60; // 60秒移动平均窗口

public:
  Resiliency(TickData &td,
             const CBuffer<float, L2::BLEN> (&bid_qty)[L2::LOB_DEPTH],
             const CBuffer<float, L2::BLEN> (&ask_qty)[L2::LOB_DEPTH],
             CBuffer<float, L2::BLEN> &ratio_bid,
             CBuffer<float, L2::BLEN> &ratio_ask,
             CBuffer<float, L2::BLEN> &imba,
             CBuffer<float, L2::BLEN> &dev_bid,
             CBuffer<float, L2::BLEN> &dev_ask,
             CBuffer<float, L2::BLEN> &mr_bid,
             CBuffer<float, L2::BLEN> &mr_ask,
             CBuffer<float, L2::BLEN> &recovery_bid,
             CBuffer<float, L2::BLEN> &recovery_ask)
      : td_(td), bid_qty_(bid_qty), ask_qty_(ask_qty),
        ratio_bid_(ratio_bid), ratio_ask_(ratio_ask), imba_(imba),
        dev_bid_(dev_bid), dev_ask_(dev_ask),
        mr_bid_(mr_bid), mr_ask_(mr_ask),
        recovery_bid_(recovery_bid), recovery_ask_(recovery_ask) {}

  inline void compute() {
    // 每笔订单时，根据订单类型和方向累计交易量
    const auto &lob = td_.lob;
    const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);
    const float vol = static_cast<float>(lob.volume);

    // 分别统计买卖两侧的挂单、成交、撤单量
    switch (lob.order_type) {
    case L2::OrderType::MAKER:  // 挂单量
      if (is_bid) vol_maker_bid_ += vol;
      else vol_maker_ask_ += vol;
      break;
    case L2::OrderType::TAKER:  // 成交量
      if (is_bid) vol_taker_bid_ += vol;
      else vol_taker_ask_ += vol;
      break;
    case L2::OrderType::CANCEL: // 撤单量
      if (is_bid) vol_cancel_bid_ += vol;
      else vol_cancel_ask_ += vol;
      break;
    }
  }

  // 每秒输出
  inline void flush() {
    // 1. 从BidQty和AskQty CBuffer计算当前总深度
    float depth_bid = 0.0f, depth_ask = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH; ++i) {
      depth_bid += bid_qty_[i].back();      // 买方总量
      depth_ask += -ask_qty_[i].back();     // 卖方总量（取反）
    }

    // 2. 更新深度移动平均（60秒滚动窗口）
    depth_sum_bid_ += depth_bid - depth_buf_bid_[buf_idx_];  // 移除旧值，加入新值
    depth_sum_ask_ += depth_ask - depth_buf_ask_[buf_idx_];
    depth_buf_bid_[buf_idx_] = depth_bid;  // 保存当前值
    depth_buf_ask_[buf_idx_] = depth_ask;
    buf_idx_ = (buf_idx_ + 1) % DEPTH_WINDOW;  // 循环移动指针
    if (buf_count_ < DEPTH_WINDOW) ++buf_count_;  // 计数直到填满窗口

    float mean_bid = buf_count_ > 0 ? depth_sum_bid_ / buf_count_ : depth_bid;
    float mean_ask = buf_count_ > 0 ? depth_sum_ask_ / buf_count_ : depth_ask;

    // 3. 计算韧性比：挂单量 / (成交量 + 撤单量)
    // >1 表示深度增长快于消耗，市场韧性强
    float consume_bid = vol_taker_bid_ + vol_cancel_bid_;  // 买方消耗量
    float consume_ask = vol_taker_ask_ + vol_cancel_ask_;  // 卖方消耗量
    float r_bid = consume_bid > 1e-6f ? vol_maker_bid_ / consume_bid : 1.0f;
    float r_ask = consume_ask > 1e-6f ? vol_maker_ask_ / consume_ask : 1.0f;
    ratio_bid_.push_back(r_bid);
    ratio_ask_.push_back(r_ask);

    // 4. 计算韧性失衡：(买侧韧性 - 卖侧韧性) / (买侧 + 卖侧)
    float sum_r = r_bid + r_ask;
    imba_.push_back(sum_r > 1e-6f ? (r_bid - r_ask) / sum_r : 0.0f);

    // 5. 计算深度偏离度：(当前深度 - 均值) / 均值
    // 正值表示深度高于平均，负值表示低于平均
    float d_bid = mean_bid > 1e-6f ? (depth_bid - mean_bid) / mean_bid : 0.0f;
    float d_ask = mean_ask > 1e-6f ? (depth_ask - mean_ask) / mean_ask : 0.0f;
    dev_bid_.push_back(d_bid);
    dev_ask_.push_back(d_ask);

    // 6. 计算均值回归速度：Δd = d_t - d_{t-1}
    // 正值表示偏离度增大（远离均值），负值表示偏离度减小（回归均值）
    float delta_d_bid = d_bid - prev_d_bid_;
    float delta_d_ask = d_ask - prev_d_ask_;
    mr_bid_.push_back(delta_d_bid);
    mr_ask_.push_back(delta_d_ask);

    // 7. 计算恢复信号：当深度低于均值时（d<0），检测是否正在恢复（Δd>0）
    // 只在深度不足时输出正恢复速度，其他时候为0
    recovery_bid_.push_back(d_bid < 0 ? std::max(0.0f, delta_d_bid) : 0.0f);
    recovery_ask_.push_back(d_ask < 0 ? std::max(0.0f, delta_d_ask) : 0.0f);

    // 保存当前偏离度，供下次计算均值回归用
    prev_d_bid_ = d_bid;
    prev_d_ask_ = d_ask;

    // 重置秒内累计器，准备下一个窗口
    vol_maker_bid_ = vol_maker_ask_ = 0.0f;
    vol_taker_bid_ = vol_taker_ask_ = 0.0f;
    vol_cancel_bid_ = vol_cancel_ask_ = 0.0f;
  }

private:
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
};
