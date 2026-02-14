#pragma once

// =============================================================================
// OA (Opening Auction) - 集合竞价特征
// =============================================================================
// 计算早盘集合竞价期间 (09:15-09:25) 的订单统计
//
// 【公式定义】
//   oa_bcr = Σ|O^{C,B}| / Σ|O^{M,B}|  (买方撤单率)
//   oa_acr = Σ|O^{C,A}| / Σ|O^{M,A}|  (卖方撤单率)
//   oa_btr = Σ|O^{T,B}| / Σ|O^{M,B}|  (买方成交率)
//   oa_atr = Σ|O^{T,A}| / Σ|O^{M,A}|  (卖方成交率)
//
// 【触发域】
//   compute: onTaker / onMaker / onCancel (仅在09:15-09:25期间累计)
//   flush:   onMinute
//
// 【输入输出】
//   输入: TickData.lob.{order_type, order_dir, volume, price, l0_index} (onTaker/onMaker/onCancel)
//   输出: oa_bcr, oa_acr, oa_btr, oa_atr (onMinute, 09:25后保持固定值)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

// compute: 每笔订单时累计, flush: 每分钟输出
class OA {
  static constexpr size_t OA_END_L0 = 600; // 10分钟 * 60秒

public:
  OA(TickData &td,
     CBuffer<float, L2::BLEN> &oa_bcr,
     CBuffer<float, L2::BLEN> &oa_acr,
     CBuffer<float, L2::BLEN> &oa_btr,
     CBuffer<float, L2::BLEN> &oa_atr)
      : td_(td),
        oa_bcr_(oa_bcr), oa_acr_(oa_acr),
        oa_btr_(oa_btr), oa_atr_(oa_atr) {}

  inline void compute() {
    // 每笔订单时，只统计集合竞价时段（09:15-09:25，前10分钟）的订单
    if (td_.l0_index >= OA_END_L0)
      return; // 超过10分钟后不再累计

    const auto &lob = td_.lob;
    // 使用金额（万元）而非量，与定义一致
    const float amt = static_cast<float>(lob.volume) * lob.price / 10000.0f;
    const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);

    // 按订单类型和方向累计集合竞价期间的成交额
    switch (lob.order_type) {
    case L2::OrderType::MAKER: // 挂单
      if (is_bid)
        amt_maker_bid_ += amt;
      else
        amt_maker_ask_ += amt;
      break;
    case L2::OrderType::TAKER: // 成交
      if (is_bid)
        amt_taker_bid_ += amt;
      else
        amt_taker_ask_ += amt;
      break;
    case L2::OrderType::CANCEL: // 撤单
      if (is_bid)
        amt_cancel_bid_ += amt;
      else
        amt_cancel_ask_ += amt;
      break;
    }
  }

  // 每分钟输出
  inline void flush() {
    // 计算集合竞价期间的各项比率（09:25后保持固定值）
    // bcr/acr：撤单率 = 撤单额 / 挂单额，反映试探性挂单
    float bcr = amt_maker_bid_ > 1e-6f ? amt_cancel_bid_ / amt_maker_bid_ : 0.0f; // 买方撤单率
    float acr = amt_maker_ask_ > 1e-6f ? amt_cancel_ask_ / amt_maker_ask_ : 0.0f; // 卖方撤单率
    // btr/atr：成交率 = 成交额 / 挂单额，反映真实交易意愿
    float btr = amt_maker_bid_ > 1e-6f ? amt_taker_bid_ / amt_maker_bid_ : 0.0f; // 买方成交率
    float atr = amt_maker_ask_ > 1e-6f ? amt_taker_ask_ / amt_maker_ask_ : 0.0f; // 卖方成交率

    // 输出比率值（集合竞价期间累计，连续竞价期间保持不变）
    oa_bcr_.push_back(bcr);
    oa_acr_.push_back(acr);
    oa_btr_.push_back(btr);
    oa_atr_.push_back(atr);
  }

  // 跨天重置
  void reset() {
    amt_maker_bid_ = amt_maker_ask_ = 0.0f;
    amt_taker_bid_ = amt_taker_ask_ = 0.0f;
    amt_cancel_bid_ = amt_cancel_ask_ = 0.0f;
  }

private:
  TickData &td_;

  // 输出 CBuffer
  CBuffer<float, L2::BLEN> &oa_bcr_, &oa_acr_, &oa_btr_, &oa_atr_;

  // 累计统计 (整个集合竞价期间, 万元)
  float amt_maker_bid_ = 0.0f, amt_maker_ask_ = 0.0f;
  float amt_taker_bid_ = 0.0f, amt_taker_ask_ = 0.0f;
  float amt_cancel_bid_ = 0.0f, amt_cancel_ask_ = 0.0f;
};
