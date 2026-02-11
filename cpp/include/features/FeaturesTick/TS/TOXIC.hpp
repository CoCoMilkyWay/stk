#pragma once

// =============================================================================
// TOXIC - 高级行为特征 (简化版)
// =============================================================================
// 计算高频交易相关的毒性指标 (简化实现, 不追踪订单ID)
//   ptc_rt = 成交前近期撤单占比 (用窗口统计近似)
//   fleet_rt = 短存活订单占比 (用撤单率近似)
//   spoof_int = 近端大额快速撤单强度 (用大额撤单率近似)
//   stale_ratio_bid/ask = 老单占比 (用深度稳定性近似)
//
// 输入频率: PER_ORDER + ON_DEPTH
// 输出频率: per sec
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

// compute: 每笔订单时累计, flush: 按秒输出（读取深度）
class Toxic {
  static constexpr float LARGE_ORDER_THRESHOLD = 10000.0f; // 大单阈值 (股)

public:
  Toxic(TickData &td,
        const CBuffer<float, L2::BLEN> (&bid_qty)[L2::LOB_DEPTH],
        const CBuffer<float, L2::BLEN> (&ask_qty)[L2::LOB_DEPTH],
        CBuffer<float, L2::BLEN> &ptc_rt,
        CBuffer<float, L2::BLEN> &fleet_rt,
        CBuffer<float, L2::BLEN> &spoof_int,
        CBuffer<float, L2::BLEN> &stale_ratio_bid,
        CBuffer<float, L2::BLEN> &stale_ratio_ask)
      : td_(td), bid_qty_(bid_qty), ask_qty_(ask_qty),
        ptc_rt_(ptc_rt), fleet_rt_(fleet_rt), spoof_int_(spoof_int),
        stale_ratio_bid_(stale_ratio_bid), stale_ratio_ask_(stale_ratio_ask) {}

  inline void compute() {
    // 每笔订单时，统计高频交易相关的毒性行为指标
    const auto &lob = td_.lob;
    const float vol = static_cast<float>(lob.volume);
    const bool is_large = vol >= LARGE_ORDER_THRESHOLD;  // 判断是否大单（>=10000股）

    switch (lob.order_type) {
    case L2::OrderType::MAKER:  // 挂单
      vol_maker_ += vol;
      break;
    case L2::OrderType::TAKER:  // 成交
      cnt_taker_++;
      // 检测"成交前撤单"行为：如果最近有撤单发生，则本次成交可能受到撤单影响
      if (cnt_recent_cancel_ > 0) cnt_ptc_++;
      break;
    case L2::OrderType::CANCEL: // 撤单
      vol_cancel_ += vol;
      cnt_recent_cancel_++;     // 记录最近撤单次数（用于PTC检测）
      // 检测"欺骗挂单"行为：大额订单快速撤单
      if (is_large) {
        vol_spoof_ += vol;      // 累计大额撤单量
      }
      break;
    }
  }

  // 每秒输出
  inline void flush() {
    // 1. ptc_rt：成交前撤单率 = 有撤单的成交次数 / 总成交次数
    // 检测成交前先撤单清理盘口的行为（price taking with cancel）
    ptc_rt_.push_back(cnt_taker_ > 0 ? static_cast<float>(cnt_ptc_) / cnt_taker_ : 0.0f);

    // 2. fleet_rt：闪单率 = 撤单量 / 挂单量
    // 检测短存活订单（fleeting orders），高频撤单行为
    fleet_rt_.push_back(vol_maker_ > 1e-6f ? vol_cancel_ / vol_maker_ : 0.0f);

    // 3. spoof_int：欺骗挂单强度 = 大额撤单 / 总撤单
    // 检测虚假大额挂单行为（spoofing），用大单制造假象后撤单
    spoof_int_.push_back(vol_cancel_ > 1e-6f ? vol_spoof_ / vol_cancel_ : 0.0f);

    // 4. stale_ratio：老单占比 = 1 - 深度变化率
    // 从BidQty和AskQty CBuffer计算当前总深度
    float depth_bid = 0.0f, depth_ask = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH; ++i) {
      depth_bid += bid_qty_[i].back();
      depth_ask += -ask_qty_[i].back();
    }

    // 计算深度变化率，深度越稳定说明老单越多（stale orders）
    float delta_bid = std::abs(depth_bid - prev_depth_bid_);  // 深度变化量
    float delta_ask = std::abs(depth_ask - prev_depth_ask_);
    // 老单占比 = 1 - 变化率，值越大说明订单簿更新慢，老单多
    stale_ratio_bid_.push_back(depth_bid > 1e-6f ? 1.0f - std::min(1.0f, delta_bid / depth_bid) : 0.0f);
    stale_ratio_ask_.push_back(depth_ask > 1e-6f ? 1.0f - std::min(1.0f, delta_ask / depth_ask) : 0.0f);

    prev_depth_bid_ = depth_bid;  // 保存当前深度作为下次的prev
    prev_depth_ask_ = depth_ask;

    // 重置秒内累计器
    vol_maker_ = vol_cancel_ = vol_spoof_ = 0.0f;
    cnt_taker_ = cnt_ptc_ = cnt_recent_cancel_ = 0;
  }

private:
  TickData &td_;
  const CBuffer<float, L2::BLEN> (&bid_qty_)[L2::LOB_DEPTH];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[L2::LOB_DEPTH];

  // 输出 CBuffer
  CBuffer<float, L2::BLEN> &ptc_rt_, &fleet_rt_, &spoof_int_;
  CBuffer<float, L2::BLEN> &stale_ratio_bid_, &stale_ratio_ask_;

  // 秒内累计
  float vol_maker_ = 0.0f, vol_cancel_ = 0.0f, vol_spoof_ = 0.0f;
  int cnt_taker_ = 0, cnt_ptc_ = 0, cnt_recent_cancel_ = 0;

  // 深度历史
  float prev_depth_bid_ = 0.0f, prev_depth_ask_ = 0.0f;
};
