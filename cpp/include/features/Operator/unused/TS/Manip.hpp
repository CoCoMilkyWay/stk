#pragma once

// =============================================================================
// MANIP (Manipulation) - 市场操纵行为特征 (降频版, 简化实现, 不追踪订单ID)
// =============================================================================
//   ptc_rt    = 成交前近期撤单占比 (窗口统计近似)
//   fleet_rt  = 短存活订单占比 (撤单率近似)
//   spoof_int = 近端大额快速撤单强度 (大额撤单率近似)
//   stale_ratio_bid/ask = 老单占比 (深度稳定性近似)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include <cmath>

class Manip {
  static constexpr float LARGE_ORDER_THRESHOLD = 10000.0f; // 大单阈值 (股)

public:
  enum Out : size_t { ptc_rt,
                      fleet_rt,
                      spoof_int,
                      stale_ratio_bid,
                      stale_ratio_ask,
                      kCount };
  float y[kCount] = {}; // 秒结算写入, 分钟末由 Node 推出

  Manip(TickData &td,
        const DepthSeries &bid_qty,
        const DepthSeries &ask_qty)
      : td_(td), bid_qty_(bid_qty), ask_qty_(ask_qty) {}

  inline void compute() {
    const uint32_t cur_sec = td_.l0_index;

    // 按秒推进：当秒变化时，聚合上一秒的数据
    while (last_sec_ < cur_sec) {
      flush_second_();
      ++last_sec_;
    }

    const auto &lob = td_.lob;
    const float vol = static_cast<float>(lob.volume);
    const bool is_large = vol >= LARGE_ORDER_THRESHOLD;

    switch (lob.order_type) {
    case L2::OrderType::MAKER:
      vol_maker_ += vol;
      break;
    case L2::OrderType::TAKER:
      cnt_taker_++;
      if (cnt_recent_cancel_ > 0) // 最近有撤单 → 成交前撤单
        cnt_ptc_++;
      break;
    case L2::OrderType::CANCEL:
      vol_cancel_ += vol;
      cnt_recent_cancel_++;
      if (is_large) // 大额撤单 → spoofing
        vol_spoof_ += vol;
      break;
    }
  }

  // 跨天重置
  inline void reset() {
    vol_maker_ = vol_cancel_ = vol_spoof_ = 0.0f;
    cnt_taker_ = cnt_ptc_ = cnt_recent_cancel_ = 0;
    prev_depth_bid_ = prev_depth_ask_ = 0.0f;
    last_sec_ = 0;
    for (size_t i = 0; i < kCount; ++i)
      y[i] = 0.0f;
  }

private:
  // 秒级聚合: 无新样本的秒沿用上一值
  inline void flush_second_() {
    // 1. 成交前撤单率 = 有撤单的成交次数 / 总成交次数
    y[ptc_rt] = cnt_taker_ > 0 ? static_cast<float>(cnt_ptc_) / cnt_taker_ : y[ptc_rt];
    // 2. 闪单率 = 撤单量 / 挂单量
    y[fleet_rt] = vol_maker_ > 1e-6f ? vol_cancel_ / vol_maker_ : y[fleet_rt];
    // 3. 欺骗挂单强度 = 大额撤单 / 总撤单
    y[spoof_int] = vol_cancel_ > 1e-6f ? vol_spoof_ / vol_cancel_ : y[spoof_int];

    // 4. 老单占比 = 1 - 深度变化率
    float depth_bid = 0.0f, depth_ask = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH; ++i) {
      depth_bid += bid_qty_[i].back();
      depth_ask += -ask_qty_[i].back(); // 卖方存负值
    }
    float delta_bid = std::fabs(depth_bid - prev_depth_bid_);
    float delta_ask = std::fabs(depth_ask - prev_depth_ask_);
    y[stale_ratio_bid] = depth_bid > 1e-6f ? 1.0f - std::fmin(1.0f, delta_bid / depth_bid) : y[stale_ratio_bid];
    y[stale_ratio_ask] = depth_ask > 1e-6f ? 1.0f - std::fmin(1.0f, delta_ask / depth_ask) : y[stale_ratio_ask];
    prev_depth_bid_ = depth_bid;
    prev_depth_ask_ = depth_ask;

    // 重置秒内累计器
    vol_maker_ = vol_cancel_ = vol_spoof_ = 0.0f;
    cnt_taker_ = cnt_ptc_ = 0;
    cnt_recent_cancel_ = 0;
  }

  TickData &td_;
  const DepthSeries &bid_qty_;
  const DepthSeries &ask_qty_;

  // 秒内累计
  float vol_maker_ = 0.0f, vol_cancel_ = 0.0f, vol_spoof_ = 0.0f;
  int cnt_taker_ = 0, cnt_ptc_ = 0, cnt_recent_cancel_ = 0;

  // 深度历史
  float prev_depth_bid_ = 0.0f, prev_depth_ask_ = 0.0f;

  // 秒推进状态
  uint32_t last_sec_ = 0;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Manip(N) N(Manip, (Manip), (tick_data, DepthData.bid_qty, DepthData.ask_qty), onTick, onMinute)

#define FIELDS_L1_Manip(X, CAT1)                                                                                                                                                                                                                                                                                                                                                                            \
  X(ptc_rt, CAT1, RATIO, NONE, "Pre-Trade Cancel Ratio", "成交前撤单比", "成交前T_pre内同向近价撤单占比(降频)", R"(\frac{\sum_{j\in O_W^{T}}\sum_{\tau=\tau_j-T_{\mathrm{pre}}}^{\tau_j}|O_{\tau}^{C,s_j}|}{\sum_{j\in O_W^{T}}|O_j|}, \quad O_W^{T}=\{j: \tau_j\in W, \mathrm{is\_trade}_j\})", OP(Manip, ptc_rt))                                                                                         \
  X(fleet_rt, CAT1, RATIO, NONE, "Fleeting Order Ratio", "闪单占比", "存活时间<Δ的订单量占比(降频)", R"(\frac{\sum_{i\in O_W^{M,\mathrm{fleet}}}|O_i|}{\sum_{i\in O_W^{M}}|O_i|}, \quad O_W^{M,\mathrm{fleet}}=\{i\in O_W^{M}: \tau_i^{\mathrm{cxl}}-\tau_i^{\mathrm{post}}<\Delta\})", OP(Manip, fleet_rt))                                                                                                \
  X(spoof_int, CAT1, RATIO, NONE, "Spoofing Intensity", "欺骗强度", "近端大额快速撤单占总撤单比例(降频)", R"(\frac{\sum_{i\in O_W^{C,\mathrm{spoof}}}|O_i|}{\sum_{i\in O_W^{C}}|O_i|}, \quad O_W^{C,\mathrm{spoof}}=\{i\in O_W^{C}: \tau_i^{\mathrm{cxl}}-\tau_i^{\mathrm{post}}<T_{\mathrm{fast}}, |P_i-P_{1,\tau_i}^{M}|\leq k\cdot\mathrm{tick}, |O_i|\geq q_{\mathrm{large}}\})", OP(Manip, spoof_int)) \
  X(stale_ratio_bid, CAT1, RATIO, NONE, "Bid Stale Order Ratio", "买侧老单占比", "存活超T秒大单量占比(降频)", R"(\frac{\sum_{i \in O_t^{M,B,\mathrm{stale}}} |O_i|}{\sum_{i \in O_t^{M,B}} |O_i|}, \quad O^{M,s,\mathrm{stale}}=\{i: t-\tau_i^{\mathrm{post}}>T, |O_i|>q_{\mathrm{large}}\})", OP(Manip, stale_ratio_bid))                                                                                  \
  X(stale_ratio_ask, CAT1, RATIO, NONE, "Ask Stale Order Ratio", "卖侧老单占比", "存活超T秒大单量占比(降频)", R"(\frac{\sum_{i \in O_t^{M,A,\mathrm{stale}}} |O_i|}{\sum_{i \in O_t^{M,A}} |O_i|}, \quad O^{M,s,\mathrm{stale}}=\{i: t-\tau_i^{\mathrm{post}}>T, |O_i|>q_{\mathrm{large}}\})", OP(Manip, stale_ratio_ask))
