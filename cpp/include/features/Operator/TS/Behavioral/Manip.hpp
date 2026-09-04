#pragma once

// =============================================================================
// MANIP (Manipulation) - 市场操纵行为特征 (降频版)
// =============================================================================
// 计算高频交易相关的操纵行为指标 (简化实现, 不追踪订单ID)
//
// 【公式定义】
//   ptc_rt = 成交前近期撤单占比 (用窗口统计近似)
//   fleet_rt = 短存活订单占比 (用撤单率近似)
//   spoof_int = 近端大额快速撤单强度 (用大额撤单率近似)
//   stale_ratio_bid/ask = 老单占比 (用深度稳定性近似)
//
// 【触发域】
//   compute: onTaker / onMaker / onCancel (内部按秒推进)
//   flush:   onMinute
//
// 【输入输出】
//   输入: TickData.lob.{order_type, volume, l0_index} (onTaker/onMaker/onCancel), bid_qty[0:29] (onDepth), ask_qty[0:29] (onDepth)
//   输出: ptc_rt, fleet_rt, spoof_int, stale_ratio_bid, stale_ratio_ask (onMinute)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
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

  Manip(TickData &td,
        const CBuffer<float, L2::BLEN> (&bid_qty)[L2::LOB_DEPTH],
        const CBuffer<float, L2::BLEN> (&ask_qty)[L2::LOB_DEPTH],
        CBuffer<float, L2::BLEN> (&out)[kCount])
      : td_(td), bid_qty_(bid_qty), ask_qty_(ask_qty),
        ptc_rt_(out[ptc_rt]), fleet_rt_(out[fleet_rt]), spoof_int_(out[spoof_int]),
        stale_ratio_bid_(out[stale_ratio_bid]), stale_ratio_ask_(out[stale_ratio_ask]) {}

  inline void compute() {
    const uint32_t cur_sec = td_.l0_index;

    // 按秒推进：当秒变化时，聚合上一秒的数据
    while (last_sec_ < cur_sec) {
      flush_second_();
      ++last_sec_;
    }

    // 每笔订单时，统计高频交易相关的操纵行为指标
    const auto &lob = td_.lob;
    const float vol = static_cast<float>(lob.volume);
    const bool is_large = vol >= LARGE_ORDER_THRESHOLD; // 判断是否大单（>=10000股）

    switch (lob.order_type) {
    case L2::OrderType::MAKER: // 挂单
      vol_maker_ += vol;
      break;
    case L2::OrderType::TAKER: // 成交
      cnt_taker_++;
      // 检测"成交前撤单"行为：如果最近有撤单发生，则本次成交可能受到撤单影响
      if (cnt_recent_cancel_ > 0)
        cnt_ptc_++;
      break;
    case L2::OrderType::CANCEL: // 撤单
      vol_cancel_ += vol;
      cnt_recent_cancel_++; // 记录最近撤单次数（用于PTC检测）
      // 检测"欺骗挂单"行为：大额订单快速撤单
      if (is_large) {
        vol_spoof_ += vol; // 累计大额撤单量
      }
      break;
    }
  }

  // 每分钟输出
  inline void flush() {
    ptc_rt_.push_back(out_ptc_rt_);
    fleet_rt_.push_back(out_fleet_rt_);
    spoof_int_.push_back(out_spoof_int_);
    stale_ratio_bid_.push_back(out_stale_ratio_bid_);
    stale_ratio_ask_.push_back(out_stale_ratio_ask_);
  }

  // 跨天重置
  inline void reset() {
    vol_maker_ = vol_cancel_ = vol_spoof_ = 0.0f;
    cnt_taker_ = cnt_ptc_ = cnt_recent_cancel_ = 0;
    prev_depth_bid_ = prev_depth_ask_ = 0.0f;
    last_sec_ = 0;
    // 重置输出缓存
    out_ptc_rt_ = out_fleet_rt_ = out_spoof_int_ = 0.0f;
    out_stale_ratio_bid_ = out_stale_ratio_ask_ = 0.0f;
  }

private:
  // 秒级聚合（内部调用）
  inline void flush_second_() {
    // 1. ptc_rt：成交前撤单率 = 有撤单的成交次数 / 总成交次数
    // 检测成交前先撤单清理盘口的行为（price taking with cancel）
    out_ptc_rt_ = cnt_taker_ > 0 ? static_cast<float>(cnt_ptc_) / cnt_taker_ : out_ptc_rt_;

    // 2. fleet_rt：闪单率 = 撤单量 / 挂单量
    // 检测短存活订单（fleeting orders），高频撤单行为
    out_fleet_rt_ = vol_maker_ > 1e-6f ? vol_cancel_ / vol_maker_ : out_fleet_rt_;

    // 3. spoof_int：欺骗挂单强度 = 大额撤单 / 总撤单
    // 检测虚假大额挂单行为（spoofing），用大单制造假象后撤单
    out_spoof_int_ = vol_cancel_ > 1e-6f ? vol_spoof_ / vol_cancel_ : out_spoof_int_;

    // 4. stale_ratio：老单占比 = 1 - 深度变化率
    // 从BidQty和AskQty CBuffer计算当前总深度
    float depth_bid = 0.0f, depth_ask = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH; ++i) {
      depth_bid += bid_qty_[i].back();
      depth_ask += -ask_qty_[i].back(); // ask_qty_为负值，取反得到正数
    }

    // 计算深度变化率，深度越稳定说明老单越多（stale orders）
    float delta_bid = std::fabs(depth_bid - prev_depth_bid_); // 深度变化量
    float delta_ask = std::fabs(depth_ask - prev_depth_ask_);
    // 老单占比 = 1 - 变化率，值越大说明订单簿更新慢，老单多
    out_stale_ratio_bid_ = depth_bid > 1e-6f ? 1.0f - std::fmin(1.0f, delta_bid / depth_bid) : out_stale_ratio_bid_;
    out_stale_ratio_ask_ = depth_ask > 1e-6f ? 1.0f - std::fmin(1.0f, delta_ask / depth_ask) : out_stale_ratio_ask_;

    prev_depth_bid_ = depth_bid; // 保存当前深度作为下次的prev
    prev_depth_ask_ = depth_ask;

    // 重置秒内累计器
    vol_maker_ = vol_cancel_ = vol_spoof_ = 0.0f;
    cnt_taker_ = cnt_ptc_ = 0;
    cnt_recent_cancel_ = 0; // 每秒重置，避免影响下一秒的所有成交
  }

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

  // 秒推进状态
  uint32_t last_sec_ = 0;

  // 输出缓存（分钟末输出）
  float out_ptc_rt_ = 0.0f, out_fleet_rt_ = 0.0f, out_spoof_int_ = 0.0f;
  float out_stale_ratio_bid_ = 0.0f, out_stale_ratio_ask_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Manip(N) N(Manip, (Manip), (tick_data, DepthData.bid_qty, DepthData.ask_qty), onTick, onMinute)

#define FIELDS_L1_Manip(X)                                                                                                                                                                                                                                                                                                                                                                                                                  \
  X(ptc_rt, 1, DATA, TS, BEHAVIORAL, RATIO, NONE, "00/100/00", "Pre-Trade Cancel Ratio", "成交前撤单比", "成交前T_pre内同向近价撤单占比(降频)", R"(\frac{\sum_{j\in O_W^{T}}\sum_{\tau=\tau_j-T_{\mathrm{pre}}}^{\tau_j}|O_{\tau}^{C,s_j}|}{\sum_{j\in O_W^{T}}|O_j|}, \quad O_W^{T}=\{j: \tau_j\in W, \mathrm{is\_trade}_j\})", OP(Manip, ptc_rt))                                                                                         \
  X(fleet_rt, 1, DATA, TS, BEHAVIORAL, RATIO, NONE, "00/100/00", "Fleeting Order Ratio", "闪单占比", "存活时间<Δ的订单量占比(降频)", R"(\frac{\sum_{i\in O_W^{M,\mathrm{fleet}}}|O_i|}{\sum_{i\in O_W^{M}}|O_i|}, \quad O_W^{M,\mathrm{fleet}}=\{i\in O_W^{M}: \tau_i^{\mathrm{cxl}}-\tau_i^{\mathrm{post}}<\Delta\})", OP(Manip, fleet_rt))                                                                                                \
  X(spoof_int, 1, DATA, TS, BEHAVIORAL, RATIO, NONE, "00/100/00", "Spoofing Intensity", "欺骗强度", "近端大额快速撤单占总撤单比例(降频)", R"(\frac{\sum_{i\in O_W^{C,\mathrm{spoof}}}|O_i|}{\sum_{i\in O_W^{C}}|O_i|}, \quad O_W^{C,\mathrm{spoof}}=\{i\in O_W^{C}: \tau_i^{\mathrm{cxl}}-\tau_i^{\mathrm{post}}<T_{\mathrm{fast}}, |P_i-P_{1,\tau_i}^{M}|\leq k\cdot\mathrm{tick}, |O_i|\geq q_{\mathrm{large}}\})", OP(Manip, spoof_int)) \
  X(stale_ratio_bid, 1, DATA, TS, BEHAVIORAL, RATIO, NONE, "00/100/00", "Bid Stale Order Ratio", "买侧老单占比", "存活超T秒大单量占比(降频)", R"(\frac{\sum_{i \in O_t^{M,B,\mathrm{stale}}} |O_i|}{\sum_{i \in O_t^{M,B}} |O_i|}, \quad O^{M,s,\mathrm{stale}}=\{i: t-\tau_i^{\mathrm{post}}>T, |O_i|>q_{\mathrm{large}}\})", OP(Manip, stale_ratio_bid))                                                                                  \
  X(stale_ratio_ask, 1, DATA, TS, BEHAVIORAL, RATIO, NONE, "00/100/00", "Ask Stale Order Ratio", "卖侧老单占比", "存活超T秒大单量占比(降频)", R"(\frac{\sum_{i \in O_t^{M,A,\mathrm{stale}}} |O_i|}{\sum_{i \in O_t^{M,A}} |O_i|}, \quad O^{M,s,\mathrm{stale}}=\{i: t-\tau_i^{\mathrm{post}}>T, |O_i|>q_{\mathrm{large}}\})", OP(Manip, stale_ratio_ask))
