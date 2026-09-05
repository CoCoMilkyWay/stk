#pragma once

// =============================================================================
// OA (Opening Auction) - 集合竞价 (09:15-09:25) 订单统计, 09:25 后保持定值
// =============================================================================
//   oa_bcr/acr = Σ|O^{C,B/A}| / Σ|O^{M,B/A}|   (买/卖方撤单率)
//   oa_btr/atr = Σ|O^{T,B/A}| / Σ|O^{M,B/A}|   (买/卖方成交率)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"

class OA {
  static constexpr size_t OA_END_L0 = 600; // 10分钟 * 60秒

public:
  enum Out : size_t { bcr,
                      acr,
                      btr,
                      atr,
                      kCount };
  float y[kCount] = {};

  explicit OA(TickData &td) : td_(td) {}

  // 每笔订单: 仅集合竞价时段按类型 × 方向累计金额 (万元)
  inline void compute() {
    if (td_.l0_index >= OA_END_L0)
      return;

    const auto &lob = td_.lob;
    const float amt = static_cast<float>(lob.volume) * lob.price / 10000.0f;
    const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);

    switch (lob.order_type) {
    case L2::OrderType::MAKER:
      (is_bid ? amt_maker_bid_ : amt_maker_ask_) += amt;
      break;
    case L2::OrderType::TAKER:
      (is_bid ? amt_taker_bid_ : amt_taker_ask_) += amt;
      break;
    case L2::OrderType::CANCEL:
      (is_bid ? amt_cancel_bid_ : amt_cancel_ask_) += amt;
      break;
    }
  }

  // 分钟末结算 (累计量不清零, 连续竞价期间比率保持不变)
  inline void flush() {
    y[bcr] = amt_maker_bid_ > 1e-6f ? amt_cancel_bid_ / amt_maker_bid_ : 0.0f;
    y[acr] = amt_maker_ask_ > 1e-6f ? amt_cancel_ask_ / amt_maker_ask_ : 0.0f;
    y[btr] = amt_maker_bid_ > 1e-6f ? amt_taker_bid_ / amt_maker_bid_ : 0.0f;
    y[atr] = amt_maker_ask_ > 1e-6f ? amt_taker_ask_ / amt_maker_ask_ : 0.0f;
  }

  void reset() {
    amt_maker_bid_ = amt_maker_ask_ = 0.0f;
    amt_taker_bid_ = amt_taker_ask_ = 0.0f;
    amt_cancel_bid_ = amt_cancel_ask_ = 0.0f;
  }

private:
  TickData &td_;

  float amt_maker_bid_ = 0.0f, amt_maker_ask_ = 0.0f;
  float amt_taker_bid_ = 0.0f, amt_taker_ask_ = 0.0f;
  float amt_cancel_bid_ = 0.0f, amt_cancel_ask_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Oa(N) N(Oa, (OA), (tick_data), onTick, onMinute)

#define FIELDS_L1_Oa(X, CAT1)                                                                                                                                                                                                                                                                                             \
  X(oa_bcr, CAT1, RATIO, NONE, "OA Bid Cancel Ratio", "早盘竞价买方撤单率", "集合竞价买方撤单额占买方挂单额比例(降频)", R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{C,B}|}{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{M,B}|}, \quad \mathcal{T}_{\mathrm{OA}}=[09\!:\!15,09\!:\!25))", OP(Oa, bcr)) \
  X(oa_acr, CAT1, RATIO, NONE, "OA Ask Cancel Ratio", "早盘竞价卖方撤单率", "集合竞价卖方撤单额占卖方挂单额比例(降频)", R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{C,A}|}{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{M,A}|}, \quad \mathcal{T}_{\mathrm{OA}}=[09\!:\!15,09\!:\!25))", OP(Oa, acr)) \
  X(oa_btr, CAT1, RATIO, NONE, "OA Bid Trade Ratio", "早盘竞价买方成交率", "集合竞价买方成交额占买方挂单额比例(降频)", R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{T,B}|}{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{M,B}|}, \quad \mathcal{T}_{\mathrm{OA}}=[09\!:\!15,09\!:\!25))", OP(Oa, btr))  \
  X(oa_atr, CAT1, RATIO, NONE, "OA Ask Trade Ratio", "早盘竞价卖方成交率", "集合竞价卖方成交额占卖方挂单额比例(降频)", R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{T,A}|}{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{M,A}|}, \quad \mathcal{T}_{\mathrm{OA}}=[09\!:\!15,09\!:\!25))", OP(Oa, atr))
