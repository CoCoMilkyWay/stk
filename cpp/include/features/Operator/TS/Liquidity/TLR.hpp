#pragma once

// =============================================================================
// TLR (Top Level Ratio) - 顶部档位占比: 前 N 档量 / 交易所全市场挂单量
// =============================================================================
//   TBR_N = Σ V_i^{M,B} / V_all^{M,B}   (IS_BID=true)
//   TAR_N = Σ V_i^{M,A} / V_all^{M,A}   (IS_BID=false)
//   值越大订单越集中在前 N 档, 越易被击穿
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

template <size_t N_LEVELS, bool IS_BID, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class TLR {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  TLR(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      TickData &td)
      : bid_qty_(bid_qty), ask_qty_(ask_qty), td_(td) {}

  inline void compute() {
    float top_sum = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i)
      top_sum += IS_BID ? bid_qty_[i].back() : -ask_qty_[i].back(); // ask 存负值

    float total_sum = static_cast<float>(IS_BID ? td_.lob.all_bid_volume : td_.lob.all_ask_volume);
    y[value] = total_sum > 1e-6f ? top_sum / total_sum : 0.0f;
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  TickData &td_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Tar_5(N) N(Tar_5, (TLR<5, false>), (DepthData.bid_qty, DepthData.ask_qty, tick_data), onMinute, onMinute)

#define FIELDS_L1_Tar_5(X) \
  X(tar_5, 1, DATA, LIQUIDITY, RATIO, NONE, "00/100/00", "Top 5-level Ask Ratio", "前5档卖单占比", "卖单侧是否容易被击穿(降频)", R"(\frac{\sum_{i=1}^{N} V_{i,t}^{M,A}}{\sum_{i=1}^{\infty} V_{i,t}^{M,A}}, \quad N = 5)", OP(Tar_5))

#define NODE_Tbr_5(N) N(Tbr_5, (TLR<5, true>), (DepthData.bid_qty, DepthData.ask_qty, tick_data), onMinute, onMinute)

#define FIELDS_L1_Tbr_5(X) \
  X(tbr_5, 1, DATA, LIQUIDITY, RATIO, NONE, "00/100/00", "Top 5-level Bid Ratio", "前5档买单占比", "买单侧是否容易被击穿(降频)", R"(\frac{\sum_{i=1}^{N} V_{i,t}^{M,B}}{\sum_{i=1}^{\infty} V_{i,t}^{M,B}}, \quad N = 5)", OP(Tbr_5))
