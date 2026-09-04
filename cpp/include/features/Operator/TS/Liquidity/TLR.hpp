#pragma once

// =============================================================================
// TLR (Top Level Ratio) - 顶部档位占比
// =============================================================================
// 前N档占总量的比例，衡量是否容易被击穿
//
// 【公式定义】
//   TBR_N = Σ V_{i}^{M,B} / V_{all}^{M,B}, i=1..N  (买侧)
//   TAR_N = Σ V_{i}^{M,A} / V_{all}^{M,A}, i=1..N  (卖侧)
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: bid_qty[0:N-1] (onDepth), ask_qty[0:N-1] (onDepth), TickData.lob.all_bid_volume (onDepth), all_ask_volume (onDepth)
//   输出: tbr_N (onMinute) / tar_N (onMinute)
//
// 【模板参数】
//   N_LEVELS - 顶部档位数
//   IS_BID   - true=买侧(TBR), false=卖侧(TAR)
//
// 【使用示例】
//   TLR<5, true>  tbr_5{BidQty_, AskQty_, td, Tbr_5_};
//   TLR<5, false> tar_5{BidQty_, AskQty_, td, Tar_5_};
//
// 【备注】
//   - 值越大说明订单集中在前N档，容易被击穿
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

  TLR(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      TickData &td,
      CBuffer<float, L2::BLEN> (&out)[kCount])
      : bid_qty_(bid_qty), ask_qty_(ask_qty), td_(td), out_(out[value]) {}

  inline void compute() {
    float top_sum = 0.0f; // 前N档总量

    // 遍历前N档，累计数量
    for (size_t i = 0; i < N_LEVELS; ++i) {
      if constexpr (IS_BID) {
        top_sum += bid_qty_[i].back(); // 累加前N档买量
      } else {
        top_sum += -ask_qty_[i].back(); // 累加前N档卖量（取反）
      }
    }

    // 从TickData读取交易所提供的全市场挂单量作为分母
    float total_sum;
    if constexpr (IS_BID) {
      total_sum = static_cast<float>(td_.lob.all_bid_volume); // 全市场买单量
    } else {
      total_sum = static_cast<float>(td_.lob.all_ask_volume); // 全市场卖单量
    }

    // 计算顶部档位占比：前N档 / 全市场总量
    // 值越大说明订单集中在前N档，容易被击穿
    value_ = total_sum > 1e-6f ? top_sum / total_sum : 0.0f;
  }

  inline void flush() {
    out_.push_back(value_);
  }

  inline void reset() {}

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  TickData &td_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Tar_5(N) N(Tar_5, (TLR<5, false>), (DepthData.bid_qty, DepthData.ask_qty, tick_data), onMinute, onMinute)

#define FIELDS_L1_Tar_5(X) \
  X(tar_5, 1, DATA, TS, LIQUIDITY, RATIO, NONE, "00/100/00", "Top 5-level Ask Ratio", "前5档卖单占比", "卖单侧是否容易被击穿(降频)", R"(\frac{\sum_{i=1}^{N} V_{i,t}^{M,A}}{\sum_{i=1}^{\infty} V_{i,t}^{M,A}}, \quad N = 5)", OP(Tar_5))

#define NODE_Tbr_5(N) N(Tbr_5, (TLR<5, true>), (DepthData.bid_qty, DepthData.ask_qty, tick_data), onMinute, onMinute)

#define FIELDS_L1_Tbr_5(X) \
  X(tbr_5, 1, DATA, TS, LIQUIDITY, RATIO, NONE, "00/100/00", "Top 5-level Bid Ratio", "前5档买单占比", "买单侧是否容易被击穿(降频)", R"(\frac{\sum_{i=1}^{N} V_{i,t}^{M,B}}{\sum_{i=1}^{\infty} V_{i,t}^{M,B}}, \quad N = 5)", OP(Tbr_5))
