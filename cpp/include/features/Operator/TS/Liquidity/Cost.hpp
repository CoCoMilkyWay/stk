#pragma once

// =============================================================================
// COST (Impact Cost) - 冲击成本
// =============================================================================
// 计算吃掉N档的加权平均执行价与中间价的偏离
//
// 【公式定义】
//   cost_buy_N  = VWAP(ask[1:N]) / mid_price - 1  (买方冲击成本, 正值)
//   cost_sell_N = 1 - VWAP(bid[1:N]) / mid_price  (卖方冲击成本, 正值)
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: {bid/ask}_price[0:N-1] (onDepth), {bid/ask}_qty[0:N-1] (onDepth), mid_price (onDepth)
//   输出: cost_{buy/sell}_N (onMinute)
//
// 【模板参数】
//   N_LEVELS - 档位数 (1, 5, 10)
//   IS_BUY   - true=买方冲击(吃ask), false=卖方冲击(吃bid)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, bool IS_BUY, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Cost {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  enum Out : size_t { value,
                      kCount };

  Cost(const CBuffer<float, L2::BLEN> (&price)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> (&qty)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> &mid_price,
       CBuffer<float, L2::BLEN> (&out)[kCount])
      : price_(price), qty_(qty), mid_price_(mid_price), out_(out[value]) {}

  inline void compute() {
    float sum_pv = 0.0f; // 价格*数量的累计（计算VWAP用）
    float sum_v = 0.0f;  // 总数量

    // 遍历前N档，计算VWAP（成交量加权平均价）
    for (size_t i = 0; i < N_LEVELS; ++i) {
      float p = price_[i].back(); // 档位价格
      float v = qty_[i].back();   // 档位数量
      if constexpr (!IS_BUY) {
        // bid qty 是正值，直接使用
      } else {
        // ask qty 是负值，取绝对值
        v = -v;
      }
      sum_pv += p * v; // 累加价格*数量
      sum_v += v;      // 累加数量
    }

    // 从MidPrice CBuffer读取中间价
    float mid = mid_price_.back();
    float cost = 0.0f;

    if (sum_v > 1e-6f && mid > 1e-6f) {
      // 计算VWAP：总金额 / 总数量
      float vwap = sum_pv / sum_v;
      if constexpr (IS_BUY) {
        // 买方冲击成本：吃掉N档ask的VWAP比mid高多少（比例）
        cost = vwap / mid - 1.0f; // 正值表示成本高于mid
      } else {
        // 卖方冲击成本：吃掉N档bid的VWAP比mid低多少（比例）
        cost = 1.0f - vwap / mid; // 正值表示收益低于mid
      }
    }

    value_ = cost;
  }

  inline void flush() {
    // 将compute中计算的冲击成本写入输出CBuffer
    out_.push_back(value_);
  }

  inline void reset() {}

private:
  const CBuffer<float, L2::BLEN> (&price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> &mid_price_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Cost_buy_1(N) N(Cost_buy_1, (Cost<1, true>), (DepthData.ask_price, DepthData.ask_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_buy_1(X) \
  X(cost_buy_1, 1, DATA, TS, LIQUIDITY, RAW, NONE, "00/100/00", "Buy Impact Cost 1-Level", "买方冲击成本1档", "吃1档卖盘的执行价vs中间价偏离(bps)(降频)", R"(\frac{\sum_{i=1}^{1} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{1} V_{i,t}^{M,A}} - 1)", OP(Cost_buy_1))

#define NODE_Cost_buy_10(N) N(Cost_buy_10, (Cost<10, true>), (DepthData.ask_price, DepthData.ask_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_buy_10(X) \
  X(cost_buy_10, 1, DATA, TS, LIQUIDITY, RAW, NONE, "00/100/00", "Buy Impact Cost 10-Level", "买方冲击成本10档", "吃10档卖盘的执行价vs中间价偏离(bps)(降频)", R"(\frac{\sum_{i=1}^{10} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{10} V_{i,t}^{M,A}} - 1)", OP(Cost_buy_10))

#define NODE_Cost_buy_5(N) N(Cost_buy_5, (Cost<5, true>), (DepthData.ask_price, DepthData.ask_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_buy_5(X) \
  X(cost_buy_5, 1, DATA, TS, LIQUIDITY, RAW, NONE, "00/100/00", "Buy Impact Cost 5-Level", "买方冲击成本5档", "吃5档卖盘的执行价vs中间价偏离(bps)(降频)", R"(\frac{\sum_{i=1}^{5} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{5} V_{i,t}^{M,A}} - 1)", OP(Cost_buy_5))

#define NODE_Cost_sell_1(N) N(Cost_sell_1, (Cost<1, false>), (DepthData.bid_price, DepthData.bid_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_sell_1(X) \
  X(cost_sell_1, 1, DATA, TS, LIQUIDITY, RAW, NONE, "00/100/00", "Sell Impact Cost 1-Level", "卖方冲击成本1档", "吃1档买盘的执行价vs中间价偏离(bps)(降频)", R"(1 - \frac{\sum_{i=1}^{1} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{1} V_{i,t}^{M,B}})", OP(Cost_sell_1))

#define NODE_Cost_sell_10(N) N(Cost_sell_10, (Cost<10, false>), (DepthData.bid_price, DepthData.bid_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_sell_10(X) \
  X(cost_sell_10, 1, DATA, TS, LIQUIDITY, RAW, NONE, "00/100/00", "Sell Impact Cost 10-Level", "卖方冲击成本10档", "吃10档买盘的执行价vs中间价偏离(bps)(降频)", R"(1 - \frac{\sum_{i=1}^{10} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{10} V_{i,t}^{M,B}})", OP(Cost_sell_10))

#define NODE_Cost_sell_5(N) N(Cost_sell_5, (Cost<5, false>), (DepthData.bid_price, DepthData.bid_qty, MidPrice.out()), onMinute, onMinute)

#define FIELDS_L1_Cost_sell_5(X) \
  X(cost_sell_5, 1, DATA, TS, LIQUIDITY, RAW, NONE, "00/100/00", "Sell Impact Cost 5-Level", "卖方冲击成本5档", "吃5档买盘的执行价vs中间价偏离(bps)(降频)", R"(1 - \frac{\sum_{i=1}^{5} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{5} V_{i,t}^{M,B}})", OP(Cost_sell_5))
