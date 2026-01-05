#pragma once

// =============================================================================
// DepthData - 盘口数据提取层
// =============================================================================
// 将 depth_buffer 的 N 档数据提取到独立的 CBuffer
// 供下游因子 (VOI, SOIR等) 和 write_lob_depth 复用
//
// 数据布局:
//   depth_buffer: [0:N-1]=ask(N→1), [N:2N-1]=bid(1→N)
//   bid_price_[i], bid_qty_[i]: 买i+1档 (i=0表示买一)
//   ask_price_[i], ask_qty_[i]: 卖i+1档 (i=0表示卖一)
//
// 单位标准 (统一转换, 下游无需再处理):
//   价格: 元 (RMB)
//   数量: 股 (shares)
//   金额: 万元 (10000 RMB)
//
// 涨跌停保护 (±20%):
//   超出涨跌停价的档位强制设为涨跌停价, qty=1股, amt=0.01万元
//   第一天用首个mid price作为基准
//
// 使用方式:
//   每tick先调用 compute()，因子从 CBuffer 读取数据
//   跨天时调用 set_prev_close() 设置前收盘价
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

template <size_t N_LEVELS = L2::LOB_DEPTH>
class DepthData {
public:
  static constexpr float PRICE_SCALE = 0.01f; // Level->price 是0.01元(分)单位 → 转为元
  static constexpr float AMT_SCALE = 1e-4f;   // 元 → 万元
  static constexpr float LIMIT_PCT = 0.20f;   // 涨跌停幅度 ±20%
  static constexpr float LIMIT_QTY = 1.0f;    // 超限档位数量: 1股
  static constexpr float LIMIT_AMT = 0.01f;   // 超限档位金额: 0.01万元

  DepthData(const TickData &tick_data,
            CBuffer<float, L2::BLEN> (&bid_price)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&ask_price)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&bid_qty)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&ask_qty)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&bid_amt)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&ask_amt)[N_LEVELS])
      : tick_data_(tick_data),
        bid_price_(bid_price),
        ask_price_(ask_price),
        bid_qty_(bid_qty),
        ask_qty_(ask_qty),
        bid_amt_(bid_amt),
        ask_amt_(ask_amt) {}

  // 设置前收盘价 (跨天时调用)
  void set_prev_close(float prev_close) {
    limit_up_ = prev_close * (1.0f + LIMIT_PCT);
    limit_down_ = prev_close * (1.0f - LIMIT_PCT);
    initialized_ = true;
  }

  void compute() {
    const auto &depth = tick_data_.lob.depth_buffer;

    // 首次调用时用 mid price 初始化涨跌停价
    if (!initialized_) [[unlikely]] {
      const Level *bid1 = depth[L2::LOB_DEPTH];
      const Level *ask1 = depth[L2::LOB_DEPTH - 1];
      float mid = (bid1->price + ask1->price) * 0.5f * PRICE_SCALE;
      set_prev_close(mid);
    }

    for (size_t i = 0; i < N_LEVELS; ++i) {
      // depth布局: [0:N-1]=ask(N→1), [N:2N-1]=bid(1→N)
      // bid_i = depth[N + i], ask_i = depth[N - 1 - i]
      const Level *bid_level = depth[L2::LOB_DEPTH + i];
      const Level *ask_level = depth[L2::LOB_DEPTH - 1 - i];

      // 价格: Level->price是分(0.01元)单位，需转为元
      float bid_price = static_cast<float>(bid_level->price) * PRICE_SCALE;
      float ask_price = static_cast<float>(ask_level->price) * PRICE_SCALE;

      // 数量: 股, 卖方保持负值
      float bid_qty = static_cast<float>(bid_level->net_quantity);
      float ask_qty = static_cast<float>(ask_level->net_quantity);

      // 金额: 万元 = price * qty / 10000
      float bid_amt = bid_price * bid_qty * AMT_SCALE;
      float ask_amt = ask_price * ask_qty * AMT_SCALE;

      // 涨跌停保护: 超限档位强制设为涨跌停价
      if (bid_price > limit_up_) [[unlikely]] {
        bid_price = limit_up_;
        bid_qty = LIMIT_QTY;
        bid_amt = LIMIT_AMT;
      } else if (bid_price < limit_down_) [[unlikely]] {
        bid_price = limit_down_;
        bid_qty = LIMIT_QTY;
        bid_amt = LIMIT_AMT;
      }

      if (ask_price > limit_up_) [[unlikely]] {
        ask_price = limit_up_;
        ask_qty = -LIMIT_QTY; // 卖方负值
        ask_amt = -LIMIT_AMT;
      } else if (ask_price < limit_down_) [[unlikely]] {
        ask_price = limit_down_;
        ask_qty = -LIMIT_QTY;
        ask_amt = -LIMIT_AMT;
      }

      bid_price_[i].push_back(bid_price);
      ask_price_[i].push_back(ask_price);
      bid_qty_[i].push_back(bid_qty);
      ask_qty_[i].push_back(ask_qty);
      bid_amt_[i].push_back(bid_amt);
      ask_amt_[i].push_back(ask_amt);
    }
  }

private:
  const TickData &tick_data_;

  // 涨跌停价 (基于前收盘价 ±20%)
  float limit_up_ = 0.0f;
  float limit_down_ = 0.0f;
  bool initialized_ = false;

  // 引用外部CBuffer (由DAG::L0管理)
  CBuffer<float, L2::BLEN> (&bid_price_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&ask_price_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&bid_qty_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&ask_qty_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&bid_amt_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&ask_amt_)[N_LEVELS];
};
