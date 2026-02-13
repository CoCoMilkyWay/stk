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
// 涨跌停保护 (根据股票代码自动判断: 主板10%, 科创/创业板20%, 北交所30%):
//   超出涨跌停价的档位强制设为涨跌停价, qty=1股, amt=0.01万元
//   第一天用首个mid price作为基准
//
// 使用方式:
//   每tick先调用 compute()，因子从 CBuffer 读取数据
//   跨天时调用 reset() 重置状态 (内部从 Taker_price_ 读取前收盘价)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

template <size_t N_LEVELS = L2::LOB_DEPTH>
class DepthData {
public:
  static constexpr float PRICE_SCALE = 0.01f; // Level->price 是0.01元(分)单位 → 转为元
  static constexpr float AMT_SCALE = 1e-4f;   // 元 → 万元
  static constexpr float LIMIT_QTY = 1.0f;    // 超限档位数量: 1股
  static constexpr float LIMIT_AMT = 0.01f;   // 超限档位金额: 0.01万元

  DepthData(const TickData &tick_data,
            CBuffer<float, L2::BLEN> &taker_price,
            CBuffer<float, L2::BLEN> (&bid_price)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&ask_price)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&bid_qty)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&ask_qty)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&bid_amt)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&ask_amt)[N_LEVELS],
            const std::string &asset_code)
      : tick_data_(tick_data),
        taker_price_(taker_price),
        bid_price_(bid_price),
        ask_price_(ask_price),
        bid_qty_(bid_qty),
        ask_qty_(ask_qty),
        bid_amt_(bid_amt),
        ask_amt_(ask_amt),
        limit_pct_(L2::infer_pct_limit(asset_code)) {}

  // 跨天重置 (清理状态, 减少计算量)
  void reset() {
    // 用前一天收盘价(最后成交价)设置depth的涨跌停保护
    float prev_close = taker_price_.size() > 0 ? taker_price_.back() : 0.0f;
    if (prev_close > 0.0f) {
      limit_up_ = prev_close * (1.0f + limit_pct_);
      limit_down_ = prev_close * (1.0f - limit_pct_);
      initialized_ = true;
    }
  }

  inline void compute() {
    const auto &depth = tick_data_.lob.depth_buffer;

    // 首次调用时用 mid price 初始化涨跌停价（第一天没有前收盘价时的fallback）
    if (!initialized_) [[unlikely]] {
      const Level *bid1 = depth[L2::LOB_DEPTH];
      const Level *ask1 = depth[L2::LOB_DEPTH - 1];
      float mid = (bid1->price + ask1->price) * 0.5f * PRICE_SCALE;
      // 用mid价设置涨跌停边界
      limit_up_ = mid * (1.0f + limit_pct_);
      limit_down_ = mid * (1.0f - limit_pct_);
      initialized_ = true;
    }

    // 遍历N档盘口数据，逐档提取并转换
    for (size_t i = 0; i < N_LEVELS; ++i) {
      // 从depth_buffer读取原始Level数据
      // 布局：[0:N-1]=ask(N→1), [N:2N-1]=bid(1→N)
      const Level *bid_level = depth[L2::LOB_DEPTH + i];     // 买i+1档
      const Level *ask_level = depth[L2::LOB_DEPTH - 1 - i]; // 卖i+1档

      // 单位转换：价格(分→元), 数量(股, 卖方保持负值), 金额(万元)
      float bid_price = static_cast<float>(bid_level->price) * PRICE_SCALE; // 分→元
      float ask_price = static_cast<float>(ask_level->price) * PRICE_SCALE;
      float bid_qty = static_cast<float>(bid_level->net_quantity); // 股
      float ask_qty = static_cast<float>(ask_level->net_quantity); // 股(负值)

      float bid_amt, ask_amt;

      // 涨跌停保护：超限的档位设为边界价，数量和金额设为最小值
      if (bid_price > limit_up_) [[unlikely]] {
        bid_price = limit_up_;
        bid_qty = LIMIT_QTY; // 1股
        bid_amt = LIMIT_AMT; // 0.01万元
      } else if (bid_price < limit_down_) [[unlikely]] {
        bid_price = limit_down_;
        bid_qty = LIMIT_QTY;
        bid_amt = LIMIT_AMT;
      } else {
        bid_amt = bid_price * bid_qty * AMT_SCALE; // 万元
      }

      if (ask_price > limit_up_) [[unlikely]] {
        ask_price = limit_up_;
        ask_qty = -LIMIT_QTY; // 卖方保持负值
        ask_amt = -LIMIT_AMT;
      } else if (ask_price < limit_down_) [[unlikely]] {
        ask_price = limit_down_;
        ask_qty = -LIMIT_QTY;
        ask_amt = -LIMIT_AMT;
      } else {
        ask_amt = ask_price * ask_qty * AMT_SCALE; // 万元(负值)
      }

      // 暂存到临时数组，flush时再批量写入CBuffer
      tmp_bid_price_[i] = bid_price;
      tmp_ask_price_[i] = ask_price;
      tmp_bid_qty_[i] = bid_qty;
      tmp_ask_qty_[i] = ask_qty;
      tmp_bid_amt_[i] = bid_amt;
      tmp_ask_amt_[i] = ask_amt;
    }
  }

  inline void flush() {
    // 将compute中计算的N档数据批量写入6组CBuffer数组
    // 每组有N_LEVELS个CBuffer，分别对应N档盘口
    for (size_t i = 0; i < N_LEVELS; ++i) {
      bid_price_[i].push_back(tmp_bid_price_[i]); // 买i+1档价格
      ask_price_[i].push_back(tmp_ask_price_[i]); // 卖i+1档价格
      bid_qty_[i].push_back(tmp_bid_qty_[i]);     // 买i+1档数量
      ask_qty_[i].push_back(tmp_ask_qty_[i]);     // 卖i+1档数量
      bid_amt_[i].push_back(tmp_bid_amt_[i]);     // 买i+1档金额
      ask_amt_[i].push_back(tmp_ask_amt_[i]);     // 卖i+1档金额
    }
  }

private:
  const TickData &tick_data_;

  // 涨跌停价 (基于前收盘价和对应的涨跌幅限制)
  const float limit_pct_;
  float limit_up_ = 0.0f;
  float limit_down_ = 0.0f;
  bool initialized_ = false;

  // 引用外部CBuffer (由DAG::L0管理)
  CBuffer<float, L2::BLEN> &taker_price_;
  CBuffer<float, L2::BLEN> (&bid_price_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&ask_price_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&bid_qty_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&ask_qty_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&bid_amt_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&ask_amt_)[N_LEVELS];

  // 临时计算结果
  float tmp_bid_price_[N_LEVELS] = {};
  float tmp_ask_price_[N_LEVELS] = {};
  float tmp_bid_qty_[N_LEVELS] = {};
  float tmp_ask_qty_[N_LEVELS] = {};
  float tmp_bid_amt_[N_LEVELS] = {};
  float tmp_ask_amt_[N_LEVELS] = {};
};
