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
//
// 使用方式:
//   每tick先调用 compute()，因子从 CBuffer 读取数据
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

template <size_t N_LEVELS = L2::LOB_DEPTH>
class DepthData {
public:
  static constexpr float PRICE_SCALE = 0.01f; // Level->price 是0.01元(分)单位 → 转为元

  DepthData(const TickData &tick_data,
            CBuffer<float, L2::BLEN> (&bid_price)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&ask_price)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&bid_qty)[N_LEVELS],
            CBuffer<float, L2::BLEN> (&ask_qty)[N_LEVELS])
      : tick_data_(tick_data),
        bid_price_(bid_price),
        ask_price_(ask_price),
        bid_qty_(bid_qty),
        ask_qty_(ask_qty) {}

  void compute() {
    const auto &depth = tick_data_.lob.depth_buffer;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      // depth布局: [0:N-1]=ask(N→1), [N:2N-1]=bid(1→N)
      // bid_i = depth[N + i], ask_i = depth[N - 1 - i]
      const Level *bid_level = depth[L2::LOB_DEPTH + i];
      const Level *ask_level = depth[L2::LOB_DEPTH - 1 - i];

      // 价格: Level->price是分(0.01元)单位，需转为元
      bid_price_[i].push_back(static_cast<float>(bid_level->price) * PRICE_SCALE);
      ask_price_[i].push_back(static_cast<float>(ask_level->price) * PRICE_SCALE);

      // 数量: 股, 卖方保持负值
      bid_qty_[i].push_back(static_cast<float>(bid_level->net_quantity));
      ask_qty_[i].push_back(static_cast<float>(ask_level->net_quantity));
    }
  }

private:
  const TickData &tick_data_;

  // 引用外部CBuffer (由Tick_Sequential管理)
  CBuffer<float, L2::BLEN> (&bid_price_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&ask_price_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&bid_qty_)[N_LEVELS];
  CBuffer<float, L2::BLEN> (&ask_qty_)[N_LEVELS];
};
