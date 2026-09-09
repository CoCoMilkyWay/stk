#pragma once

// =============================================================================
// Depth - 盘口数据提取层: depth_buffer 的 N 档 → 4 组独立 CBuffer, 供下游算子复用
// =============================================================================
//   单位: 价格(元), 数量(股); 卖方数量存负值
//   布局: depth_buffer [0:N-1]=ask(N→1), [N:2N-1]=bid(1→N); 本类 bid_*[i]/ask_*[i] = 买/卖 i+1 档
//   符号钳制: LOB 抵扣模型下 Level::net_quantity 可能反号 (乱序/过度抵扣), 这里统一钳成 bid ≥ 0 / ask ≤ 0,
//            下游 (CI / Cost / TLR / MicroPrice / LabelReturn) 只依赖这一处的符号约定.
//   跨天: reset() 清空 4 组序列 —— 当日首次盘口更新前序列为空, onMinute 采样算子据此给 NaN, 不读昨日尾值.
//   涨跌停保护: 边界 = Fund 的当日适用涨跌停价 (PIT, 与 Valuation 同源; ST 5% / 无限制 NaN 都在数据里),
//              超限档强制为边界价, qty=1股. 边界 NaN (无限制 / 缺失) → 比较恒 false → 不钳.
//   读 Fund.y 而非 Fund.out(): Fund compute=onDay 盘前已写 y, 其 Series 要到首个分钟 flush 才有当日值,
//   而盘口在 9:15 就开始更新.
//   【fast-math 契约】不做 isnan; NaN 边界靠 "比较遇 NaN 恒 false" 透传 (与 Valuation 同约).
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "features/Operator/TS/Fund/Fund.hpp" // Fund::Out 口下标
#include <algorithm>

template <size_t N_LEVELS = L2::LOB_DEPTH>
class Depth {
public:
  static constexpr float PRICE_SCALE = 0.01f; // Level->price 是0.01元(分)单位 → 转为元
  static constexpr float LIMIT_QTY = 1.0f;    // 超限档位数量: 1股
  static constexpr float LIMIT_EPS = 1e-4f;   // 边界比较容差 (档位价由整数分换算, 与解析的涨跌停价可能差 1 ulp)

  // 源层节点: 无标量输出口 (kCount = 0), 自持 4 组 N 档 CBuffer, 下游按 Depth.bid_qty 等直接引用
  enum Out : size_t { kCount = 0 };

  Depth(const TickData &tick_data, const float (&fund)[Fund::kCount])
      : tick_data_(tick_data), fund_(fund) {}

  Series bid_price[N_LEVELS];
  Series ask_price[N_LEVELS];
  Series bid_qty[N_LEVELS];
  Series ask_qty[N_LEVELS];

  // compute 与 flush 同域 (onDepth) 且相邻, 直接推入 CBuffer
  inline void compute() {
    const auto &depth = tick_data_.lob.depth_buffer;
    // Level::price 是档位下标, 加上基准才是绝对价 (分). 低价股基准为 0.
    const float base = static_cast<float>(tick_data_.lob.price_base);
    const float limit_up = fund_[Fund::up_lim] + LIMIT_EPS;
    const float limit_down = fund_[Fund::dn_lim] - LIMIT_EPS;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      const Level *bid_level = depth[L2::LOB_DEPTH + i];     // 买i+1档
      const Level *ask_level = depth[L2::LOB_DEPTH - 1 - i]; // 卖i+1档

      float bp = (base + static_cast<float>(bid_level->price)) * PRICE_SCALE;
      float ap = (base + static_cast<float>(ask_level->price)) * PRICE_SCALE;
      float bq = std::max(static_cast<float>(bid_level->net_quantity), 0.0f);
      float aq = std::min(static_cast<float>(ask_level->net_quantity), 0.0f); // 负值

      if (bp > limit_up) [[unlikely]] {
        bp = fund_[Fund::up_lim], bq = LIMIT_QTY;
      } else if (bp < limit_down) [[unlikely]] {
        bp = fund_[Fund::dn_lim], bq = LIMIT_QTY;
      }

      if (ap > limit_up) [[unlikely]] {
        ap = fund_[Fund::up_lim], aq = -LIMIT_QTY;
      } else if (ap < limit_down) [[unlikely]] {
        ap = fund_[Fund::dn_lim], aq = -LIMIT_QTY;
      }

      bid_price[i].push_back(bp);
      ask_price[i].push_back(ap);
      bid_qty[i].push_back(bq);
      ask_qty[i].push_back(aq);
    }
  }

  // 跨天: 清空序列 (昨日盘口对今日无意义)
  void reset() {
    for (size_t i = 0; i < N_LEVELS; ++i) {
      bid_price[i].clear();
      ask_price[i].clear();
      bid_qty[i].clear();
      ask_qty[i].clear();
    }
  }

private:
  const TickData &tick_data_;
  const float (&fund_)[Fund::kCount]; // Fund 节点当日输出 (up_lim / dn_lim, 元)
};

// ---- 节点实例 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Depth(N) N(Depth, (Depth<L2::LOB_DEPTH>), (tick_data, Fund.y), onDepth)
