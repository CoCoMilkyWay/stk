#pragma once

// =============================================================================
// Depth - 盘口数据提取层: depth_buffer 的 N 档 → 4 组独立 CBuffer, 供下游算子复用
// =============================================================================
//   单位: 价格(元), 数量(股); 卖方数量存负值
//   布局: depth_buffer [0:N-1]=ask(N→1), [N:2N-1]=bid(1→N); 本类 bid_*[i]/ask_*[i] = 买/卖 i+1 档
//   符号钳制: LOB 抵扣模型下 Level::net_quantity 可能反号 (乱序/过度抵扣), 这里统一钳成 bid ≥ 0 / ask ≤ 0,
//            下游 (Book / MidPrice / MicroPrice / Spread / LabelReturn) 只依赖这一处的符号约定.
//   交叉簿不到这里: 买一 ≥ 卖一 的快照在 LOB 侧就不算 depth_updated, onDepth 域整个不跑 —— 本类
//            每次 compute 必推一格, 环长与 onDepth 次数严格同步 (LabelReturn 的 offset 依赖这条).
//   跨天: reset() 清空 4 组序列 —— 当日首次盘口更新前序列为空, onMinute 采样算子据此给 NaN, 不读昨日尾值.
//   涨跌停保护: 边界 = Fund 的当日适用涨跌停价 (PIT, 与 Valuation 同源; ST 5% / 无限制 NaN 都在数据里),
//              超限档强制为边界价, qty=1股. 边界 NaN (无限制 / 缺失) → 比较恒 false → 不钳.
//   只接 Fund.y[Fund.lim_up] / Fund.y[Fund.lim_dn] 两口 (非 Fund.out()): Fund compute=onDay 盘前已写 y,
//   其 Series 要到首个分钟 flush 才有当日值, 而盘口在 9:15 就开始更新. 单口引用也让 CMake 依赖表只记这两列.
//   【fast-math 契约】不做 isnan; NaN 边界靠 "比较遇 NaN 恒 false" 透传 (与 Valuation 同约).
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include <algorithm>

template <size_t N_LEVELS = L2::LOB_DEPTH>
class Depth {
public:
  static constexpr float PRICE_SCALE = 0.01f; // Level->price 是0.01元(分)单位 → 转为元
  static constexpr float LIMIT_QTY = 1.0f;    // 超限档位数量: 1股

  // 源层节点: 无标量输出口 (kCount = 0), 自持 4 组 N 档 CBuffer, 下游按 Depth.bid_qty 等直接引用
  enum Out : size_t { kCount = 0 };

  Depth(const TickData &tick_data, const float &lim_up, const float &lim_dn)
      : tick_data_(tick_data), lim_up_(lim_up), lim_dn_(lim_dn) {}

  Series bid_price[N_LEVELS];
  Series ask_price[N_LEVELS];
  Series bid_qty[N_LEVELS];
  Series ask_qty[N_LEVELS];

  // compute 与 flush 同域 (onDepth) 且相邻, 直接推入 CBuffer
  inline void compute() {
    const auto &depth = tick_data_.lob.depth_buffer;
    // Level::price 是档位下标, 加上基准才是绝对价 (分). 低价股基准为 0.
    const float base = static_cast<float>(tick_data_.lob.price_base);
    const float limit_up = lim_up_ + kPxEps;
    const float limit_down = lim_dn_ - kPxEps;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      const Level *bid_level = depth[L2::LOB_DEPTH + i];     // 买i+1档
      const Level *ask_level = depth[L2::LOB_DEPTH - 1 - i]; // 卖i+1档

      float bp = (base + static_cast<float>(bid_level->price)) * PRICE_SCALE;
      float ap = (base + static_cast<float>(ask_level->price)) * PRICE_SCALE;
      float bq = std::max(static_cast<float>(bid_level->net_quantity), 0.0f);
      float aq = std::min(static_cast<float>(ask_level->net_quantity), 0.0f); // 负值

      if (bp > limit_up) [[unlikely]] {
        bp = lim_up_, bq = LIMIT_QTY;
      } else if (bp < limit_down) [[unlikely]] {
        bp = lim_dn_, bq = LIMIT_QTY;
      }

      if (ap > limit_up) [[unlikely]] {
        ap = lim_up_, aq = -LIMIT_QTY;
      } else if (ap < limit_down) [[unlikely]] {
        ap = lim_dn_, aq = -LIMIT_QTY;
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
  const float &lim_up_, &lim_dn_; // Fund 节点当日涨 / 跌停价 (元), 引用 y[] 槽位
};

// ---- 节点实例 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Depth(N) N(Depth, (Depth<L2::LOB_DEPTH>), (tick_data, Fund.y[Fund.lim_up], Fund.y[Fund.lim_dn]), onDepth)
