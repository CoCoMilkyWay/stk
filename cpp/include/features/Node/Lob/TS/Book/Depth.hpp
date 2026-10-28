#pragma once

// =============================================================================
// Depth - 盘口数据提取层: depth_buffer 的 N 个价位 → 4 组 DepthPorts (当前簿), 供下游节点复用
// =============================================================================
//   单位: 价格(元), 数量(股); 卖方数量存负值
//   布局: depth_buffer [0:N-1]=ask(N→1), [N:2N-1]=bid(1→N); 本类 bid_*[i]/ask_*[i] = 买/卖 i+1 价位
//   符号钳制: 订单簿抵扣模型下 Level::net_quantity 可能反号 (乱序/过度抵扣), 这里统一钳成 bid ≥ 0 / ask ≤ 0,
//            下游 (Book / MidPrice / MicroPrice / Spread / LabelLob) 只依赖这一处的符号约定.
//   集合竞价簿也到这里: 订单簿竞价分支每节流点整簿重建并置 depth_updated, 买一 ≥ 卖一 (交叉) 照写;
//            可不可成交由消费方按 lob.market_state 自判 (LabelLob 只收连续竞价簿).
//   只存当前簿: onDepth 之前读 = 上一次更新的终簿 (LabelLob 结算上一活跃秒即靠这一点), 之后读 = 本次更新.
//   跨天: reset() 置 NaN —— 当日首次盘口更新前无簿, 不读昨日尾值.
//   涨跌停保护: 边界 = Fund 的当日适用涨跌停价 (PIT, 与 Valuation 同源; ST 5% / 无限制 NaN 都在数据里),
//              超限价位强制为边界价, qty=1股. 边界 NaN (无限制 / 缺失) → 比较恒 false → 不钳.
//   只接 Fund.out(Fund.lim_up) / Fund.out(Fund.lim_dn) 两口: Fund 是日频节点, 盘前承接后即当日适用值 (9:15 起可读).
//   单口引用也让 CMake 依赖表只记这两列.
//   【fast-math 契约】不做 isnan; NaN 边界靠 "比较遇 NaN 恒 false" 透传 (与 Valuation 同约).
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include <algorithm>

template <size_t N_LEVELS = L2::LOB_DEPTH>
class Depth {
public:
  static constexpr float LIMIT_QTY = 1.0f; // 超限价位数量: 1股

  // 源层节点: 无标量输出口 (kCount = 0), 自持 4 组 N 个价位, 下游按 Depth.bid_qty 等直接引用
  enum Out : size_t { kCount = 0 };

  Depth(const TickData &tick_data, const Port &lim_up, const Port &lim_dn)
      : tick_data_(tick_data), lim_up_(lim_up), lim_dn_(lim_dn) {}

  Ports<N_LEVELS> bid_price{};
  Ports<N_LEVELS> ask_price{};
  Ports<N_LEVELS> bid_qty{};
  Ports<N_LEVELS> ask_qty{};

  // compute 与 flush 同域 (onDepth) 且相邻, 直接写当前簿
  inline void compute() {
    const LOB_Feature &lob = tick_data_.lob;
    const auto &depth = lob.depth_buffer;
    const float lim_up = lim_up_, lim_dn = lim_dn_;
    const float limit_up = lim_up + kPxEps;
    const float limit_down = lim_dn - kPxEps;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      const Level *bid_level = depth[L2::LOB_DEPTH + i];     // 买 i+1 价位
      const Level *ask_level = depth[L2::LOB_DEPTH - 1 - i]; // 卖 i+1 价位

      float bp = lob.price_of(bid_level);
      float ap = lob.price_of(ask_level);
      float bq = std::max(static_cast<float>(bid_level->net_quantity), 0.0f);
      float aq = std::min(static_cast<float>(ask_level->net_quantity), 0.0f); // 负值

      if (bp > limit_up) [[unlikely]] {
        bp = lim_up, bq = LIMIT_QTY;
      } else if (bp < limit_down) [[unlikely]] {
        bp = lim_dn, bq = LIMIT_QTY;
      }

      if (ap > limit_up) [[unlikely]] {
        ap = lim_up, aq = -LIMIT_QTY;
      } else if (ap < limit_down) [[unlikely]] {
        ap = lim_dn, aq = -LIMIT_QTY;
      }

      bid_price[i] = bp;
      ask_price[i] = ap;
      bid_qty[i] = bq;
      ask_qty[i] = aq;
    }
  }

  // 跨天: 置 NaN (昨日盘口对今日无意义)
  void reset() {
    bid_price.fill(kNaN);
    ask_price.fill(kNaN);
    bid_qty.fill(kNaN);
    ask_qty.fill(kNaN);
  }

private:
  const TickData &tick_data_;
  const Port &lim_up_, &lim_dn_; // Fund 日频口: 当日涨 / 跌停价 (元)
};

// ---- 节点实例 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Depth(N) N(Depth, (Depth<L2::LOB_DEPTH>), (tick_data, Fund.out(Fund.lim_up), Fund.out(Fund.lim_dn)), onDepth)
