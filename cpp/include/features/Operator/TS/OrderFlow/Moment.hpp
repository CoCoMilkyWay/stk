#pragma once

// =============================================================================
// Moment - 逐笔尺度高阶矩 (compute=onTick, flush=onMinute; 一阶量 Σ|O| / ΣP|O| / #O 在 Flow, 这里只放落盘后不可逆的二阶以上)
// =============================================================================
//   单笔尺度分布的分钟内可加幂和 (股^k; 事件 / 侧 与 Flow 同名同源), 因子层与 Flow 的 vol / n 合出 单笔量 均值 / 标准差 / 偏度 / 峰度
//   (日级 = 各行求和后再合, 精确):
//     vol{2,3,4}_{taker,maker,cancel}_{bid,ask}   Σ|O|², Σ|O|³, Σ|O|⁴
//   委托价一阶 / 二阶 (元 / 元²; 与 amt_maker = ΣP|O| 合出 corr(委托量, 委托价)):
//     px{1,2}_maker_{bid,ask}                      ΣP, ΣP²  只计 P>0 (市价单 P=0 不入; 配对的 n 用 Flow n_maker 时含市价单, 深市极少, 偏差可忽略)
//     成交价 / 撤单价 不做: 成交价分布归 vwap / twap / Realized; 深市撤单 P=0 无价.
//   支撑: 华泰 072 skew/kurt_order_diff / early_kurt_order / corr_buyorder_volume_price; 光大 024 bs_std_ratio (主买 / 主卖 单笔量标准差比).
//   量纲: |O| ≤ 10⁶ 股 (交易所单笔上限) → |O|⁴ ≤ 10²⁴, fp32 分钟累加不溢出; fp16 落盘 Log Tf.
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"

#define MOMENT_EVENTS(T) T(taker) T(maker) T(cancel)
#define MOMENT_ENUM(e) vol2_##e##_bid, vol2_##e##_ask, vol3_##e##_bid, vol3_##e##_ask, vol4_##e##_bid, vol4_##e##_ask,

class Moment {
  static constexpr size_t NE = 3, NK = 3; // 事件 {taker, maker, cancel} × 幂 {2, 3, 4}

public:
  // 布局: 事件 外层 × 幂 中层 × 侧 内层 —— y[(e·NK + k)·2 + s]; 其后 px1 ×2, px2 ×2
  enum Out : size_t { MOMENT_EVENTS(MOMENT_ENUM) px1_maker_bid,
                      px1_maker_ask,
                      px2_maker_bid,
                      px2_maker_ask,
                      kCount };
  static_assert(px1_maker_bid == NE * NK * 2 && kCount == NE * NK * 2 + 4);
  float y[kCount] = {};

  explicit Moment(const TickData &td) : td_(td) {}

  inline void compute() {
    const auto &lob = td_.lob;
    size_t e;
    switch (lob.order_type) {
    case L2::OrderType::TAKER:
      e = 0;
      break;
    case L2::OrderType::MAKER:
      e = 1;
      break;
    case L2::OrderType::CANCEL:
      e = 2;
      break;
    default:
      return;
    }
    const size_t s = lob.order_dir == L2::OrderDirection::BID ? 0 : 1;
    const float v = static_cast<float>(lob.volume);
    const float v2 = v * v;
    acc_[e][0][s] += v2, acc_[e][1][s] += v2 * v, acc_[e][2][s] += v2 * v2;
    if (e == 1 && lob.price > 0.0f)
      px_[0][s] += lob.price, px_[1][s] += lob.price * lob.price;
  }

  inline void flush() {
    for (size_t e = 0; e < NE; ++e)
      for (size_t k = 0; k < NK; ++k)
        for (size_t s = 0; s < 2; ++s)
          y[(e * NK + k) * 2 + s] = acc_[e][k][s];
    y[px1_maker_bid] = px_[0][0], y[px1_maker_ask] = px_[0][1];
    y[px2_maker_bid] = px_[1][0], y[px2_maker_ask] = px_[1][1];
    clear();
  }

  void reset() { clear(); }

private:
  void clear() {
    for (auto &e : acc_)
      for (auto &k : e)
        k[0] = k[1] = 0.0f;
    px_[0][0] = px_[0][1] = px_[1][0] = px_[1][1] = 0.0f;
  }

  const TickData &td_;
  float acc_[NE][NK][2] = {}; // [事件][幂-2][侧]
  float px_[2][2] = {};       // [幂-1][侧] 委托价 ΣP / ΣP²
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Moment(N) N(Moment, (Moment), (tick_data), onTick, onMinute)

// 一个事件类型 e 的 幂 {2,3,4} × 买/卖 6 行; E = 公式事件上标, EN / CN = 英 / 中文事件名 (字面串)
#define MOMENT_EVENT_ROWS(X, CAT1, e, E, EN, CN)                                                                                                                                                                 \
  X(vol2_##e##_bid, CAT1, AUTO, EN " Bid Volume^2", "买方" CN "量平方和", "分钟内买方" CN "单笔量平方和(股²)", R"(\sum_{\tau \in \Delta t} |O_\tau^{)" E R"(,B}|^2)", OP(Moment, vol2_##e##_bid, Log, None))     \
  X(vol2_##e##_ask, CAT1, AUTO, EN " Ask Volume^2", "卖方" CN "量平方和", "分钟内卖方" CN "单笔量平方和(股²)", R"(\sum_{\tau \in \Delta t} |O_\tau^{)" E R"(,A}|^2)", OP(Moment, vol2_##e##_ask, Log, None))     \
  X(vol3_##e##_bid, CAT1, AUTO, EN " Bid Volume^3", "买方" CN "量立方和", "分钟内买方" CN "单笔量立方和(股³)", R"(\sum_{\tau \in \Delta t} |O_\tau^{)" E R"(,B}|^3)", OP(Moment, vol3_##e##_bid, Log, None))     \
  X(vol3_##e##_ask, CAT1, AUTO, EN " Ask Volume^3", "卖方" CN "量立方和", "分钟内卖方" CN "单笔量立方和(股³)", R"(\sum_{\tau \in \Delta t} |O_\tau^{)" E R"(,A}|^3)", OP(Moment, vol3_##e##_ask, Log, None))     \
  X(vol4_##e##_bid, CAT1, AUTO, EN " Bid Volume^4", "买方" CN "量四次方和", "分钟内买方" CN "单笔量四次方和(股⁴)", R"(\sum_{\tau \in \Delta t} |O_\tau^{)" E R"(,B}|^4)", OP(Moment, vol4_##e##_bid, Log, None)) \
  X(vol4_##e##_ask, CAT1, AUTO, EN " Ask Volume^4", "卖方" CN "量四次方和", "分钟内卖方" CN "单笔量四次方和(股⁴)", R"(\sum_{\tau \in \Delta t} |O_\tau^{)" E R"(,A}|^4)", OP(Moment, vol4_##e##_ask, Log, None))

#define FIELDS_L1_Moment(X, CAT1)                                                                                                                                                                                                          \
  MOMENT_EVENT_ROWS(X, CAT1, taker, "T", "Taker", "主动成交")                                                                                                                                                                              \
  MOMENT_EVENT_ROWS(X, CAT1, maker, "M", "Maker", "新增委托")                                                                                                                                                                              \
  MOMENT_EVENT_ROWS(X, CAT1, cancel, "C", "Cancel", "撤单")                                                                                                                                                                                \
  X(px1_maker_bid, CAT1, AUTO, "Maker Bid Price Sum", "买委托价和", "分钟内买方新增限价委托的委托价之和(元; 市价单不入)", R"(\sum_{\tau \in \Delta t} P_\tau \mathbf{1}[O_\tau^{M,B}, P_\tau > 0])", OP(Moment, px1_maker_bid, Log, None)) \
  X(px1_maker_ask, CAT1, AUTO, "Maker Ask Price Sum", "卖委托价和", "分钟内卖方新增限价委托的委托价之和(元; 市价单不入)", R"(\sum_{\tau \in \Delta t} P_\tau \mathbf{1}[O_\tau^{M,A}, P_\tau > 0])", OP(Moment, px1_maker_ask, Log, None)) \
  X(px2_maker_bid, CAT1, AUTO, "Maker Bid Price^2 Sum", "买委托价平方和", "分钟内买方新增限价委托的委托价平方和(元²)", R"(\sum_{\tau \in \Delta t} P_\tau^2 \mathbf{1}[O_\tau^{M,B}, P_\tau > 0])", OP(Moment, px2_maker_bid, Log, None))  \
  X(px2_maker_ask, CAT1, AUTO, "Maker Ask Price^2 Sum", "卖委托价平方和", "分钟内卖方新增限价委托的委托价平方和(元²)", R"(\sum_{\tau \in \Delta t} P_\tau^2 \mathbf{1}[O_\tau^{M,A}, P_\tau > 0])", OP(Moment, px2_maker_ask, Log, None))
