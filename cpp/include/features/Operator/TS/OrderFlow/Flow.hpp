#pragma once

// =============================================================================
// Flow - 分钟增量流量 + 分钟均价 (compute=onTick, flush=onMinute; feature_list.md 1.2)
// =============================================================================
//   流量 (每分钟增量, 时段量 = 因子层按行求和; 主动方向 = 成交单 order_dir, 委托方向 = 挂单 / 撤单 order_dir):
//     {amt,vol,n,dlogp}_taker_{bid,ask}  主动买 / 主动卖 成交 额 / 量 / 笔 / Σ 1e4·Δln(成交价) (基点, 复用 TakerRet;
//                                        TradeSize 的 *_ge_{B|q} 是本口的 "≥ 阈值" 子集, 嵌套集的全体在这里)
//     {amt,vol,n}_maker_{bid,ask}        新增委托 额 / 量 / 笔 (price=0 的市价单只计笔数与量)
//     {amt,vol,n}_cancel_{bid,ask}       撤单 额 / 量 / 笔 (额 = 被撤委托所在档价 ord_price × 量: 深市撤单事件 price=0, 不能用事件价)
//     vol_maker_lim_{bid,ask}            以跌停价挂买 / 涨停价挂卖 的委托量 (彩票委托; 边界 = Fund 当日涨跌停, NaN → 0)
//     vol_first30s                       分钟前 30 秒成交量 (后 30 秒 = vol_taker_bid + ask − 本口, 不单列)
//   均价 (元, 不产 NaN):
//     vwap  Σamt_taker / Σvol_taker (无成交沿用上一有效值, 首日无历史取当日涨跌停中值)
//     twap  成交价时间加权 (末笔持有到分钟末; 无成交 → 退 vwap)
//   不单列 (因子层做): 总额 / 量 / 笔 = 买 + 卖 按行求和; 开盘 / 收盘集合竞价成交 = 09:25 行 (l1=10) / 末行 (l1=254) 的 taker 口;
//   K 线见 Basic/Bar.hpp.
//   fp16 落盘: 额 / 量 / 笔 用 Log Tf; 价 / 基点 原值.
//   只接 Fund.y[Fund.lim_up] / Fund.y[Fund.lim_dn] 两口 (非 Fund.out()): 委托 9:15 就开始, Fund 的 Series 要到首个分钟 flush 才有当日值 (与 Depth 同法).
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"

class Flow {
  static constexpr size_t NG = 10; // 累加组: taker {amt, vol, n, dlogp} + maker {amt, vol, n} + cancel {amt, vol, n}

public:
  // 布局: 前 NG 组 × 2 侧 与 acc_ 一致 —— y[amt_taker_bid + g·2 + s]
  enum Out : size_t {
    amt_taker_bid,
    amt_taker_ask,
    vol_taker_bid,
    vol_taker_ask,
    n_taker_bid,
    n_taker_ask,
    dlogp_taker_bid,
    dlogp_taker_ask,
    amt_maker_bid,
    amt_maker_ask,
    vol_maker_bid,
    vol_maker_ask,
    n_maker_bid,
    n_maker_ask,
    amt_cancel_bid,
    amt_cancel_ask,
    vol_cancel_bid,
    vol_cancel_ask,
    n_cancel_bid,
    n_cancel_ask,
    vol_maker_lim_bid,
    vol_maker_lim_ask,
    vol_first30s,
    vwap,
    twap,
    kCount
  };
  static_assert(vol_maker_lim_bid == NG * 2);
  float y[kCount] = {};

  Flow(const TickData &td, const MinuteData &md, const Series &taker_dlogp, const float &lim_up, const float &lim_dn)
      : td_(td), md_(md), taker_dlogp_(taker_dlogp), lim_up_(lim_up), lim_dn_(lim_dn) {}

  inline void compute() {
    const auto &lob = td_.lob;
    const float v = static_cast<float>(lob.volume);
    const size_t s = lob.order_dir == L2::OrderDirection::BID ? 0 : 1;

    switch (lob.order_type) {
    case L2::OrderType::TAKER: {
      const float p = lob.price;
      acc_[0][s] += p * v, acc_[1][s] += v, acc_[2][s] += 1.0f;
      acc_[3][s] += taker_dlogp_.back(); // TakerRet 同 tick 的 onTaker 域已 flush; 无效笔 = 0, 加零无影响
      if (td_.l0_index % 60 < 30)
        vol_30a_ += v;
      if (p > 0.0f) { // 时间加权: 上一有效成交价持有到本笔 (哨兵秒内 ms 回绕 → 钳零, 同 Book)
        const uint32_t t = tick_ms(td_);
        if (px_last_ > 0.0f)
          twap_acc_ += px_last_ * static_cast<float>(t > t_last_ ? t - t_last_ : 0u);
        else
          t_start_ = t; // 当日首笔: 时间加权从这里起算
        px_last_ = p, t_last_ = t;
      }
      break;
    }
    case L2::OrderType::MAKER: {
      acc_[4][s] += lob.price * v, acc_[5][s] += v, acc_[6][s] += 1.0f; // price=0 (市价单) → 额 0
      // 彩票委托: 买单挂跌停价 / 卖单挂涨停价 (边界 NaN → 比较恒 false → 不计)
      if (s == 0) {
        if (lob.price <= lim_dn_ + kPxEps && lob.price > 0.0f)
          vol_lim_[0] += v;
      } else if (lob.price >= lim_up_ - kPxEps) {
        vol_lim_[1] += v;
      }
      break;
    }
    case L2::OrderType::CANCEL:
      acc_[7][s] += lob.ord_price[s] * v, acc_[8][s] += v, acc_[9][s] += 1.0f; // 额用被撤委托的档价 (占位单无价 → 0)
      break;
    default:
      break;
    }
  }

  inline void flush() {
    for (size_t g = 0; g < NG; ++g)
      for (size_t s = 0; s < 2; ++s)
        y[amt_taker_bid + g * 2 + s] = acc_[g][s];
    y[vol_maker_lim_bid] = vol_lim_[0];
    y[vol_maker_lim_ask] = vol_lim_[1];
    y[vol_first30s] = vol_30a_;

    // 价列不留 NaN: 无成交则沿用上一有效 VWAP (跨日保留 ≈ 上日收盘价), 首日无历史退当日涨跌停中值
    const float amt_total = acc_[0][0] + acc_[0][1];
    const float vol_total = acc_[1][0] + acc_[1][1];
    if (vol_total > 0.0f)
      vwap_last_ = amt_total / vol_total;
    else if (vwap_last_ <= 0.0f && lim_up_ > 0.0f)
      vwap_last_ = (lim_up_ + lim_dn_) * 0.5f;
    y[vwap] = vwap_last_;

    // twap: 末笔持有到分钟末; 当日尚无成交 (span 0) → 退 vwap (同源同刻, 不产 NaN)
    const uint32_t t_end = (static_cast<uint32_t>(L1_to_L0(md_.l1_index)) + 60u) * 1000u;
    twap_acc_ += px_last_ * static_cast<float>(t_end > t_last_ ? t_end - t_last_ : 0u);
    const float span = static_cast<float>(t_end > t_start_ ? t_end - t_start_ : 0u);
    y[twap] = span > 0.0f && px_last_ > 0.0f ? twap_acc_ / span : vwap_last_;
    twap_acc_ = 0.0f;
    t_start_ = t_last_ = t_end;

    clear();
  }

  void reset() { // vwap_last_ 不清: 跨日保留, 供开盘首个无成交分钟兜底
    clear();
    px_last_ = twap_acc_ = 0.0f;
    t_start_ = t_last_ = 0;
  }

private:
  void clear() {
    for (auto &g : acc_)
      g[0] = g[1] = 0.0f;
    vol_30a_ = 0.0f;
    vol_lim_[0] = vol_lim_[1] = 0.0f;
  }

  const TickData &td_;
  const MinuteData &md_;
  const Series &taker_dlogp_;     // TakerRet 输出口 (onTaker 先于 onTick, back() 即本笔值)
  const float &lim_up_, &lim_dn_; // Fund 节点当日涨 / 跌停价 (元), 引用 y[] 槽位

  float acc_[NG][2] = {}; // [组][侧]: 组序 = enum 前 NG 组; 侧 = {bid, ask}
  float vol_30a_ = 0.0f;
  float vol_lim_[2] = {};
  float vwap_last_ = 0.0f; // 上一有效 VWAP, 跨日保留 (无成交分钟兜底用)
  float px_last_ = 0.0f, twap_acc_ = 0.0f;
  uint32_t t_start_ = 0, t_last_ = 0;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Flow(N) N(Flow, (Flow), (tick_data, minute_data, TakerRet.out(), Fund.y[Fund.lim_up], Fund.y[Fund.lim_dn]), onTick, onMinute)

// 一个事件类型 e (taker/maker/cancel) 的 额/量/笔 × 买/卖 6 行; E = 公式事件上标, EN / CN = 英 / 中文事件名 (字面串)
#define FLOW_EVENT_ROWS(X, CAT1, e, E, EN, CN)                                                                                                                                             \
  X(amt_##e##_bid, CAT1, AUTO, EN " Bid Amount", "买方" CN "额", "分钟内买方" CN "额(元)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^{)" E R"(,B}|)", OP(Flow, amt_##e##_bid, Log, None)) \
  X(amt_##e##_ask, CAT1, AUTO, EN " Ask Amount", "卖方" CN "额", "分钟内卖方" CN "额(元)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^{)" E R"(,A}|)", OP(Flow, amt_##e##_ask, Log, None)) \
  X(vol_##e##_bid, CAT1, AUTO, EN " Bid Volume", "买方" CN "量", "分钟内买方" CN "量(股)", R"(\sum_{\tau \in \Delta t} |O_\tau^{)" E R"(,B}|)", OP(Flow, vol_##e##_bid, Log, None))        \
  X(vol_##e##_ask, CAT1, AUTO, EN " Ask Volume", "卖方" CN "量", "分钟内卖方" CN "量(股)", R"(\sum_{\tau \in \Delta t} |O_\tau^{)" E R"(,A}|)", OP(Flow, vol_##e##_ask, Log, None))        \
  X(n_##e##_bid, CAT1, AUTO, EN " Bid Count", "买方" CN "笔数", "分钟内买方" CN "笔数", R"(\#O_{\Delta t}^{)" E R"(,B})", OP(Flow, n_##e##_bid, Log, None))                                \
  X(n_##e##_ask, CAT1, AUTO, EN " Ask Count", "卖方" CN "笔数", "分钟内卖方" CN "笔数", R"(\#O_{\Delta t}^{)" E R"(,A})", OP(Flow, n_##e##_ask, Log, None))

// taker 独有的第 4 项: 主动买 / 主动卖 成交的 Σ 对数价变 (基点; 相对上一笔任意有效成交)
#define FLOW_TAKER_DLOGP_ROWS(X, CAT1)                                                                                                                                                                                                                                      \
  X(dlogp_taker_bid, CAT1, AUTO, "Taker Bid Log-Price Change", "主动买成交价变动和", "分钟内主动买成交对上一笔成交价的对数变动之和(基点)", R"(\sum_{\tau \in \Delta t} 10^4 \ln\frac{P_\tau}{P_{\tau-1}} \mathbf{1}[O_\tau^{T,B}])", OP(Flow, dlogp_taker_bid, None, None)) \
  X(dlogp_taker_ask, CAT1, AUTO, "Taker Ask Log-Price Change", "主动卖成交价变动和", "分钟内主动卖成交对上一笔成交价的对数变动之和(基点)", R"(\sum_{\tau \in \Delta t} 10^4 \ln\frac{P_\tau}{P_{\tau-1}} \mathbf{1}[O_\tau^{T,A}])", OP(Flow, dlogp_taker_ask, None, None))

#define FIELDS_L1_Flow(X, CAT1)                                                                                                                                                                                                                                          \
  FLOW_EVENT_ROWS(X, CAT1, taker, "T", "Taker", "主动成交")                                                                                                                                                                                                              \
  FLOW_TAKER_DLOGP_ROWS(X, CAT1)                                                                                                                                                                                                                                         \
  FLOW_EVENT_ROWS(X, CAT1, maker, "M", "Maker", "新增委托")                                                                                                                                                                                                              \
  FLOW_EVENT_ROWS(X, CAT1, cancel, "C", "Cancel", "撤单")                                                                                                                                                                                                                \
  X(vol_maker_lim_bid, CAT1, AUTO, "Limit-Down Bid Order Volume", "跌停价挂买量", "分钟内以当日跌停价挂出的买委托量(股, 彩票委托)", R"(\sum_{\tau \in \Delta t} |O_\tau^{M,B}| \mathbf{1}[P_\tau \leq P^{dn}_D + \varepsilon])", OP(Flow, vol_maker_lim_bid, Log, None)) \
  X(vol_maker_lim_ask, CAT1, AUTO, "Limit-Up Ask Order Volume", "涨停价挂卖量", "分钟内以当日涨停价挂出的卖委托量(股)", R"(\sum_{\tau \in \Delta t} |O_\tau^{M,A}| \mathbf{1}[P_\tau \geq P^{up}_D - \varepsilon])", OP(Flow, vol_maker_lim_ask, Log, None))             \
  X(vol_first30s, CAT1, AUTO, "Volume First 30s", "前30秒成交量", "分钟前半段(0-29s)成交量(股); 后半段=vol_taker_bid+ask−本列", R"(\sum_{\tau \in [0,30s)} |O_\tau^T|)", OP(Flow, vol_first30s, Log, None))                                                              \
  X(vwap, CAT1, AUTO, "VWAP", "成交量加权均价", "分钟成交额/成交量(元; 无成交沿用上一有效值)", R"(\frac{\sum_{\tau \in \Delta t} P_\tau |O_\tau^T|}{\sum_{\tau \in \Delta t} |O_\tau^T|})", OP(Flow, vwap, None, None))                                                  \
  X(twap, CAT1, AUTO, "TWAP", "时间加权均价", "分钟内成交价时间加权均值(元, 末笔持有到分钟末; 当日尚无成交→vwap)", R"(\frac{\sum_\tau P_\tau (t_{\tau+1}-t_\tau)}{\sum_\tau (t_{\tau+1}-t_\tau)})", OP(Flow, twap, None, None))
