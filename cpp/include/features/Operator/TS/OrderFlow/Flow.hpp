#pragma once

// =============================================================================
// Flow - 分钟增量流量 + K 线派生 + 竞价成交 (compute=onTick, flush=onMinute; feature_list.md 1.1 / 1.2 / 1.7)
// =============================================================================
//   K 线派生 (读 MinuteData 的本分钟 bar):
//     amt = Σ 成交额 (元)    n_trade = 成交笔数    vwap = amt / vol (元)
//     ret = 1e4·ln(C_t / C_{t-1}) (基点, 日内首分钟 NaN)   range = 1e4·ln(H/L)   body = 1e4·ln(C/O)
//   流量 (每分钟增量, 时段量 = 因子层按行求和; 主动方向 = 成交单 order_dir, 委托方向 = 挂单 / 撤单 order_dir):
//     {amt,vol,n}_taker_{bid,ask}   主动买 / 主动卖 成交 额 / 量 / 笔
//     {amt,vol,n}_maker_{bid,ask}   新增委托 额 / 量 / 笔 (price=0 的市价单只计笔数与量)
//     {amt,vol,n}_cancel_{bid,ask}  撤单 额 / 量 / 笔
//     vol_lim_{bid,ask}             以跌停价挂买 / 涨停价挂卖 的委托量 (彩票委托; 边界 = Fund 当日涨跌停, NaN → 0)
//     vol_30s_a / vol_30s_b         分钟前 / 后 30 秒成交量
//   竞价 (全日常量, 定格后每分钟广播):
//     vol/amt_call_open   09:25 开盘集合竞价撮合成交 (OPENING_MATCHING_PERIOD 内的成交单)
//     vol/amt_call_close  14:57-15:00 收盘集合竞价成交 (只在末行非零)
//   fp16 落盘: 额 / 量 / 笔 用 Log Tf; 价 / 基点收益原值.
//   读 Fund.y (非 Fund.out()): 盘口 / 委托 9:15 就开始, Fund 的 Series 要到首个分钟 flush 才有当日值 (与 Depth 同法).
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "features/Operator/TS/Fund/Fund.hpp" // Fund::Out 口下标
#include <cmath>

class Flow {
  static constexpr float kBp = 1e4f;
  static constexpr float LIM_EPS = 0.005f; // 涨跌停价比较容差 (半个最小价位)

public:
  enum Out : size_t {
    amt,
    n_trade,
    vwap,
    ret,
    range,
    body,
    amt_taker_bid,
    amt_taker_ask,
    vol_taker_bid,
    vol_taker_ask,
    n_taker_bid,
    n_taker_ask,
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
    vol_lim_bid,
    vol_lim_ask,
    vol_30s_a,
    vol_30s_b,
    vol_call_open,
    amt_call_open,
    vol_call_close,
    amt_call_close,
    kCount
  };
  float y[kCount] = {};

  Flow(const TickData &td, const MinuteData &md, const float (&fund)[Fund::kCount]) : td_(td), md_(md), fund_(fund) {}

  inline void compute() {
    const auto &lob = td_.lob;
    const float v = static_cast<float>(lob.volume);
    const float a = lob.price * v; // price=0 (市价单 / 深交所零价撤单) → 额 0
    const size_t s = lob.order_dir == L2::OrderDirection::BID ? 0 : 1;

    switch (lob.order_type) {
    case L2::OrderType::TAKER: {
      acc_[0][s] += a, acc_[1][s] += v, acc_[2][s] += 1.0f;
      n_trade_ += 1.0f;
      if (td_.l0_index % 60 < 30)
        vol_30a_ += v;
      else
        vol_30b_ += v;
      const auto st = lob.market_state;
      if (st == L2::MarketState::OPENING_MATCHING_PERIOD || st == L2::MarketState::OPENING_CALL_AUCTION) [[unlikely]]
        call_open_v_ += v, call_open_a_ += a;
      else if (st == L2::MarketState::CLOSING_CALL_AUCTION || st == L2::MarketState::CLOSING_MATCHING_PERIOD) [[unlikely]]
        call_close_v_ += v, call_close_a_ += a;
      break;
    }
    case L2::OrderType::MAKER: {
      acc_[3][s] += a, acc_[4][s] += v, acc_[5][s] += 1.0f;
      // 彩票委托: 买单挂跌停价 / 卖单挂涨停价 (边界 NaN → 比较恒 false → 不计)
      if (s == 0) {
        if (lob.price <= fund_[Fund::dn_lim] + LIM_EPS && lob.price > 0.0f)
          vol_lim_[0] += v;
      } else if (lob.price >= fund_[Fund::up_lim] - LIM_EPS) {
        vol_lim_[1] += v;
      }
      break;
    }
    case L2::OrderType::CANCEL:
      acc_[6][s] += a, acc_[7][s] += v, acc_[8][s] += 1.0f;
      break;
    default:
      break;
    }
  }

  inline void flush() {
    const float amt_total = acc_[0][0] + acc_[0][1];
    const float vol_total = acc_[1][0] + acc_[1][1];
    y[amt] = amt_total;
    y[n_trade] = n_trade_;
    y[vwap] = vol_total > 0.0f ? amt_total / vol_total : kNaN;

    const float o = md_.open.back(), h = md_.high.back(), l = md_.low.back(), c = md_.close.back();
    y[ret] = prev_close_ > 0.0f ? kBp * std::log(c / prev_close_) : kNaN;
    y[range] = kBp * std::log(h / l);
    y[body] = kBp * std::log(c / o);
    prev_close_ = c;

    // 9 组 × 2 侧 与 enum 布局一致: amt_taker_bid 起连续 18 口 = acc_[g][s]
    for (size_t g = 0; g < 9; ++g)
      for (size_t s = 0; s < 2; ++s)
        y[amt_taker_bid + g * 2 + s] = acc_[g][s];

    y[vol_lim_bid] = vol_lim_[0];
    y[vol_lim_ask] = vol_lim_[1];
    y[vol_30s_a] = vol_30a_;
    y[vol_30s_b] = vol_30b_;
    y[vol_call_open] = call_open_v_;
    y[amt_call_open] = call_open_a_;
    y[vol_call_close] = call_close_v_;
    y[amt_call_close] = call_close_a_;

    for (auto &g : acc_)
      g[0] = g[1] = 0.0f;
    n_trade_ = vol_30a_ = vol_30b_ = 0.0f;
    vol_lim_[0] = vol_lim_[1] = 0.0f;
  }

  void reset() {
    for (auto &g : acc_)
      g[0] = g[1] = 0.0f;
    n_trade_ = vol_30a_ = vol_30b_ = 0.0f;
    vol_lim_[0] = vol_lim_[1] = 0.0f;
    call_open_v_ = call_open_a_ = call_close_v_ = call_close_a_ = 0.0f;
    prev_close_ = 0.0f;
  }

private:
  const TickData &td_;
  const MinuteData &md_;
  const float (&fund_)[Fund::kCount];

  // [组][侧]: 组 = {taker amt, vol, n, maker amt, vol, n, cancel amt, vol, n}; 侧 = {bid, ask}
  float acc_[9][2] = {};
  float n_trade_ = 0.0f;
  float vol_30a_ = 0.0f, vol_30b_ = 0.0f;
  float vol_lim_[2] = {};
  float call_open_v_ = 0.0f, call_open_a_ = 0.0f, call_close_v_ = 0.0f, call_close_a_ = 0.0f;
  float prev_close_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Flow(N) N(Flow, (Flow), (tick_data, minute_data, Fund.y), onTick, onMinute)

// 一个事件类型 e (taker/maker/cancel) 的 额/量/笔 × 买/卖 6 行; E = 公式事件上标, EN / CN = 英 / 中文事件名 (字面串)
#define FLOW_EVENT_ROWS(X, CAT1, e, E, EN, CN)                                                                                                                          \
  X(amt_##e##_bid, CAT1, RAW, EN " Bid Amount", "买方" CN "额", "分钟内买方" CN "额(元)", R"(\sum_{\Delta t} P\,|O^{)" E R"(,B}|)", OP(Flow, amt_##e##_bid, Log, None)) \
  X(amt_##e##_ask, CAT1, RAW, EN " Ask Amount", "卖方" CN "额", "分钟内卖方" CN "额(元)", R"(\sum_{\Delta t} P\,|O^{)" E R"(,A}|)", OP(Flow, amt_##e##_ask, Log, None)) \
  X(vol_##e##_bid, CAT1, RAW, EN " Bid Volume", "买方" CN "量", "分钟内买方" CN "量(股)", R"(\sum_{\Delta t} |O^{)" E R"(,B}|)", OP(Flow, vol_##e##_bid, Log, None))    \
  X(vol_##e##_ask, CAT1, RAW, EN " Ask Volume", "卖方" CN "量", "分钟内卖方" CN "量(股)", R"(\sum_{\Delta t} |O^{)" E R"(,A}|)", OP(Flow, vol_##e##_ask, Log, None))    \
  X(n_##e##_bid, CAT1, RAW, EN " Bid Count", "买方" CN "笔数", "分钟内买方" CN "笔数", R"(\#O_{\Delta t}^{)" E R"(,B})", OP(Flow, n_##e##_bid, Log, None))              \
  X(n_##e##_ask, CAT1, RAW, EN " Ask Count", "卖方" CN "笔数", "分钟内卖方" CN "笔数", R"(\#O_{\Delta t}^{)" E R"(,A})", OP(Flow, n_##e##_ask, Log, None))

#define FIELDS_L1_Flow(X, CAT1)                                                                                                                                                                                         \
  X(amt, CAT1, RAW, "Amount", "成交额", "分钟成交额(元)", R"(\sum_{\Delta t} P\,|O^{T}|)", OP(Flow, amt, Log, None))                                                                                                    \
  X(n_trade, CAT1, RAW, "Trade Count", "成交笔数", "分钟成交笔数; 单笔金额=amt/n_trade", R"(\#O_{\Delta t}^{T})", OP(Flow, n_trade, Log, None))                                                                         \
  X(vwap, CAT1, RAW, "VWAP", "成交量加权均价", "分钟成交额/成交量(元)", R"(\frac{\sum P\,|O^T|}{\sum |O^T|})", OP(Flow, vwap, None, None))                                                                              \
  X(ret, CAT1, RAW, "Minute Log Return", "分钟对数收益", "收盘价对上一有效分钟收盘价的对数收益(基点; 日内首分钟NaN)", R"(10^4 \ln\frac{C_t}{C_{t-1}})", OP(Flow, ret, None, None))                                      \
  X(range, CAT1, RAW, "Minute Log Range", "分钟对数振幅", "分钟最高/最低对数比(基点)", R"(10^4 \ln\frac{H_t}{L_t})", OP(Flow, range, None, None))                                                                       \
  X(body, CAT1, RAW, "Minute Log Body", "分钟对数实体", "分钟收/开对数比(基点)", R"(10^4 \ln\frac{C_t}{O_t})", OP(Flow, body, None, None))                                                                              \
  FLOW_EVENT_ROWS(X, CAT1, taker, "T", "Taker", "主动成交")                                                                                                                                                             \
  FLOW_EVENT_ROWS(X, CAT1, maker, "M", "Maker", "新增委托")                                                                                                                                                             \
  FLOW_EVENT_ROWS(X, CAT1, cancel, "C", "Cancel", "撤单")                                                                                                                                                               \
  X(vol_lim_bid, CAT1, RAW, "Limit-Down Bid Order Volume", "跌停价挂买量", "分钟内以当日跌停价挂出的买委托量(股, 彩票委托)", R"(\sum_{\Delta t} |O^{M,B}| \mathbf{1}[P = P^{dn}_D])", OP(Flow, vol_lim_bid, Log, None)) \
  X(vol_lim_ask, CAT1, RAW, "Limit-Up Ask Order Volume", "涨停价挂卖量", "分钟内以当日涨停价挂出的卖委托量(股)", R"(\sum_{\Delta t} |O^{M,A}| \mathbf{1}[P = P^{up}_D])", OP(Flow, vol_lim_ask, Log, None))             \
  X(vol_30s_a, CAT1, RAW, "Volume First 30s", "前30秒成交量", "分钟前半段(0-29s)成交量(股)", R"(\sum_{\tau \in [0,30s)} |O_\tau^T|)", OP(Flow, vol_30s_a, Log, None))                                                   \
  X(vol_30s_b, CAT1, RAW, "Volume Last 30s", "后30秒成交量", "分钟后半段(30-59s)成交量(股)", R"(\sum_{\tau \in [30s,60s)} |O_\tau^T|)", OP(Flow, vol_30s_b, Log, None))                                                 \
  X(vol_call_open, CAT1, RAW, "Opening Call Volume", "开盘集合竞价成交量", "09:25撮合成交量(股), 之后全日常量", R"(|O^{T,\mathrm{call\_open}}|)", OP(Flow, vol_call_open, Log, None))                                   \
  X(amt_call_open, CAT1, RAW, "Opening Call Amount", "开盘集合竞价成交额", "09:25撮合成交额(元), 之后全日常量", R"(\sum P\,|O^{T,\mathrm{call\_open}}|)", OP(Flow, amt_call_open, Log, None))                           \
  X(vol_call_close, CAT1, RAW, "Closing Call Volume", "收盘集合竞价成交量", "14:57-15:00成交量(股), 只在末行非零", R"(|O^{T,\mathrm{call\_close}}|)", OP(Flow, vol_call_close, Log, None))                              \
  X(amt_call_close, CAT1, RAW, "Closing Call Amount", "收盘集合竞价成交额", "14:57-15:00成交额(元), 只在末行非零", R"(\sum P\,|O^{T,\mathrm{call\_close}}|)", OP(Flow, amt_call_close, Log, None))
