#pragma once

// =============================================================================
// OrderLife - 订单生命周期 (compute=onTick, flush=onMinute; feature_list.md 1.5 前三行)
// =============================================================================
//   委托存续时长 age = 事件时刻 − 挂单时刻 (ms; 两者同为 LOB 的 h/m/s/ms10 编码, 午休不扣 —— 跨午休的单必 ≥ 300s, 不影响任何桶).
//   输入面 = LOB_Feature::ord_* (语义见 LimitOrderBookDefine.hpp), 只用可信的在簿委托: Resting 且 ord_flag ∉ {OUT_OF_ORDER, ZERO_PRICE}
//   (占位单的 ord_tick 是首笔成交/撤单时刻而非挂单时刻).
//     {amt,n}_fill_{bid,ask}_lt_τ     被动方委托 (被吃的那张, 侧 = 它的方向) 挂单→本笔成交 用时 < τ 的成交 额(元) / 笔.
//                                     只算被动方: 主动方在沪市不入簿无挂单时刻, 深市则恒 ≈ 0 —— 两所不可比, 它归 n_marketable.
//     {vol,n}_cancel_{bid,ask}_lt_τ   撤单委托 挂单→撤单 用时 < τ 的撤单 量(股) / 笔 (τ=1s 即 开源 029 高频撤单率 分子).
//     n_marketable_{bid,ask}          到达即成交的委托笔数 = 主动方首笔成交 (此前累计成交 f = 0: Aggressor ord_rest==0 / 深市 Resting ord_orig==ord_rest);
//                                     一张主动单扫 k 档只计 1 (开源 029 广义市价比例 分子; 分母 = Flow n_maker_*).
//   τ ∈ {1s, 10s, 60s, 300s}, "< τ" 为嵌套集 (分档 = 相邻差, 因子层做); 每笔只落一个桶 (首个 age < τ 的下标, 都不满足 = 4), flush 前缀和.
//   fp16 落盘: 全部 Log Tf.
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include <cstdint>

#define ORDERLIFE_TAUS(T) T(1s) T(10s) T(60s) T(300s)
#define ORDERLIFE_ENUM_FILL(t) amt_fill_bid_lt_##t, n_fill_bid_lt_##t, amt_fill_ask_lt_##t, n_fill_ask_lt_##t,
#define ORDERLIFE_ENUM_CANCEL(t) vol_cancel_bid_lt_##t, n_cancel_bid_lt_##t, vol_cancel_ask_lt_##t, n_cancel_ask_lt_##t,

class OrderLife {
  static constexpr size_t NT = 4;
  static constexpr uint32_t TAU_MS[NT] = {1000u, 10000u, 60000u, 300000u}; // 升序

public:
  // 布局: 事件 (fill, cancel) 外层 × τ 中层 × (bid {x, n}, ask {x, n}) 内层 —— y[(e·NT + k)·4 + s·2 + d]; 其后 n_marketable ×2
  enum Out : size_t { ORDERLIFE_TAUS(ORDERLIFE_ENUM_FILL) ORDERLIFE_TAUS(ORDERLIFE_ENUM_CANCEL) n_marketable_bid,
                      n_marketable_ask,
                      kCount };
  static_assert(n_marketable_bid == 2 * NT * 4 && kCount == 2 * NT * 4 + 2);
  float y[kCount] = {};

  explicit OrderLife(const TickData &td) : td_(td) {}

  inline void compute() {
    const auto &lob = td_.lob;
    if (lob.order_type == L2::OrderType::MAKER)
      return;
    const float v = static_cast<float>(lob.volume);
    const size_t act = lob.order_dir == L2::OrderDirection::BID ? 0 : 1; // TAKER: 主动方侧; CANCEL: 被撤委托侧
    const uint32_t now = ms_of(lob.hour, lob.minute, lob.second, lob.millisecond);

    if (lob.order_type == L2::OrderType::TAKER) {
      const size_t pas = 1 - act;
      if (reliable(lob, pas)) {
        float (&bk)[2] = fill_[age_bucket(now, lob.ord_tick[pas])][pas];
        bk[0] += lob.price * v, bk[1] += 1.0f;
      }
      switch (lob.ord_role[act]) { // 主动方首笔成交 → 一张到达即成交的委托
      case OrderRole::Aggressor:
        if (lob.ord_rest[act] == 0)
          mkt_[act] += 1.0f;
        break;
      case OrderRole::Resting:
        if (reliable(lob, act) && lob.ord_rest[act] == lob.ord_orig[act])
          mkt_[act] += 1.0f;
        break;
      default:
        break;
      }
      return;
    }

    if (lob.order_type == L2::OrderType::CANCEL && reliable(lob, act)) {
      float (&bk)[2] = cancel_[age_bucket(now, lob.ord_tick[act])][act];
      bk[0] += v, bk[1] += 1.0f;
    }
  }

  // "< τ_k" ⇔ 桶 ≤ k: 前缀和写到 τ_k 的槽
  inline void flush() {
    for (size_t s = 0; s < 2; ++s) {
      float rf[2] = {0.0f, 0.0f}, rc[2] = {0.0f, 0.0f};
      for (size_t k = 0; k < NT; ++k) {
        rf[0] += fill_[k][s][0], rf[1] += fill_[k][s][1];
        rc[0] += cancel_[k][s][0], rc[1] += cancel_[k][s][1];
        y[(0 * NT + k) * 4 + s * 2] = rf[0], y[(0 * NT + k) * 4 + s * 2 + 1] = rf[1];
        y[(1 * NT + k) * 4 + s * 2] = rc[0], y[(1 * NT + k) * 4 + s * 2 + 1] = rc[1];
      }
    }
    y[n_marketable_bid] = mkt_[0];
    y[n_marketable_ask] = mkt_[1];
    clear();
  }

  void reset() { clear(); }

private:
  static inline uint32_t ms_of(uint32_t h, uint32_t m, uint32_t s, uint32_t ms10) { return h * 3600000u + m * 60000u + s * 1000u + ms10 * 10u; }

  // 在簿且非占位单: ord_orig / ord_tick 可信
  static inline bool reliable(const LOB_Feature &lob, size_t s) {
    return lob.ord_role[s] == OrderRole::Resting && lob.ord_flag[s] != OrderFlags::OUT_OF_ORDER && lob.ord_flag[s] != OrderFlags::ZERO_PRICE;
  }

  // 首个 age < τ_k 的 k; 都不满足 = NT. 挂单时刻晚于事件时刻 (乱序修正后的迟到 MAKER 时间戳) → age 钳 0
  static inline size_t age_bucket(uint32_t now, uint32_t tick) {
    const uint32_t t0 = ms_of(tick >> 24, (tick >> 16) & 0xFFu, (tick >> 8) & 0xFFu, tick & 0xFFu);
    const uint32_t age = now > t0 ? now - t0 : 0u;
    size_t k = 0;
    while (k < NT && age >= TAU_MS[k])
      ++k;
    return k;
  }

  void clear() {
    for (auto &row : fill_)
      row[0][0] = row[0][1] = row[1][0] = row[1][1] = 0.0f;
    for (auto &row : cancel_)
      row[0][0] = row[0][1] = row[1][0] = row[1][1] = 0.0f;
    mkt_[0] = mkt_[1] = 0.0f;
  }

  const TickData &td_;
  float fill_[NT + 1][2][2] = {};   // [桶][侧][amt, n]
  float cancel_[NT + 1][2][2] = {}; // [桶][侧][vol, n]
  float mkt_[2] = {};
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_OrderLife(N) N(OrderLife, (OrderLife), (tick_data), onTick, onMinute)

// 一 τ (t token; TE / TF = 英文 / 公式字面) 的 成交 4 + 撤单 4 = 8 行
#define ORDERLIFE_TAU_ROWS(X, CAT1, t, TE, TF)                                                                                                                                                                                                                                                                       \
  X(amt_fill_bid_lt_##t, CAT1, AUTO, "Bid Order Fill Amount (age < " TE ")", "买委托快速成交额(<" TE ")", "分钟内被吃的买方委托 挂单→成交 用时<" TE " 的成交额(元)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^{T}| \mathbf{1}[\mathrm{age}^{B}_\tau < )" TF R"(])", OP(OrderLife, amt_fill_bid_lt_##t, Log, None)) \
  X(n_fill_bid_lt_##t, CAT1, AUTO, "Bid Order Fill Count (age < " TE ")", "买委托快速成交笔数(<" TE ")", "分钟内被吃的买方委托 挂单→成交 用时<" TE " 的成交笔数", R"(\#O_{\Delta t}^{T} \mathbf{1}[\mathrm{age}^{B}_\tau < )" TF R"(])", OP(OrderLife, n_fill_bid_lt_##t, Log, None))                                \
  X(amt_fill_ask_lt_##t, CAT1, AUTO, "Ask Order Fill Amount (age < " TE ")", "卖委托快速成交额(<" TE ")", "分钟内被吃的卖方委托 挂单→成交 用时<" TE " 的成交额(元)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^{T}| \mathbf{1}[\mathrm{age}^{A}_\tau < )" TF R"(])", OP(OrderLife, amt_fill_ask_lt_##t, Log, None)) \
  X(n_fill_ask_lt_##t, CAT1, AUTO, "Ask Order Fill Count (age < " TE ")", "卖委托快速成交笔数(<" TE ")", "分钟内被吃的卖方委托 挂单→成交 用时<" TE " 的成交笔数", R"(\#O_{\Delta t}^{T} \mathbf{1}[\mathrm{age}^{A}_\tau < )" TF R"(])", OP(OrderLife, n_fill_ask_lt_##t, Log, None))                                \
  X(vol_cancel_bid_lt_##t, CAT1, AUTO, "Bid Cancel Volume (age < " TE ")", "买委托快速撤单量(<" TE ")", "分钟内 挂单→撤单 用时<" TE " 的买方撤单量(股)", R"(\sum_{\tau \in \Delta t} |O_\tau^{C,B}| \mathbf{1}[\mathrm{age}_\tau < )" TF R"(])", OP(OrderLife, vol_cancel_bid_lt_##t, Log, None))                    \
  X(n_cancel_bid_lt_##t, CAT1, AUTO, "Bid Cancel Count (age < " TE ")", "买委托快速撤单笔数(<" TE ")", "分钟内 挂单→撤单 用时<" TE " 的买方撤单笔数", R"(\#O_{\Delta t}^{C,B} \mathbf{1}[\mathrm{age}_\tau < )" TF R"(])", OP(OrderLife, n_cancel_bid_lt_##t, Log, None))                                            \
  X(vol_cancel_ask_lt_##t, CAT1, AUTO, "Ask Cancel Volume (age < " TE ")", "卖委托快速撤单量(<" TE ")", "分钟内 挂单→撤单 用时<" TE " 的卖方撤单量(股)", R"(\sum_{\tau \in \Delta t} |O_\tau^{C,A}| \mathbf{1}[\mathrm{age}_\tau < )" TF R"(])", OP(OrderLife, vol_cancel_ask_lt_##t, Log, None))                    \
  X(n_cancel_ask_lt_##t, CAT1, AUTO, "Ask Cancel Count (age < " TE ")", "卖委托快速撤单笔数(<" TE ")", "分钟内 挂单→撤单 用时<" TE " 的卖方撤单笔数", R"(\#O_{\Delta t}^{C,A} \mathbf{1}[\mathrm{age}_\tau < )" TF R"(])", OP(OrderLife, n_cancel_ask_lt_##t, Log, None))

#define FIELDS_L1_OrderLife(X, CAT1)                                                                                                                                                                                                                                                  \
  ORDERLIFE_TAU_ROWS(X, CAT1, 1s, "1s", R"(1\mathrm{s})")                                                                                                                                                                                                                             \
  ORDERLIFE_TAU_ROWS(X, CAT1, 10s, "10s", R"(10\mathrm{s})")                                                                                                                                                                                                                          \
  ORDERLIFE_TAU_ROWS(X, CAT1, 60s, "60s", R"(60\mathrm{s})")                                                                                                                                                                                                                          \
  ORDERLIFE_TAU_ROWS(X, CAT1, 300s, "300s", R"(300\mathrm{s})")                                                                                                                                                                                                                       \
  X(n_marketable_bid, CAT1, AUTO, "Marketable Bid Order Count", "到达即成交买委托笔数", "分钟内首笔成交即为主动买的委托张数(一张扫多档只计1; 广义市价单)", R"(\#\{o^{B} \mid o \text{ 首笔成交于 } \Delta t \land o \text{ 为主动方}\})", OP(OrderLife, n_marketable_bid, Log, None)) \
  X(n_marketable_ask, CAT1, AUTO, "Marketable Ask Order Count", "到达即成交卖委托笔数", "分钟内首笔成交即为主动卖的委托张数(一张扫多档只计1; 广义市价单)", R"(\#\{o^{A} \mid o \text{ 首笔成交于 } \Delta t \land o \text{ 为主动方}\})", OP(OrderLife, n_marketable_ask, Log, None))
