#pragma once

// =============================================================================
// OrderLife - 订单生命周期 (compute=onTick, flush=onMinute; feature_list.md 1.5 前三行)
// =============================================================================
//   委托存续时长 age = 事件时刻 − 挂单时刻 (ms; 两者同为 LOB 的 h/m/s/ms10 编码, 午休不扣 —— 跨午休的单必 ≥ 300s, 不影响任何桶).
//   输入面 = LOB_Feature::ord_* (语义见 LimitOrderBookDefine.hpp; 解码 ord_reliable 见 DataDefine.hpp), 只用可信的在簿委托
//   (占位单的 ord_tick 是首笔成交/撤单时刻而非挂单时刻).
//     {amt,vol,n}_fill_{bid,ask}_lt_τ    被动方委托 (被吃的那张, 侧 = 它的方向) 挂单→本笔成交 用时 < τ 的成交 额(元, 成交价) / 量(股) / 笔.
//                                        只算被动方: 主动方在沪市不入簿无挂单时刻, 深市则恒 ≈ 0 —— 两所不可比, 它归 n_marketable.
//     {amt,vol,n}_cancel_{bid,ask}_lt_τ  撤单委托 挂单→撤单 用时 < τ 的撤单 额(元, 委托档价 ord_price) / 量(股) / 笔 (τ=1s 即 开源 029 高频撤单率 分子).
//     n_marketable_{bid,ask}             到达即成交的委托笔数 = 主动方首笔成交 (此前累计成交 f = 0: Aggressor ord_rest==0 / 深市 Resting ord_orig==ord_rest);
//                                        一张主动单扫 k 档只计 1 (开源 029 广义市价比例 分子; 分母 = Flow n_maker_*).
//   τ ∈ {1s, 10s, 60s, 300s}, "< τ" 为嵌套集 (分档 = 相邻差, 因子层做; 全体 = Flow 的 taker / cancel 口); 每笔只落一个桶
//   (首个 age < τ 的下标, 都不满足 = 4), flush 前缀和.
//   fp16 落盘: 全部 Log Tf.
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include <cstdint>

#define ORDERLIFE_TAUS(T, e) T(e, 1s) T(e, 10s) T(e, 60s) T(e, 300s)
#define ORDERLIFE_ENUM(e, t) amt_##e##_bid_lt_##t, vol_##e##_bid_lt_##t, n_##e##_bid_lt_##t, amt_##e##_ask_lt_##t, vol_##e##_ask_lt_##t, n_##e##_ask_lt_##t,

class OrderLife {
  static constexpr size_t NT = 4, ND = 3;
  static constexpr uint32_t TAU_MS[NT] = {1000u, 10000u, 60000u, 300000u}; // 升序

public:
  // 布局: 事件 (fill, cancel) 外层 × τ 中层 × 侧 × {amt, vol, n} 内层 —— y[((e·NT + k)·2 + s)·ND + d]; 其后 n_marketable ×2
  enum Out : size_t { ORDERLIFE_TAUS(ORDERLIFE_ENUM, fill) ORDERLIFE_TAUS(ORDERLIFE_ENUM, cancel) n_marketable_bid,
                      n_marketable_ask,
                      kCount };
  static_assert(n_marketable_bid == 2 * NT * 2 * ND && kCount == 2 * NT * 2 * ND + 2);
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
      if (ord_reliable(lob, pas))
        hit(bucket_[0][age_bucket(now, lob.ord_tick[pas])][pas], lob.price, v);
      switch (lob.ord_role[act]) { // 主动方首笔成交 → 一张到达即成交的委托
      case OrderRole::Aggressor:
        if (lob.ord_rest[act] == 0)
          mkt_[act] += 1.0f;
        break;
      case OrderRole::Resting:
        if (ord_reliable(lob, act) && lob.ord_rest[act] == lob.ord_orig[act])
          mkt_[act] += 1.0f;
        break;
      default:
        break;
      }
      return;
    }

    if (lob.order_type == L2::OrderType::CANCEL && ord_reliable(lob, act))
      hit(bucket_[1][age_bucket(now, lob.ord_tick[act])][act], lob.ord_price[act], v);
  }

  // "< τ_k" ⇔ 桶 ≤ k: 前缀和写到 τ_k 的槽
  inline void flush() {
    for (size_t e = 0; e < 2; ++e)
      for (size_t s = 0; s < 2; ++s) {
        float run[ND] = {};
        for (size_t k = 0; k < NT; ++k) {
          float *o = &y[((e * NT + k) * 2 + s) * ND];
          for (size_t d = 0; d < ND; ++d)
            o[d] = run[d] += bucket_[e][k][s][d];
        }
      }
    y[n_marketable_bid] = mkt_[0];
    y[n_marketable_ask] = mkt_[1];
    clear();
  }

  void reset() { clear(); }

private:
  static inline uint32_t ms_of(uint32_t h, uint32_t m, uint32_t s, uint32_t ms10) { return h * 3600000u + m * 60000u + s * 1000u + ms10 * 10u; }
  static inline void hit(float (&bk)[ND], float p, float v) { bk[0] += p * v, bk[1] += v, bk[2] += 1.0f; }

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
    for (auto &e : bucket_)
      for (auto &row : e)
        for (auto &bk : row)
          bk[0] = bk[1] = bk[2] = 0.0f;
    mkt_[0] = mkt_[1] = 0.0f;
  }

  const TickData &td_;
  float bucket_[2][NT + 1][2][ND] = {}; // [事件 fill/cancel][桶][侧][amt, vol, n]
  float mkt_[2] = {};
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_OrderLife(N) N(OrderLife, (OrderLife), (tick_data), onTick, onMinute)

// 一事件 (e token; E 公式事件上标; EN / CN 事件英 / 中文; DESC 用时描述) × 一侧 (side token; S 公式侧上标; SEN / SCN 侧英 / 中文) × 一 τ (t token; TE / TF 英文 / 公式字面) 的 额/量/笔 3 行
#define ORDERLIFE_ROWS(X, CAT1, e, E, EN, CN, DESC, side, S, SEN, SCN, t, TE, TF)                                                                                                                                                                                                                                                                \
  X(amt_##e##_##side##_lt_##t, CAT1, AUTO, SEN " Order " EN " Amount (age < " TE ")", SCN "委托快速" CN "额(<" TE ")", "分钟内" SCN "方委托 " DESC " 用时<" TE " 的" CN "额(元)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^{)" E "," S R"(}| \mathbf{1}[\mathrm{age}_\tau < )" TF R"(])", OP(OrderLife, amt_##e##_##side##_lt_##t, Log, None)) \
  X(vol_##e##_##side##_lt_##t, CAT1, AUTO, SEN " Order " EN " Volume (age < " TE ")", SCN "委托快速" CN "量(<" TE ")", "分钟内" SCN "方委托 " DESC " 用时<" TE " 的" CN "量(股)", R"(\sum_{\tau \in \Delta t} |O_\tau^{)" E "," S R"(}| \mathbf{1}[\mathrm{age}_\tau < )" TF R"(])", OP(OrderLife, vol_##e##_##side##_lt_##t, Log, None))        \
  X(n_##e##_##side##_lt_##t, CAT1, AUTO, SEN " Order " EN " Count (age < " TE ")", SCN "委托快速" CN "笔数(<" TE ")", "分钟内" SCN "方委托 " DESC " 用时<" TE " 的" CN "笔数", R"(\#O_{\Delta t}^{)" E "," S R"(} \mathbf{1}[\mathrm{age}_\tau < )" TF R"(])", OP(OrderLife, n_##e##_##side##_lt_##t, Log, None))

// 一 τ 的 成交 (买 3 + 卖 3) + 撤单 (买 3 + 卖 3) = 12 行. 成交侧 = 被吃的被动委托方向, 额 = 成交价 × 量; 撤单额 = 委托档价 × 量
#define ORDERLIFE_TAU_ROWS(X, CAT1, t, TE, TF)                                                          \
  ORDERLIFE_ROWS(X, CAT1, fill, "T", "Fill", "成交", "挂单→被吃成交", bid, "B", "Bid", "买", t, TE, TF) \
  ORDERLIFE_ROWS(X, CAT1, fill, "T", "Fill", "成交", "挂单→被吃成交", ask, "A", "Ask", "卖", t, TE, TF) \
  ORDERLIFE_ROWS(X, CAT1, cancel, "C", "Cancel", "撤单", "挂单→撤单", bid, "B", "Bid", "买", t, TE, TF) \
  ORDERLIFE_ROWS(X, CAT1, cancel, "C", "Cancel", "撤单", "挂单→撤单", ask, "A", "Ask", "卖", t, TE, TF)

#define FIELDS_L1_OrderLife(X, CAT1)                                                                                                                                                                                                                                                  \
  ORDERLIFE_TAU_ROWS(X, CAT1, 1s, "1s", R"(1\mathrm{s})")                                                                                                                                                                                                                             \
  ORDERLIFE_TAU_ROWS(X, CAT1, 10s, "10s", R"(10\mathrm{s})")                                                                                                                                                                                                                          \
  ORDERLIFE_TAU_ROWS(X, CAT1, 60s, "60s", R"(60\mathrm{s})")                                                                                                                                                                                                                          \
  ORDERLIFE_TAU_ROWS(X, CAT1, 300s, "300s", R"(300\mathrm{s})")                                                                                                                                                                                                                       \
  X(n_marketable_bid, CAT1, AUTO, "Marketable Bid Order Count", "到达即成交买委托笔数", "分钟内首笔成交即为主动买的委托张数(一张扫多档只计1; 广义市价单)", R"(\#\{o^{B} \mid o \text{ 首笔成交于 } \Delta t \land o \text{ 为主动方}\})", OP(OrderLife, n_marketable_bid, Log, None)) \
  X(n_marketable_ask, CAT1, AUTO, "Marketable Ask Order Count", "到达即成交卖委托笔数", "分钟内首笔成交即为主动卖的委托张数(一张扫多档只计1; 广义市价单)", R"(\#\{o^{A} \mid o \text{ 首笔成交于 } \Delta t \land o \text{ 为主动方}\})", OP(OrderLife, n_marketable_ask, Log, None))
