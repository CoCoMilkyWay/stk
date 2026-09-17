#pragma once

// =============================================================================
// Book - 盘口状态量的分钟降频 (compute=onDepth, flush=onMinute; feature_list.md 1.0 盘口部分 + 1.6)
// =============================================================================
//   时间加权均值 (状态量在两次盘口更新之间视为持有; 末状态持有到分钟末; 无盘口更新的分钟并入下一有效分钟):
//     spread_1_mean     2(a1−b1)/(a1+b1)                      (基点)
//     spread_{5,10}_raw (a_N − b_N) / mid                     (基点, 第 N 档原始价差)
//     spread_{5,10}_w   Σ_{i≤N}(a_i−b_i)(q^A_i+q^B_i) / Σ(q^A_i+q^B_i) / mid   (基点, 量加权价差)
//     qty_{bid,ask}_{1,5,10,all}_mean  前 N 档量深度 (股; all = 全簿单侧挂单量)
//     qty_eff_1_mean    min(q^B_1, q^A_1) (有效深度, 逐 tick 取 min 后加权)
//     amt_{bid,ask}_{5,10,all}_mean    前 N 档金额深度 Σ p_i q_i (元; all = 全簿量 × 一档价 近似)
//     micro_mean        量加权中间价 (元)
//   事件型 (每次盘口更新一个中间价变化率 r = 1e4·ln(mid/mid_prev), 分钟内累计):
//     mid_rv = Σ r²   mid_rm3 = Σ r³   mid_r_max = max r  (无更新 → NaN)
//   一档为空 (价 ≤ 0) 的快照跳过 (持有上一状态). 当日尚无有效盘口 → 全 NaN.
//   fp16 落盘: 量 / 额 / 幂和 Log Tf; 价差 (基点) / 价 / 极值 原值.
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"
#include <algorithm>
#include <cmath>
#include <cstdint>

template <size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Book {
  static_assert(DEPTH_SIZE >= 10, "Book reads 10 levels");
  static constexpr float kBp = 1e4f;
  static inline uint32_t tick_ms(const TickData &td) { return td.l0_index * 1000u + td.lob.millisecond * 10u; }

public:
  // 前 21 口 = 时间加权量 (与 tw_/cur_ 下标一致), 后 3 口 = 中间价变化率
  enum Out : size_t {
    spread_1_mean,
    spread_5_raw,
    spread_10_raw,
    spread_5_w,
    spread_10_w,
    qty_bid_1_mean,
    qty_bid_5_mean,
    qty_bid_10_mean,
    qty_bid_all_mean,
    qty_ask_1_mean,
    qty_ask_5_mean,
    qty_ask_10_mean,
    qty_ask_all_mean,
    qty_eff_1_mean,
    amt_bid_5_mean,
    amt_bid_10_mean,
    amt_bid_all_mean,
    amt_ask_5_mean,
    amt_ask_10_mean,
    amt_ask_all_mean,
    micro_mean,
    mid_rv,
    mid_rm3,
    mid_r_max,
    kCount
  };
  static constexpr size_t NTW = mid_rv; // 时间加权口数
  float y[kCount] = {};

  Book(const TickData &td, const MinuteData &md,
       const DepthSeries &bid_price, const DepthSeries &ask_price,
       const DepthSeries &bid_qty, const DepthSeries &ask_qty,
       const Series &mid_price, const Series &micro_price)
      : td_(td), md_(md), bp_(bid_price), ap_(ask_price), bq_(bid_qty), aq_(ask_qty), mid_(mid_price), micro_(micro_price) {}

  inline void compute() {
    const float b1 = bp_[0].back(), a1 = ap_[0].back();
    if (b1 <= 0.0f || a1 <= 0.0f) [[unlikely]]
      return; // 一侧为空: 持有上一状态
    const uint32_t t = tick_ms(td_);
    if (has_state_) {
      // 14:57-15:00 全部映射到哨兵秒 15299, ms 每秒回绕 → t 可倒退; uint32 差会下溢成 4e9, 必须钳零
      const float w = static_cast<float>(t > t_prev_ ? t - t_prev_ : 0u);
      for (size_t i = 0; i < NTW; ++i)
        tw_[i] += cur_[i] * w;
    } else {
      t_start_ = t; // 当日首个有效盘口: 时间加权从这里起算
    }
    t_prev_ = t;
    has_state_ = true;

    const float mid = mid_.back();
    const float inv_mid = kBp / mid;
    float qb = 0.0f, qa = 0.0f, ab = 0.0f, aa = 0.0f, ws = 0.0f, wq = 0.0f;
    for (size_t i = 0; i < 10; ++i) {
      const float pb = bp_[i].back(), pa = ap_[i].back();
      const float vb = bq_[i].back(), va = -aq_[i].back(); // ask 存负值 (Depth 已钳)
      qb += vb, qa += va;
      ab += pb * vb, aa += pa * va;
      ws += (pa - pb) * (va + vb), wq += va + vb;
      if (i == 0) {
        cur_[qty_bid_1_mean] = vb, cur_[qty_ask_1_mean] = va;
        cur_[qty_eff_1_mean] = std::min(vb, va);
        cur_[spread_1_mean] = 2.0f * (pa - pb) / (pa + pb) * kBp;
      } else if (i == 4) {
        cur_[qty_bid_5_mean] = qb, cur_[qty_ask_5_mean] = qa;
        cur_[amt_bid_5_mean] = ab, cur_[amt_ask_5_mean] = aa;
        cur_[spread_5_raw] = (pa - pb) * inv_mid;
        cur_[spread_5_w] = wq > 0.0f ? ws / wq * inv_mid : kNaN;
      }
    }
    cur_[qty_bid_10_mean] = qb, cur_[qty_ask_10_mean] = qa;
    cur_[amt_bid_10_mean] = ab, cur_[amt_ask_10_mean] = aa;
    cur_[spread_10_raw] = (ap_[9].back() - bp_[9].back()) * inv_mid;
    cur_[spread_10_w] = wq > 0.0f ? ws / wq * inv_mid : kNaN;
    const float all_b = static_cast<float>(td_.lob.all_bid_volume), all_a = static_cast<float>(td_.lob.all_ask_volume);
    cur_[qty_bid_all_mean] = all_b, cur_[qty_ask_all_mean] = all_a;
    cur_[amt_bid_all_mean] = all_b * b1, cur_[amt_ask_all_mean] = all_a * a1;
    cur_[micro_mean] = micro_.back();

    if (mid_prev_ > 0.0f) {
      const float r = kBp * std::log(mid / mid_prev_);
      const float r2 = r * r;
      rv_ += r2, rm3_ += r2 * r;
      rmax_ = n_mid_++ == 0 ? r : std::max(rmax_, r);
    }
    mid_prev_ = mid;
  }

  inline void flush() {
    const uint32_t t_end = (static_cast<uint32_t>(L1_to_L0(md_.l1_index)) + 60u) * 1000u;
    if (has_state_) {
      const float w = static_cast<float>(t_end > t_prev_ ? t_end - t_prev_ : 0u);
      const float span = static_cast<float>(t_end > t_start_ ? t_end - t_start_ : 0u);
      const float inv = span > 0.0f ? 1.0f / span : kNaN;
      for (size_t i = 0; i < NTW; ++i) {
        y[i] = (tw_[i] + cur_[i] * w) * inv;
        tw_[i] = 0.0f;
      }
      t_start_ = t_prev_ = t_end;
    } else {
      for (size_t i = 0; i < NTW; ++i)
        y[i] = kNaN;
    }
    const bool any = n_mid_ > 0;
    y[mid_rv] = any ? rv_ : kNaN;
    y[mid_rm3] = any ? rm3_ : kNaN;
    y[mid_r_max] = any ? rmax_ : kNaN;
    rv_ = rm3_ = rmax_ = 0.0f;
    n_mid_ = 0;
  }

  void reset() {
    has_state_ = false;
    t_start_ = t_prev_ = 0;
    for (size_t i = 0; i < NTW; ++i)
      tw_[i] = cur_[i] = 0.0f;
    mid_prev_ = rv_ = rm3_ = rmax_ = 0.0f;
    n_mid_ = 0;
  }

private:
  const TickData &td_;
  const MinuteData &md_;
  const DepthSeries &bp_, &ap_, &bq_, &aq_;
  const Series &mid_, &micro_;

  bool has_state_ = false;
  uint32_t t_start_ = 0, t_prev_ = 0;
  float tw_[NTW] = {};  // Σ 值 × 持有时长 (ms)
  float cur_[NTW] = {}; // 当前持有状态
  float mid_prev_ = 0.0f, rv_ = 0.0f, rm3_ = 0.0f, rmax_ = 0.0f;
  uint32_t n_mid_ = 0;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Book(N) N(Book, (Book<L2::LOB_DEPTH>), (tick_data, minute_data, Depth.bid_price, Depth.ask_price, Depth.bid_qty, Depth.ask_qty, MidPrice.out(), MicroPrice.out()), onDepth, onMinute)

// 一侧 (side token, S 公式上标, CN) 的量深度 4 行 + 金额深度 3 行
#define BOOK_SIDE_ROWS(X, CAT1, side, S, CN)                                                                                                                                                                                                               \
  X(qty_##side##_1_mean, CAT1, RAW, "Depth " S "1 Qty TW-Mean", CN "一档量均值", "分钟内" CN "一档挂单量时间加权均值(股)", R"(\overline{V_{1}^{M,)" S R"(}}^{\,tw})", OP(Book, qty_##side##_1_mean, Log, None))                                            \
  X(qty_##side##_5_mean, CAT1, RAW, "Depth " S "5 Qty TW-Mean", CN "五档量深度均值", "分钟内" CN "前5档挂单量之和的时间加权均值(股)", R"(\overline{\sum_{i \leq 5} V_{i}^{M,)" S R"(}}^{\,tw})", OP(Book, qty_##side##_5_mean, Log, None))                 \
  X(qty_##side##_10_mean, CAT1, RAW, "Depth " S "10 Qty TW-Mean", CN "十档量深度均值", "分钟内" CN "前10档挂单量之和的时间加权均值(股)", R"(\overline{\sum_{i \leq 10} V_{i}^{M,)" S R"(}}^{\,tw})", OP(Book, qty_##side##_10_mean, Log, None))            \
  X(qty_##side##_all_mean, CAT1, RAW, "Depth " S " All Qty TW-Mean", CN "全簿量均值", "分钟内" CN "全簿挂单量时间加权均值(股)", R"(\overline{V_{all}^{M,)" S R"(}}^{\,tw})", OP(Book, qty_##side##_all_mean, Log, None))                                   \
  X(amt_##side##_5_mean, CAT1, RAW, "Depth " S "5 Amount TW-Mean", CN "五档金额深度均值", "分钟内" CN "前5档挂单金额之和的时间加权均值(元)", R"(\overline{\sum_{i \leq 5} P_i V_{i}^{M,)" S R"(}}^{\,tw})", OP(Book, amt_##side##_5_mean, Log, None))      \
  X(amt_##side##_10_mean, CAT1, RAW, "Depth " S "10 Amount TW-Mean", CN "十档金额深度均值", "分钟内" CN "前10档挂单金额之和的时间加权均值(元)", R"(\overline{\sum_{i \leq 10} P_i V_{i}^{M,)" S R"(}}^{\,tw})", OP(Book, amt_##side##_10_mean, Log, None)) \
  X(amt_##side##_all_mean, CAT1, RAW, "Depth " S " All Amount TW-Mean", CN "全簿金额均值", "分钟内" CN "全簿挂单量×一档价的时间加权均值(元, 近似)", R"(\overline{P_1 V_{all}^{M,)" S R"(}}^{\,tw})", OP(Book, amt_##side##_all_mean, Log, None))

#define FIELDS_L1_Book(X, CAT1)                                                                                                                                                                                                                                                                                      \
  X(spread_1_mean, CAT1, RAW, "Relative Spread TW-Mean", "相对价差均值", "分钟内一档相对价差时间加权均值(基点)", R"(\overline{\frac{2(P_1^{M,A}-P_1^{M,B})}{P_1^{M,A}+P_1^{M,B}}}^{\,tw} \times 10^4)", OP(Book, spread_1_mean, None, None))                                                                         \
  X(spread_5_raw, CAT1, RAW, "Level-5 Raw Spread TW-Mean", "五档原始价差均值", "分钟内第5档卖价减买价对中间价的时间加权均值(基点)", R"(\overline{\frac{P_5^{M,A}-P_5^{M,B}}{P_{mid}}}^{\,tw} \times 10^4)", OP(Book, spread_5_raw, None, None))                                                                      \
  X(spread_10_raw, CAT1, RAW, "Level-10 Raw Spread TW-Mean", "十档原始价差均值", "分钟内第10档卖价减买价对中间价的时间加权均值(基点)", R"(\overline{\frac{P_{10}^{M,A}-P_{10}^{M,B}}{P_{mid}}}^{\,tw} \times 10^4)", OP(Book, spread_10_raw, None, None))                                                            \
  X(spread_5_w, CAT1, RAW, "Level-5 Weighted Spread TW-Mean", "五档加权价差均值", "前5档价差按两侧挂单量加权对中间价的时间加权均值(基点)", R"(\overline{\frac{\sum_{i\leq5}(P_i^{A}-P_i^{B})(V_i^{A}+V_i^{B})}{P_{mid}\sum_{i\leq5}(V_i^{A}+V_i^{B})}}^{\,tw} \times 10^4)", OP(Book, spread_5_w, None, None))       \
  X(spread_10_w, CAT1, RAW, "Level-10 Weighted Spread TW-Mean", "十档加权价差均值", "前10档价差按两侧挂单量加权对中间价的时间加权均值(基点)", R"(\overline{\frac{\sum_{i\leq10}(P_i^{A}-P_i^{B})(V_i^{A}+V_i^{B})}{P_{mid}\sum_{i\leq10}(V_i^{A}+V_i^{B})}}^{\,tw} \times 10^4)", OP(Book, spread_10_w, None, None)) \
  BOOK_SIDE_ROWS(X, CAT1, bid, "B", "买盘")                                                                                                                                                                                                                                                                          \
  BOOK_SIDE_ROWS(X, CAT1, ask, "A", "卖盘")                                                                                                                                                                                                                                                                          \
  X(qty_eff_1_mean, CAT1, RAW, "Effective Depth TW-Mean", "有效深度均值", "分钟内min(买一量,卖一量)的时间加权均值(股)", R"(\overline{\min(V_1^{M,B}, V_1^{M,A})}^{\,tw})", OP(Book, qty_eff_1_mean, Log, None))                                                                                                      \
  X(micro_mean, CAT1, RAW, "Micro Price TW-Mean", "微观价格均值", "分钟内量加权中间价的时间加权均值(元)", R"(\overline{P_{micro}}^{\,tw})", OP(Book, micro_mean, None, None))                                                                                                                                        \
  X(mid_rv, CAT1, RAW, "Mid-Price Realized Variance", "中间价变化率平方和", "分钟内逐次盘口更新中间价对数变化率(基点)平方和", R"(\sum_{j \in \Delta t} r_j^2,\; r_j = 10^4 \ln\frac{P_{mid,j}}{P_{mid,j-1}})", OP(Book, mid_rv, Log, None))                                                                          \
  X(mid_rm3, CAT1, RAW, "Mid-Price Third Moment", "中间价变化率立方和", "分钟内中间价对数变化率(基点)立方和; 偏度=rm3/rv^1.5", R"(\sum_{j \in \Delta t} r_j^3)", OP(Book, mid_rm3, Log, None))                                                                                                                       \
  X(mid_r_max, CAT1, RAW, "Mid-Price Max Change", "中间价变化率最大值", "分钟内中间价对数变化率(基点)最大值", R"(\max_{j \in \Delta t} r_j)", OP(Book, mid_r_max, None, None))
