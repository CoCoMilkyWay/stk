#pragma once

// =============================================================================
// Book - 盘口状态量的分钟降频: 一次扫描出全部盘口特征 (compute=onDepth, flush=onMinute; feature_list.md 1.0 盘口部分 + 1.6)
// =============================================================================
//   全部状态列 = 时间加权均值 (状态量在两次盘口更新之间视为持有; 末状态持有到分钟末; 无盘口更新的分钟并入下一有效分钟).
//   某快照某列无定义 (深度不足 / 分母 0) → 该列该段时长不计权 (ok_ 位图), 分钟内全程无定义 → NaN.
//     spread_l{1,5,10}       第 N 档 (a_N − b_N) / mid                          (基点; 单档, 非前 N 档累计; N=1 即相对价差)
//     spread_w_{5,10}        Σ_{i≤N}(a_i−b_i)(q^A_i+q^B_i) / Σ(q^A_i+q^B_i) / mid  (基点, 量加权价差)
//     qty_{bid,ask}_{1,5,10,all}   前 N 档量深度 (股; all = 全簿单侧挂单量, LOB 增量维护)
//     amt_{bid,ask}_{1,5,10,all}   前 N 档金额深度 Σ p_i q_i (元; all = 全簿量 × 一档价 近似)
//     qty_eff_1              min(q^B_1, q^A_1) (有效深度, 逐快照取 min 后加权)
//     obi_{1,5,10,all}       (Q^B_N − Q^A_N) / (Q^B_N + Q^A_N)   ∈ [−1, 1], 正 = 买方占优
//     tlr_{bid,ask}_{1,5,10} Q_N / Q_all                          前 N 档占全簿比, 越大越易被击穿
//     cost_{buy,sell}_{10w,100w,300w}  吃掉 A 元 (沿 30 档累计, 末档按比例) 的 VWAP 对 mid 偏离 (基点; 全簿不足 A → 该快照无定义)
//     micro                  量加权中间价 (元)
//     mid                    中间价 (元)
//   事件型 (每次盘口更新一个中间价变化率 r = 1e4·ln(mid/mid_prev), 分钟内累计; 命名与 Realized 的 rv_3s 族对仗):
//     rv_mid = Σ r²   rm3_mid = Σ r³   r_max_mid = max r  (无更新 → NaN)
//   一档为空 (价 ≤ 0) 的快照跳过 (持有上一状态). 当日尚无有效盘口 → 全 NaN.
//   fp16 落盘: 量 / 额 / 幂和 Log Tf; 基点 / 比率 / 价 / 极值 原值.
//   【fast-math 契约】不做 isnan; 无定义用 ok_ 位图显式表示, 不靠 NaN 传播.
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
  static constexpr size_t NCOST = 3;
  static constexpr float COST_TARGET[NCOST] = {1e5f, 1e6f, 3e6f}; // 元: 10 万 / 100 万 / 300 万 (升序)

public:
  // 前 NTW 口 = 时间加权量 (与 tw_/tt_/cur_/ok_ 下标一致), 后 3 口 = 中间价变化率
  enum Out : size_t {
    spread_l1,
    spread_l5,
    spread_l10,
    spread_w_5,
    spread_w_10,
    qty_bid_1,
    qty_bid_5,
    qty_bid_10,
    qty_bid_all,
    qty_ask_1,
    qty_ask_5,
    qty_ask_10,
    qty_ask_all,
    amt_bid_1,
    amt_bid_5,
    amt_bid_10,
    amt_bid_all,
    amt_ask_1,
    amt_ask_5,
    amt_ask_10,
    amt_ask_all,
    qty_eff_1,
    obi_1,
    obi_5,
    obi_10,
    obi_all,
    tlr_bid_1,
    tlr_bid_5,
    tlr_bid_10,
    tlr_ask_1,
    tlr_ask_5,
    tlr_ask_10,
    cost_buy_10w,
    cost_buy_100w,
    cost_buy_300w,
    cost_sell_10w,
    cost_sell_100w,
    cost_sell_300w,
    micro,
    mid,
    rv_mid,
    rm3_mid,
    r_max_mid,
    kCount
  };
  static constexpr size_t NTW = rv_mid; // 时间加权口数
  static_assert(NTW <= 64, "ok_ 位图 uint64_t");
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
    if (has_state_)                                             // 当日首个有效盘口之前无持有状态, 时间加权从它起算
      hold(static_cast<float>(t > t_prev_ ? t - t_prev_ : 0u)); // 哨兵秒 15299 内 t 可倒退 (见 tick_ms), 钳零
    t_prev_ = t;
    has_state_ = true;

    // ---- 一次扫描: 前 10 档累计深度 / 价差, 全 30 档吃单成本 ----
    const float mid = mid_.back();
    const float inv_mid = kBp / mid;
    ok_ = ~0ull; // 先全有效, 下面按分母逐口清位
    float qb = 0.0f, qa = 0.0f, ab = 0.0f, aa = 0.0f, ws = 0.0f, wq = 0.0f;
    float eb_pv = 0.0f, eb_v = 0.0f, ea_pv = 0.0f, ea_v = 0.0f; // 吃单已累计 金额 / 股数 (bid 侧 / ask 侧)
    size_t kb = 0, ka = 0;                                      // 已到达的吃单目标数 (吃 bid = sell, 吃 ask = buy)
    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      const float pb = bp_[i].back(), pa = ap_[i].back();
      const float vb = bq_[i].back(), va = -aq_[i].back(); // ask 存负值 (Depth 已钳)
      if (i < 10) {
        qb += vb, qa += va;
        ab += pb * vb, aa += pa * va;
        ws += (pa - pb) * (va + vb), wq += va + vb;
        if (i == 0) {
          cur_[qty_bid_1] = vb, cur_[qty_ask_1] = va;
          cur_[amt_bid_1] = ab, cur_[amt_ask_1] = aa;
          cur_[qty_eff_1] = std::min(vb, va);
          cur_[spread_l1] = (pa - pb) * inv_mid;
          imbalance(obi_1, qb, qa);
        } else if (i == 4) {
          cur_[qty_bid_5] = qb, cur_[qty_ask_5] = qa;
          cur_[amt_bid_5] = ab, cur_[amt_ask_5] = aa;
          cur_[spread_l5] = (pa - pb) * inv_mid;
          weighted(spread_w_5, ws, wq, inv_mid);
          imbalance(obi_5, qb, qa);
        } else if (i == 9) {
          cur_[qty_bid_10] = qb, cur_[qty_ask_10] = qa;
          cur_[amt_bid_10] = ab, cur_[amt_ask_10] = aa;
          cur_[spread_l10] = (pa - pb) * inv_mid;
          weighted(spread_w_10, ws, wq, inv_mid);
          imbalance(obi_10, qb, qa);
        }
      }
      if (kb < NCOST)
        kb = eat(cost_sell_10w, kb, pb, vb, eb_pv, eb_v, mid, false);
      if (ka < NCOST)
        ka = eat(cost_buy_10w, ka, pa, va, ea_pv, ea_v, mid, true);
    }
    for (size_t k = kb; k < NCOST; ++k)
      ok_ &= ~(1ull << (cost_sell_10w + k)); // 全簿不足目标金额: 该快照无定义
    for (size_t k = ka; k < NCOST; ++k)
      ok_ &= ~(1ull << (cost_buy_10w + k));

    const float all_b = static_cast<float>(td_.lob.all_bid_volume), all_a = static_cast<float>(td_.lob.all_ask_volume);
    cur_[qty_bid_all] = all_b, cur_[qty_ask_all] = all_a;
    cur_[amt_bid_all] = all_b * b1, cur_[amt_ask_all] = all_a * a1;
    imbalance(obi_all, all_b, all_a);
    ratio(tlr_bid_1, cur_[qty_bid_1], all_b), ratio(tlr_bid_5, cur_[qty_bid_5], all_b), ratio(tlr_bid_10, cur_[qty_bid_10], all_b);
    ratio(tlr_ask_1, cur_[qty_ask_1], all_a), ratio(tlr_ask_5, cur_[qty_ask_5], all_a), ratio(tlr_ask_10, cur_[qty_ask_10], all_a);
    cur_[micro] = micro_.back();
    cur_[Out::mid] = mid; // 局部变量 mid 遮蔽枚举名, 用 Out:: 限定

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
      hold(static_cast<float>(t_end > t_prev_ ? t_end - t_prev_ : 0u)); // 末状态持有到分钟末
      t_prev_ = t_end;
    }
    for (size_t i = 0; i < NTW; ++i) {
      y[i] = tt_[i] > 0.0f ? tw_[i] / tt_[i] : kNaN; // 分钟内全程无定义 (或当日尚无盘口) → NaN
      tw_[i] = tt_[i] = 0.0f;
    }
    const bool any = n_mid_ > 0;
    y[rv_mid] = any ? rv_ : kNaN;
    y[rm3_mid] = any ? rm3_ : kNaN;
    y[r_max_mid] = any ? rmax_ : kNaN;
    rv_ = rm3_ = rmax_ = 0.0f;
    n_mid_ = 0;
  }

  void reset() {
    has_state_ = false;
    t_prev_ = 0;
    ok_ = 0;
    for (size_t i = 0; i < NTW; ++i)
      tw_[i] = tt_[i] = cur_[i] = 0.0f;
    mid_prev_ = rv_ = rm3_ = rmax_ = 0.0f;
    n_mid_ = 0;
  }

private:
  // 当前状态持有 w ms: 有定义的口累加 值 × 时长 与 时长
  inline void hold(float w) {
    for (size_t i = 0; i < NTW; ++i)
      if ((ok_ >> i) & 1ull)
        tw_[i] += cur_[i] * w, tt_[i] += w;
  }
  // 分母 > 0 才有定义; 否则清 ok_ 位 (cur_ 残留旧值, 不参与加权)
  inline void ratio(size_t i, float num, float den) {
    if (den > 0.0f)
      cur_[i] = num / den;
    else
      ok_ &= ~(1ull << i);
  }
  inline void imbalance(size_t i, float qb, float qa) { ratio(i, qb - qa, qb + qa); }
  inline void weighted(size_t i, float ws, float wq, float inv_mid) { ratio(i, ws * inv_mid, wq); }
  // 沿档位吃单: (pv, v) 累计已吃 金额 / 股数, 每到一个目标金额 (升序) 记一次 cost (末档按比例); 返回已到达目标数
  inline size_t eat(size_t base, size_t k, float p, float q, float &pv, float &v, float mid, bool buy) {
    if (p <= 0.0f || q <= 0.0f)
      return k;
    const float amt = p * q;
    while (k < NCOST && pv + amt >= COST_TARGET[k]) {
      const float vwap = COST_TARGET[k] / (v + (COST_TARGET[k] - pv) / p);
      cur_[base + k] = (buy ? vwap / mid - 1.0f : 1.0f - vwap / mid) * kBp;
      ++k;
    }
    pv += amt, v += q;
    return k;
  }

  const TickData &td_;
  const MinuteData &md_;
  const DepthSeries &bp_, &ap_, &bq_, &aq_;
  const Series &mid_, &micro_;

  bool has_state_ = false;
  uint32_t t_prev_ = 0; // 当前状态起点 (ms)
  uint64_t ok_ = 0;     // cur_ 各口是否有定义 (位 i ↔ 口 i)
  float tw_[NTW] = {};  // Σ 值 × 持有时长 (ms), 只计有定义的段
  float tt_[NTW] = {};  // Σ 持有时长 (ms), 只计有定义的段
  float cur_[NTW] = {}; // 当前持有状态
  float mid_prev_ = 0.0f, rv_ = 0.0f, rm3_ = 0.0f, rmax_ = 0.0f;
  uint32_t n_mid_ = 0;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Book(N) N(Book, (Book<L2::LOB_DEPTH>), (tick_data, minute_data, Depth.bid_price, Depth.ask_price, Depth.bid_qty, Depth.ask_qty, MidPrice.out(), MicroPrice.out()), onDepth, onMinute)

// 一侧 (side token, S 公式上标, CN) 的 量深度 4 + 金额深度 4 + 顶部占比 3 行
#define BOOK_SIDE_ROWS(X, CAT1, side, S, CN)                                                                                                                                                                                                                            \
  X(qty_##side##_1, CAT1, AUTO, "Depth " S "1 Qty", CN "一档量", "分钟内" CN "一档挂单量时间加权均值(股)", R"(\overline{V_{1}^{M,)" S R"(}}^{\,tw})", OP(Book, qty_##side##_1, Log, None))                                                                              \
  X(qty_##side##_5, CAT1, AUTO, "Depth " S "5 Qty", CN "五档量深度", "分钟内" CN "前5档挂单量之和的时间加权均值(股)", R"(\overline{\sum_{i \leq 5} V_{i}^{M,)" S R"(}}^{\,tw})", OP(Book, qty_##side##_5, Log, None))                                                   \
  X(qty_##side##_10, CAT1, AUTO, "Depth " S "10 Qty", CN "十档量深度", "分钟内" CN "前10档挂单量之和的时间加权均值(股)", R"(\overline{\sum_{i \leq 10} V_{i}^{M,)" S R"(}}^{\,tw})", OP(Book, qty_##side##_10, Log, None))                                              \
  X(qty_##side##_all, CAT1, AUTO, "Depth " S " All Qty", CN "全簿量", "分钟内" CN "全簿挂单量时间加权均值(股)", R"(\overline{V_{all}^{M,)" S R"(}}^{\,tw})", OP(Book, qty_##side##_all, Log, None))                                                                     \
  X(amt_##side##_1, CAT1, AUTO, "Depth " S "1 Amount", CN "一档金额", "分钟内" CN "一档挂单金额时间加权均值(元)", R"(\overline{P_1 V_{1}^{M,)" S R"(}}^{\,tw})", OP(Book, amt_##side##_1, Log, None))                                                                   \
  X(amt_##side##_5, CAT1, AUTO, "Depth " S "5 Amount", CN "五档金额深度", "分钟内" CN "前5档挂单金额之和的时间加权均值(元)", R"(\overline{\sum_{i \leq 5} P_i V_{i}^{M,)" S R"(}}^{\,tw})", OP(Book, amt_##side##_5, Log, None))                                        \
  X(amt_##side##_10, CAT1, AUTO, "Depth " S "10 Amount", CN "十档金额深度", "分钟内" CN "前10档挂单金额之和的时间加权均值(元)", R"(\overline{\sum_{i \leq 10} P_i V_{i}^{M,)" S R"(}}^{\,tw})", OP(Book, amt_##side##_10, Log, None))                                   \
  X(amt_##side##_all, CAT1, AUTO, "Depth " S " All Amount", CN "全簿金额", "分钟内" CN "全簿挂单量×一档价的时间加权均值(元, 近似)", R"(\overline{P_1 V_{all}^{M,)" S R"(}}^{\,tw})", OP(Book, amt_##side##_all, Log, None))                                             \
  X(tlr_##side##_1, CAT1, AUTO, "Top Level Ratio " S "1", "一档" CN "占比", "分钟内" CN "一档量占全簿" CN "量的时间加权均值", R"(\overline{V_{1}^{M,)" S R"(} / V_{all}^{M,)" S R"(}}^{\,tw})", OP(Book, tlr_##side##_1, None, None))                                   \
  X(tlr_##side##_5, CAT1, AUTO, "Top Level Ratio " S "5", "前5档" CN "占比", "分钟内" CN "前5档量占全簿" CN "量的时间加权均值(越大越易被击穿)", R"(\overline{\sum_{i \leq 5} V_{i}^{M,)" S R"(} / V_{all}^{M,)" S R"(}}^{\,tw})", OP(Book, tlr_##side##_5, None, None)) \
  X(tlr_##side##_10, CAT1, AUTO, "Top Level Ratio " S "10", "前10档" CN "占比", "分钟内" CN "前10档量占全簿" CN "量的时间加权均值", R"(\overline{\sum_{i \leq 10} V_{i}^{M,)" S R"(} / V_{all}^{M,)" S R"(}}^{\,tw})", OP(Book, tlr_##side##_10, None, None))

// 一个吃单方向 (dir token buy/sell, EN / CN 方向名, S = 被吃一侧中文, F1 / F2 = 公式中金额前后段) 的 3 个金额档
#define BOOK_COST_ROWS(X, CAT1, dir, EN, CN, S, F1, F2)                                                                                                                                                                                                            \
  X(cost_##dir##_10w, CAT1, AUTO, "Impact Cost " EN " 100k", CN "冲击成本10万", "吃掉10万元" S "盘的执行价对中间价偏离(基点)时间加权均值; 全簿不足→该段不计", R"(\overline{)" F1 R"(10^5)" F2 R"(}^{\,tw} \times 10^4)", OP(Book, cost_##dir##_10w, None, None))   \
  X(cost_##dir##_100w, CAT1, AUTO, "Impact Cost " EN " 1M", CN "冲击成本100万", "吃掉100万元" S "盘的执行价对中间价偏离(基点)时间加权均值; 全簿不足→该段不计", R"(\overline{)" F1 R"(10^6)" F2 R"(}^{\,tw} \times 10^4)", OP(Book, cost_##dir##_100w, None, None)) \
  X(cost_##dir##_300w, CAT1, AUTO, "Impact Cost " EN " 3M", CN "冲击成本300万", "吃掉300万元" S "盘的执行价对中间价偏离(基点)时间加权均值; 全簿不足→该段不计", R"(\overline{)" F1 R"(3 \times 10^6)" F2 R"(}^{\,tw} \times 10^4)", OP(Book, cost_##dir##_300w, None, None))

#define FIELDS_L1_Book(X, CAT1)                                                                                                                                                                                                                                                                                 \
  X(spread_l1, CAT1, AUTO, "Spread L1", "第1档价差", "分钟内第1档卖价减买价对中间价的时间加权均值(基点)", R"(\overline{\frac{P_1^{M,A}-P_1^{M,B}}{P_{mid}}}^{\,tw} \times 10^4)", OP(Book, spread_l1, None, None))                                                                                              \
  X(spread_l5, CAT1, AUTO, "Spread L5", "第5档价差", "分钟内第5档卖价减买价对中间价的时间加权均值(基点)", R"(\overline{\frac{P_5^{M,A}-P_5^{M,B}}{P_{mid}}}^{\,tw} \times 10^4)", OP(Book, spread_l5, None, None))                                                                                              \
  X(spread_l10, CAT1, AUTO, "Spread L10", "第10档价差", "分钟内第10档卖价减买价对中间价的时间加权均值(基点)", R"(\overline{\frac{P_{10}^{M,A}-P_{10}^{M,B}}{P_{mid}}}^{\,tw} \times 10^4)", OP(Book, spread_l10, None, None))                                                                                   \
  X(spread_w_5, CAT1, AUTO, "Weighted Spread 5", "五档加权价差", "前5档价差按两侧挂单量加权对中间价的时间加权均值(基点)", R"(\overline{\frac{\sum_{i\leq5}(P_i^{M,A}-P_i^{M,B})(V_i^{M,A}+V_i^{M,B})}{P_{mid}\sum_{i\leq5}(V_i^{M,A}+V_i^{M,B})}}^{\,tw} \times 10^4)", OP(Book, spread_w_5, None, None))       \
  X(spread_w_10, CAT1, AUTO, "Weighted Spread 10", "十档加权价差", "前10档价差按两侧挂单量加权对中间价的时间加权均值(基点)", R"(\overline{\frac{\sum_{i\leq10}(P_i^{M,A}-P_i^{M,B})(V_i^{M,A}+V_i^{M,B})}{P_{mid}\sum_{i\leq10}(V_i^{M,A}+V_i^{M,B})}}^{\,tw} \times 10^4)", OP(Book, spread_w_10, None, None)) \
  BOOK_SIDE_ROWS(X, CAT1, bid, "B", "买盘")                                                                                                                                                                                                                                                                     \
  BOOK_SIDE_ROWS(X, CAT1, ask, "A", "卖盘")                                                                                                                                                                                                                                                                     \
  X(qty_eff_1, CAT1, AUTO, "Effective Depth", "有效深度", "分钟内min(买一量,卖一量)的时间加权均值(股)", R"(\overline{\min(V_1^{M,B}, V_1^{M,A})}^{\,tw})", OP(Book, qty_eff_1, Log, None))                                                                                                                      \
  X(obi_1, CAT1, AUTO, "Order Book Imbalance 1", "一档失衡", "分钟内一档买卖量失衡率的时间加权均值", R"(\overline{\frac{V_{1}^{M,B} - V_{1}^{M,A}}{V_{1}^{M,B} + V_{1}^{M,A}}}^{\,tw})", OP(Book, obi_1, None, None))                                                                                           \
  X(obi_5, CAT1, AUTO, "Order Book Imbalance 5", "五档失衡", "分钟内前5档累计买卖量失衡率的时间加权均值", R"(\overline{\frac{\sum_{i\leq5}(V_{i}^{M,B} - V_{i}^{M,A})}{\sum_{i\leq5}(V_{i}^{M,B} + V_{i}^{M,A})}}^{\,tw})", OP(Book, obi_5, None, None))                                                        \
  X(obi_10, CAT1, AUTO, "Order Book Imbalance 10", "十档失衡", "分钟内前10档累计买卖量失衡率的时间加权均值", R"(\overline{\frac{\sum_{i\leq10}(V_{i}^{M,B} - V_{i}^{M,A})}{\sum_{i\leq10}(V_{i}^{M,B} + V_{i}^{M,A})}}^{\,tw})", OP(Book, obi_10, None, None))                                                  \
  X(obi_all, CAT1, AUTO, "Order Book Imbalance All", "全簿失衡", "分钟内全簿买卖挂单量失衡率的时间加权均值", R"(\overline{\frac{V_{all}^{M,B} - V_{all}^{M,A}}{V_{all}^{M,B} + V_{all}^{M,A}}}^{\,tw})", OP(Book, obi_all, None, None))                                                                         \
  BOOK_COST_ROWS(X, CAT1, buy, "Buy", "买方", "卖", R"(\frac{\mathrm{VWAP}^{A}()", R"()}{P_{mid}} - 1)")                                                                                                                                                                                                        \
  BOOK_COST_ROWS(X, CAT1, sell, "Sell", "卖方", "买", R"(1 - \frac{\mathrm{VWAP}^{B}()", R"()}{P_{mid}})")                                                                                                                                                                                                      \
  X(micro, CAT1, AUTO, "Micro Price", "微观价格", "分钟内量加权中间价的时间加权均值(元)", R"(\overline{P_{micro}}^{\,tw})", OP(Book, micro, None, None))                                                                                                                                                        \
  X(mid, CAT1, AUTO, "Mid Price", "中间价", "分钟内中间价的时间加权均值(元)", R"(\overline{P_{mid}}^{\,tw})", OP(Book, mid, None, None))                                                                                                                                                                        \
  X(rv_mid, CAT1, AUTO, "Mid-Price Realized Variance", "中间价变化率平方和", "分钟内逐次盘口更新中间价对数变化率(基点)平方和", R"(\sum_{j \in \Delta t} r_j^2,\; r_j = 10^4 \ln\frac{P_{mid,j}}{P_{mid,j-1}})", OP(Book, rv_mid, Log, None))                                                                    \
  X(rm3_mid, CAT1, AUTO, "Mid-Price Third Moment", "中间价变化率立方和", "分钟内中间价对数变化率(基点)立方和; 偏度=rm3/rv^1.5", R"(\sum_{j \in \Delta t} r_j^3)", OP(Book, rm3_mid, Log, None))                                                                                                                 \
  X(r_max_mid, CAT1, AUTO, "Mid-Price Max Change", "中间价变化率最大值", "分钟内中间价对数变化率(基点)最大值", R"(\max_{j \in \Delta t} r_j)", OP(Book, r_max_mid, None, None))
