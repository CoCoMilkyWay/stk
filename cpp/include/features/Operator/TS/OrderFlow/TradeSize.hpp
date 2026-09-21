#pragma once

// =============================================================================
// TradeSize - 大小单 · 成交维度 (compute=onTaker, flush=onMinute; feature_list.md 1.3 B 轴 + q 轴)
// =============================================================================
//   按单笔成交额分档, 阈值两组共 9 个, 一次分桶:
//     B 轴 (固定金额)  4万 / 20万 / 100万 元 (Wind 口径 小/中/大/特大 的三条分界)
//     q 轴 (本股分位)  本股前 5 个交易日单笔成交额分位 q ∈ {50,80,84,93,95,98}% (P84/P93/P98 ≙ 对数正态 +1.0/1.5/2.0σ);
//                     每日一只 KLL (log 金额), reset 归档 + 各日 ICDF 分位取均值; 无历史 (首日) q 轴列全 NaN.
//   侧 = 主动方向 (与 Flow 的 {amt,vol,n}_taker_{bid,ask} 同名同源, 这里多一个 "≥ 阈值" 条件):
//     {amt,vol,n}_taker_{bid,ask}_ge_{B|q}  单笔成交额 ≥ 阈值 的主动买 / 主动卖 成交 额(元) / 量(股) / 笔  (每分钟增量)
//     dlogp_taker_{bid,ask}_ge_{B|q}        单笔成交额 ≥ 阈值 的主动买 / 主动卖 成交 Σ 1e4·Δln(成交价) (相对上一笔任意成交; 基点)
//                                           (大单推动涨幅: 华泰 072 early_active_bigorder_ret 用主动买口; 两口之和 = 不分向)
//   "≥" 为嵌套集: 分档 = 相邻两档之差 (因子层做), 全体 = Flow 的 taker 列.
//   实现: 9 阈值每日合并升序 (thr_ / slot_ 记原输出槽), 每笔只落一个桶 (桶 = 满足的阈值个数 0..n), flush 时后缀和 → 每笔 O(1).
//   Δln p 不自算: 复用 TakerRet 节点 (与 Realized 的 path_len 共享, 每笔 taker 只算一次 log).
//   fp16 落盘: 额 / 量 / 笔 Log Tf; dlogp 原值 (基点).
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "math/distribution/DailyQuantile.hpp"
#include <algorithm>
#include <cmath>

// 阈值枚举 (输出槽序): 前 3 = B 轴固定金额, 后 6 = q 轴分位
#define TRADESIZE_THRS(T) T(4w) T(20w) T(100w) T(p50) T(p80) T(p84) T(p93) T(p95) T(p98)
#define TRADESIZE_ENUM_BID(b) amt_taker_bid_ge_##b, vol_taker_bid_ge_##b, n_taker_bid_ge_##b, dlogp_taker_bid_ge_##b,
#define TRADESIZE_ENUM_ASK(b) amt_taker_ask_ge_##b, vol_taker_ask_ge_##b, n_taker_ask_ge_##b, dlogp_taker_ask_ge_##b,

class TradeSize {
  static constexpr float kBp = 1e4f;
  static constexpr size_t NFIX = 3, NQ = 6, NB = NFIX + NQ, N_DAYS = 5, ND = 4;
  static constexpr float FIX[NFIX] = {40000.0f, 200000.0f, 1000000.0f}; // 元, 升序
  static constexpr float PROBS[NQ] = {0.50f, 0.80f, 0.84f, 0.93f, 0.95f, 0.98f};

public:
  // 布局: 侧 外层 × 阈值槽 中层 × {amt, vol, n, dlogp} 内层 —— y[(s·NB + j)·ND + d]
  enum Out : size_t { TRADESIZE_THRS(TRADESIZE_ENUM_BID) TRADESIZE_THRS(TRADESIZE_ENUM_ASK) kCount };
  static_assert(kCount == 2 * NB * ND);
  float y[kCount] = {};

  TradeSize(const TickData &td, const Series &taker_dlogp) : td_(td), taker_dlogp_(taker_dlogp), dq_(PROBS) { rebuild(); }

  inline void compute() {
    const auto &lob = td_.lob;
    const float p = lob.price;
    const float v = static_cast<float>(lob.volume);
    const float a = p * v;
    if (a <= 0.0f) [[unlikely]] // 零价 / 零量: 不是有效成交 (log 也无定义)
      return;
    dq_.add(std::log(a));
    const size_t s = lob.order_dir == L2::OrderDirection::BID ? 0 : 1;
    size_t b = 0;
    while (b < n_thr_ && a >= thr_[b])
      ++b;
    float (&bk)[ND] = bucket_[s][b];
    bk[0] += a, bk[1] += v, bk[2] += 1.0f;
    bk[3] += taker_dlogp_.back(); // TakerRet 同域已 flush; 首笔 = 0, 加零无影响
  }

  // ≥ 第 k 个 (升序) 阈值 ⇔ 桶 ≥ k+1: 后缀和写到该阈值的输出槽; 无阈值的 q 轴槽 (首日) 归 0 = 该桶无笔数
  inline void flush() {
    dq_.flush_batch();
    if (!has_q_)
      for (size_t s = 0; s < 2; ++s)
        for (size_t j = NFIX; j < NB; ++j)
          for (size_t d = 0; d < ND; ++d)
            y[(s * NB + j) * ND + d] = 0.0f;
    for (size_t s = 0; s < 2; ++s) {
      float run[ND] = {};
      for (size_t k = n_thr_; k >= 1; --k) {
        float *o = &y[(s * NB + slot_[k - 1]) * ND];
        for (size_t d = 0; d < ND; ++d)
          o[d] = run[d] += bucket_[s][k][d];
      }
    }
    clear();
  }

  void reset() {
    float lq[NQ];
    has_q_ = dq_.roll(lq);
    if (has_q_)
      for (size_t j = 0; j < NQ; ++j)
        thr_q_[j] = std::exp(lq[j]);
    rebuild();
    clear();
  }

private:
  // 固定 + 分位 阈值合并升序 (插入排序, 9 个), slot_ 记每个阈值的输出槽 (0..2 固定, 3..8 分位)
  void rebuild() {
    n_thr_ = NFIX + (has_q_ ? NQ : 0);
    for (size_t k = 0; k < n_thr_; ++k)
      thr_[k] = k < NFIX ? FIX[k] : thr_q_[k - NFIX], slot_[k] = k;
    for (size_t k = 1; k < n_thr_; ++k)
      for (size_t m = k; m > 0 && thr_[m] < thr_[m - 1]; --m)
        std::swap(thr_[m], thr_[m - 1]), std::swap(slot_[m], slot_[m - 1]);
  }

  void clear() {
    for (auto &side : bucket_)
      for (auto &bk : side)
        for (auto &d : bk)
          d = 0.0f;
  }

  const TickData &td_;
  const Series &taker_dlogp_; // TakerRet 输出口 (同域 onTaker, 拓扑序在前, back() 即本笔值)
  DailyQuantile<NQ, N_DAYS> dq_;
  float thr_q_[NQ] = {}; // 元, 升序 (分位单调)
  bool has_q_ = false;
  float thr_[NB] = {};   // 当日生效阈值, 升序
  size_t slot_[NB] = {}; // thr_[k] 对应的输出槽
  size_t n_thr_ = 0;
  float bucket_[2][NB + 1][ND] = {}; // [侧][桶][amt, vol, n, dlogp]
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_TradeSize(N) N(TradeSize, (TradeSize), (tick_data, TakerRet.out()), onTaker, onMinute)

// 一侧 (side token, S = 公式侧上标, CN 侧中文) × 一阈值 (b token; TE / TC / TF = 阈值的 英文 / 中文 / 公式 字面) 的 额/量/笔/价变 4 行
#define TRADESIZE_ROWS(X, CAT1, side, S, CN, b, TE, TC, TF)                                                                                                                                                                                                                                                       \
  X(amt_taker_##side##_ge_##b, CAT1, AUTO, "Taker " S " >= " TE " Amount", "主动" CN "≥" TC "成交额", "分钟内单笔成交额≥" TC "的主动" CN "成交额(元)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^{T,)" S R"(}| \mathbf{1}[P_\tau|O_\tau| \geq )" TF R"(])", OP(TradeSize, amt_taker_##side##_ge_##b, Log, None)) \
  X(vol_taker_##side##_ge_##b, CAT1, AUTO, "Taker " S " >= " TE " Volume", "主动" CN "≥" TC "成交量", "分钟内单笔成交额≥" TC "的主动" CN "成交量(股)", R"(\sum_{\tau \in \Delta t} |O_\tau^{T,)" S R"(}| \mathbf{1}[P_\tau|O_\tau| \geq )" TF R"(])", OP(TradeSize, vol_taker_##side##_ge_##b, Log, None))        \
  X(n_taker_##side##_ge_##b, CAT1, AUTO, "Taker " S " >= " TE " Count", "主动" CN "≥" TC "成交笔数", "分钟内单笔成交额≥" TC "的主动" CN "成交笔数", R"(\#O_{\Delta t}^{T,)" S R"(} \mathbf{1}[P_\tau|O_\tau| \geq )" TF R"(])", OP(TradeSize, n_taker_##side##_ge_##b, Log, None))                                \
  X(dlogp_taker_##side##_ge_##b, CAT1, AUTO, "Taker " S " >= " TE " Log-Price Change", "主动" CN "≥" TC "成交价变动和", "分钟内单笔成交额≥" TC "的主动" CN "成交对上一笔成交价的对数变动之和(基点)", R"(\sum_{\tau \in \Delta t} 10^4 \ln\frac{P_\tau}{P_{\tau-1}} \mathbf{1}[O_\tau^{T,)" S R"(}, P_\tau|O_\tau| \geq )" TF R"(])", OP(TradeSize, dlogp_taker_##side##_ge_##b, None, None))

// 一阈值的 买 4 + 卖 4 = 8 行
#define TRADESIZE_THR_ROWS(X, CAT1, b, TE, TC, TF)       \
  TRADESIZE_ROWS(X, CAT1, bid, "B", "买", b, TE, TC, TF) \
  TRADESIZE_ROWS(X, CAT1, ask, "A", "卖", b, TE, TC, TF)

#define FIELDS_L1_TradeSize(X, CAT1)                                          \
  TRADESIZE_THR_ROWS(X, CAT1, 4w, "4w", "4万元", R"(4 \times 10^4)")          \
  TRADESIZE_THR_ROWS(X, CAT1, 20w, "20w", "20万元", R"(2 \times 10^5)")       \
  TRADESIZE_THR_ROWS(X, CAT1, 100w, "100w", "100万元", R"(10^6)")             \
  TRADESIZE_THR_ROWS(X, CAT1, p50, "P50", "前5日P50分位", R"(Q_{50\%}^{5d})") \
  TRADESIZE_THR_ROWS(X, CAT1, p80, "P80", "前5日P80分位", R"(Q_{80\%}^{5d})") \
  TRADESIZE_THR_ROWS(X, CAT1, p84, "P84", "前5日P84分位", R"(Q_{84\%}^{5d})") \
  TRADESIZE_THR_ROWS(X, CAT1, p93, "P93", "前5日P93分位", R"(Q_{93\%}^{5d})") \
  TRADESIZE_THR_ROWS(X, CAT1, p95, "P95", "前5日P95分位", R"(Q_{95\%}^{5d})") \
  TRADESIZE_THR_ROWS(X, CAT1, p98, "P98", "前5日P98分位", R"(Q_{98\%}^{5d})")
