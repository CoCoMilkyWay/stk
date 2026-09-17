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
//     dlogp_taker_ge_{B|q}                  单笔成交额 ≥ 阈值 的成交 Σ 1e4·Δln(成交价) (相对上一笔任意成交; 基点)
//   "≥" 为嵌套集: 分档 = 相邻两档之差 (因子层做), 全体 = Flow 的 taker 列.
//   实现: 9 阈值每日合并升序 (thr_ / slot_ 记原输出槽), 每笔只落一个桶 (桶 = 满足的阈值个数 0..n), flush 时后缀和 → 每笔 O(1).
//   Δln p 不自算: 复用 TakerRet 节点 (与 Realized 的 path_len 共享, 每笔 taker 只算一次 log).
//   fp16 落盘: 额 / 量 / 笔 Log Tf; dlogp 原值 (基点).
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "math/distribution/KLLcache.hpp"
#include <algorithm>
#include <cmath>
#include <vector>

// 前 N_DAYS 日分位阈值 (日更): 每日一只 KLL (log x), roll() 归档当日并以各日 ICDF 分位均值更新阈值
template <size_t NQ, size_t N_DAYS>
class DailyQuantile {
  static constexpr size_t KLL_K = 128;
  static constexpr size_t KLL_RECON = 128; // u 网格 1/127, P98 落在网格内插

public:
  explicit DailyQuantile(const float (&probs)[NQ]) : cur_(KLL_K, KLL_RECON) {
    for (size_t i = 0; i < NQ; ++i)
      probs_[i] = probs[i];
    for (auto &d : days_)
      d = KLLcache(KLL_K, KLL_RECON);
    buf_.reserve(1024);
  }

  inline void add(float x) { buf_.push_back(x); }

  inline void flush_batch() {
    if (!buf_.empty()) {
      cur_.addBatch(buf_);
      buf_.clear();
    }
  }

  // 跨日: 归档当日 (空 sketch 不入), 阈值 ← 历史各日分位均值; 返回是否有可用阈值
  bool roll(float (&thr)[NQ]) {
    flush_batch();
    if (!cur_.empty()) {
      days_[write_] = std::move(cur_);
      cur_ = KLLcache(KLL_K, KLL_RECON);
      write_ = (write_ + 1) % N_DAYS;
      if (n_days_ < N_DAYS)
        ++n_days_;
    }
    if (n_days_ == 0)
      return false;
    float sum[NQ] = {};
    for (size_t i = 0; i < n_days_; ++i) {
      const KLLcache::LinePtr icdf = days_[(write_ + N_DAYS - 1 - i) % N_DAYS].exportICDF();
      for (size_t j = 0; j < NQ; ++j)
        sum[j] += query(icdf, probs_[j]);
    }
    const float inv = 1.0f / static_cast<float>(n_days_);
    for (size_t j = 0; j < NQ; ++j)
      thr[j] = sum[j] * inv;
    return true;
  }

private:
  static float query(const KLLcache::LinePtr &icdf, float p) {
    const float u0 = icdf.x[0], u1 = icdf.x[icdf.n - 1];
    if (u1 <= u0)
      return icdf.y[0];
    float t = std::clamp((p - u0) / (u1 - u0) * static_cast<float>(icdf.n - 1), 0.0f, static_cast<float>(icdf.n - 1));
    const size_t i = static_cast<size_t>(t);
    if (i >= icdf.n - 1)
      return icdf.y[icdf.n - 1];
    const float frac = t - static_cast<float>(i);
    return icdf.y[i] + frac * (icdf.y[i + 1] - icdf.y[i]);
  }

  float probs_[NQ];
  KLLcache cur_;
  KLLcache days_[N_DAYS];
  size_t write_ = 0, n_days_ = 0;
  std::vector<float> buf_;
};

// 阈值枚举 (输出槽序): 前 3 = B 轴固定金额, 后 6 = q 轴分位
#define TRADESIZE_THRS(T) T(4w) T(20w) T(100w) T(p50) T(p80) T(p84) T(p93) T(p95) T(p98)
#define TRADESIZE_ENUM_BID(b) amt_taker_bid_ge_##b, vol_taker_bid_ge_##b, n_taker_bid_ge_##b,
#define TRADESIZE_ENUM_ASK(b) amt_taker_ask_ge_##b, vol_taker_ask_ge_##b, n_taker_ask_ge_##b,
#define TRADESIZE_ENUM_DLOGP(b) dlogp_taker_ge_##b,

class TradeSize {
  static constexpr float kBp = 1e4f;
  static constexpr size_t NFIX = 3, NQ = 6, NB = NFIX + NQ, N_DAYS = 5;
  static constexpr float FIX[NFIX] = {40000.0f, 200000.0f, 1000000.0f}; // 元, 升序
  static constexpr float PROBS[NQ] = {0.50f, 0.80f, 0.84f, 0.93f, 0.95f, 0.98f};

public:
  // 布局: 侧 外层 × 阈值槽 中层 × {amt, vol, n} 内层, 再 dlogp × 阈值槽 —— y[(s·NB + j)·3 + d], y[2·NB·3 + j]
  enum Out : size_t { TRADESIZE_THRS(TRADESIZE_ENUM_BID) TRADESIZE_THRS(TRADESIZE_ENUM_ASK) TRADESIZE_THRS(TRADESIZE_ENUM_DLOGP) kCount };
  static_assert(kCount == 2 * NB * 3 + NB);
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
    float (&bk)[3] = bucket_[s][b];
    bk[0] += a, bk[1] += v, bk[2] += 1.0f;
    dlogp_[b] += taker_dlogp_.back(); // TakerRet 同域已 flush; 首笔 = 0, 加零无影响
  }

  // ≥ 第 k 个 (升序) 阈值 ⇔ 桶 ≥ k+1: 后缀和写到该阈值的输出槽; 无阈值的 q 轴槽 (首日) NaN
  inline void flush() {
    dq_.flush_batch();
    if (!has_q_)
      for (size_t j = NFIX; j < NB; ++j) {
        for (size_t s = 0; s < 2; ++s)
          for (size_t d = 0; d < 3; ++d)
            y[(s * NB + j) * 3 + d] = kNaN;
        y[2 * NB * 3 + j] = kNaN;
      }
    for (size_t s = 0; s < 2; ++s) {
      float run[3] = {0.0f, 0.0f, 0.0f};
      for (size_t k = n_thr_; k >= 1; --k) {
        run[0] += bucket_[s][k][0], run[1] += bucket_[s][k][1], run[2] += bucket_[s][k][2];
        const size_t base = (s * NB + slot_[k - 1]) * 3;
        y[base] = run[0], y[base + 1] = run[1], y[base + 2] = run[2];
      }
    }
    float run = 0.0f;
    for (size_t k = n_thr_; k >= 1; --k) {
      run += dlogp_[k];
      y[2 * NB * 3 + slot_[k - 1]] = run;
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
        bk[0] = bk[1] = bk[2] = 0.0f;
    for (auto &d : dlogp_)
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
  float bucket_[2][NB + 1][3] = {}; // [侧][桶][amt, vol, n]
  float dlogp_[NB + 1] = {};
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_TradeSize(N) N(TradeSize, (TradeSize), (tick_data, TakerRet.out()), onTaker, onMinute)

// 一侧 (side token, S = 公式侧上标, CN 侧中文) × 一阈值 (b token; TE / TC / TF = 阈值的 英文 / 中文 / 公式 字面) 的 额/量/笔 3 行
#define TRADESIZE_ROWS(X, CAT1, side, S, CN, b, TE, TC, TF)                                                                                                                                                                                                                                                       \
  X(amt_taker_##side##_ge_##b, CAT1, AUTO, "Taker " S " >= " TE " Amount", "主动" CN "≥" TC "成交额", "分钟内单笔成交额≥" TC "的主动" CN "成交额(元)", R"(\sum_{\tau \in \Delta t} P_\tau |O_\tau^{T,)" S R"(}| \mathbf{1}[P_\tau|O_\tau| \geq )" TF R"(])", OP(TradeSize, amt_taker_##side##_ge_##b, Log, None)) \
  X(vol_taker_##side##_ge_##b, CAT1, AUTO, "Taker " S " >= " TE " Volume", "主动" CN "≥" TC "成交量", "分钟内单笔成交额≥" TC "的主动" CN "成交量(股)", R"(\sum_{\tau \in \Delta t} |O_\tau^{T,)" S R"(}| \mathbf{1}[P_\tau|O_\tau| \geq )" TF R"(])", OP(TradeSize, vol_taker_##side##_ge_##b, Log, None))        \
  X(n_taker_##side##_ge_##b, CAT1, AUTO, "Taker " S " >= " TE " Count", "主动" CN "≥" TC "成交笔数", "分钟内单笔成交额≥" TC "的主动" CN "成交笔数", R"(\#O_{\Delta t}^{T,)" S R"(} \mathbf{1}[P_\tau|O_\tau| \geq )" TF R"(])", OP(TradeSize, n_taker_##side##_ge_##b, Log, None))

// 一阈值的 买 3 + 卖 3 + dlogp 1 = 7 行
#define TRADESIZE_THR_ROWS(X, CAT1, b, TE, TC, TF)       \
  TRADESIZE_ROWS(X, CAT1, bid, "B", "买", b, TE, TC, TF) \
  TRADESIZE_ROWS(X, CAT1, ask, "A", "卖", b, TE, TC, TF) \
  X(dlogp_taker_ge_##b, CAT1, AUTO, "Big Trade Log-Price Change >= " TE, "≥" TC "成交价变动和", "分钟内单笔成交额≥" TC "的成交对上一笔成交价的对数变动之和(基点)", R"(\sum_{\tau \in \Delta t} 10^4 \ln\frac{P_\tau}{P_{\tau-1}} \mathbf{1}[P_\tau|O_\tau| \geq )" TF R"(])", OP(TradeSize, dlogp_taker_ge_##b, None, None))

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
