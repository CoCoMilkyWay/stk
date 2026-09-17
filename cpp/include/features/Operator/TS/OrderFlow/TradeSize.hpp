#pragma once

// =============================================================================
// TradeSize - 大小单 · 成交维度, 固定金额档 (compute=onTaker, flush=onMinute; feature_list.md 1.3 B 轴)
// =============================================================================
//   按单笔成交额分档 B ∈ {4万, 20万, 100万} (Wind 口径 小/中/大/特大 的三条分界), 侧 = 主动方向:
//     tk_{bid,ask}_ge_{B}_{amt,vol,n}  单笔成交额 ≥ B 的主动买 / 主动卖 成交 额(元) / 量(股) / 笔  (每分钟增量)
//     tk_ge_{B}_dlogp                  单笔成交额 ≥ B 的成交 Σ 1e4·Δln(成交价) (相对上一笔任意成交; 基点)
//   "≥ B" 为嵌套集: 分档 = 相邻两档之差 (因子层做), 全体 = Flow 的 taker 列.
//   实现: 每笔只落一个桶 (桶 = 满足的阈值个数 0..NB), flush 时后缀和 → 每笔 O(1).
//   q 轴 (TradeSizeQ): 阈值 = 本股前 5 个交易日单笔成交额分位 q ∈ {50,80,84,93,95,98}% (P84/P93/P98 ≙ 对数正态 +1.0/1.5/2.0σ),
//     每日一只 KLL (log 金额), reset 归档 + 各日 ICDF 分位取均值 (CTR 同法, 周 → 日); 无历史 (首日) 全 NaN.
//   fp16 落盘: 额 / 量 / 笔 Log Tf; dlogp 原值 (基点).
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include "math/distribution/KLLcache.hpp"
#include <algorithm>
#include <cmath>
#include <vector>

// 分桶累计核: NB 个升序阈值, 侧 × 桶 × {amt, vol, n} + 桶 × dlogp; 输出布局 = 侧 外层 × 阈值 中层 × 量纲 内层, 再 dlogp × 阈值
template <size_t NB>
class TradeSizeCore {
protected:
  static constexpr float kBp = 1e4f;
  static constexpr size_t kOut = 2 * NB * 3 + NB;

  inline void accumulate(const LOB_Feature &lob, const float (&thr)[NB]) {
    const float p = lob.price;
    if (p <= 0.0f) [[unlikely]]
      return;
    const float v = static_cast<float>(lob.volume);
    const float a = p * v;
    const size_t s = lob.order_dir == L2::OrderDirection::BID ? 0 : 1;
    size_t b = 0;
    while (b < NB && a >= thr[b])
      ++b;
    float (&bk)[3] = bucket_[s][b];
    bk[0] += a, bk[1] += v, bk[2] += 1.0f;
    if (p_last_ > 0.0f)
      dlogp_[b] += kBp * std::log(p / p_last_);
    p_last_ = p;
  }

  // ≥ 第 i 阈值 ⇔ 桶 ≥ i+1: 后缀和写 y, 清桶
  inline void flush_to(float (&y)[kOut]) {
    for (size_t s = 0; s < 2; ++s) {
      float run[3] = {0.0f, 0.0f, 0.0f};
      for (size_t b = NB; b >= 1; --b) {
        run[0] += bucket_[s][b][0], run[1] += bucket_[s][b][1], run[2] += bucket_[s][b][2];
        const size_t base = (s * NB + (b - 1)) * 3;
        y[base] = run[0], y[base + 1] = run[1], y[base + 2] = run[2];
      }
    }
    float run = 0.0f;
    for (size_t b = NB; b >= 1; --b) {
      run += dlogp_[b];
      y[2 * NB * 3 + (b - 1)] = run;
    }
    clear();
  }

  void clear() {
    for (auto &side : bucket_)
      for (auto &bk : side)
        bk[0] = bk[1] = bk[2] = 0.0f;
    for (auto &d : dlogp_)
      d = 0.0f;
  }

  float bucket_[2][NB + 1][3] = {}; // [侧][桶][amt, vol, n]
  float dlogp_[NB + 1] = {};
  float p_last_ = 0.0f; // 上一笔成交价 (任意档)
};

class TradeSize : TradeSizeCore<3> {
  static constexpr float THR[3] = {40000.0f, 200000.0f, 1000000.0f}; // 元, 升序

public:
  enum Out : size_t {
    tk_bid_ge_4w_amt,
    tk_bid_ge_4w_vol,
    tk_bid_ge_4w_n,
    tk_bid_ge_20w_amt,
    tk_bid_ge_20w_vol,
    tk_bid_ge_20w_n,
    tk_bid_ge_100w_amt,
    tk_bid_ge_100w_vol,
    tk_bid_ge_100w_n,
    tk_ask_ge_4w_amt,
    tk_ask_ge_4w_vol,
    tk_ask_ge_4w_n,
    tk_ask_ge_20w_amt,
    tk_ask_ge_20w_vol,
    tk_ask_ge_20w_n,
    tk_ask_ge_100w_amt,
    tk_ask_ge_100w_vol,
    tk_ask_ge_100w_n,
    tk_ge_4w_dlogp,
    tk_ge_20w_dlogp,
    tk_ge_100w_dlogp,
    kCount
  };
  static_assert(kCount == kOut);
  float y[kCount] = {};

  explicit TradeSize(const TickData &td) : td_(td) {}

  inline void compute() { accumulate(td_.lob, THR); }
  inline void flush() { flush_to(y); }

  void reset() {
    clear();
    p_last_ = 0.0f;
  }

private:
  const TickData &td_;
};

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

class TradeSizeQ : TradeSizeCore<6> {
  static constexpr size_t NQ = 6, N_DAYS = 5;
  static constexpr float PROBS[NQ] = {0.50f, 0.80f, 0.84f, 0.93f, 0.95f, 0.98f};

public:
  enum Out : size_t {
    tk_bid_ge_p50_amt,
    tk_bid_ge_p50_vol,
    tk_bid_ge_p50_n,
    tk_bid_ge_p80_amt,
    tk_bid_ge_p80_vol,
    tk_bid_ge_p80_n,
    tk_bid_ge_p84_amt,
    tk_bid_ge_p84_vol,
    tk_bid_ge_p84_n,
    tk_bid_ge_p93_amt,
    tk_bid_ge_p93_vol,
    tk_bid_ge_p93_n,
    tk_bid_ge_p95_amt,
    tk_bid_ge_p95_vol,
    tk_bid_ge_p95_n,
    tk_bid_ge_p98_amt,
    tk_bid_ge_p98_vol,
    tk_bid_ge_p98_n,
    tk_ask_ge_p50_amt,
    tk_ask_ge_p50_vol,
    tk_ask_ge_p50_n,
    tk_ask_ge_p80_amt,
    tk_ask_ge_p80_vol,
    tk_ask_ge_p80_n,
    tk_ask_ge_p84_amt,
    tk_ask_ge_p84_vol,
    tk_ask_ge_p84_n,
    tk_ask_ge_p93_amt,
    tk_ask_ge_p93_vol,
    tk_ask_ge_p93_n,
    tk_ask_ge_p95_amt,
    tk_ask_ge_p95_vol,
    tk_ask_ge_p95_n,
    tk_ask_ge_p98_amt,
    tk_ask_ge_p98_vol,
    tk_ask_ge_p98_n,
    tk_ge_p50_dlogp,
    tk_ge_p80_dlogp,
    tk_ge_p84_dlogp,
    tk_ge_p93_dlogp,
    tk_ge_p95_dlogp,
    tk_ge_p98_dlogp,
    kCount
  };
  static_assert(kCount == kOut);
  float y[kCount] = {};

  explicit TradeSizeQ(const TickData &td) : td_(td), dq_(PROBS) {}

  inline void compute() {
    const auto &lob = td_.lob;
    const float a = lob.price * static_cast<float>(lob.volume);
    if (a <= 0.0f) [[unlikely]]
      return;
    dq_.add(std::log(a));
    if (has_thr_)
      accumulate(lob, thr_);
  }

  inline void flush() {
    dq_.flush_batch();
    if (has_thr_) {
      flush_to(y);
      return;
    }
    for (auto &v : y)
      v = kNaN;
  }

  void reset() {
    float lq[NQ];
    has_thr_ = dq_.roll(lq);
    if (has_thr_)
      for (size_t j = 0; j < NQ; ++j)
        thr_[j] = std::exp(lq[j]);
    clear();
    p_last_ = 0.0f;
  }

private:
  const TickData &td_;
  DailyQuantile<NQ, N_DAYS> dq_;
  float thr_[NQ] = {}; // 元, 升序 (分位单调)
  bool has_thr_ = false;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_TradeSize(N) N(TradeSize, (TradeSize), (tick_data), onTaker, onMinute)
#define NODE_TradeSizeQ(N) N(TradeSizeQ, (TradeSizeQ), (tick_data), onTaker, onMinute)

// 一侧 (side token, S = 公式侧上标, CN 侧中文) × 一档 (b token, BW = 万元字面) 的 额/量/笔 3 行
#define TRADESIZE_ROWS(X, CAT1, side, S, CN, b, BW)                                                                                                                                                                                                                                                    \
  X(tk_##side##_ge_##b##_amt, CAT1, RAW, "Taker " S " >= " #BW "w Amount", "主动" CN "≥" #BW "万成交额", "分钟内单笔成交额≥" #BW "万元的主动" CN "成交额(元)", R"(\sum_{\Delta t} P|O^{T,)" S R"(}| \mathbf{1}[P|O| \geq )" #BW R"(\times 10^4])", OP(TradeSize, tk_##side##_ge_##b##_amt, Log, None)) \
  X(tk_##side##_ge_##b##_vol, CAT1, RAW, "Taker " S " >= " #BW "w Volume", "主动" CN "≥" #BW "万成交量", "分钟内单笔成交额≥" #BW "万元的主动" CN "成交量(股)", R"(\sum_{\Delta t} |O^{T,)" S R"(}| \mathbf{1}[P|O| \geq )" #BW R"(\times 10^4])", OP(TradeSize, tk_##side##_ge_##b##_vol, Log, None))  \
  X(tk_##side##_ge_##b##_n, CAT1, RAW, "Taker " S " >= " #BW "w Count", "主动" CN "≥" #BW "万成交笔数", "分钟内单笔成交额≥" #BW "万元的主动" CN "成交笔数", R"(\#O_{\Delta t}^{T,)" S R"(} \mathbf{1}[P|O| \geq )" #BW R"(\times 10^4])", OP(TradeSize, tk_##side##_ge_##b##_n, Log, None))

#define TRADESIZE_DLOGP_ROW(X, CAT1, b, BW) \
  X(tk_ge_##b##_dlogp, CAT1, RAW, "Big Trade Log-Price Change >= " #BW "w", "≥" #BW "万成交价变动和", "分钟内单笔成交额≥" #BW "万元的成交对上一笔成交价的对数变动之和(基点)", R"(\sum_{i \in \Delta t} 10^4 \ln\frac{p_i}{p_{i-1}} \mathbf{1}[p_i v_i \geq )" #BW R"(\times 10^4])", OP(TradeSize, tk_ge_##b##_dlogp, None, None))

#define FIELDS_L1_TradeSize(X, CAT1)                 \
  TRADESIZE_ROWS(X, CAT1, bid, "B", "买", 4w, 4)     \
  TRADESIZE_ROWS(X, CAT1, bid, "B", "买", 20w, 20)   \
  TRADESIZE_ROWS(X, CAT1, bid, "B", "买", 100w, 100) \
  TRADESIZE_ROWS(X, CAT1, ask, "A", "卖", 4w, 4)     \
  TRADESIZE_ROWS(X, CAT1, ask, "A", "卖", 20w, 20)   \
  TRADESIZE_ROWS(X, CAT1, ask, "A", "卖", 100w, 100) \
  TRADESIZE_DLOGP_ROW(X, CAT1, 4w, 4)                \
  TRADESIZE_DLOGP_ROW(X, CAT1, 20w, 20)              \
  TRADESIZE_DLOGP_ROW(X, CAT1, 100w, 100)

// q 轴: 一侧 × 一分位 (q token = p50..., Q = 百分数字面) 的 额/量/笔 3 行
#define TRADESIZEQ_ROWS(X, CAT1, side, S, CN, q, Q)                                                                                                                                                                                                                                                      \
  X(tk_##side##_ge_##q##_amt, CAT1, RAW, "Taker " S " >= P" #Q " Amount", "主动" CN "≥P" #Q "成交额", "分钟内单笔成交额≥前5日P" #Q "分位的主动" CN "成交额(元)", R"(\sum_{\Delta t} P|O^{T,)" S R"(}| \mathbf{1}[P|O| \geq Q_{)" #Q R"(\%}^{5d}])", OP(TradeSizeQ, tk_##side##_ge_##q##_amt, Log, None)) \
  X(tk_##side##_ge_##q##_vol, CAT1, RAW, "Taker " S " >= P" #Q " Volume", "主动" CN "≥P" #Q "成交量", "分钟内单笔成交额≥前5日P" #Q "分位的主动" CN "成交量(股)", R"(\sum_{\Delta t} |O^{T,)" S R"(}| \mathbf{1}[P|O| \geq Q_{)" #Q R"(\%}^{5d}])", OP(TradeSizeQ, tk_##side##_ge_##q##_vol, Log, None))  \
  X(tk_##side##_ge_##q##_n, CAT1, RAW, "Taker " S " >= P" #Q " Count", "主动" CN "≥P" #Q "成交笔数", "分钟内单笔成交额≥前5日P" #Q "分位的主动" CN "成交笔数", R"(\#O_{\Delta t}^{T,)" S R"(} \mathbf{1}[P|O| \geq Q_{)" #Q R"(\%}^{5d}])", OP(TradeSizeQ, tk_##side##_ge_##q##_n, Log, None))

#define TRADESIZEQ_DLOGP_ROW(X, CAT1, q, Q) \
  X(tk_ge_##q##_dlogp, CAT1, RAW, "Big Trade Log-Price Change >= P" #Q, "≥P" #Q "成交价变动和", "分钟内单笔成交额≥前5日P" #Q "分位的成交对上一笔成交价的对数变动之和(基点)", R"(\sum_{i \in \Delta t} 10^4 \ln\frac{p_i}{p_{i-1}} \mathbf{1}[p_i v_i \geq Q_{)" #Q R"(\%}^{5d}])", OP(TradeSizeQ, tk_ge_##q##_dlogp, None, None))

#define FIELDS_L1_TradeSizeQ(X, CAT1)               \
  TRADESIZEQ_ROWS(X, CAT1, bid, "B", "买", p50, 50) \
  TRADESIZEQ_ROWS(X, CAT1, bid, "B", "买", p80, 80) \
  TRADESIZEQ_ROWS(X, CAT1, bid, "B", "买", p84, 84) \
  TRADESIZEQ_ROWS(X, CAT1, bid, "B", "买", p93, 93) \
  TRADESIZEQ_ROWS(X, CAT1, bid, "B", "买", p95, 95) \
  TRADESIZEQ_ROWS(X, CAT1, bid, "B", "买", p98, 98) \
  TRADESIZEQ_ROWS(X, CAT1, ask, "A", "卖", p50, 50) \
  TRADESIZEQ_ROWS(X, CAT1, ask, "A", "卖", p80, 80) \
  TRADESIZEQ_ROWS(X, CAT1, ask, "A", "卖", p84, 84) \
  TRADESIZEQ_ROWS(X, CAT1, ask, "A", "卖", p93, 93) \
  TRADESIZEQ_ROWS(X, CAT1, ask, "A", "卖", p95, 95) \
  TRADESIZEQ_ROWS(X, CAT1, ask, "A", "卖", p98, 98) \
  TRADESIZEQ_DLOGP_ROW(X, CAT1, p50, 50)            \
  TRADESIZEQ_DLOGP_ROW(X, CAT1, p80, 80)            \
  TRADESIZEQ_DLOGP_ROW(X, CAT1, p84, 84)            \
  TRADESIZEQ_DLOGP_ROW(X, CAT1, p93, 93)            \
  TRADESIZEQ_DLOGP_ROW(X, CAT1, p95, 95)            \
  TRADESIZEQ_DLOGP_ROW(X, CAT1, p98, 98)
