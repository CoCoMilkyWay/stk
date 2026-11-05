#pragma once

// =============================================================================
// MovingAverage - 均线偏离 / 均线高低分位 (compute=on5Min; Bar 档 5 分钟箱, 回看窗自持, 跨天进 snap)
// =============================================================================
//   P_k = 5 分钟箱末后复权价 (CloseAdj5min), 一日 B = 49 箱 (含首箱竞价段), N 日 = N·B 箱, N ∈ {5, 25, 100}:
//     MA^N_k      = (1 / N·B) Σ_{j<N·B} P_{k−j}
//     ma_dev_{N}d = P_k / MA^N_k − 1
//     hl_pos_{N}d = (P_k − L^N) / (H^N − L^N),  H^N / L^N = 近 N 个交易日 (前 N−1 个整日 + 今日至本箱) 的 MA^5 最高 / 最低
//                   (高低取在 5 日均线上, 不受单箱插针影响; 现价可越出 [0, 1]; H − L ≤ ε → NaN)
//   停牌 / 无成交日照常占 B 格 (携带价, 同 Momentum); 窗内有非有限价 (上市前 / PIT 缺) → NaN, 窗满且全有限才出值.
//   均线和 = double 增量 (进出各一次); 高低按日分块: 整日 MA^5 高低进日环, 盘前 (reset) 折出前 N−1 日段, 盘中只并今日.
//   本 TU (bar_pipeline.cpp) precise-math: 有限性用 isfinite 判.
// =============================================================================

#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <limits>

class MovingAverage {
  static constexpr size_t kBins = bins_per_day(5);
  static constexpr size_t kDays[] = {5, 25, 100};
  static constexpr size_t kWin = sizeof(kDays) / sizeof(kDays[0]);
  static constexpr size_t kMaxDays = kDays[kWin - 1];
  static constexpr float kInf = std::numeric_limits<float>::infinity();

public:
  enum Out : size_t { ma_dev_5d,
                      ma_dev_25d,
                      ma_dev_100d,
                      hl_pos_5d,
                      hl_pos_25d,
                      hl_pos_100d,
                      kCount };
  float y[kCount] = {};

  explicit MovingAverage(const Port &px) : px_(px) {}

  inline void compute() {
    const float p = px_;
    for (size_t w = 0; w < kWin; ++w) {
      const size_t n = kDays[w] * kBins;
      if (px_win_.size() >= n)
        drop(w, px_win_[px_win_.size() - n]);
      add(w, p);
    }
    px_win_.push_back(p);

    float ma[kWin];
    for (size_t w = 0; w < kWin; ++w) {
      const size_t n = kDays[w] * kBins;
      ma[w] = nfin_[w] == n ? static_cast<float>(sum_[w] / static_cast<double>(n)) : kNaN;
      y[ma_dev_5d + w] = p / ma[w] - 1.0f;
    }

    const float m5 = ma[0];
    if (!std::isfinite(m5))
      day_ok_ = false;
    else if (bins_ == 0) {
      day_hi_ = m5;
      day_lo_ = m5;
    } else {
      day_hi_ = std::max(day_hi_, m5);
      day_lo_ = std::min(day_lo_, m5);
    }
    ++bins_;
    for (size_t w = 0; w < kWin; ++w) {
      const float h = std::max(past_hi_[w], day_hi_), l = std::min(past_lo_[w], day_lo_);
      y[hl_pos_5d + w] = day_ok_ && past_ok_[w] && h - l > kPxEps ? (p - l) / (h - l) : kNaN;
    }
  }

  // 跨日: 上一交易日整日 MA^5 高低进日环 (库首日之前无), 再折出各窗的前 N−1 日段
  inline void reset() {
    assert((bins_ == 0 || bins_ == kBins) && "MovingAverage: 上一交易日箱数不足一整日");
    if (bins_ == kBins) {
      hi_ring_.push_back(day_ok_ ? day_hi_ : kNaN);
      lo_ring_.push_back(day_ok_ ? day_lo_ : kNaN);
    }
    bins_ = 0;
    day_ok_ = true;
    for (size_t w = 0; w < kWin; ++w) {
      const size_t need = kDays[w] - 1;
      past_ok_[w] = hi_ring_.size() >= need;
      past_hi_[w] = -kInf;
      past_lo_[w] = kInf;
      for (size_t j = hi_ring_.size() - std::min(need, hi_ring_.size()); j < hi_ring_.size(); ++j) {
        past_ok_[w] = past_ok_[w] && std::isfinite(hi_ring_[j]);
        past_hi_[w] = std::max(past_hi_[w], hi_ring_[j]);
        past_lo_[w] = std::min(past_lo_[w], lo_ring_[j]);
      }
    }
  }

  template <class Ar>
  void snap(Ar &ar) {
    ar(px_win_);
    ar(sum_);
    ar(nfin_);
    ar(hi_ring_);
    ar(lo_ring_);
    ar(day_hi_);
    ar(day_lo_);
    ar(day_ok_);
    ar(bins_);
  }

private:
  inline void add(size_t w, float x) {
    if (std::isfinite(x)) {
      sum_[w] += x;
      ++nfin_[w];
    }
  }
  inline void drop(size_t w, float x) {
    if (std::isfinite(x)) {
      sum_[w] -= x;
      --nfin_[w];
    }
  }

  const Port &px_;                                // CloseAdj5min.value (同域 on5Min, 拓扑序在前, 即本箱值)
  CBuffer<float, kMaxDays * kBins> px_win_;       // 最近 100·B 箱的 P (back() = 本箱)
  double sum_[kWin] = {};                         // 各窗有限 P 之和
  size_t nfin_[kWin] = {};                        // 各窗有限 P 个数 (= 窗长才出均线)
  CBuffer<float, kMaxDays - 1> hi_ring_;          // 已收整日的 MA^5 最高 (该日有非有限 MA^5 → NaN)
  CBuffer<float, kMaxDays - 1> lo_ring_;          // 同上, 最低
  float day_hi_ = 0.0f, day_lo_ = 0.0f;           // 今日至本箱 MA^5 最高 / 最低
  bool day_ok_ = true;                            // 今日至本箱 MA^5 全有限
  size_t bins_ = 0;                               // 今日已收箱数
  float past_hi_[kWin] = {}, past_lo_[kWin] = {}; // 前 N−1 日段高低 (reset 由日环折出)
  bool past_ok_[kWin] = {};                       // 前 N−1 日段齐且全有限
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_MovingAverage(N) N(MovingAverage, (MovingAverage), (CloseAdj5min.out(CloseAdj5min.value)), on5Min)

#define MA_ROWS(X, CAT1, D)                                                                                                                                                                                                      \
  X(ma_dev_##D##d, CAT1, AUTO, "MA Deviation " #D "d", #D "日均线偏离", "5分钟箱末后复权价相对" #D "日均线(" #D "×49箱)的偏离", R"(\frac{P_k}{\mathrm{MA}^{()" #D R"(d)}_k} - 1)", TS(MovingAverage, ma_dev_##D##d, None, None)) \
  X(hl_pos_##D##d, CAT1, AUTO, "MA5 High-Low Position " #D "d", #D "日高低分位", "现价在近" #D "日5日均线最高/最低区间的位置(高低取在均线上防插针; 区间≤ε→NaN)", R"(\frac{P_k - \min_{()" #D R"(d)} \mathrm{MA}^{(5d)}}{\max_{()" #D R"(d)} \mathrm{MA}^{(5d)} - \min_{()" #D R"(d)} \mathrm{MA}^{(5d)}})", TS(MovingAverage, hl_pos_##D##d, None, None))

#define FIELDS_L_5min_MovingAverage(X, CAT1) \
  MA_ROWS(X, CAT1, 5)                        \
  MA_ROWS(X, CAT1, 25)                       \
  MA_ROWS(X, CAT1, 100)
