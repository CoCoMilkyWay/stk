#pragma once

// =============================================================================
// SizeThresholds - 大小单分档阈值: 固定金额 B 轴 + 本股前 N_DAYS 日分位 q 轴, 合并升序一次分桶
// =============================================================================
//   阈值 NB = NFIX + NQ 个, 输出槽序固定 (前 NFIX 固定, 后 NQ 分位); 当日生效阈值合并升序, slot(k) 记升序第 k 个的输出槽.
//   每笔 bucket(a) = 满足 "a ≥ 阈值" 的个数 0..n(); "≥ 第 k 个阈值" ⇔ 桶 ≥ k+1, 消费方 flush 时按桶做后缀和 / 分派.
//   分位: 每日一只 KLL (log a), roll() 归档 + 取历史各日分位均值 → 首日无历史, n() = NFIX, q 轴槽消费方归 0.
//   用法: add(a) 逐样本 (a > 0), flush_batch() 每分钟, roll() 每日 reset. 使用方: TradeSize (单笔成交额), OrderQuad (委托申报额).
// =============================================================================

#include "math/distribution/DailyQuantile.hpp"
#include <cmath>
#include <cstddef>
#include <utility>

template <size_t NFIX, size_t NQ, size_t N_DAYS>
class SizeThresholds {
public:
  static constexpr size_t NB = NFIX + NQ;

  SizeThresholds(const float (&fix)[NFIX], const float (&probs)[NQ]) : dq_(probs) {
    for (size_t k = 0; k < NFIX; ++k)
      fix_[k] = fix[k];
    rebuild();
  }

  inline void add(float a) { dq_.add(std::log(a)); }
  inline void flush_batch() { dq_.flush_batch(); }

  // 跨日: 取前 N_DAYS 日分位阈值, 与固定阈值合并升序
  void roll() {
    float lq[NQ];
    has_q_ = dq_.roll(lq);
    if (has_q_)
      for (size_t j = 0; j < NQ; ++j)
        thr_q_[j] = std::exp(lq[j]);
    rebuild();
  }

  inline size_t bucket(float a) const {
    size_t b = 0;
    while (b < n_ && a >= thr_[b])
      ++b;
    return b;
  }
  size_t n() const { return n_; }
  size_t slot(size_t k) const { return slot_[k]; }
  bool has_q() const { return has_q_; }

private:
  // 固定 + 分位 阈值合并升序 (插入排序, NB 个), slot_ 记每个阈值的输出槽 (0..NFIX-1 固定, NFIX.. 分位)
  void rebuild() {
    n_ = NFIX + (has_q_ ? NQ : 0);
    for (size_t k = 0; k < n_; ++k)
      thr_[k] = k < NFIX ? fix_[k] : thr_q_[k - NFIX], slot_[k] = k;
    for (size_t k = 1; k < n_; ++k)
      for (size_t m = k; m > 0 && thr_[m] < thr_[m - 1]; --m)
        std::swap(thr_[m], thr_[m - 1]), std::swap(slot_[m], slot_[m - 1]);
  }

  DailyQuantile<NQ, N_DAYS> dq_;
  float fix_[NFIX] = {}; // 元, 升序
  float thr_q_[NQ] = {}; // 元, 升序 (分位单调)
  bool has_q_ = false;
  float thr_[NB] = {};   // 当日生效阈值, 升序
  size_t slot_[NB] = {}; // thr_[k] 对应的输出槽
  size_t n_ = 0;
};
