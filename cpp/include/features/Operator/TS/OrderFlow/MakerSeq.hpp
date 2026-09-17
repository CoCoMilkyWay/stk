#pragma once

// =============================================================================
// MakerSeq - 委托方向序列的分钟统计 (compute=onMaker, flush=onMinute; feature_list.md 1.5 中不依赖委托号的两项)
// =============================================================================
//   d_τ = +1 (买委托) / −1 (卖委托), 按到达序.
//     dir_acf_maker_k = (1/n_k) Σ_{τ ∈ Δt} d_τ d_{τ−k},  k ∈ {1, 5, 20}   (分钟内配对, 滞后项可跨分钟; n_k = 0 → NaN)
//     runlen_maker_{bid,ask} = 分钟内已结束的连续同向委托段的平均段长 (无已结束段 → NaN; 跨分钟的段归其结束分钟)
//   开源 029: 高维记忆 MEMO / 长期记忆强度 LMS / 分拆痕迹 OST / 连续买入·卖出笔数.
//   委托号相关项 (成交用时 / 撤单用时 / 广义市价 / 委托维度大小单) 需 LOB 暴露委托记账, 另议.
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include <cstdint>

class MakerSeq {
  static constexpr size_t NK = 3;
  static constexpr size_t LAGS[NK] = {1, 5, 20};
  static constexpr size_t RING = 32; // ≥ max lag + 1, 2 的幂

public:
  enum Out : size_t { dir_acf_maker_1,
                      dir_acf_maker_5,
                      dir_acf_maker_20,
                      runlen_maker_bid,
                      runlen_maker_ask,
                      kCount };
  float y[kCount] = {};

  explicit MakerSeq(const TickData &td) : td_(td) {}

  inline void compute() {
    const int8_t d = td_.lob.order_dir == L2::OrderDirection::BID ? 1 : -1;
    for (size_t j = 0; j < NK; ++j) {
      const size_t k = LAGS[j];
      if (n_seen_ >= k) {
        s_[j] += static_cast<float>(d * ring_[(head_ - k) & (RING - 1)]);
        n_[j] += 1.0f;
      }
    }
    ring_[head_ & (RING - 1)] = d;
    ++head_;
    ++n_seen_;

    if (run_len_ == 0) [[unlikely]] {
      run_dir_ = d, run_len_ = 1;
    } else if (d == run_dir_) {
      ++run_len_;
    } else {
      const size_t s = run_dir_ > 0 ? 0 : 1;
      run_sum_[s] += static_cast<float>(run_len_), run_n_[s] += 1.0f;
      run_dir_ = d, run_len_ = 1;
    }
  }

  inline void flush() {
    for (size_t j = 0; j < NK; ++j) {
      y[dir_acf_maker_1 + j] = n_[j] > 0.0f ? s_[j] / n_[j] : kNaN;
      s_[j] = n_[j] = 0.0f;
    }
    y[runlen_maker_bid] = run_n_[0] > 0.0f ? run_sum_[0] / run_n_[0] : kNaN;
    y[runlen_maker_ask] = run_n_[1] > 0.0f ? run_sum_[1] / run_n_[1] : kNaN;
    run_sum_[0] = run_sum_[1] = run_n_[0] = run_n_[1] = 0.0f;
  }

  void reset() {
    for (size_t j = 0; j < NK; ++j)
      s_[j] = n_[j] = 0.0f;
    head_ = n_seen_ = 0;
    run_dir_ = 0, run_len_ = 0;
    run_sum_[0] = run_sum_[1] = run_n_[0] = run_n_[1] = 0.0f;
  }

private:
  const TickData &td_;
  int8_t ring_[RING] = {};
  size_t head_ = 0, n_seen_ = 0;
  float s_[NK] = {}, n_[NK] = {};
  int8_t run_dir_ = 0;
  uint32_t run_len_ = 0;
  float run_sum_[2] = {}, run_n_[2] = {};
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_MakerSeq(N) N(MakerSeq, (MakerSeq), (tick_data), onMaker, onMinute)

#define MAKERSEQ_ACF_ROW(X, CAT1, k) \
  X(dir_acf_maker_##k, CAT1, AUTO, "Order Direction ACF lag " #k, "委托方向自相关lag" #k, "分钟内委托方向(±1)序列的lag-" #k "自相关(滞后项可跨分钟; 无配对→NaN)", R"(\frac{1}{n}\sum_{\tau \in \Delta t} d_\tau d_{\tau-)" #k R"(},\; d_\tau = \pm 1)", OP(MakerSeq, dir_acf_maker_##k, None, None))

#define FIELDS_L1_MakerSeq(X, CAT1)                                                                                                                                                                                    \
  MAKERSEQ_ACF_ROW(X, CAT1, 1)                                                                                                                                                                                         \
  MAKERSEQ_ACF_ROW(X, CAT1, 5)                                                                                                                                                                                         \
  MAKERSEQ_ACF_ROW(X, CAT1, 20)                                                                                                                                                                                        \
  X(runlen_maker_bid, CAT1, AUTO, "Bid Order Run Length", "连续买委托平均段长", "分钟内结束的连续买方委托段的平均笔数(无→NaN)", R"(\overline{\mathrm{run}}(d_\tau = +1))", OP(MakerSeq, runlen_maker_bid, None, None)) \
  X(runlen_maker_ask, CAT1, AUTO, "Ask Order Run Length", "连续卖委托平均段长", "分钟内结束的连续卖方委托段的平均笔数(无→NaN)", R"(\overline{\mathrm{run}}(d_\tau = -1))", OP(MakerSeq, runlen_maker_ask, None, None))
