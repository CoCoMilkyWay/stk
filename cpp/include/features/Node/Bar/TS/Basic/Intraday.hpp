#pragma once

// =============================================================================
// Intraday - 日内位置与短窗收益 (compute=onMinute; Bar 档 bar1m, 日内节点)
// =============================================================================
//   P_t = Kline close (携带约定: 日内尚无成交 = 前收); O = 当日首个有成交分钟的开盘价; H / L = 当日至本分钟有成交分钟的最高 / 最低价:
//     ret_lag{k}     = P_t / P_{t−k} − 1, k ∈ {5, 15, 30} 分钟 (L_1min 行; t − k 落到日前 → 基准 = 前收, 与 Kline 携带约定一致)
//     ret_open       = P_t / O − 1
//     gap_open       = O / P^pre − 1        (隔夜缺口, 开盘后日内恒定)
//     pos_intra      = (P_t − L) / (H − L)  (H − L ≤ ε → NaN: 一字板 / 全日单价)
//     rebound_intra  = P_t / L − 1
//     pullback_intra = P_t / H − 1
//   当日首笔成交前 O / H / L 无定义 → 后五列 NaN; 前收 PIT 缺失 (上市前) → NaN 经算术透传.
// =============================================================================

#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

#include <algorithm>
#include <cstddef>

class Intraday {
  static constexpr size_t kLags[] = {5, 15, 30};
  static constexpr size_t kLagCount = sizeof(kLags) / sizeof(kLags[0]);

public:
  enum Out : size_t { ret_lag5,
                      ret_lag15,
                      ret_lag30,
                      ret_open,
                      gap_open,
                      pos_intra,
                      rebound_intra,
                      pullback_intra,
                      kCount };
  float y[kCount] = {};

  Intraday(const MinuteData &md, const Port &close, const Port &pre_close) : md_(md), close_(close), pre_close_(pre_close) {}

  inline void compute() {
    const float p = close_;
    for (size_t i = 0; i < kLagCount; ++i) {
      const size_t k = kLags[i];
      const float base = win_.size() >= k ? win_[win_.size() - k] : pre_close_;
      y[ret_lag5 + i] = p / base - 1.0f;
    }
    win_.push_back(p);

    if (md_.traded()) {
      if (!opened_) {
        o_ = md_.open;
        h_ = md_.high;
        l_ = md_.low;
        opened_ = true;
      } else {
        h_ = std::max(h_, md_.high);
        l_ = std::min(l_, md_.low);
      }
    }
    if (!opened_) {
      for (size_t i = ret_open; i < kCount; ++i)
        y[i] = kNaN;
      return;
    }
    y[ret_open] = p / o_ - 1.0f;
    y[gap_open] = o_ / pre_close_ - 1.0f;
    y[pos_intra] = h_ - l_ > kPxEps ? (p - l_) / (h_ - l_) : kNaN;
    y[rebound_intra] = p / l_ - 1.0f;
    y[pullback_intra] = p / h_ - 1.0f;
  }

  inline void reset() {
    win_.clear();
    opened_ = false;
  }

private:
  const MinuteData &md_;
  const Port &close_;                        // Kline close (同域, 拓扑序在前, 即本分钟值)
  const Port &pre_close_;                    // Fund 日频口 (盘中 = 当日前收)
  CBuffer<float, kLags[kLagCount - 1]> win_; // 当日已过分钟的 P (不含本分钟): [size() − k] = P_{t−k}
  float o_ = 0.0f, h_ = 0.0f, l_ = 0.0f;
  bool opened_ = false; // 当日已有成交
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Intraday(N) N(Intraday, (Intraday), (minute_data, Kline.out(Kline.close), Fund.out(Fund.pre_close)), onMinute)

#define INTRADAY_LAG_ROW(X, CAT1, k) \
  X(ret_lag##k, CAT1, AUTO, "Return Lag " #k "m", #k "分钟收益率", "现价相对" #k "分钟前收盘价涨跌幅(早于当日首行的基准=前收)", R"(\frac{P_t}{P_{t-)" #k R"(}} - 1)", TS(Intraday, ret_lag##k, None, None))

#define FIELDS_L_1min_Intraday(X, CAT1)                                                                                                                                                                                                                                              \
  INTRADAY_LAG_ROW(X, CAT1, 5)                                                                                                                                                                                                                                                       \
  INTRADAY_LAG_ROW(X, CAT1, 15)                                                                                                                                                                                                                                                      \
  INTRADAY_LAG_ROW(X, CAT1, 30)                                                                                                                                                                                                                                                      \
  X(ret_open, CAT1, AUTO, "Return Since Open", "开盘至今收益率", "现价相对当日开盘价涨跌幅(首笔成交前NaN)", R"(\frac{P_t}{P^{\mathrm{open}}_D} - 1)", TS(Intraday, ret_open, None, None))                                                                                            \
  X(gap_open, CAT1, AUTO, "Overnight Gap", "隔夜缺口", "当日开盘价相对前收涨跌幅(首笔成交前NaN)", R"(\frac{P^{\mathrm{open}}_D}{P^{pre}_D} - 1)", TS(Intraday, gap_open, None, None))                                                                                                \
  X(pos_intra, CAT1, AUTO, "Intraday Position", "日内分位", "现价在当日至今高低区间的位置(区间≤ε或首笔成交前NaN)", R"(\frac{P_t - \min_{s \le t} P^{\mathrm{low}}_s}{\max_{s \le t} P^{\mathrm{high}}_s - \min_{s \le t} P^{\mathrm{low}}_s})", TS(Intraday, pos_intra, None, None)) \
  X(rebound_intra, CAT1, AUTO, "Intraday Rebound", "日内反弹", "现价相对当日至今最低价涨幅(首笔成交前NaN)", R"(\frac{P_t}{\min_{s \le t} P^{\mathrm{low}}_s} - 1)", TS(Intraday, rebound_intra, None, None))                                                                         \
  X(pullback_intra, CAT1, AUTO, "Intraday Pullback", "日内回落", "现价相对当日至今最高价跌幅(首笔成交前NaN)", R"(\frac{P_t}{\max_{s \le t} P^{\mathrm{high}}_s} - 1)", TS(Intraday, pullback_intra, None, None))
