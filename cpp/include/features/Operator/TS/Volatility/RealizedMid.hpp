#pragma once

// =============================================================================
// RealizedMid - 盘口更新中间价变化率的分钟幂和 (compute=onDepth, flush=onMinute; feature_list.md 1.0 智臾 中间价变化率族)
// =============================================================================
//   每次有效盘口更新 (两侧一档皆有价, 与 Book 同判) 一个 r_j = 1e4·ln(mid_j / mid_{j-1}) (基点), 分钟内累计:
//     {rv, rv_up, rv_dn, rm3, rm4, r_max, r_min}_mid   口序 = RetMoments (math/distribution/RetMoments.hpp), 与 Realized 的 Δ 网格族对仗
//   事件型 (非网格): 无 bpv / tpv / 大跳跃 (那三项依赖等距格). 无盘口更新的分钟 → 全 0 = 无变动 (不延续: 延续会凭空造出波动).
//   mid_prev 跨分钟连续, 跨日清零 (除权会造假跳变).
//   fp16 落盘: 幂和 Log Tf; 极值原值.
// =============================================================================

#include "features/DataDefine.hpp"
#include "math/distribution/RetMoments.hpp"
#include <cmath>

class RealizedMid {
  static constexpr float kBp = 1e4f;

public:
  enum Out : size_t { rv_mid,
                      rv_up_mid,
                      rv_dn_mid,
                      rm3_mid,
                      rm4_mid,
                      r_max_mid,
                      r_min_mid,
                      kCount };
  static_assert(size_t{kCount} == size_t{RetMoments::kCount});
  float y[kCount] = {};

  RealizedMid(const Series &bid_price_0, const Series &ask_price_0, const Series &mid_price)
      : bp_(bid_price_0), ap_(ask_price_0), mid_(mid_price) {}

  inline void compute() {
    if (bp_.back() <= 0.0f || ap_.back() <= 0.0f) [[unlikely]]
      return; // 一侧为空: 中间价无定义, 跳过 (同 Book)
    const float mid = mid_.back();
    if (mid_prev_ > 0.0f)
      mom_.add(kBp * std::log(mid / mid_prev_));
    mid_prev_ = mid;
  }

  inline void flush() {
    mom_.write(y);
    mom_.clear();
  }

  void reset() {
    mid_prev_ = 0.0f;
    mom_.clear();
  }

private:
  const Series &bp_, &ap_, &mid_;
  float mid_prev_ = 0.0f;
  RetMoments mom_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_RealizedMid(N) N(RealizedMid, (RealizedMid), (Depth.bid_price[0], Depth.ask_price[0], MidPrice.out()), onDepth, onMinute)

#define FIELDS_L1_RealizedMid(X, CAT1)                                                                                                                                                                                                              \
  X(rv_mid, CAT1, AUTO, "Mid-Price Realized Variance", "中间价变化率平方和", "分钟内逐次盘口更新中间价对数变化率(基点)平方和", R"(\sum_{j \in \Delta t} r_j^2,\; r_j = 10^4 \ln\frac{P_{mid,j}}{P_{mid,j-1}})", OP(RealizedMid, rv_mid, Log, None)) \
  X(rv_up_mid, CAT1, AUTO, "Mid-Price Upside Variance", "中间价上行变化率平方和", "分钟内中间价正变化率平方和(基点²)", R"(\sum_{j \in \Delta t} r_j^2 \mathbf{1}[r_j>0])", OP(RealizedMid, rv_up_mid, Log, None))                                   \
  X(rv_dn_mid, CAT1, AUTO, "Mid-Price Downside Variance", "中间价下行变化率平方和", "分钟内中间价负变化率平方和(基点²)", R"(\sum_{j \in \Delta t} r_j^2 \mathbf{1}[r_j<0])", OP(RealizedMid, rv_dn_mid, Log, None))                                 \
  X(rm3_mid, CAT1, AUTO, "Mid-Price Third Moment", "中间价变化率立方和", "分钟内中间价对数变化率(基点)立方和; 偏度=rm3/rv^1.5", R"(\sum_{j \in \Delta t} r_j^3)", OP(RealizedMid, rm3_mid, Log, None))                                              \
  X(rm4_mid, CAT1, AUTO, "Mid-Price Fourth Moment", "中间价变化率四次方和", "分钟内中间价对数变化率(基点)四次方和; 峰度=rm4/rv²", R"(\sum_{j \in \Delta t} r_j^4)", OP(RealizedMid, rm4_mid, Log, None))                                            \
  X(r_max_mid, CAT1, AUTO, "Mid-Price Max Change", "中间价变化率最大值", "分钟内中间价对数变化率(基点)最大值(无更新→0)", R"(\max_{j \in \Delta t} r_j)", OP(RealizedMid, r_max_mid, None, None))                                                    \
  X(r_min_mid, CAT1, AUTO, "Mid-Price Min Change", "中间价变化率最小值", "分钟内中间价对数变化率(基点)最小值(无更新→0)", R"(\min_{j \in \Delta t} r_j)", OP(RealizedMid, r_min_mid, None, None))
