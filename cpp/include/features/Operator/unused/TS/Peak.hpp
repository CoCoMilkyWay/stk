#pragma once

// =============================================================================
// PEAK - 前 5 档深度分布的峰值位置与集中度
// =============================================================================
//   loc   = argmax(V[1:5])              (1-indexed, 越大峰值离盘口越远)
//   ratio = max(V[1:5]) / mean(V[1:5])  (>=1, 越大越集中在单一档位)
//   一侧一个节点两口; IS_BID: true=买侧, false=卖侧
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"

template <bool IS_BID, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Peak {
  static constexpr size_t N_LEVELS = 5;

public:
  enum Out : size_t { loc,
                      ratio,
                      kCount };
  float y[kCount] = {};

  explicit Peak(const DepthSeries &qty) : qty_(qty) {}

  inline void compute() {
    float max_v = 0.0f, sum_v = 0.0f;
    size_t max_idx = 0;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      float v = IS_BID ? qty_[i].back() : -qty_[i].back(); // ask 存负值
      sum_v += v;
      if (v > max_v) {
        max_v = v;
        max_idx = i;
      }
    }
    float mean_v = sum_v / static_cast<float>(N_LEVELS);
    y[loc] = static_cast<float>(max_idx + 1);
    y[ratio] = mean_v > 1e-6f ? max_v / mean_v : 1.0f;
  }

private:
  const DepthSeries &qty_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Peak_ask(N) N(Peak_ask, (Peak<false>), (DepthData.ask_qty), onMinute)

#define FIELDS_L1_Peak_ask(X, CAT1)                                                                                                                                        \
  X(peak_loc_ask, CAT1, RAW, NONE, "Ask Peak Location", "卖侧峰位置", "卖侧深度最大值所在档位(前5档)(降频)", R"(\arg\max_{i \in [1,5]} V_{i,t}^{M,A})", OP(Peak_ask, loc)) \
  X(peak_ratio_ask, CAT1, RATIO, NONE, "Ask Peak Concentration", "卖侧峰集中度", "卖侧最大档位深度/平均档位深度(前5档)(降频)", R"(\frac{\max_{i \in [1,5]} V_{i,t}^{M,A}}{\frac{1}{5}\sum_{i=1}^{5} V_{i,t}^{M,A}})", OP(Peak_ask, ratio))

#define NODE_Peak_bid(N) N(Peak_bid, (Peak<true>), (DepthData.bid_qty), onMinute)

#define FIELDS_L1_Peak_bid(X, CAT1)                                                                                                                                        \
  X(peak_loc_bid, CAT1, RAW, NONE, "Bid Peak Location", "买侧峰位置", "买侧深度最大值所在档位(前5档)(降频)", R"(\arg\max_{i \in [1,5]} V_{i,t}^{M,B})", OP(Peak_bid, loc)) \
  X(peak_ratio_bid, CAT1, RATIO, NONE, "Bid Peak Concentration", "买侧峰集中度", "买侧最大档位深度/平均档位深度(前5档)(降频)", R"(\frac{\max_{i \in [1,5]} V_{i,t}^{M,B}}{\frac{1}{5}\sum_{i=1}^{5} V_{i,t}^{M,B}})", OP(Peak_bid, ratio))
