#pragma once

// =============================================================================
// TakerRet - 逐笔成交对数收益 (compute=onTaker; 中间节点, 无落盘列)
// =============================================================================
//   dlogp = 1e4·ln(P_τ / P_{τ-1})  相对上一笔有效成交价 (基点)
//   有效成交 = 单笔额 > 0 (零价市价单 / 零量不算, 也不更新 P_{τ-1});
//   当日首笔 / 无效笔输出 0 —— 消费端都是求和 (Σ dlogp / Σ|dlogp|), 加 0 恒等价于跳过.
//   TradeSize (dlogp_taker_ge_*) 与 Realized (path_len) 共享此值: 每笔 taker 只算一次 log.
// =============================================================================

#include "features/DataDefine.hpp"
#include <cmath>

class TakerRet {
  static constexpr float kBp = 1e4f;

public:
  enum Out : size_t { dlogp,
                      kCount };
  float y[kCount] = {};

  explicit TakerRet(const TickData &td) : td_(td) {}

  inline void compute() {
    const float p = td_.lob.price;
    const float a = p * static_cast<float>(td_.lob.volume);
    if (a <= 0.0f) [[unlikely]] { // 零价 / 零量: 不是有效成交
      y[dlogp] = 0.0f;
      return;
    }
    y[dlogp] = p_prev_ > 0.0f ? kBp * std::log(p / p_prev_) : 0.0f;
    p_prev_ = p;
  }

  void reset() {
    p_prev_ = 0.0f;
    y[dlogp] = 0.0f;
  }

private:
  const TickData &td_;
  float p_prev_ = 0.0f; // 上一笔有效成交价 (元)
};

// ---- 节点实例 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_TakerRet(N) N(TakerRet, (TakerRet), (tick_data), onTaker)
