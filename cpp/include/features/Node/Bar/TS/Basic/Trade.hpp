#pragma once

// =============================================================================
// Trade - 分钟成交量 / 成交额 (compute=onMinute; Bar 档 bar1m)
// =============================================================================
//   bar1m 只有总量, 不分主动买卖: CoreBar 把总量整份放进 MinuteData 的 bid 侧, ask 侧恒 0 ——
//   与 Lob 档 "bid + ask = 分钟总成交" 的 MinuteData 约定一致, Tod 等读 bid + ask 的节点两档同式.
//   无成交分钟 = 0. 落盘 Log 压尾 (fp16 存不下单分钟成交额 1e9+).
// =============================================================================

#include "features/DataDefine.hpp"

class Trade {
public:
  enum Out : size_t { volume,
                      amount,
                      kCount };
  float y[kCount] = {};

  explicit Trade(const MinuteData &md) : md_(md) {}

  inline void compute() {
    y[volume] = static_cast<float>(md_.bid_volume.back() + md_.ask_volume.back());
    y[amount] = md_.bid_amount.back() + md_.ask_amount.back();
  }

private:
  const MinuteData &md_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Trade(N) N(Trade, (Trade), (minute_data), onMinute)

#define FIELDS_L1_Trade(X, CAT1)                                                                                 \
  X(volume, CAT1, AUTO, "Volume", "成交量", "分钟成交量(股)", R"(\mathrm{Vol}_t)", OP(Trade, volume, Log, None)) \
  X(amount, CAT1, AUTO, "Amount", "成交额", "分钟成交额(元); 分钟 VWAP = amount / volume (因子层)", R"(\mathrm{Amt}_t)", OP(Trade, amount, Log, None))
