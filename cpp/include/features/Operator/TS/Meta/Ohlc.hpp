#pragma once

// =============================================================================
// Ohlc - 分钟 K 线 (GUI / 截面源列): 开高低收 (元) + 成交量 (股)
// =============================================================================
//   直接透传 MinuteData 的分钟聚合 (ResamplerTick2Min 填充), 走节点表统一写回.
// =============================================================================

#include "features/DataDefine.hpp"

class Ohlc {
public:
  enum Out : size_t { open,
                      high,
                      low,
                      close,
                      volume,
                      kCount };
  float y[kCount] = {};

  explicit Ohlc(const MinuteData &md) : md_(md) {}

  inline void compute() {
    y[open] = md_.open.back();
    y[high] = md_.high.back();
    y[low] = md_.low.back();
    y[close] = md_.close.back();
    y[volume] = static_cast<float>(md_.bid_volume.back() + md_.ask_volume.back());
  }

private:
  const MinuteData &md_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Ohlc(N) N(Ohlc, (Ohlc), (minute_data), onMinute)

#define FIELDS_L1_Ohlc(X, CAT1)                                                                        \
  X(_ohlc_open, CAT1, RAW, NONE, "OHLC Open", "开盘价", "分钟开盘价(元)", R"(O_t)", OP(Ohlc, open))    \
  X(_ohlc_high, CAT1, RAW, NONE, "OHLC High", "最高价", "分钟最高价(元)", R"(H_t)", OP(Ohlc, high))    \
  X(_ohlc_low, CAT1, RAW, NONE, "OHLC Low", "最低价", "分钟最低价(元)", R"(L_t)", OP(Ohlc, low))       \
  X(_ohlc_close, CAT1, RAW, NONE, "OHLC Close", "收盘价", "分钟收盘价(元)", R"(C_t)", OP(Ohlc, close)) \
  X(_ohlc_volume, CAT1, RAW, NONE, "OHLC Volume", "成交量", "分钟成交量(股)", R"(V_t)", OP(Ohlc, volume))
