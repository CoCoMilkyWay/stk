#pragma once

// =============================================================================
// Bar - 分钟 K 线: 开高低收 (元) (compute=onMinute; feature_list.md 1.1)
// =============================================================================
//   透传 MinuteData 的分钟聚合 (ResamplerTick2Min 填充), 走节点表统一写回.
//   无成交分钟 O=H=L=C=日内最近成交价 (Resampler 携带); 日内尚无成交 (bar 价 0) → 前收 (Fund PIT, 除权后):
//   K 线标准携带约定, 振幅 / 实体 = 0. 价类下游 (Valuation 等) 读本节点 close 口, 不直接读 MinuteData.
//   对数派生不单列 (因子层做, 同行 / 相邻行纯函数): 收益 ln(C_t / C_{t-1}) (日内首分钟基准 = Fund pre_close), 振幅 ln(H/L), 实体 ln(C/O).
//   成交量 / 成交额 不在此: 与 Flow 的 vol_taker_bid + vol_taker_ask / amt_taker_* 完全同源同值, 因子层按行求和.
// =============================================================================

#include "features/DataDefine.hpp"

class Bar {
public:
  enum Out : size_t { open,
                      high,
                      low,
                      close,
                      kCount };
  float y[kCount] = {};

  Bar(const MinuteData &md, const Series &pre_close) : md_(md), pre_close_(pre_close) {}

  inline void compute() {
    const float c = md_.close.back();
    if (c > 0.0f) [[likely]] {
      y[open] = md_.open.back();
      y[high] = md_.high.back();
      y[low] = md_.low.back();
      y[close] = c;
    } else { // 日内尚无成交: 四价 = 前收
      y[open] = y[high] = y[low] = y[close] = pre_close_.back();
    }
  }

private:
  const MinuteData &md_;
  const Series &pre_close_; // Fund 口 (同域 onMinute, 拓扑序在前, back() 即当日值)
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Bar(N) N(Bar, (Bar), (minute_data, Fund.out(Fund.pre_close)), onMinute)

#define FIELDS_L1_Bar(X, CAT1)                                                                                 \
  X(open, CAT1, AUTO, "Open", "开盘价", "分钟开盘价(元)", R"(P^{\mathrm{open}}_t)", OP(Bar, open, None, None)) \
  X(high, CAT1, AUTO, "High", "最高价", "分钟最高价(元)", R"(P^{\mathrm{high}}_t)", OP(Bar, high, None, None)) \
  X(low, CAT1, AUTO, "Low", "最低价", "分钟最低价(元)", R"(P^{\mathrm{low}}_t)", OP(Bar, low, None, None))     \
  X(close, CAT1, AUTO, "Close", "收盘价", "分钟收盘价(元)", R"(P^{\mathrm{close}}_t)", OP(Bar, close, None, None))
