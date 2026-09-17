#pragma once

// =============================================================================
// Bar - 分钟 K 线: 开高低收 (元) + 对数派生 (基点) (compute=onMinute; feature_list.md 1.1)
// =============================================================================
//   透传 MinuteData 的分钟聚合 (ResamplerTick2Min 填充), 走节点表统一写回.
//     open / high / low / close   分钟 OHLC (元)
//     ret   = 1e4·ln(C_t / C_{t-1})  上一有效分钟收盘 (日内首分钟 NaN)
//     range = 1e4·ln(H_t / L_t)      振幅
//     body  = 1e4·ln(C_t / O_t)      实体
//   成交量 / 成交额 不在此: 与 Flow 的 vol_taker_bid + vol_taker_ask / amt_taker_* 完全同源同值, 因子层按行求和.
// =============================================================================

#include "features/DataDefine.hpp"
#include <cmath>

class Bar {
  static constexpr float kBp = 1e4f;

public:
  enum Out : size_t { open,
                      high,
                      low,
                      close,
                      ret,
                      range,
                      body,
                      kCount };
  float y[kCount] = {};

  explicit Bar(const MinuteData &md) : md_(md) {}

  inline void compute() {
    const float o = md_.open.back(), h = md_.high.back(), l = md_.low.back(), c = md_.close.back();
    y[open] = o;
    y[high] = h;
    y[low] = l;
    y[close] = c;
    y[ret] = prev_close_ > 0.0f ? kBp * std::log(c / prev_close_) : kNaN;
    y[range] = kBp * std::log(h / l);
    y[body] = kBp * std::log(c / o);
    prev_close_ = c;
  }

  void reset() { prev_close_ = 0.0f; }

private:
  const MinuteData &md_;
  float prev_close_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Bar(N) N(Bar, (Bar), (minute_data), onMinute)

#define FIELDS_L1_Bar(X, CAT1)                                                                                                                                                                                       \
  X(open, CAT1, AUTO, "Open", "开盘价", "分钟开盘价(元)", R"(P^{\mathrm{open}}_t)", OP(Bar, open, None, None))                                                                                                       \
  X(high, CAT1, AUTO, "High", "最高价", "分钟最高价(元)", R"(P^{\mathrm{high}}_t)", OP(Bar, high, None, None))                                                                                                       \
  X(low, CAT1, AUTO, "Low", "最低价", "分钟最低价(元)", R"(P^{\mathrm{low}}_t)", OP(Bar, low, None, None))                                                                                                           \
  X(close, CAT1, AUTO, "Close", "收盘价", "分钟收盘价(元)", R"(P^{\mathrm{close}}_t)", OP(Bar, close, None, None))                                                                                                   \
  X(ret, CAT1, AUTO, "Minute Log Return", "分钟对数收益", "收盘价对上一有效分钟收盘价的对数收益(基点; 日内首分钟NaN)", R"(10^4 \ln\frac{P^{\mathrm{close}}_t}{P^{\mathrm{close}}_{t-1}})", OP(Bar, ret, None, None)) \
  X(range, CAT1, AUTO, "Minute Log Range", "分钟对数振幅", "分钟最高/最低对数比(基点)", R"(10^4 \ln\frac{P^{\mathrm{high}}_t}{P^{\mathrm{low}}_t})", OP(Bar, range, None, None))                                     \
  X(body, CAT1, AUTO, "Minute Log Body", "分钟对数实体", "分钟收/开对数比(基点)", R"(10^4 \ln\frac{P^{\mathrm{close}}_t}{P^{\mathrm{open}}_t})", OP(Bar, body, None, None))
