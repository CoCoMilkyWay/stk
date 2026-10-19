#pragma once

// =============================================================================
// Kline - 分钟 K 线: 开高低收 (元) (compute=onMinute; feature_list.md 1.1)
// =============================================================================
//   透传 MinuteData 的分钟聚合 (Lob: ResamplerTick2Min 逐笔聚合; Bar: CoreBar 喂 bar1m), 走节点表统一写回.
//   无成交分钟 O=H=L=C=日内最近成交价 (两档喂入端都携带); 日内尚无成交 (bar 价 0) → 前收 (Fund PIT, 除权后):
//   K 线标准携带约定, 振幅 / 实体 = 0. 价类下游 (Valuation 等) 读本节点 close 口, 不直接读 MinuteData.
//   对数派生不单列 (因子层做, 同行 / 相邻行纯函数): 收益 ln(C_t / C_{t-1}) (日内首分钟基准 = Fund pre_close), 振幅 ln(H/L), 实体 ln(C/O).
//   两档共用节点类; 节点实例 / 落盘列见 Node/{Lob,Bar}/TS/Basic/Kline.hpp (Lob 列名 lob_*, 与 Bar 档的 bar1m K 线区分).
//   KlineT<BinData> = 分钟箱 / 日 K 线 (compute=on<n>Min / onClose, 同一携带约定; 读 min<n>_data / day_data); Kline = 分钟版.
// =============================================================================

#include "features/DataDefine.hpp"

template <class Bars>
class KlineT {
public:
  enum Out : size_t { open,
                      high,
                      low,
                      close,
                      kCount };
  float y[kCount] = {};

  KlineT(const Bars &md, const Series &pre_close) : md_(md), pre_close_(pre_close) {}

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
  const Bars &md_;
  const Series &pre_close_; // Fund 口 (onMinute 域, 拓扑序在前 / 行末 onMinute 先于 on<n>Min / onClose, back() 即当日值)
};
using Kline = KlineT<MinuteData>;
