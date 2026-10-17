#pragma once

// =============================================================================
// Ma - 10 分钟收盘价均线 (compute=on10Min; Bar 档 L_10min 样板节点, dummy)
// =============================================================================
//   MA_k = 最近 kWindow 根 10 分钟 bar (min10_data) 收盘价均值; 日内不足 kWindow 根 → 取已有的全部 (首箱 = 首箱收盘).
//   日内尚无成交 (箱 close 0 = 无价) → 前收 (Fund PIT, 除权后), 与 Kline 携带约定一致. min10_data 日末清空, 均线不跨日.
// =============================================================================

#include "features/DataDefine.hpp"

#include <algorithm>
#include <cassert>

class Ma {
public:
  static constexpr size_t kWindow = 5;
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  Ma(const Min10Data &md, const Series &pre_close) : md_(md), pre_close_(pre_close) {}

  inline void compute() {
    const size_t sz = md_.close.size();
    assert(sz > 0 && "on10Min: 本箱 bar 已出");
    const size_t n = std::min(kWindow, sz);
    const float pc = pre_close_.back();
    float s = 0.0f;
    for (size_t i = sz - n; i < sz; ++i) {
      const float c = md_.close[i];
      s += c > 0.0f ? c : pc;
    }
    y[value] = s / static_cast<float>(n);
  }

private:
  const Min10Data &md_;
  const Series &pre_close_; // Fund 口 (onMinute 域, 箱末 onMinute 先于 on10Min, back() 即当日值)
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Ma(N) N(Ma, (Ma), (min10_data, Fund.out(Fund.pre_close)), on10Min)

#define FIELDS_L_10min_Ma(X, CAT1) \
  X(ma5_10min, CAT1, AUTO, "MA5 (10min)", "10分钟均线MA5", "最近 5 根 10 分钟 K 收盘价均值(元); 日内不足 5 根取已有; dummy 样板列", R"(\frac{1}{5}\sum_{j=0}^{4} P^{\mathrm{close}}_{k-j})", TS(Ma, None, None))
