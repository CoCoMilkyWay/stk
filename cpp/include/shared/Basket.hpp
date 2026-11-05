#pragma once

// 篮子表 (行情价格指数 Database::BasketHistory 与回测 Factors::baskets 同一张): 篮子 = 切片 × 权重
//   切片 c: 0 = 全A, 1..kAllCuts−1 = SW2021 一级行业 (全A 内切), kAllCuts.. = 子池 (池表序)
//   线 basket_line(c, w) = kWeights · c + w; 两边的线序 / 名同此
#include "features/Method/Fundamental.hpp" // SW2021_L1_COUNT / NAMES
#include "shared/FeatureLib.hpp"           // pool_count / pool_def

#include <cstddef>
#include <string>

// 篮子两权重: 成分各自的权 (等权 = 1, 加权 = 总市值)
enum Weight : int { kWeightEq,    // 等权
                    kWeightCap }; // 总市值加权
inline constexpr int kWeights = 2;

inline constexpr size_t kAllCuts = fund::SW2021_L1_COUNT; // 全A + 行业 (ind_l1 0 = 未知 只进全A)
inline size_t cut_count() { return kAllCuts + pool_count() - 1; }
inline size_t cut_of(PoolId p) { return p == kAllPool ? 0 : kAllCuts + p - 1; }
inline size_t basket_count() { return cut_count() * kWeights; }
inline size_t basket_line(size_t c, Weight w) { return kWeights * c + static_cast<size_t>(w); }

// 篮子中文名: 全A 分权重; 行业默认加权、池默认等权, 非默认权重加后缀
inline std::string basket_name(size_t c, Weight w) {
  if (c == 0)
    return w == kWeightEq ? "全A等权" : "全A加权";
  if (c < kAllCuts)
    return std::string(fund::SW2021_L1_NAMES[c]) + (w == kWeightEq ? "等权" : "");
  return std::string(pool_def(static_cast<PoolId>(c - kAllCuts + 1)).name_cn) + (w == kWeightCap ? "加权" : "");
}
