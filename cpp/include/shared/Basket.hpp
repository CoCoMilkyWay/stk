#pragma once

// 篮子表 (行情价格指数 Database::BasketHistory 与回测 Factors::baskets 同一张): 篮子 = 切片 × 权重
//   切片 c: 0 = 全A, 1..kAllCuts−1 = SW2021 一级行业 (全A 内切), kAllCuts.. = 子池 (池表序)
//   线 basket_line(c, w) = kWeights · c + w: 两边一切按篮子的量 (点位 / 收益 / 估值 / 换手 / 成员权) 都以线为键, 线序 / 名同此
#include "features/Method/Fundamental.hpp" // SW2021_L1_COUNT / NAMES
#include "shared/FeatureLib.hpp"           // pool_count / pool_def

#include <cstddef>
#include <cstdio>
#include <string>
#include <vector>

// 篮子两权重: 成分各自的权 (等权 = 1, 加权 = 总市值)
enum Weight : int { kWeightEq,    // 等权
                    kWeightCap }; // 总市值加权
inline constexpr int kWeights = 2;

inline constexpr size_t kAllCuts = fund::SW2021_L1_COUNT; // 全A + 行业 (ind_l1 0 = 未知 只进全A)
inline size_t cut_count() { return kAllCuts + pool_count() - 1; }
inline size_t cut_of(PoolId p) { return p == kAllPool ? 0 : kAllCuts + p - 1; }
inline size_t basket_count() { return cut_count() * kWeights; }
inline size_t basket_line(size_t c, Weight w) { return kWeights * c + static_cast<size_t>(w); }
inline size_t cut_of_line(size_t line) { return line / kWeights; }
inline Weight weight_of(size_t line) { return static_cast<Weight>(line % kWeights); }

// 切片代码: 全A / 子池 = 池名, 行业 = sw<ID>
inline std::string cut_code(size_t c) {
  if (c == 0)
    return pool_def(kAllPool).name;
  if (c < kAllCuts) {
    char s[8];
    std::snprintf(s, sizeof(s), "sw%02zu", c);
    return s;
  }
  return pool_def(static_cast<PoolId>(c - kAllCuts + 1)).name;
}

// 篮子中文名: 全A 分权重; 行业默认加权、池默认等权, 非默认权重加后缀
inline std::string basket_name(size_t c, Weight w) {
  if (c == 0)
    return w == kWeightEq ? "全A等权" : "全A加权";
  if (c < kAllCuts)
    return std::string(fund::SW2021_L1_NAMES[c]) + (w == kWeightEq ? "等权" : "");
  return std::string(pool_def(static_cast<PoolId>(c - kAllCuts + 1)).name_cn) + (w == kWeightCap ? "加权" : "");
}
inline std::string basket_name(size_t line) { return basket_name(cut_of_line(line), weight_of(line)); }

// 篮子目录 (展示用的两组线): 指数 = 各池整体 (全A 加权 + 等权, 其余池等权), 行业 = 申万一级 (加权)
enum BasketGroup : int { kGroupPool,
                         kGroupInd };
inline constexpr int kBasketGroups = 2;
inline std::vector<size_t> basket_group(BasketGroup g) {
  std::vector<size_t> ls;
  if (g == kGroupPool) {
    ls.push_back(basket_line(0, kWeightCap));
    for (const PoolId p : pool_ids())
      ls.push_back(basket_line(cut_of(p), kWeightEq));
  } else {
    for (size_t c = 1; c < kAllCuts; ++c)
      ls.push_back(basket_line(c, kWeightCap));
  }
  return ls;
}
