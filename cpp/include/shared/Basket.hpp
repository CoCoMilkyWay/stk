#pragma once

// 篮子表 (行情价格指数 Database::BasketHistory 与回测 Factors::baskets 同一张): 篮子 = 切片 × 权重
//   切片 c: 0 = 全A, 1..kAllCuts−1 = SW2021 一级行业 (全A 内切), kAllCuts.. = 子池 (池表序), concept_cut(0).. = 概念 (全A 内切, 概念表序)
//   线 basket_line(c, w) = kWeights · c + w: 两边一切按篮子的量 (点位 / 收益 / 估值 / 换手 / 成员权) 都以线为键, 线序 / 名同此
#include "features/Method/Fundamental.hpp" // SW2021_L1_COUNT / NAMES
#include "shared/Concept.hpp"              // concepts
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
inline size_t concept_cut(size_t k) { return kAllCuts + pool_count() - 1 + k; }
inline size_t cut_count() { return concept_cut(concepts().size()); }
inline size_t cut_of(PoolId p) { return p == kAllPool ? 0 : kAllCuts + p - 1; }
inline size_t basket_count() { return cut_count() * kWeights; }
inline size_t basket_line(size_t c, Weight w) { return kWeights * c + static_cast<size_t>(w); }
inline size_t cut_of_line(size_t line) { return line / kWeights; }
inline Weight weight_of(size_t line) { return static_cast<Weight>(line % kWeights); }

// 切片代码: 全A / 子池 = 池名, 行业 = sw<ID>, 概念 = 板块代码
inline std::string cut_code(size_t c) {
  if (c == 0)
    return pool_def(kAllPool).name;
  if (c < kAllCuts) {
    char s[8];
    std::snprintf(s, sizeof(s), "sw%02zu", c);
    return s;
  }
  if (c < concept_cut(0))
    return pool_def(static_cast<PoolId>(c - kAllCuts + 1)).name;
  return concepts().code[c - concept_cut(0)];
}

// 篮子中文名 = 切片名 + 权重后缀 (.等 = 等权 / .加 = 总市值加权)
inline std::string basket_name(size_t c, Weight w) {
  const std::string cut = c == 0               ? std::string("全A")
                          : c < kAllCuts       ? std::string(fund::SW2021_L1_NAMES[c])
                          : c < concept_cut(0) ? std::string(pool_def(static_cast<PoolId>(c - kAllCuts + 1)).name_cn)
                                               : concepts().name[c - concept_cut(0)];
  return cut + (w == kWeightEq ? ".等" : ".加");
}
inline std::string basket_name(size_t line) { return basket_name(cut_of_line(line), weight_of(line)); }

// 篮子目录 (展示用的三组线): 指数 = 各池整体 (全A 加权 + 等权, 其余池等权), 行业 = 申万一级 (加权), 概念 = 东财题材概念 (加权)
enum BasketGroup : int { kGroupPool,
                         kGroupInd,
                         kGroupConcept };
inline constexpr int kBasketGroups = 3;
inline std::vector<size_t> basket_group(BasketGroup g) {
  std::vector<size_t> ls;
  if (g == kGroupPool) {
    ls.push_back(basket_line(0, kWeightCap));
    for (const PoolId p : pool_ids())
      ls.push_back(basket_line(cut_of(p), kWeightEq));
  } else if (g == kGroupInd) {
    for (size_t c = 1; c < kAllCuts; ++c)
      ls.push_back(basket_line(c, kWeightCap));
  } else {
    for (size_t k = 0; k < concepts().size(); ++k)
      ls.push_back(basket_line(concept_cut(k), kWeightCap));
  }
  return ls;
}
