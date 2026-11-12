#pragma once

// 篮子表 (行情价格指数 Quote::BasketHistory 与回测 Engine::baskets 同一张): 篮子 = 切片 × 该切片的权重 (pool::Weight), 一切片一个篮子
//   切片 c: 0..kIndCuts−1 = SW2021 一级行业 (ind_l1 = c + 1), kIndCuts.. = 子池 (池表序, 首两池 = 全A 等权 / 加权), concept_cut(0).. = 概念 (概念表序)
//   行业 / 概念 = 全A (kAllAPool) 内切, 加权; 子池的成员 / 权重 / 周期 = 池参数
//   线 = 篮子下标 = 切片 c; 两边一切按篮子的量 (点位 / 收益 / 估值 / 换手 / 成员权) 都以线为键, 线序 / 名同此
#include "features/Panel/Fund.hpp"         // SW2021_L1_COUNT / NAMES
#include "features/Storage/FeatureLib.hpp" // pool_count / pool_def / kAllAPool
#include "shared/Concept.hpp"              // concepts

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <string>
#include <vector>

inline constexpr size_t kIndCuts = fund::SW2021_L1_COUNT - 1; // ind_l1 1..; 0 = 未知 (不在市)
inline size_t ind_cut(uint8_t ind) {
  assert(ind >= 1 && size_t{ind} <= kIndCuts);
  return size_t{ind} - 1;
}
inline size_t concept_cut(size_t k) { return kIndCuts + pool_count() - 1 + k; }
inline size_t basket_count() { return concept_cut(concepts().size()); }
inline size_t cut_of(PoolId p) {
  assert(p != kAllPool && p < pool_count() && "all 不是篮子");
  return kIndCuts + p - 1;
}
inline bool pool_cut(size_t c) { return c >= kIndCuts && c < concept_cut(0); }
inline PoolId cut_pool(size_t c) {
  assert(pool_cut(c));
  return static_cast<PoolId>(c - kIndCuts + 1);
}
inline pool::Weight weight_of(size_t c) { return pool_cut(c) ? pool_def(cut_pool(c)).spec.weight() : pool::kWeightCap; }
// 切片的调仓周期: 子池 = 池参数的, 其余日频 (成员源 = 前一交易日收盘, 见 features/Panel/PoolSelect.hpp src_of)
inline pool::Rebal cut_rebal(size_t c) { return pool_cut(c) ? pool_def(cut_pool(c)).spec.rebal() : pool::kRebalDay; }

// 切片代码: 子池 = 池名, 行业 = sw<ID>, 概念 = 板块代码
inline std::string cut_code(size_t c) {
  if (c < kIndCuts) {
    char s[8];
    std::snprintf(s, sizeof(s), "sw%02zu", c + 1);
    return s;
  }
  if (pool_cut(c))
    return pool_def(cut_pool(c)).name;
  return concepts().code[c - concept_cut(0)];
}

// 切片中文名 (子池 = 池的显示名, 已带权重后缀)
inline std::string cut_name(size_t c) {
  if (c < kIndCuts)
    return std::string(fund::SW2021_L1_NAMES[c + 1]);
  if (pool_cut(c))
    return pool_def(cut_pool(c)).name_cn;
  return concepts().name[c - concept_cut(0)];
}
// 篮子中文名 = 切片名 + 权重后缀 (.等 = 等权 / .加 = 总市值加权; 子池的显示名已带)
inline std::string basket_name(size_t c) { return pool_cut(c) ? cut_name(c) : cut_name(c) + pool::weight_suffix(weight_of(c)); }

// 篮子目录 (展示用的三组线): 指数 = 各子池 (池表序, 全A 两池起头), 行业 = 申万一级, 概念 = 东财题材概念
enum BasketGroup : int { kGroupPool,
                         kGroupInd,
                         kGroupConcept };
inline constexpr int kBasketGroups = 3;
inline std::vector<size_t> basket_group(BasketGroup g) {
  std::vector<size_t> ls;
  if (g == kGroupPool) {
    for (const PoolId p : pool_ids())
      if (p != kAllPool)
        ls.push_back(cut_of(p));
  } else if (g == kGroupInd) {
    for (size_t c = 0; c < kIndCuts; ++c)
      ls.push_back(c);
  } else {
    for (size_t k = 0; k < concepts().size(); ++k)
      ls.push_back(concept_cut(k));
  }
  return ls;
}
