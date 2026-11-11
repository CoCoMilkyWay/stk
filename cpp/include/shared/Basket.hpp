#pragma once

// 篮子表 (行情价格指数 Quote::BasketHistory 与回测 Engine::baskets 同一张): 篮子 = 切片 × 该切片的权重 (pool::Weight)
//   切片 c: 0 = 全A, 1..kAllCuts−1 = SW2021 一级行业 (全A 内切), kAllCuts.. = 子池 (池表序), concept_cut(0).. = 概念 (全A 内切, 概念表序)
//   权重: 全A 两种 (等权 / 加权, 基准), 行业 / 概念 = 加权, 子池 = 池参数的权重维 (pool::kDimWeight)
//   线 = 篮子下标: 全A 两条在前 (线 = 权重), 其余切片各一条 (线 = c + 1); 两边一切按篮子的量 (点位 / 收益 / 估值 / 换手 / 成员权) 都以线为键, 线序 / 名同此
#include "features/Panel/Fund.hpp"         // SW2021_L1_COUNT / NAMES
#include "features/Storage/FeatureLib.hpp" // pool_count / pool_def
#include "shared/Concept.hpp"              // concepts

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <string>
#include <vector>

static_assert(pool::kWeights == 2, "全A 两条线 = 两种权重");

inline constexpr size_t kAllCuts = fund::SW2021_L1_COUNT; // 全A + 行业 (ind_l1 0 = 未知 只进全A)
inline size_t concept_cut(size_t k) { return kAllCuts + pool_count() - 1 + k; }
inline size_t cut_count() { return concept_cut(concepts().size()); }
inline size_t cut_of(PoolId p) { return p == kAllPool ? 0 : kAllCuts + p - 1; }
inline bool pool_cut(size_t c) { return c >= kAllCuts && c < concept_cut(0); }
inline PoolId cut_pool(size_t c) {
  assert(pool_cut(c));
  return static_cast<PoolId>(c - kAllCuts + 1);
}

// 切片 c 的线 [first_line(c), end_line(c))
inline size_t first_line(size_t c) { return c ? c + 1 : 0; }
inline size_t end_line(size_t c) { return c + 2; }
inline size_t basket_count() { return end_line(cut_count() - 1); }
inline size_t cut_of_line(size_t line) { return line < pool::kWeights ? 0 : line - 1; }
inline pool::Weight weight_of(size_t line) {
  const size_t c = cut_of_line(line);
  return c == 0 ? static_cast<pool::Weight>(line) : pool_cut(c) ? pool_def(cut_pool(c)).spec.weight()
                                                                : pool::kWeightCap;
}
inline size_t basket_line(size_t c, pool::Weight w) {
  const size_t l = first_line(c) + (c ? 0 : static_cast<size_t>(w));
  assert(weight_of(l) == w && "切片没有这种权重的篮子");
  return l;
}
// 切片的调仓周期: 子池 = 池参数的, 其余日频 (成员源 = 前一交易日收盘, 见 features/Panel/PoolSelect.hpp src_of)
inline pool::Rebal cut_rebal(size_t c) { return pool_cut(c) ? pool_def(cut_pool(c)).spec.rebal() : pool::kRebalDay; }

// 切片代码: 全A / 子池 = 池名, 行业 = sw<ID>, 概念 = 板块代码
inline std::string cut_code(size_t c) {
  if (c == 0)
    return pool_def(kAllPool).name;
  if (c < kAllCuts) {
    char s[8];
    std::snprintf(s, sizeof(s), "sw%02zu", c);
    return s;
  }
  if (pool_cut(c))
    return pool_def(cut_pool(c)).name;
  return concepts().code[c - concept_cut(0)];
}

// 切片中文名 (子池 = 池的显示名, 已带权重后缀)
inline std::string cut_name(size_t c) {
  if (c == 0)
    return "全A";
  if (c < kAllCuts)
    return std::string(fund::SW2021_L1_NAMES[c]);
  if (pool_cut(c))
    return pool_def(cut_pool(c)).name_cn;
  return concepts().name[c - concept_cut(0)];
}
// 篮子中文名 = 切片名 + 权重后缀 (.等 = 等权 / .加 = 总市值加权; 子池的显示名已带)
inline std::string basket_name(size_t line) {
  const size_t c = cut_of_line(line);
  return pool_cut(c) ? cut_name(c) : cut_name(c) + pool::weight_suffix(weight_of(line));
}

// 篮子目录 (展示用的三组线): 指数 = 全A (加权 + 等权) + 各子池, 行业 = 申万一级, 概念 = 东财题材概念
enum BasketGroup : int { kGroupPool,
                         kGroupInd,
                         kGroupConcept };
inline constexpr int kBasketGroups = 3;
inline std::vector<size_t> basket_group(BasketGroup g) {
  std::vector<size_t> ls;
  if (g == kGroupPool) {
    ls.push_back(basket_line(0, pool::kWeightCap));
    for (const PoolId p : pool_ids())
      ls.push_back(p == kAllPool ? basket_line(0, pool::kWeightEq) : first_line(cut_of(p)));
  } else if (g == kGroupInd) {
    for (size_t c = 1; c < kAllCuts; ++c)
      ls.push_back(first_line(c));
  } else {
    for (size_t k = 0; k < concepts().size(); ++k)
      ls.push_back(first_line(concept_cut(k)));
  }
  return ls;
}
