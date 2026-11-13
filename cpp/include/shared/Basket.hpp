#pragma once

// 篮子表 (行情价格库 Quote::PriceLib (篮子侧) / 回测 Engine::baskets 同一张): 篮子 = 切片 × 该切片的权重 (pool::Weight), 一切片一个篮子
//   三段, 段序 = 目录组序 = 下标序: 子池 (池表序, 首两池 = 全A 等权 / 加权) | 申万一级行业 (ind_l1 序) | 概念 (概念表序)
//   子池的成员 / 权重 / 周期 = 池参数; 行业 / 概念 = 母集 (全A 等权池) 内切, 加权, 日频
//   下标 b = 篮子号: 两边一切按篮子的量 (点位 / 收益 / 估值 / 换手 / 成员权) 都以 b 为键, 序 / 名同此; 一日的持仓 = 成员表 (define/Members.hpp, 键 = b)
//   逐篮子的属性建表时定 (热循环只查数组); 成员判据只此一处 (each ⇄ holds)
#include "features/Panel/Fund.hpp"         // SW2021_L1_COUNT / NAMES
#include "features/Storage/FeatureLib.hpp" // pool_count / pool_def / kAllAPool / PoolMask
#include "misc/epoch.hpp"
#include "shared/Concept.hpp" // concepts

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <vector>

// 段 = 篮子目录的三组线: 指数 = 各子池, 行业 = 申万一级, 概念 = 东财题材概念
enum BasketGroup : int { kGroupPool,
                         kGroupInd,
                         kGroupConcept };
inline constexpr int kBasketGroups = 3;
inline constexpr size_t kIndBaskets = fund::SW2021_L1_COUNT - 1;  // ind_l1 1..; 0 = 未知 (不在市)
inline constexpr PoolId kBasketBase = kAllAPool[pool::kWeightEq]; // 行业 / 概念篮子的母集

// 一只一日的事实 (判据的当日输入, 同取成员源行 = D−1 收盘, 整日常量); 所属概念是资产常量, 另给
struct Fact {
  PoolMask in = 0; // 当日所在的池 (位 kAllPool 不看)
  uint8_t ind = 0; // 申万一级 (0 = 未知)
};

class BasketTable {
public:
  BasketTable();

  size_t size() const { return lo_[kBasketGroups]; }
  uint64_t hash() const { return hash_; } // 池表 + 概念表的指纹: 按篮子下标存的缓存以此判废

  // 段 ⇄ 篮子: 段 g 第 k 个 / 段 g 的篮子 / 篮子 b 所在的段
  size_t at(BasketGroup g, size_t k) const {
    assert(k < lo_[g + 1] - lo_[g]);
    return lo_[g] + k;
  }
  auto of(BasketGroup g) const { return std::views::iota(lo_[g], lo_[g + 1]); }
  BasketGroup group(size_t b) const {
    assert(b < size());
    return b < lo_[kGroupInd] ? kGroupPool : b < lo_[kGroupConcept] ? kGroupInd
                                                                    : kGroupConcept;
  }
  // 切片 ⇄ 篮子: 子池 p / 行业 ind_l1 / 概念表下标 k
  size_t at_pool(PoolId p) const {
    assert(p != kAllPool && "all 不是篮子");
    return at(kGroupPool, p - 1u);
  }
  size_t at_ind(uint8_t i) const {
    assert(i >= 1);
    return at(kGroupInd, i - 1u);
  }
  size_t at_concept(size_t k) const { return at(kGroupConcept, k); }
  PoolId pool_of(size_t b) const {
    assert(group(b) == kGroupPool);
    return static_cast<PoolId>(b + 1);
  }
  // 权所依的池: 子池 = 本池, 行业 / 概念 = 母集
  PoolId base(size_t b) const { return group(b) == kGroupPool ? pool_of(b) : kBasketBase; }

  // 逐篮子的属性: 权重 / 调仓周期 (子池 = 池参数, 其余日频: 成员源 = 前一交易日收盘, 见 features/Panel/PoolSelect.hpp src_of)
  //   / 代码 (子池 = 池名, 行业 = sw<ID>, 概念 = 板块代码) / 名 = 主干 (切片名) + 权重后缀
  pool::Weight weight(size_t b) const { return weight_[b]; }
  pool::Rebal rebal(size_t b) const { return rebal_[b]; }
  const std::string &code(size_t b) const { return code_[b]; }
  const std::string &stem(size_t b) const { return stem_[b]; }
  std::string name(size_t b) const { return stem_[b] + pool::weight_suffix(weight_[b]); }

  // 成员判据两向 (价格指数的成员表 Quote::members / 回测池篮子 Engine::baskets / 仓位链同出于此): 一只一日的事实 f + 所属概念 cs (概念表下标升序)
  //   子池 p: f.in 含 p; 行业 k: f.in 含母集 ∧ f.ind = k; 概念 k: f.in 含母集 ∧ k ∈ cs
  //   each 枚举所属的篮子 fn(b) (b 升序) ⇄ holds 测是否在篮子 b
  template <class Fn>
  void each(const Fact &f, std::span<const uint16_t> cs, Fn &&fn) const {
    for (PoolMask r = f.in & ~pool_bit(kAllPool); r; r &= r - 1)
      fn(at_pool(pool_first(r)));
    if (!(f.in & pool_bit(kBasketBase)))
      return;
    if (f.ind > 0)
      fn(at_ind(f.ind));
    for (const uint16_t k : cs)
      fn(at_concept(k));
  }
  bool holds(size_t b, const Fact &f, std::span<const uint16_t> cs) const {
    const BasketGroup g = group(b);
    if (g == kGroupPool)
      return (f.in & pool_bit(pool_of(b))) != 0;
    if (!(f.in & pool_bit(kBasketBase)))
      return false;
    const size_t k = b - lo_[g];
    return g == kGroupInd ? static_cast<size_t>(f.ind) == k + 1 : std::ranges::binary_search(cs, static_cast<uint16_t>(k));
  }

private:
  std::array<size_t, kBasketGroups + 1> lo_{}; // 段界
  uint64_t hash_ = 0;
  std::vector<pool::Weight> weight_;
  std::vector<pool::Rebal> rebal_;
  std::vector<std::string> code_, stem_;
};

// 每进程代一份 (随池表 / 概念表换代)
inline const BasketTable &basket_table() {
  static misc::Epochal<BasketTable> t;
  return t.get();
}

inline BasketTable::BasketTable() {
  const ConceptTable &ct = concepts();
  const size_t NP = pool_count() - 1, NC = ct.size();
  lo_ = {0, NP, NP + kIndBaskets, NP + kIndBaskets + NC};
  hash_ = ct.hash ^ pool_hash() * 0x9E3779B97F4A7C15ULL;
  weight_.assign(size(), pool::kWeightCap);
  rebal_.assign(size(), pool::kRebalDay);
  code_.resize(size()), stem_.resize(size());
  for (size_t k = 0; k < NP; ++k) {
    const PoolDef &d = pool_def(static_cast<PoolId>(k + 1));
    const std::string_view suf = pool::weight_suffix(d.spec.weight());
    assert(d.name_cn.ends_with(suf) && "子池显示名以权重后缀收尾");
    const size_t b = at(kGroupPool, k);
    weight_[b] = d.spec.weight(), rebal_[b] = d.spec.rebal();
    code_[b] = d.name, stem_[b] = d.name_cn.substr(0, d.name_cn.size() - suf.size());
  }
  for (size_t k = 0; k < kIndBaskets; ++k) {
    char s[8];
    std::snprintf(s, sizeof(s), "sw%02zu", k + 1);
    const size_t b = at(kGroupInd, k);
    code_[b] = s, stem_[b] = fund::SW2021_L1_NAMES[k + 1];
  }
  for (size_t k = 0; k < NC; ++k) {
    const size_t b = at(kGroupConcept, k);
    code_[b] = ct.code[k], stem_[b] = ct.name[k];
  }
}
