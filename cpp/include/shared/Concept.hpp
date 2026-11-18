// 概念表 — 东财题材概念板块成分视图 (源 = output/data/fundamental/_meta/concept.parquet, 随基本面同步落盘).
//   题材 = 名称不命中剔除词 (指数成分 / 交易制度 / 资金持股 / 行情统计 / 风格 / 业绩预告) ∧ 成分 ≥ kConceptMin.
//   进程内唯一 (concepts(), misc::Epochal): 每进程代首次访问载入, 代内不变 (新同步的随下一代生效); 成分按这一份倒推全史 (篮子表概念切片同一份)
#pragma once

#include "define/Csr.hpp"
#include "misc/date.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

inline constexpr std::size_t kConceptMin = 10; // 成分下限

// 载入的概念表: 成分映射到全局 A 轴 (轴外的码不收)
struct ConceptTable {
  misc::Ymd date = 0;             // 快照交易日 (0 = 未同步, 表空)
  std::vector<std::string> code;  // [概念] 板块代码 "BK0493"
  std::vector<std::string> name;  // [概念] 名称 "新能源"
  std::uint64_t hash = 0;         // 代码 + 成分全局 id 的指纹 (篮子缓存键)
  std::vector<std::uint64_t> sig; // [概念] 本概念代码 + 成分全局 id 的指纹 (篮子的身份)
  Csr<std::uint16_t> of;          // [全局 id] 所属概念下标 (升序)
  std::size_t size() const { return code.size(); }
  // 资产 g 所属的概念下标; g 在载入后才进轴 → 空
  std::span<const std::uint16_t> concepts_of(std::uint32_t g) const {
    return g < of.size() ? of[g] : std::span<const std::uint16_t>{};
  }
};

const ConceptTable &concepts();
