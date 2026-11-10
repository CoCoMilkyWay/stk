// 概念表 — 东财概念板块 (只留题材) 的成分快照: output/data/concept/YYYYMMDD.tsv, 一日一份 (Fundamental 联网同步时拉当日缺的).
//   一行一个概念: 板块代码 \t 名称 \t 成分码 (A 轴码形, 逗号分隔), 按板块代码升序.
//   题材 = 名称不命中剔除词 (指数成分 / 交易制度 / 资金持股 / 行情统计 / 风格 / 业绩预告) ∧ 成分 ≥ kConceptMin.
//   进程内唯一 (concepts()): 首次访问载入最近一份, 此后不变 (本进程新拉的下次启动生效); 成分按这一份倒推全史 (篮子表概念切片同一份)
#pragma once

#include "misc/date.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

inline constexpr size_t kConceptMin = 10; // 成分下限

struct Concept {
  std::string code, name;        // "BK0493" / "新能源"
  std::vector<std::string> syms; // 成分码 (快照原样)
};

// 题材 (名称不命中剔除词)
bool concept_theme(std::string_view name);
// 最近一份快照日 (0 = 无)
misc::Ymd concept_last();
// 当日快照落盘 (list 按板块代码升序, 各自成分 ≥ kConceptMin)
void save_concepts(misc::Ymd date, const std::vector<Concept> &list);

// 载入的概念表: 成分映射到全局 A 轴 (轴外的码不收)
struct ConceptTable {
  misc::Ymd date = 0;            // 快照日 (0 = 无快照, 表空)
  std::vector<std::string> code; // [概念] 板块代码
  std::vector<std::string> name; // [概念] 名称
  uint64_t hash = 0;             // 代码 + 成分全局 id 的指纹 (篮子缓存键)
  std::vector<uint32_t> at;      // [全局 id + 1] 资产 g 所属概念 = of[at[g], at[g + 1]) (升序)
  std::vector<uint16_t> of;
  size_t size() const { return code.size(); }
  // 资产 g 所属的概念下标; g 在载入后才进轴 → 空
  std::span<const uint16_t> concepts_of(uint32_t g) const {
    return g + 1 < at.size() ? std::span<const uint16_t>(of.data() + at[g], at[g + 1] - at[g]) : std::span<const uint16_t>{};
  }
};
const ConceptTable &concepts();
