// FactorsLoad — 因子评估的特征库装载层, FactorsService (全表 Run) 与 InspectService (单因子深看) 共用:
//   Loaded = 一轮评估的宿主平面 (特征 [feat] + 截面门控 cs + 标签 [hold] + 冲击成本 [amt]), load_planes 逐天并行读特征库散进去;
//   check_leaf_domains 按 OpTable in 列查特征叶数据值域; valid_pct_of / rank_pct_of 根平面有效率 / 可秩率; n_full_of 满档只数;
//   cost_tiers 换手成本 (两服务同一份); prep_labels / labels_raw 标签的契约视图 (CPU 预处理过 / GPU 会话自己预处理; cap = n_full_of).
// 两个服务的差别只在装载之后: FactorsService 共享 DAG 顺序走全部因子只留 HoldStat; InspectService 一棵 DAG 留一级 Row[H][T].
#pragma once

#include "factor/Check.hpp" // check::Plane
#include "factor/Dag.hpp"
#include "factor/Exec.hpp"
#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

class FeatureRead;
namespace factor::cpu::stat {
struct Workspace;
}

namespace GUI::Factors {

// ---- 装载好的一轮数据 ----
struct LabelPlane {
  std::vector<uint16_t> v[factor::stat::kLegs]; // 两腿毛收益 fp16 位: 做多 / 做空 (与落盘同格式, Stat 直接吃)
  std::vector<uint8_t> m;
};
struct CostPlane {
  std::vector<uint16_t> imp[factor::stat::kLegs]; // 两腿建仓冲击 (比例) fp16 位: lb_cost_buy_<amt>w (按 Loaded::max_impact 封顶) / lb_cost_sell_<amt>w (原值)
  std::vector<uint8_t> m;                         // 有效位 (两列皆有限; 吃不到 = NaN → 无效)
};
// 宿主平面 → 契约视图 (整张量, 未偏). ry = 预处理的做多标签 r16 (nullptr = 未预处理, GPU 会话自己做)
inline factor::stat::Label label_of(const LabelPlane &p, const uint16_t *ry) {
  return {{p.v[factor::stat::kLong].data(), p.v[factor::stat::kShort].data()}, p.m.data(), ry};
}
inline factor::stat::Cost cost_of(const CostPlane &p) { return {{p.imp[factor::stat::kLong].data(), p.imp[factor::stat::kShort].data()}, p.m.data()}; }
struct Loaded {
  int T = 0, A = 0, days = 0;
  float max_impact = 0.f;                   // 买入冲击封顶 (Config::max_impact): with_costs 装载前由调用方设 (> 0), buy = min(原值, 封顶)
  std::vector<std::string> feat_codes;      // 去重特征 (平面下标)
  std::vector<factor::check::Plane> planes; // [feat] 值 + 掩码 (按 ts_valid 门控)
  factor::check::Plane cs;                  // 截面门控平面: v = cs_valid 原值, m = 当日在池 ∧ 当日有成交 (ts_valid 任一分钟 ≠ 0; 整日常量)
  std::vector<LabelPlane> labels;           // [hold] (按 cs.m 门控), 与 hd / label_idx 同序
  std::vector<CostPlane> costs;             // [amt] (with_costs 才装), 下标 = FeatureTable::costs 下标
  std::vector<float> index;                 // [T] universe 等权指数 (with_index 才算; 起点 1): 逐分钟 Σ_成分 (close / 上一有效 close − 1) / n 连乘,
                                            //   成分 = 该刻 cs_valid; 上一有效 close 跨日 (隔夜跳空进当日首笔); 无成分可算的分钟持平
  factor::stat::Holds hd;                   // hd.h[hi] = hold
  int n_hold = 0;
  std::vector<int> label_idx; // [hi] → FeatureTable::labels 下标 (init_loaded 全部持有期; select_holds 收窄到子集)
  size_t n() const { return static_cast<size_t>(T) * A; }
  factor::stat::Span span() const { return {T, A}; } // 整张量 (t0_abs = t_lo = 0)
};

// CPU 后端的标签: ry[hi] = r16(做多标签) 一次预处理 (全核), L = 契约视图 (指 Loaded::labels / ry; Loaded 须活过它)
struct LabelsHost {
  std::vector<std::vector<uint16_t>> ry; // [hi][T·A]
  factor::stat::Labels L;
};
void prep_labels(factor::exec::ForkJoin &ex, const Loaded &L, factor::cpu::stat::Workspace &w, LabelsHost &out);
// 未预处理的契约视图 (ry 空; GPU 会话自己预处理)
factor::stat::Labels labels_raw(const Loaded &L);

// 维度 / 标签组: days × A → T; hd = 字段表的全部持有期 (容量断言在此)
void init_loaded(const FeatureTable &ft, int days, int A, Loaded &L);
// 标签组收窄到 holds (Search: 搜索窗内的持有期, 只装 / 只评这些标签); 每个 hold 须在字段表里, 非空. 装载前调
void select_holds(const FeatureTable &ft, const std::vector<int> &holds, Loaded &L);

// 逐天并行读 (ex 抢单): 特征列 (L.feat_codes) + (with_labels) L.hd 的标签列 (+ with_costs 冲击成本列) + ts_valid + cs_valid 一次 load_day_columns,
// 门控后散进平面; with_index 再加读 close 列, 顺带出 L.index (逐天并行攒日内项, 收尾串行补跨日首笔 + 连乘).
// 返回 false = 取消 或 reader 判废 (调用方查 reader.stale()). done 每天 +1
bool load_planes(const FeatureTable &ft, FeatureRead &reader, const std::vector<std::string> &dates, Loaded &L, factor::exec::ForkJoin &ex,
                 std::atomic<bool> &cancel, std::atomic<int> &done, bool with_labels = true, bool with_costs = false, bool with_index = false);

// 算子节点的特征叶元: 按 OpTable in 列的严格域逐元查数据; 越界 → err 非空 (因子 BROKEN)
void check_leaf_domains(const factor::Dag &d, int node, const Loaded &L, std::string &err);

// 池内有效率: Σ(m ∧ gate) / Σgate (gate = 截面池 cs_valid; 池外格不进分子分母)
float valid_pct_of(const uint8_t *m, const uint8_t *gate, size_t n);
// 可秩率 (%): 逐行 = 所在并列块 (同 r16) 不超过 ⌈nv / kGroups⌉ 只的格数 / nv (nv = 该行 m ∧ gate 格数; 整行 kRankNone → 0),
// 沿 nv ≥ 1 的行取均值. rx = 该因子的 r16 [T][A] (rank_x 出)
float rank_pct_of(factor::exec::ForkJoin &ex, const uint16_t *rx, const uint8_t *m, const uint8_t *gate, int T, int A);

// 满档一腿只数 (Stat 契约【持仓口径】的 cap, Labels::cap) = 池内 (cs.m) 平均每行有效票数 / kGroups: 池宽决定, 与因子无关. Run / Search / Inspect 同一份
double n_full_of(const Loaded &L);

// 换手成本 (Stat 契约【换手成本】, Run / Inspect 同一份): lc = 腿码 [T][A] (leg_codes(rx); 两后端同一份),
// Stat cost_rows (每 t 一遍 A 轴攒全部 (h, c) + cover) → out[c].acc[hi] (c 按 L.costs 序, hi 按 L.hd 序), cover = Σ(有秩 ∧ 成本有效) / Σ 有秩.
// w = Stat CPU 工作区 (ensure 过 C = ft.costs.size()). 须 with_costs 装过 (L.costs.size() == ft.costs.size()); 无金额档 → out 空
void cost_tiers(factor::exec::ForkJoin &ex, const uint8_t *lc, const Loaded &L, const FeatureTable &ft, factor::cpu::stat::Workspace &w,
                std::vector<CostTier> &out);

} // namespace GUI::Factors
