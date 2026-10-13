// FactorsLoad — 因子评估的特征库装载层, FactorsService (全表 Run) 与 InspectService (单因子深看) 共用:
//   Loaded = 一轮评估的宿主平面 (特征 [feat] + 截面门控 cs + 标签 [hold] + 冲击成本 [amt]), load_planes 逐天并行读特征库散进去;
//   check_leaf_domains 按 OpTable in 列查特征叶数据值域; valid_pct_of 根平面有效率; n_full_of 满仓只数; cost_tiers 换手成本 (两服务同一份).
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

namespace GUI::Factors {

// ---- 装载好的一轮数据 ----
struct LabelPlane {
  std::vector<uint16_t> lv, sv; // 做多 / 做空毛收益 fp16 位 (与落盘同格式, Stat 直接吃)
  std::vector<uint8_t> m;
};
struct CostPlane {
  std::vector<uint16_t> buy, sell; // 建仓冲击 (比例) fp16 位: lb_cost_buy_<amt>w (按 Loaded::max_impact 封顶) / lb_cost_sell_<amt>w (原值)
  std::vector<uint8_t> m;          // 有效位 (两列皆有限; 吃不到 = NaN → 无效)
};
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
  std::vector<int> label_idx; // [hi] → FeatureTable::labels 下标 (init_loaded 全部持有期; select_hold 收窄到一个)
  size_t n() const { return static_cast<size_t>(T) * A; }
};

// 维度 / 标签组: days × A → T; hd = 字段表的全部持有期 (容量断言在此)
void init_loaded(const FeatureTable &ft, int days, int A, Loaded &L);
// 标签组收窄到一个持有期 (Search: 一次搜索一个固定 h, 只装 / 只评这一组标签); hold 须在字段表里. 装载前调
void select_hold(const FeatureTable &ft, int hold, Loaded &L);

// 逐天并行读 (ex 抢单): 特征列 (L.feat_codes) + (with_labels) L.hd 的标签列 (+ with_costs 冲击成本列) + ts_valid + cs_valid 一次 load_day_columns,
// 门控后散进平面; with_index 再加读 close 列, 顺带出 L.index (逐天并行攒日内项, 收尾串行补跨日首笔 + 连乘).
// 返回 false = 取消 或 reader 判废 (调用方查 reader.stale()). done 每天 +1
bool load_planes(const FeatureTable &ft, FeatureRead &reader, const std::vector<std::string> &dates, Loaded &L, factor::exec::ForkJoin &ex,
                 std::atomic<bool> &cancel, std::atomic<int> &done, bool with_labels = true, bool with_costs = false, bool with_index = false);

// 算子节点的特征叶元: 按 OpTable in 列的严格域逐元查数据; 越界 → err 非空 (因子 BROKEN)
void check_leaf_domains(const factor::Dag &d, int node, const Loaded &L, std::string &err);

// 池内有效率: Σ(m ∧ gate) / Σgate (gate = 截面池 cs_valid; 池外格不进分子分母)
float valid_pct_of(const uint8_t *m, const uint8_t *gate, size_t n);

// 满仓一腿只数 (Search 契约【目标】util 的分母) = 池内 (cs.m) 平均每行有效票数 / kGroups: 池宽决定, 与因子无关. Run / Search 同一份
double n_full_of(const Loaded &L);

// 换手成本 (Stat 契约【换手成本】, Run / Inspect 同一份; Search 后端在核内攒同一份 cost_cell): rx = r16(x) [T][A] (池内秩, kRankNone 无效),
// 每 (t ≥ h, h, c) 一行 cost_row → out[c].acc[hi] (c 按 L.costs 序, hi 按 L.hd 序), 顺带 cover = Σ(rx ∧ 成本有效) / Σ rx 有效.
// 行区间并行 (ex), 线程私有累加器末尾 merge. 须 with_costs 装过 (L.costs.size() == ft.costs.size())
void cost_tiers(factor::exec::ForkJoin &ex, const uint16_t *rx, const Loaded &L, const FeatureTable &ft, std::vector<CostTier> &out);

} // namespace GUI::Factors
