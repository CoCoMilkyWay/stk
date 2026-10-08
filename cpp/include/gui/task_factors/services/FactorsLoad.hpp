// FactorsLoad — 因子评估的特征库装载层, FactorsService (全表 Run) 与 InspectService (单因子深看) 共用:
//   Loaded = 一轮评估的宿主平面 (特征 [feat] + 截面门控 cs + 标签 [amt × hold]), load_planes 逐天并行读特征库散进去;
//   check_leaf_domains 按 OpTable in 列查特征叶数据值域; valid_pct_of 根平面有效率.
// 两个服务的差别只在装载之后: FactorsService 共享 DAG 顺序走全部因子只留 HoldStat; InspectService 一棵 DAG 留一级 Row[H][T].
#pragma once

#include "factor/Check.hpp" // check::Plane
#include "factor/Dag.hpp"
#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

class FeatureRead;

namespace GUI::Factors {

size_t hw_threads();
// 一波线程抢任务 (同 Correlation.cpp 的 parallel_for; fn(i, tid))
void parallel_for(size_t n_tasks, size_t n_threads, const std::atomic<bool> &cancel, const std::function<void(size_t, size_t)> &fn);

// ---- 装载好的一轮数据 ----
struct LabelPlane {
  std::vector<uint16_t> lv, sv; // 做多 / 做空净收益 fp16 位 (与落盘同格式, Stat 直接吃)
  std::vector<uint8_t> m;
};
struct Loaded {
  int T = 0, A = 0, days = 0;
  std::vector<std::string> feat_codes;      // 去重特征 (平面下标)
  std::vector<factor::check::Plane> planes; // [feat] 值 + 掩码 (按 ts_valid 门控)
  factor::check::Plane cs;                  // 截面门控平面: v = cs_valid 原值, m = data_valid(cs_valid) (= 当日在池, 整日常量)
  std::vector<LabelPlane> labels;           // [amt × hold] 展平: labels[ai * n_hold + hi] (按 cs_valid 门控)
  factor::stat::Holds hd;                   // hd.h[ai * n_hold + hi] = hold
  int n_amt = 0, n_hold = 0;
  size_t n() const { return static_cast<size_t>(T) * A; }
};

// 维度 / 标签组: days × A → T; hd 由字段表的 amt × hold 展平 (容量断言在此)
void init_loaded(const FeatureTable &ft, int days, int A, Loaded &L);

// 逐天并行读: 特征列 (L.feat_codes) + (with_labels) 全部标签列 + ts_valid + cs_valid 一次 load_day_columns, 门控后散进平面.
// 返回 false = 取消 或 reader 判废 (调用方查 reader.stale()). done 每天 +1
bool load_planes(const FeatureTable &ft, FeatureRead &reader, const std::vector<std::string> &dates, Loaded &L, std::atomic<bool> &cancel,
                 std::atomic<int> &done, bool with_labels = true);

// 算子节点的特征叶元: 按 OpTable in 列的严格域逐元查数据; 越界 → err 非空 (因子 BROKEN)
void check_leaf_domains(const factor::Dag &d, int node, const Loaded &L, std::string &err);

// CS 节点的特征叶: 掩码 ⊇ 截面门控 (EvalCpu.hpp cs_gate 契约) 断言, 每叶一次
void assert_cs_leaves_covered(const factor::Dag &d, const Loaded &L);

float valid_pct_of(const uint8_t *m, size_t n);

} // namespace GUI::Factors
