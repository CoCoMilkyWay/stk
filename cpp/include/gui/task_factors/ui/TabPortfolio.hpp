// Factors→Portfolio: 因子组合页 + Factors 页 1. 区右侧的两个组合小框.
//   小框 1: 选组合 (<portfolio_dir>/<name>.json) / New / Rename / Del;  小框 2: 当前组合的条目 "(池, 持有期): 名", Add = 加入 Factors 表里高光的
//   权重 (持有期 = Factors 页 Hold), Del = 删选中条目; 失效条目 (因子文件里找不到该规范串等) 红. 组合选择在内存, 内容即文件.
//   Portfolio 页 (对仗 OrderFlow 图 2 的横轴: x = 日下标 · 255 + 行末分钟, 刻度 YY/MM/DD HH:MM; 不画个股):
//     组合两条线 = 各因子独立回测 (PortfolioService FactorCurve, 各自的层) 的逐行增量等权聚合: 每个交易日在当日有数据的 n 个因子间等分,
//     因子增量 = (行收益 − 行成本) / h (h = hold_rows, 重叠持有期折算), 记在该行行末分钟; 累计 = 简单求和 (%).
//       G20 多头 = G20 做多绝对收益 − 多腿成本;  LS = 多空 − 两腿成本. 成本 = 选定金额段 (默认最小非零段; Bar 层无成本段 → 毛) + Config::sell_impact
//     池等权指数线 (日层, 每池一条, 全部池在图例): 只画因子日并集轴范围内, 当日收盘一点, 日收益简单求和 (%); 默认只显组合里因子所在的池
#pragma once

#include "gui/task_factors/services/PortfolioService.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Factors {

// Portfolio 页派生 (UI 线程, 结果 / 内容 / 成本选项变才重算)
struct PortfolioDerived {
  std::vector<misc::Ymd> dates;         // 日并集轴 (横轴 day 下标)
  std::vector<double> x, cum_l, cum_ls; // 组合: 起点 + 每个有增量的 x (%)
  float ann_l = 0.f, ann_ls = 0.f;      // 末点按日数折年化 (%)
  struct Pool {
    std::string label;        // 图例 (池名 + 中文名)
    bool own = false;         // 组合里有因子在这个池
    std::vector<double> x, y; // 收盘点 (%)
  };
  std::vector<Pool> pools;      // [feature_pools 序]
  std::vector<int> amts;        // 可选金额段 (各因子并集, 升序)
  int n_ready = 0, n_error = 0; // 已算成 / 算失败的因子数
};

struct PortfolioUIState {
  // ---- 组合 (小框) ----
  std::string dir;                     // names 对应的目录 (config.portfolio_dir 变 → 重列)
  std::vector<std::string> names;      // 目录下的组合 (主干名)
  std::string name;                    // 当前组合; 空 = 无
  std::vector<PortfolioEntry> entries; // 当前组合内容 (= 文件)
  std::vector<PortfolioItem> items;    // 解析结果 (TaskFactors 在内容变 / 因子重扫后重解析; 与 entries 同序, 可能落后一帧)
  uint64_t content_gen = 0;            // 换组合 / 改内容 +1
  uint64_t items_gen = 0;              // items 每改一次 +1 (解析 / 建请求时标失效)
  int sel = -1;                        // 小框 2 选中条目
  char name_buf[64] = "";              // New / Rename 的名字
  std::string msg;                     // 上次操作结果
  bool confirm_delete = false;
  // ---- Portfolio 页 ----
  int amt = -1; // 成本金额段 (万); 0 = 毛; -1 = 未定 (派生出可选段后取最小非零)
  uint64_t der_gen = ~0ull, der_items = ~0ull;
  int der_amt = -2;
  double der_sell = -1.0;
  PortfolioDerived der;
  std::string vis_key; // 池线默认显隐已按它设过 (组合 + 其因子所在池; 变了重设)
};

// GUI 线程每帧 (两页都调): 目录变 → 重列, 当前组合不在了 → 选第一个 (载入内容, content_gen +1)
void SyncPortfolioDir(PortfolioUIState &ui, const std::string &dir);
// Factors 页 1. 区右侧两框. view_row = Factors 表高光行 (nullptr = 无); (pool, seg, hold) = Factors 页当前池 / 层 / Hold
void RenderPortfolioBoxes(PortfolioUIState &ui, const FactorRow *view_row, const std::string &pool, int seg, int hold);
// 返回值: 1 = Recompute (弃缓存重算), -1 = Cancel, 0 = 无
int RenderTabPortfolio(PortfolioService &svc, PortfolioUIState &ui, double sell_impact);

} // namespace GUI::Factors
