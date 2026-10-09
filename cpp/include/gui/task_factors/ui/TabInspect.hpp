// Factors→Inspect: 单因子展示页. 对象 = Factors 页表格里点行高光的那一个 (FactorsUIState::view_file); 数据 = InspectService
// 对它算出的一级 Row[H][T] (沿时间) + 二级 HoldStat (每持有期).
//
// 布局: 顶部 头信息 (文件 / 口径 / 规范串 / note) + 控件 (Hold / 冲击 / Compute / Cancel / 状态) + 选定持有期的一行 stat;
//   冲击 = 标签口径: 无 (毛价格收益, 默认, 与 Factors Run 同) | <amt>w (扣建仓冲击 lb_cost_{buy,sell}_<amt>w + 平仓固定冲击 Config::sell_impact);
//   换档 = 换请求 key → 服务即兴重算 Stat (净标签不缓存), 期限结构 / 分层 / IC 全部随之换口径
// 下面 2×2 四图:
//   左上 分层累计: 20 组沿交易日累计的组均收益 (图内右上角两个按钮: 毛|净 = 是否每往返扣税佣 (Config, 尚未接线) / 超额 e = lv − mkt | 绝对 lv;
//        重叠持有期按 1/h 折算 = h 个相位非重叠链的平均)
//        + 多空 LS 粗线 (图例标注年化 Sharpe; LS 本就市场中性, 两模式同一条). 分层色 = G1 冷 → G20 暖 两色线性渐变 (单调, 不交织)
//   右上 期限结构: 全部持有期 (1m … t5) 的 L (G20 做多超额) / S (G1 做多超额) / LS 均值折线 (PCHIP 圆滑) → 这个因子预测哪个频段.
//        三条线图例可点切, 默认只显 LS. 口径 = 全 universe 全时刻池化 (Σ 组内超额 / Σ 组样本数), 不是逐资产. 只用 HoldStat:
//        Inspect 算过用算的, 没算过用 Factors 表里 (文件) 的, 所以进页即有
//   左下 IC 分布: 逐行 rank IC (ok 行) 当随机变量 → KLL → PDF (与顶部 stat 的 mean/std/skew/kurt 同一组样本), 标 0 线 + 均值线
//   右下 留位 (Markowitz CDF 仓位映射)
// 触发: 高光因子 (文件 / 规范串 / 作用域) 变 且 服务空闲 → 自动起算 (缓存命中只补特征); Compute 按钮 = 弃缓存整体重读.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp"
#include "gui/task_factors/services/InspectService.hpp"
#include "gui/task_factors/ui/TabFactors.hpp" // FactorsUIState / FactorsUIContext
#include "shared/Analysis.hpp"                // AggPdf (KLL 成品快照)

#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Factors {

// 从 result 派生的日级序列 (UI 线程持锁算一次, 选项变才重算)
struct InspectDerived {
  int days = 0;
  std::vector<std::string> dates;                    // [days] 轴刻度用
  std::vector<float> x_day;                          // [days + 1] 0..days (累计线横轴, 日末)
  std::vector<float> grp_cum[factor::stat::kGroups]; // [days + 1]
  std::vector<float> ls_cum;                         // [days + 1]
  analysis::AggPdf ic_pdf;                           // 逐行 rank IC (ok 行) 的 KLL PDF 成品 (n_pts = 0 → 样本不足不画)
  factor::stat::HoldStat hs;                         // 选定 hold 的二级
  bool valid = false;
};

struct InspectUIState {
  int hold_idx = 0;         // FeatureTable labels 下标 (与 Factors 页独立)
  int impact_amt = 0;       // 冲击口径: 0 = 无 (毛); 否则 FeatureTable::costs 里的金额档 (万). 进请求 key
  bool absolute = false;    // 左上: 超额 (false) | 绝对 (true)
  bool net_cost = false;    // 左上: 每往返扣税佣 (Config::commission × 2 + stamp; 分层线各 1 次, LS 2 次). 只有按钮, 扣减尚未接线
  std::string last_req_key; // 上次自动起算的 InspectRequest::key (取消后不反复重起)
  // 本帧请求交接 (TabInspect → TaskFactors): action ≠ 0 时 req_row 有效
  FactorRow req_row;
  bool req_reload = false;
  // 派生缓存
  uint64_t derived_epoch = ~0ull;
  int derived_sel = -1; // hold_idx
  bool derived_abs = false;
  InspectDerived der;
};

// 返回值: 1 = 起算 (ui.req_row / ui.req_reload 已填), -1 = Cancel, 0 = 无
int RenderTabInspect(FactorsService &fsvc, InspectService &isvc, const FactorsUIState &fui, InspectUIState &ui, const FactorsUIContext &ctx);

} // namespace GUI::Factors
