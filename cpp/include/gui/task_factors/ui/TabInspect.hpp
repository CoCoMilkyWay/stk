// Factors→Inspect: 单因子展示页. 对象 = Factors 页表格里点行高光的那一个 (FactorsUIState::view_file); 数据 = InspectService
// 对它算出的一级 Row[H][T] (沿时间) + 二级 HoldStat (每持有期).
//
// 布局: 顶部 头信息 (文件 / 口径 / 规范串 / note) + 控件 (Amt / Hold / 超额|绝对 / Compute / Cancel / 状态) + 选定持有期的一行 stat;
// 下面 2×2 四图:
//   左上 分层累计: 20 组沿交易日累计的组均收益 (按钮切 超额 e = lv − mkt | 绝对 lv; 重叠持有期按 1/h 折算 = h 个相位非重叠链的平均)
//        + 多空 LS 粗线 (图例标注年化 Sharpe; LS 本就市场中性, 两模式同一条)
//   右上 期限结构: 全部持有期 (1m … t5) 的 Top 组 / Bottom 组 / LS 超额均值柱 → 这个因子预测哪个频段. 只用 HoldStat:
//        Inspect 算过用算的, 没算过用 Factors 表里 (文件) 的, 所以进页即有
//   左下 IC 时序: 日均 rank IC 柱 + 累计线 (全 universe 截面)
//   右下 留位 (Markowitz CDF 仓位映射)
// 触发: 高光因子 (文件 / 规范串 / 作用域) 变 且 服务空闲 → 自动起算 (缓存命中只补特征); Compute 按钮 = 弃缓存整体重读.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp"
#include "gui/task_factors/services/InspectService.hpp"
#include "gui/task_factors/ui/TabFactors.hpp" // FactorsUIState / FactorsUIContext

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
  std::vector<float> x_mid, ic_day, ic_cum;          // [days] 柱心 d + 0.5 / 日均 IC (无 ok 行 = NaN) / 累计
  factor::stat::HoldStat hs;                         // 选定 (amt, hold) 的二级
  bool valid = false;
};

struct InspectUIState {
  int amt_idx = 0, hold_idx = 0; // FeatureTable amts / labels 下标 (与 Factors 页独立)
  bool absolute = false;         // 左上: 超额 (false) | 绝对 (true)
  std::string last_req_key;      // 上次自动起算的 InspectRequest::key (取消后不反复重起)
  // 本帧请求交接 (TabInspect → TaskFactors): action ≠ 0 时 req_row 有效
  FactorRow req_row;
  bool req_reload = false;
  // 派生缓存
  uint64_t derived_epoch = ~0ull;
  int derived_sel = -1; // amt_idx * n_hold + hold_idx
  bool derived_abs = false;
  InspectDerived der;
};

// 返回值: 1 = 起算 (ui.req_row / ui.req_reload 已填), -1 = Cancel, 0 = 无
int RenderTabInspect(FactorsService &fsvc, InspectService &isvc, const FactorsUIState &fui, InspectUIState &ui, const FactorsUIContext &ctx);

} // namespace GUI::Factors
