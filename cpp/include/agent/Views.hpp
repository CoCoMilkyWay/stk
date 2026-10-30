#pragma once

// =============================================================================
// Views — 观察工具 (Run::Look) 的两半: 调用 → 要的原料 (LookSpec) / 原料 → 交给 agent 的文本
// =============================================================================
//   LookOf  按参数查因子 / 特征 / 资产 / 日期与时段写法 (Lab 的三个校验, 不碰数据); 交割单 / 响应曲线 / 成分 / 个股视图另要本层 K 线叶 (LookSpec::kline);
//           exprs 各式各自校验: 不成的带着原因不算 (Render 写在该式那里), 其余照算; view_feature 一式也不成才算没做成; view_layers 的 cond 不成即没做成
//   Render  Look → markdown: 表 + 火花线 (▁..█, 无效 ·; 长序列分箱); 收益一律 bp, 超额 = 毛 − 本行在池均值; 依赖数据的参数 (日期在不在评估期) 在这里查;
//           评估期 / 行 / 持有期在系统提示里, 观察日段与作用域评估期不同时才在开头注明
//   统计口径与分层回测同一份: 组的每期超额 = 有统计行的组超额之和 / 全部行数; 多空 = (G20 − G1) / 2; mono = 组号与组超额的 Spearman
//   例外: 条件分层 (view_layers cond) 与成分剥离 (view_explain) 要按别的分数重新分组, 用标签口径近似: 逐行把 在池 ∧ 标签有效 的格等分 20 组,
//         组超额 = 组内 (标签 − 本行在池均值) 等权均值; 建不了仓的格不剔, 只作同一视图内各行之间对照
//   view_book 不走 Look: BookOf 查参数 → Lab::book 逐笔重放 → RenderBook 成文 (逐时表每 step 秒并成一行; 额一律万元; 价位距离按 0.01 元一跳)
// =============================================================================

#include "agent/Bridge.hpp"
#include "agent/Tools.hpp"

#include <string>

namespace agent {

// 不成 → false, err / out = 原因 (作该调用的结果)
bool LookOf(const Call &c, const Lab &lab, LookSpec &out, std::string &err);
bool Render(const Call &c, const Look &look, const Context &ctx, std::string &out);
// eval_factor 无显著时代表点的观察 → 分层摘要 ("- " 起: 20 组 + 分时段 mono), 接在回测结果后
std::string RepText(const Look &look);
// view_book
bool BookOf(const Call &c, const Lab &lab, BookSpec &out, std::string &err);
bool RenderBook(const Call &c, const Book &book, std::string &out);

} // namespace agent
