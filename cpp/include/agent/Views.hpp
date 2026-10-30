#pragma once

// =============================================================================
// Views — 观察工具 (Run::Look) 的两半: 调用 → 要的原料 (LookSpec) / 原料 → 交给 agent 的文本
// =============================================================================
//   LookOf  按参数查因子 / 特征 / 资产 / 日期写法 (Lab 的三个校验, 不碰数据); 交割单 / 个股视图另把本层 K 线叶 (Context::kline) 排在特征前
//   Render  Look → markdown: 表 + 火花线 (▁..█, 无效 ·); 收益一律 bp, 超额 = 毛 − 本行在池均值; 依赖数据的参数 (日期在不在评估期) 在这里查
//   统计口径与分层回测同一份: 组的每期超额 = 有统计行的组超额之和 / 全部行数; 多空 = (G20 − G1) / 2; mono = 组号与组超额的 Spearman
// =============================================================================

#include "agent/Bridge.hpp"
#include "agent/Tools.hpp"

#include <string>

namespace agent {

// 不成 → false, err / out = 原因 (作该调用的结果)
bool LookOf(const Call &c, const Lab &lab, LookSpec &out, std::string &err);
bool Render(const Call &c, const Look &look, const Context &ctx, std::string &out);

} // namespace agent
