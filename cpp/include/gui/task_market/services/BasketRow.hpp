// 篮子合成的 all.bar 源 (BasketHistory 价格指数 / Factors CTA 库同一份): L_1day 收盘行 → 次日成员表, L_1min → 分钟根.
//   交易日 D 的成员 / 权重 / 基准一律取库内前一交易日 (D−1) 的收盘行 (口径见 BasketHistory.hpp); 合成核 = Kline.hpp Fold.
#pragma once

#include "features/Storage/FeatureRead.hpp"
#include "gui/task_market/services/Kline.hpp"
#include "shared/AssetAxis.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace GUI::Market {

// 一日的 L_1day 收盘行 (A 轴); 缺 / 非有限 → 0
struct Row {
  std::vector<float> px[4], amount, pre, shares; // 开 高 低 收 / 成交额 [元] / 前收 (该行 = 次日除权后前收) / 总股本
  std::vector<uint8_t> live, trade, ind;         // cs_valid / ts_valid / 申万一级 (0 = 未知)
  std::vector<uint8_t> pool;                     // [(p − 1) · A + a] 子池 p 的池列
};

// L_1day 读的列 (read_row 的列序): 收盘行各量 + 各子池池列
std::vector<size_t> row_cols();
// L_1min 读的列 (bars_of 的列序): ts_valid / 开 / 高 / 低 / 收 / 额
std::vector<size_t> minute_cols();

// dc = row_cols 那天的读 → r; NP = 池数
void read_row(const FeatureRead::DayColumns &dc, size_t A, size_t NP, Row &r);
// 收盘行 r 定次日成员表 (下标 = 库轴, 篮子 = basket_line): 所属切片各进等权 (1) / 加权 (收盘市值; 缺股本不进); 概念随全A
void members(const Row &r, const PoolAxis &axis, size_t NP, Members &m);
// 一只一日的 L_1min (mc = minute_cols 那天的读) → 分钟根: 无成交的行不收 (与 bar1m 的 bars_of 同一并根)
void bars_of(const FeatureRead::DayColumns &mc, size_t a, misc::Ymd d, Bars &b);

} // namespace GUI::Market