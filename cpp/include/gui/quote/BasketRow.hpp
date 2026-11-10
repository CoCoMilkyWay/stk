// 篮子合成的 all.bar 源 (BasketHistory 价格指数 / Market → CTA 的库同一份): L_1day 收盘行 + 选股表 → 次日成员表, L_1min → 分钟根.
//   交易日 D 的成员 / 权重 / 基准一律取库内前一交易日 (D−1) 的收盘行 (口径见 BasketHistory.hpp); 子池成员 = 选股表 D 的成员; 合成核 = Quote.hpp Fold.
#pragma once

#include "features/Panel/PoolSelect.hpp" // pool::Picks
#include "features/Storage/FeatureRead.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/quote/Quote.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace GUI::Quote {

// 一日的 L_1day 收盘行 (A 轴); 缺 / 非有限 → 0
struct Row {
  std::vector<float> px[4], amount, pre, shares; // 开 高 低 收 / 成交额 [元] / 前收 (该行 = 次日除权后前收) / 总股本
  std::vector<uint8_t> live, trade, ind;         // cs_valid / ts_valid / 申万一级 (0 = 未知)
};

// L_1day 读的列 (read_row 的列序)
std::vector<size_t> row_cols();
// L_1min 读的列 (bars_of 的列序): ts_valid / 开 / 高 / 低 / 收 / 额
std::vector<size_t> minute_cols();

// dc = row_cols 那天的读 → r
void read_row(const FeatureRead::DayColumns &dc, size_t A, Row &r);
// 收盘行 r 定次日 d 的成员表 (下标 = 库轴, 篮子 = basket_line): 所属切片各进等权 (1) / 加权 (收盘市值; 缺股本不进); 概念随全A;
//   子池 = pk 在 d 的成员 (pk 与 axis 同为 all.bar 轴)
void members(const Row &r, const PoolAxis &axis, const pool::Picks &pk, misc::Ymd d, Members &m);
// 一只一日的 L_1min (mc = minute_cols 那天的读) → 分钟根: 无成交的行不收 (与 bar1m 的 bars_of 同一并根)
void bars_of(const FeatureRead::DayColumns &mc, size_t a, misc::Ymd d, Bars &b);

} // namespace GUI::Quote