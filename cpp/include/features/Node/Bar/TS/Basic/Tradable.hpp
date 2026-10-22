#pragma once

// =============================================================================
// Tradable - 可成交标记 (compute=onMinute; Bar 档 bar1m): 行末那一刻 / 当日开盘能否买入 / 卖出
// =============================================================================
//   bar1m 无盘口, 封板用整分钟价判: 最近一个有成交分钟 low ≥ P_up − ε → 封涨停, 买不进; high ≤ P_dn + ε → 封跌停, 卖不出.
//   无成交分钟延续上一有成交分钟的状态; 当日首笔成交前 (含全日停牌) 两者皆 0.
//   开盘 (开盘型 T+N 的 exit 时刻): 开盘价 = 09:25 撮合 bar 的价, 无撮合则首个有成交连续竞价 bar 的 open (与 LabelHold::open_at 同一定义);
//   开盘价 ≥ P_up − ε → 开盘买不进, ≤ P_dn + ε → 开盘卖不出; 截到开盘价之前 (含全日停牌) 两者皆 0, 之后当日恒定.
//   无涨跌幅限制 (涨跌停价 NaN): 比较恒 false → 可成交.
//   各层行取本层行末分钟的值 (本层节拍取 Series 最新值); L_1day 行 = 收盘竞价分钟.
// =============================================================================

#include "features/DataDefine.hpp"

class Tradable {
public:
  enum Out : size_t { can_buy,
                      can_sell,
                      can_buy_open,
                      can_sell_open,
                      kCount };
  float y[kCount] = {};

  Tradable(const MinuteData &md, const Series &lim_up, const Series &lim_dn) : md_(md), lim_up_(lim_up), lim_dn_(lim_dn) {}

  inline void compute() {
    if (md_.close.back() > 0 && md_.bid_volume.back() + md_.ask_volume.back() > 0) {
      y[can_buy] = md_.low.back() >= lim_up_.back() - kPxEps ? 0.0f : 1.0f;
      y[can_sell] = md_.high.back() <= lim_dn_.back() + kPxEps ? 0.0f : 1.0f;
      if (!open_ok_ && (md_.index == kAuctionMin || (md_.index >= kContOpenMin && md_.index < kContCloseMin))) {
        const float px = md_.index == kAuctionMin ? md_.close.back() : md_.open.back();
        y[can_buy_open] = px >= lim_up_.back() - kPxEps ? 0.0f : 1.0f;
        y[can_sell_open] = px <= lim_dn_.back() + kPxEps ? 0.0f : 1.0f;
        open_ok_ = true;
      }
    }
  }

  inline void reset() {
    for (float &v : y)
      v = 0.0f;
    open_ok_ = false;
  }

private:
  // L_1min 行号: 09:25 撮合 bar / 连续竞价 [09:30, 14:57) (与 LabelHold kAuctionMin / kContOpenMin / kContCloseMin 同值)
  static constexpr uint32_t kAuctionMin = 10, kContOpenMin = 15, kContCloseMin = 252;

  const MinuteData &md_;
  const Series &lim_up_, &lim_dn_; // Fund 日频口 (盘中 back() = 当日适用)
  bool open_ok_ = false;           // 当日已截到开盘价
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Tradable(N) N(Tradable, (Tradable), (minute_data, Fund.out(Fund.lim_up), Fund.out(Fund.lim_dn)), onMinute)

// 列名带层后缀 (_<n>min / _day)
#define BAR_TRADABLE(X, CAT1, sfx, cn)                                                                                                                                                                                                                                                   \
  X(can_buy_##sfx, CAT1, AUTO, "Can Buy (" #sfx ")", cn "可买", "行末可买入=1: 最近有成交分钟未封涨停 (low < 涨停价); 当日尚无成交=0", R"(\mathbf{1}[P^{\mathrm{low}}_{\tau} < P^{up}_{D} - \varepsilon])", TS(Tradable, can_buy, None, None))                                           \
  X(can_sell_##sfx, CAT1, AUTO, "Can Sell (" #sfx ")", cn "可卖", "行末可卖出=1: 最近有成交分钟未封跌停 (high > 跌停价); 当日尚无成交=0", R"(\mathbf{1}[P^{\mathrm{high}}_{\tau} > P^{dn}_{D} + \varepsilon])", TS(Tradable, can_sell, None, None))                                      \
  X(can_buy_open_##sfx, CAT1, AUTO, "Can Buy at Open (" #sfx ")", cn "开盘可买", "当日开盘可买入=1: 开盘价 (09:25撮合价, 无撮合则连续竞价首个有成交bar的开盘价) < 涨停价; 尚未开盘=0", R"(\mathbf{1}[P^{open}_{D} < P^{up}_{D} - \varepsilon])", TS(Tradable, can_buy_open, None, None)) \
  X(can_sell_open_##sfx, CAT1, AUTO, "Can Sell at Open (" #sfx ")", cn "开盘可卖", "当日开盘可卖出=1: 开盘价 > 跌停价; 尚未开盘=0", R"(\mathbf{1}[P^{open}_{D} > P^{dn}_{D} + \varepsilon])", TS(Tradable, can_sell_open, None, None))
#define FIELDS_L_5min_Tradable(X, CAT1) BAR_TRADABLE(X, CAT1, 5min, "5分钟")
#define FIELDS_L_15min_Tradable(X, CAT1) BAR_TRADABLE(X, CAT1, 15min, "15分钟")
#define FIELDS_L_30min_Tradable(X, CAT1) BAR_TRADABLE(X, CAT1, 30min, "30分钟")
#define FIELDS_L_60min_Tradable(X, CAT1) BAR_TRADABLE(X, CAT1, 60min, "60分钟")
#define FIELDS_L_1day_Tradable(X, CAT1) BAR_TRADABLE(X, CAT1, day, "日")
