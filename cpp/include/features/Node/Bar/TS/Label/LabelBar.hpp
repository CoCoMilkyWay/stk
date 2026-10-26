#pragma once

// =============================================================================
// LabelBar - Bar 档价格收益标签 (分钟 VWAP 口径; bar1m 无盘口 → 无冲击成本列)
// =============================================================================
//   标签 (lb_long_<h> / lb_short_<h>, 组见 Common/Label/LabelGroups.hpp, 与 Lob 档同名同组) = 纯价格收益, 不含冲击与税佣:
//     做多: (exit − entry) / entry        做空: (entry − exit) / entry
//   entry(m) = 行 m 之后首个有成交的连续竞价分钟 r ∈ [m+1, kContCloseMin) 的 VWAP (bar1m amount / volume):
//     行 m 的特征在分钟 m 末才可知, 最早在分钟 m+1 内成交; 无成交分钟顺延 (稀疏 / 盘中停牌),
//     09:30 前的行 (含 09:25 撮合) 顺延到开盘后首个有成交分钟; 集合竞价 bar 不作 entry (与 Lob 档"集合竞价簿不可成交"同约).
//     之后再无连续竞价成交 (行 m ≥ kContCloseMin − 1 / 尾盘无成交) → 收盘价建仓 (分钟 / 收盘型即建即平 = 0).
//   名义平仓:
//     MIN     行 m 持有 n 分钟 = entry(m+n) (同一基准, 相邻行精确伸缩, 与 Lob 档同构); m+n 过收盘 → 收盘价 (T0 当日平仓)
//     CLOSE   当日收盘价 = 末个有成交 bar 的 close (有收盘竞价即撮合价)
//     OPEN    T+n 开盘价 = 09:25 撮合 bar 的价, 无撮合则首个有成交连续竞价 bar 的 open
//     CLOSE_N T+n 收盘价
//   可成交: 分钟价点 = Tradable 节点本分钟 can_buy / can_sell (整分钟封板判); 开盘 / 收盘价点按单一价撮合判 (LabelHold::auction_px).
//     建仓作废 / 平仓顺延 / 环满按最近收盘价结清 见 Common/Label/LabelHold.hpp (两档同一骨架).
//   全日无成交 (停牌 / 无 bar1m): 当日行无 entry, 不写; 名义今日到期的组进等待.
//   价格均为 bar1m 价 × 当日复权比 (Fund adj_ratio, 日内恒定): 日内型与不复权同值, 日级型跨除权日的收益不含除权缺口 (与 Lob 档同约).
//   落盘层: 不落 L_1min (255 行代价太大), 只落 L_5min / L_15min / L_30min / L_60min / L_1day —— 行 = 该层行的末分钟 m 的标签;
//     每层只留本层及以上的持有期组 (LABEL_GROUPS_FROM_<LVL>: 5min 层无 1m, L_1day 只有日级型). 逐分钟照算, 只是不落非行末分钟.
//
// 非 DAG 节点 (未来标签需回填, 不走 Node), CoreTs 驱动, 接口与 LabelLob 对仗:
//   minute_closed(t_min, writer)   有成交的分钟 (onMinute 之后) 一调: 截开盘价点, 连续竞价分钟的 VWAP 价点填给之前悬着的行 = 这些行的 entry.
//   day_begin / day_end         日历: 日期轴每一日各调一次 (无数据日也调, 由 CoreTs 保证).
//   finish_all                  回测区间末: 悬挂日全部按最近一次收盘价结清, 全部释放.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/Node/Common/Label/LabelGroups.hpp"
#include "features/Node/Common/Label/LabelHold.hpp"

class LabelBar : public LabelHold {
public:
  static constexpr size_t MIN_LABEL_COUNT = HOLD_COLS; // LB 块总列数

  // can_buy / can_sell = Tradable 节点本分钟的可成交标记 (onMinute 已算, 先于 minute_closed)
  LabelBar(const MinuteData &md, const float &can_buy, const float &can_sell, const Series &lim_up, const Series &lim_dn, const Series &adj_ratio)
      : md_(md), can_buy_(can_buy), can_sell_(can_sell), lim_up_src_(lim_up), lim_dn_src_(lim_dn), adj_ratio_(adj_ratio) {}

  // 有成交的分钟 bar 结算后 (CoreTs 只在有成交的分钟调). writer(col, n, t_min, values, days_ago)
  template <class Writer>
  inline void minute_closed(size_t t_min, Writer &&writer) {
    assert(px_scale_ > 0.0f && "有成交但当日无复权比 (bar1d adjust_factor 缺失)");
    last_raw_ = md_.close.back(); // 当日末笔成交价 (收盘价候选)
    if (!open_at(t_min, md_, writer))
      return; // 集合竞价 bar 不作 entry
    const LabelPx e{md_.bid_amount.back() / static_cast<float>(md_.bid_volume.back()) * px_scale_, can_buy_ != 0.0f, can_sell_ != 0.0f};
    while (n_entry() < t_min)
      push_entry(e, writer); // 行 m < t_min (m+1 ≤ t_min) 悬着的 entry 全部到位
  }

  // 收盘结算 (当日有无成交皆调): 收盘价点 = 当日末笔成交价 (单一价撮合口径判可成交); release(days_ago) = 该日写完
  template <class Writer, class Release>
  inline void day_end(Writer &&writer, Release &&release) {
    close_day(last_raw_ > 0.0f, auction_px(last_raw_), writer, release, [](size_t, const LabelPx &) {});
  }

  // 每日重置: 当日开盘 / 当日末笔归零; 取当日涨跌停 / 复权比 (节点 carry 之后调, Fund back() = 当日)
  inline void reset() {
    LabelHold::reset();
    assert(!lim_up_src_.empty() && !lim_dn_src_.empty() && !adj_ratio_.empty() && "LabelBar: Fund 日频口盘前须已承接");
    last_raw_ = 0.0f;
    lim_up_ = lim_up_src_.back();
    lim_dn_ = lim_dn_src_.back();
    px_scale_ = adj_ratio_.back();
  }

private:
  const MinuteData &md_;                   // 当前 bar (close / open / bid 侧 = bar1m 总量额)
  const float &can_buy_, &can_sell_;       // Tradable 节点 y (本分钟)
  const Series &lim_up_src_, &lim_dn_src_; // Fund 日频口 (lim_up / lim_dn)
  const Series &adj_ratio_;                // Fund 日频口

  float last_raw_ = 0.0f; // 当日末笔成交价 (原始价; 0 = 当日尚无成交; reset 清)
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 行序: 每个 hold 一组 [long, short] (组序 = LABEL_GROUPS 序; 层内按名定址回 LB 块 hold_col(h), 见 CoreTs). 英 / 中片段见 LabelGroups.hpp
#define LBAR_NOTE_MIN(n) "行m+" #n "的entry(其后首个有成交连续竞价分钟的VWAP, 与entry同基准, 相邻行精确伸缩); 过收盘则按当日收盘价"
#define LBAR_NOTE_CLOSE(n) "当日收盘价(末笔成交价)"
#define LBAR_NOTE_OPEN(n) "T+" #n "日开盘价(09:25集合竞价撮合价, 无撮合则连续竞价首个有成交bar的开盘价)"
#define LBAR_NOTE_CLOSE_N(n) "T+" #n "日收盘价(末笔成交价)"
#define LBAR_TEX_MIN(n) R"(, \quad P_{exit}=\mathrm{VWAP}_{t_{entry}+)" #n R"(\,\mathrm{min}})"
#define LBAR_TEX_CLOSE(n) R"(, \quad P_{exit}=P^{close}_{D})"
#define LBAR_TEX_OPEN(n) R"(, \quad P_{exit}=P^{open}_{D+)" #n "}"
#define LBAR_TEX_CLOSE_N(n) R"(, \quad P_{exit}=P^{close}_{D+)" #n "}"
#define LBAR_ROW(X, CAT1, side, en, cn, formula, name, kind, n)                                       \
  X(lb_##side##_##name, CAT1, ret, en " " LABEL_EN_##kind(n) " Return", cn LABEL_CN_##kind(n) "收益", \
    cn LABEL_CN_##kind(n) "价格收益(比例; 不含冲击与税佣; 后复权). entry: 其后首个有成交连续竞价分钟的VWAP, 无成交顺延, 再无则按收盘价建仓; entry价不可两向成交(封板)则本行NaN. exit: " LBAR_NOTE_##kind(n) "; 该腿不可成交(封板)或停牌则顺延至之后首个该腿可成交价点, 环满(" LABEL_DELAY_NOTE ")按最近收盘价; 越过区间末则按最后一日收盘价", formula LBAR_TEX_##kind(n), LABEL)
#define LBAR_ROW_LONG(name, kind, n, X, CAT1) LBAR_ROW(X, CAT1, long, "Long", "做多", R"(\frac{P_{exit}-\mathrm{VWAP}_{entry}}{\mathrm{VWAP}_{entry}})", name, kind, n)
#define LBAR_ROW_SHORT(name, kind, n, X, CAT1) LBAR_ROW(X, CAT1, short, "Short", "做空", R"(\frac{\mathrm{VWAP}_{entry}-P_{exit}}{\mathrm{VWAP}_{entry}})", name, kind, n)
#define LBAR_GROUP(name, kind, n, X, CAT1) LBAR_ROW_LONG(name, kind, n, X, CAT1) LBAR_ROW_SHORT(name, kind, n, X, CAT1)

#define FIELDS_L_5min_LabelBar(X, CAT1) LABEL_GROUPS_FROM_L_5min(LBAR_GROUP, X, CAT1)
#define FIELDS_L_15min_LabelBar(X, CAT1) LABEL_GROUPS_FROM_L_15min(LBAR_GROUP, X, CAT1)
#define FIELDS_L_30min_LabelBar(X, CAT1) LABEL_GROUPS_FROM_L_30min(LBAR_GROUP, X, CAT1)
#define FIELDS_L_60min_LabelBar(X, CAT1) LABEL_GROUPS_FROM_L_60min(LBAR_GROUP, X, CAT1)
#define FIELDS_L_1day_LabelBar(X, CAT1) LABEL_GROUPS_FROM_L_1day(LBAR_GROUP, X, CAT1)
