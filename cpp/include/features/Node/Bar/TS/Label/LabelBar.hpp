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
//   exit:
//     MIN   行 m 持有 n 分钟 = entry(m+n) (同一基准, 相邻行精确伸缩, 与 Lob 档同构); m+n 过收盘 → 收盘价 (T0 当日平仓)
//     CLOSE 当日收盘价 = 末个有成交 bar 的 close (有收盘竞价即撮合价)
//     OPEN  T+n 开盘价 = 09:25 撮合 bar 的价, 无撮合则首个有成交连续竞价 bar 的 open; 当日无开盘 (停牌) → 顺延,
//           最多再等 n 日 (D+2n 仍无 → 最近收盘价); 越过区间末 → 最后一日收盘价
//   全日无成交 (停牌 / 无 bar1m): 分钟 / 收盘型全行留 0, 当日不悬挂; 开盘型到期照常结算.
//   价格均为 bar1m 价 × 当日复权比 (Fund adj_ratio, 日内恒定): 日内型与不复权同值, 开盘型跨除权日的收益不含除权缺口 (与 Lob 档同约).
//   落盘层: 不落 L_1min (255 行代价太大), 只落 L_5min / L_15min / L_30min / L_60min / L_1day —— 行 = 该层行的末分钟 m 的标签;
//     每层只留本层及以上的持有期组 (LABEL_GROUPS_FROM_<LVL>: 5min 层无 1m, L_1day 只有开盘型). 逐分钟照算, 只是不落非行末分钟.
//
// 非 DAG 节点 (未来标签需回填, 不走 Node), CoreTs 驱动, 接口与 LabelLob 对仗:
//   minute_closed(t_min, writer)   有成交的分钟 (onMinute 之后) 一调: 截开盘价, 连续竞价分钟的 VWAP 填给之前悬着的行 = 这些行的 entry,
//                               行 e 的 entry 到位即行 e−n 的分钟型 exit 到位, 顺带写 (e−n, n).
//   day_begin / day_end         日历: 日期轴每一日各调一次 (无数据日也调, 由 CoreTs 保证).
//   finish_all                  回测区间末: 悬挂日全部按最近一次收盘价结算, 全部释放.
// 开盘型跨日回填 / 悬挂日环 / 写句柄延迟归还见 Common/Label/LabelHold.hpp (两档同一骨架).
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/Node/Common/Label/LabelGroups.hpp"
#include "features/Node/Common/Label/LabelHold.hpp"

// 分钟 VWAP entry (bar1m 无盘口 → 只有价)
struct BarEntry {
  float px = 0.0f;
};

class LabelBar : public LabelHold<BarEntry> {
public:
  static constexpr size_t MIN_LABEL_COUNT = HOLD_COUNT * GROUP_SIZE; // LB 块总列数

  LabelBar(const MinuteData &md, const Series &adj_ratio) : md_(md), adj_ratio_(adj_ratio) {}

  // 有成交的分钟 bar 结算后 (CoreTs 只在有成交的分钟调). writer(col, n, t_min, values, days_ago), 此处 days_ago 恒 0
  template <class Writer>
  inline void minute_closed(size_t t_min, Writer &&writer) {
    assert(px_scale_ > 0.0f && "有成交但当日无复权比 (bar1d adjust_factor 缺失)");
    last_px_ = md_.close.back() * px_scale_; // 当日末笔成交价 (收盘价候选)
    has_px_ = true;
    if (!open_at(t_min, md_))
      return; // 集合竞价 bar 不作 entry
    const BarEntry e{md_.bid_amount.back() / static_cast<float>(md_.bid_volume.back()) * px_scale_};
    Pend &p = today();
    for (; p.n_entry < t_min; ++p.n_entry)
      p.entry[p.n_entry] = e; // 行 m < t_min (m+1 ≤ t_min) 悬着的 entry 全部到位
    emit_min(writer);
  }

  // 收盘结算 (当日有无成交皆调): 收盘价 = 当日末笔成交价; release(days_ago) = 该日全部组已写完
  template <class Writer, class Release>
  inline void day_end(Writer &&writer, Release &&release) {
    close_day(has_px_, last_px_, BarEntry{last_px_}, writer, release, [](size_t, const BarEntry &) {});
  }

  // 每日重置: 行游标 / 当日开盘 / 当日末笔归零; 取当日复权比 (节点 carry 之后调, Fund back() = 当日)
  inline void reset() {
    LabelHold::reset();
    has_px_ = false;
    px_scale_ = adj_ratio_.back();
  }

private:
  const MinuteData &md_;    // 当前 bar (close / open / bid 侧 = bar1m 总量额)
  const Series &adj_ratio_; // Fund 日频口

  float last_px_ = 0.0f; // 当日末笔成交价 (has_px_ = 当日有过成交; reset 清)
  bool has_px_ = false;
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 行序: 每个 hold 一组 [long, short] (组序 = LABEL_GROUPS 序; 层内按名定址回 LB 块 hold_col(h), 见 CoreTs). 英 / 中片段见 LabelGroups.hpp
#define LBAR_NOTE_MIN(n) "行m+" #n "的entry(其后首个有成交连续竞价分钟的VWAP, 与entry同基准, 相邻行精确伸缩); 过收盘则按当日收盘价"
#define LBAR_NOTE_CLOSE(n) "当日收盘价(末笔成交价)"
#define LBAR_NOTE_OPEN(n) "T+" #n "日开盘价(09:25集合竞价撮合价, 无撮合则连续竞价首个有成交bar的开盘价); 当日无开盘则顺延至之后首个有开盘日(最多再等" #n "日, 仍无则取最近收盘价); 越过区间末则持有到最后一日收盘"
#define LBAR_TEX_MIN(n) R"(, \quad P_{exit}=\mathrm{VWAP}_{t_{entry}+)" #n R"(\,\mathrm{min}})"
#define LBAR_TEX_CLOSE(n) R"(, \quad P_{exit}=P^{close}_{D})"
#define LBAR_TEX_OPEN(n) R"(, \quad P_{exit}=P^{open}_{D+)" #n "}"
#define LBAR_ROW(X, CAT1, side, en, cn, formula, name, kind, n)                                       \
  X(lb_##side##_##name, CAT1, ret, en " " LABEL_EN_##kind(n) " Return", cn LABEL_CN_##kind(n) "收益", \
    cn LABEL_CN_##kind(n) "价格收益(比例; 不含冲击与税佣). entry: 其后首个有成交连续竞价分钟的VWAP, 无成交顺延, 再无则按收盘价建仓. exit: " LBAR_NOTE_##kind(n), formula LBAR_TEX_##kind(n), LABEL)
#define LBAR_ROW_LONG(name, kind, n, X, CAT1) LBAR_ROW(X, CAT1, long, "Long", "做多", R"(\frac{P_{exit}-\mathrm{VWAP}_{entry}}{\mathrm{VWAP}_{entry}})", name, kind, n)
#define LBAR_ROW_SHORT(name, kind, n, X, CAT1) LBAR_ROW(X, CAT1, short, "Short", "做空", R"(\frac{\mathrm{VWAP}_{entry}-P_{exit}}{\mathrm{VWAP}_{entry}})", name, kind, n)
#define LBAR_GROUP(name, kind, n, X, CAT1) LBAR_ROW_LONG(name, kind, n, X, CAT1) LBAR_ROW_SHORT(name, kind, n, X, CAT1)

#define FIELDS_L_5min_LabelBar(X, CAT1) LABEL_GROUPS_FROM_L_5min(LBAR_GROUP, X, CAT1)
#define FIELDS_L_15min_LabelBar(X, CAT1) LABEL_GROUPS_FROM_L_15min(LBAR_GROUP, X, CAT1)
#define FIELDS_L_30min_LabelBar(X, CAT1) LABEL_GROUPS_FROM_L_30min(LBAR_GROUP, X, CAT1)
#define FIELDS_L_60min_LabelBar(X, CAT1) LABEL_GROUPS_FROM_L_60min(LBAR_GROUP, X, CAT1)
#define FIELDS_L_1day_LabelBar(X, CAT1) LABEL_GROUPS_FROM_L_1day(LBAR_GROUP, X, CAT1)
