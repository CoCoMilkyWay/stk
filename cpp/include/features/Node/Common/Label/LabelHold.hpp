#pragma once

// =============================================================================
// LabelHold - 持有期标签的骨架 (LabelBar 继承; 持有期表 / 时钟 / 结清窗见 define/Label.hpp)
// =============================================================================
//   价点 LabelPx = 一次报价: 后复权价 + 该价能否买入 / 卖出. 派生类只管价点怎么来, 骨架管持有期组的全部日历与结算.
//   全仓只做多; 一个持有期组 = 做多收益列 lb_<h> (平仓顺延) + 偏移型另有名义收益列 lb_nom_<h> (名义价点即平, 不顺延: 续持者的估值).
//   可成交性全在标签里定 (消费端只读标签, 不另读可成交列):
//     行 m 的 entry 价点 (push_entry): 行末分钟 m 之后首个价点 (有成交连续竞价分钟; 再无 → 收盘价点).
//     【建仓可成交码】ENTRY_COL (lb_entry_ok) = entry 价点 可买 1 | 可卖 2 (封涨停只可卖, 封跌停只可买). 收益照写 (不作废):
//       该行能否真建仓 / 已持仓能否续持 由消费端按持仓判 (Stat 契约【仓位感知】). 价点上不可能两向都堵 (无价点 = 无行).
//     名义平仓价点: Min = 行 m+n 的 entry 价点 (越过收盘 → 收盘价点); Close = 当日收盘; Open = T+n 开盘; Same = T+n 日行 m 的 entry 价点.
//     【平仓顺延】做多腿: 名义价点卖不出 (封跌停) 或名义日无成交 (停牌) → 该 (行, 组) 进等待, 之后首个可卖价点平仓
//       (盘中价点 / 收盘 / 开盘; 分钟级 / 日级同一规则, 可跨日). 名义腿: 名义价点即平; 名义日停牌 → 之后首个价点.
//       等待到悬挂环满 (行所在日起 kPendDays − 1 个交易日) 仍未平 → 按最近一次收盘价估值结清.
//     价点来源: 盘中 = push_entry; 开盘 = open_at (09:25 撮合价, 无撮合则首个有成交连续竞价 bar 的 open); 收盘 = close_day.
//     开盘 / 收盘价点的可成交 (auction_px, 单一价撮合): 价 < 涨停 − ε 可买, 价 > 跌停 + ε 可卖; 涨跌停 NaN (无限制) → 可成交.
//     day_begin / finish_all / reset   日历 / 区间末 (全部按最近收盘价结清) / 每日重置
//     snap / held                      续算点 (跨日状态全部) / 日 age 的写句柄是否仍持 (续算接回尾窗日时 CoreTs 据此归还)
//   跨日回填: 日 D 的行 entry 存在悬挂槽里, 日级组 / 等待中的组写完前 D 的写句柄由 CoreTs 持着 (writer 带 days_ago);
//   写完的日在收盘 / 区间末按日序 release (老日未写完则新日也不放: FeatureStore "d+1 计满 ⇒ d 计满").
//   writer(col, n, t_min, values, days_ago) 写 LB 块内偏移 col 起的 n 列 (分钟 t_min 的标签; 落哪些层的哪行由 CoreTs 定);
//   writer.rows(col) = 列 col 落盘的行集 (行末分钟): 骨架只结算落盘行 —— 不落盘的 (行, 列) 不写可成交码, 不平仓, 不进等待;
//     entry 价点逐行照收 (分钟型名义平仓 = 后续行的 entry). 一组各列落盘行相同 (rows_ok).
//   release(days_ago) = 该日全部组已写完, 写句柄可归还.
//   LB 块布局: 按 HOLDS 序每组 [lb_<h>, (偏移型) lb_nom_<h>], 之后 lb_entry_ok (ENTRY_COL); 字段表 LB(port) 直连 enum Out 定址.
// =============================================================================

#include "define/Label.hpp"
#include "features/DataDefine.hpp"
#include "features/Node/Common/Label/RowSet.hpp"
#include "features/TimeIndex.hpp"
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>

// 价点: 后复权价 + 该价能否买入 / 卖出 (建仓的两向记进可成交码, 做多平仓要可卖)
struct LabelPx {
  float px = 0.0f;
  bool buy = false, sell = false;
  float code() const { return static_cast<float>((buy ? 1 : 0) | (sell ? 2 : 0)); } // lb_entry_ok
  bool exits(size_t leg) const { return leg != 0 || sell; }                         // 做多腿 (0) 要可卖; 名义腿 (1) 任一价点即平
};

// LB 块布局: 组 h 的列数 (偏移型 [做多, 名义], 锚点型 [做多]) / 组 h 的首列
inline constexpr std::array<size_t, kHoldCount + 1> kLabelCol = [] {
  std::array<size_t, kHoldCount + 1> c{};
  for (size_t h = 0; h < kHoldCount; ++h)
    c[h + 1] = c[h] + (hold_kind(static_cast<HoldId>(h)) == HoldKind::Min || hold_kind(static_cast<HoldId>(h)) == HoldKind::Same ? 2 : 1);
  return c;
}();
constexpr size_t label_legs(size_t h) { return kLabelCol[h + 1] - kLabelCol[h]; }
constexpr size_t label_col(size_t h) { return kLabelCol[h]; }

class LabelHold {
public:
  using RowSet = LabelRows;
  static constexpr size_t HOLD_COUNT = kHoldCount;
  static constexpr size_t LEG_MAX = 2; // [做多, 名义]
  static constexpr size_t legs(size_t h) { return label_legs(h); }
  static constexpr size_t hold_col(size_t h) { return label_col(h); }
  static constexpr size_t ENTRY_COL = label_col(HOLD_COUNT); // lb_entry_ok (建仓可成交码)
  static constexpr size_t MIN_LABEL_COUNT = ENTRY_COL + 1;   // LB 块总列数

// LB 块输出口 (字段表 LB(port) 的 port): 枚举值 = LB 块偏移 (组序 = HOLDS 序, 组内 [ret, nom], 末位 entry_ok)
#define LABEL_OUT_NOM_Min(tok) nom_##tok,
#define LABEL_OUT_NOM_Close(tok)
#define LABEL_OUT_NOM_Open(tok)
#define LABEL_OUT_NOM_Same(tok) nom_##tok,
#define LABEL_OUT_ONE(Id, tok, kind, n, ...) ret_##tok, LABEL_OUT_NOM_##kind(tok)
  enum Out : size_t { HOLDS(LABEL_OUT_ONE) entry_ok };
#undef LABEL_OUT_ONE
#undef LABEL_OUT_NOM_Min
#undef LABEL_OUT_NOM_Close
#undef LABEL_OUT_NOM_Open
#undef LABEL_OUT_NOM_Same
  static_assert(Out::entry_ok == ENTRY_COL);
  static_assert(kHoldCount <= 32, "日级组排期位图用 uint32_t");
  static_assert([] {
    for (size_t h = 0; h < kHoldCount; ++h)
      if (hold_kind(static_cast<HoldId>(h)) != HoldKind::Close && hold_n(static_cast<HoldId>(h)) < 1)
        return false;
    return true;
  }(),
                "分钟型 / 日级型的 n 须 ≥ 1");

  static constexpr size_t PEND_DAYS = kPendDays;
  // FeatureStore 日环下限: 悬挂日全 BUSY 时同日的 TS 还要 1 个 slot 才能推进; 再 +1: IO(d) 等 CS(d+1) close (截面跨日读 d−1 槽位,
  // 见 FeatureStore::io_try_open), d 的 slot 要多持到 d+1 结清 (其余 CS / IO 阶段的 slot 另算, 只影响吞吐不致死锁)
  static constexpr size_t MIN_RING_SLOTS = PEND_DAYS + 2;

  static_assert(kAuctionMin == (L2::MORNING_MATCHING_START_HOUR * 60 + L2::MORNING_MATCHING_START_MINUTE) - MORNING_START_MIN);
  static_assert(kContOpenMin == (L2::CONTINUOUS_TRADING_MORNING_START_HOUR * 60 + L2::CONTINUOUS_TRADING_MORNING_START_MINUTE) - MORNING_START_MIN);
  static_assert(static_cast<size_t>(kContCloseMin) == MORNING_MINUTES + (AFTERNOON_END_MIN - AFTERNOON_START_MIN));

  // 落盘行约束 (CoreTs 编译期查; rows[c] = LB 块偏移 c 的落盘行): 骨架按组结算, 一组各列落盘行须相同
  template <class Rows>
  static constexpr bool rows_ok(const Rows &rows) {
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      for (size_t s = 1; s < legs(h); ++s)
        if (!(rows[hold_col(h) + s] == rows[hold_col(h)]))
          return false;
    return true;
  }

  // ---- 日历 (日期轴每一日各调一次, 无数据日也调) ----

  // 新的一日: 悬挂环前进. 被顶掉的槽必已结清 (close_day 每日按环满结清最老一日)
  inline void day_begin() {
    cur_ = (cur_ + 1) % PEND_DAYS;
    Pend &p = pend_[cur_];
    assert(!p.held && "悬挂日环被顶掉的日尚未结清");
    p.held = true;
    p.sched = 0;
    p.n_entry = 0;
  }

  // 回测区间末: 悬挂日全部按最近一次收盘价结清 (持有到最后一日收盘), 全部释放
  template <class Writer, class Release>
  inline void finish_all(Writer &&writer, Release &&release) {
    for (size_t age = PEND_DAYS; age-- > 0;)
      mark_close(age, writer);
    release_settled(release);
    assert(n_wait_[0] == 0 && n_wait_[1] == 0);
  }

  // 每日重置: 当日开盘归零
  inline void reset() { open_ok_ = false; }

  // 续算点: 跨日状态 (悬挂日环 + 等待计数 + 估值价); 日内状态由次日 reset / 派生类 reset 重给
  template <class Ar>
  void snap(Ar &ar) {
    ar(pend_);
    ar(cur_);
    ar(n_wait_);
    ar(last_close_);
    ar(has_last_);
  }
  bool held(size_t age) const { return pend_[(cur_ + PEND_DAYS - age) % PEND_DAYS].held; }

protected:
  float px_scale_ = 1.0f;               // 价格缩放 = 当日复权比 (派生类 reset 时设; 跨日收益不含除权缺口)
  float lim_up_ = kNaN, lim_dn_ = kNaN; // 当日涨跌停价 (原始价; 派生类 reset 时设)

  // 单一价撮合 (开盘 / 收盘) 的价点: raw = 原始价
  LabelPx auction_px(float raw) const { return {raw * px_scale_, !(raw >= lim_up_ - kPxEps), !(raw <= lim_dn_ + kPxEps)}; }

  // 当日已定 entry 的行数 (行按锚点单调, 前缀)
  size_t n_entry() const { return pend_[cur_].n_entry; }

  // 当日下一行的 entry 价点 (days_ago 恒 0): 写可成交码; 行 r 到位 = 行 r−n 的分钟型名义平仓到位 = n 日前行 r 的同刻型名义平仓到位; 再喂等待
  template <class Writer>
  inline void push_entry(const LabelPx &e, Writer &&writer) {
    Pend &p = pend_[cur_];
    assert(p.n_entry < TRADE_MINUTES_PER_DAY);
    assert((e.buy || e.sell) && "价点不可能两向都不可成交");
    const size_t r = p.n_entry++;
    p.entry[r] = e;
    if (writer.rows(ENTRY_COL).has(r)) {
      const float code = e.code();
      writer(ENTRY_COL, size_t{1}, r, &code, size_t{0});
    }
    for (size_t h = 0; h < HOLD_COUNT; ++h) {
      const HoldId id = static_cast<HoldId>(h);
      const size_t n = static_cast<size_t>(hold_n(id));
      if (hold_kind(id) == HoldKind::Min) {
        if (r >= n && writer.rows(hold_col(h)).has(r - n))
          exit_row(p, 0, h, r - n, e, writer);
      } else if (hold_kind(id) == HoldKind::Same) { // n 日前排着本组 (该日有成交) → 其行 r 按本价点平
        Pend &q = at(n);
        if ((q.sched >> h & 1u) && writer.rows(hold_col(h)).has(r))
          exit_row(q, n, h, r, e, writer);
      }
    }
    resolve(e, writer);
  }

  // 有成交的分钟 bar 截开盘价点 (当日首个: 09:25 撮合 bar 的撮合价, 无撮合则首个连续竞价 bar 的首笔成交价 = 交易所开盘价定义):
  // 名义 T+n 开盘到期的组平仓, 再喂等待. 返回该 bar 是否连续竞价 (可作 entry)
  template <class Writer>
  inline bool open_at(size_t t_min, const MinuteData &md, Writer &&writer) {
    assert((t_min != static_cast<size_t>(kAuctionMin) || !open_ok_) && "09:25 撮合 bar 之前不该有开盘价");
    const bool cont = t_min >= static_cast<size_t>(kContOpenMin) && t_min < static_cast<size_t>(kContCloseMin);
    if (open_ok_ || (t_min != static_cast<size_t>(kAuctionMin) && !cont))
      return cont;
    open_ok_ = true;
    const LabelPx o = auction_px(t_min == static_cast<size_t>(kAuctionMin) ? md.close : md.open);
    for (size_t age = 1; age < PEND_DAYS; ++age) {
      Pend &q = at(age);
      for (size_t h = 0; h < HOLD_COUNT; ++h)
        if (hold_kind(static_cast<HoldId>(h)) == HoldKind::Open && static_cast<size_t>(hold_n(static_cast<HoldId>(h))) == age && (q.sched >> h & 1u)) {
          exit_rows(q, age, h, o, writer);
          q.sched &= ~(1u << h);
        }
    }
    resolve(o, writer);
    return cont;
  }

  // 收盘结算 (当日有无成交皆调). traded: 当日有收盘价点 close.
  //   尾部行 (其后再无盘中价点) 按收盘价点建仓;
  //   分钟型尾部 (名义平仓过收盘) / 收盘型全行 → 收盘价点平仓; 名义今日的开盘型 (今日开盘未到) → 收盘价点平仓;
  //   名义今日的同刻型: 盘中各行已随 push_entry 平, 尾部行随收盘价点补齐; 再喂等待.
  //   无成交: 名义今日到期的日级组全部进等待. 之后环满的最老一日按最近收盘价结清, 写完的日按日序释放
  template <class Writer, class Release>
  inline void close_day(bool traded, const LabelPx &close, Writer &&writer, Release &&release) {
    Pend &p = pend_[cur_];
    for (size_t age = 1; age < PEND_DAYS; ++age) {
      Pend &q = at(age);
      for (size_t h = 0; h < HOLD_COUNT; ++h) {
        const HoldId id = static_cast<HoldId>(h);
        if ((q.sched >> h & 1u) && static_cast<size_t>(hold_n(id)) <= age && !(traded && hold_kind(id) == HoldKind::Same)) { // Open 到期未开盘 / 无成交日到期
          if (traded)
            exit_rows(q, age, h, close, writer);
          else
            wait_rows(q, h, writer.rows(hold_col(h)));
          q.sched &= ~(1u << h);
        }
      }
    }
    if (traded) {
      last_close_ = close.px;
      has_last_ = true;
      while (p.n_entry < TRADE_MINUTES_PER_DAY)
        push_entry(close, writer);
      for (size_t age = 1; age < PEND_DAYS; ++age) // 名义今日的同刻型: 全部行已在 push_entry 平 (或进等待)
        for (size_t h = 0; h < HOLD_COUNT; ++h)
          if (hold_kind(static_cast<HoldId>(h)) == HoldKind::Same && static_cast<size_t>(hold_n(static_cast<HoldId>(h))) <= age)
            at(age).sched &= ~(1u << h);
      for (size_t h = 0; h < HOLD_COUNT; ++h) {
        const HoldId id = static_cast<HoldId>(h);
        if (hold_kind(id) == HoldKind::Min) {
          const size_t n = std::min(static_cast<size_t>(hold_n(id)), TRADE_MINUTES_PER_DAY);
          for (size_t m = TRADE_MINUTES_PER_DAY - n; m < TRADE_MINUTES_PER_DAY; ++m)
            if (writer.rows(hold_col(h)).has(m))
              exit_row(p, 0, h, m, close, writer);
        } else if (hold_kind(id) == HoldKind::Close) {
          exit_rows(p, 0, h, close, writer);
        }
      }
      resolve(close, writer);
      p.sched = kDailyMask;
    }
    mark_close(PEND_DAYS - 1, writer);
    release_settled(release);
  }

private:
  static constexpr uint32_t kDailyMask = [] {
    uint32_t m = 0;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (hold_daily(static_cast<HoldId>(h)))
        m |= 1u << h;
    return m;
  }();

  // 悬挂日槽: 一日的 entry 价点 (全部组共用) + 日级组排期 + 等待平仓的 (组, 腿) 行集
  struct Pend {
    bool held = false;                    // 写句柄未归还
    uint32_t sched = 0;                   // 名义平仓未到的日级组 (位 = h)
    size_t n_entry = 0;                   // 已定 entry 的行数
    LabelPx entry[TRADE_MINUTES_PER_DAY]; // 各行 entry 价点
    RowSet wait[HOLD_COUNT][LEG_MAX];     // 名义价点平不了, 等之后首个该腿可平价点 (名义腿只在名义日停牌时等)
    bool busy() const {
      if (sched)
        return true;
      for (const auto &hw : wait)
        for (const RowSet &s : hw)
          if (s.any())
            return true;
      return false;
    }
  };

  Pend &at(size_t age) { return pend_[(cur_ + PEND_DAYS - age) % PEND_DAYS]; }

  // 做多价格收益率 (两腿同式, 只差平仓价点)
  static inline float calc_return(float entry_px, float exit_px) {
    assert(entry_px > 0.0f && exit_px > 0.0f && "entry / 可平仓价点必有正价");
    return (exit_px - entry_px) / entry_px;
  }

  template <class Writer>
  static inline void write_leg(size_t age, size_t h, size_t s, size_t m, float entry_px, float exit_px, Writer &&writer) {
    const float v = calc_return(entry_px, exit_px);
    writer(hold_col(h) + s, size_t{1}, m, &v, age);
  }

  // 行 m 的组 h 在价点 x 平仓: 平得了的腿写出, 平不了的腿进等待
  template <class Writer>
  inline void exit_row(Pend &q, size_t age, size_t h, size_t m, const LabelPx &x, Writer &&writer) {
    for (size_t s = 0; s < legs(h); ++s)
      if (x.exits(s)) {
        write_leg(age, h, s, m, q.entry[m].px, x.px, writer);
      } else {
        q.wait[h][s].set(m);
        ++n_wait_[s];
      }
  }

  // 组 h 的全部落盘行在价点 x 平仓
  template <class Writer>
  inline void exit_rows(Pend &q, size_t age, size_t h, const LabelPx &x, Writer &&writer) {
    writer.rows(hold_col(h)).below(q.n_entry).each([&](size_t m) { exit_row(q, age, h, m, x, writer); });
  }

  // 组 h 名义平仓日无成交: 全部落盘行 (rows) 各腿进等待
  inline void wait_rows(Pend &q, size_t h, const RowSet &rows) {
    const RowSet r = rows.below(q.n_entry);
    for (size_t s = 0; s < legs(h); ++s)
      n_wait_[s] += q.wait[h][s].merge(r);
  }

  // 价点 x 喂等待: 该腿可平 → 全部在等的 (日, 组, 行) 该腿按 x 平仓
  template <class Writer>
  inline void resolve(const LabelPx &x, Writer &&writer) {
    for (size_t s = 0; s < LEG_MAX; ++s) {
      if (!n_wait_[s] || !x.exits(s))
        continue;
      for (size_t age = 0; age < PEND_DAYS; ++age) {
        Pend &q = at(age);
        for (size_t h = 0; h < HOLD_COUNT; ++h)
          if (s < legs(h))
            q.wait[h][s].drain([&](size_t m) { write_leg(age, h, s, m, q.entry[m].px, x.px, writer); });
      }
      n_wait_[s] = 0;
    }
  }

  // 日 age 尚未写完的组 (排期未到 / 等待中) 全部按最近一次收盘价结清
  template <class Writer>
  inline void mark_close(size_t age, Writer &&writer) {
    Pend &q = at(age);
    if (!q.held || !q.busy())
      return;
    assert(has_last_ && "悬挂日必有 entry ⇒ 必有过成交 ⇒ last_close_ 有值");
    const LabelPx mk{last_close_, true, true};
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (q.sched >> h & 1u)
        exit_rows(q, age, h, mk, writer);
    q.sched = 0;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      for (size_t s = 0; s < legs(h); ++s)
        n_wait_[s] -= q.wait[h][s].drain([&](size_t m) { write_leg(age, h, s, m, q.entry[m].px, mk.px, writer); });
  }

  // 写完的日按日序 release (老日先; 遇到未写完的日即停)
  template <class Release>
  inline void release_settled(Release &&release) {
    for (size_t age = PEND_DAYS; age-- > 0;) {
      Pend &q = at(age);
      if (!q.held)
        continue;
      if (q.busy())
        return;
      q.held = false;
      release(age);
    }
  }

  bool open_ok_ = false; // 当日已截到开盘价点 (reset 清)

  // 跨日状态 (reset 不清)
  std::array<Pend, PEND_DAYS> pend_; // 悬挂日环, pend_[cur_] = 当日; 日 D 的槽 = (cur_ − age) mod PEND_DAYS
  size_t cur_ = PEND_DAYS - 1;       // 首个 day_begin 推到 0
  size_t n_wait_[LEG_MAX] = {};      // 各腿等待中的 (日, 组, 行) 数 (0 → resolve 跳过)
  float last_close_ = 0.0f;          // 最近一次收盘价 (后复权), 环满 / 区间末的估值价
  bool has_last_ = false;
};
