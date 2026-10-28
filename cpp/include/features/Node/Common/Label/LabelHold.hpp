#pragma once

// =============================================================================
// LabelHold - 持有期标签的档无关骨架 (LabelLob 盘口中间价 / LabelBar 分钟 VWAP 共用)
// =============================================================================
//   价点 LabelPx = 一次报价: 后复权价 + 该价能否买入 / 卖出. 档只管价点怎么来, 骨架管持有期组的全部日历与结算.
//   可成交性全在标签里定 (消费端只读标签, 不另读可成交列):
//     行 m 的 entry 价点 (档 push_entry): 行末分钟 m 之后首个价点 (Lob 可成交盘口 / Bar 有成交连续竞价分钟; 再无 → 收盘价点).
//     【建仓可成交码】ENTRY_COL (lb_entry_ok) = entry 价点 可买 1 | 可卖 2 (封涨停只可卖, 封跌停只可买). 两腿收益照写 (不作废):
//       该行能否真建仓 / 已持仓能否续持 由消费端按持仓判 (Stat 契约【仓位感知】). 价点上不可能两向都堵 (无价点 = 无行).
//     名义平仓价点: MIN = 行 m+n 的 entry 价点 (越过收盘 → 收盘价点); CLOSE = 当日收盘; OPEN = T+n 开盘; CLOSE_N = T+n 收盘.
//     【平仓顺延】名义价点上该腿平不了 (做多腿卖不出 / 做空腿买不回), 或名义日无成交 (停牌) → 该 (行, 组, 腿) 进等待,
//       之后首个该腿可成交的价点平仓 (盘中价点 / 收盘 / 开盘; 分钟级 / 日级同一规则, 可跨日).
//       等待到悬挂环满 (行所在日起 PEND_DAYS − 1 个交易日) 仍未平 → 按最近一次收盘价估值结清 (不管可成交).
//     价点来源: 盘中 = 档 push_entry; 开盘 = open_at (09:25 撮合价, 无撮合则首个有成交连续竞价 bar 的 open); 收盘 = close_day (档给).
//     开盘 / 收盘价点的可成交 (auction_px, 单一价撮合): 价 < 涨停 − ε 可买, 价 > 跌停 + ε 可卖; 涨跌停 NaN (无限制) → 可成交.
//     day_begin / finish_all / reset   日历 / 区间末 (全部按最近收盘价结清) / 每日重置
//     snap / held                      续算点 (跨日状态全部) / 日 age 的写句柄是否仍持 (续算接回尾窗日时 CoreTs 据此归还)
//   跨日回填: 日 D 的行 entry 存在悬挂槽里, 日级组 / 等待中的组写完前 D 的写句柄由 CoreTs 持着 (writer 带 days_ago);
//   写完的日在收盘 / 区间末按日序 release (老日未写完则新日也不放: FeatureStore "d+1 计满 ⇒ d 计满").
//   writer(col, n, t_min, values, days_ago) 写 LB 块内偏移 col 起的 n 列 (分钟 t_min 的标签; 落哪些层的哪行由 CoreTs 定);
//   writer.rows(col) = 列 col 落盘的行集 (行末分钟): 骨架只结算落盘行 —— 不落盘的 (行, 列) 不写可成交码, 不平仓, 不进等待;
//     entry 价点逐行照收 (分钟型名义平仓 = 后续行的 entry). 一组两腿落盘行相同.
//   release(days_ago) = 该日全部组已写完, 写句柄可归还.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/Node/Common/Label/LabelGroups.hpp" // LABEL_GROUPS / HoldKind / LABEL_KIND / LABEL_N
#include "features/TimeIndex.hpp"
#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cstdint>
#include <iterator>

#define LABEL_DELAY_NOTE "行所在日起8个交易日" // 字段描述用, = LabelHold::PEND_DAYS − 1

// 价点: 后复权价 + 该价能否买入 / 卖出 (建仓的两向记进可成交码, 平仓只要本腿那一向)
struct LabelPx {
  float px = 0.0f;
  bool buy = false, sell = false;
  float code() const { return static_cast<float>((buy ? 1 : 0) | (sell ? 2 : 0)); } // lb_entry_ok
  bool exits(size_t side) const { return side == 0 ? sell : buy; }                  // 做多腿 (0) 卖出平仓, 做空腿 (1) 买回平仓
};

class LabelHold {
public:
  static constexpr size_t HOLD_COUNT = std::size(LABEL_KIND);
  static constexpr size_t GROUP_SIZE = 2;                                 // 一个 hold 组: [long, short]
  static constexpr const char *SIDE_NAME[GROUP_SIZE] = {"long", "short"}; // 列名 lb_<side>_<name> 的 side
  static constexpr size_t hold_col(size_t h) { return h * GROUP_SIZE; }
  static constexpr size_t HOLD_COLS = HOLD_COUNT * GROUP_SIZE;
  static constexpr size_t ENTRY_COL = HOLD_COLS; // lb_entry_ok (建仓可成交码); 档附带列从 ENTRY_COL + 1 起
  static_assert(HOLD_COUNT <= 32, "日级组排期位图用 uint32_t");
  static_assert([] {
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] != HoldKind::CLOSE && LABEL_N[h] < 1)
        return false;
    return true;
  }(),
                "分钟型 / 日级型的 n 须 ≥ 1");

  // 日级组最远名义平仓日 (行所在日起第几个交易日)
  static constexpr size_t DAYS_MAX = [] {
    size_t m = 0;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (hold_daily(LABEL_KIND[h]))
        m = std::max(m, LABEL_N[h]);
    return m;
  }();
  static constexpr size_t DELAY_DAYS = 5; // 名义平仓日之后至少还能顺延的交易日数 (环满按最近收盘价结清)
  // 悬挂日环 (含当日): 同时未写完 (写句柄未归还) 的日数上限
  static constexpr size_t PEND_DAYS = DAYS_MAX + DELAY_DAYS + 1;
  static_assert(PEND_DAYS == 9, "LABEL_DELAY_NOTE 的天数随 PEND_DAYS − 1 改");
  // FeatureStore 日环下限: 悬挂日全 BUSY 时同日的 TS 还要 1 个 slot 才能推进; 再 +1: IO(d) 等 CS(d+1) close (截面跨日读 d−1 槽位,
  // 见 FeatureStore::io_try_open), d 的 slot 要多持到 d+1 结清 (其余 CS / IO 阶段的 slot 另算, 只影响吞吐不致死锁)
  static constexpr size_t MIN_RING_SLOTS = PEND_DAYS + 2;

  // 09:25 撮合 bar (L_1min kAuctionMin) / 连续竞价 [kContOpenMin, kContCloseMin) = [09:30, 14:57) / 收盘竞价 (L_1min 252..254)
  static constexpr size_t kAuctionMin = (L2::MORNING_MATCHING_START_HOUR * 60 + L2::MORNING_MATCHING_START_MINUTE) - MORNING_START_MIN;
  static constexpr size_t kContOpenMin = (L2::CONTINUOUS_TRADING_MORNING_START_HOUR * 60 + L2::CONTINUOUS_TRADING_MORNING_START_MINUTE) - MORNING_START_MIN;
  static constexpr size_t kContCloseMin = MORNING_MINUTES + (AFTERNOON_END_MIN - AFTERNOON_START_MIN);
  static_assert(kAuctionMin == 10 && kContOpenMin == 15 && kContCloseMin == 252);

  // 一日内的行集合 (位 = L_1min 行): 列的落盘行 (writer.rows) / 等待平仓的行
  struct RowSet {
    uint64_t w[(TRADE_MINUTES_PER_DAY + 63) / 64] = {};
    constexpr void set(size_t m) { w[m / 64] |= uint64_t{1} << (m % 64); }
    constexpr bool has(size_t m) const { return w[m / 64] >> (m % 64) & 1u; }
    constexpr bool operator==(const RowSet &) const = default;
    bool any() const {
      for (uint64_t x : w)
        if (x)
          return true;
      return false;
    }
    // 行 < n 的子集
    RowSet below(size_t n) const {
      RowSet r;
      for (size_t i = 0; i < std::size(w); ++i) {
        const size_t lo = i * 64;
        r.w[i] = n >= lo + 64 ? w[i] : n > lo ? w[i] & ((uint64_t{1} << (n - lo)) - 1)
                                              : 0;
      }
      return r;
    }
    // 并入 s; 返回新增行数
    size_t merge(const RowSet &s) {
      size_t n = 0;
      for (size_t i = 0; i < std::size(w); ++i) {
        n += static_cast<size_t>(std::popcount(s.w[i] & ~w[i]));
        w[i] |= s.w[i];
      }
      return n;
    }
    // 逐行回调
    template <class F>
    void each(F &&f) const {
      for (size_t i = 0; i < std::size(w); ++i)
        for (uint64_t x = w[i]; x; x &= x - 1)
          f(i * 64 + static_cast<size_t>(std::countr_zero(x)));
    }
    // 逐行回调并清空; 返回行数
    template <class F>
    size_t drain(F &&f) {
      size_t n = 0;
      for (size_t i = 0; i < std::size(w); ++i)
        for (; w[i]; w[i] &= w[i] - 1, ++n)
          f(i * 64 + static_cast<size_t>(std::countr_zero(w[i])));
      return n;
    }
  };

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

  // 续算点: 跨日状态 (悬挂日环 + 等待计数 + 估值价); 日内状态由次日 reset / 档 reset 重给
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
  float px_scale_ = 1.0f;               // 价格缩放 = 当日复权比 (两档 reset 时设; 跨日收益不含除权缺口)
  float lim_up_ = kNaN, lim_dn_ = kNaN; // 当日涨跌停价 (原始价; 两档 reset 时设)

  // 单一价撮合 (开盘 / 收盘) 的价点: raw = 原始价
  LabelPx auction_px(float raw) const { return {raw * px_scale_, !(raw >= lim_up_ - kPxEps), !(raw <= lim_dn_ + kPxEps)}; }

  // 当日已定 entry 的行数 (行按锚点单调, 前缀)
  size_t n_entry() const { return pend_[cur_].n_entry; }

  // 当日下一行的 entry 价点 (days_ago 恒 0): 写可成交码; 行 r 到位 = 行 r−n 的分钟型名义平仓到位; 再喂等待
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
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] == HoldKind::MIN && r >= LABEL_N[h] && writer.rows(hold_col(h)).has(r - LABEL_N[h]))
        exit_row(p, 0, h, r - LABEL_N[h], e, writer);
    resolve(e, writer);
  }

  // 有成交的分钟 bar 截开盘价点 (当日首个: 09:25 撮合 bar 的撮合价, 无撮合则首个连续竞价 bar 的首笔成交价 = 交易所开盘价定义):
  // 名义 T+n 开盘到期的组平仓, 再喂等待. 返回该 bar 是否连续竞价 (可作 entry)
  template <class Writer>
  inline bool open_at(size_t t_min, const MinuteData &md, Writer &&writer) {
    assert((t_min != kAuctionMin || !open_ok_) && "09:25 撮合 bar 之前不该有开盘价");
    const bool cont = t_min >= kContOpenMin && t_min < kContCloseMin;
    if (open_ok_ || (t_min != kAuctionMin && !cont))
      return cont;
    open_ok_ = true;
    const LabelPx o = auction_px(t_min == kAuctionMin ? md.close : md.open);
    for (size_t age = 1; age < PEND_DAYS; ++age) {
      Pend &q = at(age);
      for (size_t h = 0; h < HOLD_COUNT; ++h)
        if (LABEL_KIND[h] == HoldKind::OPEN && LABEL_N[h] == age && (q.sched >> h & 1u)) {
          exit_rows(q, age, h, o, writer);
          q.sched &= ~(1u << h);
        }
    }
    resolve(o, writer);
    return cont;
  }

  // 收盘结算 (当日有无成交皆调). traded: 当日有收盘价点 close.
  //   尾部行 (其后再无盘中价点) 按收盘价点建仓, on_tail(m, close) = 档的附带写 (Lob: 冲击成本列);
  //   分钟型尾部 (名义平仓过收盘) / 收盘型全行 → 收盘价点平仓; 名义今日收盘 (或今日开盘未到) 的日级组 → 收盘价点平仓; 再喂等待.
  //   无成交: 名义今日到期的日级组全部进等待. 之后环满的最老一日按最近收盘价结清, 写完的日按日序释放
  template <class Writer, class Release, class OnTail>
  inline void close_day(bool traded, const LabelPx &close, Writer &&writer, Release &&release, OnTail &&on_tail) {
    Pend &p = pend_[cur_];
    for (size_t age = 1; age < PEND_DAYS; ++age) {
      Pend &q = at(age);
      for (size_t h = 0; h < HOLD_COUNT; ++h)
        if ((q.sched >> h & 1u) && LABEL_N[h] <= age) { // OPEN 到期未开盘 (n == age) / CLOSE_N 到期 (n == age)
          if (traded)
            exit_rows(q, age, h, close, writer);
          else
            wait_rows(q, h, writer.rows(hold_col(h)));
          q.sched &= ~(1u << h);
        }
    }
    if (traded) {
      last_close_ = close.px;
      has_last_ = true;
      while (p.n_entry < TRADE_MINUTES_PER_DAY) {
        on_tail(p.n_entry, close);
        push_entry(close, writer);
      }
      for (size_t h = 0; h < HOLD_COUNT; ++h) {
        if (LABEL_KIND[h] == HoldKind::MIN) {
          for (size_t m = TRADE_MINUTES_PER_DAY - std::min(LABEL_N[h], TRADE_MINUTES_PER_DAY); m < TRADE_MINUTES_PER_DAY; ++m)
            if (writer.rows(hold_col(h)).has(m))
              exit_row(p, 0, h, m, close, writer);
        } else if (LABEL_KIND[h] == HoldKind::CLOSE) {
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
      if (hold_daily(LABEL_KIND[h]))
        m |= 1u << h;
    return m;
  }();

  // 悬挂日槽: 一日的 entry 价点 (全部组共用) + 日级组排期 + 等待平仓的 (组, 腿) 行集
  struct Pend {
    bool held = false;                    // 写句柄未归还
    uint32_t sched = 0;                   // 名义平仓未到的日级组 (位 = h)
    size_t n_entry = 0;                   // 已定 entry 的行数
    LabelPx entry[TRADE_MINUTES_PER_DAY]; // 各行 entry 价点
    RowSet wait[HOLD_COUNT][GROUP_SIZE];  // 名义价点平不了, 等之后首个该腿可成交价点
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

  // 单个 label 的价格收益率
  static inline float calc_return(float entry_px, float exit_px, bool is_long) {
    assert(entry_px > 0.0f && exit_px > 0.0f && "entry / 可平仓价点必有正价");
    return is_long ? (exit_px - entry_px) / entry_px : (entry_px - exit_px) / entry_px;
  }

  template <class Writer>
  static inline void write_side(size_t age, size_t h, size_t s, size_t m, float entry_px, float exit_px, Writer &&writer) {
    const float v = calc_return(entry_px, exit_px, s == 0);
    writer(hold_col(h) + s, size_t{1}, m, &v, age);
  }

  // 行 m 的组 h 在价点 x 平仓: 平得了的腿写出, 平不了的腿进等待
  template <class Writer>
  inline void exit_row(Pend &q, size_t age, size_t h, size_t m, const LabelPx &x, Writer &&writer) {
    for (size_t s = 0; s < GROUP_SIZE; ++s)
      if (x.exits(s)) {
        write_side(age, h, s, m, q.entry[m].px, x.px, writer);
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

  // 组 h 名义平仓日无成交: 全部落盘行 (rows) 两腿进等待
  inline void wait_rows(Pend &q, size_t h, const RowSet &rows) {
    const RowSet r = rows.below(q.n_entry);
    for (size_t s = 0; s < GROUP_SIZE; ++s)
      n_wait_[s] += q.wait[h][s].merge(r);
  }

  // 价点 x 喂等待: 该腿可成交 → 全部在等的 (日, 组, 行) 该腿按 x 平仓
  template <class Writer>
  inline void resolve(const LabelPx &x, Writer &&writer) {
    for (size_t s = 0; s < GROUP_SIZE; ++s) {
      if (!n_wait_[s] || !x.exits(s))
        continue;
      for (size_t age = 0; age < PEND_DAYS; ++age) {
        Pend &q = at(age);
        for (size_t h = 0; h < HOLD_COUNT; ++h)
          q.wait[h][s].drain([&](size_t m) { write_side(age, h, s, m, q.entry[m].px, x.px, writer); });
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
      for (size_t s = 0; s < GROUP_SIZE; ++s)
        n_wait_[s] -= q.wait[h][s].drain([&](size_t m) { write_side(age, h, s, m, q.entry[m].px, mk.px, writer); });
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
  size_t n_wait_[GROUP_SIZE] = {};   // 各腿等待中的 (日, 组, 行) 数 (0 → resolve 跳过)
  float last_close_ = 0.0f;          // 最近一次收盘价 (后复权), 环满 / 区间末的估值价
  bool has_last_ = false;
};
