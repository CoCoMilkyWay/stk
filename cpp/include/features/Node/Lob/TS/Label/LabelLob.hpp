#pragma once

// =============================================================================
// LabelLob - 建仓冲击成本 (盘口; 收益标签在 Bar 档 LabelBar, 本档只补真实冲击)
// =============================================================================
//   冲击成本 (lb_cost_{buy,sell}_<a>w) = 行 m 的建仓盘口吃 a 万元的 VWAP 相对中间价的偏离 (比例, ≥0):
//     cost_buy = VWAP^A(a)/mid − 1,  cost_sell = 1 − VWAP^B(a)/mid.
//     建仓盘口 = 锚点 (分钟 m 末) + LABEL_DELAY_SECONDS 的可成交盘口 (精确到秒的下单时刻); 行 m 的特征在分钟 m 末才可知, 锚到 m 起始秒会前视一分钟.
//     收益标签 (Bar 档) 的 entry 是次个有成交分钟的 VWAP, 不含冲击; 消费端 (Run / Inspect / Portfolio) 在毛超额二级上按组池化扣:
//     组均 lv − 组均 cost_buy(a) − fixed_impact (做空腿 cost_sell); 不进标签再算超额 —— 超额对每 t 去均值, 成本进标签只剩截面相对项, 扣不出绝对成本.
//     平仓侧冲击 = 固定冲击 Config::fixed_impact (模拟值, 不按金额变). 税佣 (Config::commission / stamp) 同理事后扣.
//   盘口价点 = 连续竞价时段的盘口 (订单簿 market_state). 集合竞价期 (09:15-09:25 / 14:57-15:00) 的簿只是堆单,
//     可交叉 (买一 ≥ 卖一) 且撮合前一股不成交, 拿它定冲击 = 假价格, 一律不入环.
//   锚点在非交易空窗 (09:25 撮合后到 09:30 开盘) / 稀疏盘口 → 顺延到之后首个入环簿 = 最早能下单的时刻 (见 get_snapshot_tradable)
//   锚点后再无入环簿 (14:57 起) → 收盘价建仓 (单一价撮合, 冲击 0; 封板的那侧 NaN)
//   中间价: 买一 / 卖一 皆在取均值; 一侧全空 (涨跌停封板) 取另一侧 (= 涨跌停价); 可买 = 卖一侧有挂单, 可卖 = 买一侧有挂单
//   逐侧算 (买入冲击要可买, 卖出冲击要可卖; 不可成交的那侧 NaN); 全簿吃不完 → 余量按涨跌停价成交 (与 Book 吃单成本同约, 见 calc_vwap)
//   全日无入环簿 (缺数据 / 停牌日): 当日行不写.
//
// 非 DAG 节点 (行 m 的值要等锚点后的盘口, 回填, 不走 Node). 一份盘口快照环给建仓用, 只写当日 (不跨日):
//   snapshot(t)                 每次盘口更新在 onDepth 之前调: 只记账, 快照惰性结算 —— 同一秒内只有最后一次
//                               盘口状态会被消费, 所以等下一次盘口更新到来时 (Depth 尚未覆写 = 上一秒终簿)
//                               才结算入环; 查询目标恰为当前秒 (还没结算) 时从 Depth 当前簿现算.
//                               与"每次更新都算"逐值一致, 计算次数从每笔盘口更新降到每活跃秒一次.
//                               只有可成交簿记为活跃秒.
//   minute_anchored(t, writer)  L_1min 分钟锚定建仓捕获, 只在可成交更新上推进: 建仓秒已完结的行写冲击成本列.
//   minute_closed(t_min, writer)   CoreTs 分钟结算口 (冲击只看盘口, 不用分钟 bar)
//   day_begin / day_end         日历: 日期轴每一日各调一次 (无数据日也调, 由 CoreTs 保证). day_end 写尾部行 (收盘价建仓) 并归还当日句柄.
//   finish_all                  回测区间末: 无悬挂 (每日 day_end 已归还)
//   日句柄环 / 日环下限与 Bar 档同一份 (LabelHold::PEND_DAYS / MIN_RING_SLOTS): 续算尾窗与计算配置两档一致.
// 落盘列 (LB 块, 连续): [cost_buy × 各金额, cost_sell × 各金额]; writer(col, n, t_min, values, days_ago) 写 LB 块内偏移 col 起的 n 列.
//   配置 (LABEL_AMTS) 同时生成 constexpr 数组和落盘字段行, 只改一处.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/Node/Common/Label/LabelHold.hpp" // RowSet / PEND_DAYS / MIN_RING_SLOTS (日句柄环两档同一份)
#include "features/TimeIndex.hpp"                   // L_1min_to_L_sec (分钟锚定路径)
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <string_view>

// ---- 配置 ----
#define LABEL_AMTS(X, ...) X(5, __VA_ARGS__) X(30, __VA_ARGS__) // 冲击成本的下单金额段 (万元), 升序
constexpr size_t LABEL_DELAY_SECONDS = 3;                       // 下单延迟 (秒)

#define LABEL_LIST_ONE(v, ...) v,
inline constexpr size_t LABEL_AMOUNT_WAN[] = {LABEL_AMTS(LABEL_LIST_ONE)};
#undef LABEL_LIST_ONE
inline constexpr size_t LABEL_AMT_COUNT = std::size(LABEL_AMOUNT_WAN);
#define LABEL_COST_CODE(a, side) "lb_cost_" #side "_" #a "w",
inline constexpr const char *LABEL_COST_CODES[] = {LABEL_AMTS(LABEL_COST_CODE, buy) LABEL_AMTS(LABEL_COST_CODE, sell)}; // LB 块列名 (序 = write_cost)
#undef LABEL_COST_CODE

// 连续竞价盘口快照 (建仓价点用): 可买 / 可卖 + 各金额段吃单冲击
struct LobSnapshot {
  float cost_buy[LABEL_AMT_COUNT] = {};  // 吃 ask a 万: VWAP^A/mid − 1 (≥0; 不可买 NaN)
  float cost_sell[LABEL_AMT_COUNT] = {}; // 吃 bid a 万: 1 − VWAP^B/mid (≥0; 不可卖 NaN)
  size_t sec_index = 0;
  bool valid = false;             // 入环 (有中间价, 见 materialize)
  bool buy = false, sell = false; // 卖一侧有挂单 = 可买, 买一侧有挂单 = 可卖 (封板该侧全空)
};

class LabelLob {
  using Snapshot = LobSnapshot;

public:
  using RowSet = LabelHold::RowSet;
  static constexpr size_t AMT_COUNT = LABEL_AMT_COUNT;
  static constexpr size_t MIN_LABEL_COUNT = 2 * AMT_COUNT; // LB 块总列数 [cost_buy × 各金额, cost_sell × 各金额]
  static constexpr size_t PEND_DAYS = LabelHold::PEND_DAYS;
  static constexpr size_t MIN_RING_SLOTS = LabelHold::MIN_RING_SLOTS;
  static_assert(std::size(LABEL_COST_CODES) == MIN_LABEL_COUNT);
  static_assert([] {
    for (size_t a = 1; a < AMT_COUNT; ++a)
      if (LABEL_AMOUNT_WAN[a] <= LABEL_AMOUNT_WAN[a - 1])
        return false;
    return true;
  }(),
                "LABEL_AMTS 须严格升序");

  // LB 块偏移 ← 列名 (CoreTs 按名定址); 对不上 → SIZE_MAX
  static constexpr size_t col_of(std::string_view code) {
    for (size_t c = 0; c < MIN_LABEL_COUNT; ++c)
      if (code == LABEL_COST_CODES[c])
        return c;
    return SIZE_MAX;
  }
  // 落盘行约束 (CoreTs 编译期查): 一行的冲击成本整组一次写 (write_cost), 各列落盘行须相同
  template <class Rows>
  static constexpr bool rows_ok(const Rows &rows) {
    for (size_t c = 1; c < MIN_LABEL_COUNT; ++c)
      if (!(rows[c] == rows[0]))
        return false;
    return true;
  }

  LabelLob(const TickData &td, const MinuteData &md,
           const DepthPorts &bid_price, const DepthPorts &ask_price,
           const DepthPorts &bid_qty, const DepthPorts &ask_qty,
           const Port &lim_up, const Port &lim_dn)
      : td_(td), md_(md), bid_price_(bid_price), ask_price_(ask_price), bid_qty_(bid_qty), ask_qty_(ask_qty),
        lim_up_src_(lim_up), lim_dn_src_(lim_dn) {}

  // 记录当前秒有盘口更新 (每次盘口更新调一次, 先于 onDepth 与 minute_anchored).
  // 换秒 (或本次更新不可成交) 时把上一个活跃秒结算入环: Depth 此刻尚未被本次更新覆写, 正是那一秒最后
  // 一次更新的盘口. 集合竞价的更新同样覆写 Depth, 所以不管本次可不可成交都得此刻结算.
  // 可成交 = 连续竞价时段 (订单簿的 market_state); 集合竞价期的簿不入环, pending 清空 → minute_anchored 本次不推进.
  inline void snapshot(size_t t) {
    const bool ok = tradable();
    if (pending_sec_ != kNoPending && (pending_sec_ != t || !ok))
      flush_pending();
    if (ok)
      pending_sec_ = t;
  }

  // L_1min 分钟锚定建仓捕获: 锚点 = 分钟 m 末 (= m+1 起始秒; 11:29 → 13:00:00), 建仓 = 锚点+DELAY.
  //   建仓秒完结 (entry_sec < t) 且 [entry_sec − 60, t − 1] 内有盘口才算捕获, 捕获即写该行的冲击成本列
  //   (锚点后无入环簿 = 顺延中, 等下一活跃秒; 一直没有 → day_end 按收盘价建仓).
  //   只在可成交更新上推进 (snapshot 刚把 pending 设为 t): 集合竞价期的更新什么都不做 —— 连续竞价已结束,
  //   悬着的行都是"收盘价建仓", 留给 day_end. 不用时钟判 14:57.
  template <class Writer>
  inline void minute_anchored(size_t t, Writer &&writer) {
    if (pending_sec_ != t)
      return; // 本次更新不可成交 (集合竞价)
    while (n_entry_ < TRADE_MINUTES_PER_DAY) {
      const size_t entry_sec = L_1min_to_L_sec(n_entry_ + 1) + LABEL_DELAY_SECONDS; // 末分钟 254 → 15303 (盘后), 永不捕获 → day_end
      if (entry_sec >= t)
        break; // 建仓秒尚未完结: 同秒只消费最后一次盘口
      const Snapshot *e = get_snapshot_tradable(entry_sec, t - 1);
      if (!e)
        break; // 锚点起至 t−1 无可成交簿 (09:25-09:30 空窗等), 顺延中
      write_cost(n_entry_++, *e, writer);
    }
  }

  template <class Writer>
  inline void minute_closed(size_t, Writer &&) {}

  inline void day_begin() { held_ = true; }

  // 收盘结算 (当日有无盘口皆调): 锚点后再无可成交簿的尾部行按收盘价建仓 (单一价撮合, 冲击 0; 封板的那侧 NaN).
  //   收盘价 = 当日末笔成交价 (MinuteData 末 bar 的 close; CoreLob::end_day 先 finish 末分钟再调这里), 可买 / 可卖按涨跌停判;
  //   全日有入环簿却无一笔成交 (极罕见) → 可买 / 可卖取最后入环簿. 全日无入环簿: 当日行不写.
  //   之后归还当日句柄: release(0).
  template <class Writer, class Release>
  inline void day_end(Writer &&writer, Release &&release) {
    if (pending_sec_ != kNoPending)
      flush_pending(); // 最后一个可成交秒之后再无盘口更新: Depth 当前簿就是它的终簿
    if (last_sec_ != kNoPending) {
      const float raw = md_.close;
      Snapshot c;
      c.valid = true;
      if (raw > 0.0f) {
        c.buy = !(raw >= lim_up_ - kPxEps);
        c.sell = !(raw <= lim_dn_ + kPxEps);
      } else {
        const Snapshot &s = ring_[last_sec_ % RING_SIZE];
        c.buy = s.buy;
        c.sell = s.sell;
      }
      while (n_entry_ < TRADE_MINUTES_PER_DAY)
        write_cost(n_entry_++, c, writer);
    }
    held_ = false;
    release(size_t{0});
  }

  template <class Writer, class Release>
  inline void finish_all(Writer &&, Release &&) {}

  // 每日重置: 快照环作废, 行游标归零; 当日涨跌停价取 Fund 日频口 (盘前承接之后 = 当日适用)
  inline void reset() {
    lim_up_ = lim_up_src_;
    lim_dn_ = lim_dn_src_;
    for (auto &snap : ring_)
      snap.valid = false;
    pending_sec_ = kNoPending; // 昨日最后一个活跃秒不结算 (当日 ring 已整体作废)
    last_sec_ = kNoPending;
    n_entry_ = 0;
  }

  // 续算点: 无跨日状态
  template <class Ar>
  void snap(Ar &) {}
  bool held(size_t age) const { return age == 0 && held_; }

private:
  // 当前盘口可成交 = 连续竞价时段. 集合竞价 (含撮合期) 的簿是撮合前的堆单, 不可成交; 其余状态 (CLOSED, 脏时间戳) 也不算
  inline bool tradable() const {
    const auto s = td_.lob.market_state;
    return s == L2::MarketState::CONTINUOUS_TRADING_MORNING || s == L2::MarketState::CONTINUOUS_TRADING_AFTERNOON;
  }

  // 悬着的活跃秒结算入环: 下一次盘口更新到来时 (onDepth 之前) 或收盘时, Depth 当前簿都是该秒终簿
  //   终簿吃不到 (valid = false) 即不入环, 不算活跃秒
  inline void flush_pending() {
    Snapshot &s = ring_[pending_sec_ % RING_SIZE];
    materialize(s, pending_sec_);
    if (s.valid)
      last_sec_ = pending_sec_;
    pending_sec_ = kNoPending;
  }

  // 环长: 只给建仓捕获查 (get_snapshot 60s 回溯 + 顺延扫描到 t−1); 条目按 sec_index 核对, 过长的盘口空窗只是多扫几格
  static constexpr size_t RING_SIZE = 1024;

  // 行 m 的冲击成本列 [cost_buy × 各金额, cost_sell × 各金额] (当日, days_ago 0); 不可成交的那侧 NaN; 单一价撮合 (尾部行) 冲击 0
  template <class Writer>
  static inline void write_cost(size_t m, const Snapshot &entry, Writer &&writer) {
    float values[MIN_LABEL_COUNT];
    for (size_t a = 0; a < AMT_COUNT; ++a) {
      values[a] = entry.buy ? entry.cost_buy[a] : kNaN;
      values[AMT_COUNT + a] = entry.sell ? entry.cost_sell[a] : kNaN;
    }
    writer(size_t{0}, MIN_LABEL_COUNT, m, static_cast<const float *>(values), size_t{0});
  }

  // Depth 当前簿结算快照:
  //   中间价 = (买一 + 卖一)/2; 一侧全空 (封板) 取另一侧 (= 涨跌停价).
  //   入环 (valid) = 有中间价; 可买 = 卖一侧有挂单, 可卖 = 买一侧有挂单 (封涨停 → 只可卖, 封跌停 → 只可买)
  //   冲击 (逐侧: 买入要可买, 卖出要可卖) = 吃 a 万的 VWAP 相对中间价 (余量按涨跌停价, 见 calc_vwap), 恒有限; 不可成交的那侧 NaN
  void materialize(Snapshot &snap, size_t t_sec) const {
    snap.sec_index = t_sec;
    const float b1 = bid_price_[0], a1 = ask_price_[0];
    const bool has_b = b1 > 1e-6f && bid_qty_[0] > 1e-6f, has_a = a1 > 1e-6f && -ask_qty_[0] > 1e-6f;
    const float mid = has_b && has_a ? 0.5f * (b1 + a1) : (has_b ? b1 : (has_a ? a1 : 0.0f));
    snap.valid = mid > 1e-6f;
    snap.buy = has_a;
    snap.sell = has_b;
    for (size_t a = 0; a < AMT_COUNT; ++a) {
      const float amt = static_cast<float>(LABEL_AMOUNT_WAN[a]) * 10000.0f;
      const float vb = has_a ? calc_vwap(ask_price_, ask_qty_, amt, true, lim_up_) : 0.0f;  // 吃 ask (买入): 余量按涨停
      const float vs = has_b ? calc_vwap(bid_price_, bid_qty_, amt, false, lim_dn_) : 0.0f; // 吃 bid (卖出): 余量按跌停
      assert((!has_a || vb > 1e-6f) && (!has_b || vs > 1e-6f) && "materialize: 有挂单的一侧须吃得到");
      snap.cost_buy[a] = has_a ? vb / mid - 1.0f : kNaN;
      snap.cost_sell[a] = has_b ? 1.0f - vs / mid : kNaN;
    }
  }

  // 指定 t_sec 时刻的快照; 深度不是每秒都更新, 向前找 ≤60s 内最近的有效快照
  const Snapshot *get_snapshot(size_t target) const {
    if (target == pending_sec_) { // 当前秒未结算: 从 Depth 当前簿现算 (只有锚点查询走到, 频次 ~分钟级); 吃不到则照常回溯
      materialize(scratch_, target);
      if (scratch_.valid)
        return &scratch_;
    }
    const auto &s = ring_[target % RING_SIZE];
    if (s.valid && s.sec_index == target)
      return &s;
    for (size_t off = 1; off <= 60 && off <= target; ++off) {
      const auto &ss = ring_[(target - off) % RING_SIZE];
      if (ss.valid && ss.sec_index == target - off)
        return &ss;
    }
    return nullptr;
  }

  // 可成交快照: 锚点先按常规 60s 回溯; 落在无可成交簿的时段 (09:25 撮合后到 09:30 开盘不受理委托 / 稀疏盘口)
  // 时顺延到 limit 之前首个可成交时刻 —— "最早能成交的时刻才是成交点", 比留 NaN 贴近 T0 实盘.
  // 取锚点之后的快照不引入前视: 冲击本就是建仓时刻的量, 行 m 的特征在分钟 m 末已定.
  const Snapshot *get_snapshot_tradable(size_t target, size_t limit) const {
    if (const auto *s = get_snapshot(target))
      return s;
    for (size_t t_sec = target + 1; t_sec <= limit; ++t_sec) {
      if (t_sec == pending_sec_) { // 当前秒未结算, 同 get_snapshot: 从 Depth 当前簿现算
        materialize(scratch_, t_sec);
        if (scratch_.valid)
          return &scratch_;
        continue;
      }
      const auto &s = ring_[t_sec % RING_SIZE];
      if (s.valid && s.sec_index == t_sec)
        return &s;
    }
    return nullptr;
  }

  // 模拟吃单: 遍历盘口深度算 VWAP (返回; 吃不到一股 → 0). is_buy: 吃 ask (qty 存负值); 否则吃 bid (正值)
  // limit_px: 全簿吃不完时余量的成交价 (买 → 涨停, 卖 → 跌停), 与 Book 的吃单成本同约;
  //           涨跌停封板 (该侧全空, 如涨停无卖盘) 也走这条; 边界 NaN (无限制股) 不补 → 该侧全空时 0 (materialize 已判不可成交)
  static inline float calc_vwap(const DepthPorts &price, const DepthPorts &qty, float amount, bool is_buy, float limit_px) {
    float cost = 0.0f, sh = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH && amount > 1e-6f; ++i) {
      const float p = price[i];
      const float q = is_buy ? -qty[i] : qty[i];
      if (p < 1e-6f || q < 1e-6f)
        continue;
      const float fill = std::min(amount, p * q); // 本档成交金额
      cost += fill;
      sh += fill / p;
      amount -= fill;
    }
    if (amount > 1e-6f && limit_px > 0.0f) { // 余量按涨跌停价成交 (边界 NaN = 无限制股 → 比较恒 false, 不补)
      cost += amount;
      sh += amount / limit_px;
    }
    return sh > 1e-6f ? cost / sh : 0.0f;
  }

  const TickData &td_;          // market_state: 当前簿可成交与否
  const MinuteData &md_;        // 末 bar close = 当日末笔成交价 (收盘)
  const DepthPorts &bid_price_; // Depth 当前簿 (见 snapshot: 调用时序决定它是哪一次更新的簿)
  const DepthPorts &ask_price_;
  const DepthPorts &bid_qty_;
  const DepthPorts &ask_qty_;
  const Port &lim_up_src_, &lim_dn_src_; // Fund 日频口 (lim_up / lim_dn)

  static constexpr size_t kNoPending = SIZE_MAX;

  float lim_up_ = kNaN, lim_dn_ = kNaN;  // 当日涨跌停价 (簿子吃不完时补齐余量 / 收盘价可成交判定)
  std::array<Snapshot, RING_SIZE> ring_; // 盘口快照环 (只收可成交簿)
  size_t pending_sec_ = kNoPending;      // 当前活跃秒 (有可成交盘口更新, 尚未结算入环)
  size_t last_sec_ = kNoPending;         // 当日最近一个已入环的活跃秒 (kNoPending = 全日尚无可成交簿)
  size_t n_entry_ = 0;                   // 当日已写冲击成本的行数 (行按锚点单调, 前缀)
  bool held_ = false;                    // 当日句柄未归还 (day_begin → day_end)
  mutable Snapshot scratch_;             // pending 秒被查询时的现算暂存
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 行由配置生成, 顺序 = LB 块布局 [cost_buy × LABEL_AMTS, cost_sell × LABEL_AMTS] (与 LABEL_COST_CODES / write_cost 对应).
// 描述统一四段: 口径 | 吃单 | 边界 | 净收益换算 (买入 / 卖出逐词对应)
#define LABEL_COST_BUY(a, X, CAT1)                                                         \
  X(lb_cost_buy_##a##w, CAT1, ratio_pos, "Entry Buy Impact " #a "w", "买入冲击(" #a "万)", \
    "建仓买入冲击(比例≥0; 标签不含). 吃单: 锚点(分钟末)+3s的连续竞价盘口吃ask " #a "万元, 成交VWAP相对中间价的溢价; 空窗则顺延至首个入环簿, 锚点后再无入环簿则按收盘价建仓(冲击0). 边界: 全簿不足余量按涨停价成交; 无卖盘(封涨停, 不可买)则NaN. 做多净收益=lb_long_*−本列−Config::fixed_impact", R"(\frac{\mathrm{VWAP}^{A}_{entry}(A)}{P^{mid}_{entry}}-1, \quad A=)" #a R"(\mathrm{w})", LABEL)
#define LABEL_COST_SELL(a, X, CAT1)                                                          \
  X(lb_cost_sell_##a##w, CAT1, ratio_pos, "Entry Sell Impact " #a "w", "卖出冲击(" #a "万)", \
    "建仓卖出冲击(比例≥0; 标签不含). 吃单: 锚点(分钟末)+3s的连续竞价盘口吃bid " #a "万元, 成交VWAP相对中间价的折价; 空窗则顺延至首个入环簿, 锚点后再无入环簿则按收盘价建仓(冲击0). 边界: 全簿不足余量按跌停价成交; 无买盘(封跌停, 不可卖)则NaN. 做空净收益=lb_short_*−本列−Config::fixed_impact", R"(1-\frac{\mathrm{VWAP}^{B}_{entry}(A)}{P^{mid}_{entry}}, \quad A=)" #a R"(\mathrm{w})", LABEL)

#define FIELDS_L_1min_LabelLob(X, CAT1) LABEL_AMTS(LABEL_COST_BUY, X, CAT1) LABEL_AMTS(LABEL_COST_SELL, X, CAT1)
