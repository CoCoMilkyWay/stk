#pragma once

// =============================================================================
// LabelLob - 价格收益标签 + 建仓冲击成本特征
// =============================================================================
//   标签 (lb_long_<h> / lb_short_<h>) = 纯价格收益, 不含深度冲击、不含税佣:
//     做多: (exit − mid_entry) / mid_entry        做空: (mid_entry − exit) / mid_entry
//     entry = 锚点 (分钟末) + LABEL_DELAY_SECONDS 的可成交盘口 **中间价** (精确到秒的建仓时刻, 对 entry 敏感);
//     exit  = 分钟型: 行 m 持有 n 分钟的 exit = **行 m+n 的 entry 快照中间价** (同一条快照, 同一基准). entry / exit 同为 mid ⇒
//             相邻行标签精确伸缩 (Σ 连续 n 个 1m = 一个 nm), 与 Stat 换手成本"同一价点交接只算差额 Δw"自洽; 价差 / 冲击全部在成本列,
//             标签里不残留 (exit 用成交 VWAP 会把 taker 侧半价差记进标签: 被主动买的资产 VWAP 贴 ask, 失衡类因子每期白捡一个水平项);
//             收盘型: 当日收盘价 (末笔成交价 = 收盘竞价撮合价); 开盘型: T+N 日开盘价 (09:25 集合竞价撮合价, 无撮合则连续竞价首笔成交价);
//             T+N 收盘型: T+N 日收盘价.
//     价格均 × 当日复权比 (Fund adj_ratio, 日内恒定): 日内型与不复权同值, 日级型跨除权日的收益不含除权缺口.
//     建仓可成交码 (lb_entry_ok; entry 盘口单侧 = 封板) / 平仓顺延 (该腿平不了 / 停牌) / 环满按最近收盘价结清: 见 Common/Label/LabelHold.hpp.
//   冲击成本特征 (lb_cost_{buy,sell}_<a>w) = entry 同一盘口吃 a 万元的 VWAP 相对中间价的偏离 (比例, ≥0):
//     cost_buy = VWAP^A(a)/mid − 1,  cost_sell = 1 − VWAP^B(a)/mid.  "mid − 成交价" 单拆出来, 标签 − 成本 = 精确的吃单成交收益;
//     默认不扣 (Factors Run 只看毛), 消费端 (Inspect 期限结构) 在毛超额二级上按组池化扣: 组均 lv − 组均 cost_buy(a) − fixed_impact
//     (做空腿 cost_sell); 不进标签再算超额 —— 超额对每 t 去均值, 成本进标签只剩截面相对项, 扣不出绝对成本.
//     平仓侧冲击 = 固定冲击 Config::fixed_impact (模拟值, 不按金额变). 税佣 (Config::commission / stamp) 同理事后扣.
//     为什么不进标签: 评估 (IC / 超额) 对常数平移不变, 冲击 / 税佣留在标签里只会把 LS / Sharpe 按每行一次往返压死 (1m 持有期一天 255 次),
//     高频因子没法研究; 拆成特征后其他阶段想加随时能加.
//   标签一律取"实盘真能做到的那笔交易", 时间缺口用可成交时刻顶上; 标签无 NaN (entry 盘口单侧照写, 可否建仓记 lb_entry_ok):
//     盘口价点 = 连续竞价时段的盘口 (订单簿 market_state). 集合竞价期 (09:15-09:25 / 14:57-15:00) 的簿只是堆单,
//       可交叉 (买一 ≥ 卖一) 且撮合前一股不成交, 拿它定建仓价 = 假价格, 一律不入环.
//     entry 锚点在非交易空窗 (09:25 撮合后到 09:30 开盘) / 稀疏盘口 → 顺延到之后首个入环簿 = 最早能建仓的时刻 (见 get_snapshot_tradable)
//     分钟型 exit 同一规则 (就是行 m+n 的 entry): 锚点无簿 → 顺延 = 最早能平的时刻 (09:30 前的名义 exit 退化为开盘首个入环簿)
//     exit 锚点落在连续竞价结束之后 (行 m+n 的锚点 ≥ 14:57, 即 m+n ≥ kSegLen − kCloseAuction − 1; 永不捕获) → 持有到收盘 (T0 当日平仓):
//       按当日末笔成交价 (= 收盘竞价撮合价, 无撮合则连续竞价末笔; 取 MinuteData 末 bar close) 平仓 —— 用成交而非时钟判收盘 (见 day_end)
//     entry 锚点后再无入环簿 (14:57 起) → 收盘价建仓 (mid = close, 冲击 0): 分钟型 / 收盘型 即建即平 = 0, 日级型 = 收盘买, 到期卖
//     中间价: 买一 / 卖一 皆在取均值; 一侧全空 (涨跌停封板) 取另一侧 (= 涨跌停价); 可买 = 卖一侧有挂单, 可卖 = 买一侧有挂单
//     冲击成本: 逐侧算 (买入冲击要可买, 卖出冲击要可卖; 不可成交的那侧 NaN); 全簿吃不完 → 余量按涨跌停价成交 (与 Book 吃单成本同约, 见 calc_vwap)
//
// 非 DAG 节点 (未来标签需回填, 不走 Node). 一份盘口快照环 (中间价 + 各金额段冲击) 给 entry 用, 分钟型 exit 复用后续行的 entry:
//   snapshot(t)                 每次 onDepth 先调: 只记账, 快照惰性结算 —— 同一秒内只有最后一次
//                               盘口状态会被消费, 所以等下一次盘口更新到来时才从 Depth 环的上一格结算
//                               入环 (materialize offset=1); 查询目标恰为当前秒 (还没结算) 时从环末现算
//                               (offset=0). 与"每次更新都算"逐值一致, 计算次数从每笔盘口更新降到每活跃秒一次.
//                               只有可成交簿记为活跃秒.
//   minute_anchored(t, writer)  L_1min 分钟锚定 entry 捕获 (锚点 = 分钟末, 与行 m 特征的可知时刻对齐), 只在可成交更新上推进:
//                               把 entry 秒已完结的行的 entry 快照存进当日悬挂槽 (所有组共用一份 entry), 同时写该行的冲击成本列;
//                               行 e 捕获即是行 e−n 的分钟型 exit 到位, 顺带写分钟型 (e−n, n) 的标签.
//   minute_closed(t_min, writer)   分钟 bar 结算后 (CoreTs::close_minute, 只在有成交的分钟调): 只截开盘价点 (开盘型 T+N 的 exit,
//                               见 LabelHold::open_at); entry 走 minute_anchored.
//   day_begin / day_end         日历: 日期轴每一日各调一次 (无数据日也调, 由 CoreTs 保证).
//                               day_end 结算 分钟型尾部 (exit 过收盘 → 收盘价) + 收盘型 全行 + 日级型 到期日, 并释放写完的日.
//   finish_all                  回测区间末: 悬挂日全部按 最近一次收盘价 结清 (持有到最后一日收盘), 全部释放.
// 日级型跨日回填 / 悬挂日环 / 写句柄延迟归还见 Common/Label/LabelHold.hpp (两档同一骨架); 开盘 / 收盘价 = 集合竞价撮合价,
//   单一价全额成交, 无价差无冲击.
// 落盘列 (LB 块, 连续): [hold 组 × (long, short)] + lb_entry_ok + [cost_buy × 各金额, cost_sell × 各金额]; writer(col, n, t_min, values, days_ago)
//   写 LB 块内偏移 col 起的 n 列. 配置 (LABEL_GROUPS / LABEL_AMTS) 同时生成 constexpr 数组和落盘字段行, 只改一处.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/Node/Common/Label/LabelGroups.hpp" // LABEL_GROUPS / HoldKind / LABEL_KIND / LABEL_N (两档共用)
#include "features/Node/Common/Label/LabelHold.hpp"   // 持有期骨架 (两档共用)
#include "features/TimeIndex.hpp"                     // L_1min_to_L_sec (分钟锚定路径)
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iterator>

// ---- 配置 ----
// 持有期组见 Common/Label/LabelGroups.hpp (MIN 型 exit = 行 m+n 的 entry 快照中间价, 即锚点后整 n 分钟 + DELAY 的可成交盘口)
#define LABEL_AMTS(X, ...) X(5, __VA_ARGS__) X(30, __VA_ARGS__) // 冲击成本特征的下单金额段 (万元), 升序
constexpr size_t LABEL_DELAY_SECONDS = 3;                       // 下单延迟 (秒)

#define LABEL_LIST_ONE(v, ...) v,
inline constexpr size_t LABEL_AMOUNT_WAN[] = {LABEL_AMTS(LABEL_LIST_ONE)};
#undef LABEL_LIST_ONE
inline constexpr size_t LABEL_AMT_COUNT = std::size(LABEL_AMOUNT_WAN);

// 连续竞价盘口快照 (entry 价点用): 中间价 + 可买 / 可卖 + 各金额段吃单冲击
struct LobSnapshot {
  float px = 0.0f;                       // 中间价 (后复权)
  float cost_buy[LABEL_AMT_COUNT] = {};  // 吃 ask a 万: VWAP^A/mid − 1 (≥0; 不可买 NaN)
  float cost_sell[LABEL_AMT_COUNT] = {}; // 吃 bid a 万: 1 − VWAP^B/mid (≥0; 不可卖 NaN)
  size_t sec_index = 0;
  bool valid = false;             // 入环 (有中间价, 见 materialize)
  bool buy = false, sell = false; // 卖一侧有挂单 = 可买, 买一侧有挂单 = 可卖 (封板该侧全空)
};

class LabelLob : public LabelHold {
  using Snapshot = LobSnapshot;

public:
  static constexpr size_t AMT_COUNT = LABEL_AMT_COUNT;
  static constexpr size_t COST_COL = ENTRY_COL + 1;                // 冲击成本列在 LB 块内的起始偏移
  static constexpr size_t COST_COUNT = 2 * AMT_COUNT;              // [cost_buy × 各金额, cost_sell × 各金额]
  static constexpr size_t MIN_LABEL_COUNT = COST_COL + COST_COUNT; // LB 块总列数
  static_assert([] {
    for (size_t a = 1; a < AMT_COUNT; ++a)
      if (LABEL_AMOUNT_WAN[a] <= LABEL_AMOUNT_WAN[a - 1])
        return false;
    return true;
  }(),
                "LABEL_AMTS 须严格升序");
  // 分钟型 exit 不看 bar: 行 m+n 的 entry 锚点 ≥ 14:57 (m+n ≥ kContCloseMin − 1) 永不捕获 → 持有到收盘 (与 Stat tail_masked 同一条界).

  LabelLob(const TickData &td, const MinuteData &md,
           const DepthSeries &bid_price, const DepthSeries &ask_price,
           const DepthSeries &bid_qty, const DepthSeries &ask_qty,
           const Series &lim_up, const Series &lim_dn, const Series &adj_ratio)
      : td_(td), md_(md), bid_price_(bid_price), ask_price_(ask_price), bid_qty_(bid_qty), ask_qty_(ask_qty),
        lim_up_src_(lim_up), lim_dn_src_(lim_dn), adj_ratio_(adj_ratio) {}

  // 记录当前秒有盘口更新 (每次 onDepth 调一次, 先于 second / minute_anchored).
  // 换秒 (或本次更新不可成交) 时把上一个活跃秒结算入环: Depth 环的上一格 (offset=1) 正是那一秒最后
  // 一次更新的盘口 —— 与旧实现"每次更新覆盖写"的最终留存值逐位相同. Depth 每次 onDepth 必推一格
  // (集合竞价的更新也推), 所以不管本次可不可成交都得立刻结算, 否则 offset 对不上.
  // 可成交 = 连续竞价时段 (订单簿的 market_state); 集合竞价期的簿不入环, pending 清空 → minute_anchored 本次不推进.
  inline void snapshot(size_t t) {
    const bool ok = tradable();
    if (pending_sec_ != kNoPending && (pending_sec_ != t || !ok))
      flush_pending(1);
    if (ok)
      pending_sec_ = t;
  }

  // L_1min 分钟锚定 entry 捕获: 锚点 = 分钟 m 末 (= m+1 起始秒; 11:29 → 13:00:00), entry = 锚点+DELAY.
  //   L_1min 行 m 的特征是分钟 m 结束时才可知的 (CoreLob 顺序), 所以标签只能从 m 末起算, 锚到 m 起始秒会前视一分钟.
  //   entry 价点按行交给骨架 (push_entry, 所有组共用): entry 秒完结 (entry_sec < t) 且 [entry_sec − 60, t − 1] 内有盘口才算捕获
  //   (锚点后无入环簿 = 顺延中, 等下一活跃秒; 一直没有 → day_end 按收盘价建仓). 捕获即写该行的冲击成本列 (只依赖 entry 盘口).
  //   只在可成交更新上推进 (snapshot 刚把 pending 设为 t): 集合竞价期的更新什么都不做 —— 连续竞价已结束,
  //   悬着的行都是"持有到收盘", 留给 day_end 按收盘价结算. 不用时钟判 14:57.
  //   分钟型标签 / 平仓顺延由骨架按价点写 (见 LabelHold::push_entry). writer(col, n, label_min, values, days_ago) 负责落盘.
  template <class Writer>
  inline void minute_anchored(size_t t, Writer &&writer) {
    if (pending_sec_ != t)
      return; // 本次更新不可成交 (集合竞价)
    while (n_entry() < TRADE_MINUTES_PER_DAY) {
      const size_t m = n_entry();
      const size_t entry_sec = L_1min_to_L_sec(m + 1) + LABEL_DELAY_SECONDS; // 末分钟 254 → 15303 (盘后), 永不捕获 → day_end
      if (entry_sec >= t)
        break; // entry 秒尚未完结: 同秒只消费最后一次盘口
      const Snapshot *e = get_snapshot_tradable(entry_sec, t - 1);
      if (!e)
        break; // 锚点起至 t−1 无可成交簿 (09:25-09:30 空窗等), 顺延中
      write_cost(m, *e, writer);
      push_entry(px_of(*e), writer);
    }
  }

  // 分钟 bar 结算 (CoreTs 只在有成交的分钟调): 只截开盘价点 (开盘型平仓 / 喂等待); entry 走盘口 (minute_anchored)
  template <class Writer>
  inline void minute_closed(size_t t_min, Writer &&writer) {
    open_at(t_min, md_, writer);
  }

  // 收盘结算 (当日有无盘口皆调):
  //   收盘 = 当日末笔成交价全额成交 (收盘竞价是单一价撮合: 无价差无冲击). 不读收盘竞价期的簿 (堆单, 可交叉, 不可成交),
  //     也不用时钟判 15:00 —— MinuteData 末 bar 的 close 就是末笔成交 (CoreLob::end_day 先 finish 末分钟再调这里;
  //     末分钟 254 含 14:57-15:00, 有撮合即撮合价, 无撮合即连续竞价末笔). 全日有入环簿却无一笔成交 (极罕见) → 退到最后入环簿.
  //   分钟型尾部 (行 m+n 的 entry 锚点落在连续竞价之后, 永不捕获 ⇒ 行 m 的 exit 永不到位) —— T0 头寸必须当日平掉,
  //     标签 = "持有到收盘" 的真实可交易收益, 而非缺失. 持有窗口随行号递减, 缩到 0 时自然退化为 0 (收盘价建仓即平), 连续无跳变.
  //   收盘型 全行 / 日级型到期 exit = 收盘 (骨架 close_day).
  //   全日无入环簿 (缺数据 / 停牌日): 当日行无 entry, 不写; 因子层 (FactorsLoad) 把"当日无任何成交"的 asset-day 剔出截面池门控.
  //   writer(col, n, t_min, values, days_ago); release(days_ago) = 该日全部组已写完, 写句柄可归还.
  template <class Writer, class Release>
  inline void day_end(Writer &&writer, Release &&release) {
    if (pending_sec_ != kNoPending)
      flush_pending(0); // 最后一个可成交秒之后再无盘口更新: 环末就是它的终簿
    const bool traded = last_sec_ != kNoPending;
    const float bar_close = md_.close.empty() ? 0.0f : md_.close.back();
    LabelPx close;
    if (traded)
      close = bar_close > 0.0f ? auction_px(bar_close) : px_of(ring_[last_sec_ % RING_SIZE]); // 全日无成交 (极罕见) → 最后可成交簿
    // 锚点后再无可成交簿 (14:57 起的锚点 / 至收盘无盘口): 收盘价建仓, 冲击 0
    close_day(traded, close, writer, release, [&](size_t m, const LabelPx &c) { write_cost(m, at_price(c), writer); });
  }

  // 每日重置: 快照环作废, 行游标归零; 当日涨跌停价 / 复权比取 Fund 日频口 back() (盘前承接之后 = 当日适用, 收盘后 Fund 已推进到次日)
  inline void reset() {
    LabelHold::reset();
    assert(!lim_up_src_.empty() && !lim_dn_src_.empty() && !adj_ratio_.empty() && "LabelLob: Fund 日频口盘前须已承接");
    lim_up_ = lim_up_src_.back();
    lim_dn_ = lim_dn_src_.back();
    px_scale_ = adj_ratio_.back();
    for (auto &snap : ring_)
      snap.valid = false;
    pending_sec_ = kNoPending; // 昨日最后一个活跃秒不结算 (当日 ring 已整体作废)
    last_sec_ = kNoPending;
  }

private:
  // 当前盘口可成交 = 连续竞价时段. 集合竞价 (含撮合期) 的簿是撮合前的堆单, 不可成交; 其余状态 (CLOSED, 脏时间戳) 也不算
  inline bool tradable() const {
    const auto s = td_.lob.market_state;
    return s == L2::MarketState::CONTINUOUS_TRADING_MORNING || s == L2::MarketState::CONTINUOUS_TRADING_AFTERNOON;
  }

  // 悬着的活跃秒结算入环: offset=1 在下一次盘口更新到来时 (Depth 环上一格 = 该秒终簿), offset=0 在收盘 (环末即该秒终簿)
  //   终簿吃不到 (valid = false) 即不入环, 不算活跃秒
  inline void flush_pending(size_t offset) {
    Snapshot &s = ring_[pending_sec_ % RING_SIZE];
    materialize(s, pending_sec_, offset);
    if (s.valid)
      last_sec_ = pending_sec_;
    pending_sec_ = kNoPending;
  }

  static LabelPx px_of(const Snapshot &s) { return {s.px, s.buy, s.sell}; }

  // 单一价全额成交 (收盘竞价撮合价): 中间价 = px, 各金额段冲击 0
  static Snapshot at_price(const LabelPx &c) {
    Snapshot s;
    s.valid = true;
    s.px = c.px;
    s.buy = c.buy;
    s.sell = c.sell;
    return s;
  }

  // 环长: 只给 entry 捕获查 (get_snapshot 60s 回溯 + 顺延扫描到 t−1), 不需覆盖持有期; 条目按 sec_index 核对, 过长的盘口空窗只是多扫几格
  static constexpr size_t RING_SIZE = 1024;

  // 行 m 的冲击成本列 [cost_buy × 各金额, cost_sell × 各金额] (当日, days_ago 0); 不可成交的那侧 NaN (materialize 已置)
  template <class Writer>
  static inline void write_cost(size_t m, const Snapshot &entry, Writer &&writer) {
    float values[COST_COUNT];
    for (size_t a = 0; a < AMT_COUNT; ++a) {
      values[a] = entry.buy ? entry.cost_buy[a] : kNaN;
      values[AMT_COUNT + a] = entry.sell ? entry.cost_sell[a] : kNaN;
    }
    writer(COST_COL, COST_COUNT, m, static_cast<const float *>(values), size_t{0});
  }

  // 从 Depth 环末尾回退 offset 格的盘口状态结算快照 (0 = 当前更新, 1 = 上一次更新):
  //   中间价 = (买一 + 卖一)/2; 一侧全空 (封板) 取另一侧 (= 涨跌停价).
  //   入环 (valid) = 有中间价; 可买 = 卖一侧有挂单, 可卖 = 买一侧有挂单 (封涨停 → 只可卖, 封跌停 → 只可买)
  //   冲击 (逐侧: 买入要可买, 卖出要可卖) = 吃 a 万的 VWAP 相对中间价 (余量按涨跌停价, 见 calc_vwap), 恒有限; 不可成交的那侧 NaN
  //   px 存后复权价 (mid × 当日复权比); 冲击是同日比值, 按原始价算
  void materialize(Snapshot &snap, size_t t_sec, size_t offset) const {
    assert(px_scale_ > 0.0f && "有可成交盘口但当日无复权比 (bar1d adjust_factor 缺失)");
    snap.sec_index = t_sec;
    assert(bid_price_[0].size() > offset && ask_price_[0].size() > offset && "materialize: Depth 环深度不足 offset");
    const size_t k = bid_price_[0].size() - 1 - offset;
    const float b1 = bid_price_[0][k], a1 = ask_price_[0][k];
    const bool has_b = b1 > 1e-6f && bid_qty_[0][k] > 1e-6f, has_a = a1 > 1e-6f && -ask_qty_[0][k] > 1e-6f;
    const float mid = has_b && has_a ? 0.5f * (b1 + a1) : (has_b ? b1 : (has_a ? a1 : 0.0f));
    snap.px = mid * px_scale_;
    snap.valid = mid > 1e-6f;
    snap.buy = has_a;
    snap.sell = has_b;
    for (size_t a = 0; a < AMT_COUNT; ++a) {
      const float amt = static_cast<float>(LABEL_AMOUNT_WAN[a]) * 10000.0f;
      const float vb = has_a ? calc_vwap(ask_price_, ask_qty_, amt, true, offset, lim_up_) : 0.0f;  // 吃 ask (买入): 余量按涨停
      const float vs = has_b ? calc_vwap(bid_price_, bid_qty_, amt, false, offset, lim_dn_) : 0.0f; // 吃 bid (卖出): 余量按跌停
      assert((!has_a || vb > 1e-6f) && (!has_b || vs > 1e-6f) && "materialize: 有挂单的一侧须吃得到");
      snap.cost_buy[a] = has_a ? vb / mid - 1.0f : kNaN;
      snap.cost_sell[a] = has_b ? 1.0f - vs / mid : kNaN;
    }
  }

  // 指定 t_sec 时刻的快照; 深度不是每秒都更新, 向前找 ≤60s 内最近的有效快照
  const Snapshot *get_snapshot(size_t target) const {
    if (target == pending_sec_) { // 当前秒未结算: 从环末现算 (只有锚点查询走到, 频次 ~分钟级); 吃不到则照常回溯
      materialize(scratch_, target, 0);
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
  // 时顺延到 limit 之前首个可成交时刻 —— "最早能成交的时刻才是成交点", 建仓 / 平仓同约, 比留 NaN 贴近 T0 实盘.
  // 取锚点之后的快照不引入前视: 标签本就是未来量, 行 m 的特征在分钟 m 末已定.
  const Snapshot *get_snapshot_tradable(size_t target, size_t limit) const {
    if (const auto *s = get_snapshot(target))
      return s;
    for (size_t t_sec = target + 1; t_sec <= limit; ++t_sec) {
      if (t_sec == pending_sec_) { // 当前秒未结算, 同 get_snapshot: 从环末现算
        materialize(scratch_, t_sec, 0);
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
  // offset: 从 Depth 环末尾回退几格取盘口 (所有档的环长同步推进, 下标一致)
  // limit_px: 全簿吃不完时余量的成交价 (买 → 涨停, 卖 → 跌停), 与 Book 的吃单成本同约;
  //           涨跌停封板 (该侧全空, 如涨停无卖盘) 也走这条; 边界 NaN (无限制股) 不补 → 该侧全空时 0 (materialize 已判不可成交)
  static inline float calc_vwap(const DepthSeries &price, const DepthSeries &qty, float amount, bool is_buy, size_t offset, float limit_px) {
    assert(price[0].size() > offset && "calc_vwap: Depth 环深度不足 offset");
    float cost = 0.0f, sh = 0.0f;
    for (size_t i = 0; i < L2::LOB_DEPTH && amount > 1e-6f; ++i) {
      const size_t k = price[i].size() - 1 - offset;
      const float p = price[i][k];
      const float q = is_buy ? -qty[i][k] : qty[i][k];
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

  const TickData &td_;   // market_state: 当前簿可成交与否
  const MinuteData &md_; // 末 bar close = 当日末笔成交价 (收盘)
  const DepthSeries &bid_price_;
  const DepthSeries &ask_price_;
  const DepthSeries &bid_qty_;
  const DepthSeries &ask_qty_;
  const Series &lim_up_src_, &lim_dn_src_; // Fund 日频口 (lim_up / lim_dn)
  const Series &adj_ratio_;                // Fund 日频口 (当日复权比 → px_scale_); 当日涨跌停价在 LabelHold (簿子吃不完时补齐余量也用)

  static constexpr size_t kNoPending = SIZE_MAX;

  std::array<Snapshot, RING_SIZE> ring_; // 盘口快照环 (只收可成交簿)
  size_t pending_sec_ = kNoPending;      // 当前活跃秒 (有可成交盘口更新, 尚未结算入环)
  size_t last_sec_ = kNoPending;         // 当日最近一个已入环的活跃秒 (kNoPending = 全日尚无可成交簿)
  mutable Snapshot scratch_;             // pending 秒被查询时的现算暂存
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 行由配置生成, 顺序 = LB 块布局: L_1min 每个 hold 一组 [long, short] (组序 = LABEL_GROUPS 序, 与 hold_col(h) 对应),
// 之后 lb_entry_ok (ENTRY_COL), [cost_buy × LABEL_AMTS, cost_sell × LABEL_AMTS] (与 COST_COL / write_cost 对应).
// 每种 KIND 的描述片段 (英 / 中 在 LabelGroups.hpp; exit 备注 / 公式尾在此), 由 LABEL_ROW 按 kind token 拼接.
// 备注统一三段: 平仓价 | 顺延规则 | 尾部规则 (收盘型无顺延 / 尾部)
#define LABEL_NOTE_MIN(n) "锚点后整" #n "分钟+3s的连续竞价盘口中间价(= 行m+" #n "的entry快照, 与entry同基准, 相邻行精确伸缩); 空窗则顺延至首个入环簿; 该锚点落入14:57起则持有到当日收盘(末笔成交价)"
#define LABEL_NOTE_CLOSE(n) "当日收盘价(末笔成交价=收盘竞价撮合价)"
#define LABEL_NOTE_OPEN(n) "T+" #n "日开盘价(09:25集合竞价撮合价, 无撮合则连续竞价首笔成交价)"
#define LABEL_NOTE_CLOSE_N(n) "T+" #n "日收盘价(末笔成交价=收盘竞价撮合价)"
#define LABEL_TEX_MIN(n) R"(, \quad P_{exit}=P^{mid}_{t_{entry}+)" #n R"(\,\mathrm{min}})"
#define LABEL_TEX_CLOSE(n) R"(, \quad P_{exit}=P^{close}_{D})"
#define LABEL_TEX_OPEN(n) R"(, \quad P_{exit}=P^{open}_{D+)" #n "}"
#define LABEL_TEX_CLOSE_N(n) R"(, \quad P_{exit}=P^{close}_{D+)" #n "}"
// 描述统一四段: 口径 | entry | exit | 净收益换算. entry 各型共用, exit 按 KIND, 净收益按 side (做多扣买入冲击, 做空扣卖出冲击)
#define LABEL_ROW(X, CAT1, side, cost, en, cn, formula, name, kind, n)                                \
  X(lb_##side##_##name, CAT1, ret, en " " LABEL_EN_##kind(n) " Return", cn LABEL_CN_##kind(n) "收益", \
    cn LABEL_CN_##kind(n) "价格收益(比例; 不含冲击与税佣; 后复权). entry: 锚点(分钟末)+3s的连续竞价盘口中间价, 空窗则顺延至首个入环簿, 锚点后再无入环簿则按收盘价建仓; entry盘口可否买卖见lb_entry_ok(单侧簿照写, 消费端按持仓判). exit: " LABEL_NOTE_##kind(n) "; 该腿不可成交(封板)或停牌则顺延至之后首个该腿可成交价点, 环满(" LABEL_DELAY_NOTE ")按最近收盘价; 越过区间末则按最后一日收盘价. 净收益=本列−lb_cost_" cost "_<a>w−Config::fixed_impact(消费端可选扣减)", formula LABEL_TEX_##kind(n), LABEL)
#define LABEL_ROW_LONG(name, kind, n, X, CAT1) LABEL_ROW(X, CAT1, long, "buy", "Long", "做多", R"(\frac{P_{exit}-P^{mid}_{entry}}{P^{mid}_{entry}})", name, kind, n)
#define LABEL_ROW_SHORT(name, kind, n, X, CAT1) LABEL_ROW(X, CAT1, short, "sell", "Short", "做空", R"(\frac{P^{mid}_{entry}-P_{exit}}{P^{mid}_{entry}})", name, kind, n)
#define LABEL_GROUP(name, kind, n, X, CAT1) LABEL_ROW_LONG(name, kind, n, X, CAT1) LABEL_ROW_SHORT(name, kind, n, X, CAT1)
#define LABEL_ENTRY(X, CAT1) \
  X(lb_entry_ok, CAT1, flag, "Entry Tradable", "建仓可成交", "entry盘口可成交码: 可买(卖一侧有挂单)=1 | 可卖(买一侧有挂单)=2 (封涨停=2, 封跌停=1, 两向=3; 收盘价点按单一价撮合判). 各持有期组共用", LABEL_ENTRY_TEX, LABEL)
// 建仓冲击成本 (比例, ≥0): 与标签同一 entry 盘口, 吃 a 万元的成交 VWAP 相对中间价的偏离; 全簿不足余量按涨跌停价成交; 该侧不可成交 NaN
// 描述统一四段: 口径 | 吃单 | 边界 | 净收益换算 (买入 / 卖出逐词对应)
#define LABEL_COST_BUY(a, X, CAT1)                                                         \
  X(lb_cost_buy_##a##w, CAT1, ratio_pos, "Entry Buy Impact " #a "w", "买入冲击(" #a "万)", \
    "建仓买入冲击(比例≥0; 标签不含). 吃单: entry同一盘口吃ask " #a "万元, 成交VWAP相对中间价的溢价. 边界: 全簿不足余量按涨停价成交; 无卖盘(封涨停, 不可买)则NaN. 做多净收益=lb_long_*−本列−Config::fixed_impact", R"(\frac{\mathrm{VWAP}^{A}_{entry}(A)}{P^{mid}_{entry}}-1, \quad A=)" #a R"(\mathrm{w})", LABEL)
#define LABEL_COST_SELL(a, X, CAT1)                                                          \
  X(lb_cost_sell_##a##w, CAT1, ratio_pos, "Entry Sell Impact " #a "w", "卖出冲击(" #a "万)", \
    "建仓卖出冲击(比例≥0; 标签不含). 吃单: entry同一盘口吃bid " #a "万元, 成交VWAP相对中间价的折价. 边界: 全簿不足余量按跌停价成交; 无买盘(封跌停, 不可卖)则NaN. 做空净收益=lb_short_*−本列−Config::fixed_impact", R"(1-\frac{\mathrm{VWAP}^{B}_{entry}(A)}{P^{mid}_{entry}}, \quad A=)" #a R"(\mathrm{w})", LABEL)

#define FIELDS_L_1min_LabelLob(X, CAT1) LABEL_GROUPS(LABEL_GROUP, X, CAT1) LABEL_ENTRY(X, CAT1) LABEL_AMTS(LABEL_COST_BUY, X, CAT1) LABEL_AMTS(LABEL_COST_SELL, X, CAT1)
