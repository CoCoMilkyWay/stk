#pragma once

// =============================================================================
// LabelReturn - 价格收益标签 + 建仓冲击成本特征
// =============================================================================
//   标签 (lb_long_<h> / lb_short_<h>) = 纯价格收益, 不含深度冲击、不含税佣:
//     做多: (exit − mid_entry) / mid_entry        做空: (mid_entry − exit) / mid_entry
//     entry = 锚点 (分钟末) + LABEL_DELAY_SECONDS 的可成交盘口 **中间价** (精确到秒的建仓时刻, 对 entry 敏感);
//     exit  = 分钟档: 名义平仓时刻所在分钟的 **成交 VWAP** (= Flow.vwap 特征, Σ成交额/Σ成交量, 买卖主动单都算; 对 exit 的高频噪声不敏感);
//             收盘档: 当日收盘价 (末笔成交价 = 收盘竞价撮合价); 开盘档: T+N 日开盘价 (09:25 集合竞价撮合价, 无撮合则连续竞价首笔成交价).
//   冲击成本特征 (lb_cost_{buy,sell}_<a>w) = entry 同一盘口吃 a 万元的 VWAP 相对中间价的偏离 (比例, ≥0):
//     cost_buy = VWAP^A(a)/mid − 1,  cost_sell = 1 − VWAP^B(a)/mid.  "mid − 成交价" 单拆出来, 标签 − 成本 = 精确的吃单成交收益;
//     默认不扣 (Factors Run 只看毛), 消费端 (Inspect 冲击选择) 即兴扣: lv − cost_buy(a) − sell_impact, sv − cost_sell(a) − sell_impact,
//     平仓侧冲击 Config::sell_impact 为固定模拟值 (不按金额变). 税佣 (Config::commission / stamp) 同理事后扣.
//     为什么不进标签: 评估 (IC / 超额) 对常数平移不变, 冲击 / 税佣留在标签里只会把 LS / Sharpe 按每行一次往返压死 (1m 档一天 255 次),
//     高频因子没法研究; 拆成特征后其他阶段想加随时能加.
//   【不产 NaN (标签)】标签一律取"实盘真能做到的那笔交易", 缺口用可成交时刻/价格顶上, 幅值都在收益量纲内, 多日拉取无跳变:
//     可成交簿 = 连续竞价时段的盘口 (LOB market_state). 集合竞价期 (09:15-09:25 / 14:57-15:00) 的簿只是堆单,
//       可交叉 (买一 ≥ 卖一) 且撮合前一股不成交, 拿它定建仓价 = 假价格, 一律不入环.
//     entry 锚点在非交易空窗 (09:25 撮合后到 09:30 开盘) / 稀疏盘口 → 顺延到之后首个可成交簿 = 最早能建仓的时刻 (见 get_snapshot_tradable)
//     名义 exit 分钟无成交 → 之后首个有成交的连续竞价分钟的 VWAP = 最早能平的时刻 (09:30 前的名义 exit 退化为开盘首分钟 VWAP)
//     exit 落在连续竞价结束之后 (14:57 起的分钟) → 持有到收盘 (T0 当日平仓): 按当日末笔成交价 (= 收盘竞价撮合价,
//       无撮合则连续竞价末笔; 取 MinuteData 末 bar close) 平仓 —— 用成交而非时钟判收盘 (见 day_end)
//     entry 锚点后再无可成交簿 (14:57 起) → 收盘价建仓 (mid = close, 冲击 0): 分钟档 / 收盘档 即建即平 = 0, 开盘档 = 收盘买次日开盘卖
//     中间价: 买一 / 卖一 皆在取均值; 一侧全空 (涨跌停封板) 取另一侧 (= 涨跌停价); 双侧全空 → 无价, 这笔交易成不了, 收益 0 (未建仓)
//     冲击成本: 全簿吃不完 / 封板该侧全空 → 余量按涨跌停价成交 (与 Book 吃单成本同约, 见 calc_vwap); 该侧全空且无涨跌停价可补
//       (无限制股) → 吃不到, 成本 NaN (消费端扣减时该格无效) —— 这是成本特征唯一的 NaN 来源
//
// 非 DAG 节点 (未来标签需回填, 不走 Node). 一份盘口快照环 (中间价 + 各金额档冲击) 只给 entry 用; exit 走分钟 bar:
//   snapshot(t)                 每次 onDepth 先调: 只记账, 快照惰性结算 —— 同一秒内只有最后一次
//                               盘口状态会被消费, 所以等下一次盘口更新到来时才从 Depth 环的上一格结算
//                               入环 (materialize offset=1); 查询目标恰为当前秒 (还没结算) 时从环末现算
//                               (offset=0). 与"每次更新都算"逐值一致, 计算次数从每笔盘口更新降到每活跃秒一次.
//                               只有可成交簿记为活跃秒.
//   minute_anchored(t, writer)  L1 分钟锚定 entry 捕获 (锚点 = 分钟末, 与行 m 特征的可知时刻对齐), 只在可成交更新上推进:
//                               把 entry 秒已完结的行的 entry 快照存进当日悬挂槽 (所有组共用一份 entry), 同时写该行的冲击成本列.
//   minute_closed(l1, w)        分钟 bar 结算后 (CoreSequential::run_minute, 只在有成交的分钟调: 无成交无法平仓): 连续竞价分钟的 Flow.vwap 作 exit,
//                               写分钟档已到 exit 的行; 09:25 撮合 bar 的撮合价记为开盘价 (开盘档 T+N 的 exit),
//                               无撮合则退首个连续竞价 bar 的首笔成交价 (交易所开盘价定义).
//   day_begin / day_end         日历: 日期轴每一日各调一次 (无数据日也调, 由 CoreSequential 保证).
//                               day_end 结算 分钟档尾部 (exit 过收盘 → 收盘价) + 收盘档 全行 + 开盘档 到期日 (见下), 并释放结清的日.
//   finish_all                  回测区间末: 悬挂日全部按 最近一次收盘价 结算 (持有到最后一日收盘), 全部释放.
// 开盘档 (T+N) 跨日回填: 日 D 的行 entry 存在悬挂槽里, 等到 D+N 日开盘 (集合竞价撮合价; 单一价全额成交, 无价差无冲击) 才写 D 的张量 →
//   D 的写句柄由 CoreSequential 持到该日结清 (writer 带 days_ago). D+N 无开盘 (停牌 / 全日无成交) → 顺延到之后首个有开盘的日,
//   最多再等 N 日 (D+2N 仍无 → 最近一次收盘价). 悬挂日环长 PEND_DAYS = 2·N_max + 1, 顶掉的槽必已结清 (断言).
// 落盘列 (LB 块, 连续): [hold 组 × (long, short)] + [cost_buy × 各金额, cost_sell × 各金额]; writer(col, n, l1, values, days_ago)
//   写 LB 块内偏移 col 起的 n 列. 配置 (LABEL_GROUPS / LABEL_AMTS) 同时生成 constexpr 数组和落盘字段行, 只改一处.
// =============================================================================

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp" // L1_to_L0 (分钟锚定路径)
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iterator>

// ---- 配置 ----
// 持有期组 (name, KIND, n): name = 列名 token (lb_<side>_<name>; 也是 Stat 持有期键的来源, 见 factor/Stat/Contract.hpp 【持有期键】)
//   MIN   持仓 n 分钟 (exit = 分钟 m+1+n 的 VWAP, 即名义平仓时刻所在分钟)
//   CLOSE 持有到当日收盘 (n 不用, 写 0)
//   OPEN  T+n 开盘平仓 (跨日回填, 见文件头)
#define LABEL_GROUPS(X, ...)      \
  X(1m, MIN, 1, __VA_ARGS__)      \
  X(5m, MIN, 5, __VA_ARGS__)      \
  X(15m, MIN, 15, __VA_ARGS__)    \
  X(30m, MIN, 30, __VA_ARGS__)    \
  X(60m, MIN, 60, __VA_ARGS__)    \
  X(close, CLOSE, 0, __VA_ARGS__) \
  X(t1, OPEN, 1, __VA_ARGS__)     \
  X(t3, OPEN, 3, __VA_ARGS__)     \
  X(t5, OPEN, 5, __VA_ARGS__)
#define LABEL_AMTS(X, ...) X(5, __VA_ARGS__) X(30, __VA_ARGS__) // 冲击成本特征的下单金额档 (万元), 升序
constexpr size_t LABEL_DELAY_SECONDS = 3;                       // 下单延迟 (秒)

enum class HoldKind : uint8_t { MIN,
                                CLOSE,
                                OPEN };
#define LABEL_KIND_ONE(name, kind, n, ...) HoldKind::kind,
#define LABEL_N_ONE(name, kind, n, ...) n,
#define LABEL_LIST_ONE(v, ...) v,
inline constexpr HoldKind LABEL_KIND[] = {LABEL_GROUPS(LABEL_KIND_ONE)};
inline constexpr size_t LABEL_N[] = {LABEL_GROUPS(LABEL_N_ONE)};
inline constexpr size_t LABEL_AMOUNT_WAN[] = {LABEL_AMTS(LABEL_LIST_ONE)};
#undef LABEL_KIND_ONE
#undef LABEL_N_ONE
#undef LABEL_LIST_ONE

class LabelReturn {
public:
  static constexpr size_t HOLD_COUNT = std::size(LABEL_KIND);
  static constexpr size_t AMT_COUNT = std::size(LABEL_AMOUNT_WAN);
  static constexpr size_t GROUP_SIZE = 2;                         // 一个 hold 组: [long, short]
  static constexpr size_t COST_COL = HOLD_COUNT * GROUP_SIZE;     // 冲击成本列在 LB 块内的起始偏移
  static constexpr size_t COST_COUNT = 2 * AMT_COUNT;             // [cost_buy × 各金额, cost_sell × 各金额]
  static constexpr size_t L1_LABEL_COUNT = COST_COL + COST_COUNT; // LB 块总列数
  static constexpr size_t hold_col(size_t h) { return h * GROUP_SIZE; }
  static_assert(HOLD_COUNT <= 32, "开盘档结算位图用 uint32_t");
  static_assert([] {
    for (size_t a = 1; a < AMT_COUNT; ++a)
      if (LABEL_AMOUNT_WAN[a] <= LABEL_AMOUNT_WAN[a - 1])
        return false;
    return true;
  }(),
                "LABEL_AMTS 须严格升序");

  // 开盘档最大 N: 日 D 的 T+N 组最晚在 D+2N 结算 → 同时悬挂 (张量未结清, 写句柄未归还) 的日数 ≤ PEND_DAYS (含当日)
  static constexpr size_t OPEN_MAX = [] {
    size_t m = 0;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] == HoldKind::OPEN)
        m = std::max(m, LABEL_N[h]);
    return m;
  }();
  static constexpr size_t PEND_DAYS = 2 * OPEN_MAX + 1;
  static_assert([] {
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] != HoldKind::CLOSE && LABEL_N[h] < 1)
        return false;
    return true;
  }(),
                "分钟档 / 开盘档的 n 须 ≥ 1");
  // FeatureStore 池下限: 悬挂日全 BUSY 时同日的 TS 还要 1 个 slot 才能推进 (CS / IO 阶段的 slot 另算, 只影响吞吐不致死锁)
  static constexpr size_t MIN_POOL_SLOTS = PEND_DAYS + 1;

  // 连续竞价分钟区间 [kContOpenL1, kContCloseL1) = [09:30, 14:57): 只有这些分钟的 bar VWAP 可作分钟档 exit.
  // 09:25 撮合 (L1 kAuctionL1) / 收盘竞价 (L1 252..254) 的 bar 是集合竞价成交, 不是能主动平仓的价:
  // 开盘撮合价 = 开盘档 exit (开盘价), 收盘用末笔成交价走 day_end.
  static constexpr size_t kAuctionL1 = (L2::MORNING_MATCHING_START_HOUR * 60 + L2::MORNING_MATCHING_START_MINUTE) - MORNING_START_MIN;
  static constexpr size_t kContOpenL1 = (L2::CONTINUOUS_TRADING_MORNING_START_HOUR * 60 + L2::CONTINUOUS_TRADING_MORNING_START_MINUTE) - MORNING_START_MIN;
  static constexpr size_t kContCloseL1 = MORNING_MINUTES + (AFTERNOON_END_MIN - AFTERNOON_START_MIN);
  static_assert(kAuctionL1 == 10 && kContOpenL1 == 15 && kContCloseL1 == 252);

  LabelReturn(const TickData &td, const MinuteData &md,
              const DepthSeries &bid_price, const DepthSeries &ask_price,
              const DepthSeries &bid_qty, const DepthSeries &ask_qty,
              const float &lim_up, const float &lim_dn, const float &vwap)
      : td_(td), md_(md), bid_price_(bid_price), ask_price_(ask_price), bid_qty_(bid_qty), ask_qty_(ask_qty),
        lim_up_(lim_up), lim_dn_(lim_dn), vwap_(vwap) {}

  // 记录当前秒有盘口更新 (每次 onDepth 调一次, 先于 second / minute_anchored).
  // 换秒 (或本次更新不可成交) 时把上一个活跃秒结算入环: Depth 环的上一格 (offset=1) 正是那一秒最后
  // 一次更新的盘口 —— 与旧实现"每次更新覆盖写"的最终留存值逐位相同. Depth 每次 onDepth 必推一格
  // (集合竞价的更新也推), 所以不管本次可不可成交都得立刻结算, 否则 offset 对不上.
  // 可成交 = 连续竞价时段 (LOB 的 market_state); 集合竞价期的簿不入环, pending 清空 → minute_anchored 本次不推进.
  inline void snapshot(size_t t) {
    const bool ok = tradable();
    if (pending_l0_ != kNoPending && (pending_l0_ != t || !ok))
      flush_pending(1);
    if (ok)
      pending_l0_ = t;
  }

  // L1 分钟锚定 entry 捕获: 锚点 = 分钟 m 末 (= m+1 起始秒; 11:29 → 13:00:00), entry = 锚点+DELAY.
  //   L1 行 m 的特征是分钟 m 结束时才可知的 (CoreSequential 顺序), 所以标签只能从 m 末起算, 锚到 m 起始秒会前视一分钟.
  //   entry 快照按行存进当日悬挂槽 (所有组共用): entry 秒完结 (entry_l0 < t) 且 [entry_l0 − 60, t − 1] 内有盘口才算捕获
  //   (锚点后无可成交簿 = 顺延中, 等下一活跃秒; 一直没有 → day_end 按收盘价建仓). 捕获即写该行的冲击成本列 (只依赖 entry 盘口).
  //   只在可成交更新上推进 (snapshot 刚把 pending 设为 t): 集合竞价期的更新什么都不做 —— 连续竞价已结束,
  //   悬着的行都是"持有到收盘", 留给 day_end 按收盘价结算. 不用时钟判 14:57.
  //   writer(col, n, label_l1, values, days_ago) 负责落盘 (此处 days_ago 恒 0).
  template <class Writer>
  inline void minute_anchored(size_t t, Writer &&writer) {
    if (pending_l0_ != t)
      return; // 本次更新不可成交 (集合竞价)
    Pend &p = pend_[cur_];
    while (p.n_entry < TRADE_MINUTES_PER_DAY) {
      const size_t entry_l0 = L1_to_L0(p.n_entry + 1) + LABEL_DELAY_SECONDS; // 末分钟 254 → 15303 (盘后), 永不捕获 → day_end
      if (entry_l0 >= t)
        break; // entry 秒尚未完结: 同秒只消费最后一次盘口
      const Snapshot *e = get_snapshot_tradable(entry_l0, t - 1);
      if (!e)
        break; // 锚点起至 t−1 无可成交簿 (09:25-09:30 空窗等), 顺延中
      p.entry[p.n_entry] = *e;
      write_cost(p.n_entry, *e, writer);
      ++p.n_entry;
    }
  }

  // 分钟 bar 结算 (CoreSequential 只在有成交的分钟调; onMinute 域已 flush, Flow.vwap = 本分钟 Σ额/Σ量): 连续竞价分钟的 VWAP 作分钟档 exit 价.
  //   分钟档: 行 m 的名义 exit 分钟 = m+1+n (锚点分钟末 + n 分钟所在的分钟); 该分钟无成交 (不调本函数) → 自然落到之后首个有成交的
  //   分钟 = 最早能平的时刻. 只写 entry 已捕获的行 (未捕获的等 entry; 一直没有 → day_end).
  //   开盘价 (开盘档 exit): 09:25 撮合 bar 的撮合价 (单一价, bar 的 open = close); 无撮合 → 首个连续竞价 bar 的首笔成交价 (交易所定义).
  //   集合竞价分钟 (09:25 撮合 / 14:57 起) 的 bar 不作分钟档 exit: 开盘前名义 exit 的行等开盘首 bar; 盘尾的行持有到收盘 (day_end).
  template <class Writer>
  inline void minute_closed(size_t l1, Writer &&writer) {
    if (l1 == kAuctionL1) {
      assert(!open_ok_ && "09:25 撮合 bar 之前不该有开盘价");
      open_px_ = md_.close.back();
      open_ok_ = true;
      return;
    }
    if (l1 < kContOpenL1 || l1 >= kContCloseL1)
      return;
    const float vwap = vwap_;
    assert(vwap > 0.0f && "有成交的分钟 Flow.vwap 必 > 0");
    if (!open_ok_) {
      open_px_ = md_.open.back();
      open_ok_ = true;
    }
    const Pend &p = pend_[cur_];
    float values[GROUP_SIZE];
    for (size_t h = 0; h < HOLD_COUNT; ++h) {
      if (LABEL_KIND[h] != HoldKind::MIN)
        continue;
      for (;;) {
        const size_t m = next_label_l1_[h];
        if (m >= p.n_entry || m + 1 + LABEL_N[h] > l1)
          break; // entry 未捕获 (含 m == 255 全部写完) / 名义 exit 分钟未到
        fill_values(p.entry[m], vwap, values);
        writer(hold_col(h), GROUP_SIZE, m, static_cast<const float *>(values), size_t{0});
        ++next_label_l1_[h];
      }
    }
  }

  // ---- 日历 (日期轴每一日各调一次, 无数据日也调) ----

  // 新的一日: 悬挂环前进. 被顶掉的槽必已结清 (日 D 最晚 D+2N 结算, 环长 2N+1)
  inline void day_begin() {
    cur_ = (cur_ + 1) % PEND_DAYS;
    Pend &p = pend_[cur_];
    assert(!p.active && "悬挂日环被顶掉的日尚未结清 (PEND_DAYS 与到期规则不符)");
    p.n_entry = 0;
    p.open_mask = 0;
  }

  // 收盘结算 (当日有无盘口皆调):
  //   收盘 = 当日末笔成交价全额成交 (收盘竞价是单一价撮合: 无价差无冲击). 不读收盘竞价期的簿 (堆单, 可交叉, 不可成交),
  //     也不用时钟判 15:00 —— MinuteData 末 bar 的 close 就是末笔成交 (CoreSequential::end_day 先 finish 末分钟再调这里;
  //     末分钟 254 含 14:57-15:00, 有撮合即撮合价, 无撮合即连续竞价末笔). 全日有可成交簿却无一笔成交 (极罕见) → 退到最后可成交簿.
  //   分钟档尾部 (exit 落在连续竞价之后, 永不过线) —— T0 头寸必须当日平掉, 标签 = "持有到收盘" 的真实可交易收益, 而非缺失.
  //     持有窗口随行号递减, 缩到 0 时自然退化为 0 (收盘价建仓即平), 连续无跳变.
  //   收盘档 全行 exit = 收盘.
  //   开盘档 到期日: 悬挂日 P (age = 今日 − P) 的 T+N 组, age ≥ N 且今日有开盘 → exit = 今日开盘价; age ≥ 2N 仍无 → 最近一次收盘.
  //   全日无可成交簿 (缺数据 / 停牌日): 分钟 / 收盘档无标签可写, 全行留 0, 当日不悬挂; 开盘档到期照常结算.
  //     标签列 ALL 不受 ts_valid 门控 → 这些 0 会被当成收益 0; 在池停牌日靠 cs_valid / Fund.is_susp 在因子层剔 (待议).
  //   writer(col, n, l1, values, days_ago); release(days_ago) = 该日全部组已写完, 写句柄可归还.
  template <class Writer, class Release>
  inline void day_end(Writer &&writer, Release &&release) {
    Pend &p = pend_[cur_];
    if (pending_l0_ != kNoPending)
      flush_pending(0); // 最后一个可成交秒之后再无盘口更新: 环末就是它的终簿
    if (last_l0_ != kNoPending) {
      const float bar_close = md_.close.empty() ? 0.0f : md_.close.back();
      const float close_px = bar_close > 0.0f ? bar_close : ring_[last_l0_ % RING_SIZE].mid; // 全日无成交 (极罕见) → 最后可成交簿的中间价
      last_close_ = close_px;
      has_last_ = true;
      const Snapshot close_snap = at_price(close_px);
      for (size_t m = p.n_entry; m < TRADE_MINUTES_PER_DAY; ++m) {
        p.entry[m] = close_snap; // 锚点后再无可成交簿 (14:57 起的锚点 / 至收盘无盘口): 收盘价建仓, 冲击 0
        write_cost(m, close_snap, writer);
      }
      p.n_entry = TRADE_MINUTES_PER_DAY;
      float values[GROUP_SIZE];
      for (size_t h = 0; h < HOLD_COUNT; ++h) {
        if (LABEL_KIND[h] == HoldKind::OPEN)
          continue;
        const bool is_min = LABEL_KIND[h] == HoldKind::MIN;
        for (size_t m = is_min ? next_label_l1_[h] : 0; m < TRADE_MINUTES_PER_DAY; ++m) {
          fill_values(p.entry[m], close_px, values);
          writer(hold_col(h), GROUP_SIZE, m, static_cast<const float *>(values), size_t{0});
        }
        if (is_min)
          next_label_l1_[h] = TRADE_MINUTES_PER_DAY;
      }
      p.active = kOpenMask != 0; // 有开盘档才悬挂
    } else {
      p.active = false;
    }
    settle_open(writer, release, false);
  }

  // 回测区间末: 悬挂日全部按最近一次收盘价结算 (持有到最后一日收盘), 全部释放
  template <class Writer, class Release>
  inline void finish_all(Writer &&writer, Release &&release) {
    settle_open(writer, release, true);
  }

  // 每日重置: 快照环作废, 行游标归零
  inline void reset() {
    for (auto &snap : ring_)
      snap.valid = false;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      next_label_l1_[h] = 0;
    pending_l0_ = kNoPending; // 昨日最后一个活跃秒不结算 (当日 ring 已整体作废)
    last_l0_ = kNoPending;
    open_ok_ = false;
  }

private:
  // 可成交盘口快照 (entry 用): 中间价 + 各金额档吃单冲击
  struct Snapshot {
    float mid = 0.0f;                // 中间价 (建仓价); 0 = 双侧全空, 无价
    float cost_buy[AMT_COUNT] = {};  // 吃 ask a 万: VWAP^A/mid − 1 (≥0); NaN = 吃不到 (该侧全空且无涨停价)
    float cost_sell[AMT_COUNT] = {}; // 吃 bid a 万: 1 − VWAP^B/mid (≥0); NaN 同上
    size_t l0_index = 0;
    bool valid = false;
  };

  // 当前盘口可成交 = 连续竞价时段. 集合竞价 (含撮合期) 的簿是撮合前的堆单, 不可成交; 其余状态 (CLOSED, 脏时间戳) 也不算
  inline bool tradable() const {
    const auto s = td_.lob.market_state;
    return s == L2::MarketState::CONTINUOUS_TRADING_MORNING || s == L2::MarketState::CONTINUOUS_TRADING_AFTERNOON;
  }

  // 悬着的活跃秒结算入环: offset=1 在下一次盘口更新到来时 (Depth 环上一格 = 该秒终簿), offset=0 在收盘 (环末即该秒终簿)
  inline void flush_pending(size_t offset) {
    Snapshot &s = ring_[pending_l0_ % RING_SIZE];
    materialize(s, pending_l0_, offset);
    last_l0_ = pending_l0_;
    pending_l0_ = kNoPending;
  }

  // 单一价全额成交 (收盘竞价撮合价): 中间价 = px, 各金额档冲击 0
  static Snapshot at_price(float px) {
    Snapshot s;
    s.valid = true;
    s.mid = px;
    return s;
  }

  // 环长: 只给 entry 捕获查 (get_snapshot 60s 回溯 + 顺延扫描到 t−1), 不需覆盖持有期; 条目按 l0_index 核对, 过长的盘口空窗只是多扫几格
  static constexpr size_t RING_SIZE = 1024;

  // 开盘档位图 (按 h 下标)
  static constexpr uint32_t kOpenMask = [] {
    uint32_t m = 0;
    for (size_t h = 0; h < HOLD_COUNT; ++h)
      if (LABEL_KIND[h] == HoldKind::OPEN)
        m |= 1u << h;
    return m;
  }();

  // 悬挂日槽: 一日的 entry 快照 (全部组共用) + 开盘档结算进度
  struct Pend {
    bool active = false;                   // 有开盘档未结清 (写句柄未归还)
    uint32_t open_mask = 0;                // 已结算的开盘档 (位 = h)
    size_t n_entry = 0;                    // 已捕获 entry 的行数 (行按锚点单调, 前缀)
    Snapshot entry[TRADE_MINUTES_PER_DAY]; // 各行 entry 快照
  };

  // [long, short]: 建仓中间价 vs 平仓价 exit_px (分钟 VWAP / 收盘价 / 开盘撮合价)
  static inline void fill_values(const Snapshot &entry, float exit_px, float *values) {
    values[0] = calc_return(entry, exit_px, true);
    values[1] = calc_return(entry, exit_px, false);
  }

  // 行 m 的冲击成本列 [cost_buy × 各金额, cost_sell × 各金额] (当日, days_ago 0)
  template <class Writer>
  static inline void write_cost(size_t m, const Snapshot &entry, Writer &&writer) {
    float values[COST_COUNT];
    for (size_t a = 0; a < AMT_COUNT; ++a) {
      values[a] = entry.cost_buy[a];
      values[AMT_COUNT + a] = entry.cost_sell[a];
    }
    writer(COST_COL, COST_COUNT, m, static_cast<const float *>(values), size_t{0});
  }

  // 开盘档到期结算 (day_end 尾 / finish_all): 遍历悬挂日 (老日先, 释放序随日序 —— FeatureStore 的"d+1 计满 ⇒ d 计满"),
  //   到期组写出, 结清的日 release(age). final = 区间末: 不管到期, 全部按最近一次收盘价结算
  template <class Writer, class Release>
  inline void settle_open(Writer &&writer, Release &&release, bool final) {
    float values[GROUP_SIZE];
    for (size_t age = PEND_DAYS; age-- > 0;) {
      Pend &q = pend_[(cur_ + PEND_DAYS - age) % PEND_DAYS];
      if (!q.active) {
        if (age == 0 && !final)
          release(age); // 当日不悬挂 (无盘口 / 无开盘档): 句柄当日归还
        continue;
      }
      assert(has_last_ && "悬挂日必有 entry ⇒ 必有过盘口 ⇒ last_close_ 有值");
      for (size_t h = 0; h < HOLD_COUNT; ++h) {
        if (LABEL_KIND[h] != HoldKind::OPEN || (q.open_mask & (1u << h)))
          continue;
        const size_t n = LABEL_N[h];
        float exit_px = 0.0f;
        if (final || age >= 2 * n)
          exit_px = last_close_; // 区间末 / 顺延到期仍无开盘: 最近一次收盘价
        else if (age >= n && open_ok_)
          exit_px = open_px_; // 到期 (或顺延中) 且今日有开盘
        else
          continue;
        for (size_t m = 0; m < TRADE_MINUTES_PER_DAY; ++m) {
          fill_values(q.entry[m], exit_px, values);
          writer(hold_col(h), GROUP_SIZE, m, static_cast<const float *>(values), age);
        }
        q.open_mask |= 1u << h;
      }
      if (q.open_mask == kOpenMask) {
        q.active = false;
        release(age);
      }
    }
  }

  // 单个 label 的价格收益率; 建仓无价 (双侧全空) / 平仓无价 → 这笔交易根本成不了, 收益 0 (未建仓 = 无盈亏)
  static inline float calc_return(const Snapshot &entry, float exit_px, bool is_long) {
    const float mid = entry.mid;
    if (mid < 1e-6f || exit_px < 1e-6f)
      return 0.0f;
    return is_long ? (exit_px - mid) / mid : (mid - exit_px) / mid;
  }

  // 从 Depth 环末尾回退 offset 格的盘口状态结算快照 (0 = 当前更新, 1 = 上一次更新):
  //   中间价 = (买一 + 卖一)/2; 一侧全空 (封板) 取另一侧 (= 涨跌停价); 双侧全空 → 0 (无价).
  //   冲击 = 吃 a 万的 VWAP 相对中间价 (余量按涨跌停价, 见 calc_vwap); 吃不到 (VWAP 0) 或无中间价 → NaN
  void materialize(Snapshot &snap, size_t l0, size_t offset) const {
    snap.l0_index = l0;
    snap.valid = true;
    assert(bid_price_[0].size() > offset && ask_price_[0].size() > offset && "materialize: Depth 环深度不足 offset");
    const size_t k = bid_price_[0].size() - 1 - offset;
    const float b1 = bid_price_[0][k], a1 = ask_price_[0][k];
    const bool has_b = b1 > 1e-6f && bid_qty_[0][k] > 1e-6f, has_a = a1 > 1e-6f && -ask_qty_[0][k] > 1e-6f;
    const float mid = has_b && has_a ? 0.5f * (b1 + a1) : (has_b ? b1 : (has_a ? a1 : 0.0f));
    snap.mid = mid;
    for (size_t a = 0; a < AMT_COUNT; ++a) {
      const float amt = static_cast<float>(LABEL_AMOUNT_WAN[a]) * 10000.0f;
      const float vb = calc_vwap(ask_price_, ask_qty_, amt, true, offset, lim_up_);  // 吃 ask (买入): 余量按涨停
      const float vs = calc_vwap(bid_price_, bid_qty_, amt, false, offset, lim_dn_); // 吃 bid (卖出): 余量按跌停
      snap.cost_buy[a] = mid > 1e-6f && vb > 1e-6f ? vb / mid - 1.0f : kNaN;
      snap.cost_sell[a] = mid > 1e-6f && vs > 1e-6f ? 1.0f - vs / mid : kNaN;
    }
  }

  // 指定 l0 时刻的快照; 深度不是每秒都更新, 向前找 ≤60s 内最近的有效快照
  const Snapshot *get_snapshot(size_t target) const {
    if (target == pending_l0_) { // 当前秒未结算: 从环末现算 (只有锚点查询走到, 频次 ~分钟级)
      materialize(scratch_, target, 0);
      return &scratch_;
    }
    const auto &s = ring_[target % RING_SIZE];
    if (s.valid && s.l0_index == target)
      return &s;
    for (size_t off = 1; off <= 60 && off <= target; ++off) {
      const auto &ss = ring_[(target - off) % RING_SIZE];
      if (ss.valid && ss.l0_index == target - off)
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
    for (size_t l0 = target + 1; l0 <= limit; ++l0) {
      if (l0 == pending_l0_) { // 当前秒未结算, 同 get_snapshot: 从环末现算
        materialize(scratch_, l0, 0);
        return &scratch_;
      }
      const auto &s = ring_[l0 % RING_SIZE];
      if (s.valid && s.l0_index == l0)
        return &s;
    }
    return nullptr;
  }

  // 模拟吃单: 遍历盘口深度算 VWAP (返回; 吃不到一股 → 0). is_buy: 吃 ask (qty 存负值); 否则吃 bid (正值)
  // offset: 从 Depth 环末尾回退几格取盘口 (所有档的环长同步推进, 下标一致)
  // limit_px: 全簿吃不完时余量的成交价 (买 → 涨停, 卖 → 跌停), 与 Book 的吃单成本同约;
  //           涨跌停封板 (该侧全空, 如涨停无卖盘) 也走这条; 边界 NaN (无限制股) 不补 → 可能 0
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
  const float &lim_up_, &lim_dn_; // Fund 当日涨跌停价 (盘前已知), 簿子吃不完时补齐余量用
  const float &vwap_;             // Flow.y[vwap]: 本分钟成交 VWAP (onMinute flush 后有效), 分钟档 exit 价

  static constexpr size_t kNoPending = SIZE_MAX;

  std::array<Snapshot, RING_SIZE> ring_;  // 盘口快照环 (只收可成交簿)
  size_t next_label_l1_[HOLD_COUNT] = {}; // 分钟档: 各组下一个待写 L1 行 (收盘 / 开盘档不用)
  size_t pending_l0_ = kNoPending;        // 当前活跃秒 (有可成交盘口更新, 尚未结算入环)
  size_t last_l0_ = kNoPending;           // 当日最近一个已入环的活跃秒 (kNoPending = 全日尚无可成交簿)
  mutable Snapshot scratch_;              // pending 秒被查询时的现算暂存

  // 跨日状态 (reset 不清)
  std::array<Pend, PEND_DAYS> pend_; // 悬挂日环, pend_[cur_] = 当日; 日 D 的槽 = (cur_ − age) mod PEND_DAYS
  size_t cur_ = PEND_DAYS - 1;       // 首个 day_begin 推到 0
  float open_px_ = 0.0f;             // 当日开盘价 (09:25 撮合价; 无撮合则连续竞价首笔成交价), open_ok_ = 已截到 (reset 清)
  bool open_ok_ = false;
  float last_close_ = 0.0f; // 最近一次收盘 (最近有可成交簿日的末笔成交价), 停牌到期 / 区间末的 exit
  bool has_last_ = false;
};

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 行由配置生成, 顺序 = LB 块布局: L1 每个 hold 一组 [long, short] (组序 = LABEL_GROUPS 序, 与 hold_col(h) 对应),
// 之后 [cost_buy × LABEL_AMTS, cost_sell × LABEL_AMTS] (与 COST_COL / write_cost 对应).
// 每种 KIND 的描述片段 (英 / 中 / exit 备注 / 公式尾), 由 LABEL_ROW 按 kind token 拼接.
// 备注统一三段: 平仓价 | 顺延规则 | 尾部规则 (收盘档无顺延 / 尾部)
#define LABEL_EN_MIN(n) #n "min"
#define LABEL_EN_CLOSE(n) "to-Close"
#define LABEL_EN_OPEN(n) "T+" #n " Open"
#define LABEL_CN_MIN(n) #n "分钟"
#define LABEL_CN_CLOSE(n) "至收盘"
#define LABEL_CN_OPEN(n) "至T+" #n "开盘"
#define LABEL_NOTE_MIN(n) "锚点后第" #n "分钟所在分钟的成交VWAP(Σ额/Σ量); 该分钟无成交则顺延至之后首个有成交的连续竞价分钟; 越过14:57则持有到当日收盘(末笔成交价)"
#define LABEL_NOTE_CLOSE(n) "当日收盘价(末笔成交价=收盘竞价撮合价)"
#define LABEL_NOTE_OPEN(n) "T+" #n "日开盘价(09:25集合竞价撮合价, 无撮合则连续竞价首笔成交价); 当日无开盘则顺延至之后首个有开盘日(最多再等" #n "日, 仍无则取最近收盘价); 越过区间末则持有到最后一日收盘"
#define LABEL_TEX_MIN(n) R"(, \quad P_{exit}=\mathrm{VWAP}^{1\mathrm{min}}_{t_{entry}+)" #n R"(\,\mathrm{min}})"
#define LABEL_TEX_CLOSE(n) R"(, \quad P_{exit}=P^{close}_{D})"
#define LABEL_TEX_OPEN(n) R"(, \quad P_{exit}=P^{open}_{D+)" #n "}"
// 描述统一四段: 口径 | entry | exit | 净收益换算. entry 三档共用, exit 按 KIND, 净收益按 side (做多扣买入冲击, 做空扣卖出冲击)
#define LABEL_ROW(X, CAT1, side, cost, en, cn, formula, name, kind, n)                                \
  X(lb_##side##_##name, CAT1, ret, en " " LABEL_EN_##kind(n) " Return", cn LABEL_CN_##kind(n) "收益", \
    cn LABEL_CN_##kind(n) "价格收益(比例; 不含冲击与税佣). entry: 锚点(分钟末)+3s的可成交盘口中间价, 空窗则顺延至首个可成交簿, 锚点后再无可成交簿则按收盘价建仓. exit: " LABEL_NOTE_##kind(n) ". 净收益=本列−lb_cost_" cost "_<a>w−Config::sell_impact(消费端可选扣减)", formula LABEL_TEX_##kind(n), LABEL)
#define LABEL_ROW_LONG(name, kind, n, X, CAT1) LABEL_ROW(X, CAT1, long, "buy", "Long", "做多", R"(\frac{P_{exit}-P^{mid}_{entry}}{P^{mid}_{entry}})", name, kind, n)
#define LABEL_ROW_SHORT(name, kind, n, X, CAT1) LABEL_ROW(X, CAT1, short, "sell", "Short", "做空", R"(\frac{P^{mid}_{entry}-P_{exit}}{P^{mid}_{entry}})", name, kind, n)
#define LABEL_GROUP(name, kind, n, X, CAT1) LABEL_ROW_LONG(name, kind, n, X, CAT1) LABEL_ROW_SHORT(name, kind, n, X, CAT1)
// 建仓冲击成本 (比例, ≥0): 与标签同一 entry 盘口, 吃 a 万元的成交 VWAP 相对中间价的偏离; 全簿不足余量按涨跌停价成交; 该侧全空且无涨跌停价 = NaN
// 描述统一四段: 口径 | 吃单 | 边界 | 净收益换算 (买入 / 卖出逐词对应)
#define LABEL_COST_BUY(a, X, CAT1)                                                         \
  X(lb_cost_buy_##a##w, CAT1, ratio_pos, "Entry Buy Impact " #a "w", "买入冲击(" #a "万)", \
    "建仓买入冲击(比例≥0; 标签不含). 吃单: entry同一盘口吃ask " #a "万元, 成交VWAP相对中间价的溢价. 边界: 全簿不足余量按涨停价成交, 卖侧全空且无涨停价则NaN. 做多净收益=lb_long_*−本列−Config::sell_impact", R"(\frac{\mathrm{VWAP}^{A}_{entry}(A)}{P^{mid}_{entry}}-1, \quad A=)" #a R"(\mathrm{w})", LABEL)
#define LABEL_COST_SELL(a, X, CAT1)                                                          \
  X(lb_cost_sell_##a##w, CAT1, ratio_pos, "Entry Sell Impact " #a "w", "卖出冲击(" #a "万)", \
    "建仓卖出冲击(比例≥0; 标签不含). 吃单: entry同一盘口吃bid " #a "万元, 成交VWAP相对中间价的折价. 边界: 全簿不足余量按跌停价成交, 买侧全空且无跌停价则NaN. 做空净收益=lb_short_*−本列−Config::sell_impact", R"(1-\frac{\mathrm{VWAP}^{B}_{entry}(A)}{P^{mid}_{entry}}, \quad A=)" #a R"(\mathrm{w})", LABEL)

#define FIELDS_L1_LabelReturn(X, CAT1) LABEL_GROUPS(LABEL_GROUP, X, CAT1) LABEL_AMTS(LABEL_COST_BUY, X, CAT1) LABEL_AMTS(LABEL_COST_SELL, X, CAT1)
