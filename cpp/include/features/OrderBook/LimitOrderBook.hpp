#pragma once

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <vector>

#include "define/FastBitmap.hpp"
#include "features/CoreLob.hpp"
#include "features/OrderBook/LimitOrderBookDefine.hpp"
#include "orders/OrderFrame.hpp"

//========================================================================================
// MAIN CLASS
//========================================================================================

static_assert(L2::L2_MIN_TIME_INTERVAL_MS < 60000, "interval >= 60s, need minute carry");

class LimitOrderBook {

public:
  //======================================================================================
  // CONSTRUCTOR & CONFIGURATION
  //======================================================================================

  // 订单簿是可换绑的工作区: 簿状态 (价位数组/订单池/桶表/位图/TOB) 全部日内瞬态
  // (每天 unbind() 归零), 不含任何跨日状态 —— 跨日状态 (DAG 暖历史 + Fund 状态机 +
  // 分钟缓冲 + TickData) 全在 per-asset 的 CoreLob 里, 经 bind() 换绑.
  //   回测: 每 worker 一个工作区, 资产串行复用 (页面/cache/TLB 常驻, 稳态零扩容);
  //   实盘: 每资产一个工作区, 启动时 bind 一次.
  // DAG 与簿的唯一耦合是 tick_data_ 指针: TickData 归 core 所有 (DAG 节点引用
  // 它, 终身有效), bind() 把本簿的写出口指过去 —— 资产因此不与任何 worker 绑死,
  // 处置权可转移 (见 worker/compute/Pipeline.hpp ts_loop 的负载再平衡).
  explicit LimitOrderBook(size_t ORDER_SIZE)
      : order_table_(ORDER_SIZE) { // 委托表按最忙资产预留, reserve_orders 按日收窄
    auction_bids_.reserve(1024);
    auction_asks_.reserve(1024);
    touched_levels_.reserve(4096);
    init_sentinel_levels();
  }

  // 生命周期 (成对): core.begin_day → bind → run_frame → core.end_day → unbind
  //   core 收盘之后才 unbind: 写出口的盘口 (depth_buffer) 指向本簿价位, 收盘末分钟的结算还要读
  //   GUI 重放无特征侧: bind(sink) → begin_frame → process… → unbind
  // 换绑资产. 前置条件: 未绑定 (刚构造或已 unbind(), 簿因此是干净的).
  // 每单内部的时序编排 (update_depth → 特征 → update_lob) 是本类的不变量,
  // 特征侧只是一个可换绑的出口 —— 这是回测/实盘一致性的一部分.
  void bind(CoreLob *core, size_t asset_id, L2::ExchangeType exchange_type) {
    assert(core && "bind: null core");
    assert(!bound() && "bind: 上一个资产没 unbind");
    core_ = core;
    exchange_type_ = exchange_type;
    tick_data_ = &core->tick_data();
    assert(tick_data_->asset_id == static_cast<uint32_t>(asset_id) && "bind: core/asset 不匹配");
  }

  // 无特征侧绑定 (GUI OrderFlow 重放): 盘口写出口指向调用方的 TickData, 不跑 DAG.
  // 与回测绑定的唯一差异是 core_ == nullptr → 特征侧出口全部跳过 (if (core_), 恒真
  // 指针在回测热路径上是被完美预测的分支). 撮合/簿维护逐字节同一条路径.
  void bind(TickData *sink, size_t asset_id, L2::ExchangeType exchange_type) {
    assert(sink && "bind: null sink");
    assert(!bound() && "bind: 上一个资产没 unbind");
    core_ = nullptr;
    exchange_type_ = exchange_type;
    sink->asset_id = static_cast<uint32_t>(asset_id);
    tick_data_ = sink;
  }

  bool bound() const { return tick_data_ != &detached_; }

  // 当前绑定资产的 TickData (归 core 所有)
  TickData &tick_data() {
    assert(bound() && "tick_data: LOB not bound");
    return *tick_data_;
  }

  // Get TOB invalid count
  size_t get_tob_invalid_count() const {
    return tob_invalid_cnt_;
  }

  // Get TOB refresh count
  size_t get_tob_refresh_count() const {
    return tob_refresh_cnt_;
  }

  // 交叉簿率 = crossed / seen: 定位报单乱序 / TOB 失效的量级
  size_t get_depth_seen_count() const {
    return depth_seen_;
  }

  size_t get_depth_crossed_count() const {
    return depth_crossed_;
  }

  //======================================================================================
  // PUBLIC API: Order Processing
  //======================================================================================

  // Process single order (live trading interface - keeps function boundary for hooks/monitoring)
  HOT_NOINLINE bool process(const L2::Order &order) {
    return process_impl(order);
  }

  // 整帧喂入 (回测): begin_frame + process_impl 内联进循环, 返回无效条数.
  HOT_INLINE size_t run_frame(const L2::FrameView &view) {
    begin_frame(view);
    size_t invalid_count = 0;
    for (size_t i = 0; i < view.head.count; ++i) {
      if (!process_impl(view.orders[i])) [[unlikely]] {
        ++invalid_count;
      }
    }
    return invalid_count;
  }

  //======================================================================================
  // PUBLIC API: Utilities
  //======================================================================================

  // 设置当日价位索引基准 (分), 随帧而来 (见 begin_frame). 必须在 bind() 之后、喂第一条
  // 订单之前调用 —— 一天之内不可改变, 否则簿里已有的价位下标会指向别的价格.
  HOT_NOINLINE void set_price_base(uint32_t price_base) {
    price_base_ = price_base;
    LOB_feature_ref().price_base = price_base;
  }

  // 当日事件数 (解码后已知). bind() 之后、第一条订单之前调用: 委托表按 2× 事件数定长, 冷门资产的表
  // 小到进 L2, 换日只清这一段. 不调用 (实盘, 事件数未知) 则用整张预留表.
  HOT_NOINLINE void reserve_orders(size_t events) {
    order_table_.reserve(events);
  }

  // 一帧 (解码产物) 的前置: 价位基准 + 委托表定长. 帧里的逐笔随后经 process 逐条
  // 喂入 (重放); 回测走 run_frame 整帧. FrameDecoder::decode 的对端.
  void begin_frame(const L2::FrameView &view) {
    set_price_base(view.head.price_base);
    reserve_orders(view.head.count);
  }

  // 解绑: 簿归零 (只清当日触碰的价位 —— touched_levels_ 脏列表, 不扫 512KB 指针表;
  // 哨兵价位重建一次 ~60 格), 委托表/TOB/节流游标 + 写出口的 LOB_Feature, 写出口退回 detached_.
  // bind 的对端 —— 归还干净工作区, 下一个资产才能 bind.
  HOT_NOINLINE void unbind() {
    assert(bound() && "unbind: LOB not bound");
    for (const Price p : touched_levels_)
      price_levels_[p] = nullptr;
    touched_levels_.clear();
    order_table_.clear();
    loc_[0] = loc_[1] = nullptr;
    last_taker_id_ = 0;
    last_taker_cum_ = 0;
    visible_price_bitmap_.reset();
    tob_price_ = 0;
    tob_valid_ = false;
    tob_invalid_cnt_ = 0;
    tob_refresh_cnt_ = 0;
    depth_seen_ = 0;
    depth_crossed_ = 0;
    best_bid_ = 0;
    best_ask_ = 0;
    LOB_feature_ref() = {};
    LOB_feature_ref().depth_buffer.clear();
    last_depth_update_tick_ = 0;
    next_depth_update_tick_ = 0;
    depth_from_auction_ = false;
    was_in_matching_period_ = false;
    prev_tick_ = 0;
    curr_tick_ = 0;
    new_tick_ = false;
    in_call_auction_ = false;
    in_matching_period_ = false;
    in_continuous_trading_ = false;
    delta_qty_ = 0;
    target_id_ = 0;
    actual_price_ = 0;
    price_base_ = 0; // 由 set_price_base() 按当日文件头重新给定
    core_ = nullptr;
    tick_data_ = &detached_;

    init_sentinel_levels();
  }

  // 全簿真实价位枚举 (GUI OrderFlow 重放用): 按价升序回调 f(price, net_qty).
  // price 为绝对价 (元); 只给真实价位 (见 is_real_level). net_qty 为股数, SIGNED: 买压 > 0, 卖压 < 0.
  template <typename F>
  void for_each_visible_level(F &&f) const {
    for (size_t p = visible_price_bitmap_.find_next(L2::LOB_DEPTH); p < kHighSentinelBegin;
         p = visible_price_bitmap_.find_next(p)) {
      const Level *level = price_levels_[p];
      assert(level && "for_each_visible_level: visible price without level");
      f(LOB_feature_ref().price_of(level), level->net_quantity);
    }
  }

private:
  //======================================================================================
  // INITIALIZATION
  //======================================================================================

  // 两端哨兵价位: 垫满盘口 (构造期建一次; unbind 重建 —— 编码期停靠价位可能挂过单, 值要归零重设)
  void init_sentinel_levels() {
    // Low price end (bid side): price 1 to L2::LOB_DEPTH, net_quantity = +1
    for (Price p = 1; p <= (L2::LOB_DEPTH - 1 + 1); ++p) {
      Level *level = level_create(p);
      level->net_quantity = 1;
      visibility_mark_visible(p);
    }

    // High price end (ask side): price kHighSentinelBegin to (PRICE_RANGE_SIZE - 2), net_quantity = -1
    for (Price p = kHighSentinelBegin; p <= (PRICE_RANGE_SIZE - 1 - 1); ++p) {
      Level *level = level_create(p);
      level->net_quantity = -1;
      visibility_mark_visible(p);
    }
  }

  //======================================================================================
  // DATA STRUCTURES (按层次组织)
  //======================================================================================

  //------------------------------------------------------------------------------------
  // Layer 1: Price Level Storage (价格价位基础层)
  //------------------------------------------------------------------------------------
  // 扁平价位表: 价位下标即下标, 构造期一次开全 (1MB, Level 16B × 65536);
  //   深度走扫相邻价位同缓存行, 不再经 deque 跳块. price_levels_ = 存在性指针表
  //   (nullptr = 本日未触碰; 指进 levels_), touched_levels_ = 当日新建价位脏列表
  //   (unbind 只清它们, 哨兵另由 init_sentinel_levels 重建)
  std::vector<Level> levels_ = std::vector<Level>(PRICE_RANGE_SIZE, Level(0));
  std::array<Level *, PRICE_RANGE_SIZE> price_levels_{};
  std::vector<Price> touched_levels_;

  // 当日价位索引基准 (分). 数组按 (绝对价 - 基准) 寻址, 见 price_to_index().
  uint32_t price_base_ = 0;

  //------------------------------------------------------------------------------------
  // Layer 2: Order Tracking Infrastructure (订单追踪层)
  //------------------------------------------------------------------------------------
  OrderTable order_table_; // OrderId → Order (委托就地存槽), 见 LimitOrderBookDefine.hpp

  // 本笔两侧命中的委托 (下标 0=bid 1=ask). process_impl 入口查一次, 快照与 update_lob 共用 ——
  // 查的正是 update_lob 要动的那几个 id, 探针数与不做快照时相同. 中间只隔 compute_and_store
  // (不改簿), 表内槽位不搬家, 指针跨过去仍有效. 没查 / 查无的一侧 nullptr.
  Order *loc_[2] = {nullptr, nullptr};

  // 主动单累计成交量寄存器. 撮合对一笔进场单是原子的, 它打出的 N 笔成交在流里连续、中间不夹
  // 别的事件 —— 所以"这单截止上一笔成交了多少"只需记住上一笔主动 id 与其累计量, 不需要 id 表.
  // 乱序到达会让累计从头计, 与 OUT_OF_ORDER 占位单是同一类近似.
  OrderId last_taker_id_ = 0;
  uint32_t last_taker_cum_ = 0;

  //------------------------------------------------------------------------------------
  // Layer 3: Global Visibility Tracking (全局可见性层 - 逐笔更新)
  //------------------------------------------------------------------------------------
  FastBitmap<PRICE_RANGE_SIZE> visible_price_bitmap_; // Bitmap: mark all prices with net_quantity ≠ 0
                                                      // Usage: find_next/prev for adjacent price lookup
                                                      // Update: immediate O(1) on any level change

  //------------------------------------------------------------------------------------
  // Layer 4: Tick-by-Tick TOB (逐笔盘口层)
  //------------------------------------------------------------------------------------
  mutable Price tob_price_ = 0;        // Tick-by-tick (one-side) TOB price
  mutable bool tob_valid_ = false;     // Tick-by-tick TOB validity: false=invalid, true=valid
  mutable size_t tob_invalid_cnt_ = 0; // Tick-by-tick TOB invalid count
  mutable size_t tob_refresh_cnt_ = 0; // Tick-by-tick TOB invalid count
  mutable size_t depth_seen_ = 0;      // 连续竞价段满 2N 价位的盘口更新次数
  mutable size_t depth_crossed_ = 0;   // 其中买一 ≥ 卖一 被判交叉丢弃的次数
  mutable Price best_bid_ = 0;         // Tick-by-tick best bid (highest buy price with visible quantity)
  mutable Price best_ask_ = 0;         // Tick-by-tick best ask (lowest sell price with visible quantity)

  //------------------------------------------------------------------------------------
  // Layer 5: Feature Depth (特征深度层 - 时间驱动低频更新)
  //------------------------------------------------------------------------------------
  // 写出口: 指向当前绑定 core 的 TickData (bind() 换绑). 未绑定时指向 detached_
  // —— 构造期 init_sentinel_levels 的可见性回调要读 depth_buffer (空, 早退),
  // 给它一个合法对象而不是在热路径上加空指针分支.
  TickData detached_{};
  TickData *tick_data_ = &detached_;

  // 便捷访问当前 LOB_Feature (原引用成员, 换绑后必须走指针)
  HOT_INLINE LOB_Feature &LOB_feature_ref() const { return tick_data_->lob; }

  // Time-driven depth update control
  mutable uint32_t last_depth_update_tick_ = 0; // Last tick when depth was updated
  mutable uint32_t next_depth_update_tick_ = 0; // Next allowed tick for depth update

  // 集合竞价 depth 分支状态: buffer 出自竞价重建 (出竞价首个连续更新强制 rebuild) + 分侧走扫 scratch
  bool depth_from_auction_ = false;
  std::vector<Level *> auction_bids_, auction_asks_; // 价升序, 每次竞价更新重填 (常驻容量)

  // Track matching period transition
  bool was_in_matching_period_ = false;

  //------------------------------------------------------------------------------------
  // Auxiliary State (辅助状态)
  //------------------------------------------------------------------------------------

  // Market timestamp tracking (hour|minute|second|millisecond)
  uint32_t prev_tick_ = 0; // Previous tick timestamp
  uint32_t curr_tick_ = 0; // Current tick timestamp
  bool new_tick_ = false;  // Flag: entered new tick

  // Trading session state cache (computed once per order)
  bool in_call_auction_ = false;
  bool in_matching_period_ = false;
  bool in_continuous_trading_ = false;

  // Exchange type - determines matching mechanism (SSE vs SZSE)
  L2::ExchangeType exchange_type_ = L2::ExchangeType::SSE;

  // Hot path temporary variable cache (reduce allocation overhead)
  mutable Quantity delta_qty_; // Signed quantity change (+add/-deduct)
  mutable OrderId target_id_;  // Target order ID for current operation
  mutable Price actual_price_; // Actual effective price for current operation

  // Order parsing cache (parsed once per order in process())
  mutable bool is_maker_;
  mutable bool is_taker_;
  mutable bool is_cancel_;
  mutable bool is_bid_;

  // Feature update component (per-asset 跨日状态, bind() 换绑; 每单一次指针间接,
  // 相对 DAG 本身的开销可忽略)
  CoreLob *core_ = nullptr;

  //======================================================================================
  // LEVEL MANAGEMENT (价格价位基础操作)
  //======================================================================================

  // 绝对价 (分) → 价位数组下标.
  //
  // 下标 0 是留给市价单/无价格单的特殊价位, 所以 price==0 原样透传, 不减基准;
  // price_base_ 为 0 时折算退化成恒等.
  //
  // 上下夹紧只是越界护栏: 落盘时 park_price 已把价格折进窗口, 正常情况下这两个
  // 分支走不到. 留着是因为一旦走到就是价位数组越界.
  HOT_INLINE Price price_to_index(uint32_t price) const {
    if (price == 0) [[unlikely]]
      return 0;
    if (price <= price_base_) [[unlikely]]
      return 1;
    const uint32_t index = price - price_base_;
    return static_cast<Price>(index < PRICE_RANGE_SIZE ? index : PRICE_RANGE_SIZE - 1);
  }

  HOT_INLINE Level *level_get_or_create(Price price) {
    Level *level = price_levels_[price];
    if (level == nullptr) [[unlikely]]
      level = level_create(price);
    return level;
  }

  // Create: Create new level (assumes level doesn't exist). 扁平表就位 + 记脏 (unbind 据此复位)
  HOT_INLINE Level *level_create(Price price) {
    Level *level = &levels_[price];
    *level = Level(price);
    price_levels_[price] = level;
    touched_levels_.push_back(price);
    return level;
  }

  // Remove: Delete empty level from book
  HOT_INLINE void level_remove(Level *level, bool update_visibility = true) {
    Price price = level->price;
    // Don't erase from price_levels_ - keep Level* pointer alive for depth_buffer stability
    // Just mark as invisible, level will be reused when orders come back to this price
    if (update_visibility) {
      visibility_mark_invisible_safe(price);
    }
  }

  //======================================================================================
  // VISIBILITY TRACKING (可见性位图维护)
  //======================================================================================

  // Check if price has visible quantity
  HOT_INLINE bool visibility_is_visible(Price price) const {
    return visible_price_bitmap_.test(price);
  }

  // Mark price as visible (O(1))
  HOT_INLINE void visibility_mark_visible(Price price) {
    visible_price_bitmap_.set(price);
    // Order-driven depth update: level became visible
    Level *level = price_levels_[price];
    assert(level && "set level not found");
    if (level) {
      depth_on_level_add_remove(level, true);
    }
  }

  // Mark price as invisible (O(1))
  HOT_INLINE void visibility_mark_invisible(Price price) {
    visible_price_bitmap_.clear(price);
    // Order-driven depth update: level became invisible
    Level *level = price_levels_[price];
    assert(level && "clear level not found");
    if (level) {
      depth_on_level_add_remove(level, false);
    }
  }

  // Mark with duplicate check
  HOT_INLINE void visibility_mark_visible_safe(Price price) {
    if (!visibility_is_visible(price)) {
      visibility_mark_visible(price);
    }
  }

  HOT_INLINE void visibility_mark_invisible_safe(Price price) {
    if (visibility_is_visible(price)) {
      visibility_mark_invisible(price);
    }
  }

  // Update visibility based on level state
  HOT_INLINE void visibility_update_from_level(Level *level) {
    if (level->has_visible_quantity()) {
      visibility_mark_visible_safe(level->price);
    } else {
      visibility_mark_invisible_safe(level->price);
    }
  }

  // Find next visible price (bitmap scan)
  HOT_INLINE Price next_ask_above(Price from_price) const {
    size_t next = visible_price_bitmap_.find_next(from_price);
    return (next < PRICE_RANGE_SIZE) ? static_cast<Price>(next) : 0;
  }

  HOT_INLINE Price next_bid_below(Price from_price) const {
    size_t prev = visible_price_bitmap_.find_prev(from_price);
    return (prev < PRICE_RANGE_SIZE) ? static_cast<Price>(prev) : 0;
  }

  //======================================================================================
  // TRADING SESSION STATE (交易时段状态管理)
  //======================================================================================

  // Update session state flags based on current time (called when tick changes)
  HOT_NOINLINE void update_trading_session_state() {
    const uint16_t hhmm = ((curr_tick_ >> 16) & 0xFFFF); // hour * 256 + minute
    assert(((hhmm >> 8) < 24) && "hour out of range");
    assert(((hhmm & 0xFF) < 60) && "minute out of range");

    // Lookup table for market state (indexed by hhmm)
    // Size: 24 * 256 = 6144 bytes (covers all possible hour:minute combinations)
    static const auto &state_table = []() {
      static std::array<L2::MarketState, 24 * 256> table;
      table.fill(L2::MarketState::CLOSED);

      constexpr uint16_t T_0915 = (L2::MORNING_CALL_AUCTION_START_HOUR << 8) | L2::MORNING_CALL_AUCTION_START_MINUTE;
      constexpr uint16_t T_0925 = (L2::MORNING_MATCHING_START_HOUR << 8) | L2::MORNING_MATCHING_START_MINUTE;
      constexpr uint16_t T_0930 = (L2::CONTINUOUS_TRADING_MORNING_START_HOUR << 8) | L2::CONTINUOUS_TRADING_MORNING_START_MINUTE;
      constexpr uint16_t T_1130 = (L2::CONTINUOUS_TRADING_MORNING_END_HOUR << 8) | L2::CONTINUOUS_TRADING_MORNING_END_MINUTE;
      constexpr uint16_t T_1300 = (L2::CONTINUOUS_TRADING_AFTERNOON_START_HOUR << 8) | L2::CONTINUOUS_TRADING_AFTERNOON_START_MINUTE;
      constexpr uint16_t T_1457 = (L2::CLOSING_CALL_AUCTION_START_HOUR << 8) | L2::CLOSING_CALL_AUCTION_START_MINUTE;
      constexpr uint16_t T_1500 = (L2::CLOSING_CALL_AUCTION_END_HOUR << 8) | L2::CLOSING_CALL_AUCTION_END_MINUTE;

      for (uint16_t h = 0; h < 24; ++h) {
        for (uint16_t m = 0; m < 60; ++m) {
          uint16_t time = (h << 8) | m;
          if (time >= T_0915 && time < T_0925) {
            table[time] = L2::MarketState::OPENING_CALL_AUCTION;
          } else if (time >= T_0925 && time < T_0930) {
            table[time] = L2::MarketState::OPENING_MATCHING_PERIOD;
          } else if (time >= T_0930 && time < T_1130) {
            table[time] = L2::MarketState::CONTINUOUS_TRADING_MORNING;
          } else if (time >= T_1300 && time < T_1457) {
            table[time] = L2::MarketState::CONTINUOUS_TRADING_AFTERNOON;
          } else if (time >= T_1457 && time < T_1500) {
            table[time] = L2::MarketState::CLOSING_CALL_AUCTION;
          } else if (time == T_1500) {
            table[time] = L2::MarketState::CLOSING_MATCHING_PERIOD;
          }
        }
      }
      return table;
    }();

    // Single lookup instead of multiple branches
    assert(hhmm < state_table.size() && "market state table index out of bounds");
    LOB_feature_ref().market_state = state_table[hhmm];

    // Precompute flags using lookup table for faster access
    static constexpr bool is_matching[7] = {
        false, // CLOSED
        false, // OPENING_CALL_AUCTION
        true,  // OPENING_MATCHING_PERIOD
        false, // CONTINUOUS_TRADING_MORNING
        false, // CONTINUOUS_TRADING_AFTERNOON
        false, // CLOSING_CALL_AUCTION
        true,  // CLOSING_MATCHING_PERIOD
    };

    in_matching_period_ = is_matching[LOB_feature_ref().market_state];
    in_call_auction_ = (LOB_feature_ref().market_state == L2::MarketState::OPENING_CALL_AUCTION ||
                        LOB_feature_ref().market_state == L2::MarketState::CLOSING_CALL_AUCTION ||
                        in_matching_period_);
    in_continuous_trading_ = !in_call_auction_;
  }

  // Clear CALL_AUCTION flags at 9:30:00 (orders stay at current levels)
  HOT_NOINLINE void flush_call_auction_flags() {
    // 一天一次, O(当日表长)
    order_table_.for_each([&](Order &order) {
      if (order.flags == OrderFlags::CALL_AUCTION)
        order.flags = OrderFlags::NORMAL;
    });
    // TOB will be updated by first continuous trading order
  }

  //======================================================================================
  // ORDER OPERATIONS (订单生命周期管理)
  //======================================================================================

  // Helper: Extract operation parameters (delta_qty and target_id) atomically
  // Sets: delta_qty_ (signed quantity change) and target_id_ (target order ID)
  // NOTE: Uses cached is_maker_/is_taker_/is_cancel_ and is_bid_ from process()
  HOT_INLINE void order_extract_params(const L2::Order &order) {
    if (is_maker_) {
      delta_qty_ = is_bid_ ? +order.volume : -order.volume;
      target_id_ = is_bid_ ? order.bid_order_id : order.ask_order_id;
    } else if (is_cancel_) {
      delta_qty_ = is_bid_ ? -order.volume : +order.volume;
      target_id_ = is_bid_ ? order.bid_order_id : order.ask_order_id;
    } else if (is_taker_) {
      // For TAKER, determine maker side based on which ID is smaller (earlier order)
      if (order.bid_order_id != 0 && order.ask_order_id != 0) {
        const bool target_is_bid = (order.bid_order_id < order.ask_order_id);
        delta_qty_ = target_is_bid ? -order.volume : +order.volume;
        target_id_ = target_is_bid ? order.bid_order_id : order.ask_order_id;
      } else {
        const bool target_is_bid = (order.bid_order_id != 0);
        delta_qty_ = target_is_bid ? -order.volume : +order.volume;
        target_id_ = target_is_bid ? order.bid_order_id : order.ask_order_id;
      }
    } else {
      delta_qty_ = 0;
      target_id_ = 0;
    }
  }

  // Core: Upsert order (update existing or insert new)
  // This is the heart of the 订单簿 reconstruction engine
  //
  // Parameters:
  //   order_id        - Unique order identifier
  //   price           - Price level for this order
  //   quantity_delta  - Signed quantity change (+add, -deduct)
  //   order           - 表内槽: 已建委托 (level != nullptr) → 更新; 刚预留的槽 (level == nullptr) → 建单;
  //                     nullptr → 本函数自己 find_or_insert 一个 (冷路径: 占位单)
  //   flags           - Order state flags (NORMAL, OUT_OF_ORDER, etc.)
  //   level_hint      - Optional pre-fetched level pointer (optimization)
  //
  // Returns: true if order was fully consumed (removed), false otherwise
  HOT_NOINLINE bool order_upsert(
      OrderId order_id,
      Price price,
      Quantity quantity_delta,
      Order *order,
      OrderFlags flags = OrderFlags::NORMAL,
      Level *level_hint = nullptr) {

    if (order != nullptr && order->level != nullptr) [[likely]] {
      // ORDER EXISTS - Update existing order (HOT PATH)
      Level *level = order->level;

      // Apply quantity delta
      const Quantity old_qty = order->qty;
      const Quantity new_qty = old_qty + quantity_delta;

      if (new_qty == 0) [[unlikely]] {
        // FULLY CONSUMED - Remove order completely (COLD PATH)

        // Update feature all_volume (incremental) before removal
        const uint32_t abs_qty = std::abs(old_qty);
        LOB_feature_ref().all_bid_volume -= (old_qty > 0) ? abs_qty : 0;
        LOB_feature_ref().all_ask_volume -= (old_qty < 0) ? abs_qty : 0;

        // Record visibility state BEFORE removal
        const bool was_visible = level->has_visible_quantity();

        // 离开价位 + 就地墓碑, 无需再探针
        level->detach(old_qty);
        order_table_.erase(order);

        // Cleanup: Remove empty level or update visibility
        if (level->empty()) [[unlikely]] {
          level_remove(level, was_visible);
        } else {
          // Visibility changed: was_visible (before) → has_visible_quantity() (after)
          if (was_visible != level->has_visible_quantity()) [[unlikely]] {
            visibility_update_from_level(level);
          }
        }

        return true; // Fully consumed
      } else {
        // PARTIALLY CONSUMED - Update order quantity (HOT PATH)
        const bool was_visible = level->has_visible_quantity();
        level->adjust(quantity_delta);
        order->qty = new_qty;

        // Update feature all_volume: 按订单所属侧 (qty 符号) 换算贡献, 旧值出账新值入账.
        // 抵扣 (delta 与 qty 反号) 是减量, 且 qty 可能翻号 (过度抵扣), 不能按 delta 符号直接加到某一侧.
        auto side_bid = [](Quantity q) -> uint32_t { return q > 0 ? static_cast<uint32_t>(q) : 0u; };
        auto side_ask = [](Quantity q) -> uint32_t { return q < 0 ? static_cast<uint32_t>(-q) : 0u; };
        LOB_feature_ref().all_bid_volume += side_bid(new_qty) - side_bid(old_qty);
        LOB_feature_ref().all_ask_volume += side_ask(new_qty) - side_ask(old_qty);

        // Update flags if needed (rare)
        if ((flags != OrderFlags::NORMAL || order->flags != OrderFlags::NORMAL)) [[unlikely]]
          order->flags = flags;

        // Update visibility only if it changed (rare)
        if (was_visible != level->has_visible_quantity()) [[unlikely]] {
          visibility_update_from_level(level);
        }
        return false; // Partially consumed
      }

    } else {
      // ORDER DOESN'T EXIST - Create new order (LESS FREQUENT)
      if (order == nullptr) { // 占位单 (冷路径): 调用方只做过 find, 这里再占槽
        auto [slot, inserted] = order_table_.find_or_insert(order_id);
        assert(inserted && "order_upsert: 建单时 id 已在表中");
        order = slot;
      }
      assert(order->id == order_id && order->level == nullptr && "order_upsert: 槽不是刚预留的");

      // Get level: use hint if provided (optimization), otherwise get or create atomically
      Level *level = level_hint ? level_hint : level_get_or_create(price);

      // 建单时刻始终填: 挂单→成交 / 挂单→撤单 用时只有这一个来源.
      order->qty = quantity_delta;
      order->timestamp = curr_tick_;
      order->orig_qty = static_cast<uint32_t>(std::abs(quantity_delta));
      order->flags = flags;
      order->level = level;
      assert(order->orig_qty == static_cast<uint32_t>(std::abs(quantity_delta)) && "orig_qty:28 容不下建单量");
      level->attach(quantity_delta);

      // Update feature all_volume (incremental) for new order
      const uint32_t abs_delta = std::abs(quantity_delta);
      LOB_feature_ref().all_bid_volume += (quantity_delta > 0) ? abs_delta : 0;
      LOB_feature_ref().all_ask_volume += (quantity_delta < 0) ? abs_delta : 0;

      // Update visibility if level just became visible
      if (quantity_delta != 0 && !visibility_is_visible(level->price)) [[likely]] {
        visibility_mark_visible(level->price);
      }

      return false; // New order created
    }
  }

  // Move: Relocate order from one price level to another (委托本体不动, 只改归属与两个价位的聚合量)
  HOT_NOINLINE void order_move_to_price(
      Order *order,
      Price new_price) {
    Level *old_level = order->level;
    Price old_price = old_level->price;

    if (old_price == new_price)
      return; // Already at correct level

    Level *new_level = level_get_or_create(new_price);
    old_level->detach(order->qty);
    new_level->attach(order->qty);
    order->level = new_level;

    visibility_update_from_level(old_level);
    visibility_update_from_level(new_level);

    if (old_level->empty()) {
      level_remove(old_level);
    }
  }

  //======================================================================================
  // TOB MANAGEMENT (盘口管理)
  //======================================================================================

  // Update TOB: scan from tob_price_ to find bid/ask boundary
  // Key: farther side is true TOB, derive the other side from it
  HOT_INLINE void update_tob() {
    int bid_dist = 0, ask_dist = 0;

    // Scan down for bid (qty > 0), count distance
    for (Price p = tob_price_; p != 0; p = next_bid_below(p)) {
      if (Level *lv = price_levels_[p]; lv && lv->net_quantity > 0) {
        best_bid_ = p;
        break;
      }
      ++bid_dist;
    }

    // Scan up for ask (qty < 0), count distance
    for (Price p = tob_price_; p != 0; p = next_ask_above(p)) {
      if (Level *lv = price_levels_[p]; lv && lv->net_quantity < 0) {
        best_ask_ = p;
        break;
      }
      ++ask_dist;
    }

    // Valid: at least one side at tob_price_ (distance = 0)
    if (bid_dist <= 1 || ask_dist <= 1) {
      // Farther side is true, derive closer side from it
      if (bid_dist > ask_dist) {
        best_ask_ = next_ask_above(best_bid_);
      } else {
        best_bid_ = next_bid_below(best_ask_);
      }
      tob_valid_ = true;
    } else {
      tob_valid_ = false;
      tob_invalid_cnt_++;
    }

    tob_refresh_cnt_++;
  }

  //======================================================================================
  // HIGH-LEVEL PROCESSING (高层处理逻辑)
  //======================================================================================

  // Core order processing implementation (shared by single/batch interfaces)
  HOT_INLINE bool process_impl(const L2::Order &order_abs) {
    // 帧里存的是绝对价 (分), 而价位数组只开了 kPriceIndexRange 个价位并按下标直接
    // 寻址. 在入口一次性折算, 函数体内此后所有价格比较与寻址都留在下标空间; 只有
    // 对外暴露的 LOB_feature_ref().price 用回绝对价.
    L2::Order order = order_abs;
    order.price = price_to_index(order_abs.price);

    // Skip dirty orders @09:26:00
    if (order.price == 0 && order.volume == 0) [[unlikely]] {
      return true;
    }

    curr_tick_ = order.tick;
    new_tick_ = curr_tick_ != prev_tick_;

    // Update feature timestamp (only on new tick)
    if (new_tick_) {
      LOB_feature_ref().hour = L2::tick_hour(curr_tick_);
      LOB_feature_ref().minute = L2::tick_minute(curr_tick_);
      LOB_feature_ref().second = L2::tick_second(curr_tick_);
      LOB_feature_ref().millisecond = L2::tick_millisecond(curr_tick_);
      update_trading_session_state();
    }

    // Parse order metadata (cache for reuse)
    is_maker_ = (order.order_type == L2::OrderType::MAKER);
    is_taker_ = (order.order_type == L2::OrderType::TAKER);
    is_cancel_ = (order.order_type == L2::OrderType::CANCEL);
    is_bid_ = (order.order_dir == L2::OrderDirection::BID);

    // Update feature order metadata (every order)
    LOB_feature_ref().order_type = order.order_type;
    LOB_feature_ref().order_dir = order.order_dir;
    LOB_feature_ref().price = order_abs.price * 0.01; // 对外一律给绝对价
    LOB_feature_ref().volume = order.volume;

    // 操作参数 + 簿内位置一次算好, 快照与 update_lob 共用 (二者之间簿不变).
    // 快照必须在 update_lob 之前取: 下游 compute_and_store 看的就是"更新前"的簿.
    order_extract_params(order);
    resolve_order_locations(order);
    publish_order_snapshot(order);

    // Detect transition from matching period to continuous trading
    if (was_in_matching_period_ && !in_matching_period_ && !in_call_auction_) [[unlikely]] {
      flush_call_auction_flags();
    }
    was_in_matching_period_ = in_matching_period_;

    // ==================== depth update triggered =======================

    // 计算特征(depth 更新后)
    update_depth();

    if (core_) [[likely]]
      core_->compute_and_store();

    // ========================= lob update ==============================
    bool result = update_lob(order);

    prev_tick_ = curr_tick_;

    return result;
  }

  // 双边成交 (两侧委托都要动): 深交所全程, 沪市只在集合竞价撮合段. 沪市连续竞价是单边 —— 主动方
  // 的委托记录在它的成交之后才到 (残量才入簿), 成交时它不在簿, 只动被动方.
  HOT_INLINE bool is_bilateral(const L2::Order &order) const {
    const bool need = (exchange_type_ == L2::ExchangeType::SZSE) ||
                      (exchange_type_ == L2::ExchangeType::SSE && in_matching_period_);
    return is_taker_ && need && order.bid_order_id != 0 && order.ask_order_id != 0;
  }

  // target_id_ (order_extract_params 所取: MAKER/CANCEL 本方, TAKER 取 id 较小者 = 先到的被动方) 落在哪一侧
  HOT_INLINE size_t target_side(const L2::Order &order) const {
    return target_id_ == order.bid_order_id ? 0 : 1;
  }

  // 只查 update_lob 真正要动的 id, 探针数与不做快照时逐笔相同:
  //   双边 TAKER: 两侧;  单边 TAKER / CANCEL: 仅 target;  MAKER: 不查 (自己的 find 与建单 try_emplace 同桶, 留在 update_lob)
  // 单边 TAKER 的主动方不查 —— 它按数据形态就不在簿 (见 is_bilateral), 查也是 miss 走满整条链.
  HOT_INLINE void resolve_order_locations(const L2::Order &order) {
    loc_[0] = loc_[1] = nullptr;
    if (is_maker_)
      return;
    if (is_bilateral(order)) {
      loc_[0] = order_table_.find(order.bid_order_id);
      loc_[1] = order_table_.find(order.ask_order_id);
    } else if (target_id_ != 0) {
      loc_[target_side(order)] = order_table_.find(target_id_);
    }
  }

  // 把两侧委托状态摊平到 LOB_Feature (字段语义见 LOB_Feature 的 ord_* 注释). 全部来自 loc_ 与寄存器, 不查表.
  HOT_INLINE void publish_order_snapshot(const L2::Order &order) {
    LOB_Feature &lf = LOB_feature_ref();
    for (size_t s = 0; s < 2; ++s) {
      lf.ord_role[s] = OrderRole::None;
      lf.ord_orig[s] = 0;
      lf.ord_rest[s] = 0;
      lf.ord_tick[s] = 0;
      lf.ord_flag[s] = OrderFlags::NORMAL;
      lf.ord_price[s] = 0.0f;
    }
    if (is_maker_) // 新委托此刻还没进簿, 两侧恒 None
      return;

    for (size_t s = 0; s < 2; ++s) {
      if (const Order *o = loc_[s]) {
        assert(o->level != nullptr && "在簿委托必有价位");
        const Quantity q = o->qty;
        lf.ord_role[s] = OrderRole::Resting;
        lf.ord_orig[s] = o->orig_qty;
        lf.ord_rest[s] = static_cast<uint32_t>(q < 0 ? -q : q);
        lf.ord_tick[s] = o->timestamp;
        lf.ord_flag[s] = o->flags;
        lf.ord_price[s] = lf.price_of(o->level);
      }
    }

    // 主动方 (target 的对侧) 不在簿 → 累计量出自寄存器. 在簿 (深交所: 委托记录先到) 则上面已按 Resting 给出,
    // 消费方用 ord_orig − ord_rest 得已成交量. id 为 0 (单边数据不给主动 id) 留 None.
    if (is_taker_) {
      const size_t a = 1 - target_side(order);
      const OrderId aid = a == 0 ? order.bid_order_id : order.ask_order_id;
      if (loc_[a] == nullptr && aid != 0) {
        lf.ord_role[a] = OrderRole::Aggressor;
        lf.ord_rest[a] = (aid == last_taker_id_) ? last_taker_cum_ : 0;
      }
    }
  }

  // 主动单成交量入寄存器 (同一主动 id 连续成交累加, 换 id 重计)
  HOT_INLINE void taker_register_add(OrderId id, uint32_t volume) {
    last_taker_cum_ = (id == last_taker_id_) ? last_taker_cum_ + volume : volume;
    last_taker_id_ = id;
  }

  // Helper: Process one taker side (for bilateral or unilateral)
  // loc: 该 id 的委托 (resolve_order_locations 查好的; nullptr = 不在簿)
  // Returns: true if order was fully consumed
  HOT_INLINE bool process_taker_side(
      const L2::Order &order,
      OrderId order_id,
      Quantity delta,
      Order *loc) {

    const bool found = (loc != nullptr);

    // FAST PATH: found && correct price && not in call auction
    if (found && !in_call_auction_) [[likely]] {
      Level *level = loc->level;
      if (level->price == order.price) [[likely]] {
        actual_price_ = order.price;
        return order_upsert(order_id, order.price, delta, loc, OrderFlags::NORMAL, level);
      }
    }

    // DEFERRED PATH: Handle corner cases
    target_id_ = order_id;
    delta_qty_ = delta;
    return update_lob_deferred(order, loc, found, in_call_auction_, in_matching_period_);
  }

  // Main 订单簿 update logic (dispatches to MAKER/TAKER/CANCEL)
  HOT_NOINLINE bool update_lob(const L2::Order &order) {
    //====================================================================================
    // MAKER: Always unilateral, never enters loop
    //====================================================================================
    if (is_maker_) {
      if (delta_qty_ == 0 || target_id_ == 0) [[unlikely]]
        return false;

      // 一次探针: 查无则同时占槽 (99% 的 MAKER 走这里)
      auto [slot, inserted] = order_table_.find_or_insert(target_id_);

      // FAST PATH: Normal maker with price
      if (inserted && !in_call_auction_ && order.price != 0) [[likely]] {
        actual_price_ = order.price;
        order_upsert(target_id_, actual_price_, delta_qty_, slot, OrderFlags::NORMAL);
        return true;
      }

      // DEFERRED PATH: Special cases (out-of-order, call auction, price=0)
      return update_lob_deferred(order, slot, !inserted, in_call_auction_, in_matching_period_);
    }

    //====================================================================================
    // TAKER/CANCEL: May be bilateral or unilateral
    //====================================================================================
    if (is_bilateral(order)) {
      //==================================================================================
      // BILATERAL TAKER: 被动方扣减; 主动方在簿则同扣, 不在簿则只进寄存器
      //==================================================================================
      // 主动方 = 后到达 = id 较大的一侧 (与 order_extract_params 取 id 较小者当 target 同一判据).
      // 深交所委托记录先到, 主动方通常已在簿 (含 price=0 挂 Level[0] 的市价单), 照常扣减.
      // 查无 (乱序) 不建占位单: 占位单的 -volume 会原地抵掉被动侧的 +volume, 成交就不消耗盘口
      // 深度了, qty_* / obi_* 系统性偏大; 且与沪市单边路径 (主动方本就不入簿) 语义不一致, 截面不可比.
      // 两侧同 id 时 loc_[0]==loc_[1], 先扣主动方再用同一槽扣被动方会踩到已打墓碑的槽
      assert(order.bid_order_id != order.ask_order_id && "bilateral: 买卖同一委托 id");
      const size_t passive_s = target_side(order), active_s = 1 - passive_s;
      const bool is_active_bid = (active_s == 0);
      const OrderId active_id = is_active_bid ? order.bid_order_id : order.ask_order_id;
      const OrderId passive_id = is_active_bid ? order.ask_order_id : order.bid_order_id;
      // 被动方是卖单 (qty < 0) 则加正量趋零, 是买单 (qty > 0) 则加负量趋零
      const Quantity passive_delta = is_active_bid ? +static_cast<Quantity>(order.volume)
                                                   : -static_cast<Quantity>(order.volume);

      if (loc_[active_s] != nullptr) {
        process_taker_side(order, active_id, -passive_delta, loc_[active_s]);
      } else {
        taker_register_add(active_id, order.volume);
      }
      [[maybe_unused]] bool consumed_passive = process_taker_side(order, passive_id, passive_delta, loc_[passive_s]);

      actual_price_ = order.price; // for safety
      tob_price_ = actual_price_;
      return true;

    } else {
      //==================================================================================
      // UNILATERAL TAKER/CANCEL: Single target
      //==================================================================================
      if (delta_qty_ == 0 || target_id_ == 0) [[unlikely]]
        return false;

      // 单边成交的主动方不在簿 (见 is_bilateral), 只进寄存器; 主动 id 为 0 (数据不给) 时寄存器不写
      if (is_taker_) {
        const OrderId active_id = target_side(order) == 0 ? order.ask_order_id : order.bid_order_id;
        if (active_id != 0)
          taker_register_add(active_id, order.volume);
      }

      [[maybe_unused]] bool consumed = process_taker_side(order, target_id_, delta_qty_, loc_[target_side(order)]);

      if (is_taker_)
        tob_price_ = actual_price_;
      return true;
    }
  };

  // Slow path: handle corner cases (out-of-order, call auction, special prices)
  // NOTE: Uses cached is_maker_/is_taker_/is_cancel_ and is_bid_ from process()
  // loc: found 时是已建委托; MAKER 且 !found 时是 find_or_insert 刚预留的槽; TAKER/CANCEL 且 !found 时为 nullptr
  [[gnu::cold]] [[gnu::noinline]] bool update_lob_deferred(
      const L2::Order &order,
      Order *loc,
      bool found,
      bool in_call_auction,
      bool in_matching_period) {

    //====================================================================================
    // MAKER ORDER
    //====================================================================================
    if (is_maker_) {
      // Determine placement and flags
      Price placement_price;
      OrderFlags flags;

      if (order.price == 0) {
        // SPECIAL_MAKER: price=0 (market order, best-for-us, etc.)
        placement_price = 0; // Level[0]
        flags = OrderFlags::SPECIAL_MAKER;
      } else if (in_call_auction || in_matching_period) {
        // CALL_AUCTION: 9:15-9:30, price may not be final trade price
        placement_price = order.price;
        flags = OrderFlags::CALL_AUCTION;
      } else {
        // NORMAL: continuous auction with known price
        placement_price = order.price;
        flags = OrderFlags::NORMAL;
      }

      if (found) {
        // OUT_OF_ORDER: order exists (created by earlier TAKER/CANCEL)
        Price existing_price = loc->level->price;
        if (existing_price != placement_price) {
          order_move_to_price(loc, placement_price);
        }
        // 迟到的 MAKER 才带真实申报量与挂单时刻: 占位单建单时记的是首笔成交/撤单量, 这里改正.
        // (改正后 order_upsert 加上 delta_qty_ → qty = 申报量 − 已成交量 = 真实余量)
        loc->orig_qty = static_cast<uint32_t>(std::abs(delta_qty_));
        loc->timestamp = curr_tick_;
        order_upsert(target_id_, placement_price, delta_qty_, loc, flags);
      } else {
        // Create new order
        order_upsert(target_id_, placement_price, delta_qty_, loc, flags);
      }

      return true;
    }

    //====================================================================================
    // TAKER ORDER
    //====================================================================================
    if (is_taker_) {
      // Handle target order (counterparty)
      bool fully_consumed = false;
      if (found) {
        Price target_price = loc->level->price;

        if (target_price != order.price) {
          // 连续竞价里挂价 ≠ 成交价是异常撮合; 竞价段挂价本就不是最终价, 迁移不打标.
          // 占位单 (OUT_OF_ORDER / ZERO_PRICE) 的标记不覆盖: 它是 "orig_qty 不是申报量" 的唯一凭据 (LOB_Feature::ord_flag 消费方靠它剔除)
          order_move_to_price(loc, order.price);
          if (!in_call_auction && !in_matching_period && loc->flags != OrderFlags::OUT_OF_ORDER && loc->flags != OrderFlags::ZERO_PRICE)
            loc->flags = OrderFlags::ANOMALY_MATCH;
        }

        actual_price_ = order.price;
        fully_consumed = order_upsert(target_id_, actual_price_, delta_qty_, loc);
      } else {
        // OUT_OF_ORDER: create placeholder
        actual_price_ = order.price;
        order_upsert(target_id_, actual_price_, delta_qty_, nullptr, OrderFlags::OUT_OF_ORDER);
        fully_consumed = false;
      }

      return fully_consumed;
    }

    //====================================================================================
    // CANCEL ORDER
    //====================================================================================
    if (is_cancel_) {
      if (found) {
        Price self_price = loc->level->price;

        // Migrate from Level[0] if CANCEL has price
        if (self_price == 0 && order.price != 0) {
          order_move_to_price(loc, order.price);
          self_price = order.price;
        }

        order_upsert(target_id_, self_price, delta_qty_, loc);
      } else {
        // OUT_OF_ORDER or ZERO_PRICE: create placeholder
        Price placement_price = (order.price == 0) ? 0 : order.price;
        OrderFlags flags = (order.price == 0) ? OrderFlags::ZERO_PRICE : OrderFlags::OUT_OF_ORDER;
        order_upsert(target_id_, placement_price, delta_qty_, nullptr, flags);
      }

      return true;
    }

    return false;
  };

  //======================================================================================
  // DEPTH BUFFER MANAGEMENT (N 价位深度缓冲区管理 - 订单+时间双驱动)
  //======================================================================================

  // Binary search price in entire depth_buffer (descending order)
  HOT_INLINE size_t depth_binary_search(Price price) const {
    size_t left = 0, right = LOB_feature_ref().depth_buffer.size();
    while (left < right) {
      size_t mid = left + (right - left) / 2;
      if (LOB_feature_ref().depth_buffer[mid]->price > price) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    return left;
  }

  // Order-driven: Add level to depth buffer
  // 竞价期跳过: 交叉簿下 buffer 非单调 (卖1 可 < 买1), 二分失效; 竞价分支每节流点整簿重建
  HOT_INLINE void depth_on_level_add_remove(Level *level, bool add) {
    if (level->price == 0 || in_call_auction_ || (LOB_feature_ref().depth_buffer.size() <= 2)) [[unlikely]]
      return;

    // Check if price is within current range
    Price high_price = LOB_feature_ref().depth_buffer.front()->price;
    Price low_price = LOB_feature_ref().depth_buffer.back()->price;
    if (level->price > high_price || low_price > level->price)
      return;

    // Binary search to find insert position
    size_t idx = depth_binary_search(level->price);

    if (add) {
      // Just insert, CBuffer will auto-pop front when full
      LOB_feature_ref().depth_buffer.insert(idx, level);
    } else {
      // Just remove, CBuffer will auto-pop back when empty
      LOB_feature_ref().depth_buffer.erase(idx);
    }
  }

  //======================================================================================
  // FEATURE UPDATES (特征更新 - 时间驱动)
  //======================================================================================

  // 节流推进: packed tick add, 跳过所有空 slot (竞价/连续两分支共用)
  HOT_INLINE void depth_advance_throttle() {
    last_depth_update_tick_ = curr_tick_;

    constexpr uint32_t INTERVAL_10MS = (L2::L2_MIN_TIME_INTERVAL_MS / 10) % 100;
    constexpr uint32_t INTERVAL_S = (L2::L2_MIN_TIME_INTERVAL_MS / 10) / 100;
    do {
      uint32_t ms = next_depth_update_tick_ & 0xFF;
      uint32_t s = (next_depth_update_tick_ >> 8) & 0xFF;
      if constexpr (INTERVAL_10MS > 0) {
        ms += INTERVAL_10MS;
        if (ms >= 100) {
          ms -= 100;
          s++;
        }
      }
      if constexpr (INTERVAL_S > 0) {
        s += INTERVAL_S;
        if (s >= 60) {
          s -= 60;
          next_depth_update_tick_ += (1 << 16);
        }
      }
      next_depth_update_tick_ = (next_depth_update_tick_ & 0xFFFF0000) | (s << 8) | ms;
    } while (next_depth_update_tick_ <= curr_tick_);
  }

  // ==== 集合竞价 depth 更新 (9:15-9:30 / 14:57-15:00) ====
  // 交叉簿 (bid1 可 ≥ ask1) 下 TOB 扫描与增量维护的单调性不变量全部失效: 每个节流点
  // 整簿重建 —— 一次升序位图走扫分侧收集 (交叉区买卖价位混排, 按 net_quantity 符号过滤),
  // 随手完成预撮合 (最大成交量价位, 平局取失衡最小); 真实价位不足 N 由两端哨兵垫满
  // (与连续路径口径一致). O(可见价位数), 每秒至多一次, 成本可忽略.
  HOT_NOINLINE void update_depth_auction() {
    LOB_Feature &lf = LOB_feature_ref();

    auction_bids_.clear();
    auction_asks_.clear();
    for (size_t p = visible_price_bitmap_.find_next(L2::LOB_DEPTH); p < kHighSentinelBegin;
         p = visible_price_bitmap_.find_next(p)) {
      Level *lv = price_levels_[p];
      assert(lv && "auction scan: visible price without level");
      if (lv->net_quantity > 0)
        auction_bids_.push_back(lv);
      else if (lv->net_quantity < 0)
        auction_asks_.push_back(lv);
    }

    lf.auction_ref_price = 0.0f;
    lf.auction_matched_qty = 0;
    lf.auction_imbalance = 0;
    if (auction_bids_.empty() || auction_asks_.empty()) {
      lf.depth_updated = false; // 单边簿: 无盘口可出 (对仗连续路径 TOB 无效)
      return;
    }

    best_bid_ = auction_bids_.back()->price; // 交叉簿下可 ≥ best_ask_
    best_ask_ = auction_asks_.front()->price;

    // ---- 重建 depth_buffer: 两侧各取近端 N 个价位, 不足由哨兵垫满 ----
    auto &buf = lf.depth_buffer;
    buf.clear();
    {
      // 卖侧升序 push_front → [0]=卖N ... [N-1]=卖1 (对仗连续路径的填法)
      const size_t n_ask = std::min(auction_asks_.size(), L2::LOB_DEPTH);
      for (size_t i = 0; i < n_ask; ++i)
        buf.push_front(auction_asks_[i]);
      for (size_t i = n_ask, p = kHighSentinelBegin; i < L2::LOB_DEPTH; ++i, ++p)
        buf.push_front(price_levels_[p]);

      // 买侧降序 push_back → [N]=买1 ... [2N-1]=买N
      const size_t n_bid = std::min(auction_bids_.size(), L2::LOB_DEPTH);
      for (size_t i = 0; i < n_bid; ++i)
        buf.push_back(auction_bids_[auction_bids_.size() - 1 - i]);
      for (size_t i = n_bid, p = L2::LOB_DEPTH; i < L2::LOB_DEPTH; ++i, --p)
        buf.push_back(price_levels_[p]);
    }
    depth_from_auction_ = true;

    // ---- 预撮合: 簿交叉时求最大成交量价位 ----
    // 候选价 p ∈ 交叉区价位并集 (升序双指针); A(p) = Σ 卖量 (价 ≤ p) 单调升,
    // B(p) = Σ 买量 (价 ≥ p) 单调降 → matched = min(A,B) 单峰; 平局取 |B-A| 最小.
    if (best_bid_ > best_ask_) {
      // 参与价位: 买价 ≥ 卖1, 卖价 ≤ 买1
      size_t bid_lo = 0;
      while (bid_lo < auction_bids_.size() && auction_bids_[bid_lo]->price < best_ask_)
        ++bid_lo;
      size_t ask_hi = auction_asks_.size();
      while (ask_hi > 0 && auction_asks_[ask_hi - 1]->price > best_bid_)
        --ask_hi;

      int64_t B = 0;
      for (size_t i = bid_lo; i < auction_bids_.size(); ++i)
        B += auction_bids_[i]->net_quantity;

      int64_t A = 0;
      int64_t best_v = -1, best_imb = 0;
      uint32_t best_p = 0;
      size_t ai = 0, bi = bid_lo;
      while (ai < ask_hi || bi < auction_bids_.size()) {
        const uint32_t pa = ai < ask_hi ? auction_asks_[ai]->price : UINT32_MAX;
        const uint32_t pb = bi < auction_bids_.size() ? auction_bids_[bi]->price : UINT32_MAX;
        const uint32_t p = std::min(pa, pb);
        // A(p): 卖 ≤ p 全部计入 (含 p 价位, 头为卖时在此消费)
        while (ai < ask_hi && auction_asks_[ai]->price <= p)
          A += -static_cast<int64_t>(auction_asks_[ai++]->net_quantity);

        const int64_t v = std::min(A, B);
        const int64_t imb = B - A;
        if (v > best_v || (v == best_v && std::abs(imb) < std::abs(best_imb))) {
          best_v = v;
          best_imb = imb;
          best_p = p;
        }

        // B(下个候选): 评估后消费 p 价位买头 (bid == p 计入本轮 B, 不计入更高价);
        // 每轮至少消费一个头 (卖 ≤ p 或买 == p) —— 推进保证, 不然 p 不增长死循环
        while (bi < auction_bids_.size() && auction_bids_[bi]->price == p)
          B -= auction_bids_[bi++]->net_quantity;
      }

      lf.auction_ref_price = lf.price_of(static_cast<Price>(best_p));
      lf.auction_matched_qty = static_cast<int32_t>(best_v);
      lf.auction_imbalance = static_cast<int32_t>(best_imb);
    }

    lf.depth_updated = buf.size() >= 2 * L2::LOB_DEPTH;
  }

  // Update depth if TOB is valid (called from process() when time interval reached)
  HOT_NOINLINE void update_depth() {
    LOB_Feature &lf = LOB_feature_ref();

    if (!(new_tick_ && curr_tick_ >= next_depth_update_tick_)) {
      lf.depth_updated = false;
      return;
    }

    // 竞价期 (含撮合期): 交叉簿专用路径 (整簿重建 + 预撮合)
    if (in_call_auction_) [[unlikely]] {
      update_depth_auction();
      depth_advance_throttle();
      return;
    }
    // 连续竞价: 预撮合口清零 (mid/micro 回常规公式; 仅竞价交叉时 > 0)
    lf.auction_ref_price = 0.0f;
    lf.auction_matched_qty = 0;
    lf.auction_imbalance = 0;

    update_tob();

    // Determine if rebuild needed
    size_t current_depth = lf.depth_buffer.size();

    // Calculate insertion counts
    size_t bid_idx = depth_binary_search(best_bid_);
    size_t ask_count = static_cast<int>(L2::LOB_DEPTH) - static_cast<int>(bid_idx);
    size_t bid_count = static_cast<int>(bid_idx + L2::LOB_DEPTH) - static_cast<int>(current_depth);

    // 出竞价首个更新强制重建: 竞价 buffer 可能交叉 (非单调), 增量口径不可续用
    bool need_rebuild = current_depth <= 2 || ask_count >= L2::LOB_DEPTH ||
                        bid_count >= L2::LOB_DEPTH || depth_from_auction_;
    depth_from_auction_ = false;

    ask_count = need_rebuild ? L2::LOB_DEPTH : ask_count;
    bid_count = need_rebuild ? L2::LOB_DEPTH : bid_count;

    if (need_rebuild)
      lf.depth_buffer.clear();

    // Fill ask side (upper half)
    Price price = need_rebuild ? best_ask_ : lf.depth_buffer.front()->price;
    for (size_t i = 0; i < ask_count && price > 0; ++i) {
      if (!need_rebuild) {
        price = next_ask_above(price);
        if (price == 0)
          break;
      }
      lf.depth_buffer.push_front(price_levels_[price]);
      if (need_rebuild)
        price = next_ask_above(price);
    }

    // Fill bid side (lower half)
    price = need_rebuild ? best_bid_ : lf.depth_buffer.back()->price;
    for (size_t i = 0; i < bid_count && price > 0; ++i) {
      if (!need_rebuild) {
        price = next_bid_below(price);
        if (price == 0)
          break;
      }
      lf.depth_buffer.push_back(price_levels_[price]);
      if (need_rebuild)
        price = next_bid_below(price);
    }

    depth_advance_throttle();

    if (lf.depth_buffer.size() < 2 * L2::LOB_DEPTH) {
      lf.depth_updated = false;
      return;
    }

    // 交叉簿: 买一 ≥ 卖一. 报单乱序到达 / TOB 失效导致的瞬时错位, 此刻的簿不是真盘口.
    // 判定放在这里而不是下游节点里 —— 不算一次盘口更新, onDepth 整个域都不跑, 于是
    // "每个 onDepth tick, Depth 环必推一格" 对下游 (LabelLob 的 offset 回溯) 恒成立.
    // 连续竞价段簿已单调, 两侧最优价位必是真实价位 (不足 2N 已在上面返回), 无需再筛哨兵.
    ++depth_seen_;
    const Price bid1 = lf.depth_buffer[L2::LOB_DEPTH]->price;
    const Price ask1 = lf.depth_buffer[L2::LOB_DEPTH - 1]->price;
    if (bid1 >= ask1) [[unlikely]] {
      ++depth_crossed_;
      lf.depth_updated = false;
      return;
    }

    lf.depth_updated = true;
  }
};
