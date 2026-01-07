#pragma once

#include <cmath>
#include <cstddef>
#include <deque>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

// #include "codec/L2_DataType.hpp"
#include "define/FastBitmap.hpp"
#include "define/MemPool.hpp"
#include "features/CoreSequential.hpp"
#include "features/backend/FeatureStore.hpp"
#include "lob/LimitOrderBookDefine.hpp"
// #include "math/sample/ResampleRunBar.hpp"

#if DEBUG_ANOMALY_PRINT == 1
#include <unordered_set>
#endif

#if DEBUG_ORDER_FLAGS_CREATE || DEBUG_ORDER_FLAGS_RESOLVE || DEBUG_ANOMALY_PRINT || DEBUG_BOOK_PRINT
#include "misc/logging.hpp"
#endif

inline constexpr size_t DEBUG_ASSET_IDS[] = {3};

inline constexpr size_t DEBUG_PRINT_DAYS = 0;                    // print first N consecutive days: 0
inline constexpr const char *DEBUG_PRINT_DATES[] = {"20240124"}; // print on specific dates

inline constexpr size_t DEBUG_ASSET_IDS_COUNT = sizeof(DEBUG_ASSET_IDS) / sizeof(DEBUG_ASSET_IDS[0]);
inline constexpr size_t DEBUG_PRINT_DATES_COUNT = sizeof(DEBUG_PRINT_DATES) / sizeof(DEBUG_PRINT_DATES[0]);

//========================================================================================
// MAIN CLASS
//========================================================================================

static_assert(L2::L2_MIN_TIME_INTERVAL_MS < 60000, "interval >= 60s, need minute carry");

class LimitOrderBook {

public:
  //======================================================================================
  // CONSTRUCTOR & CONFIGURATION
  //======================================================================================

  explicit LimitOrderBook(size_t ORDER_SIZE,
                          GlobalFeatureStore &store,
                          L2::ExchangeType exchange_type = L2::ExchangeType::SSE,
                          size_t asset_id = 0,
                          size_t core_id = 0)
      : order_lookup_(ORDER_SIZE),      // BumpDict with pre-allocated capacity
        order_memory_pool_(ORDER_SIZE), // BumpPool for Order objects
        exchange_type_(exchange_type),
        asset_id_(asset_id),
        core_sequential_(tick_data_, store, asset_id, core_id) {
    init_sentinel_levels();
  }

  // Set current date for feature computation
  void set_current_date(const std::string &date_str) {
    core_sequential_.set_date(date_str);

#if DEBUG_BOOK_PRINT
    should_log_this_day_ = false;

    // Check if asset_id is in debug list
    for (size_t i = 0; i < DEBUG_ASSET_IDS_COUNT; ++i) {
      if (DEBUG_ASSET_IDS[i] == asset_id_) {
        // Found debug asset - increment day counter
        debug_day_count_++;

        // Condition 1: First N consecutive days
        if (DEBUG_PRINT_DAYS > 0 && debug_day_count_ <= DEBUG_PRINT_DAYS) {
          should_log_this_day_ = true;
        }

        // Condition 2: Specific dates (both conditions can trigger)
        for (size_t j = 0; j < DEBUG_PRINT_DATES_COUNT; ++j) {
          if (date_str == DEBUG_PRINT_DATES[j]) {
            should_log_this_day_ = true;
            break;
          }
        }
        break;
      }
    }
#endif
  }

  // Get TOB invalid count
  size_t get_tob_invalid_count() const {
    return tob_invalid_cnt_;
  }

  // Get TOB refresh count
  size_t get_tob_refresh_count() const {
    return tob_refresh_cnt_;
  }

  //======================================================================================
  // PUBLIC API: Order Processing
  //======================================================================================

  // Process single order (live trading interface - keeps function boundary for hooks/monitoring)
  HOT_NOINLINE bool process(const L2::Order &order) {
    return process_impl(order);
  }

  // Process batch of orders (backtesting optimized - zero-overhead inlined loop)
  // Returns count of invalid orders
  HOT_INLINE size_t process_batch(const L2::Order *orders, size_t count) {
    size_t invalid_count = 0;
    for (size_t i = 0; i < count; ++i) {
      if (!process_impl(orders[i])) [[unlikely]] {
        ++invalid_count;
      }
    }
    return invalid_count;
  }

  //======================================================================================
  // PUBLIC API: Utilities
  //======================================================================================

  // Complete reset
  HOT_NOINLINE void clear() {
    price_levels_.fill(nullptr); // Reset direct array (all nullptr)
    level_storage_.clear();
    order_lookup_.clear();
    order_memory_pool_.reset();
    visible_price_bitmap_.reset();
    tob_price_ = 0;
    tob_dir_ = false;
    tob_valid_ = false;
    tob_invalid_cnt_ = 0;
    tob_refresh_cnt_ = 0;
    best_bid_ = 0;
    best_ask_ = 0;
    LOB_feature_ = {};
    LOB_feature_.depth_buffer.clear();
    last_depth_update_tick_ = 0;
    next_depth_update_tick_ = 0;
    was_in_matching_period_ = false;
    prev_tick_ = 0;
    curr_tick_ = 0;
    new_tick_ = false;
    new_sec_ = false;
    in_call_auction_ = false;
    in_matching_period_ = false;
    in_continuous_trading_ = false;
    delta_qty_ = 0;
    target_id_ = 0;
    actual_price_ = 0;
#if DEBUG_ANOMALY_PRINT
    debug_.printed_anomalies.clear();
#endif

    // Reset feature state for new day
    core_sequential_.reset();

    // Reinitialize sentinel levels
    init_sentinel_levels();
  }

private:
  //======================================================================================
  // INITIALIZATION
  //======================================================================================

  // Initialize sentinel levels at price range boundaries to ensure depth buffer can always be filled
  void init_sentinel_levels() {
    // Low price end (bid side): price 1 to L2::LOB_DEPTH, net_quantity = +1
    for (Price p = 1; p <= (L2::LOB_DEPTH - 1 + 1); ++p) {
      Level *level = level_create(p);
      level->net_quantity = 1;
      visibility_mark_visible(p);
    }

    // High price end (ask side): price (PRICE_RANGE_SIZE - L2::LOB_DEPTH) to (PRICE_RANGE_SIZE - 1), net_quantity = -1
    for (Price p = (PRICE_RANGE_SIZE - L2::LOB_DEPTH - 1); p <= (PRICE_RANGE_SIZE - 1 - 1); ++p) {
      Level *level = level_create(p);
      level->net_quantity = -1;
      visibility_mark_visible(p);
    }
  }

  //======================================================================================
  // DATA STRUCTURES (按层次组织)
  //======================================================================================

  //------------------------------------------------------------------------------------
  // Layer 1: Price Level Storage (价格档位基础层)
  //------------------------------------------------------------------------------------
  std::deque<Level> level_storage_;                      // All price levels (deque guarantees stable pointers)
  std::array<Level *, PRICE_RANGE_SIZE> price_levels_{}; // Direct array: Price -> Level* mapping for O(1) lookup (512 KB), initialized to all nullptr

  //------------------------------------------------------------------------------------
  // Layer 2: Order Tracking Infrastructure (订单追踪层)
  //------------------------------------------------------------------------------------
  MemPool::BumpDict<OrderId, Location, OrderIdHash> order_lookup_; // OrderId -> Location(Level*, index) for O(1) order lookup
  MemPool::BumpPool<Order> order_memory_pool_;                     // Memory pool for Order object allocation

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
  mutable bool tob_dir_ = false;       // Tick-by-tick TOB direction: false=bid, true=ask
  mutable bool tob_valid_ = false;     // Tick-by-tick TOB validity: false=invalid, true=valid
  mutable size_t tob_invalid_cnt_ = 0; // Tick-by-tick TOB invalid count
  mutable size_t tob_refresh_cnt_ = 0; // Tick-by-tick TOB invalid count
  mutable Price best_bid_ = 0;         // Tick-by-tick best bid (highest buy price with visible quantity)
  mutable Price best_ask_ = 0;         // Tick-by-tick best ask (lowest sell price with visible quantity)

  //------------------------------------------------------------------------------------
  // Layer 5: Feature Depth (特征深度层 - 时间驱动低频更新)
  //------------------------------------------------------------------------------------
  mutable TickData tick_data_;                // Tick data wrapper containing LOB_feature
  LOB_Feature &LOB_feature_ = tick_data_.lob; // Feature depth data with integrated depth_buffer (reference for convenience)

  // Time-driven depth update control
  mutable uint32_t last_depth_update_tick_ = 0; // Last tick when depth was updated
  mutable uint32_t next_depth_update_tick_ = 0; // Next allowed tick for depth update

  // Track matching period transition
  bool was_in_matching_period_ = false;

  //------------------------------------------------------------------------------------
  // Auxiliary State (辅助状态)
  //------------------------------------------------------------------------------------

  // Market timestamp tracking (hour|minute|second|millisecond)
  uint32_t prev_tick_ = 0; // Previous tick timestamp
  uint32_t curr_tick_ = 0; // Current tick timestamp
  uint32_t prev_sec_ = 0;  // Previous second timestamp
  uint32_t curr_sec_ = 0;  // Current second timestamp
  bool new_tick_ = false;  // Flag: entered new tick
  bool new_sec_ = false;   // Flag: entered new second (for feature snapshot update)

  // Trading session state cache (computed once per order)
  bool in_call_auction_ = false;
  bool in_matching_period_ = false;
  bool in_continuous_trading_ = false;

  // Exchange type - determines matching mechanism (SSE vs SZSE)
  L2::ExchangeType exchange_type_ = L2::ExchangeType::SSE;

  // Asset ID for debug logging
  [[maybe_unused]] size_t asset_id_ = 0;
#if DEBUG_BOOK_PRINT
  // Debug printing control (computed once per day)
  bool should_log_this_day_ = false;
  size_t debug_day_count_ = 0; // Counter for consecutive days (for DEBUG_PRINT_DAYS)
#endif

  // Hot path temporary variable cache (reduce allocation overhead)
  mutable Quantity delta_qty_; // Signed quantity change (+add/-deduct)
  mutable OrderId target_id_;  // Target order ID for current operation
  mutable Price actual_price_; // Actual effective price for current operation

  // Order parsing cache (parsed once per order in process())
  mutable bool is_maker_;
  mutable bool is_taker_;
  mutable bool is_cancel_;
  mutable bool is_bid_;

  // Resampling components
  // ResampleRunBar resampler_;

  // Feature update component (unified sequential core)
  CoreSequential core_sequential_;

  //======================================================================================
  // LEVEL MANAGEMENT (价格档位基础操作)
  //======================================================================================

  // Get or create: Atomically get existing level or create new one (single array access)
  HOT_INLINE Level *level_get_or_create(Price price) {
    Level *level = price_levels_[price];
    if (level == nullptr) [[unlikely]] {
      level_storage_.emplace_back(price);
      level = &level_storage_.back();
      price_levels_[price] = level;
    }
    return level;
  }

  // Create: Create new level (assumes level doesn't exist)
  HOT_INLINE Level *level_create(Price price) {
    level_storage_.emplace_back(price);
    Level *level = &level_storage_.back();
    price_levels_[price] = level;
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
    // if (should_log()) {
    //   Logger::log(std::to_string(asset_id_), "set visible: " + std::to_string(price));
    // }
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
    // if (should_log()) {
    //   Logger::log(std::to_string(asset_id_), "clear visible: " + std::to_string(price));
    // }
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
  // TIME UTILITIES (时间工具函数)
  //======================================================================================

  // Convert packed timestamp to human-readable format (HH:MM:SS.mmm)
  std::string format_time() const {
    uint8_t hours = (curr_tick_ >> 24) & 0xFF;
    uint8_t minutes = (curr_tick_ >> 16) & 0xFF;
    uint8_t seconds = (curr_tick_ >> 8) & 0xFF;
    uint8_t milliseconds = curr_tick_ & 0xFF;

    std::ostringstream time_formatter;
    time_formatter << std::setfill('0')
                   << std::setw(2) << int(hours) << ":"
                   << std::setw(2) << int(minutes) << ":"
                   << std::setw(2) << int(seconds) << "."
                   << std::setw(3) << int(milliseconds * 10);
    return time_formatter.str();
  }

#if DEBUG_ANOMALY_PRINT
  // Format timestamp as HH:MM:SS.mmm
  inline std::string format_timestamp(uint32_t ts) const {
    std::ostringstream oss;
    oss << std::setfill('0')
        << std::setw(2) << ((ts >> 24) & 0xFF) << ":"
        << std::setw(2) << ((ts >> 16) & 0xFF) << ":"
        << std::setw(2) << ((ts >> 8) & 0xFF) << "."
        << std::setw(3) << ((ts & 0xFF) * 10);
    return oss.str();
  }

  // Convert packed tick to milliseconds
  HOT_INLINE uint32_t tick_to_ms(uint32_t tick) const {
    return ((tick >> 24) & 0xFF) * 3600000 + ((tick >> 16) & 0xFF) * 60000 +
           ((tick >> 8) & 0xFF) * 1000 + (tick & 0xFF) * 10;
  }
#endif

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
    LOB_feature_.market_state = state_table[hhmm];

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

    in_matching_period_ = is_matching[LOB_feature_.market_state];
    in_call_auction_ = (LOB_feature_.market_state == L2::MarketState::OPENING_CALL_AUCTION ||
                        LOB_feature_.market_state == L2::MarketState::CLOSING_CALL_AUCTION ||
                        in_matching_period_);
    in_continuous_trading_ = !in_call_auction_;
  }

  // Clear CALL_AUCTION flags at 9:30:00 (orders stay at current levels)
  HOT_NOINLINE void flush_call_auction_flags() {
    // Optimization: Iterate level_storage_ (actual levels) instead of price_levels_ array (full range)
    // Complexity: O(active_levels) instead of O(PRICE_RANGE_SIZE)
    // Performance gain: 10x-100x for sparse order books
    for (Level &level : level_storage_) {
      for (Order *order : level.orders) {
        if (order->flags == OrderFlags::CALL_AUCTION) {
#if DEBUG_ORDER_FLAGS_RESOLVE
          print_order_flags_resolve(order->id, level.price, level.price, order->qty, order->qty, OrderFlags::CALL_AUCTION, OrderFlags::NORMAL, "FLUSH_930 ");
#endif
          order->flags = OrderFlags::NORMAL;
        }
      }
    }
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
  // This is the heart of the LOB reconstruction engine
  //
  // Parameters:
  //   order_id        - Unique order identifier
  //   price           - Price level for this order
  //   quantity_delta  - Signed quantity change (+add, -deduct)
  //   loc             - Location pointer from order_lookup (from prior find operation)
  //   flags           - Order state flags (NORMAL, OUT_OF_ORDER, etc.)
  //   level_hint      - Optional pre-fetched level pointer (optimization)
  //
  // Returns: true if order was fully consumed (removed), false otherwise
  //
  // Optimization: Pass level_hint to avoid redundant hash lookups when caller
  //               already knows the target level
  HOT_NOINLINE bool order_upsert(
      OrderId order_id,
      Price price,
      Quantity quantity_delta,
      Location *loc,
      OrderFlags flags = OrderFlags::NORMAL,
      Level *level_hint = nullptr) {

    if (loc != nullptr) [[likely]] {
      // ORDER EXISTS - Update existing order (HOT PATH)
      Level *level = loc->level;
      size_t order_index = loc->index;
      Order *order = level->orders[order_index];

      // Apply quantity delta
      const Quantity old_qty = order->qty;
      const Quantity new_qty = old_qty + quantity_delta;

      if (new_qty == 0) [[unlikely]] {
        // FULLY CONSUMED - Remove order completely (COLD PATH)
#if DEBUG_ORDER_FLAGS_RESOLVE
        if (order->flags != OrderFlags::NORMAL) {
          print_order_flags_resolve(order_id, price, price, old_qty, 0, order->flags, OrderFlags::NORMAL, "CONSUME   ");
        }
#endif

        // Update feature all_volume (incremental) before removal
        const uint32_t abs_qty = std::abs(old_qty);
        LOB_feature_.all_bid_volume -= (old_qty > 0) ? abs_qty : 0;
        LOB_feature_.all_ask_volume -= (old_qty < 0) ? abs_qty : 0;

        // Record visibility state BEFORE removal
        const bool was_visible = level->has_visible_quantity();

        // Remove order from level (BumpPool doesn't need individual deallocation)
        level->remove(order_index);
        order_lookup_.erase(order_id);
        // order_memory_pool_.deallocate(order); // No-op for BumpPool, memory reclaimed at EOD clear()

        // Fix up order_lookup_ index for moved order (swap-and-pop side effect)
        if (order_index < level->orders.size()) {
          Location *moved_loc = order_lookup_.find(level->orders[order_index]->id);
          if (moved_loc != nullptr) {
            moved_loc->index = order_index;
          }
        }

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
        level->net_quantity += quantity_delta;
        order->qty = new_qty;

        // Update feature all_volume (incremental) - apply delta
        const uint32_t abs_delta = std::abs(quantity_delta);
        LOB_feature_.all_bid_volume += (quantity_delta > 0) ? abs_delta : 0;
        LOB_feature_.all_ask_volume += (quantity_delta < 0) ? abs_delta : 0;

        // Update flags if needed (rare)
        if ((flags != OrderFlags::NORMAL || order->flags != OrderFlags::NORMAL)) [[unlikely]] {
#if DEBUG_ORDER_FLAGS_RESOLVE
          [[maybe_unused]] OrderFlags old_flags = order->flags;
          if (old_flags != flags) {
            print_order_flags_resolve(order_id, price, price, old_qty, new_qty, old_flags, flags, "UPDATE_FLG");
          }
#endif
          order->flags = flags;
        }

        // Update visibility only if it changed (rare)
        if (was_visible != level->has_visible_quantity()) [[unlikely]] {
          visibility_update_from_level(level);
        }
        return false; // Partially consumed
      }

    } else {
      // ORDER DOESN'T EXIST - Create new order (LESS FREQUENT)
      const uint32_t ts = DEBUG_ANOMALY_PRINT ? curr_tick_ : 0;
      Order *new_order = order_memory_pool_.construct(quantity_delta, order_id, ts, flags);
      // Note: BumpPool never returns nullptr, it throws on OOM

      // Get level: use hint if provided (optimization), otherwise get or create atomically
      Level *level = level_hint ? level_hint : level_get_or_create(price);

      // Add order to level and register in lookup table
      size_t order_index = level->orders.size();
      level->add(new_order);
      order_lookup_.try_emplace(order_id, Location{level, order_index});

      // Update feature all_volume (incremental) for new order
      const uint32_t abs_delta = std::abs(quantity_delta);
      LOB_feature_.all_bid_volume += (quantity_delta > 0) ? abs_delta : 0;
      LOB_feature_.all_ask_volume += (quantity_delta < 0) ? abs_delta : 0;

      // Update visibility if level just became visible
      if (quantity_delta != 0 && !visibility_is_visible(level->price)) [[likely]] {
        visibility_mark_visible(level->price);
      }

#if DEBUG_ORDER_FLAGS_CREATE
      print_order_flags_create(order_id, price, quantity_delta, flags);
#endif

      return false; // New order created
    }
  }

  // Move: Relocate order from one price level to another (using location pointer)
  // Optimization: Accepts location pointer to avoid redundant hash lookup
  HOT_NOINLINE void order_move_to_price(
      Location *loc,
      Price new_price) {
    Level *old_level = loc->level;
    size_t old_index = loc->index;
    Price old_price = old_level->price;

    if (old_price == new_price)
      return; // Already at correct level

    Order *order = old_level->orders[old_index];

    // Step 1: Remove from old level (swap-and-pop)
    old_level->remove(old_index);

    // Step 2: Fix up order_lookup_ for swapped order (swap-and-pop side effect)
    if (old_index < old_level->orders.size()) {
      Location *swapped_loc = order_lookup_.find(old_level->orders[old_index]->id);
      if (swapped_loc != nullptr) {
        swapped_loc->index = old_index;
      }
    }

    // Step 3: Add to new level (get or create atomically)
    Level *new_level = level_get_or_create(new_price);

    size_t new_index = new_level->orders.size();
    new_level->add(order);

    // Step 4: Update order_lookup_ to point to new location
    loc->level = new_level;
    loc->index = new_index;

    // Step 5: Update visibility for both levels
    visibility_update_from_level(old_level);
    visibility_update_from_level(new_level);

    // Step 6: Cleanup empty old level
    if (old_level->empty()) {
      level_remove(old_level);
    }

#if DEBUG_ORDER_FLAGS_RESOLVE
    print_order_flags_resolve(order->id, old_price, new_price, order->qty, order->qty, order->flags, order->flags, "MIGRATE   ");
#endif
  }

  // Move: Relocate order by ID (requires hash lookup, less efficient)
  HOT_NOINLINE void order_move_by_id(OrderId order_id, Price new_price) {
    Location *loc = order_lookup_.find(order_id);
    if (loc == nullptr)
      return; // Order not found

    order_move_to_price(loc, new_price);
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
      // if (should_log()) {
      //   Logger::log(std::to_string(asset_id_), "lv=" + std::to_string(price_levels_[p]->price) + ", qty=" + std::to_string(price_levels_[p]->net_quantity));
      // }
    }

    // Scan up for ask (qty < 0), count distance
    for (Price p = tob_price_; p != 0; p = next_ask_above(p)) {
      if (Level *lv = price_levels_[p]; lv && lv->net_quantity < 0) {
        best_ask_ = p;
        break;
      }
      ++ask_dist;
      // if (should_log()) {
      //   Logger::log(std::to_string(asset_id_), "lv=" + std::to_string(price_levels_[p]->price) + ", qty=" + std::to_string(price_levels_[p]->net_quantity));
      // }
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

    // if (should_log()) {
    //   Logger::log(std::to_string(asset_id_), "TOB: bid_dist=" + std::to_string(bid_dist) + ", ask_dist=" + std::to_string(ask_dist) + ", best_bid_=" + std::to_string(best_bid_) + ", best_ask_=" + std::to_string(best_ask_) + ", tob_price_=" + std::to_string(tob_price_));
    // }
    tob_refresh_cnt_++;
  }

  //======================================================================================
  // HIGH-LEVEL PROCESSING (高层处理逻辑)
  //======================================================================================

  // Core order processing implementation (shared by single/batch interfaces)
  HOT_INLINE bool process_impl(const L2::Order &order) {
    // Skip dirty orders @09:26:00
    if (order.price == 0 && order.volume == 0) [[unlikely]] {
      return true;
    }

    // Parse timestamp
    curr_tick_ = (order.hour << 24) | (order.minute << 16) | (order.second << 8) | order.millisecond;
    new_tick_ = curr_tick_ != prev_tick_;
    curr_sec_ = (curr_tick_ >> 8);
    new_sec_ = curr_sec_ != prev_sec_;

    // Update feature timestamp (only on new tick)
    if (new_tick_) {
      LOB_feature_.hour = order.hour;
      LOB_feature_.minute = order.minute;
      LOB_feature_.second = order.second;
      LOB_feature_.millisecond = order.millisecond;
      update_trading_session_state();
    }

    // Parse order metadata (cache for reuse)
    is_maker_ = (order.order_type == L2::OrderType::MAKER);
    is_taker_ = (order.order_type == L2::OrderType::TAKER);
    is_cancel_ = (order.order_type == L2::OrderType::CANCEL);
    is_bid_ = (order.order_dir == L2::OrderDirection::BID);

    // Update feature order metadata (every order)
    LOB_feature_.order_type = order.order_type;
    LOB_feature_.order_dir = order.order_dir;
    LOB_feature_.price = order.price * 0.01;
    LOB_feature_.volume = order.volume;

    // Detect transition from matching period to continuous trading
    if (was_in_matching_period_ && !in_matching_period_ && !in_call_auction_) [[unlikely]] {
      flush_call_auction_flags();
    }
    was_in_matching_period_ = in_matching_period_;

    // ==================== depth update triggered =======================

    // // Process resampling (only for TAKER orders)
    // if (is_taker_) {
    //   if (resampler_.resample(curr_tick_, is_bid_, order.volume)) {
    //     // std::cout << "[RESAMPLE] Bar formed at " << format_time() << std::endl;
    //   }
    // }

    // 计算特征(depth 更新后)
    if (update_depth()) {
#if DEBUG_BOOK_PRINT
      print_book();
#endif
    }

    core_sequential_.compute_and_store();

    // ========================= lob update ==============================
    bool result = update_lob(order);

    prev_tick_ = curr_tick_;
    prev_sec_ = curr_tick_ >> 8;

    return result;
  }

  // Helper: Process one taker side (for bilateral or unilateral)
  // Returns: true if order was fully consumed
  HOT_INLINE bool process_taker_side(
      const L2::Order &order,
      OrderId order_id,
      Quantity delta) {

    Location *loc = order_lookup_.find(order_id);
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

  // Main LOB update logic (dispatches to MAKER/TAKER/CANCEL)
  HOT_NOINLINE bool update_lob(const L2::Order &order) {
#if DEBUG_ANOMALY_PRINT
    debug_.last_order = &order;
#endif

    //====================================================================================
    // MAKER: Always unilateral, never enters loop
    //====================================================================================
    if (is_maker_) {
      order_extract_params(order);
      if (delta_qty_ == 0 || target_id_ == 0) [[unlikely]]
        return false;

      // FAST PATH: Normal maker with price
      if (!in_call_auction_ && order.price != 0) [[likely]] {
        Location *loc = order_lookup_.find(target_id_);
        if (loc == nullptr) [[likely]] {
          actual_price_ = order.price;
          order_upsert(target_id_, actual_price_, delta_qty_, loc, OrderFlags::NORMAL);
          return true;
        }
      }

      // DEFERRED PATH: Special cases (out-of-order, call auction, price=0)
      Location *loc = order_lookup_.find(target_id_);
      return update_lob_deferred(order, loc, loc != nullptr, in_call_auction_, in_matching_period_);
    }

    //====================================================================================
    // TAKER/CANCEL: May be bilateral or unilateral
    //====================================================================================
    const bool need_bilateral = (exchange_type_ == L2::ExchangeType::SZSE) || (exchange_type_ == L2::ExchangeType::SSE && in_matching_period_);
    const bool is_bilateral = is_taker_ && need_bilateral && order.bid_order_id != 0 && order.ask_order_id != 0;

    if (is_bilateral) {
      //==================================================================================
      // BILATERAL TAKER: Process both bid and ask sides (symmetrically)
      //==================================================================================
      const bool is_active_bid = (order.bid_order_id > order.ask_order_id);

      [[maybe_unused]] bool consumed_bid = process_taker_side(order, order.bid_order_id, -static_cast<Quantity>(order.volume));
      [[maybe_unused]] bool consumed_ask = process_taker_side(order, order.ask_order_id, +static_cast<Quantity>(order.volume));

      actual_price_ = order.price; // for safety
      tob_price_ = actual_price_;
      tob_dir_ = is_active_bid;
      // update_tob(is_active_bid, is_active_bid ? consumed_ask : consumed_bid, actual_price_);
      return true;

    } else {
      //==================================================================================
      // UNILATERAL TAKER/CANCEL: Single target
      //==================================================================================
      order_extract_params(order);
      if (delta_qty_ == 0 || target_id_ == 0) [[unlikely]]
        return false;

      [[maybe_unused]] bool consumed = process_taker_side(order, target_id_, delta_qty_);

      if (is_taker_) {
        tob_price_ = actual_price_;
        tob_dir_ = is_bid_;
        // update_tob(is_bid_, consumed, actual_price_);
      }
      return true;
    }
  };

  // Slow path: handle corner cases (out-of-order, call auction, special prices)
  // NOTE: Uses cached is_maker_/is_taker_/is_cancel_ and is_bid_ from process()
  [[gnu::cold]] [[gnu::noinline]] bool update_lob_deferred(
      const L2::Order &order,
      Location *loc,
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
      const OrderId self_id = is_bid_ ? order.bid_order_id : order.ask_order_id;
      const bool both_ids_present = (order.bid_order_id != 0 && order.ask_order_id != 0);

      // Handle target order (counterparty)
      bool fully_consumed = false;
      if (found) {
        Price target_price = loc->level->price;

        if (target_price != order.price) {
          OrderFlags anomaly_flag = OrderFlags::NORMAL;
          if (!in_call_auction && !in_matching_period) {
            anomaly_flag = OrderFlags::ANOMALY_MATCH;
          }

          order_move_to_price(loc, order.price);

          if (anomaly_flag == OrderFlags::ANOMALY_MATCH) {
            Order *target_order = loc->level->orders[loc->index];
            target_order->flags = anomaly_flag;
          }
        }

        actual_price_ = order.price;
        fully_consumed = order_upsert(target_id_, actual_price_, delta_qty_, loc);
      } else {
        // OUT_OF_ORDER: create placeholder
        actual_price_ = order.price;
        order_upsert(target_id_, actual_price_, delta_qty_, loc, OrderFlags::OUT_OF_ORDER);
        fully_consumed = false;
      }

      // Handle self order (market orders from Level[0])
      const bool need_self_order = (self_id != 0 && self_id != target_id_) && !both_ids_present;

      if (need_self_order) {
        Location *self_loc = order_lookup_.find(self_id);
        if (self_loc != nullptr) {
          Order *self_order = self_loc->level->orders[self_loc->index];

          if (self_order->flags == OrderFlags::SPECIAL_MAKER) {
            Price self_price = self_loc->level->price;
            if (self_price != order.price) {
              order_move_to_price(self_loc, order.price);
            }
          }

          order_upsert(self_id, order.price, -delta_qty_, self_loc);
        }
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
        order_upsert(target_id_, placement_price, delta_qty_, loc, flags);
      }

      return true;
    }

    return false;
  };

  //======================================================================================
  // DEPTH BUFFER MANAGEMENT (N档深度缓冲区管理 - 订单+时间双驱动)
  //======================================================================================

  // Binary search price in entire depth_buffer (descending order)
  HOT_INLINE size_t depth_binary_search(Price price) const {
    size_t left = 0, right = LOB_feature_.depth_buffer.size();
    while (left < right) {
      size_t mid = left + (right - left) / 2;
      if (LOB_feature_.depth_buffer[mid]->price > price) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    return left;
  }

  // Find N visible levels beyond boundary price using bitmap
  // is_ask_side: true = ask side (higher prices), false = bid side (lower prices)
  // boundary_price: starting price (exclusive)
  // count: number of levels to find
  // Returns: vector of level pointers (may be < count if not enough levels available)
  HOT_INLINE std::vector<Level *> depth_find_levels_beyond(bool is_ask_side, Price boundary_price, size_t count) const {
    std::vector<Level *> levels;
    levels.reserve(count);

    Price current_price = boundary_price;
    for (size_t i = 0; i < count; ++i) {
      Price next_price = is_ask_side ? next_ask_above(current_price) : next_bid_below(current_price);
      if (next_price == 0)
        break;

      Level *next_level = price_levels_[next_price];
      if (next_level && next_level->has_visible_quantity()) {
        levels.push_back(next_level);
        current_price = next_price;
      } else {
        break;
      }
    }

    return levels;
  }

  // Order-driven: Add level to depth buffer
  HOT_INLINE void depth_on_level_add_remove(Level *level, bool add) {
    if (level->price == 0 || (LOB_feature_.depth_buffer.size() <= 2)) [[unlikely]]
      return;

    // Check if price is within current range
    Price high_price = LOB_feature_.depth_buffer.front()->price;
    Price low_price = LOB_feature_.depth_buffer.back()->price;
    if (level->price > high_price || low_price > level->price)
      return;

    // Binary search to find insert position
    size_t idx = depth_binary_search(level->price);

    // Logger::log(std::to_string(asset_id_), std::to_string(level->price) + ": add/remove " + std::to_string(add) + " high_price=" + std::to_string(high_price) + " low_price=" + std::to_string(low_price) + " idx=" + std::to_string(idx));
    if (add) {
      // Just insert, CBuffer will auto-pop front when full
      LOB_feature_.depth_buffer.insert(idx, level);
    } else {
      // Just remove, CBuffer will auto-pop back when empty
      LOB_feature_.depth_buffer.erase(idx);
    }
  }

  //======================================================================================
  // FEATURE UPDATES (特征更新 - 时间驱动)
  //======================================================================================

  // Update depth if TOB is valid (called from process() when time interval reached)
  HOT_NOINLINE bool update_depth() {

    if (!(new_tick_ && curr_tick_ >= next_depth_update_tick_)) {
      // if (!(new_tick_)) {
      LOB_feature_.depth_updated = false;
      return false;
    };

    update_tob();

    // Determine if rebuild needed
    size_t current_depth = LOB_feature_.depth_buffer.size();

    // Calculate insertion counts
    size_t bid_idx = depth_binary_search(best_bid_);
    size_t ask_count = static_cast<int>(L2::LOB_DEPTH) - static_cast<int>(bid_idx);
    size_t bid_count = static_cast<int>(bid_idx + L2::LOB_DEPTH) - static_cast<int>(current_depth);

    bool need_rebuild = current_depth <= 2 || ask_count >= L2::LOB_DEPTH || bid_count >= L2::LOB_DEPTH;

    ask_count = need_rebuild ? L2::LOB_DEPTH : ask_count;
    bid_count = need_rebuild ? L2::LOB_DEPTH : bid_count;

    // if (should_log()) {
    //   Logger::log(std::to_string(asset_id_), "update_depth: need_rebuild=" + std::to_string(need_rebuild) + ", bid_idx=" + std::to_string(bid_idx) + ", ask_count=" + std::to_string(ask_count) + ", bid_count=" + std::to_string(bid_count));
    // }

    if (need_rebuild)
      LOB_feature_.depth_buffer.clear();

    // Fill ask side (upper half)
    Price price = need_rebuild ? best_ask_ : LOB_feature_.depth_buffer.front()->price;
    for (size_t i = 0; i < ask_count && price > 0; ++i) {
      if (!need_rebuild) {
        price = next_ask_above(price);
        if (price == 0)
          break;
      }
      LOB_feature_.depth_buffer.push_front(price_levels_[price]);
      if (need_rebuild)
        price = next_ask_above(price);
    }

    // Fill bid side (lower half)
    price = need_rebuild ? best_bid_ : LOB_feature_.depth_buffer.back()->price;
    for (size_t i = 0; i < bid_count && price > 0; ++i) {
      if (!need_rebuild) {
        price = next_bid_below(price);
        if (price == 0)
          break;
      }
      LOB_feature_.depth_buffer.push_back(price_levels_[price]);
      if (need_rebuild)
        price = next_bid_below(price);
    }

    last_depth_update_tick_ = curr_tick_;

    // packed tick add: 跳过所有空slot, 避免稀疏数据导致连续触发
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

    return LOB_feature_.depth_updated = LOB_feature_.depth_buffer.size() >= L2::LOB_DEPTH;
  }

  //======================================================================================
  // DEBUG UTILITIES (调试工具)
  //======================================================================================

  // Check if current asset should dump debug logs for this day
  inline bool should_log() const {
#if DEBUG_BOOK_PRINT
    return should_log_this_day_;
#else
    return false;
#endif
  }

  // Helper: Get flags string for debug output
  static const char *get_order_flags_str(OrderFlags flags) {
    switch (flags) {
    case OrderFlags::NORMAL:
      return "NORMAL         ";
    case OrderFlags::UNKNOWN:
      return "UNKNOWN        ";
    case OrderFlags::OUT_OF_ORDER:
      return "OUT_OF_ORDER   ";
    case OrderFlags::CALL_AUCTION:
      return "CALL_AUCTION   ";
    case OrderFlags::SPECIAL_MAKER:
      return "SPECIAL_MAKER  ";
    case OrderFlags::ZERO_PRICE:
      return "ZERO_PRICE     ";
    case OrderFlags::ANOMALY_MATCH:
      return "ANOMALY_MATCH  ";
    default:
      return "UNKNOWN_FLAG   ";
    }
  }

#if DEBUG_ORDER_FLAGS_CREATE
  // 🟡 CREATE: Print when order with special flags is created (Yellow)
  void print_order_flags_create(OrderId order_id, Price price, Quantity qty, OrderFlags flags) const {
    if (flags == OrderFlags::NORMAL)
      return; // Skip normal orders
    if (!should_log())
      return;

    std::ostringstream msg;
    msg << "\033[33m[CREATE] " << format_time()
        << " | " << get_order_flags_str(flags)
        << " | ID=" << std::setw(7) << std::right << order_id
        << " Price=" << std::setw(5) << std::right << price
        << " Qty=" << std::setw(6) << std::right << qty
        << " | TotalOrders=" << std::setw(5) << std::right << (order_lookup_.size() + 1)
        << "\033[0m";
    Logger::log(std::to_string(asset_id_), msg.str());
  }
#endif

#if DEBUG_ORDER_FLAGS_RESOLVE
  // 🔵 RESOLVE: Print when order with special flags is resolved (Blue)
  // Types: MIGRATE (price change), CONSUME (fully matched), UPDATE_FLAGS (flags changed)
  void print_order_flags_resolve(OrderId order_id, Price old_price, Price new_price,
                                 Quantity old_qty, Quantity new_qty,
                                 OrderFlags old_flags, OrderFlags new_flags,
                                 const char *action) const {
    if (old_flags == OrderFlags::NORMAL && new_flags == OrderFlags::NORMAL)
      return; // Skip normal orders
    if (!should_log())
      return;

    std::ostringstream msg;
    msg << "\033[36m[" << action << "] " << format_time()
        << " | " << get_order_flags_str(old_flags)
        << " → " << get_order_flags_str(new_flags)
        << " | ID=" << std::setw(7) << std::right << order_id;

    // Price field: show old→new if changed, otherwise single value (12 chars total)
    if (old_price != new_price) {
      msg << " Price=" << std::setw(5) << std::right << old_price
          << "→" << std::setw(5) << std::right << new_price;
    } else {
      msg << " Price=" << std::setw(5) << std::right << old_price << "      ";
    }

    // Qty field: show old→new if changed, otherwise single value (12 chars total)
    if (old_qty != new_qty) {
      msg << " Qty=" << std::setw(6) << std::right << old_qty
          << "→" << std::setw(5) << std::right << new_qty;
    } else {
      msg << " Qty=" << std::setw(6) << std::right << new_qty << "      ";
    }

    msg << " | TotalOrders=" << std::setw(5) << std::right << order_lookup_.size()
        << "\033[0m";
    Logger::log(std::to_string(asset_id_), msg.str());
  }
#endif

#if DEBUG_ANOMALY_PRINT
  // Debug state storage
  struct DebugState {
    const L2::Order *last_order = nullptr;
    std::unordered_set<Price> printed_anomalies;
  };
  mutable DebugState debug_;

  // Check for sign anomaly in level (print far anomalies N+ ticks from TOB during continuous trading)
  void check_anomaly(Level *level) const {

    // Skip level 0 (special level)
    if (level->price == 0)
      return;

    // Step 2: Classify by price relative to TOB mid price
    const Price tob_mid = (best_bid_ + best_ask_) / 2;
    const bool is_bid_side = (level->price < tob_mid);

    const bool has_anomaly = (is_bid_side && level->net_quantity < 0) || (!is_bid_side && level->net_quantity > 0);

    // Skip if no anomaly or already printed
    if (!has_anomaly)
      return;
    if (debug_.printed_anomalies.count(level->price))
      return;

    // Step 3: Time filter - only print during continuous trading (use cached state)
    if (!in_continuous_trading_) {
      return; // Anomaly exists but not printed (call auction period)
    }

    // Print and mark as printed
    debug_.printed_anomalies.insert(level->price);
    print_anomaly_level(level, is_bid_side);
  }

  // Print detailed anomaly information for a level
  void print_anomaly_level(Level *level, bool is_bid_side) const {
    if (!should_log())
      return;

    // Collect all reverse-sign orders (unmatched orders)
    std::vector<Order *> anomaly_orders;
    anomaly_orders.reserve(level->order_count); // Pre-allocate

    for (Order *order : level->orders) {
      const bool is_reverse = (is_bid_side && order->qty < 0) || (!is_bid_side && order->qty > 0);
      if (is_reverse)
        anomaly_orders.push_back(order);
    }
    if (anomaly_orders.empty())
      return;

    // Sort by ID (smallest first, earlier orders)
    std::sort(anomaly_orders.begin(), anomaly_orders.end(),
              [](const Order *a, const Order *b) { return a->id < b->id; });

    // Print level summary header
    std::ostringstream msg;
    msg << "\033[35m[ANOMALY_LEVEL] " << format_time()
        << " Level=" << level->price << " ExpectedSide=" << (is_bid_side ? "BID" : "ASK")
        << " NetQty=" << level->net_quantity << " TotalOrders=" << level->order_count
        << " UnmatchedOrders=" << anomaly_orders.size()
        << " | TOB: Bid=" << best_bid_ << " Ask=" << best_ask_ << "\033[0m";
    Logger::log(std::to_string(asset_id_), msg.str());

    // Print all unmatched orders sorted by size
    for (size_t i = 0; i < anomaly_orders.size(); ++i) {
      const Order *order = anomaly_orders[i];
      std::ostringstream order_msg;
      order_msg << "\033[35m  [" << (i + 1) << "] ID=" << order->id
                << " Qty=" << order->qty
                << " Created=" << format_timestamp(order->timestamp)
                << " Age=" << (tick_to_ms(curr_tick_) - tick_to_ms(order->timestamp)) << "ms\033[0m";
      Logger::log(std::to_string(asset_id_), order_msg.str());
    }
  }

#endif // DEBUG_ANOMALY_PRINT

  //======================================================================================
  // DEBUG: Book Display
  //======================================================================================

#if DEBUG_BOOK_PRINT

  inline static constexpr size_t LEVEL_WIDTH = 12;

  // Helper: Calculate display width excluding ANSI codes
  inline size_t display_width(const std::string &s) const {
    size_t width = 0;
    bool in_ansi = false;
    for (char c : s) {
      if (c == '\033') {
        in_ansi = true;
      } else if (in_ansi && c == 'm') {
        in_ansi = false;
      } else if (!in_ansi) {
        ++width;
      }
    }
    return width;
  }

  // Helper: Format level string for display
  inline std::string format_level(Price price, int32_t volume) const {
    const bool is_anomaly = (volume < 0);
#if DEBUG_BOOK_AS_AMOUNT == 0
    const std::string qty_str = std::to_string(volume);
#else
    const float amount = std::abs(volume) * price / 1000000.0;
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << amount;
    const std::string qty_str = (volume < 0 ? "-" : "") + oss.str();
#endif
    const std::string level_str = std::to_string(price) + "x" + qty_str;
    return is_anomaly ? "\033[31m" + level_str + "\033[0m" : level_str;
  }

  // Real-time depth printer: Compute N levels directly from TOB + bitmap (golden reference)
  void inline print_book_realtime() const {
    if (!in_continuous_trading_)
      return;
    if (!should_log())
      return;

    constexpr size_t N = L2::LOB_DEPTH;

    std::ostringstream out;
    out << "\033[32m[RT] " << format_time() << "\033[0m ["
        << std::setfill('0') << std::setw(3) << (price_levels_[0] ? price_levels_[0]->order_count : 0)
        << std::setfill(' ') << "] ";

    // Collect ask/bid levels
    auto collect = [&](Price start, auto next_func, size_t count) {
      std::vector<std::pair<Price, int32_t>> levels;
      for (Price p = start; levels.size() < count && p > 0; p = next_func(p)) {
        if (Level *lv = price_levels_[p]; lv) // && lv->has_visible_quantity())
          levels.push_back({lv->price, lv->net_quantity});
      }
      return levels;
    };

    auto asks = collect(best_ask_, [&](Price p) { return next_ask_above(p); }, N);
    auto bids = collect(best_bid_, [&](Price p) { return next_bid_below(p); }, N);

    // Display asks (reverse)
    for (size_t i = 0; i < L2::LOB_DEPTH - N; ++i)
      out << std::setw(LEVEL_WIDTH) << " ";
    for (int i = asks.size() - 1; i >= 0; --i) {
      std::string level_str = format_level(asks[i].first, -asks[i].second);
      out << level_str << std::string(LEVEL_WIDTH > display_width(level_str) ? LEVEL_WIDTH - display_width(level_str) : 0, ' ');
    }
    for (size_t i = asks.size(); i < N; ++i)
      out << std::setw(LEVEL_WIDTH) << " ";

    out << " (" << std::setw(4) << best_ask_ << ")ASK " << (tob_dir_ ? "<|" : "|>") << " BID(" << std::setw(4) << best_bid_ << ") ";

    // Display bids
    for (size_t i = 0; i < bids.size(); ++i) {
      std::string level_str = format_level(bids[i].first, bids[i].second);
      out << level_str << std::string(LEVEL_WIDTH > display_width(level_str) ? LEVEL_WIDTH - display_width(level_str) : 0, ' ');
    }
    for (size_t i = bids.size(); i < L2::LOB_DEPTH; ++i)
      out << std::setw(LEVEL_WIDTH) << " ";

    Logger::log(std::to_string(asset_id_), out.str());
  }

  // Depth-buffer-based printer: Display from LOB_feature_ (buffered depth)
  void inline print_book_buffered() const {
    if (!in_continuous_trading_)
      return;
    if (!should_log())
      return;

    constexpr size_t N = L2::LOB_DEPTH;

    std::ostringstream out;
    out << "\033[34m[BUF]" << format_time() << "\033[0m ["
        << std::setfill('0') << std::setw(3) << (price_levels_[0] ? price_levels_[0]->order_count : 0)
        << std::setfill(' ') << "] ";

    // Display asks (reverse)
    for (size_t i = 0; i < L2::LOB_DEPTH - N; ++i)
      out << std::setw(LEVEL_WIDTH) << " ";
    for (int i = N - 1; i >= 0; --i) {
      const size_t buf_idx = L2::LOB_DEPTH - 1 - i;
      if (buf_idx < LOB_feature_.depth_buffer.size() && LOB_feature_.depth_buffer[buf_idx]) {
        Price p = LOB_feature_.depth_buffer[buf_idx]->price;
        int32_t v = LOB_feature_.depth_buffer[buf_idx]->net_quantity;
        std::string level_str = format_level(p, -v);
        out << level_str << std::string(LEVEL_WIDTH > display_width(level_str) ? LEVEL_WIDTH - display_width(level_str) : 0, ' ');
      } else {
        out << std::setw(LEVEL_WIDTH) << " ";
      }
    }

    out << " (" << std::setw(4) << best_ask_ << ")ASK " << (tob_dir_ ? "<|" : "|>") << " BID(" << std::setw(4) << best_bid_ << ") ";

    // Display bids
    for (size_t i = 0; i < N; ++i) {
      const size_t buf_idx = L2::LOB_DEPTH + i;
      if (buf_idx < LOB_feature_.depth_buffer.size() && LOB_feature_.depth_buffer[buf_idx]) {
        Price p = LOB_feature_.depth_buffer[buf_idx]->price;
        int32_t v = LOB_feature_.depth_buffer[buf_idx]->net_quantity;
        std::string level_str = format_level(p, v);
        out << level_str << std::string(LEVEL_WIDTH > display_width(level_str) ? LEVEL_WIDTH - display_width(level_str) : 0, ' ');
      } else {
        out << std::setw(LEVEL_WIDTH) << " ";
      }
    }
    for (size_t i = N; i < L2::LOB_DEPTH; ++i)
      out << std::setw(LEVEL_WIDTH) << " ";

    Logger::log(std::to_string(asset_id_), out.str());
  }

  // Unified printer: calls both real-time and buffered for comparison
  void inline print_book() const {
    print_book_realtime();
    print_book_buffered();
  }

#endif // DEBUG_BOOK_PRINT
};
