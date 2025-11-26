#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "define/CBuffer.hpp"

//========================================================================================
// PROFILING CONTROL MACROS
//========================================================================================

// Control inlining based on PROFILE_MODE (set by CMake)
#ifdef PROFILE_MODE
#define HOT_INLINE [[gnu::noinline]]   // Disable inlining for profiler visibility
#define HOT_NOINLINE [[gnu::noinline]] // Keep function boundary visible
#else
#define HOT_INLINE [[gnu::hot, gnu::always_inline]] inline // Aggressive inlining
#define HOT_NOINLINE [[gnu::hot]]                          // Hot but not inlined
#endif

//========================================================================================
// DEBUG CONFIGURATION
//========================================================================================

// Debug switches
#define DEBUG_ORDER_PRINT 0         // Print every order processing
#define DEBUG_ORDER_FLAGS_CREATE 0  // Print when order with special flags is created
#define DEBUG_ORDER_FLAGS_RESOLVE 0 // Print when order with special flags is resolved/migrated
#define DEBUG_BOOK_PRINT 0          // Print order book snapshot when effective TOB updated
#define DEBUG_BOOK_AS_AMOUNT 1      // 0: 股, 1: 1万元, 2: 2万元, 3: 3万元, ...
#define DEBUG_ANOMALY_PRINT 1       // Print max unmatched order with creation timestamp

// Auto-disable dependent switches based on logical relationships
#if DEBUG_BOOK_PRINT == 0
#undef DEBUG_BOOK_AS_AMOUNT
#define DEBUG_BOOK_AS_AMOUNT 0
#undef DEBUG_ANOMALY_PRINT
#define DEBUG_ANOMALY_PRINT 0
#endif

//========================================================================================
// CONFIGURATION PARAMETERS
//========================================================================================

// Trading session time points (China A-share market)
namespace TradingSession {
// Morning call auction (集合竞价)
constexpr uint8_t MORNING_CALL_AUCTION_START_HOUR = 9;
constexpr uint8_t MORNING_CALL_AUCTION_START_MINUTE = 15;
constexpr uint8_t MORNING_CALL_AUCTION_END_MINUTE = 25;

// Morning matching period (集合竞价撮合期)
constexpr uint8_t MORNING_MATCHING_START_MINUTE = 25;
constexpr uint8_t MORNING_MATCHING_END_MINUTE = 30;

// Continuous auction (连续竞价)
constexpr uint8_t CONTINUOUS_TRADING_START_HOUR = 9;
constexpr uint8_t CONTINUOUS_TRADING_START_MINUTE = 30;
constexpr uint8_t CONTINUOUS_TRADING_END_HOUR = 15;
constexpr uint8_t CONTINUOUS_TRADING_END_MINUTE = 0;

// Closing call auction (收盘集合竞价 - Shenzhen only)
constexpr uint8_t CLOSING_CALL_AUCTION_START_HOUR = 14;
constexpr uint8_t CLOSING_CALL_AUCTION_START_MINUTE = 57;
constexpr uint8_t CLOSING_CALL_AUCTION_END_HOUR = 15;
constexpr uint8_t CLOSING_CALL_AUCTION_END_MINUTE = 0;
} // namespace TradingSession

// LOB Feature depth levels configuration
inline constexpr size_t LOB_FEATURE_DEPTH_LEVELS = 10; // Number of depth levels to maintain (one-side)

// Effective TOB filter parameters
namespace EffectiveTOBFilter {
constexpr uint32_t MIN_TIME_INTERVAL_MS = 1000; // Minimum time interval in milliseconds for effective TOB update
}

//========================================================================================
// CONSTANTS AND TYPES
//========================================================================================

// LOB reconstruction engine configuration
static constexpr size_t EXPECTED_QUEUE_SIZE = 128;
static constexpr size_t CACHE_LINE_SIZE = 64;
static constexpr float HASH_LOAD_FACTOR = 0.4f; // Ultra-low load factor

// Core types
using Price = uint16_t;
using Quantity = int32_t; // Supports negative quantities for deduction model
using OrderId = uint32_t;

static constexpr uint32_t PRICE_RANGE_SIZE = static_cast<uint32_t>(UINT16_MAX) + 1; // 0-65535

// 设计理念: 抵扣模型 + Order迁移机制 + 增量depth维护
// --------------------------------------------------------------------------------
// - TAKER成交价是最终真相, MAKER挂价可能不准确
// - 不要尝试维护完全准确的TOB, TOB用最近的成交价定义, 如果吃空则顺延
// - 所有Order都存在于某个Level中 (包括Level[0]特殊档位)
// - Order携带flags标记状态, 支持在Level间迁移
// - 通过order_lookup_全局索引实现O(1)定位和迁移
// - 通过类似Cbuffer/Dequeue结构, 动态维护depth多档信息
//
//========================================================================================
// CORE DATA STRUCTURES
//========================================================================================
//
// 1. order_lookup_: unordered_map<OrderId, Location>
//    - O(1)全局索引, 任何Order都能快速定位到(Level*, index)
//    - 支持Order在不同Level间迁移后更新Location
//
// 2. price_levels_: unordered_map<Price, Level*>
//    - 价格到Level的映射, 快速找到任意价格档位
//    - Level[0]是特殊档位, 存放price=0的订单
//
// 3. Level: 价格档位, 内部维护Order队列
//    - vector<Order*> orders: FIFO队列 (时间优先)
//    - Quantity net_quantity: 缓存的总量 (正=买压, 负=卖压)
//    - operations: add(), remove(), refresh_total()
//
// 4. Order: 订单对象 (在order_memory_pool_中分配)
//    - Quantity qty: 有符号数量 (正=买方增加/卖方减少, 负=买方减少/卖方增加)
//    - OrderId id: 订单唯一标识
//    - OrderFlags flags: 订单状态标记
//
// Level[0] 特殊档位:
// --------------------------------------------------------------------------------
// - 存放price=0的订单: SPECIAL_MAKER, ZERO_PRICE, UNKNOWN等
// - 数量极少(<0.1%总订单数), 通常<10个订单
// - O(n)遍历开销可忽略
// - 统一接口: Level[0]和其他Level使用相同的操作(add/remove/move)
//
//========================================================================================
// CORNER CASES SUMMARY
//========================================================================================
//
// 交易所逐笔数据存在多种corner cases (总计~5% orders):
//
// 1. OUT_OF_ORDER (乱序到达) - ~2-5% - TAKER/CANCEL先于MAKER到达, 预创建Order后抵扣
// 2. CALL_AUCTION (集合竞价) - 9:15-9:30, 14:57-15:00 - MAKER挂价≠撮合价, TAKER到达时迁移
// 3. SPECIAL_MAKER (特殊挂单) - ~1-2% - 市价单('1')/本方最优('U')等price=0订单, 存Level[0]后迁移
// 4. ZERO_PRICE_CANCEL (深交所零价格撤单) - ~5-10% of cancels - 深交所撤单price=0, 存Level[0]待MAKER到达迁移
// 5. ANOMALY_MATCH (异常撮合) - rare - 连续竞价时MAKER挂价≠TAKER成交价, 按成交价迁移并标记
// 6. UNKNOWN (信息不足) - temporary - 无法确定订单类型的暂时状态, 等待后续数据澄清
//
//========================================================================================
// ORDER PROCESSING FLOW (3 ORDER TYPES)
//========================================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ 1. MAKER FLOW (挂单)                                                            │
// └─────────────────────────────────────────────────────────────────────────────────┘
//
// 触发: order_type == MAKER
// 特点: 永不进入循环,单次lookup → order_upsert → 返回
//
// 流程 (99%+):
//   1. lookup(order_id) → not found
//   2. order_upsert(创建Order, flags=NORMAL, 放置在Level[price])
//      └─ level->add(order) → level.net_quantity += qty
//      └─ visibility: if (!bitmap.test(price)) → bitmap.set(price)
//   3. return
//
// 特殊情况 (1%-):
//   - OUT_OF_ORDER: order已存在 → 迁移价格 + 补充qty
//      └─ order_move_to_price() → 更新 old_level + new_level visibility
//   - CALL_AUCTION: in_call_auction=true → flags=CALL_AUCTION
//   - SPECIAL_MAKER: price=0 → 放置在Level[0]
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ 2. TAKER FLOW (成交)                                                            │
// └─────────────────────────────────────────────────────────────────────────────────┘
//
// 触发: order_type == TAKER
// 交易所差异:
//   - SSE: 集合竞价撮合期(9:25-9:30, 14:57-15:00)双边,连续竞价(9:30-14:57)单边
//   - SZSE: 全天双边
//
// 核心函数: process_taker_side(order, order_id, delta)
//   - order_id: 对手方订单ID
//   - delta: bid侧负值, ask侧正值
//   - 返回: true=完全消耗, false=部分消耗或新创建
//
// ─────────────────────────────────────────────────────────────────────────────────
// 2.1 双边撮合
// ─────────────────────────────────────────────────────────────────────────────────
// 触发: need_bilateral && bid_id != 0 && ask_id != 0
//
// 流程 (99%+):
//   1. consumed_bid = process_taker_side(order, bid_id, -volume)
//      → lookup(bid_id) → order_upsert(抵扣qty) → 返回是否完全消耗
//      └─ level.net_quantity -= volume
//      └─ visibility: 检查 level.has_visible_quantity()
//         - 仍有挂单(≠0): 无操作
//         - 变空(=0): bitmap.clear(price)
//   2. consumed_ask = process_taker_side(order, ask_id, +volume)
//      → lookup(ask_id) → order_upsert(抵扣qty) → 返回是否完全消耗
//      └─ visibility: 同上
//   3. update_tob(is_active_bid, consumed_bid, price)
//
// 特殊情况 (1%-):
//   - 价格不匹配 → order_move_to_price() + flags=ANOMALY_MATCH
//      └─ 更新 old_level + new_level visibility
//   - OUT_OF_ORDER → 预创建Order于Level[0]
//
// ─────────────────────────────────────────────────────────────────────────────────
// 2.2 单边撮合
// ─────────────────────────────────────────────────────────────────────────────────
// 触发: !need_bilateral || 只有一个ID != 0
//
// 流程 (99%+):
//   1. order_extract_params(order) → (target_id, delta_qty, is_bid)
//   2. consumed = process_taker_side(order, target_id, delta_qty)
//      → lookup(target_id) → order_upsert(抵扣qty) → 返回是否完全消耗
//      └─ visibility: 检查 level.has_visible_quantity()
//         - 仍有挂单(≠0): 无操作
//         - 变空(=0): bitmap.clear(price)
//   3. update_tob(is_bid, consumed, price)
//
// 特殊情况 (1%-): 与双边相同
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ 3. CANCEL FLOW (撤单)                                                           │
// └─────────────────────────────────────────────────────────────────────────────────┘
//
// 触发: order_type == CANCEL
// 特点: 复用process_taker_side(),但不更新TOB
//
// 流程 (99%+):
//   1. order_extract_params(order) → (target_id, delta_qty, is_bid)
//   2. consumed = process_taker_side(order, target_id, delta_qty)
//      → lookup(target_id) → order_upsert(减少qty) → 返回是否完全消耗
//      └─ level.net_quantity -= qty
//      └─ visibility: 检查 level.has_visible_quantity()
//         - 仍有挂单(≠0): 无操作
//         - 变空(=0): bitmap.clear(price)
//   3. return (不更新TOB)
//
// 特殊情况 (1%-):
//   - ZERO_PRICE_CANCEL: price=0撤单 → 预创建于Level[0]
//   - SPECIAL_MAKER撤单: 撤销Level[0]的订单
//   - OUT_OF_ORDER → 预创建Order于Level[0]
//
//========================================================================================
// SPECIAL HANDLING: CALL AUCTION (集合竞价)
//========================================================================================
//
// 9:15-9:25 集合竞价期 (in_call_auction_ = true, in_matching_period_ = false):
//   - MAKER: 创建到Level[报价], flags=CALL_AUCTION
//   - TAKER: 暂无 (集合竞价期不产生成交)
//
// 9:25-9:30 集合竞价撮合期 (in_call_auction_ = true, in_matching_period_ = true):
//   - MAKER: 继续创建到Level[报价], flags=CALL_AUCTION
//   - TAKER: 按统一撮合价成交
//     → SSE: 双边撮合 (handle_bilateral_matching)
//     → 找到对手MAKER, 若价格不匹配则迁移
//     → 抵扣qty, 更新flags=NORMAL
//
// 9:30:00 连续竞价开始 (in_call_auction_ = false, in_matching_period_ = false):
//   - flush_call_auction_flags(): 遍历所有Order, 清除flags=CALL_AUCTION
//   - 剩余MAKER按原挂单价继续挂牌 (已在Level[挂单价], 无需搬运)
//
// 14:57-15:00 收盘集合竞价 (in_call_auction_ = true, in_matching_period_ = true):
//   - SSE: 双边撮合 (handle_bilateral_matching), 与开盘集合竞价处理相同
//   - SZSE: 全天双边撮合, 收盘集合竞价与其他时段处理相同
//
//========================================================================================
// PERFORMANCE CHARACTERISTICS
//========================================================================================
//
// Hot Path (95%+ orders): 正常连续竞价, 价格匹配
// --------------------------------------------------------------------------------
// MAKER:  1x hash lookup (order_lookup_) + create Order     → O(1)
// TAKER:  1/2x hash lookup (order_lookup_) + deduct Order   → O(1/2)
// CANCEL: 1x hash lookup (order_lookup_) + deduct Order     → O(1)
//
// Cold Path (5%- orders): Corner cases, 需要迁移或特殊处理
// --------------------------------------------------------------------------------
// OUT_OF_ORDER:    1x hash + possible move                  → O(1)
// CALL_AUCTION:    1x hash + possible move                  → O(1)
// SPECIAL_MAKER:   1x hash + move from Level[0]             → O(1)
// ZERO_PRICE:      1x hash + move from Level[0]             → O(1)
// ANOMALY_MATCH:   1x hash + move + flag update             → O(1)
// Level[0] scan:   O(n) where n < 10 typically              → negligible
//
//========================================================================================
// N-档市场深度数据维护
//========================================================================================
//
// 订单(逐笔, 高频)+tob更新(时间过滤, 低频)驱动
// visible_price_bitmap_: 逐笔全局位图(逐笔): price → next/prev_visible(net_quantity!=0)_price index 映射
// LOB_feature_.depth_buffer_[2N]: (订单+时间):
//    CBuffer: [0]:卖N, [N-1]:卖1, [N]:买1, ..., [2N-1]:买N, 价格单调下降
//    数据单元: {Level* level_ptr}, 支持ring_buffer/dequeue通用API: pop, push, insert, erase, etc.
//    index -> price/volume(Level) 映射
//
// 更新流程:
//   1. 订单based(逐笔, 高频):
//          每个订单(哪怕是双边撮合的taker)只会最多trigger一个add/remove level操作
//          通过判断 depth_buffer_[0/-1].price 来判断价格是否在N-档范围内, 如果在, 用binary search做price -> index, 更新depth_buffer_
//   2. tob更新based(时间过滤, 低频):
//          tob 更新触发条件:
//          - best_bid_ < best_ask_
//          - best_bid_volume > 0 && best_ask_volume < 0
//          - curr_tick_ > next_depth_update_tick_
//          TOB更新晚于逐笔更新, 所以触发后, 只需要判断向左/右push几次就可以, 先通过bitmap_找到对应的price, Level, 再push_front/back

// Order flags enumeration - track order state and corner cases
enum class OrderFlags : uint8_t {
  NORMAL,        // Normal order (price and direction match, fully confirmed)
  UNKNOWN,       // Unknown type (insufficient information, temporary state)
  OUT_OF_ORDER,  // Out-of-order arrival (TAKER/CANCEL arrived before MAKER)
  CALL_AUCTION,  // Call auction order (price may not be final trade price)
  SPECIAL_MAKER, // Special MAKER (price=0, market order, etc.)
  ZERO_PRICE,    // Zero-price order (CANCEL with price=0, Shenzhen exchange)
  ANOMALY_MATCH  // Anomaly match (continuous trading, price/direction mismatch)
};

//========================================================================================
// CORE DATA STRUCTURES
//========================================================================================

// Ultra-compact order entry - cache-optimized with flags
struct alignas(16) Order {
  Quantity qty;
  OrderId id;
  uint32_t timestamp; // Creation timestamp (updated in DEBUG_ANOMALY_PRINT mode only)
  OrderFlags flags;   // Order state/type flags
  uint8_t padding[3]; // Explicit padding for alignment

  Order(Quantity q, OrderId i, uint32_t ts = 0, OrderFlags f = OrderFlags::NORMAL)
      : qty(q), id(i), timestamp(ts), flags(f) {}

  // Fast operations - always inline
  bool is_positive() const { return qty > 0; }
  bool is_depleted() const { return qty <= 0; }
  void subtract(Quantity amount) { qty -= amount; }
  void add(Quantity amount) { qty += amount; }
};

// Simple unified price level - no side field needed
struct alignas(CACHE_LINE_SIZE) Level {
  Price price;                    // Price level identifier
  Quantity net_quantity = 0;      // Cached sum of all quantities (can be negative)
  uint16_t order_count = 0;       // Fast size tracking
  uint16_t alignment_padding = 0; // Explicit padding for cache line alignment
  std::vector<Order *> orders;    // Pointers to orders at this price level

  explicit Level(Price p) : price(p) {
    orders.reserve(EXPECTED_QUEUE_SIZE);
  }

  // Move constructor (for deque reallocation optimization)
  Level(Level &&other) noexcept
      : price(other.price),
        net_quantity(other.net_quantity),
        order_count(other.order_count),
        orders(std::move(other.orders)) {}

  // Disable copy to prevent accidental expensive copies
  Level(const Level &) = delete;
  Level &operator=(const Level &) = delete;
  Level &operator=(Level &&) = delete;

  // High-performance order management
  HOT_INLINE void add(Order *order) {
    orders.push_back(order);
    ++order_count;
    net_quantity += order->qty;
  }

  HOT_INLINE void remove(size_t order_index) {
    assert(order_index < orders.size());
    Order *removed_order = orders[order_index];

    // Update cached total before removal
    net_quantity -= removed_order->qty;

    // Swap-and-pop for O(1) removal
    if (order_index != orders.size() - 1) {
      orders[order_index] = orders.back();
    }
    orders.pop_back();
    --order_count;
  }

  // Fast level state queries
  HOT_INLINE bool empty() const { return order_count == 0; }
  HOT_INLINE bool has_visible_quantity() const { return net_quantity != 0; }

  // Recalculate cached total from scratch
  void refresh_total() {
    net_quantity = 0;
    for (const Order *current_order : orders) {
      net_quantity += current_order->qty;
    }
  }
};

// Order location tracking (POD for fast construction)
struct Location {
  Level *level;
  size_t index;

  Location() = default;
  constexpr Location(Level *l, size_t i) noexcept : level(l), index(i) {}
};

// Fast identity hash for OrderId (uint32_t) - avoids std::hash overhead
struct OrderIdHash {
  HOT_INLINE size_t operator()(OrderId id) const noexcept {
    // Direct identity hash - OrderIds are already well-distributed
    return static_cast<size_t>(id);
  }
};

//========================================================================================
// LOB FEATURE STRUCTURE
//========================================================================================

// Forward declaration
struct Level;

// 订单簿逐笔特征流(用于高频因子计算)
struct LOB_Feature {
  uint8_t hour;        // 5bit
  uint8_t minute;      // 6bit
  uint8_t second;      // 6bit
  uint8_t millisecond; // 7bit (in 10ms)

  bool is_maker;   // 1bit
  bool is_taker;   // 1bit
  bool is_cancel;  // 1bit
  bool is_bid;     // 1bit - 0:ask 1:bid
  uint16_t price;  // 14bit - price in 0.01 RMB units
  uint32_t volume; // 22bit - in shares (expanded to support up to 4M shares)

  uint32_t all_bid_volume; // 22bit - volume of all bid orders in shares
  uint32_t all_ask_volume; // 22bit - volume of all bid orders in shares

  // N-level depth buffer (订单+时间双驱动):
  // CBuffer: [0]:卖N, [N-1]:卖1, [N]:买1, ..., [2N-1]:买N, 价格单调下降
  CBuffer<Level *, 2 * LOB_FEATURE_DEPTH_LEVELS> depth_buffer;
  bool depth_updated; // 低频条件过滤, 表示depth_buffer已更新, 可以进行相关特征计算
};
