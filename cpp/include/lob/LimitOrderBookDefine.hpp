#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <type_traits>
#include <utility>

#include "codec/L2_DataType.hpp"
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
#define DEBUG_BOOK_AS_AMOUNT 1      // 0: 股, 1: 1万元
#define DEBUG_ANOMALY_PRINT 1       // Print max unmatched order with creation timestamp

// Auto-disable dependent switches based on logical relationships
#if DEBUG_BOOK_PRINT == 0
#undef DEBUG_BOOK_AS_AMOUNT
#define DEBUG_BOOK_AS_AMOUNT 0
#undef DEBUG_ANOMALY_PRINT
#define DEBUG_ANOMALY_PRINT 0
#endif

//========================================================================================
// CONSTANTS AND TYPES
//========================================================================================

// LOB reconstruction engine configuration
static constexpr size_t CACHE_LINE_SIZE = 64;

// Core types
using Price = uint16_t;
using Quantity = int32_t; // Supports negative quantities for deduction model
using OrderId = uint32_t;

static constexpr uint32_t PRICE_RANGE_SIZE = static_cast<uint32_t>(UINT16_MAX) + 1; // 0-65535

// 设计理念: 抵扣模型 + Order迁移机制 + 增量depth维护
// --------------------------------------------------------------------------------
// - TAKER成交价是最终真相, MAKER挂价可能不准确
// - 不要尝试维护完全准确的TOB, TOB用最近的成交价定义, 如果吃空则顺延
// - 所有Order都归属于某个Level (包括Level[0]特殊档位), Order 自带 Level*
// - Order携带flags标记状态, 支持在Level间迁移 (改 Level* + 两档聚合量)
// - 通过 order_table_ (开放寻址, 委托就地存槽) 实现 O(1) 定位, 建单/删除不另占探针
// - 通过类似Cbuffer/Dequeue结构, 动态维护depth多档信息
//
//========================================================================================
// CORE DATA STRUCTURES
//========================================================================================
//
// 1. order_table_: OrderTable (OrderId → Order, 线性探测, 墓碑删除)
//    - 委托本体就在槽里, 查到 id 即查到委托; Order* 整日稳定
//    - 表长按当日事件数 reserve, 换日只清用过的前缀
//
// 2. price_levels_: array<Level*, PRICE_RANGE_SIZE>
//    - 价格到Level的映射, 快速找到任意价格档位
//    - Level[0]是特殊档位, 存放price=0的订单
//
// 3. Level: 价格档位聚合 (16 字节, 不持有委托队列)
//    - Quantity net_quantity: 在档 qty 之和 (正=买压, 负=卖压)
//    - uint32_t order_count: 在档委托数
//    - operations: attach(q), detach(q), adjust(delta)
//
// 4. Order: 委托 (32 字节, 存于 order_table_ 槽内)
//    - Quantity qty: 有符号数量 (正=买方增加/卖方减少, 负=买方减少/卖方增加)
//    - OrderId id: 订单唯一标识 (即哈希键)
//    - Level *level: 所在档位
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
// 特点: 永不进入循环, find_or_insert 一次探针既查又占槽 → order_upsert 填槽 → 返回
//
// 流程 (99%+):
//   1. find_or_insert(order_id) → 新槽 (inserted)
//   2. order_upsert(填 Order, flags=NORMAL, 归属 Level[price])
//      └─ level->attach(qty) → level.net_quantity += qty
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
// 核心函数: process_taker_side(order, order_id, delta, loc)
//   - order_id: 对手方订单ID
//   - delta: bid侧负值, ask侧正值
//   - loc: 该 id 的簿内位置, process_impl 入口 resolve_order_locations 查好 (快照与簿更新共用一次 lookup)
//   - 返回: true=完全消耗, false=部分消耗或新创建
//
// ─────────────────────────────────────────────────────────────────────────────────
// 2.1 双边撮合
// ─────────────────────────────────────────────────────────────────────────────────
// 触发: is_bilateral: (SZSE || SSE 撮合期) && bid_id != 0 && ask_id != 0
// 被动方 = id 较小者 (先到), 主动方 = id 较大者
//
// 流程 (99%+):
//   1. 主动方在簿 (深交所委托记录先到) → process_taker_side(order, active_id, ∓volume, loc[active])
//      不在簿 (乱序) → 只进主动单累计寄存器, 不建占位单 (占位单会原地抵掉被动侧扣减, 成交不耗深度)
//   2. consumed_passive = process_taker_side(order, passive_id, ±volume, loc[passive])
//      → order_upsert(抵扣qty) → 返回是否完全消耗
//      └─ level.net_quantity -= volume
//      └─ visibility: 检查 level.has_visible_quantity()
//         - 仍有挂单(≠0): 无操作
//         - 变空(=0): bitmap.clear(price)
//   3. update_tob(is_active_bid, consumed_passive, price)
//
// 特殊情况 (1%-):
//   - 价格不匹配 → order_move_to_price() + flags=ANOMALY_MATCH
//      └─ 更新 old_level + new_level visibility
//   - OUT_OF_ORDER (被动方查无) → 预创建Order于Level[0]
//
// ─────────────────────────────────────────────────────────────────────────────────
// 2.2 单边撮合
// ─────────────────────────────────────────────────────────────────────────────────
// 触发: !is_bilateral (沪市连续竞价 / 只有一个ID != 0)
// 主动方不在簿 (沪市委托记录在其成交之后才到, 残量才入簿), 不查, 只进累计寄存器
//
// 流程 (99%+):
//   1. order_extract_params(order) → (target_id, delta_qty, is_bid)
//   2. consumed = process_taker_side(order, target_id, delta_qty, loc[target])
//      → order_upsert(抵扣qty) → 返回是否完全消耗
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
//   2. consumed = process_taker_side(order, target_id, delta_qty, loc[target])
//      → order_upsert(减少qty) → 返回是否完全消耗
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
// MAKER:  1x probe (find_or_insert) + fill Order in place   → O(1)
// TAKER:  1/2x probe (find) + deduct Order                  → O(1/2)   全量消耗: 就地墓碑, 零额外探针
// CANCEL: 1x probe (find) + deduct Order                    → O(1)
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

// 本笔事件某一侧的委托角色 (LOB_Feature::ord_role), 委托维度算子的分支判据.
enum class OrderRole : uint8_t {
  None,      // 该侧无 id, 或该侧是查无的被动方 (乱序到达)
  Resting,   // 在簿委托: ord_orig 是申报量 (flags ∉ {OUT_OF_ORDER, ZERO_PRICE} 才可信), ord_rest 是处理前余量, ord_tick 是挂单时刻
  Aggressor, // 不在簿的主动方: ord_orig 恒 0 (申报量未知), ord_rest 是本笔之前它连续成交的累计量 (首笔 0), ord_tick 恒 0
};

//========================================================================================
// CORE DATA STRUCTURES
//========================================================================================

// 价格档位: 只记聚合量与计数, 不持有委托队列 —— 委托本体在 OrderTable 里, 每条自带 Level*.
// 抵扣模型不需要队内位置 (无时间优先撮合), 去掉队列后档位就是 16 字节, 相邻档同缓存行, 深度走扫更省.
struct alignas(16) Level {
  Price price; // 档位下标 (非绝对价, 见 LOB_Feature::price_base)
  uint16_t padding_ = 0;
  Quantity net_quantity = 0; // 在档委托 qty 之和 (SIGNED: + 买压, - 卖压; 抵扣模型下可反号)
  uint32_t order_count = 0;  // 在档委托数 (涨跌停封板可达十万级, 16 位不够)

  explicit Level(Price p) : price(p) {}

  HOT_INLINE void attach(Quantity q) {
    ++order_count;
    net_quantity += q;
  }
  HOT_INLINE void detach(Quantity q) {
    assert(order_count > 0 && "Level::detach: 空档");
    --order_count;
    net_quantity -= q;
  }
  HOT_INLINE void adjust(Quantity delta) { net_quantity += delta; }

  HOT_INLINE bool empty() const { return order_count == 0; }
  HOT_INLINE bool has_visible_quantity() const { return net_quantity != 0; }
};
static_assert(sizeof(Level) == 16, "Level 应为 16 字节 (4 档一缓存行)");

// 委托: 就地存在 OrderTable 的槽里, id 即哈希键. 没有独立的池与位置表 —— 查到 id 就查到了委托本体.
// orig_qty / timestamp 是委托维度与订单生命周期算子的全部输入 (算子不自建哈希表):
//   orig_qty  建单时的量 (绝对值, 股). MAKER 建单 = 真实申报量; 占位单 (OUT_OF_ORDER,
//             乱序到达) = 首笔量, 不是申报量 —— 消费方按 flags 区分, 见 LOB_Feature.
//   timestamp 建单时刻, 用于挂单→成交 / 挂单→撤单 的用时
//   level     所在档位 (nullptr = 槽刚被 OrderTable 预留、委托尚未建立)
// 32 字节: 两条委托一缓存行, 线性探测的下一槽多半已在行内.
struct alignas(32) Order {
  OrderId id;             // 0 = 空槽, kTomb = 墓碑 (见 OrderTable)
  Quantity qty;           // SIGNED: + 买, - 卖 (抵扣模型下可反号)
  uint32_t timestamp;     // Creation timestamp (hour<<24|minute<<16|second<<8|ms10)
  uint32_t orig_qty : 28; // 建单量绝对值 (股; 交易所单笔申报上限 100 万股)
  OrderFlags flags : 4;   // Order state/type flags
  Level *level;
  uint64_t reserved_; // 凑 32 字节, 未用
};
static_assert(sizeof(Order) == 32, "Order 必须定宽 32 字节");
static_assert(std::is_trivial_v<Order>, "OrderTable 用 memset 建/清槽, Order 必须是平凡类型");

// 委托表: OrderId → Order, 开放寻址 + 线性探测, 委托就地存槽.
//   查找 = 1 次探针 (通常 1 条缓存行); 建单 = 同一次探针里完成 (find_or_insert); 删除 = 就地打墓碑, 零探针.
//   墓碑不搬槽 → Order* 整日稳定: process_impl 入口查好的指针可以跨 compute_and_store 复用.
//   表长按当日事件数定 (reserve: ≥ 2× 事件数 → 计墓碑的负载 ≤ 0.5, 每笔至多建 1 条委托), 小票的表小到
//   能进 L2; clear 只清用过的前缀 —— 换日成本与当日工作量成比例, 不再是固定的整表 memset.
//   id 直接取模做桶号: 沪市委托号按证券连续, 相邻委托落相邻槽, 零冲突; 深市按通道连续, 对单只股票近似均匀.
class OrderTable {
public:
  static constexpr OrderId kTomb = ~OrderId{0};
  static constexpr size_t kMinSlots = 1024;

  explicit OrderTable(size_t capacity_orders) { grow(slots_for(capacity_orders)); }
  ~OrderTable() { operator delete[](slots_, std::align_val_t{CACHE_LINE_SIZE}); }
  OrderTable(const OrderTable &) = delete;
  OrderTable &operator=(const OrderTable &) = delete;

  // 换日: 清用过的前缀 (O(当日表长)), 表长回到已分配上限 (未 reserve 的缺省)
  void clear() {
    std::memset(static_cast<void *>(slots_), 0, (mask_ + 1) * sizeof(Order));
    mask_ = alloc_ - 1;
    size_ = 0;
    occupied_ = 0;
  }

  // 当日事件数已知时调用 (clear 之后、首笔之前): 表长 = next_pow2(2·events); 超过已分配则扩容 (奢侈预留同 BumpPool)
  void reserve(size_t events) {
    assert(occupied_ == 0 && "OrderTable::reserve: 表非空");
    const size_t need = slots_for(events);
    if (need > alloc_)
      grow(need);
    mask_ = need - 1;
  }

  [[nodiscard]] HOT_INLINE Order *find(OrderId id) {
    assert(id != 0 && id != kTomb && "OrderTable::find: 非法 id");
    for (size_t i = id & mask_;; i = (i + 1) & mask_) {
      Order &o = slots_[i];
      if (o.id == id)
        return &o;
      if (o.id == 0)
        return nullptr;
    }
  }

  // 查到则返回 {现有, false}; 否则在探测路上第一个墓碑或空槽建槽, 返回 {新槽, true}:
  // 新槽只写了 id, level == nullptr, 其余字段由 order_upsert 填.
  [[nodiscard]] HOT_INLINE std::pair<Order *, bool> find_or_insert(OrderId id) {
    assert(id != 0 && id != kTomb && "OrderTable::find_or_insert: 非法 id");
    Order *tomb = nullptr;
    for (size_t i = id & mask_;; i = (i + 1) & mask_) {
      Order &o = slots_[i];
      if (o.id == id)
        return {&o, false};
      if (o.id == 0) {
        Order *slot = tomb ? tomb : &o;
        // 负载守卫: reserve 按 2× 事件数定表, 这里恒成立; 未 reserve (实盘) 时提前于探测退化处失败
        assert(occupied_ * 2 <= mask_ + 1 && "OrderTable: 负载超 0.5, 事件数超出 reserve");
        occupied_ += (slot == &o);
        ++size_;
        slot->id = id;
        slot->level = nullptr;
        return {slot, true};
      }
      if (o.id == kTomb && !tomb)
        tomb = &o;
    }
  }

  HOT_INLINE void erase(Order *o) {
    assert(o->id != 0 && o->id != kTomb && "OrderTable::erase: 槽非存活");
    o->id = kTomb;
    --size_;
  }

  [[nodiscard]] size_t size() const { return size_; }
  [[nodiscard]] size_t slots() const { return mask_ + 1; }

  // 遍历存活委托 (9:30 清竞价标记 / 调试打印), O(当日表长)
  template <typename F>
  void for_each(F &&f) {
    for (size_t i = 0; i <= mask_; ++i) {
      Order &o = slots_[i];
      if (o.id != 0 && o.id != kTomb)
        f(o);
    }
  }

private:
  Order *slots_ = nullptr;
  size_t alloc_ = 0;    // 已分配槽数 (2 的幂)
  size_t mask_ = 0;     // 当日在用槽数 - 1
  size_t size_ = 0;     // 存活委托
  size_t occupied_ = 0; // 存活 + 墓碑 (清表前只增不减)

  static size_t slots_for(size_t events) {
    size_t n = kMinSlots;
    while (n < events * 2)
      n <<= 1;
    return n;
  }

  void grow(size_t n) {
    assert(occupied_ == 0 && "OrderTable::grow: 只能在空表时扩容");
    operator delete[](slots_, std::align_val_t{CACHE_LINE_SIZE});
    slots_ = static_cast<Order *>(operator new[](n * sizeof(Order), std::align_val_t{CACHE_LINE_SIZE}));
    std::memset(static_cast<void *>(slots_), 0, n * sizeof(Order));
    alloc_ = n;
    mask_ = n - 1;
  }
};

//========================================================================================
// LOB FEATURE STRUCTURE
//========================================================================================

// Forward declaration
struct Level;

// 订单簿逐笔特征流(用于高频因子计算)
struct LOB_Feature {
  // =================================================================================================
  // =========================================[高频逐笔更新]===========================================

  // 当前订单(增删改成交)时间戳
  uint8_t hour = 0;        // 5bit
  uint8_t minute = 0;      // 6bit
  uint8_t second = 0;      // 6bit
  uint8_t millisecond = 0; // 7bit (in 10ms)

  // 当前市场状态,订单类型和方向
  L2::MarketState market_state = L2::MarketState::CLOSED;
  L2::OrderType order_type = L2::OrderType::MAKER;
  L2::OrderDirection order_dir = L2::OrderDirection::BID;

  // 当前订单的价格和数量
  float price = 0.0;   // 元, 绝对价 (已还原, 消费方直接用)
  uint32_t volume = 0; // 股

  // depth_buffer 里 Level::price 是档位下标而非绝对价, 还原成分要加上这个基准.
  // 低价股恒为 0. 来源是 .bin 的文件头, 见 L2_DataType.hpp 的 kPriceIndexRange.
  uint32_t price_base = 0;

  // 全市场挂单量
  uint32_t all_bid_volume = 0; // 22bit - volume of all bid orders in shares
  uint32_t all_ask_volume = 0; // 22bit - volume of all ask orders in shares

  // 集合竞价预撮合 (update_depth 竞价分支产出, 与 depth_buffer 同节流频率):
  //   ref_price = 最大成交量价位 (元); 仅竞价期且簿交叉 (bid1 ≥ ask1) 时 > 0, 其余恒 0
  //   → 消费方 (MidPrice/MicroPrice/GUI) 单分支 ref > 0 判定, 无 NaN (fast-math 安全)
  float auction_ref_price = 0.0f;  // 元, 预撮合参考价 (0 = 非竞价/未交叉/单边簿)
  int32_t auction_matched_qty = 0; // 股, 参考价位的虚拟匹配量
  int32_t auction_imbalance = 0;   // 股, 参考价位的未匹配量 (SIGNED: + 买剩, - 卖剩)

  // 本笔事件两侧的委托状态 (下标 0=bid 1=ask), 委托维度 / 订单生命周期算子的唯一输入面 ——
  // 算子不自建 id 哈希表 (那会把 per-worker 的簿开销变成 per-asset ×5000).
  // 快照取在 LOB 更新 *之前* (与 depth_buffer / price / volume 同相位, 见 process_impl 的调用序),
  // 所以 ord_rest 是本笔处理前的量 —— 要"含本笔"由消费方自己加 volume.
  //   ord_role 该侧角色, 分支判据全看它 (None / Resting / Aggressor, 见 OrderRole)
  //   ord_orig 申报量 (股; 仅 Resting 有值. ord_flag ∈ {OUT_OF_ORDER, ZERO_PRICE} 的占位单记的是首笔成交/撤单量而非申报量, 不可信;
  //            迟到的 MAKER 会改正并换 flag, 其余 flag (含 ANOMALY_MATCH / SPECIAL_MAKER) 不动 orig, 可信)
  //   ord_rest 处理前量 (股; Resting = 未成交余量, Aggressor = 本笔之前连续成交的累计量)
  //   ord_tick 挂单时刻 (仅 Resting; 与本结构 hour/minute/second/millisecond 同编码 h<<24|m<<16|s<<8|ms10)
  //   ord_flag 建单时的 OrderFlags (仅 Resting)
  // 主动方已成交量: Resting (深交所, 委托记录先到) = ord_orig − ord_rest; Aggressor (沪市) = ord_rest.
  // 语义速查 (order_type, order_dir):
  //   (MAKER,  BID) [0]=本次新挂的买委托 (簿里还没有 → None, 建单在 update_lob 里)
  //   (MAKER,  ASK) [1]=同上, 卖侧
  //   (CANCEL, BID) [0]=被撤的买委托 (Resting)          (CANCEL, ASK) [1]=被撤的卖委托 (Resting)
  //   (TAKER,  BID) 主动买: [0]=主动买单 (Resting / Aggressor), [1]=被吃的卖委托 (Resting)
  //   (TAKER,  ASK) 主动卖: [0]=被吃的买委托 (Resting), [1]=主动卖单 (Resting / Aggressor)
  OrderRole ord_role[2] = {OrderRole::None, OrderRole::None};
  uint32_t ord_orig[2] = {};
  uint32_t ord_rest[2] = {};
  uint32_t ord_tick[2] = {};
  OrderFlags ord_flag[2] = {OrderFlags::NORMAL, OrderFlags::NORMAL};

  // =================================================================================================
  // =========================================[低频定时更新]===========================================

  // 时间驱动(低频): 新订单满足时间间隔(L2_MIN_TIME_INTERVAL_MS)时批量重建
  CBuffer<Level *, 2 * L2::LOB_DEPTH> depth_buffer; // CBuffer: [0]:卖N, [N-1]:卖1, [N]:买1, ..., [2N-1]:买N, 价格单调下降
  bool depth_updated = false;                       // 标志位, depth_buffer已更新时为true, 用来采样depth_buffer
};
