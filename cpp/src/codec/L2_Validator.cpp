#include "codec/L2_Validator.hpp"
#include "codec/binary_encoder_L2.hpp"

#include <algorithm>
#include <bit>
#include <cassert>

namespace L2 {

// ============================================================================
// 开放寻址表
// ============================================================================
//
// 交易所委托号是通道内递增的稠密序号, 直接取低位会让同一批订单挤在相邻槽里,
// 所以用 Fibonacci 散列取高位.
static constexpr uint64_t kHashMultiplier = 0x9E3779B97F4A7C15ull;

void Validator::reset(size_t expected_makers) {
  // 负载因子上限 2/3 —— 线性探测在这个水位平均探测长度还在 2 附近, 再往下压
  // 只会让表白白翻倍: 表按当日实际委托数开, 但容量只增不减 (与 encoder 的
  // 中间缓冲同策略), 每个 worker 迟早会撞上一个二十万笔的活跃标的.
  size_t capacity = 1024;
  while (capacity * 2 < expected_makers * 3)
    capacity <<= 1;

  // assign 在容量够时不重新分配, 只重填空标记 — 与 encoder 的中间缓冲同策略
  table_.assign(capacity, Slot{kEmptyId, 0, 0});
  mask_ = capacity - 1;
  shift_ = 64u - static_cast<unsigned>(std::countr_zero(capacity));
}

Validator::Slot *Validator::lookup(uint64_t id) {
  size_t pos = static_cast<size_t>((id * kHashMultiplier) >> shift_);
  for (;;) {
    Slot &slot = table_[pos];
    if (slot.id == id)
      return &slot;
    if (slot.id == kEmptyId)
      return nullptr;
    pos = (pos + 1) & mask_;
  }
}

Validator::Slot *Validator::insert(uint64_t id, bool &existed) {
  size_t pos = static_cast<size_t>((id * kHashMultiplier) >> shift_);
  for (;;) {
    Slot &slot = table_[pos];
    if (slot.id == id) {
      existed = true;
      return &slot;
    }
    if (slot.id == kEmptyId) {
      existed = false;
      slot.id = id;
      slot.remaining = 0;
      slot.side = 0;
      return &slot;
    }
    pos = (pos + 1) & mask_;
  }
}

// ============================================================================
// 判定
// ============================================================================

// 撤单编码: 委托表里是"委托类型 D", 成交表里是"成交代码 C". 两种编码互斥 ——
// 上报 D 的数据源成交代码恒为 '0', 上报 C 的数据源委托类型里没有 D —— 所以
// 按记录自身判断就够, 不需要知道是哪个交易所.
static inline bool is_cancel_order(char order_type) {
  return order_type == 'D' || order_type == 'd';
}

static inline bool is_cancel_trade(char trade_code) {
  return trade_code == 'C' || trade_code == 'c';
}

// 时间字段能否原样装进 Order. 比位宽更严: 位宽只挡得住 hour>31, 而 25:00:00
// 这种同样是坏数据, 且装进 5bit 后看不出异常.
static inline bool clock_invalid(uint32_t time_int) {
  const uint32_t sec = (time_int / 1000) % 100;
  const uint32_t min = (time_int / 100000) % 100;
  const uint32_t hour = time_int / 10000000;
  return hour > 23 || min > 59 || sec > 59;
}

void Validator::run(const std::vector<CSVOrder> &orders,
                    const std::vector<CSVTrade> &trades,
                    const MarketSummary &market,
                    ValidationReport &out) {
  out = ValidationReport{};
  reset(orders.size());

  // ---- 挂单入表 ----
  //
  // 委托表里夹着撤单时 (委托类型 D), 必须等挂单全部入表后再处理 —— 撤单在
  // 时间上总是晚于它的挂单, 但判定不该依赖文件内的行序.
  // 档位窗口要覆盖所有会落盘的价格, 所以委托和成交两张表都统计, 且在任何
  // continue 之前 —— 被判为 lob_unusable 的记录照样会写进 .bin.
  uint32_t price_min = UINT32_MAX;
  uint32_t price_max = 0;

  bool has_order_cancels = false;
  for (const CSVOrder &order : orders) {
    if (order.price == 0 && order.volume == 0)
      continue; // 数据源占位行

    if (order.price != 0) {
      price_min = std::min(price_min, order.price);
      price_max = std::max(price_max, order.price);
    }

    if (order.price > PRICE_BOUND || order.volume > VOLUME_BOUND ||
        order.exchange_order_id > ORDER_ID_BOUND || clock_invalid(order.time))
      ++out.field_overflow;

    // price=0 且 volume>0 是市价单/本方最优单, 正常记录, 不在此列
    if (order.volume == 0 || order.exchange_order_id == 0) {
      ++out.lob_unusable;
      continue;
    }

    if (is_cancel_order(order.order_type)) {
      has_order_cancels = true;
      continue;
    }

    bool existed = false;
    Slot *slot = insert(order.exchange_order_id, existed);
    if (existed) {
      ++out.dup_maker;
      continue;
    }
    slot->remaining = static_cast<int64_t>(order.volume);
    slot->side = (order.order_side == 'S' || order.order_side == 's') ? 1 : 0;
  }

  // ---- 委托表里的撤单 ----
  if (has_order_cancels) {
    for (const CSVOrder &order : orders) {
      if (order.volume == 0 || order.exchange_order_id == 0 ||
          !is_cancel_order(order.order_type))
        continue;

      Slot *slot = lookup(order.exchange_order_id);
      if (slot == nullptr) {
        ++out.cancel_unresolved;
        continue;
      }
      slot->remaining -= static_cast<int64_t>(order.volume);
    }
  }

  // ---- 成交表 (成交代码 C 的撤单也走这里) ----
  uint64_t cum_volume = 0;
  uint64_t cum_turnover_fen = 0;
  uint32_t high = 0;
  uint32_t low = 0;
  uint32_t last_price = 0;
  uint32_t last_time = 0;
  size_t trade_count = 0;

  for (const CSVTrade &trade : trades) {
    if (trade.price == 0 && trade.volume == 0)
      continue; // 数据源占位行

    if (trade.price != 0) {
      price_min = std::min(price_min, trade.price);
      price_max = std::max(price_max, trade.price);
    }

    if (trade.price > PRICE_BOUND || trade.volume > VOLUME_BOUND ||
        trade.bid_order_id > ORDER_ID_BOUND || trade.ask_order_id > ORDER_ID_BOUND ||
        clock_invalid(trade.time))
      ++out.field_overflow;

    // 撤单成交只填单侧 id, 所以要求"至少一侧非零"而非"两侧都非零"
    if (trade.volume == 0 || (trade.bid_order_id == 0 && trade.ask_order_id == 0)) {
      ++out.lob_unusable;
      continue;
    }

    Slot *bid = (trade.bid_order_id != 0) ? lookup(trade.bid_order_id) : nullptr;
    Slot *ask = (trade.ask_order_id != 0) ? lookup(trade.ask_order_id) : nullptr;

    if (is_cancel_trade(trade.trade_code)) {
      Slot *slot = (trade.bid_order_id != 0) ? bid : ask;
      if (slot == nullptr) {
        ++out.cancel_unresolved;
        continue;
      }
      slot->remaining -= static_cast<int64_t>(trade.volume);
      continue;
    }

    if (bid == nullptr && ask == nullptr)
      ++out.trade_both_missing;
    else if (bid == nullptr || ask == nullptr)
      ++out.trade_side_missing;

    if (bid != nullptr)
      bid->remaining -= static_cast<int64_t>(trade.volume);
    if (ask != nullptr)
      ask->remaining -= static_cast<int64_t>(trade.volume);

    cum_volume += trade.volume;
    cum_turnover_fen += static_cast<uint64_t>(trade.price) * trade.volume;
    if (trade.price > high)
      high = trade.price;
    if (low == 0 || trade.price < low)
      low = trade.price;
    if (trade.time >= last_time) {
      last_time = trade.time;
      last_price = trade.price;
    }
    ++trade_count;
  }

  // ---- 委托流完不完整 ----
  //
  // 无成交的一天推断不出上游有没有省略主动单, 一律按不完整处理 —— 宁可少判
  // 几条, 不能凭空拦下好数据.
  out.strict_ledger =
      trade_count > 0 &&
      static_cast<int64_t>(out.trade_side_missing) * 100 <=
          static_cast<int64_t>(trade_count) * kStrictLedgerRatioPct;

  // ---- 收盘残余挂单 ----
  int64_t bid_residual = 0;
  int64_t ask_residual = 0;
  for (const Slot &slot : table_) {
    if (slot.id == kEmptyId)
      continue;
    if (slot.remaining < 0) {
      ++out.over_consumed;
      continue;
    }
    if (slot.side == 0)
      bid_residual += slot.remaining;
    else
      ask_residual += slot.remaining;
  }

  // ---- LOB 档位窗口 ----
  //
  // 低价股 (最高价 < 655.35 元) 的 base 取 0, 档位下标就是绝对价本身, 与扩位
  // 之前逐位一致; 只有高价股才平移. 平移后最低价落在 [guard, 2*guard) 上, 既
  // 避开下标 0 (LOB 的市价单档) 也避开低端哨兵区; 上端同样留一段 guard.
  if (price_max != 0) {
    out.price_min = price_min;
    out.price_max = price_max;
    // 判据是"最高价会不会顶进上端留白", 而不是"会不会超出窗口" —— 655 元附近
    // 的票不平移虽然装得下, 下标却正落在高端哨兵区里.
    const bool shift =
        price_max >= kPriceIndexRange - kPriceIndexGuard && price_min > kPriceIndexGuard;
    out.price_base =
        shift ? ((price_min - kPriceIndexGuard) & ~(kPriceIndexGuard - 1)) : 0u;
    if (price_max - out.price_base >= kPriceIndexRange - kPriceIndexGuard)
      out.flags |= Check::PriceSpanTooWide;
  }

  // ---- 逐笔流自洽性判据 ----
  if (out.field_overflow != 0)
    out.flags |= Check::FieldOverflow;
  if (out.lob_unusable != 0)
    out.flags |= Check::LobUnusable;
  if (out.dup_maker != 0)
    out.flags |= Check::DupMakerId;
  if (out.cancel_unresolved != 0)
    out.flags |= Check::CancelUnresolved;
  if (out.trade_both_missing != 0)
    out.flags |= Check::TradeBothMissing;
  if (out.strict_ledger && out.trade_side_missing != 0)
    out.flags |= Check::TradeSideMissing;
  if (out.strict_ledger && out.over_consumed != 0)
    out.flags |= Check::OverConsumed;

  // ---- 与快照对拍 ----
  if (!market.valid) {
    out.flags |= Check::MarketAbsent;
    return;
  }

  out.volume_delta = static_cast<int64_t>(cum_volume) - static_cast<int64_t>(market.cum_volume);
  if (out.volume_delta != 0)
    out.flags |= Check::VolumeMismatch;

  if (out.strict_ledger) {
    out.bid_delta = bid_residual - static_cast<int64_t>(market.bid_total);
    out.ask_delta = ask_residual - static_cast<int64_t>(market.ask_total);
    if (out.bid_delta != 0 || out.ask_delta != 0)
      out.flags |= Check::BookMismatch;
  }

  out.turnover_delta = static_cast<int64_t>(cum_turnover_fen) -
                       static_cast<int64_t>(market.cum_turnover) * 100;
  if (out.turnover_delta > kTurnoverToleranceFen || out.turnover_delta < -kTurnoverToleranceFen)
    out.flags |= Check::TurnoverMismatch;

  if (trade_count > 0 &&
      (high != market.high || low != market.low || last_price != market.last_price))
    out.flags |= Check::PriceMismatch;
}

// ============================================================================
// 日志描述
// ============================================================================

std::string ValidationReport::describe() const {
  if (flags == 0)
    return "ok";

  std::string text;
  auto append = [&text](const char *name, int64_t value) {
    if (!text.empty())
      text += ' ';
    text += name;
    text += '=';
    text += std::to_string(value);
  };

  if (flags & Check::FieldOverflow)
    append("field_overflow", static_cast<int64_t>(field_overflow));
  if (flags & Check::LobUnusable)
    append("lob_unusable", static_cast<int64_t>(lob_unusable));
  if (flags & Check::PriceSpanTooWide) {
    append("price_min", price_min);
    append("price_max", price_max);
  }
  if (flags & Check::DupMakerId)
    append("dup_maker", static_cast<int64_t>(dup_maker));
  if (flags & Check::CancelUnresolved)
    append("cancel_unresolved", static_cast<int64_t>(cancel_unresolved));
  if (flags & Check::TradeBothMissing)
    append("trade_both_missing", static_cast<int64_t>(trade_both_missing));
  if (flags & Check::TradeSideMissing)
    append("trade_side_missing", static_cast<int64_t>(trade_side_missing));
  if (flags & Check::OverConsumed)
    append("over_consumed", static_cast<int64_t>(over_consumed));
  if (flags & Check::MarketAbsent) {
    if (!text.empty())
      text += ' ';
    text += "market_absent";
  }
  if (flags & Check::VolumeMismatch)
    append("volume_delta", volume_delta);
  if (flags & Check::BookMismatch) {
    append("bid_delta", bid_delta);
    append("ask_delta", ask_delta);
  }
  if (flags & Check::TurnoverMismatch)
    append("turnover_delta_fen", turnover_delta);
  if (flags & Check::PriceMismatch) {
    if (!text.empty())
      text += ' ';
    text += "price_mismatch";
  }
  return text;
}

} // namespace L2
