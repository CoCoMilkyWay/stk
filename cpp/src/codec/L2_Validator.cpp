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
  table_.assign(capacity, Slot{kEmptyId, 0});
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

// delta 是否落在 uint32 回绕周期的整数倍附近 (容差 tolerance 以内). 成交额/
// 成交量两个字段共用这一条, 差别只在 wrap_unit 与 tolerance 的换算.
static inline bool delta_is_uint32_wrap_multiple(int64_t delta, int64_t wrap_unit,
                                                 int64_t tolerance) {
  assert(wrap_unit > tolerance && "uint32 回绕周期必须大于容差");
  const int64_t abs_delta = delta < 0 ? -delta : delta;
  if (abs_delta <= tolerance)
    return true;

  const int64_t rem = abs_delta % wrap_unit;
  return rem <= tolerance || wrap_unit - rem <= tolerance;
}

static inline bool turnover_delta_is_uint32_wrap(int64_t delta_fen) {
  constexpr int64_t wrap_fen = static_cast<int64_t>(kUint32WrapCount) * 100;
  return delta_is_uint32_wrap_multiple(delta_fen, wrap_fen, kTurnoverToleranceFen);
}

// 成交量是纯整数股, 不像成交额那样有取整残差, 容差取 0 —— 只放行回绕本身.
static inline bool volume_delta_is_uint32_wrap(int64_t delta_shares) {
  constexpr int64_t wrap_shares = static_cast<int64_t>(kUint32WrapCount);
  return delta_is_uint32_wrap_multiple(delta_shares, wrap_shares, 0);
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

    if (order.volume > VOLUME_BOUND || order.exchange_order_id > ORDER_ID_BOUND ||
        clock_invalid(order.time))
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
  size_t trade_count = 0;

  for (const CSVTrade &trade : trades) {
    if (trade.price == 0 && trade.volume == 0)
      continue; // 数据源占位行

    if (trade.price != 0) {
      price_min = std::min(price_min, trade.price);
      price_max = std::max(price_max, trade.price);
    }

    if (trade.volume > VOLUME_BOUND || trade.bid_order_id > ORDER_ID_BOUND ||
        trade.ask_order_id > ORDER_ID_BOUND || clock_invalid(trade.time))
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

  // ---- 超额扣减 ----
  for (const Slot &slot : table_) {
    if (slot.id != kEmptyId && slot.remaining < 0)
      ++out.over_consumed;
  }

  // ---- LOB 档位窗口 ----
  //
  // 窗口锚在成交带中心而不是委托价的下界: 委托流里混有离盘口极远、不可能成交
  // 的报价, 拿它们定基准会把真正的盘口挤出窗口. 成交带受涨跌停约束, 才是这只
  // 标的当天有意义的价格区间; 把它摆在窗口正中, 两侧各留半个窗口.
  //
  // 中心落在半窗以内时 base 取 0, 档位下标就等于绝对价, 低价标的因此不受平移
  // 影响.
  if (price_max != 0) {
    out.price_min = price_min;
    out.price_max = price_max;

    uint32_t center = 0;
    if (high != 0)
      center = low / 2 + high / 2;
    else if (market.valid && market.last_price != 0)
      center = market.last_price; // 没有成交, 退而用快照末价
    else
      center = price_min; // 连快照也没有, 只能拿委托下界顶上

    constexpr uint32_t kHalf = kPriceIndexRange / 2;
    out.price_base =
        (center > kHalf) ? ((center - kHalf) & ~(kPriceIndexGuard - 1)) : 0u;

    // 成交带本身必须装进窗口. 窗口外的报价停靠到边缘是无损的 (它们不可能成交,
    // 只承载"无穷远"这一个信息), 但成交价被停靠就是真实盘口被扭曲了. 涨跌停
    // 决定了这不会发生, 唯一的例外是无涨跌幅限制的新股 —— 那种日子宁可拦下.
    if (high != 0 && (low <= out.price_base || high >= out.price_base + kPriceIndexRange))
      out.flags |= Check::TradeBandUnfit;
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
  if (!volume_delta_is_uint32_wrap(out.volume_delta))
    out.flags |= Check::VolumeMismatch;

  out.turnover_delta = static_cast<int64_t>(cum_turnover_fen) -
                       static_cast<int64_t>(market.cum_turnover) * 100;
  const bool turnover_unreliable = market.turnover_capped || market.turnover_broken;
  if (!turnover_unreliable && !turnover_delta_is_uint32_wrap(out.turnover_delta))
    out.flags |= Check::TurnoverMismatch;

  if (trade_count > 0 && (high != market.high || low != market.low))
    out.flags |= Check::PriceMismatch;
}

// ============================================================================
// 判据位的名字
// ============================================================================

static constexpr CheckMeta kCheckMeta[kCheckBitCount] = {
    {"dup", "dup_maker", "同一挂单 id 在委托表里出现两次"},
    {"cancel", "cancel_unresolved", "撤单引用了不存在的挂单\n沪市走委托表 D 记录, 深市走成交表 C 记录"},
    {"both", "trade_both_missing", "一笔成交的买卖双方 id 都查不到\n与委托流完不完整无关的硬不变量"},
    {"side", "trade_side_missing", "成交任一侧 id 查不到\n只在委托流完整时判 (单侧缺失率 ≤ 1%)"},
    {"over", "over_consumed", "某挂单被扣减的量超过它挂出的量\n只在委托流完整时判"},
    {"mkt", "market_absent", "行情.csv 缺失或末行解析不出来\n没有对照真值, 对拍类判据无从判断"},
    {"vol", "volume_mismatch", "Σ逐笔成交量 ≠ 快照当日累计成交量 (已放行 uint32 回绕)\n成交流完整性的判决书"},
    {nullptr, nullptr, nullptr}, // bit 7: Check 里没有这一位
    {"ovf", "field_overflow", "源数据字段超出 Order 位宽, 落盘会被静默截断"},
    {"lob", "lob_unusable", "LimitOrderBook 处理不了的记录\n数量为 0, 或定位 id 为 0"},
    {"band", "trade_band_unfit", "当日成交带装不进 LOB 档位窗口\n只可能在无涨跌幅限制的新股上命中"},
    {"turn", "turnover_mismatch", "Σ(成交价×成交量) 与快照当日成交额之差超出取整残差\n已放行 int32 封顶/卡死与 uint32 回绕"},
    {"price", "price_mismatch", "最高/最低价与快照不符"},
};

const CheckMeta &check_meta(size_t bit) {
  assert(bit < kCheckBitCount && "check_meta: 位号越界");
  return kCheckMeta[bit];
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
  if (flags & Check::TradeBandUnfit) {
    append("price_min", price_min);
    append("price_max", price_max);
    append("price_base", price_base);
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
