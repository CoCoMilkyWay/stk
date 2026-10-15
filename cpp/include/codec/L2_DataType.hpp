#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

// https://github.com/fpga2u/AXOrderBook FPGA-real-time-parser
// https://zhuanlan.zhihu.com/p/649040063 深交所orderbook重建
// https://zhuanlan.zhihu.com/p/649400934 上交所orderbook重建
// https://zhuanlan.zhihu.com/p/662438311 沪市level2数据重建
// https://zhuanlan.zhihu.com/p/665919675 实时重建沪市level2数据
// https://zhuanlan.zhihu.com/p/708215930 订单簿成像股价走势预测
// https://zhuanlan.zhihu.com/p/640661128 Weighted Mid Price定价模型的改进
// https://zhuanlan.zhihu.com/p/660995304 浅谈深层订单簿建模问题之复杂性(上)
// https://zhuanlan.zhihu.com/p/672245189 浅谈深层订单簿建模之复杂性(下)
// https://zhuanlan.zhihu.com/p/678879213 订单簿的一些性质
// https://zhuanlan.zhihu.com/p/680914693 低价股的订单簿单队列建模
// https://zhuanlan.zhihu.com/p/518906022 Santa Fe Model and Hawkes Process

namespace L2 {

inline constexpr size_t DEFAULT_ENCODER_ORDER_SIZE = 200000; // 逐笔合并(增删改成交), OrderEncoder 中间缓冲初始预留

// LimitOrderBook 委托表 (OrderTable) 初始预留: 槽数 = next_pow2(2 × 本值), 32 B/槽.
//
// LOB 是每 worker 一个的复用工作区 (非每资产常驻, 见 sequential_worker), 所以
// 按"最忙资产的单日逐笔数"奢侈预留 (每 worker 64MB, ×W 而非 ×A). 每日再按当日
// 事件数 reserve_orders 收窄在用表长; 事件数超过预留时表会在空表状态下扩容 (不截断).
inline constexpr size_t LOB_ORDER_CAPACITY = 1000000;

// Data Struct
inline constexpr int BLEN = 100;            // default length for Cbuffers (feature computation)
inline constexpr int SNAPSHOT_INTERVAL = 3; // 全量快照间隔
inline constexpr int TRADE_HRS_PER_DAY = 4; // 单日交易时间

// Resample
inline constexpr int RESAMPLE_INIT_VOLUME_THD = 10000; // initial volume threshold (n*shares*100rmb/100 = n*100rmb)
inline constexpr int RESAMPLE_TRADE_HRS_PER_DAY = 4;   // number of trading hours in a day
inline constexpr int RESAMPLE_MIN_PERIOD = 1;          // minimal sample period (in seconds)
inline constexpr int RESAMPLE_TARGET_PERIOD = 30;      // target sample period (in seconds) (more dense sample in the morning)
inline constexpr int RESAMPLE_EMA_DAYS_PERIOD = 5;     // shouldn't be too large, std(delta_t) will instead go larger
// days   3   5   10  25
// stddev 108 110 114 124

// LimitOrderBook
inline constexpr size_t LOB_DEPTH = 30;                   // Number of depth levels to maintain (one-side)
inline constexpr uint32_t L2_MIN_TIME_INTERVAL_MS = 1000; // Minimum time interval in milliseconds for effective TOB update

// Trading Sessions
// Morning call auction (集合竞价)
constexpr uint8_t MORNING_CALL_AUCTION_START_HOUR = 9;
constexpr uint8_t MORNING_CALL_AUCTION_START_MINUTE = 15;
constexpr uint8_t MORNING_CALL_AUCTION_END_MINUTE = 25;

// Morning matching period (集合竞价撮合期)
constexpr uint8_t MORNING_MATCHING_START_HOUR = 9;
constexpr uint8_t MORNING_MATCHING_START_MINUTE = 25;
constexpr uint8_t MORNING_MATCHING_END_MINUTE = 30;

// Continuous auction (连续竞价-上午)
constexpr uint8_t CONTINUOUS_TRADING_MORNING_START_HOUR = 9;
constexpr uint8_t CONTINUOUS_TRADING_MORNING_START_MINUTE = 30;
constexpr uint8_t CONTINUOUS_TRADING_MORNING_END_HOUR = 11;
constexpr uint8_t CONTINUOUS_TRADING_MORNING_END_MINUTE = 30;

// Continuous auction (连续竞价-下午)
constexpr uint8_t CONTINUOUS_TRADING_AFTERNOON_START_HOUR = 13;
constexpr uint8_t CONTINUOUS_TRADING_AFTERNOON_START_MINUTE = 0;
constexpr uint8_t CONTINUOUS_TRADING_AFTERNOON_END_HOUR = 14;
constexpr uint8_t CONTINUOUS_TRADING_AFTERNOON_END_MINUTE = 57;

// Closing call auction (收盘集合竞价 - Shenzhen only)
constexpr uint8_t CLOSING_CALL_AUCTION_START_HOUR = 14;
constexpr uint8_t CLOSING_CALL_AUCTION_START_MINUTE = 57;
constexpr uint8_t CLOSING_CALL_AUCTION_END_HOUR = 15;
constexpr uint8_t CLOSING_CALL_AUCTION_END_MINUTE = 0;

// | Compressor name     | Ratio | Compression | Decompress |
// |---------------------|-------|-------------|------------|
// | zstd 1.5.7 -1       | 2.896 | 510 MB/s    | 1550 MB/s  |
// | brotli 1.1.0 -1     | 2.883 | 290 MB/s    | 425 MB/s   |
// | zlib 1.3.1 -1       | 2.743 | 105 MB/s    | 390 MB/s   |
// | zstd 1.5.7 --fast=1 | 2.439 | 545 MB/s    | 1850 MB/s  |
// | quicklz 1.5.0 -1    | 2.238 | 520 MB/s    | 750 MB/s   |
// | zstd 1.5.7 --fast=4 | 2.146 | 665 MB/s    | 2050 MB/s  |
// | lzo1x 2.10 -1       | 2.106 | 650 MB/s    | 780 MB/s   |
// | lz4 1.10.0          | 2.101 | 675 MB/s    | 3850 MB/s  |
// | snappy 1.2.1        | 2.089 | 520 MB/s    | 1500 MB/s  |
// | lzf 3.6 -1          | 2.077 | 410 MB/s    | 820 MB/s   |

enum OrderType : uint8_t {
  MAKER = 0,
  CANCEL = 1,
  TAKER = 3,
};

enum OrderDirection : uint8_t {
  BID = 0,
  ASK = 1,
};

enum MarketState : uint8_t {
  CLOSED = 0,
  OPENING_CALL_AUCTION = 1,
  OPENING_MATCHING_PERIOD = 2,
  CONTINUOUS_TRADING_MORNING = 3,
  CONTINUOUS_TRADING_AFTERNOON = 4,
  CLOSING_CALL_AUCTION = 5,
  CLOSING_MATCHING_PERIOD = 6,
};

enum ValidType : uint8_t {
  ALL = 0,
  DATA = 1,
  DEPTH = 2, // depth valid ⊆ data valid
};

// 逐笔合并(增删改成交)
//
// 定长 20 字节. 字段按"热路径直接寻址"排: volume、两个 id、四个时间字段都是
// 平凡成员, 取值零开销; 只有价格与类型/方向共用最后 4 字节的位域 —— 29+2+1
// 正好填满一个 uint32, 既不浪费也不让结构变大, 读价格只多一次 AND.
//
// price 29bit 覆盖到 536 万元. A 股最高价在千元量级, 位宽窄于 18bit 就会把高价
// 股的价格钳平; volume 给满 32bit, 交易所单笔申报上限是 100 万股. 这两个字段的
// 富余都是位域凑数凑出来的, 不占额外空间.
//
// price 是绝对价 (分), 但已折进 LOB 的档位窗口: 窗口外的报价在落盘时就停靠到
// 边缘, 见 OrderEncoder::park_price 与 kPriceIndexRange.
struct Order {
  uint32_t volume;       // 股
  uint32_t bid_order_id; // 32bit
  uint32_t ask_order_id; // 32bit

  uint8_t hour;        // 5bit
  uint8_t minute;      // 6bit
  uint8_t second;      // 6bit
  uint8_t millisecond; // 7bit (in 10ms)

  uint32_t price : 29;          // 绝对价, 0.01 RMB 单位 (分), 上限 536 万元
  OrderType order_type : 2;     // 0:maker(order) 1:cancel 3:taker(trade)
  OrderDirection order_dir : 1; // 0:bid 1:ask
  // (order_type, order_dir)== |(0,0)        |(0,1)         |(1,0)         |(1,1)          |(2,0) |(2,1) |(3,0)         |(3,1)
  // bid_order_id:             |buy_maker_id |0             |buy_cancel_id |0              |0     |0     |buy_taker_id  |buy_maker_id
  // ask_order_id:             |0            |sell_maker_id |0             |sell_cancel_id |0     |0     |sell_maker_id |sell_taker_id
};
// 位域必须与前面三个 uint32 和四个 uint8 严丝合缝地凑满 20 字节 —— 一旦编译器
// 给位域另起了存储单元, 落盘格式就悄悄变了.
static_assert(sizeof(Order) == 20, "Order 必须定宽 20 字节");

// LOB 档位数组的窗口宽度 (分), 即 655.35 元.
//
// LimitOrderBook 拿价格当档位数组的下标直接索引, 数组不能开成绝对价的全域: 每
// 个资产一个 LOB 且全部常驻, 五千多个实例按 65536 档算已是 3 GB 量级, 按千元级
// 价格的全域开就是十几 GB. 所以窗口只覆盖单只标的一天的价格跨度, 按
// (price - price_base) 索引.
//
// 655.35 元的宽度对单日成交带绰绰有余 —— 涨跌停把它限制在前收盘的 ±20% 以内.
// 下标 0 保留给市价单/无价格档 (LOB 的 Level[0]), 所以窗口的有效范围是
// [base+1, base+kPriceIndexRange-1]; base 为 0 时下标就等于绝对价.
inline constexpr uint32_t kPriceIndexRange = 65536;

// price_base 的对齐粒度 (分). 只为让基准取整可读, 不参与任何判据.
inline constexpr uint32_t kPriceIndexGuard = 4096;

// 一个 (资产, 当天) 一个 zstd 帧, 解开是 [u64 count][Order × count] (见
// codec/OrderFrame.hpp). 帧装在当天全市场的日文件里, 位置 / 条数 / price_base
// 记在日文件目录 (见 shared/OrdersDay.hpp). price_base 是该资产当天全部非零价格
// 的下界 (分), 向下对齐; LOB 用它把绝对价折进 65536 档的直接索引数组: index =
// price - price_base. 低价股恒为 0, 只有高价股才非零. 见 LimitOrderBookDefine.hpp.
inline constexpr size_t frame_raw_size(size_t order_count) {
  return sizeof(uint64_t) + order_count * sizeof(Order);
}

//========================================================================================
// MARKET CLASSIFICATION AND EXCHANGE TYPES
//========================================================================================

// Exchange type enumeration - determines matching mechanism for order book reconstruction
enum class ExchangeType : uint8_t {
  SSE,  // Shanghai Stock Exchange (上交所) - bilateral in call auction, unilateral in continuous
  SZSE, // Shenzhen Stock Exchange (深交所) - bilateral all day
  BSE,  // Beijing Stock Exchange (北交所)
  NEEQ, // National Equities Exchange and Quotations (新三板)
  UNKNOWN
};

// Shanghai Stock Exchange (上交所)
inline bool is_sse_asset(const std::string &prefix) {
  return prefix == "600" || // 沪市主板
         prefix == "601" || // 沪市主板
         prefix == "603" || // 沪市主板
         prefix == "605" || // 沪市主板
         prefix == "688" || // 科创板
         prefix == "689" || // 科创板存托凭证
         prefix == "900";   // 沪市B股
}

// Shenzhen Stock Exchange (深交所)
inline bool is_szse_asset(const std::string &prefix) {
  return prefix == "000" || // 深市主板
         prefix == "001" || // 深市主板
         prefix == "002" || // 深市中小板
         prefix == "003" || // 深市中小板
         prefix == "004" || // 深市中小板
         prefix == "300" || // 创业板
         prefix == "301" || // 创业板
         prefix == "302" || // 创业板
         prefix == "309" || // 创业板存托凭证
         prefix == "200" || // 深市B股
         prefix == "201";   // 深市B股
}

// Beijing Stock Exchange (北交所)
inline bool is_bse_asset(const std::string &asset_code) {
  if (asset_code.length() < 2)
    return false;
  const std::string prefix_2 = asset_code.substr(0, 2);
  return prefix_2 == "87" || // 北交所精选层
         prefix_2 == "88" || // 北交所精选层
         prefix_2 == "92";   // 北交所
}

// National Equities Exchange and Quotations (新三板)
inline bool is_neeq_asset(const std::string &asset_code) {
  if (asset_code.length() < 2)
    return false;

  const std::string prefix_3 = asset_code.length() >= 3 ? asset_code.substr(0, 3) : "";
  const std::string prefix_2 = asset_code.substr(0, 2);

  return prefix_3 == "400" || // 新三板基础层
         prefix_3 == "420" || // 新三板基础层
         prefix_3 == "430" || // 新三板基础层
         prefix_2 == "82" ||  // 新三板创新层
         prefix_2 == "83";    // 新三板创新层
}

// Infer exchange type from asset code (for order book matching mechanism)
// Usage: L2::ExchangeType exchange_type = L2::infer_exchange_type("600000.SH");
inline ExchangeType infer_exchange_type(const std::string &asset_code) {
  if (asset_code.length() < 2)
    return ExchangeType::UNKNOWN;

  const std::string prefix_3 = asset_code.length() >= 3 ? asset_code.substr(0, 3) : "";

  if (is_sse_asset(prefix_3))
    return ExchangeType::SSE;
  if (is_szse_asset(prefix_3))
    return ExchangeType::SZSE;
  if (is_bse_asset(asset_code))
    return ExchangeType::BSE;
  if (is_neeq_asset(asset_code))
    return ExchangeType::NEEQ;

  return ExchangeType::UNKNOWN;
}

// Infer price limit percentage from asset code (for depth data protection)
// Usage: float pct = L2::infer_pct_limit("600000"); // 10% for main board
// Note: ST/*ST stocks have 5% limit but require name/status info, not handled here
inline float infer_pct_limit(const std::string &code) {
  assert(!code.empty() && "asset_code must not be empty");
  assert(code.size() >= 6 && "asset_code must be at least 6 digits");

  const std::string prefix_3 = code.substr(0, 3);
  const std::string prefix_2 = code.substr(0, 2);

  // 北交所 87/88/92: 30%
  if (prefix_2 == "87" || prefix_2 == "88" || prefix_2 == "92")
    return 0.30f;

  // 科创板 688/689: 20%
  if (prefix_3 == "688" || prefix_3 == "689")
    return 0.20f;

  // 创业板 300/301/302/309: 20%
  if (prefix_3 == "300" || prefix_3 == "301" ||
      prefix_3 == "302" || prefix_3 == "309")
    return 0.20f;

  // 其他 (主板/中小板/B股等): 10%
  // 注意: ST/*ST股票为5%, 但需要额外的状态信息判断, 此处未处理
  return 0.10f;
}

} // namespace L2
