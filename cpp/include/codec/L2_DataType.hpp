#pragma once

#include <cmath>
#include <cstdint>
#include <string>
#include <string_view>

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

inline constexpr size_t DEFAULT_ENCODER_SNAPSHOT_SIZE = 5000; // 3秒全量快照 4*3600/3=4800
inline constexpr size_t DEFAULT_ENCODER_ORDER_SIZE = 200000;  // 逐笔合并(增删改成交)

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

struct Snapshot {
  uint8_t hour;                 // 5bit
  uint8_t minute;               // 6bit
  uint8_t second;               // 6bit
  uint8_t trade_count;          // 8bit
  uint32_t volume;              // 22bit - in shares (expanded to support up to 4M shares)
  uint32_t turnover;            // 32bit - RMB
  uint16_t close;               // 14bit - price in 0.01 RMB units
  uint16_t bid_price_ticks[10]; // 14bits * 10 - prices in 0.01 RMB units
  uint32_t bid_volumes[10];     // 22bits * 10 - in shares (expanded to support up to 4M shares per level)
  uint16_t ask_price_ticks[10]; // 14bits * 10 - prices in 0.01 RMB units
  uint32_t ask_volumes[10];     // 22bits * 10 - in shares (expanded to support up to 4M shares per level)
  bool direction;               // 1bit - 0: buy, 1: sell (vwap_last > vwap_now)
  uint16_t all_bid_vwap;        // 15bit - vwap in 0.001 RMB units of all bid orders
  uint16_t all_ask_vwap;        // 15bit - vwap in 0.001 RMB units of all ask orders
  uint32_t all_bid_volume;      // 22bit - volume of all bid orders in shares
  uint32_t all_ask_volume;      // 22bit - volume of all bid orders in shares
};

// 逐笔合并(增删改成交)
struct Order {
  uint8_t hour;        // 5bit
  uint8_t minute;      // 6bit
  uint8_t second;      // 6bit
  uint8_t millisecond; // 7bit (in 10ms)

  uint8_t order_type; // 2bit - 0:maker(order) 1:cancel 2:change 3:taker(trade)
  uint8_t order_dir;  // 1bit - 0:bid 1:ask
  uint16_t price;     // 14bit - price in 0.01 RMB units
  uint32_t volume;    // 22bit - in shares (expanded to support up to 4M shares)

  uint32_t bid_order_id; // 32bit
  uint32_t ask_order_id; // 32bit
  // (order_type, order_dir)== |(0,0)        |(0,1)         |(1,0)         |(1,1)          |(2,0) |(2,1) |(3,0)         |(3,1)
  // bid_order_id:             |buy_maker_id |0             |buy_cancel_id |0              |0     |0     |buy_taker_id  |buy_maker_id
  // ask_order_id:             |0            |sell_maker_id |0             |sell_cancel_id |0     |0     |sell_maker_id |sell_taker_id
};

namespace OrderType {
constexpr uint8_t MAKER = 0;
constexpr uint8_t CANCEL = 1;
constexpr uint8_t TAKER = 3;
} // namespace OrderType

namespace OrderDirection {
constexpr uint8_t BID = 0;
constexpr uint8_t ASK = 1;
} // namespace OrderDirection

struct ColumnMeta {
  std::string_view column_name; // 列名
  uint8_t bit_width;            // 实际存储 bit 宽度
};

// clang-format off
constexpr ColumnMeta Snapshot_Schema[] = {
    // snapshot
    {"hour",               5  },// "取值范围 0-23,5bit 足够"},
    {"minute",             6  },// "取值范围 0-59,6bit 足够"},
    {"second",             6  },// "取值范围 0-59,6bit 足够"},
    {"trade_count",        8  },// "波动较大, 多数时候为0或小值"},
    {"volume",             22 },// "成交量(股), expanded to 22bit to support up to 4M shares"},
    {"turnover",           32 },// "波动较大,但也有大量0"},
    {"close",              14 },// "价格(0.01 RMB units)"},
    {"bid_price_ticks[10]",14 },// "订单价(0.01 RMB units)"},
    {"bid_volumes[10]",    22 },// "订单量(股), expanded to 22bit to support up to 4M shares per level"},
    {"ask_price_ticks[10]",14 },// "订单价(0.01 RMB units)"},
    {"ask_volumes[10]",    22 },// "订单量(股), expanded to 22bit to support up to 4M shares per level"},
    {"direction",         1   },// "仅买/卖两种值"},
    {"all_bid_vwap",      15  },// "VWAP价格(0.001 RMB units)"},
    {"all_ask_vwap",      15  },// "VWAP价格(0.001 RMB units)"},
    {"all_bid_volume",    22  },// "总量(股)"},
    {"all_ask_volume",    22  },// "总量(股)"},

    // order
    {"millisecond",       7   },// "取值范围 0-127,7bit 足够"},
    {"order_type",        2   },// "仅增删改成交四种值"},
    {"order_dir",         1   },// "仅bid ask 两种值"},
    {"price",             14  },// "价格(0.01 RMB units)"},
    {"volume",            22  },// "成交量(股), expanded to 22bit to support up to 4M shares"},
    {"bid_order_id",      32  },// "订单id"},
    {"ask_order_id",      32  },// "订单id"},
  };
// clang-format on

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

// Market asset validation function
// Returns true ONLY for test assets (600000-600020, 300000-300020)
// Returns false for all actual market assets
inline bool is_valid_market_asset(const std::string &asset_code) {
  if (asset_code.length() < 6)
    return false;

  const std::string prefix_3 = asset_code.substr(0, 3);

  // Special case: Test assets 600000-600020 and 300000-300020
  if (prefix_3 == "600" || prefix_3 == "300") {
    try {
      int code_num = std::stoi(asset_code.substr(3));
      if (code_num >= 0 && code_num <= 20)
        return true; // Test asset
    } catch (...) {
      // Fall through to normal validation
    }
  }

  // Exclude all actual market assets
  if (is_sse_asset(prefix_3))
    return false; // 上交所
  if (is_szse_asset(prefix_3))
    return false; // 深交所
  if (is_bse_asset(asset_code))
    return false; // 北交所
  if (is_neeq_asset(asset_code))
    return false; // 新三板

  return false; // Not a valid market asset (likely an index or other instrument)
}

//========================================================================================
// SCHEMA UTILITIES
//========================================================================================

// Compile-time upper bound calculations based on schema definitions
namespace SchemaUtils {
// Helper to find column index by name in schema
constexpr size_t find_column_index(const ColumnMeta *schema, size_t schema_size, std::string_view column_name) {
  for (size_t i = 0; i < schema_size; ++i) {
    if (schema[i].column_name == column_name) {
      return i;
    }
  }
  return schema_size; // Return invalid index if not found
}

// Get bitwidth for a column from schema
constexpr uint8_t get_column_bitwidth(const ColumnMeta *schema, size_t schema_size, std::string_view column_name) {
  size_t index = find_column_index(schema, schema_size, column_name);
  return (index < schema_size) ? schema[index].bit_width : 0;
}

// Calculate max value from bitwidth
constexpr uint64_t bitwidth_to_max(uint8_t bitwidth) {
  return bitwidth > 0 ? ((1ull << bitwidth) - 1) : 0;
}

// Helper functions for safe casting with bounds checking
template <typename T>
constexpr T clamp_to_bound(uint64_t value, T bound_val) {
  return static_cast<T>(value > bound_val ? bound_val : value);
}

// Calculate decimal digits needed for given bit width
constexpr int calc_digits_from_bitwidth(uint8_t bit_width) {
  if (bit_width == 0)
    return 1;
  uint64_t max_val = (1ull << bit_width) - 1;

  int digits = 0;
  do {
    digits++;
    max_val /= 10;
  } while (max_val > 0);

  return digits;
}

constexpr size_t SCHEMA_SIZE = sizeof(Snapshot_Schema) / sizeof(Snapshot_Schema[0]);

// Snapshot field upper bounds extracted from schema
constexpr uint32_t HOUR_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "hour"));
constexpr uint32_t MINUTE_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "minute"));
constexpr uint32_t SECOND_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "second"));
constexpr uint32_t TRADE_COUNT_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "trade_count"));
constexpr uint32_t VOLUME_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "volume"));
constexpr uint64_t TURNOVER_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "turnover"));
constexpr uint32_t PRICE_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "close"));
constexpr uint32_t ORDERBOOK_VOLUME_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "bid_volumes[10]"));
constexpr uint32_t VWAP_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "all_bid_vwap"));
constexpr uint32_t TOTAL_VOLUME_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "all_bid_volume"));

// Order field upper bounds extracted from schema
constexpr uint32_t MILLISECOND_BOUND = 127; // 7 bits for millisecond in 10ms units (not in schema)
constexpr uint32_t ORDER_TYPE_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "order_type"));
constexpr uint32_t ORDER_DIR_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "order_dir"));
constexpr uint64_t ORDER_ID_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "bid_order_id"));

// Snapshot field display widths extracted from schema
constexpr int HOUR_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "hour"));
constexpr int MINUTE_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "minute"));
constexpr int SECOND_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "second"));
constexpr int TRADE_COUNT_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "trade_count"));
constexpr int VOLUME_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "volume"));
constexpr int TURNOVER_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "turnover"));
constexpr int PRICE_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "close"));
constexpr int DIRECTION_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "direction"));
constexpr int VWAP_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "all_bid_vwap"));
constexpr int TOTAL_VOLUME_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "all_bid_volume"));
constexpr int BID_VOLUME_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "bid_volumes[10]"));
constexpr int ASK_VOLUME_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "ask_volumes[10]"));

// Order field display widths extracted from schema
constexpr int MILLISECOND_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "millisecond"));
constexpr int ORDER_TYPE_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "order_type"));
constexpr int ORDER_DIR_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "order_dir"));
constexpr int ORDER_PRICE_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "price"));
constexpr int ORDER_VOLUME_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "volume"));
constexpr int ORDER_ID_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "bid_order_id"));
} // namespace SchemaUtils

} // namespace L2
