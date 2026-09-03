#pragma once

#include "L2_DataType.hpp"
#include "L2_Validator.hpp"
#include <cassert>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <zstd.h>

namespace L2 {

// ============================================================================
// Configuration
// ============================================================================

// Zstandard compression level (offline encoding optimized)
// Trade-off: higher level → better compression ratio, slower encoding
// Level 6: balanced for storage efficiency + acceptable encoding speed
inline constexpr int ZSTD_COMPRESSION_LEVEL = 6;

// finish_asset 的结果 — 增量编码要区分四种结局:
//   TooFewOrders:  源数据为空 (停牌日的纯表头文件), 确定性结论 → 写墓碑,
//                  别再重试; 低流动性但非空的照常编码, 不在存储层做策略过滤
//   CorruptSource: 源 CSV 有坏行 (字段错位/代码非法), 多半是归档成员 CRC
//                  损坏. 不产出任何东西也不写墓碑 —— 跳过并留日志, 等人把
//                  源文件修好, 下次增量自动重来. 绝不能 abort: 这是几小时
//                  的批处理, 一个坏标的不该让整轮白跑.
//   InvalidData:   行能解析, 但逐笔流本身不自洽或与快照对不上 (见
//                  L2_Validator.hpp). 处置与 CorruptSource 完全相同 —— 缺片
//                  的逐笔编成 .bin 会让 LOB 沉默地重建出错误盘口, 不如不落.
//   Error:         环境错误 (磁盘满/压缩失败), 下次增量重跑时重试
enum class EncodeResult : uint8_t { Ok,
                                    TooFewOrders,
                                    CorruptSource,
                                    InvalidData,
                                    Error };

// ============================================================================
// Intermediate CSV Structures
// ============================================================================

// 行情.csv (盘口快照) 没有对应结构 — 快照不再编码, 见 encode_orders_from_csv.

// Order data (逐笔委托)
struct CSVOrder {
  std::string stock_code;
  std::string exchange_code;
  uint32_t date;
  uint32_t time;
  uint64_t order_id;          // Internal ID (for data validation)
  uint64_t exchange_order_id; // Exchange ID (actual order ID)
  char order_type;            // SSE: A=add, D=delete; SZSE: varies
  char order_side;            // B=bid, S=ask
  uint32_t price;             // in 0.01 RMB units
  uint32_t volume;            // in shares
};

// Trade data (逐笔成交)
struct CSVTrade {
  std::string stock_code;
  std::string exchange_code;
  uint32_t date;
  uint32_t time;
  uint64_t trade_id;
  char trade_code; // SZSE: 0=trade, C=cancel; SSE: unused
  char dummy_code; // unused
  char bs_flag;    // B=buy, S=sell, empty=cancel
  uint32_t price;  // in 0.01 RMB units
  uint32_t volume; // in shares
  uint64_t ask_order_id;
  uint64_t bid_order_id;
};

// ============================================================================
// Compression Statistics
// ============================================================================

struct CompressionStats {
  size_t original_size = 0;
  size_t compressed_size = 0;
  float ratio = 0.0;
};

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

constexpr size_t SCHEMA_SIZE = sizeof(Snapshot_Schema) / sizeof(Snapshot_Schema[0]);

// Order 各字段的上界.
//
// 直接写死成 Order 位域的宽度, 不走 Snapshot_Schema 的名字查表 —— 那张表里盘口
// 列和逐笔列同名不同宽, 查表拿到的会是盘口那一列的位宽. 位宽的真相只应该有一
// 处, 就是 Order 的定义本身.
constexpr uint32_t HOUR_BOUND = 31;              // 5bit
constexpr uint32_t MINUTE_BOUND = 63;            // 6bit
constexpr uint32_t SECOND_BOUND = 63;            // 6bit
constexpr uint32_t MILLISECOND_BOUND = 127;      // 7bit (10ms 单位)
constexpr uint32_t ORDER_TYPE_BOUND = 3;         // 2bit
constexpr uint32_t ORDER_DIR_BOUND = 1;          // 1bit
constexpr uint32_t PRICE_BOUND = (1u << 29) - 1; // 29bit, 上限 536 万元
constexpr uint32_t VOLUME_BOUND = UINT32_MAX;    // 满宽
constexpr uint64_t ORDER_ID_BOUND = UINT32_MAX;  // 32bit

// ============================================================================
// Binary Encoder Class
// ============================================================================

class BinaryEncoder_L2 {
public:
  // Constructor with optional capacity hint
  explicit BinaryEncoder_L2(size_t estimated_orders = 1000000);

  // Destructor: clean up ZSTD context
  ~BinaryEncoder_L2();

  // ------------------------------------------------------------
  // CSV Parsing API
  // ------------------------------------------------------------

  // 内存里的整块 CSV → 中间结构. CSV 由 unrar p 管道直接送进内存, 不落盘
  // (见 misc/archive.hpp 里对落盘往返代价的说明).
  bool parse_order_csv(const char *data, size_t len, std::vector<CSVOrder> &orders);
  bool parse_trade_csv(const char *data, size_t len, std::vector<CSVTrade> &trades);

  // 行情.csv → 末行摘要. 只从尾部回扫到第一条可解析的数据行, 不碰前面的
  // 四千多行 —— 累计字段在末行已是当日终值, 中间行对校验没有额外信息.
  static bool parse_market_tail(const char *data, size_t len, MarketSummary &summary);

  // ------------------------------------------------------------
  // Data Conversion API
  // ------------------------------------------------------------

  // Convert CSV structures to binary structures
  // price_base 是 LOB 档位窗口的下沿, 落盘的价格都折进该窗口 (见 park_price).
  static Order csv_to_order(const CSVOrder &csv, uint32_t price_base);
  static Order csv_to_trade(const CSVTrade &csv, uint32_t price_base);

  // 把价格折进 LOB 档位窗口 [base+1, base+kPriceIndexRange-1], 窗口外一律停靠
  // 到相应边缘; price 为 0 表示无价格 (市价单), 原样透传.
  //
  // 窗口宽 655.35 元, 而单只标的一天的成交带受涨跌停约束远窄于此, 所以被停靠的
  // 只有那些离盘口极远、根本不可能成交的报价. 对它们而言"具体是 9999 元还是
  // 999999 元"没有信息量, 只有"在买/卖方向上无穷远"这一件事是真的, 而这正是
  // 边缘档位表达的含义. 统一在落盘时折算, 使 .bin 与簿内档位一一对应, 挂单与
  // 它后续的撤单也必然落到同一个价格上.
  static uint32_t park_price(uint32_t price, uint32_t price_base) {
    if (price == 0)
      return 0;
    if (price <= price_base)
      return price_base + 1;
    const uint32_t top = price_base + kPriceIndexRange - 1;
    return price < top ? price : top;
  }

  // ------------------------------------------------------------
  // Binary Encoding API
  // ------------------------------------------------------------

  // Encode and compress binary structures to file.
  // price_base 随头落盘, 解码后交给 LOB 还原档位下标 (见 L2_DataType.hpp).
  bool encode_orders(const std::vector<Order> &orders, const std::string &filepath, uint32_t price_base);

  // ------------------------------------------------------------
  // High-Level Interface (流式: begin → feed... → finish)
  // ------------------------------------------------------------
  //
  // 一个资产的两个 CSV (逐笔委托 + 逐笔成交) 由 unrar p 管道先后送达, 且共用
  // 一块复用缓冲 —— 后一个到达时前一个的原始字节已被覆盖. 所以接口是流式的:
  // 每块到达就地解析成中间结构, 两块都喂完再合并排序落盘.
  //
  // 快照 (行情.csv) 不编码落盘: 其产物全项目无人读取 —— 特征计算只吃 orders,
  // 靠 LimitOrderBook 重建盘口. 但它必须被读进来: 末行是交易所给的当日结算
  // 口径, 是逐笔流唯一的外部真值, 编码后归档就不再打开了 (见 L2_Validator.hpp).
  // 代价只有多解压那一份字节 (归档在 NVMe 上, 多路并发直读无寻道代价),
  // 且解析只碰末行, 不做 delta 编码/压缩/落盘.

  void begin_asset();
  bool feed_order_csv(const char *data, size_t len);
  bool feed_trade_csv(const char *data, size_t len);
  bool feed_market_csv(const char *data, size_t len);

  // 合并 → 按时间/优先级排序 → 压缩落盘 (tmp + rename 原子).
  // tag 仅用于日志定位 (形如 "20260803 600519.SH").
  EncodeResult finish_asset(const std::string &output_file, const std::string &tag);

  // 上一次 finish_asset 的准入校验结果 — 返回 InvalidData 时,
  // 调用方靠 flags 把这一对记进当天的账目 (见 shared/EncodeDayRecord.hpp)
  const ValidationReport &get_validation_report() const { return report_; }

  // Get compression statistics
  const CompressionStats &get_compression_stats() const { return compression_stats; }

  // ------------------------------------------------------------
  // Utility Functions (public for testing)
  // ------------------------------------------------------------

  static std::vector<std::string_view> split_csv_line_view(std::string_view line);
  static uint32_t parse_time_to_ms(uint32_t time_int);
  static inline uint32_t parse_price_to_fen(std::string_view str);
  static inline uint32_t parse_volume(std::string_view str);

private:
  // ------------------------------------------------------------
  // Compression Helpers
  // ------------------------------------------------------------

  bool compress_and_write_data(const std::string &filepath, const void *data, size_t data_size, uint32_t price_base);
  static size_t calculate_compression_bound(size_t data_size);

  // ------------------------------------------------------------
  // Reusable Buffers (avoid reallocation)
  // ------------------------------------------------------------

  // 解析/合并中间结果. 一个 worker 顺序处理成千上万个 (资产, 日期),
  // 这几个 vector 只 clear 不释放, 容量涨到峰值后就不再 malloc.
  std::vector<CSVOrder> csv_orders_;
  std::vector<CSVTrade> csv_trades_;
  std::vector<Order> orders_;

  // 当前资产累计的坏行数 (字段数不足 / 代码字段不是 .SZ|.SH). 非零即判定
  // 源损坏, finish_asset 报 CorruptSource 并带上行数, 方便定位到具体标的.
  size_t bad_line_count_ = 0;

  // 准入校验 (见 L2_Validator.hpp). validator_ 内部的哈希表跨资产复用.
  MarketSummary market_;
  Validator validator_;
  ValidationReport report_;

  // Order buffers
  mutable std::vector<uint8_t> temp_order_hours, temp_order_minutes, temp_order_seconds, temp_order_millis;
  mutable std::vector<uint16_t> temp_order_prices;
  mutable std::vector<uint32_t> temp_order_bid_ids, temp_order_ask_ids;

  // Compression statistics
  mutable CompressionStats compression_stats;

  // ZSTD compression context (reused across calls)
  ZSTD_CCtx *zstd_ctx_;
};

} // namespace L2
