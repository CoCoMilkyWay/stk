#pragma once

#include "L2_DataType.hpp"
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

// finish_asset 的三态结果 — 增量编码要区分:
//   TooFewOrders: 源数据为空 (停牌日的纯表头文件), 确定性结论 → 写墓碑,
//                 别再重试; 低流动性但非空的照常编码, 不在存储层做策略过滤
//   Error:        环境错误 (磁盘满/压缩失败), 下次增量重跑时重试
enum class EncodeResult : uint8_t { Ok,
                                    TooFewOrders,
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

// Order field upper bounds extracted from schema.
//
// Snapshot_Schema 是全字段位宽表, 逐笔字段的位宽也从这里取. 盘口专属的上界
// (trade_count / turnover / 十档量 / vwap 之类) 随 csv_to_snapshot 一起删了.
constexpr uint32_t HOUR_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "hour"));
constexpr uint32_t MINUTE_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "minute"));
constexpr uint32_t SECOND_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "second"));
constexpr uint32_t VOLUME_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "volume"));
constexpr uint32_t PRICE_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "close"));
constexpr uint32_t MILLISECOND_BOUND = 127; // 7 bits for millisecond in 10ms units (not in schema)
constexpr uint32_t ORDER_TYPE_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "order_type"));
constexpr uint32_t ORDER_DIR_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "order_dir"));
constexpr uint64_t ORDER_ID_BOUND = bitwidth_to_max(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "bid_order_id"));

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

  // ------------------------------------------------------------
  // Data Conversion API
  // ------------------------------------------------------------

  // Convert CSV structures to binary structures
  static Order csv_to_order(const CSVOrder &csv);
  static Order csv_to_trade(const CSVTrade &csv);

  // ------------------------------------------------------------
  // Binary Encoding API
  // ------------------------------------------------------------

  // Encode and compress binary structures to file
  bool encode_orders(const std::vector<Order> &orders,
                     const std::string &filepath);

  // ------------------------------------------------------------
  // High-Level Interface (流式: begin → feed... → finish)
  // ------------------------------------------------------------
  //
  // 一个资产的两个 CSV (逐笔委托 + 逐笔成交) 由 unrar p 管道先后送达, 且共用
  // 一块复用缓冲 —— 后一个到达时前一个的原始字节已被覆盖. 所以接口是流式的:
  // 每块到达就地解析成中间结构, 两块都喂完再合并排序落盘.
  //
  // 快照 (行情.csv) 不再编码: 其产物全项目无人读取 —— 特征计算只吃 orders,
  // 靠 LimitOrderBook 重建盘口. 省掉的是单日 46.2 GB 里的 10.13 GB 解析量,
  // 外加整条快照 delta 编码 + zstd + 写文件的开销.

  void begin_asset();
  bool feed_order_csv(const char *data, size_t len);
  bool feed_trade_csv(const char *data, size_t len);

  // 合并 → 按时间/优先级排序 → 压缩落盘 (tmp + rename 原子).
  // tag 仅用于日志定位 (形如 "20260803 600519.SH").
  EncodeResult finish_asset(const std::string &output_file, const std::string &tag);

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

  bool compress_and_write_data(const std::string &filepath, const void *data, size_t data_size);
  static size_t calculate_compression_bound(size_t data_size);

  // ------------------------------------------------------------
  // Reusable Buffers (avoid reallocation)
  // ------------------------------------------------------------

  // 解析/合并中间结果. 一个 worker 顺序处理成千上万个 (资产, 日期),
  // 这几个 vector 只 clear 不释放, 容量涨到峰值后就不再 malloc.
  std::vector<CSVOrder> csv_orders_;
  std::vector<CSVTrade> csv_trades_;
  std::vector<Order> orders_;

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
