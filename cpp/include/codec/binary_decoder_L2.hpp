#pragma once

#include "L2_DataType.hpp"
#include <string>
#include <vector>

// Zstandard compression library
#include "zstd.h"

namespace L2 {

// find_column_index / get_column_bitwidth / SCHEMA_SIZE 在 L2_DataType.hpp,
// 与 Snapshot_Schema 同处一地 (encoder 那侧也用同一份)

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

// Order field display widths extracted from schema.
//
// Snapshot_Schema 是全字段位宽表 (盘口字段 + 逐笔字段共处一表), 逐笔的宽度也从
// 这里取. 盘口专属的宽度常量随 print_all_snapshots 一起删了 —— 快照不再落盘.
constexpr int HOUR_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "hour"));
constexpr int MINUTE_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "minute"));
constexpr int SECOND_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "second"));
constexpr int MILLISECOND_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "millisecond"));
constexpr int ORDER_TYPE_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "order_type"));
constexpr int ORDER_DIR_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "order_dir"));
constexpr int ORDER_PRICE_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "price"));
constexpr int ORDER_VOLUME_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "volume"));
constexpr int ORDER_ID_WIDTH = calc_digits_from_bitwidth(get_column_bitwidth(Snapshot_Schema, SCHEMA_SIZE, "bid_order_id"));

class BinaryDecoder_L2 {
public:
  // Constructor with optional capacity hints for better memory allocation
  explicit BinaryDecoder_L2(size_t estimated_orders = 500000);
  ~BinaryDecoder_L2();

  BinaryDecoder_L2(const BinaryDecoder_L2 &) = delete;
  BinaryDecoder_L2 &operator=(const BinaryDecoder_L2 &) = delete;

  // Decoder functions (zero-copy streaming design)
  // Internal buffer is reused across decode calls for maximum efficiency

  // decode_orders_stream: zero-copy streaming decode, returns pointer to internal buffer
  // Returns: pointer to Order array (valid until next decode_* call), or nullptr on error
  // order_num: output parameter for actual order count
  // Note: Caller must process orders before next decode call (buffer is reused)
  const Order *decode_orders_stream(const std::string &filepath, size_t &order_num);

  // 上一次 decode_orders_stream 读到的档位索引基准 (分). 调用方在把这批 orders
  // 喂给 LimitOrderBook 之前必须先用它 reset —— 绝对价要减去它才是档位下标.
  uint32_t last_price_base() const { return last_price_base_; }

  // Zstandard decompression helper functions (pure standard decompression)
  static bool read_and_decompress_data(const std::string &filepath, void *data, size_t expected_size, size_t &actual_size);

  // Print order in human-readable format
  static void print_order(const Order &order, size_t index = 0);

  // Print all orders with array details
  static void print_all_orders(const std::vector<Order> &orders);

  // Convert time components back to readable format
  static std::string time_to_string(uint8_t hour, uint8_t minute, uint8_t second, uint8_t millisecond_10ms = 0);

  // 从文件头读条数与文件大小, 不开解压.
  //
  // 文件布局见 L2_DataType.hpp 的 L2FileHeader — raw_size 精确决定条数,
  // 而文件总长恒等于 32 + compressed_size, 所以读了头就同时拿到了大小,
  // 不必再 stat 一次 (全库扫描时那是五百万次多余的路径解析).
  //
  // 返回 false 表示文件不存在或头部损坏 (含旧格式).
  static bool read_file_stats(const std::string &filepath, size_t &order_count, size_t &file_size);

private:
  // Reusable vector tables for delta decoding (orders)
  mutable std::vector<uint8_t> temp_order_hours, temp_order_minutes, temp_order_seconds, temp_order_milliseconds;
  mutable std::vector<uint16_t> temp_order_prices;
  mutable std::vector<uint32_t> temp_bid_order_ids, temp_ask_order_ids;

  // Streaming decompression buffer (reused across all decode calls for zero-allocation)
  mutable std::vector<char> stream_decompression_buffer_;
  mutable std::vector<char> stream_compressed_buffer_;

  uint32_t last_price_base_ = 0;

  // 复用的解压上下文, 配置为跳过帧内容校验 — 热路径零额外成本
  // (顺带省掉 ZSTD_decompress 每次调用内部建/销 DCtx 的开销)
  ZSTD_DCtx *dctx_ = nullptr;

  static const char *order_type_to_string(uint8_t order_type);
  static const char *order_dir_to_string(uint8_t order_dir);
  static const char *order_type_to_char(uint8_t order_type);
  static const char *order_dir_to_char(uint8_t order_dir);
};

} // namespace L2
