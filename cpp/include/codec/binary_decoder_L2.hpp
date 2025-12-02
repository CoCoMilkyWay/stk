#pragma once

#include "L2_DataType.hpp"
#include <cmath>
#include <string>
#include <vector>

// Zstandard compression library
#include "../../package/zstd-1.5.7/zstd.h"

namespace L2 {

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

class BinaryDecoder_L2 {
public:
  // Constructor with optional capacity hints for better memory allocation
  BinaryDecoder_L2(size_t estimated_snapshots = 100000, size_t estimated_orders = 500000);

  // Decoder functions (zero-copy streaming design)
  // Internal buffer is reused across decode calls for maximum efficiency

  // decode_snapshots_stream: zero-copy streaming decode, returns pointer to internal buffer
  // Returns: pointer to Snapshot array (valid until next decode_* call), or nullptr on error
  // snapshot_num: output parameter for actual snapshot count
  // Note: Caller must process snapshots before next decode call (buffer is reused)
  const Snapshot *decode_snapshots_stream(const std::string &filepath, size_t &snapshot_num);

  // decode_orders_stream: zero-copy streaming decode, returns pointer to internal buffer
  // Returns: pointer to Order array (valid until next decode_* call), or nullptr on error
  // order_num: output parameter for actual order count
  // Note: Caller must process orders before next decode call (buffer is reused)
  const Order *decode_orders_stream(const std::string &filepath, size_t &order_num);

  // Zstandard decompression helper functions (pure standard decompression)
  static bool read_and_decompress_data(const std::string &filepath, void *data, size_t expected_size, size_t &actual_size);

  // Print snapshot in human-readable format
  static void print_snapshot(const Snapshot &snapshot, size_t index = 0);

  // Print order in human-readable format
  static void print_order(const Order &order, size_t index = 0);

  // Print all snapshots with array details
  static void print_all_snapshots(const std::vector<Snapshot> &snapshots);

  // Print all orders with array details
  static void print_all_orders(const std::vector<Order> &orders);

  // Convert time components back to readable format
  static std::string time_to_string(uint8_t hour, uint8_t minute, uint8_t second, uint8_t millisecond_10ms = 0);

  // Convert price from internal format to readable format (hot path - inlined)
  static inline float price_to_rmb(uint16_t price_ticks);

  // Convert VWAP price from internal format to readable format (0.001 RMB units)
  static inline float vwap_to_rmb(uint16_t vwap_ticks);

  // Get volume as-is (no conversion needed, already in shares)
  static inline uint32_t get_volume(uint32_t volume_shares);

  // Helper function to extract count from filename (used by dictionary compression)
  static size_t extract_count_from_filename(const std::string &filepath);

private:
  // Reusable vector tables for delta decoding (snapshots)
  mutable std::vector<uint8_t> temp_hours, temp_minutes, temp_seconds;
  // mutable std::vector<uint16_t> temp_highs, temp_lows;
  mutable std::vector<uint16_t> temp_closes;
  mutable std::vector<uint16_t> temp_bid_prices[10], temp_ask_prices[10];
  mutable std::vector<uint16_t> temp_all_bid_vwaps, temp_all_ask_vwaps;
  mutable std::vector<uint32_t> temp_all_bid_volumes, temp_all_ask_volumes;

  // Reusable vector tables for delta decoding (orders)
  mutable std::vector<uint8_t> temp_order_hours, temp_order_minutes, temp_order_seconds, temp_order_milliseconds;
  mutable std::vector<uint16_t> temp_order_prices;
  mutable std::vector<uint32_t> temp_bid_order_ids, temp_ask_order_ids;

  // Streaming decompression buffer (reused across all decode calls for zero-allocation)
  mutable std::vector<char> stream_decompression_buffer_;
  mutable std::vector<char> stream_compressed_buffer_;

  static const char *order_type_to_string(uint8_t order_type);
  static const char *order_dir_to_string(uint8_t order_dir);
  static const char *order_type_to_char(uint8_t order_type);
  static const char *order_dir_to_char(uint8_t order_dir);
};

} // namespace L2
