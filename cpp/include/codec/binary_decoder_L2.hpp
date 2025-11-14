#pragma once

#include "L2_DataType.hpp"
#include <cmath>
#include <string>
#include <vector>

// Zstandard compression library
#include "../../package/zstd-1.5.7/zstd.h"

namespace L2 {


class BinaryDecoder_L2 {
public:
  // Constructor with optional capacity hints for better memory allocation
  BinaryDecoder_L2(size_t estimated_snapshots = 100000, size_t estimated_orders = 500000);

  // decoder functions
  bool decode_snapshots(const std::string &filepath, std::vector<Snapshot> &snapshots);
  // decode_orders: fills orders vector up to its capacity, returns actual count via out parameter
  // Returns false if: file error, decompression error, or order count exceeds vector capacity
  bool decode_orders(const std::string &filepath, std::vector<Order> &orders, size_t &order_num);

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
  static inline double price_to_rmb(uint16_t price_ticks);

  // Convert VWAP price from internal format to readable format (0.001 RMB units)
  static inline double vwap_to_rmb(uint16_t vwap_ticks);

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

  static const char *order_type_to_string(uint8_t order_type);
  static const char *order_dir_to_string(uint8_t order_dir);
  static const char *order_type_to_char(uint8_t order_type);
  static const char *order_dir_to_char(uint8_t order_dir);
};

} // namespace L2
