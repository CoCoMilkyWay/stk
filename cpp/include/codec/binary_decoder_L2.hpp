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

  // 切到某天的日文件 (见 shared/EncodeDayRecord.hpp). 同一路径重复调用是空操作,
  // 所以按天遍历时一天只 open 一次; 换天才关旧开新. 文件必须存在.
  void open_day(const std::string &day_path);

  // 关掉当前日文件. 偶发的单次读取 (GUI 重放) 读完就关: 日文件可能随后被重编
  // 换掉 (rename 到同一路径), 留着旧 fd 会让下次同路径的 open_day 读到旧 inode.
  void close_day();

  // 从当前日文件按目录项解出一个资产的逐笔: pread 一个帧 → 复用 DCtx 解压.
  // offset / compressed_size / order_count 来自日文件目录 (DateInfo 或
  // read_day_file), 三者与帧内容必须一致, 否则断言.
  // 返回指向内部缓冲的 Order 数组, 在下一次 decode 前有效.
  const Order *decode(uint64_t offset, uint32_t compressed_size, uint32_t order_count);

  // Print order in human-readable format
  static void print_order(const Order &order, size_t index = 0);

  // Print all orders with array details
  static void print_all_orders(const std::vector<Order> &orders);

  // Convert time components back to readable format
  static std::string time_to_string(uint8_t hour, uint8_t minute, uint8_t second, uint8_t millisecond_10ms = 0);

private:
  // Reusable vector tables for delta decoding (orders)
  mutable std::vector<uint8_t> temp_order_hours, temp_order_minutes, temp_order_seconds, temp_order_milliseconds;
  mutable std::vector<uint16_t> temp_order_prices;
  mutable std::vector<uint32_t> temp_bid_order_ids, temp_ask_order_ids;

  // Streaming decompression buffer (reused across all decode calls for zero-allocation)
  mutable std::vector<char> stream_decompression_buffer_;
  mutable std::vector<char> stream_compressed_buffer_;

  std::string day_path_;
  int day_fd_ = -1;

  // 复用的解压上下文, 配置为跳过帧内容校验 — 热路径零额外成本
  // (顺带省掉 ZSTD_decompress 每次调用内部建/销 DCtx 的开销)
  ZSTD_DCtx *dctx_ = nullptr;

  static const char *order_type_to_string(uint8_t order_type);
  static const char *order_dir_to_string(uint8_t order_dir);
  static const char *order_type_to_char(uint8_t order_type);
  static const char *order_dir_to_char(uint8_t order_dir);
};

} // namespace L2
