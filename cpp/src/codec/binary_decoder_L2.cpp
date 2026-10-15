// 暴露 ZSTD_d_forceIgnoreChecksum (实验参数, 需静态链接宏) — 必须在任何
// zstd.h 展开之前定义, 所以放在本文件最顶部的 include 之前.
#define ZSTD_STATIC_LINKING_ONLY

#include "codec/binary_decoder_L2.hpp"
#include "misc/cross_platform.hpp"
#include "misc/profiler.hpp"
#include <cassert>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace L2 {

// Constructor with capacity hints
BinaryDecoder_L2::BinaryDecoder_L2(size_t estimated_orders) {
  // Pre-reserve space for order vectors
  temp_order_hours.reserve(estimated_orders);
  temp_order_minutes.reserve(estimated_orders);
  temp_order_seconds.reserve(estimated_orders);
  temp_order_milliseconds.reserve(estimated_orders);
  temp_order_prices.reserve(estimated_orders);
  temp_bid_order_ids.reserve(estimated_orders);
  temp_ask_order_ids.reserve(estimated_orders);

  dctx_ = ZSTD_createDCtx();
  assert(dctx_ && "Failed to create ZSTD DCtx");
  // 跳过帧内容校验: 日文件落盘是原子的 (tmp+rename), 目录自洽 + 解压尺寸 +
  // count 对账已能挡住结构性损坏; 每次解码再算 xxh64 是白花的 (特征/因子
  // 计算会高频跑成千上万遍).
  ZSTD_DCtx_setParameter(dctx_, ZSTD_d_forceIgnoreChecksum, ZSTD_d_ignoreChecksum);
}

BinaryDecoder_L2::~BinaryDecoder_L2() {
  if (day_fd_ >= 0)
    file_close(day_fd_);
  ZSTD_freeDCtx(dctx_);
}

void BinaryDecoder_L2::open_day(const std::string &day_path) {
  if (day_fd_ >= 0 && day_path == day_path_)
    return;
  if (day_fd_ >= 0)
    file_close(day_fd_);
  day_fd_ = file_open_read(day_path.c_str());
  assert(day_fd_ >= 0 && "open_day: 日文件打不开 (扫描之后被删/挪走?)");
  day_path_ = day_path;
}

void BinaryDecoder_L2::close_day() {
  if (day_fd_ >= 0)
    file_close(day_fd_);
  day_fd_ = -1;
  day_path_.clear();
}

const Order *BinaryDecoder_L2::decode(uint64_t offset, uint32_t compressed_size, uint32_t order_count) {
  assert(day_fd_ >= 0 && "decode: 先 open_day");
  assert(compressed_size > 0 && order_count > 0 && "decode: 墓碑没有帧");
  const size_t raw_size = frame_raw_size(order_count);

  if (stream_compressed_buffer_.size() < compressed_size)
    stream_compressed_buffer_.resize(compressed_size);
  if (stream_decompression_buffer_.size() < raw_size)
    stream_decompression_buffer_.resize(raw_size);

  {
    TraceN("FileIO");
    const size_t got = file_pread(day_fd_, stream_compressed_buffer_.data(), compressed_size, offset);
    assert(got == compressed_size && "decode: 帧越过日文件末尾 (目录与文件不符)");
    (void)got;
  }

  size_t decompressed_bytes;
  {
    TraceN("ZstdDecompress");
    // 复用 DCtx (已配置跳过帧内容校验), 直接解到复用缓冲 — 热路径零分配
    decompressed_bytes = ZSTD_decompressDCtx(
        dctx_,
        stream_decompression_buffer_.data(), raw_size,
        stream_compressed_buffer_.data(), compressed_size);
  }
  assert(!ZSTD_isError(decompressed_bytes) && decompressed_bytes == raw_size &&
         "decode: 帧解压失败或尺寸与目录不符");
  (void)decompressed_bytes;

  uint64_t count;
  std::memcpy(&count, stream_decompression_buffer_.data(), sizeof(count));
  assert(count == order_count && "decode: 帧内条数与目录不符");
  (void)count;

  return reinterpret_cast<const Order *>(stream_decompression_buffer_.data() + sizeof(uint64_t));
}

std::string BinaryDecoder_L2::time_to_string(uint8_t hour, uint8_t minute, uint8_t second, uint8_t millisecond_10ms) {
  std::ostringstream oss;
  oss << std::setfill('0') << std::setw(2) << static_cast<int>(hour) << ":"
      << std::setfill('0') << std::setw(2) << static_cast<int>(minute) << ":"
      << std::setfill('0') << std::setw(2) << static_cast<int>(second);

  if (millisecond_10ms > 0) {
    oss << "." << std::setfill('0') << std::setw(2) << static_cast<int>(millisecond_10ms * 10);
  }

  return oss.str();
}

// 只给下面的 print_* 用 —— 展示用的单位换算, 不属于解码热路径.
static float price_to_rmb(uint32_t price_ticks) {
  return static_cast<float>(price_ticks) * 0.01; // 0.01 RMB units → RMB
}

const char *BinaryDecoder_L2::order_type_to_string(uint8_t order_type) {
  switch (order_type) {
  case 0:
    return "MAKER";
  case 1:
    return "CANCEL";
  case 2:
    return "CHANGE";
  case 3:
    return "TAKER";
  default:
    return "UNKNOWN";
  }
}

const char *BinaryDecoder_L2::order_dir_to_string(uint8_t order_dir) {
  return order_dir == 0 ? "BID" : "ASK";
}

const char *BinaryDecoder_L2::order_type_to_char(uint8_t order_type) {
  switch (order_type) {
  case 0:
    return "M";
  case 1:
    return "C";
  case 2:
    return "A";
  case 3:
    return "T";
  default:
    return "?";
  }
}

const char *BinaryDecoder_L2::order_dir_to_char(uint8_t order_dir) {
  return order_dir == 0 ? "B" : "S";
}

void BinaryDecoder_L2::print_order(const Order &order, size_t index) {
  std::cout << "=== Order " << index << " ===" << std::endl;
  std::cout << "Time: " << time_to_string(order.hour, order.minute, order.second, order.millisecond) << std::endl;
  std::cout << "Type: " << order_type_to_string(order.order_type) << std::endl;
  std::cout << "Direction: " << order_dir_to_string(order.order_dir) << std::endl;
  std::cout << "Price: " << std::fixed << std::setprecision(2) << price_to_rmb(order.price) << " RMB" << std::endl;
  std::cout << "Volume: " << order.volume << " shares" << std::endl;
  std::cout << "Bid Order ID: " << order.bid_order_id << std::endl;
  std::cout << "Ask Order ID: " << order.ask_order_id << std::endl;
  std::cout << std::endl;
}

void BinaryDecoder_L2::print_all_orders(const std::vector<Order> &orders) {

  // hr mn sc  ms t d price   vol bid_ord_id ask_ord_id
  // 9  15  0   2 0 0   601     1     137525          0
  // 9  15  0   2 0 1   727     1          0     137524

  // Print aligned header using compile-time bit width calculations

  std::cout << std::setw(HOUR_WIDTH) << std::right << "hr" << " "
            << std::setw(MINUTE_WIDTH) << std::right << "mn" << " "
            << std::setw(SECOND_WIDTH) << std::right << "sc" << " "
            << std::setw(MILLISECOND_WIDTH) << std::right << "ms" << " "
            << std::setw(ORDER_TYPE_WIDTH) << std::right << "t" << " "
            << std::setw(ORDER_DIR_WIDTH) << std::right << "d" << " "
            << std::setw(ORDER_PRICE_WIDTH) << std::right << "price" << " "
            << std::setw(ORDER_VOLUME_WIDTH) << std::right << "vol" << " "
            << std::setw(ORDER_ID_WIDTH) << std::right << "bid_ord_id" << " "
            << std::setw(ORDER_ID_WIDTH) << std::right << "ask_ord_id" << std::endl;

  // Print data rows with aligned formatting using compile-time bit width calculations
  for (const auto &order : orders) {
    std::cout << std::setw(HOUR_WIDTH) << std::right << static_cast<int>(order.hour) << " "
              << std::setw(MINUTE_WIDTH) << std::right << static_cast<int>(order.minute) << " "
              << std::setw(SECOND_WIDTH) << std::right << static_cast<int>(order.second) << " "
              << std::setw(MILLISECOND_WIDTH) << std::right << static_cast<int>(order.millisecond) << " "
              << std::setw(ORDER_TYPE_WIDTH) << std::right << order_type_to_char(order.order_type) << " "
              << std::setw(ORDER_DIR_WIDTH) << std::right << order_dir_to_char(order.order_dir) << " "
              << std::setw(ORDER_PRICE_WIDTH) << std::right << order.price << " "
              << std::setw(ORDER_VOLUME_WIDTH) << std::right << order.volume << " "
              << std::setw(ORDER_ID_WIDTH) << std::right << order.bid_order_id << " "
              << std::setw(ORDER_ID_WIDTH) << std::right << order.ask_order_id << std::endl;
  }
}

} // namespace L2
