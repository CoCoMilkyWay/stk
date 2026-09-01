// 暴露 ZSTD_d_forceIgnoreChecksum (实验参数, 需静态链接宏) — 必须在任何
// zstd.h 展开之前定义, 所以放在本文件最顶部的 include 之前.
#define ZSTD_STATIC_LINKING_ONLY

#include "codec/binary_decoder_L2.hpp"
#include "misc/profiler.hpp"
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
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
  // 跳过帧内容校验: 落盘是原子的 (tmp+rename), 头部自洽 + count 对账已能
  // 挡住结构性损坏; 每次解码再算 xxh64 是白花的 (特征/因子计算会高频跑
  // 成千上万遍).
  ZSTD_DCtx_setParameter(dctx_, ZSTD_d_forceIgnoreChecksum, ZSTD_d_ignoreChecksum);
}

BinaryDecoder_L2::~BinaryDecoder_L2() {
  if (dctx_)
    ZSTD_freeDCtx(dctx_);
}

// 读 + 校验定宽头. 失败返回 false (文件不存在/太短/magic 或尺寸不自洽).
static bool read_l2_header(std::ifstream &file, L2FileHeader &header) {
  file.read(reinterpret_cast<char *>(&header), sizeof(header));
  return !file.fail() && header.sane();
}

size_t BinaryDecoder_L2::read_order_count(const std::string &filepath) {
  std::ifstream file(filepath, std::ios::binary);
  if (!file.is_open())
    return 0;

  L2FileHeader header;
  if (!read_l2_header(file, header))
    return 0;

  return header.order_count();
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
static float price_to_rmb(uint16_t price_ticks) {
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

// decoder functions
const Order *BinaryDecoder_L2::decode_orders_stream(const std::string &filepath, size_t &order_num) {
  // 缓冲尺寸直接由文件头的 raw_size 决定 —— 它已经精确等于
  // [u64 count][Order × count] 的长度, 头就是唯一来源 (见 L2FileHeader).
  constexpr size_t count_size = sizeof(uint64_t); // u64 count

  L2FileHeader header;
  {
    TraceN("FileIO");
    // Open file and read compression metadata
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) [[unlikely]] {
      std::cerr << "L2 Decoder: Failed to open file: " << filepath << std::endl;
      return nullptr;
    }

    if (!read_l2_header(file, header)) [[unlikely]] {
      std::cerr << "L2 Decoder: Corrupt/legacy header (magic/version/sizes), re-encode: "
                << filepath << std::endl;
      return nullptr;
    }

    // Resize reusable buffers if needed (only grows, never shrinks - amortized O(1))
    if (stream_compressed_buffer_.size() < header.compressed_size) {
      stream_compressed_buffer_.resize(header.compressed_size);
    }
    if (stream_decompression_buffer_.size() < header.raw_size) {
      stream_decompression_buffer_.resize(header.raw_size);
    }

    // Read compressed data into reusable buffer
    file.read(stream_compressed_buffer_.data(), static_cast<std::streamsize>(header.compressed_size));
    if (file.fail()) [[unlikely]] {
      std::cerr << "L2 Decoder: Failed to read compressed data: " << filepath << std::endl;
      return nullptr;
    }
  }

  {
    TraceN("ZstdDecompress");
    // 复用 DCtx (已配置跳过帧内容校验), 直接解到复用缓冲 — 热路径零分配
    size_t decompressed_bytes = ZSTD_decompressDCtx(
        dctx_,
        stream_decompression_buffer_.data(), header.raw_size,
        stream_compressed_buffer_.data(), header.compressed_size);

    if (ZSTD_isError(decompressed_bytes) || decompressed_bytes != header.raw_size) [[unlikely]] {
      std::cerr << "L2 Decoder: Decompression failed: "
                << (ZSTD_isError(decompressed_bytes) ? ZSTD_getErrorName(decompressed_bytes) : "size mismatch")
                << ": " << filepath << std::endl;
      return nullptr;
    }
  }

  // 解压后头部的 count 与外层 raw_size 推出的条数必须一致
  uint64_t count;
  std::memcpy(&count, stream_decompression_buffer_.data(), count_size);

  if (count != header.order_count()) [[unlikely]] {
    std::cerr << "L2 Decoder: Count mismatch - header implies " << header.order_count()
              << " but data says " << count << ": " << filepath << std::endl;
    return nullptr;
  }

  // Return pointer to Order array (skip header) - ZERO COPY
  order_num = count;
  return reinterpret_cast<const Order *>(stream_decompression_buffer_.data() + count_size);
}

// Zstandard decompression helper function (pure standard decompression)
bool BinaryDecoder_L2::read_and_decompress_data(const std::string &filepath, void *data, size_t expected_size, size_t &actual_size) {
  std::ifstream file(filepath, std::ios::binary);
  if (!file.is_open()) [[unlikely]] {
    std::cerr << "L2 Decoder: Failed to open file for decompression: " << filepath << std::endl;
    std::exit(1);
  }

  // Read fixed-width self-describing header
  L2FileHeader header;
  if (!read_l2_header(file, header)) [[unlikely]] {
    std::cerr << "L2 Decoder: Corrupt/legacy header: " << filepath << std::endl;
    std::exit(1);
  }

  // Verify expected size matches
  if (header.raw_size != expected_size) [[unlikely]] {
    std::cerr << "L2 Decoder: Size mismatch - expected " << expected_size
              << " but header says " << header.raw_size << std::endl;
    std::exit(1);
  }
  const size_t compressed_size = header.compressed_size;

  // Read compressed data
  auto compressed_buffer = std::make_unique<char[]>(compressed_size);
  file.read(compressed_buffer.get(), static_cast<std::streamsize>(compressed_size));

  if (file.fail()) [[unlikely]] {
    std::cerr << "L2 Decoder: Failed to read compressed data: " << filepath << std::endl;
    std::exit(1);
  }

  // Standard Zstandard decompression
  size_t decompressed_size = ZSTD_decompress(
      data, expected_size,
      compressed_buffer.get(), compressed_size);

  if (ZSTD_isError(decompressed_size)) [[unlikely]] {
    std::cerr << "L2 Decoder: Decompression failed: " << ZSTD_getErrorName(decompressed_size) << std::endl;
    std::exit(1);
  }

  if (decompressed_size != expected_size) [[unlikely]] {
    std::cerr << "L2 Decoder: Decompressed size mismatch - expected " << expected_size
              << " but got " << decompressed_size << std::endl;
    std::exit(1);
  }

  actual_size = decompressed_size;

  // Print decompression statistics
  // float compression_ratio = static_cast<float>(original_size) / static_cast<float>(compressed_size);
  // std::cout << "L2 Decoder: Decompressed " << compressed_size << " bytes to " << original_size
  //           << " bytes (ratio: " << std::fixed << std::setprecision(2) << compression_ratio << "x)" << std::endl;

  return true;
}

} // namespace L2
