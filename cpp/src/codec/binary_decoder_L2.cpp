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
}

size_t BinaryDecoder_L2::read_order_count(const std::string &filepath) {
  std::ifstream file(filepath, std::ios::binary);
  if (!file.is_open())
    return 0;

  size_t original_size = 0;
  file.read(reinterpret_cast<char *>(&original_size), sizeof(original_size));
  if (file.fail())
    return 0;

  // original_size = sizeof(size_t) [count] + count * sizeof(Order)
  if (original_size < sizeof(size_t))
    return 0;
  const size_t payload = original_size - sizeof(size_t);
  if (payload % sizeof(Order) != 0)
    return 0;

  return payload / sizeof(Order);
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
  // 缓冲尺寸直接由文件头的 original_size 决定 —— 它已经精确等于
  // [size_t count][Order × count] 的长度. 早先这里是拿文件名里的条数去推,
  // 再跟文件头对账; 现在文件名不带条数了 (纯冗余), 头就是唯一来源.
  constexpr size_t header_size = sizeof(size_t); // size_t count

  size_t original_size, compressed_size;
  size_t decompressed_size;
  {
    TraceN("FileIO");
    // Open file and read compression metadata
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) [[unlikely]] {
      std::cerr << "L2 Decoder: Failed to open file: " << filepath << std::endl;
      return nullptr;
    }

    file.read(reinterpret_cast<char *>(&original_size), sizeof(original_size));
    file.read(reinterpret_cast<char *>(&compressed_size), sizeof(compressed_size));

    if (file.fail()) [[unlikely]] {
      std::cerr << "L2 Decoder: Failed to read compression header: " << filepath << std::endl;
      return nullptr;
    }

    decompressed_size = original_size;

    // 头部自洽性: 必须恰好装得下整数条 Order
    if (original_size < header_size ||
        (original_size - header_size) % sizeof(Order) != 0) [[unlikely]] {
      std::cerr << "L2 Decoder: Corrupt header - original_size " << original_size
                << " is not header + N*sizeof(Order): " << filepath << std::endl;
      return nullptr;
    }

    // Resize reusable buffers if needed (only grows, never shrinks - amortized O(1))
    if (stream_compressed_buffer_.size() < compressed_size) {
      stream_compressed_buffer_.resize(compressed_size);
    }
    if (stream_decompression_buffer_.size() < decompressed_size) {
      stream_decompression_buffer_.resize(decompressed_size);
    }

    // Read compressed data into reusable buffer
    file.read(stream_compressed_buffer_.data(), compressed_size);
    if (file.fail()) [[unlikely]] {
      std::cerr << "L2 Decoder: Failed to read compressed data: " << filepath << std::endl;
      return nullptr;
    }
  }

  {
    TraceN("ZstdDecompress");
    // Streaming decompression: decompress directly to reusable buffer (zero-allocation hot path)
    size_t decompressed_bytes = ZSTD_decompress(
        stream_decompression_buffer_.data(), decompressed_size,
        stream_compressed_buffer_.data(), compressed_size);

    if (ZSTD_isError(decompressed_bytes)) [[unlikely]] {
      std::cerr << "L2 Decoder: Decompression failed: " << ZSTD_getErrorName(decompressed_bytes) << std::endl;
      return nullptr;
    }
  }

  // 解压后头部的 count 与外层 original_size 推出的条数必须一致
  size_t count;
  std::memcpy(&count, stream_decompression_buffer_.data(), header_size);

  if (count != (original_size - header_size) / sizeof(Order)) [[unlikely]] {
    std::cerr << "L2 Decoder: Count mismatch - header implies "
              << (original_size - header_size) / sizeof(Order)
              << " but data says " << count << ": " << filepath << std::endl;
    return nullptr;
  }

  // Return pointer to Order array (skip header) - ZERO COPY
  order_num = count;
  return reinterpret_cast<const Order *>(stream_decompression_buffer_.data() + header_size);
}

// Zstandard decompression helper function (pure standard decompression)
bool BinaryDecoder_L2::read_and_decompress_data(const std::string &filepath, void *data, size_t expected_size, size_t &actual_size) {
  std::ifstream file(filepath, std::ios::binary);
  if (!file.is_open()) [[unlikely]] {
    std::cerr << "L2 Decoder: Failed to open file for decompression: " << filepath << std::endl;
    std::exit(1);
  }

  // Read header: original size and compressed size
  size_t original_size, compressed_size;
  file.read(reinterpret_cast<char *>(&original_size), sizeof(original_size));
  file.read(reinterpret_cast<char *>(&compressed_size), sizeof(compressed_size));

  if (file.fail()) [[unlikely]] {
    std::cerr << "L2 Decoder: Failed to read compression header: " << filepath << std::endl;
    std::exit(1);
  }

  // Verify expected size matches
  if (original_size != expected_size) [[unlikely]] {
    std::cerr << "L2 Decoder: Size mismatch - expected " << expected_size
              << " but header says " << original_size << std::endl;
    std::exit(1);
  }

  // Read compressed data
  auto compressed_buffer = std::make_unique<char[]>(compressed_size);
  file.read(compressed_buffer.get(), compressed_size);

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
