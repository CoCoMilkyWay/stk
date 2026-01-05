#include "codec/binary_decoder_L2.hpp"
#include "misc/profiler.hpp"
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <regex>
#include <sstream>

namespace L2 {

// Constructor with capacity hints
BinaryDecoder_L2::BinaryDecoder_L2(size_t estimated_snapshots, size_t estimated_orders) {
  // Pre-reserve space for snapshot vectors
  temp_hours.reserve(estimated_snapshots);
  temp_minutes.reserve(estimated_snapshots);
  temp_seconds.reserve(estimated_snapshots);
  // temp_highs.reserve(estimated_snapshots);
  // temp_lows.reserve(estimated_snapshots);
  temp_closes.reserve(estimated_snapshots);
  temp_all_bid_vwaps.reserve(estimated_snapshots);
  temp_all_ask_vwaps.reserve(estimated_snapshots);
  temp_all_bid_volumes.reserve(estimated_snapshots);
  temp_all_ask_volumes.reserve(estimated_snapshots);

  for (size_t i = 0; i < 10; ++i) {
    temp_bid_prices[i].reserve(estimated_snapshots);
    temp_ask_prices[i].reserve(estimated_snapshots);
  }

  // Pre-reserve space for order vectors
  temp_order_hours.reserve(estimated_orders);
  temp_order_minutes.reserve(estimated_orders);
  temp_order_seconds.reserve(estimated_orders);
  temp_order_milliseconds.reserve(estimated_orders);
  temp_order_prices.reserve(estimated_orders);
  temp_bid_order_ids.reserve(estimated_orders);
  temp_ask_order_ids.reserve(estimated_orders);
}

size_t BinaryDecoder_L2::extract_count_from_filename(const std::string &filepath) {
  // Extract count from filename pattern: *_snapshots_<count>.bin or *_orders_<count>.bin
  std::filesystem::path file_path(filepath);
  std::string filename = file_path.stem().string(); // Get filename without extension

  // Look for pattern _<number> at the end
  std::regex count_regex(R"(_(\d+)$)");
  std::smatch match;

  if (std::regex_search(filename, match, count_regex)) {
    return std::stoull(match[1].str());
  }

  return 0; // Return 0 if count cannot be extracted
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

inline float BinaryDecoder_L2::price_to_rmb(uint16_t price_ticks) {
  return static_cast<float>(price_ticks) * 0.01; // Convert from 0.01 RMB units to RMB
}

inline float BinaryDecoder_L2::vwap_to_rmb(uint16_t vwap_ticks) {
  return static_cast<float>(vwap_ticks) * 0.001; // Convert from 0.001 RMB units to RMB
}

inline uint32_t BinaryDecoder_L2::get_volume(uint32_t volume_shares) {
  return volume_shares; // Already in shares, no conversion needed
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

void BinaryDecoder_L2::print_snapshot(const Snapshot &snapshot, size_t index) {
  std::cout << "=== Snapshot " << index << " ===" << std::endl;
  std::cout << "Time: " << time_to_string(snapshot.hour, snapshot.minute, snapshot.second) << std::endl;
  std::cout << "Close: " << std::fixed << std::setprecision(2) << price_to_rmb(snapshot.close) << " RMB" << std::endl;
  // std::cout << "High: " << price_to_rmb(snapshot.high) << " RMB" << std::endl;
  // std::cout << "Low: " << price_to_rmb(snapshot.low) << " RMB" << std::endl;
  std::cout << "Volume: " << get_volume(snapshot.volume) << " shares" << std::endl;
  std::cout << "Turnover: " << snapshot.turnover << " fen" << std::endl;
  std::cout << "Trade Count (incremental): " << static_cast<int>(snapshot.trade_count) << std::endl;

  std::cout << "Bid Prices: ";
  for (int i = 0; i < 10; i++) {
    if (snapshot.bid_price_ticks[i] > 0) {
      std::cout << price_to_rmb(snapshot.bid_price_ticks[i]) << " ";
    }
  }
  std::cout << std::endl;

  std::cout << "Ask Prices: ";
  for (int i = 0; i < 10; i++) {
    if (snapshot.ask_price_ticks[i] > 0) {
      std::cout << price_to_rmb(snapshot.ask_price_ticks[i]) << " ";
    }
  }
  std::cout << std::endl;

  std::cout << "VWAP - Bid: " << vwap_to_rmb(snapshot.all_bid_vwap)
            << ", Ask: " << vwap_to_rmb(snapshot.all_ask_vwap) << std::endl;
  std::cout << "Total Volume - Bid: " << get_volume(snapshot.all_bid_volume)
            << ", Ask: " << get_volume(snapshot.all_ask_volume) << std::endl;
  std::cout << std::endl;
}

void BinaryDecoder_L2::print_order(const Order &order, size_t index) {
  std::cout << "=== Order " << index << " ===" << std::endl;
  std::cout << "Time: " << time_to_string(order.hour, order.minute, order.second, order.millisecond) << std::endl;
  std::cout << "Type: " << order_type_to_string(order.order_type) << std::endl;
  std::cout << "Direction: " << order_dir_to_string(order.order_dir) << std::endl;
  std::cout << "Price: " << std::fixed << std::setprecision(2) << price_to_rmb(order.price) << " RMB" << std::endl;
  std::cout << "Volume: " << get_volume(order.volume) << " shares" << std::endl;
  std::cout << "Bid Order ID: " << order.bid_order_id << std::endl;
  std::cout << "Ask Order ID: " << order.ask_order_id << std::endl;
  std::cout << std::endl;
}

void BinaryDecoder_L2::print_all_snapshots(const std::vector<Snapshot> &snapshots) {
  // hr mn sc trd   vol   turnover  high   low close   bp0   bp1   bp2   bp3   bp4   bp5   bp6   bp7   bp8   bp9   bv0   bv1   bv2   bv3   bv4   bv5   bv6   bv7   bv8   bv9   ap0   ap1   ap2   ap3   ap4   ap5   ap6   ap7   ap8   ap9   av0   av1   av2   av3   av4   av5   av6   av7   av8   av9 d b_vwp a_vwp b_vol a_vol
  // 14 48 45 157     2       1320   665   660   660   660   659   658   657   656   655   654   653   652   651  3854  3831  2879  4500  1294   813   301  1449   714  1236   661   662   663   664   665   666   667   668   669   670  4094  4526  3664  4811  7374  3123  3026  2396  3277  3513 0  6339  6864 53296 65535
  std::cout << "=== All Snapshots ===" << std::endl;

  // Print aligned header using compile-time bit width calculations

  std::cout << std::setw(HOUR_WIDTH) << std::right << "hr" << " "
            << std::setw(MINUTE_WIDTH) << std::right << "mn" << " "
            << std::setw(SECOND_WIDTH) << std::right << "sc" << " "
            << std::setw(TRADE_COUNT_WIDTH) << std::right << "trd" << " "
            << std::setw(VOLUME_WIDTH) << std::right << "vol" << " "
            << std::setw(TURNOVER_WIDTH) << std::right << "turnover" << " "
            // << std::setw(PRICE_WIDTH) << std::right << "high" << " "
            // << std::setw(PRICE_WIDTH) << std::right << "low" << " "
            << std::setw(PRICE_WIDTH) << std::right << "close" << " ";

  // bid_price_ticks[10] - using price bit width from schema
  for (int i = 0; i < 10; i++) {
    std::cout << std::setw(PRICE_WIDTH) << std::right << ("bp" + std::to_string(i)) << " ";
  }

  // bid_volumes[10] - using schema-derived width
  for (int i = 0; i < 10; i++) {
    std::cout << std::setw(BID_VOLUME_WIDTH) << std::right << ("bv" + std::to_string(i)) << " ";
  }

  // ask_price_ticks[10] - using price bit width from schema
  for (int i = 0; i < 10; i++) {
    std::cout << std::setw(PRICE_WIDTH) << std::right << ("ap" + std::to_string(i)) << " ";
  }

  // ask_volumes[10] - using schema-derived width
  for (int i = 0; i < 10; i++) {
    std::cout << std::setw(ASK_VOLUME_WIDTH) << std::right << ("av" + std::to_string(i)) << " ";
  }

  std::cout << std::setw(DIRECTION_WIDTH) << std::right << "d" << " "
            << std::setw(VWAP_WIDTH) << std::right << "b_vwp" << " "
            << std::setw(VWAP_WIDTH) << std::right << "a_vwp" << " "
            << std::setw(TOTAL_VOLUME_WIDTH) << std::right << "b_vol" << " "
            << std::setw(TOTAL_VOLUME_WIDTH) << std::right << "a_vol" << std::endl;

  // Print data rows with aligned formatting using compile-time bit width calculations
  for (const auto &snapshot : snapshots) {
    std::cout << std::setw(HOUR_WIDTH) << std::right << static_cast<int>(snapshot.hour) << " "
              << std::setw(MINUTE_WIDTH) << std::right << static_cast<int>(snapshot.minute) << " "
              << std::setw(SECOND_WIDTH) << std::right << static_cast<int>(snapshot.second) << " "
              << std::setw(TRADE_COUNT_WIDTH) << std::right << static_cast<int>(snapshot.trade_count) << " "
              << std::setw(VOLUME_WIDTH) << std::right << snapshot.volume << " "
              << std::setw(TURNOVER_WIDTH) << std::right << snapshot.turnover << " "
              // << std::setw(PRICE_WIDTH) << std::right << snapshot.high << " "
              // << std::setw(PRICE_WIDTH) << std::right << snapshot.low << " "
              << std::setw(PRICE_WIDTH) << std::right << snapshot.close << " ";

    // Output bid_price_ticks[10] - using price bit width from schema
    for (int i = 0; i < 10; i++) {
      std::cout << std::setw(PRICE_WIDTH) << std::right << snapshot.bid_price_ticks[i] << " ";
    }

    // Output bid_volumes[10] - using schema-derived width
    for (int i = 0; i < 10; i++) {
      std::cout << std::setw(BID_VOLUME_WIDTH) << std::right << snapshot.bid_volumes[i] << " ";
    }

    // Output ask_price_ticks[10] - using price bit width from schema
    for (int i = 0; i < 10; i++) {
      std::cout << std::setw(PRICE_WIDTH) << std::right << snapshot.ask_price_ticks[i] << " ";
    }

    // Output ask_volumes[10] - using schema-derived width
    for (int i = 0; i < 10; i++) {
      std::cout << std::setw(ASK_VOLUME_WIDTH) << std::right << snapshot.ask_volumes[i] << " ";
    }

    std::cout << std::setw(DIRECTION_WIDTH) << std::right << (snapshot.direction ? 1 : 0) << " "
              << std::setw(VWAP_WIDTH) << std::right << snapshot.all_bid_vwap << " "
              << std::setw(VWAP_WIDTH) << std::right << snapshot.all_ask_vwap << " "
              << std::setw(TOTAL_VOLUME_WIDTH) << std::right << snapshot.all_bid_volume << " "
              << std::setw(TOTAL_VOLUME_WIDTH) << std::right << snapshot.all_ask_volume << std::endl;
  }
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
const Snapshot *BinaryDecoder_L2::decode_snapshots_stream(const std::string &filepath, size_t &snapshot_num) {
  // Extract count from filename to calculate required buffer size
  size_t estimated_count = extract_count_from_filename(filepath);
  if (estimated_count == 0) [[unlikely]] {
    std::cerr << "L2 Decoder: Could not extract count from filename: " << filepath << std::endl;
    return nullptr;
  }

  // Calculate buffer sizes
  constexpr size_t header_size = sizeof(size_t); // size_t count
  const size_t snapshots_size = estimated_count * sizeof(Snapshot);
  const size_t decompressed_size = header_size + snapshots_size;

  // Open file and read compression metadata
  std::ifstream file(filepath, std::ios::binary);
  if (!file.is_open()) [[unlikely]] {
    std::cerr << "L2 Decoder: Failed to open file: " << filepath << std::endl;
    return nullptr;
  }

  size_t original_size, compressed_size;
  file.read(reinterpret_cast<char *>(&original_size), sizeof(original_size));
  file.read(reinterpret_cast<char *>(&compressed_size), sizeof(compressed_size));

  if (file.fail()) [[unlikely]] {
    std::cerr << "L2 Decoder: Failed to read compression header: " << filepath << std::endl;
    return nullptr;
  }

  // Verify size matches expectation
  if (original_size != decompressed_size) [[unlikely]] {
    std::cerr << "L2 Decoder: Size mismatch - expected " << decompressed_size
              << " but header says " << original_size << std::endl;
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

  // Streaming decompression: decompress directly to reusable buffer (zero-allocation hot path)
  size_t decompressed_bytes = ZSTD_decompress(
      stream_decompression_buffer_.data(), decompressed_size,
      stream_compressed_buffer_.data(), compressed_size);

  if (ZSTD_isError(decompressed_bytes)) [[unlikely]] {
    std::cerr << "L2 Decoder: Decompression failed: " << ZSTD_getErrorName(decompressed_bytes) << std::endl;
    return nullptr;
  }

  // Extract and verify count from decompressed header
  size_t count;
  std::memcpy(&count, stream_decompression_buffer_.data(), header_size);

  if (count != estimated_count) [[unlikely]] {
    std::cerr << "L2 Decoder: Count mismatch - filename says " << estimated_count
              << " but data says " << count << std::endl;
    return nullptr;
  }

  // Return pointer to Snapshot array (skip header) - ZERO COPY
  snapshot_num = count;
  return reinterpret_cast<const Snapshot *>(stream_decompression_buffer_.data() + header_size);
}

const Order *BinaryDecoder_L2::decode_orders_stream(const std::string &filepath, size_t &order_num) {
    // Extract count from filename to calculate required buffer size
  size_t estimated_count = extract_count_from_filename(filepath);
    if (estimated_count == 0) [[unlikely]] {
      std::cerr << "L2 Decoder: Could not extract count from filename: " << filepath << std::endl;
      return nullptr;
  }

  // Calculate buffer sizes
  constexpr size_t header_size = sizeof(size_t); // size_t count
  const size_t orders_size = estimated_count * sizeof(Order);
  const size_t decompressed_size = header_size + orders_size;

  size_t original_size, compressed_size;
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

    // Verify size matches expectation
    if (original_size != decompressed_size) [[unlikely]] {
      std::cerr << "L2 Decoder: Size mismatch - expected " << decompressed_size
                << " but header says " << original_size << std::endl;
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

  // Extract and verify count from decompressed header
  size_t count;
  std::memcpy(&count, stream_decompression_buffer_.data(), header_size);

  if (count != estimated_count) [[unlikely]] {
    std::cerr << "L2 Decoder: Count mismatch - filename says " << estimated_count
              << " but data says " << count << std::endl;
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
