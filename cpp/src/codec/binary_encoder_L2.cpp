#include "codec/binary_encoder_L2.hpp"
#include "misc/logging.hpp"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <locale>
#include <memory>

// Data format reference: config/sample/L2/README

namespace L2 {

// ============================================================================
// Section 1: Utility Functions (Low-level helpers)
// ============================================================================

// Open CSV file with GBK encoding support
static std::ifstream open_csv_with_gbk(const std::string &filepath) {
  std::ifstream file(filepath);
  if (file.is_open()) {
    try {
      file.imbue(std::locale("zh_CN.GBK"));
    } catch (const std::exception &) {
      // Fallback to default locale if GBK unavailable
    }
  }
  return file;
}

// Fast integer parsing (avoids std::stoul/stoull overhead)
static inline uint32_t fast_parse_u32(std::string_view s) {
  if (s.empty())
    return 0;
  uint32_t val = 0;
  for (char c : s) {
    if (c >= '0' && c <= '9') {
      val = val * 10 + (c - '0');
    }
  }
  return val;
}

static inline uint64_t fast_parse_u64(std::string_view s) {
  if (s.empty())
    return 0;
  uint64_t val = 0;
  for (char c : s) {
    if (c >= '0' && c <= '9') {
      val = val * 10 + (c - '0');
    }
  }
  return val;
}

// Generic number string parser with whitespace handling
// Returns parsed integer, divided by specified divisor
static inline uint64_t parse_numeric_field(std::string_view str, uint32_t divisor) {
  if (str.empty())
    return 0;

  const char *p = str.data();
  const char *end = p + str.size();

  // Skip leading whitespace (handles \x20, \x00, \t, \n, \r)
  while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r' || *p == '\0'))
    ++p;
  if (p == end)
    return 0;

  // Parse integer part
  uint64_t value = 0;
  while (p < end && *p >= '0' && *p <= '9') {
    value = value * 10 + (*p - '0');
    ++p;
  }

  // Skip decimal part if present (not used)
  if (p < end && *p == '.') {
    ++p;
    while (p < end && *p >= '0' && *p <= '9')
      ++p;
  }

  return value == 0 ? 0 : value / divisor;
}

// Time decomposition helpers
static inline uint8_t extract_hour(uint32_t time_ms) {
  return static_cast<uint8_t>(time_ms / 3600000);
}

static inline uint8_t extract_minute(uint32_t time_ms) {
  return static_cast<uint8_t>((time_ms % 3600000) / 60000);
}

static inline uint8_t extract_second(uint32_t time_ms) {
  return static_cast<uint8_t>((time_ms % 60000) / 1000);
}

static inline uint8_t extract_millisecond_10ms(uint32_t time_ms) {
  return static_cast<uint8_t>((time_ms % 1000) / 10);
}

// Market detection
static inline bool is_shenzhen_market(const std::string &stock_code) {
  if (stock_code.size() >= 3) {
    const char *suffix = stock_code.data() + stock_code.size() - 3;
    // Check for .SZ or .sz
    if (suffix[0] == '.' && (suffix[1] == 'S' || suffix[1] == 's') &&
        (suffix[2] == 'Z' || suffix[2] == 'z')) {
      return true;
    }
    // Check for .SH or .sh
    if (suffix[0] == '.' && (suffix[1] == 'S' || suffix[1] == 's') &&
        (suffix[2] == 'H' || suffix[2] == 'h')) {
      return false;
    }
  }
  assert(false && "Invalid stock code format (must end with .SZ or .SH)");
  return false;
}

// Order type determination based on exchange rules
static inline L2::OrderType determine_order_type(char csv_order_type, char csv_trade_code,
                                                 bool is_trade, bool is_shenzhen) {
  if (is_trade) {
    // Trade: cancel(1) or taker(3)
    return (is_shenzhen && csv_trade_code == 'C') ? L2::OrderType::CANCEL : L2::OrderType::TAKER;
  }

  // Order: all maker(0) for SZSE; maker(0) or cancel(1) for SSE
  if (is_shenzhen) {
    return L2::OrderType::MAKER; // SZSE orders are all maker
  } else {
    // SSE: A/a=add/maker, D/d=delete/cancel
    return (csv_order_type == 'A' || csv_order_type == 'a') ? L2::OrderType::MAKER : (csv_order_type == 'D' || csv_order_type == 'd') ? L2::OrderType::CANCEL
                                                                                                                                      : L2::OrderType::MAKER;
  }
}

// Order direction: false=bid(B), true=ask(S)
static inline L2::OrderDirection determine_order_direction(char side_flag) {
  return (side_flag == 'S' || side_flag == 's') ? L2::OrderDirection::ASK : L2::OrderDirection::BID;
}

// ============================================================================
// Section 2: CSV Parsing (String → Intermediate structures)
// ============================================================================

// Split CSV line into string_view fields (zero-copy)
std::vector<std::string_view> BinaryEncoder_L2::split_csv_line_view(std::string_view line) {
  std::vector<std::string_view> fields;
  fields.reserve(70); // Typical snapshot has ~65 fields

  size_t start = 0;
  const size_t len = line.length();

  for (size_t pos = 0; pos < len; ++pos) {
    if (line[pos] == ',') {
      fields.emplace_back(line.data() + start, pos - start);
      start = pos + 1;
    }
  }

  // Last field
  if (start <= len) {
    fields.emplace_back(line.data() + start, len - start);
  }

  return fields;
}

// Convert time from HHMMSSMMM format to milliseconds
uint32_t BinaryEncoder_L2::parse_time_to_ms(uint32_t time_int) {
  uint32_t ms = time_int % 1000;
  time_int /= 1000;
  uint32_t sec = time_int % 100;
  time_int /= 100;
  uint32_t min = time_int % 100;
  uint32_t hour = time_int / 100;

  return hour * 3600000 + min * 60000 + sec * 1000 + ms;
}

// Parse price fields (CSV value in 0.0001 RMB → 0.01 RMB units)
inline uint32_t BinaryEncoder_L2::parse_price_to_fen(std::string_view str) {
  return static_cast<uint32_t>(parse_numeric_field(str, 100));
}

// Parse VWAP fields (CSV value in 0.0001 RMB → 0.001 RMB units)
inline uint32_t BinaryEncoder_L2::parse_vwap_price(std::string_view str) {
  return static_cast<uint32_t>(parse_numeric_field(str, 10));
}

// Parse volume fields (shares → shares, no conversion)
inline uint32_t BinaryEncoder_L2::parse_volume(std::string_view str) {
  return static_cast<uint32_t>(parse_numeric_field(str, 1));
}

// Parse turnover fields (keep as-is, integer fen)
inline uint64_t BinaryEncoder_L2::parse_turnover_to_fen(std::string_view str) {
  return parse_numeric_field(str, 1);
}

// Helper: Detect CSV line delimiter (find first line ending)
static char detect_csv_delimiter(const std::string &filepath) {
  std::ifstream file(filepath, std::ios::binary);
  if (!file.is_open())
    return '\n';

  char ch;
  while (file.get(ch)) {
    if (ch == '\r')
      return '\r';
    if (ch == '\n')
      return '\n';
  }
  return '\n';
}

// Helper: Parse CSV with detected delimiter
template <typename ParseFunc>
static bool parse_csv_with_delimiter(const std::string &filepath, char delimiter, ParseFunc parse_func) {
  auto file = open_csv_with_gbk(filepath);
  if (!file.is_open())
    return false;

  std::string line;
  if (!std::getline(file, line, delimiter))
    return false;
  // Remove trailing \n or \r if present (handle mixed line endings)
  while (!line.empty() && (line.back() == '\n' || line.back() == '\r'))
    line.pop_back();

  size_t line_count = 0;
  while (std::getline(file, line, delimiter)) {
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r'))
      line.pop_back();
    if (line.empty())
      continue;
    parse_func(line);
    line_count++;
  }

  return line_count > 0;
}

// Parse snapshot CSV file
bool BinaryEncoder_L2::parse_snapshot_csv(const std::string &filepath,
                                          std::vector<CSVSnapshot> &snapshots) {
  uint32_t prev_trade_count = 0;
  size_t line_number = 1;

  auto parse_line = [&](const std::string &line) {
    ++line_number;
    auto fields = split_csv_line_view(line);
    if (fields.size() < 65)
      return;

    try {
      CSVSnapshot snap = {};
      snap.stock_code = fields[0];
      snap.exchange_code = fields[1];
      snap.date = fast_parse_u32(fields[2]);
      snap.time = fast_parse_u32(fields[3]);
      snap.price = parse_price_to_fen(fields[4]);
      snap.volume = parse_volume(fields[5]);
      snap.turnover = parse_turnover_to_fen(fields[6]);

      // Convert cumulative trade count to incremental
      // Handle edge case: trade count may reset or have data quality issues
      uint32_t curr_trade_count = fast_parse_u32(fields[7]);
      if (curr_trade_count >= prev_trade_count) {
        snap.trade_count = curr_trade_count - prev_trade_count;
      } else {
        Logger::log("encoding", "Trade count decreased at line " + std::to_string(line_number) + ": " + filepath);
        snap.trade_count = 0;
      }
      prev_trade_count = curr_trade_count;

      // Parse 10-level orderbook
      for (int i = 0; i < 10; ++i) {
        snap.ask_prices[i] = parse_price_to_fen(fields[17 + i]);
        snap.ask_volumes[i] = parse_volume(fields[27 + i]);
        snap.bid_prices[i] = parse_price_to_fen(fields[37 + i]);
        snap.bid_volumes[i] = parse_volume(fields[47 + i]);
      }

      snap.weighted_avg_ask_price = parse_vwap_price(fields[57]);
      snap.weighted_avg_bid_price = parse_vwap_price(fields[58]);
      snap.total_ask_volume = parse_volume(fields[59]);
      snap.total_bid_volume = parse_volume(fields[60]);

      snapshots.push_back(snap);
    } catch (const std::exception &e) {
      Logger::log("encoding", "Error parsing snapshot: " + std::string(e.what()));
      return;
    }
  };

  // Auto-detect delimiter and parse
  char delimiter = detect_csv_delimiter(filepath);
  if (!parse_csv_with_delimiter(filepath, delimiter, parse_line)) {
    Logger::log("encoding", "Cannot parse snapshot CSV: " + filepath);
    return false;
  }

  return true;
}

// Parse order CSV file
bool BinaryEncoder_L2::parse_order_csv(const std::string &filepath,
                                       std::vector<CSVOrder> &orders) {
  auto parse_line = [&](const std::string &line) {
    auto fields = split_csv_line_view(line);
    if (fields.size() < 10)
      return;

    try {
      CSVOrder order = {};
      order.stock_code = fields[0];
      order.exchange_code = fields[1];
      order.date = fast_parse_u32(fields[2]);
      order.time = fast_parse_u32(fields[3]);
      order.order_id = fast_parse_u64(fields[4]);
      order.exchange_order_id = fast_parse_u64(fields[5]);

      bool is_szse = is_shenzhen_market(order.stock_code);

      // Parse order type and side (format differs by exchange)
      if (!fields[6].empty() && fields[6][0] != ' ' && fields[6][0] != '\0') {
        order.order_type = fields[6][0];
      } else {
        order.order_type = is_szse ? '0' : 'A'; // Default: normal(SZSE) or add(SSE)
      }

      if (!fields[7].empty() && fields[7][0] != ' ' && fields[7][0] != '\0') {
        order.order_side = fields[7][0];
      } else {
        order.order_side = ' '; // Empty for cancellation
      }

      order.price = parse_price_to_fen(fields[8]);
      order.volume = parse_volume(fields[9]);

      orders.push_back(order);
    } catch (const std::exception &e) {
      Logger::log("encoding", "Error parsing order: " + std::string(e.what()));
      return;
    }
  };

  // Auto-detect delimiter and parse
  char delimiter = detect_csv_delimiter(filepath);
  if (!parse_csv_with_delimiter(filepath, delimiter, parse_line)) {
    Logger::log("encoding", "Cannot parse order CSV: " + filepath);
    return false;
  }

  return true;
}

// Parse trade CSV file
bool BinaryEncoder_L2::parse_trade_csv(const std::string &filepath,
                                       std::vector<CSVTrade> &trades) {
  auto parse_line = [&](const std::string &line) {
    auto fields = split_csv_line_view(line);
    if (fields.size() < 12)
      return;

    try {
      CSVTrade trade = {};
      trade.stock_code = fields[0];
      trade.exchange_code = fields[1];
      trade.date = fast_parse_u32(fields[2]);
      trade.time = fast_parse_u32(fields[3]);
      trade.trade_id = fast_parse_u64(fields[4]);

      bool is_szse = is_shenzhen_market(trade.stock_code);

      // Parse trade code and BS flag (format differs by exchange)
      if (is_szse) {
        trade.trade_code = !fields[5].empty() ? fields[5][0] : '0';
        trade.bs_flag = !fields[7].empty() ? fields[7][0] : ' ';
      } else {
        trade.trade_code = '0'; // SSE doesn't use trade_code
        trade.bs_flag = !fields[7].empty() ? fields[7][0] : ' ';
      }
      trade.dummy_code = ' '; // Unused

      trade.price = parse_price_to_fen(fields[8]);
      trade.volume = parse_volume(fields[9]);
      trade.ask_order_id = fast_parse_u64(fields[10]);
      trade.bid_order_id = fast_parse_u64(fields[11]);

      trades.push_back(trade);
    } catch (const std::exception &e) {
      Logger::log("encoding", "Error parsing trade: " + std::string(e.what()));
      return;
    }
  };

  // Auto-detect delimiter and parse
  char delimiter = detect_csv_delimiter(filepath);
  if (!parse_csv_with_delimiter(filepath, delimiter, parse_line)) {
    Logger::log("encoding", "Cannot parse trade CSV: " + filepath);
    return false;
  }

  return true;
}

// ============================================================================
// Section 3: Data Conversion (CSV structures → Binary structures)
// ============================================================================

Snapshot BinaryEncoder_L2::csv_to_snapshot(const CSVSnapshot &csv) {
  Snapshot snap = {};

  // Time fields
  uint32_t time_ms = parse_time_to_ms(csv.time);
  snap.hour = clamp_to_bound(extract_hour(time_ms), HOUR_BOUND);
  snap.minute = clamp_to_bound(extract_minute(time_ms), MINUTE_BOUND);
  snap.second = clamp_to_bound(extract_second(time_ms), SECOND_BOUND);

  // Trade info
  snap.trade_count = clamp_to_bound(csv.trade_count, TRADE_COUNT_BOUND);
  snap.volume = clamp_to_bound(csv.volume, VOLUME_BOUND);
  snap.turnover = clamp_to_bound(csv.turnover, TURNOVER_BOUND);
  snap.close = clamp_to_bound(csv.price, PRICE_BOUND);
  snap.direction = false; // Default

  // 10-level orderbook
  for (int i = 0; i < 10; ++i) {
    snap.bid_price_ticks[i] = clamp_to_bound(csv.bid_prices[i], PRICE_BOUND);
    snap.bid_volumes[i] = clamp_to_bound(csv.bid_volumes[i], ORDERBOOK_VOLUME_BOUND);
    snap.ask_price_ticks[i] = clamp_to_bound(csv.ask_prices[i], PRICE_BOUND);
    snap.ask_volumes[i] = clamp_to_bound(csv.ask_volumes[i], ORDERBOOK_VOLUME_BOUND);
  }

  // Aggregated bid/ask info
  snap.all_bid_vwap = clamp_to_bound(csv.weighted_avg_bid_price, VWAP_BOUND);
  snap.all_ask_vwap = clamp_to_bound(csv.weighted_avg_ask_price, VWAP_BOUND);
  snap.all_bid_volume = clamp_to_bound(csv.total_bid_volume, TOTAL_VOLUME_BOUND);
  snap.all_ask_volume = clamp_to_bound(csv.total_ask_volume, TOTAL_VOLUME_BOUND);

  return snap;
}

Order BinaryEncoder_L2::csv_to_order(const CSVOrder &csv) {
  Order order = {};

  // Time fields
  uint32_t time_ms = parse_time_to_ms(csv.time);
  order.hour = clamp_to_bound(extract_hour(time_ms), HOUR_BOUND);
  order.minute = clamp_to_bound(extract_minute(time_ms), MINUTE_BOUND);
  order.second = clamp_to_bound(extract_second(time_ms), SECOND_BOUND);
  order.millisecond = clamp_to_bound(extract_millisecond_10ms(time_ms), MILLISECOND_BOUND);

  // Order attributes
  bool is_szse = is_shenzhen_market(csv.stock_code);
  order.order_type = determine_order_type(csv.order_type, '0', false, is_szse);
  order.order_dir = determine_order_direction(csv.order_side);
  order.price = clamp_to_bound(csv.price, PRICE_BOUND);
  order.volume = clamp_to_bound(csv.volume, VOLUME_BOUND);

  // Order IDs (only one side is set based on direction)
  if (order.order_dir == L2::OrderDirection::BID) { // Bid
    order.bid_order_id = clamp_to_bound(csv.exchange_order_id, ORDER_ID_BOUND);
    order.ask_order_id = 0;
  } else { // Ask
    order.bid_order_id = 0;
    order.ask_order_id = clamp_to_bound(csv.exchange_order_id, ORDER_ID_BOUND);
  }

  return order;
}

Order BinaryEncoder_L2::csv_to_trade(const CSVTrade &csv) {
  Order order = {};

  // Time fields
  uint32_t time_ms = parse_time_to_ms(csv.time);
  order.hour = clamp_to_bound(extract_hour(time_ms), HOUR_BOUND);
  order.minute = clamp_to_bound(extract_minute(time_ms), MINUTE_BOUND);
  order.second = clamp_to_bound(extract_second(time_ms), SECOND_BOUND);
  order.millisecond = clamp_to_bound(extract_millisecond_10ms(time_ms), MILLISECOND_BOUND);

  // Trade attributes
  bool is_szse = is_shenzhen_market(csv.stock_code);
  order.order_type = determine_order_type('0', csv.trade_code, true, is_szse);

  // Direction: for SZSE cancellation (bs_flag empty), infer from bid_order_id
  if (is_szse && (csv.bs_flag == ' ' || csv.bs_flag == '\0')) {
    char effective_side = (csv.bid_order_id != 0) ? 'B' : 'S';
    order.order_dir = determine_order_direction(effective_side);
  } else {
    order.order_dir = determine_order_direction(csv.bs_flag);
  }

  order.price = clamp_to_bound(csv.price, PRICE_BOUND);
  order.volume = clamp_to_bound(csv.volume, VOLUME_BOUND);

  // Trade has both order IDs
  order.bid_order_id = clamp_to_bound(csv.bid_order_id, ORDER_ID_BOUND);
  order.ask_order_id = clamp_to_bound(csv.ask_order_id, ORDER_ID_BOUND);

  return order;
}

// ============================================================================
// Section 4: Encoding Layer (Binary → Compressed binary with delta encoding)
// ============================================================================

// Constructor: preallocate buffers and initialize ZSTD context
BinaryEncoder_L2::BinaryEncoder_L2(size_t est_snapshots, size_t est_orders) {
  // Snapshot buffers
  temp_snap_hours.reserve(est_snapshots);
  temp_snap_minutes.reserve(est_snapshots);
  temp_snap_seconds.reserve(est_snapshots);
  temp_snap_closes.reserve(est_snapshots);
  temp_snap_bid_vwaps.reserve(est_snapshots);
  temp_snap_ask_vwaps.reserve(est_snapshots);
  temp_snap_bid_volumes.reserve(est_snapshots);
  temp_snap_ask_volumes.reserve(est_snapshots);

  for (auto &vec : temp_snap_bid_prices)
    vec.reserve(est_snapshots);
  for (auto &vec : temp_snap_ask_prices)
    vec.reserve(est_snapshots);

  // Order buffers
  temp_order_hours.reserve(est_orders);
  temp_order_minutes.reserve(est_orders);
  temp_order_seconds.reserve(est_orders);
  temp_order_millis.reserve(est_orders);
  temp_order_prices.reserve(est_orders);
  temp_order_bid_ids.reserve(est_orders);
  temp_order_ask_ids.reserve(est_orders);

  // Initialize ZSTD compression context
  zstd_ctx_ = ZSTD_createCCtx();
  assert(zstd_ctx_ && "Failed to create ZSTD context");
  ZSTD_CCtx_setParameter(zstd_ctx_, ZSTD_c_compressionLevel, ZSTD_COMPRESSION_LEVEL);
}

// Destructor: clean up ZSTD context
BinaryEncoder_L2::~BinaryEncoder_L2() {
  if (zstd_ctx_) {
    ZSTD_freeCCtx(zstd_ctx_);
  }
}

bool BinaryEncoder_L2::encode_snapshots(const std::vector<Snapshot> &snapshots,
                                        const std::string &filepath) {
  if (snapshots.empty()) {
    Logger::log("encoding", "No snapshots to encode: " + filepath);
    return true;
  }

  const size_t count = snapshots.size();

  // Prepare buffer: header + data
  const size_t header_size = sizeof(count);
  const size_t data_size = snapshots.size() * sizeof(Snapshot);
  const size_t total_size = header_size + data_size;

  auto buffer = std::make_unique<char[]>(total_size);
  std::memcpy(buffer.get(), &count, header_size);
  std::memcpy(buffer.get() + header_size, snapshots.data(), data_size);

  return compress_and_write_data(filepath, buffer.get(), total_size);
}

bool BinaryEncoder_L2::encode_orders(const std::vector<Order> &orders,
                                     const std::string &filepath) {
  if (orders.empty()) {
    Logger::log("encoding", "No orders to encode: " + filepath);
    return false;
  }

  const size_t count = orders.size();

  // Prepare buffer: header + data
  const size_t header_size = sizeof(count);
  const size_t data_size = orders.size() * sizeof(Order);
  const size_t total_size = header_size + data_size;

  auto buffer = std::make_unique<char[]>(total_size);
  std::memcpy(buffer.get(), &count, header_size);
  std::memcpy(buffer.get() + header_size, orders.data(), data_size);

  return compress_and_write_data(filepath, buffer.get(), total_size);
}

// Compression helper
size_t BinaryEncoder_L2::calculate_compression_bound(size_t data_size) {
  return ZSTD_compressBound(data_size);
}

bool BinaryEncoder_L2::compress_and_write_data(const std::string &filepath,
                                               const void *data, size_t data_size) {
  std::ofstream file(filepath, std::ios::binary);
  if (!file.is_open()) [[unlikely]] {
    Logger::log("encoding", "Cannot open file for writing: " + filepath);
    return false;
  }

  // Compress using reusable context
  const size_t bound = calculate_compression_bound(data_size);
  auto compressed = std::make_unique<char[]>(bound);
  const size_t compressed_size = ZSTD_compress2(
      zstd_ctx_, compressed.get(), bound, data, data_size);

  if (ZSTD_isError(compressed_size)) [[unlikely]] {
    Logger::log("encoding", "Compression failed: " + std::string(ZSTD_getErrorName(compressed_size)));
    return false;
  }

  // Write header + compressed data
  file.write(reinterpret_cast<const char *>(&data_size), sizeof(data_size));
  file.write(reinterpret_cast<const char *>(&compressed_size), sizeof(compressed_size));
  if (file.fail()) [[unlikely]] {
    Logger::log("encoding", "Failed to write header: " + filepath);
    return false;
  }

  file.write(compressed.get(), compressed_size);
  if (file.fail()) [[unlikely]] {
    Logger::log("encoding", "Failed to write data: " + filepath);
    return false;
  }

  // Store stats
  compression_stats.original_size = data_size;
  compression_stats.compressed_size = compressed_size;
  compression_stats.ratio = static_cast<float>(data_size) / compressed_size;

  return true;
}

// ============================================================================
// Section 5: High-Level Interface (Orchestration)
// ============================================================================

bool BinaryEncoder_L2::process_stock_data(const std::string &stock_dir,
                                          const std::string &output_dir,
                                          const std::string &stock_code,
                                          std::vector<Snapshot> *out_snapshots,
                                          std::vector<Order> *out_orders) {
  std::filesystem::create_directories(output_dir);

  // Parse CSV files
  std::vector<CSVSnapshot> csv_snaps;
  std::vector<CSVOrder> csv_orders;
  std::vector<CSVTrade> csv_trades;

  const std::string snap_file = stock_dir + "/行情.csv";
  const std::string order_file = stock_dir + "/逐笔委托.csv";
  const std::string trade_file = stock_dir + "/逐笔成交.csv";

  if (std::filesystem::exists(snap_file)) {
    if (!parse_snapshot_csv(snap_file, csv_snaps))
      return false;
  }

  if (std::filesystem::exists(order_file)) {
    if (!parse_order_csv(order_file, csv_orders))
      return false;
  }

  if (std::filesystem::exists(trade_file)) {
    if (!parse_trade_csv(trade_file, csv_trades))
      return false;
  }

  // Convert and encode snapshots
  if (!csv_snaps.empty()) {
    std::vector<Snapshot> snapshots;
    snapshots.reserve(csv_snaps.size());
    for (const auto &csv : csv_snaps) {
      snapshots.push_back(csv_to_snapshot(csv));
    }

    // Validate snapshot count
    constexpr size_t MIN_EXPECTED_COUNT = 10;
    if (snapshots.size() < MIN_EXPECTED_COUNT) {
      Logger::log("encoding", "Abnormal snapshot count: " + stock_code + " " + stock_dir +
                                  " has only " + std::to_string(snapshots.size()) + " snapshots");
      // std::exit(1);
      return false;
    }

    if (out_snapshots)
      *out_snapshots = snapshots;

    const std::string output_file = output_dir + "/" + stock_code +
                                    "_snapshots_" + std::to_string(snapshots.size()) + ".bin";
    if (!encode_snapshots(snapshots, output_file))
      return false;
  }

  // Convert and encode orders + trades
  std::vector<Order> all_orders;
  all_orders.reserve(csv_orders.size() + csv_trades.size());

  for (const auto &csv : csv_orders)
    all_orders.push_back(csv_to_order(csv));
  for (const auto &csv : csv_trades)
    all_orders.push_back(csv_to_trade(csv));

  // Sort by time, then by priority (maker → taker → cancel)
  std::sort(all_orders.begin(), all_orders.end(), [](const Order &a, const Order &b) {
    const uint32_t time_a = a.hour * 3600000 + a.minute * 60000 + a.second * 1000 + a.millisecond * 10;
    const uint32_t time_b = b.hour * 3600000 + b.minute * 60000 + b.second * 1000 + b.millisecond * 10;

    if (time_a != time_b)
      return time_a < time_b;

    // Priority: maker(0)=0, taker(3)=1, cancel(1)=2
    auto priority = [](uint8_t type) -> uint8_t {
      if (type == 0)
        return 0;
      if (type == 3)
        return 1;
      if (type == 1)
        return 2;
      return 3;
    };

    return priority(a.order_type) < priority(b.order_type);
  });

  if (!all_orders.empty()) {
    // Validate order count
    constexpr size_t MIN_EXPECTED_COUNT = 500;
    if (all_orders.size() < MIN_EXPECTED_COUNT) {
      Logger::log("encoding", "Abnormal order count: " + stock_code + " " + stock_dir +
                                  " has only " + std::to_string(all_orders.size()) + " orders");
      // std::exit(1);
      return false;
    }

    if (out_orders)
      *out_orders = all_orders;

    const std::string output_file = output_dir + "/" + stock_code +
                                    "_orders_" + std::to_string(all_orders.size()) + ".bin";
    if (!encode_orders(all_orders, output_file))
      return false;
  }

  // Release temporary memory after each day
  csv_snaps.clear();
  csv_snaps.shrink_to_fit();
  csv_orders.clear();
  csv_orders.shrink_to_fit();
  csv_trades.clear();
  csv_trades.shrink_to_fit();

  return true;
}

} // namespace L2
