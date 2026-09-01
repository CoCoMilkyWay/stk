#include "codec/binary_encoder_L2.hpp"
#include "misc/logging.hpp"
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>

// Data format reference: config/sample/L2/README

namespace L2 {

// ============================================================================
// Section 1: Utility Functions (Low-level helpers)
// ============================================================================

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

// Market detection: 1 = 深, 0 = 沪, -1 = 代码字段非法.
//
// -1 只可能来自损坏的源数据 —— 实测 20250813 的归档里 000701.SZ 逐笔成交
// 第 6471 行是 "000701.S00,1000,1..." (unrar 对该成员报 CRC 失败, 解出来的
// 字节整体错位粘连). 解析边界按这个三态判断把坏行挑出来计数, 内部转换路径
// (csv_to_order/csv_to_trade) 只会见到已验证过的行, 所以那里仍然 assert.
static inline int market_of(const std::string &stock_code) {
  if (stock_code.size() >= 3) {
    const char *suffix = stock_code.data() + stock_code.size() - 3;
    if (suffix[0] == '.' && (suffix[1] == 'S' || suffix[1] == 's')) {
      if (suffix[2] == 'Z' || suffix[2] == 'z')
        return 1;
      if (suffix[2] == 'H' || suffix[2] == 'h')
        return 0;
    }
  }
  return -1;
}

static inline bool is_shenzhen_market(const std::string &stock_code) {
  const int market = market_of(stock_code);
  assert(market >= 0 && "Invalid stock code format (must end with .SZ or .SH)");
  return market == 1;
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

// Parse volume fields (shares → shares, no conversion)
inline uint32_t BinaryEncoder_L2::parse_volume(std::string_view str) {
  return static_cast<uint32_t>(parse_numeric_field(str, 1));
}

// Helper: 逐行回调解析内存里的整块 CSV.
//
// encode 的 CSV 来自 unrar p 管道, 不落盘 (见 misc/archive.hpp), 所以这里只有
// 内存版, 没有文件版. 跳过表头行, 跳过空行.
//
// 编码: 原 CSV 是 GBK, 但中文只出现在被跳过的表头里 — 所有被解析的字段
// (代码/日期/时间/价量) 都是 ASCII, 按字节切分即可, 无需转码.
//
// 传给 parse_func 的是 string_view, 零拷贝: 行内容直接指向输入缓冲.
template <typename ParseFunc>
static bool parse_csv_buffer(const char *data, size_t len, ParseFunc parse_func) {
  if (data == nullptr || len == 0)
    return false;

  const char *const end = data + len;
  const char *pos = data;

  auto next_line = [&](std::string_view &out) -> bool {
    if (pos >= end)
      return false;
    const char *nl = static_cast<const char *>(std::memchr(pos, '\n', static_cast<size_t>(end - pos)));
    const char *line_end = (nl == nullptr) ? end : nl;
    const char *trimmed = line_end;
    while (trimmed > pos && (trimmed[-1] == '\r' || trimmed[-1] == '\n'))
      --trimmed;
    out = std::string_view(pos, static_cast<size_t>(trimmed - pos));
    pos = (nl == nullptr) ? end : nl + 1;
    return true;
  };

  std::string_view line;
  if (!next_line(line)) // 表头
    return false;

  size_t line_count = 0;
  while (next_line(line)) {
    if (line.empty())
      continue;
    parse_func(line);
    line_count++;
  }

  return line_count > 0;
}

// 逐笔委托单行 → CSVOrder. 坏行只计数不解析 (见 market_of).
static void parse_order_line(std::string_view line, std::vector<CSVOrder> &orders,
                             size_t &bad_lines) {
  auto fields = BinaryEncoder_L2::split_csv_line_view(line);
  if (fields.size() < 10) {
    ++bad_lines;
    return;
  }

  CSVOrder order = {};
  order.stock_code = fields[0];
  order.exchange_code = fields[1];
  order.date = fast_parse_u32(fields[2]);
  order.time = fast_parse_u32(fields[3]);
  order.order_id = fast_parse_u64(fields[4]);
  order.exchange_order_id = fast_parse_u64(fields[5]);

  const int market = market_of(order.stock_code);
  if (market < 0) {
    ++bad_lines;
    return;
  }
  const bool is_szse = market == 1;

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

  order.price = BinaryEncoder_L2::parse_price_to_fen(fields[8]);
  order.volume = BinaryEncoder_L2::parse_volume(fields[9]);

  orders.push_back(order);
}

// 逐笔成交单行 → CSVTrade. 坏行只计数不解析 (见 market_of).
static void parse_trade_line(std::string_view line, std::vector<CSVTrade> &trades,
                             size_t &bad_lines) {
  auto fields = BinaryEncoder_L2::split_csv_line_view(line);
  if (fields.size() < 12) {
    ++bad_lines;
    return;
  }

  CSVTrade trade = {};
  trade.stock_code = fields[0];
  trade.exchange_code = fields[1];
  trade.date = fast_parse_u32(fields[2]);
  trade.time = fast_parse_u32(fields[3]);
  trade.trade_id = fast_parse_u64(fields[4]);

  const int market = market_of(trade.stock_code);
  if (market < 0) {
    ++bad_lines;
    return;
  }
  const bool is_szse = market == 1;

  // Parse trade code and BS flag (format differs by exchange)
  if (is_szse) {
    trade.trade_code = !fields[5].empty() ? fields[5][0] : '0';
    trade.bs_flag = !fields[7].empty() ? fields[7][0] : ' ';
  } else {
    trade.trade_code = '0'; // SSE doesn't use trade_code
    trade.bs_flag = !fields[7].empty() ? fields[7][0] : ' ';
  }
  trade.dummy_code = ' '; // Unused

  trade.price = BinaryEncoder_L2::parse_price_to_fen(fields[8]);
  trade.volume = BinaryEncoder_L2::parse_volume(fields[9]);
  trade.ask_order_id = fast_parse_u64(fields[10]);
  trade.bid_order_id = fast_parse_u64(fields[11]);

  trades.push_back(trade);
}

bool BinaryEncoder_L2::parse_order_csv(const char *data, size_t len,
                                       std::vector<CSVOrder> &orders) {
  return parse_csv_buffer(data, len, [&orders, this](std::string_view line) {
    parse_order_line(line, orders, bad_line_count_);
  });
}

bool BinaryEncoder_L2::parse_trade_csv(const char *data, size_t len,
                                       std::vector<CSVTrade> &trades) {
  return parse_csv_buffer(data, len, [&trades, this](std::string_view line) {
    parse_trade_line(line, trades, bad_line_count_);
  });
}

// ============================================================================
// Section 3: Data Conversion (CSV structures → Binary structures)
// ============================================================================

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
BinaryEncoder_L2::BinaryEncoder_L2(size_t est_orders) {
  // 解析/合并中间结果 (每个 worker 复用一份, 见 hpp)
  csv_orders_.reserve(est_orders);
  csv_trades_.reserve(est_orders);
  orders_.reserve(est_orders * 2);

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
  // 帧内容校验 (xxh64): 压缩时顺手算 (成本可忽略), 离线 Verify 靠它查
  // 位腐烂; 热读路径显式跳过 (见 decoder), 不花解压之外的钱.
  ZSTD_CCtx_setParameter(zstd_ctx_, ZSTD_c_checksumFlag, 1);
}

// Destructor: clean up ZSTD context
BinaryEncoder_L2::~BinaryEncoder_L2() {
  if (zstd_ctx_) {
    ZSTD_freeCCtx(zstd_ctx_);
  }
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
  // 原子落盘: 写 tmp, 全部成功后 rename 到最终路径. rename 是原子的 —
  // 最终路径上的文件不可能是半截, "存在即完整"由此成立 (进程被杀/崩溃只会
  // 留下 tmp 垃圾, 由下次增量编码覆盖).
  const std::string tmp_path = filepath + ".tmp";

  // Compress using reusable context
  const size_t bound = calculate_compression_bound(data_size);
  auto compressed = std::make_unique<char[]>(bound);
  const size_t compressed_size = ZSTD_compress2(
      zstd_ctx_, compressed.get(), bound, data, data_size);

  if (ZSTD_isError(compressed_size)) [[unlikely]] {
    Logger::log("encoding", "Compression failed: " + std::string(ZSTD_getErrorName(compressed_size)));
    return false;
  }

  {
    std::ofstream file(tmp_path, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) [[unlikely]] {
      Logger::log("encoding", "Cannot open file for writing: " + tmp_path);
      return false;
    }

    L2FileHeader header{};
    header.magic = L2FileHeader::kL2Magic;
    header.version = L2FileHeader::kL2FormatVersion;
    header.raw_size = data_size;
    header.compressed_size = compressed_size;

    file.write(reinterpret_cast<const char *>(&header), sizeof(header));
    file.write(compressed.get(), compressed_size);
    file.close(); // close 才把缓冲刷给 OS, fail 位在此之后才完整

    if (file.fail()) [[unlikely]] {
      Logger::log("encoding", "Failed to write data: " + tmp_path);
      std::error_code ec;
      std::filesystem::remove(tmp_path, ec);
      return false;
    }
  }

  std::error_code ec;
  std::filesystem::rename(tmp_path, filepath, ec);
  if (ec) [[unlikely]] {
    Logger::log("encoding", "Failed to rename into place: " + filepath + " (" + ec.message() + ")");
    std::filesystem::remove(tmp_path, ec);
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

// 复用成员缓冲: 一个 worker 顺序处理成千上万个 (资产, 日期), 每次重新分配这几个
// vector 就是几千万次 malloc. clear() 保留容量.
void BinaryEncoder_L2::begin_asset() {
  csv_orders_.clear();
  csv_trades_.clear();
  orders_.clear();
  bad_line_count_ = 0;
}

bool BinaryEncoder_L2::feed_order_csv(const char *data, size_t len) {
  if (len == 0)
    return true;
  return parse_order_csv(data, len, csv_orders_);
}

bool BinaryEncoder_L2::feed_trade_csv(const char *data, size_t len) {
  if (len == 0)
    return true;
  return parse_trade_csv(data, len, csv_trades_);
}

EncodeResult BinaryEncoder_L2::finish_asset(const std::string &output_file, const std::string &tag) {
  // 源损坏优先于一切: 有坏行就整个 (资产, 日期) 作废, 不产出半真半假的
  // 产物 —— 部分数据会静默污染下游特征. 日志带上标的与坏行数, 人工修好源
  // 文件后, 下次增量因为既无 .bin 也无墓碑而自动重来.
  if (bad_line_count_ > 0) {
    Logger::log("encoding", "[CORRUPT SOURCE] " + tag + " has " +
                                std::to_string(bad_line_count_) +
                                " malformed CSV lines — skipped, source needs repair");
    return EncodeResult::CorruptSource;
  }

  // Convert and encode orders + trades
  std::vector<Order> &all_orders = orders_;
  all_orders.reserve(csv_orders_.size() + csv_trades_.size());

  for (const auto &csv : csv_orders_)
    all_orders.push_back(csv_to_order(csv));
  for (const auto &csv : csv_trades_)
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

  // 只拦"全空" (停牌日的纯表头文件) — 空 .bin 会让下游把停牌日当有数据,
  // 缺席 (墓碑) 才是正确语义. 低流动性但真实交易的标的 (如 ST 股单日几百笔)
  // 是真实市场数据, 照常编码, 要不要用是特征/因子层的策略决定, 不在存储层砍.
  //
  // 与环境错误区分开: 这是"源数据本身为空"的确定性结论, 调用方应写墓碑,
  // 增量重跑时不再对它反复解码.
  if (all_orders.empty()) {
    Logger::log("encoding", "Empty order data: " + tag + " (suspended)");
    return EncodeResult::TooFewOrders;
  }

  return encode_orders(all_orders, output_file) ? EncodeResult::Ok : EncodeResult::Error;
}

} // namespace L2
