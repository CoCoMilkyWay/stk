#include "codec/OrderEncoder.hpp"
#include "misc/logging.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>

// Data format reference: config/sample/L2/README

namespace L2 {

// ============================================================================
// Section 1: Utility Functions (Low-level helpers)
// ============================================================================

// Fast integer parsing (avoids std::stoul/stoull overhead)
template <typename T>
static inline T fast_parse(std::string_view s) {
  T val = 0;
  for (char c : s) {
    if (c >= '0' && c <= '9') {
      val = val * 10 + static_cast<T>(c - '0');
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

// 价格字段是否用了科学计数法 (如 1.7226e+006 = 1722600).
// parse_numeric_field 不认 'e', 只读 '.' 之前的整数位就停, 再 /divisor 得到 0
// 或错误值 —— 这种源数据不该入库, 由 Validator::PriceScientific 拦下.
// 在 parse_order_line/parse_trade_line 里与 bad_lines 同处一行内计数 (对仗),
// 不另起独立扫描, 也不进 CSVOrder (不污染下游).
static inline bool price_is_scientific(std::string_view str) {
  for (const char c : str)
    if (c == 'e' || c == 'E')
      return true;
  return false;
}

// Market detection: 1 = 深, 0 = 沪, -1 = 代码字段非法.
//
// -1 只可能来自损坏的源数据: unrar 对成员报 CRC 失败时仍会吐出字节, 解出来的
// 行会整体错位粘连, 代码字段因此不成形. 解析边界拿它与本资产的市场比对, 不符
// 的行挑出来计数; 资产自己的代码 (begin) 必须成形, 那里 assert.
static inline int market_of(std::string_view stock_code) {
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

// 委托: 深市全是挂单; 沪市 D/d = 撤单, 其余 (A/a) = 挂单
static inline L2::OrderType order_type_of(const CSVOrder &csv, bool szse) {
  return (!szse && (csv.order_type == 'D' || csv.order_type == 'd')) ? L2::OrderType::CANCEL : L2::OrderType::MAKER;
}

// 成交: 深市成交代码 C = 撤单, 其余 = 主动成交
static inline L2::OrderType order_type_of(const CSVTrade &csv, bool szse) {
  return (szse && csv.trade_code == 'C') ? L2::OrderType::CANCEL : L2::OrderType::TAKER;
}

// Order direction: false=bid(B), true=ask(S)
static inline L2::OrderDirection dir_of(char side_flag) {
  return (side_flag == 'S' || side_flag == 's') ? L2::OrderDirection::ASK : L2::OrderDirection::BID;
}

// ============================================================================
// Section 2: CSV Parsing (String → Intermediate structures)
// ============================================================================

// Split CSV line into string_view fields (zero-copy, zero-allocation)
size_t OrderEncoder::split_csv_line(std::string_view line, std::string_view (&fields)[kCsvFields]) {
  const char *pos = line.data();
  const char *const end = pos + line.size();
  size_t n = 0;
  for (;;) {
    const char *comma = static_cast<const char *>(std::memchr(pos, ',', static_cast<size_t>(end - pos)));
    const char *field_end = comma == nullptr ? end : comma;
    if (n < kCsvFields)
      fields[n] = std::string_view(pos, static_cast<size_t>(field_end - pos));
    ++n;
    if (comma == nullptr)
      return n;
    pos = comma + 1;
  }
}

uint32_t OrderEncoder::parse_tick(uint32_t time_int) {
  const uint32_t ms = time_int % 1000;
  time_int /= 1000;
  const uint32_t sec = time_int % 100;
  time_int /= 100;
  const uint32_t min = time_int % 100;
  const uint32_t hour = time_int / 100;

  return make_tick(clamp_to_bound(hour, HOUR_BOUND), clamp_to_bound(min, MINUTE_BOUND),
                   clamp_to_bound(sec, SECOND_BOUND), clamp_to_bound(ms / 10, MILLISECOND_BOUND));
}

// Parse price fields (CSV value in 0.0001 RMB → 0.01 RMB units)
//
// 先在 64 位里钳到 PRICE_BOUND 再窄化: 委托里存在"不限价"哨兵, 数值可达亿元
// 量级, 直接窄化成 uint32 会回绕成一个既非真值也非哨兵的数. 钳住至少保住了
// "要多高有多高"的语义, 而这类价格随后都会被 park_price 折到窗口边缘.
inline uint32_t OrderEncoder::parse_price_to_fen(std::string_view str) {
  const uint64_t fen = parse_numeric_field(str, 100);
  return static_cast<uint32_t>(fen > PRICE_BOUND ? PRICE_BOUND : fen);
}

// Parse volume fields (shares → shares, no conversion)
inline uint32_t OrderEncoder::parse_volume(std::string_view str) {
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

  // 行结束符先探一次, 再拿它 memchr 扫全文 —— 归档里 CRLF 和裸 CR 两种都有.
  // 只认 '\n' 会把裸 CR 的文件整个读成一行: 表头被当成唯一一行跳掉, 余下所有
  // 记录粘成一坨, split_csv_line 取走开头十来个字段, 于是"成功"解析出恰好
  // 一条记录 —— 既不报坏行也不报空文件, 产物静默地只剩一条.
  const char *const first_lf =
      static_cast<const char *>(std::memchr(data, '\n', len));
  const char *const first_cr =
      static_cast<const char *>(std::memchr(data, '\r', len));
  const char delim =
      (first_cr != nullptr && (first_lf == nullptr || first_cr < first_lf)) ? '\r' : '\n';

  auto next_line = [&](std::string_view &out) -> bool {
    // 跳过上一行残留的另一半分隔符 (CRLF 的 '\n') 以及空行
    while (pos < end && (*pos == '\r' || *pos == '\n'))
      ++pos;
    if (pos >= end)
      return false;
    const char *nl =
        static_cast<const char *>(std::memchr(pos, delim, static_cast<size_t>(end - pos)));
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
    parse_func(line);
    line_count++;
  }

  return line_count > 0;
}

// 逐笔委托单行 → CSVOrder. 坏行只计数不解析 (见 market_of).
// sci_count 与 bad_lines 对仗: 同一行内计数"价格字段用了科学计数法"的记录,
// 不另起独立扫描, 也不进 CSVOrder (见 Validator::PriceScientific).
static void parse_order_line(std::string_view line, bool szse, std::vector<CSVOrder> &orders,
                             size_t &bad_lines, size_t &sci_count) {
  std::string_view fields[OrderEncoder::kCsvFields];
  if (OrderEncoder::split_csv_line(line, fields) < 10) {
    ++bad_lines;
    return;
  }

  CSVOrder order = {};
  order.time = fast_parse<uint32_t>(fields[3]);
  if (order.time >= kAuctionCloseTime)
    return; // 盘后固定价格交易, 见 kAuctionCloseTime
  order.exchange_order_id = fast_parse<uint64_t>(fields[5]);

  if (market_of(fields[0]) != (szse ? 1 : 0)) {
    ++bad_lines;
    return;
  }

  // Parse order type and side (format differs by exchange)
  if (!fields[6].empty() && fields[6][0] != ' ' && fields[6][0] != '\0') {
    order.order_type = fields[6][0];
  } else {
    order.order_type = szse ? '0' : 'A'; // Default: normal(SZSE) or add(SSE)
  }

  if (!fields[7].empty() && fields[7][0] != ' ' && fields[7][0] != '\0') {
    order.order_side = fields[7][0];
  } else {
    order.order_side = ' '; // Empty for cancellation
  }

  order.price = OrderEncoder::parse_price_to_fen(fields[8]);
  order.volume = OrderEncoder::parse_volume(fields[9]);
  if (price_is_scientific(fields[8]))
    ++sci_count;

  orders.push_back(order);
}

// 逐笔成交单行 → CSVTrade. 坏行只计数不解析 (见 market_of).
// sci_count 与 bad_lines 对仗 (见 parse_order_line).
static void parse_trade_line(std::string_view line, bool szse, std::vector<CSVTrade> &trades,
                             size_t &bad_lines, size_t &sci_count) {
  std::string_view fields[OrderEncoder::kCsvFields];
  if (OrderEncoder::split_csv_line(line, fields) < 12) {
    ++bad_lines;
    return;
  }

  CSVTrade trade = {};
  trade.time = fast_parse<uint32_t>(fields[3]);
  if (trade.time >= kAuctionCloseTime)
    return; // 盘后固定价格交易, 见 kAuctionCloseTime

  if (market_of(fields[0]) != (szse ? 1 : 0)) {
    ++bad_lines;
    return;
  }

  // Parse trade code (SSE doesn't use it)
  trade.trade_code = (szse && !fields[5].empty()) ? fields[5][0] : '0';

  trade.price = OrderEncoder::parse_price_to_fen(fields[8]);
  trade.volume = OrderEncoder::parse_volume(fields[9]);
  trade.ask_order_id = fast_parse<uint64_t>(fields[10]);
  trade.bid_order_id = fast_parse<uint64_t>(fields[11]);

  // BS flag: 深市撤单的源字段为空, 按非零 id 侧补齐 —— 之后与委托的 order_side 同一解法
  trade.bs_flag = !fields[7].empty() ? fields[7][0] : ' ';
  if (szse && (trade.bs_flag == ' ' || trade.bs_flag == '\0'))
    trade.bs_flag = (trade.bid_order_id != 0) ? 'B' : 'S';

  if (price_is_scientific(fields[8]))
    ++sci_count;

  trades.push_back(trade);
}

// 行情.csv 的字段布局 (表头见 config/sample/L2/README). 只取校验用得上的.
namespace {
// 快照行的最小字段数. 取得比实际读到的列 (最低价, 下标 14) 宽得多 —— 它同时
// 当作行完整性的门槛: 列数不足说明这行是截断的, 不该拿来当真值.
constexpr size_t kMarketFieldCount = 61;
constexpr size_t kMarketDate = 2;
constexpr size_t kMarketLastPrice = 4;
constexpr size_t kMarketCumVolume = 11;
constexpr size_t kMarketCumTurnover = 12;
constexpr size_t kMarketHigh = 13;
constexpr size_t kMarketLow = 14;

// kMarketTurnoverBrokenYuan 的文本形式, 只算一次 —— parse_numeric_field 不认
// 负号, 判定"卡死哨兵值"只能在换算前拿原始字段文本直接比对.
const std::string kTurnoverBrokenText = std::to_string(kMarketTurnoverBrokenYuan);
} // namespace

static_assert(kMarketLow < OrderEncoder::kCsvFields, "行情.csv 要读的列超出了定长切片");

bool OrderEncoder::parse_market_tail(const char *data, size_t len, MarketSummary &summary) {
  summary = MarketSummary{};
  if (data == nullptr || len == 0)
    return false;

  // 从尾部往前逐行回扫. 停牌日的 行情.csv 只有表头, 回扫到表头就停 —— 表头
  // 的"自然日"列不含数字, fast_parse 给 0, 以此与数据行区分.
  //
  // 但"最后一行"不等于"收盘行": 部分板块收盘后还会再推若干行占位快照, 其当日
  // 累计量与成交额都是 0. 取到这种行, 当日累计量就成了 0, 逐笔累加与它的差恰好
  // 等于全天成交量. 所以要一直回扫到累计量非零的那一行.
  //
  // 全程没有非零行时退回末行 (have_fallback): 那是真的一天没有成交, 让它与
  // Σ逐笔成交量=0 对上, 而不是误报成 MarketAbsent.
  bool have_fallback = false;
  const char *line_end = data + len;
  while (line_end > data) {
    while (line_end > data && (line_end[-1] == '\n' || line_end[-1] == '\r'))
      --line_end;
    if (line_end == data)
      break;

    const char *line_begin = line_end;
    while (line_begin > data && line_begin[-1] != '\n' && line_begin[-1] != '\r')
      --line_begin;

    const std::string_view line(line_begin, static_cast<size_t>(line_end - line_begin));
    std::string_view fields[kCsvFields];
    if (split_csv_line(line, fields) >= kMarketFieldCount && fast_parse<uint32_t>(fields[kMarketDate]) != 0) {
      const auto cum_volume = parse_numeric_field(fields[kMarketCumVolume], 1);
      if (cum_volume != 0 || !have_fallback) {
        summary.valid = true;
        summary.last_price = parse_price_to_fen(fields[kMarketLastPrice]);
        summary.high = parse_price_to_fen(fields[kMarketHigh]);
        summary.low = parse_price_to_fen(fields[kMarketLow]);
        summary.cum_volume = cum_volume;
        // parse_numeric_field 只认数字, 不认负号 —— 卡死哨兵值 (-2147483648) 会被
        // 它读成 0, 所以要在换算前拿原始字段文本单独判一次.
        summary.turnover_broken = fields[kMarketCumTurnover] == kTurnoverBrokenText;
        summary.cum_turnover = parse_numeric_field(fields[kMarketCumTurnover], 1);
        summary.turnover_capped = summary.cum_turnover == kMarketTurnoverCapYuan;
        if (cum_volume != 0)
          return true;
        have_fallback = true; // 记下末行, 继续往前找真正的收盘行
      }
    }

    line_end = line_begin;
  }

  return summary.valid;
}

// ============================================================================
// Section 3: Data Conversion (CSV structures → Binary structures)
// ============================================================================

Order OrderEncoder::csv_to_order(const CSVOrder &csv, uint32_t price_base, bool szse) {
  Order order = {};
  order.tick = parse_tick(csv.time);
  order.order_type = order_type_of(csv, szse);
  order.order_dir = dir_of(csv.order_side);
  order.price = park_price(csv.price, price_base);
  order.volume = clamp_to_bound(csv.volume, VOLUME_BOUND);

  // 委托只有本方一侧 id
  const uint32_t id = clamp_to_bound(csv.exchange_order_id, ORDER_ID_BOUND);
  order.bid_order_id = order.order_dir == L2::OrderDirection::BID ? id : 0;
  order.ask_order_id = order.order_dir == L2::OrderDirection::ASK ? id : 0;
  return order;
}

Order OrderEncoder::csv_to_trade(const CSVTrade &csv, uint32_t price_base, bool szse) {
  Order order = {};
  order.tick = parse_tick(csv.time);
  order.order_type = order_type_of(csv, szse);
  order.order_dir = dir_of(csv.bs_flag);
  order.price = park_price(csv.price, price_base);
  order.volume = clamp_to_bound(csv.volume, VOLUME_BOUND);

  // 成交两侧 id 都有
  order.bid_order_id = clamp_to_bound(csv.bid_order_id, ORDER_ID_BOUND);
  order.ask_order_id = clamp_to_bound(csv.ask_order_id, ORDER_ID_BOUND);
  return order;
}

// ============================================================================
// Section 4: Frame Order (时刻 → 优先级 → 源序)
// ============================================================================

// 帧序键: [tick 32bit][优先级 2bit][源序下标 30bit]. 源序 = 委托在前, 成交在后.
// tick 的整数序即时间序 (见 make_tick). 下标兜底使键两两不等, 排序结果与 std::sort
// 的实现无关 —— 同一份输入永远编出同一个帧. 优先级 maker → taker → cancel.
// 键直接从中间结构算, 与 csv_to_order / csv_to_trade 同一套 tick / 类型解法.
constexpr size_t kFrameKeyIndexBits = 30;

template <typename CSV>
static inline uint64_t frame_key(const CSV &csv, bool szse, size_t idx) {
  const OrderType type = order_type_of(csv, szse);
  const uint64_t priority = type == MAKER ? 0 : type == TAKER ? 1
                                            : type == CANCEL  ? 2
                                                              : 3;
  return (uint64_t(OrderEncoder::parse_tick(csv.time)) << 32) | (priority << kFrameKeyIndexBits) | idx;
}

// ============================================================================
// Section 5: High-Level Interface (begin → feed... → finish)
// ============================================================================

OrderEncoder::OrderEncoder(size_t est_orders) {
  // 解析/合并中间结果 (每个 worker 复用一份, 见 hpp)
  csv_orders_.reserve(est_orders);
  csv_trades_.reserve(est_orders);
  keys_.reserve(est_orders * 2);
  sorted_.reserve(est_orders * 2);
}

// 复用成员缓冲: 一个 worker 顺序处理成千上万个 (资产, 日期), 每次重新分配这几个
// vector 就是几千万次 malloc. clear() 保留容量.
void OrderEncoder::begin(const std::string &code_ex) {
  const int market = market_of(code_ex);
  assert(market >= 0 && "begin: 资产代码必须以 .SZ / .SH 结尾");
  szse_ = market == 1;

  csv_orders_.clear();
  csv_trades_.clear();
  keys_.clear();
  sorted_.clear();
  bad_line_count_ = 0;
  sci_price_count_ = 0;
  market_ = MarketSummary{};
  report_ = ValidationReport{};
}

void OrderEncoder::feed(Member member, const char *data, size_t len) {
  if (len == 0)
    return;
  switch (member) {
  case Member::Order:
    parse_csv_buffer(data, len, [this](std::string_view line) {
      parse_order_line(line, szse_, csv_orders_, bad_line_count_, sci_price_count_);
    });
    return;
  case Member::Trade:
    parse_csv_buffer(data, len, [this](std::string_view line) {
      parse_trade_line(line, szse_, csv_trades_, bad_line_count_, sci_price_count_);
    });
    return;
  case Member::Market:
    parse_market_tail(data, len, market_);
    return;
  }
  assert(false && "feed: 未覆盖的 Member");
}

FrameView OrderEncoder::orders() const {
  return {sorted_.data(), {static_cast<uint32_t>(sorted_.size()), report_.price_base}};
}

Outcome OrderEncoder::finish(const std::string &tag) {
  // 源损坏优先于一切: 有坏行就整个 (资产, 日期) 作废, 不产出半真半假的
  // 产物 —— 部分数据会静默污染下游特征. 日志带上标的与坏行数, 人工修好源
  // 文件后, 下次增量因为既无帧也无墓碑而自动重来.
  if (bad_line_count_ > 0) {
    Logger::log("encoding", "[CORRUPT SOURCE] " + tag + " has " +
                                std::to_string(bad_line_count_) +
                                " malformed CSV lines — skipped, source needs repair");
    return Outcome::Corrupt;
  }

  // 只拦"全空" (停牌日的纯表头文件) — 空帧会让下游把停牌日当有数据,
  // 缺席 (墓碑) 才是正确语义. 低流动性但真实交易的标的 (如 ST 股单日几百笔)
  // 是真实市场数据, 照常编码, 要不要用是特征/因子层的策略决定, 不在存储层砍.
  //
  // 与环境错误区分开: 这是"源数据本身为空"的确定性结论, 调用方应写墓碑,
  // 增量重跑时不再对它反复解码. 必须先于校验判定 —— 停牌日的 行情.csv 同样
  // 只有表头, 走进校验只会被 MarketAbsent 误判成待人工处理的数据问题.
  if (csv_orders_.empty() && csv_trades_.empty()) {
    Logger::log("encoding", "Empty order data: " + tag + " (suspended)");
    return Outcome::Tombstone;
  }

  // 准入校验 — 判据与经验依据见 L2_Validator.hpp.
  //
  // 用中间结构而非转换后的 Order: 中间结构是源数据原样, 而转换会把价格折进档位
  // 窗口、把其余字段钳到位宽上界, 拿加工过的值去和快照对拍等于自己骗自己.
  //
  // 也必须先于转换: 档位窗口的基准由校验算出, 转换要拿它折价.
  validator_.run(csv_orders_, csv_trades_, market_, report_);

  // 科学计数法价格在 parse_numeric_field 里被解析成 0, 校验器拿到的 CSVOrder 已无原始
  // 字符串, 无法在 run() 里判 (price=0 与市价单('1'/'U') 不可区分). 这里用 feed 阶段
  // 独立扫描原始缓冲累计的计数补进 report_, 命中即拦 (见 Validator::PriceScientific).
  if (sci_price_count_ > 0) {
    report_.price_scientific = sci_price_count_;
    report_.flags |= Check::PriceScientific;
  }

  if (report_.blocked()) {
    Logger::log("encoding", "[INVALID DATA] " + tag + " — " + report_.describe() +
                                " — skipped, source needs check");
    return Outcome::Invalid;
  }

  // 排键 (源序: 委托在前, 成交在后) → 按键序就地转换成帧序.
  // 排 8 字节的键而不是 20 字节的 Order, 比较只是一次整数比较; 转换直接写进
  // sorted_, 不经源序的 Order 中间缓冲.
  const size_t n_orders = csv_orders_.size();
  const size_t n_trades = csv_trades_.size();
  const size_t n = n_orders + n_trades;
  assert(n < (size_t(1) << kFrameKeyIndexBits) && "finish: 单日逐笔条数装不进帧序键的下标段");
  keys_.resize(n);
  for (size_t i = 0; i < n_orders; ++i)
    keys_[i] = frame_key(csv_orders_[i], szse_, i);
  for (size_t i = 0; i < n_trades; ++i)
    keys_[n_orders + i] = frame_key(csv_trades_[i], szse_, n_orders + i);
  std::sort(keys_.begin(), keys_.end());

  constexpr uint64_t kIndexMask = (uint64_t(1) << kFrameKeyIndexBits) - 1;
  sorted_.resize(n);
  for (size_t i = 0; i < n; ++i) {
    const size_t idx = keys_[i] & kIndexMask;
    sorted_[i] = idx < n_orders ? csv_to_order(csv_orders_[idx], report_.price_base, szse_)
                                : csv_to_trade(csv_trades_[idx - n_orders], report_.price_base, szse_);
  }

  return Outcome::Ok;
}

} // namespace L2
