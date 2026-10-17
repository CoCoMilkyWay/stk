#pragma once

#include "L2_DataType.hpp"
#include "L2_Validator.hpp"
#include "OrderFrame.hpp"
#include <cassert>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace L2 {

// ============================================================================
// 归档成员 — 一个 (资产, 当天) 在归档里的三个 CSV
// ============================================================================
//
// 委托必有 (没有就无从重建盘口); 成交可缺; 行情不编码落盘, 只读末行做准入校验
// (见 L2_Validator.hpp).
enum class Member : uint8_t { Order,
                              Trade,
                              Market };
inline constexpr size_t kMemberCount = 3;

// ============================================================================
// Intermediate CSV Structures
// ============================================================================
//
// 纯 POD: 代码 / 交易所列不进结构 —— 沪深由 begin 一次定下, 逐行只校验代码列的
// 后缀与之相符 (不符即坏行). 只收下游 (校验 / 转换) 真正读的列, 两结构同序排布.

// Order data (逐笔委托)
struct CSVOrder {
  uint32_t time;              // HHMMSSmmm
  uint32_t price;             // in 0.01 RMB units
  uint32_t volume;            // in shares
  char order_type;            // SSE: A=add, D=delete; SZSE: varies
  char order_side;            // B=bid, S=ask
  uint64_t exchange_order_id; // 本方委托号
};

// Trade data (逐笔成交)
struct CSVTrade {
  uint32_t time;         // HHMMSSmmm
  uint32_t price;        // in 0.01 RMB units
  uint32_t volume;       // in shares
  char trade_code;       // SZSE: 0=trade, C=cancel; SSE: '0'
  char bs_flag;          // B=buy, S=sell; 深市撤单 (源为空) 解析时按非零 id 侧补齐
  uint64_t ask_order_id; // 卖方委托号
  uint64_t bid_order_id; // 买方委托号
};
static_assert(sizeof(CSVOrder) == 24 && sizeof(CSVTrade) == 32, "CSV 中间结构必须紧凑");

// Calculate max value from bitwidth
constexpr uint64_t bitwidth_to_max(uint8_t bitwidth) {
  return bitwidth > 0 ? ((1ull << bitwidth) - 1) : 0;
}

// Helper functions for safe casting with bounds checking
template <typename T>
constexpr T clamp_to_bound(uint64_t value, T bound_val) {
  return static_cast<T>(value > bound_val ? bound_val : value);
}

// Order 各字段的上界.
//
// 价格 / 类型 / 方向直接写死成 Order 位域的宽度 —— 位宽的真相只应该有一处, 就是
// Order 的定义本身. tick 的各段钳在一个字节以内, 整数序 = 时间序 (见 make_tick).
constexpr uint32_t HOUR_BOUND = 31;
constexpr uint32_t MINUTE_BOUND = 63;
constexpr uint32_t SECOND_BOUND = 63;
constexpr uint32_t MILLISECOND_BOUND = 127;      // 10ms 单位
constexpr uint32_t ORDER_TYPE_BOUND = 3;         // 2bit
constexpr uint32_t ORDER_DIR_BOUND = 1;          // 1bit
constexpr uint32_t PRICE_BOUND = (1u << 29) - 1; // 29bit, 上限 536 万元
constexpr uint32_t VOLUME_BOUND = UINT32_MAX;    // 满宽
constexpr uint64_t ORDER_ID_BOUND = UINT32_MAX;  // 32bit

// ============================================================================
// OrderEncoder — 一个 (资产, 当天) 的三个 CSV → 排好序的逐笔
// ============================================================================
//
// 写侧 OrderEncoder → FrameEncoder → DayWriter 与读侧 DayReader → FrameDecoder →
// LimitOrderBook 逐层互逆; 本类只到 FrameView 为止, 压帧 / 落盘不归它.

class OrderEncoder {
public:
  explicit OrderEncoder(size_t estimated_orders = DEFAULT_ENCODER_ORDER_SIZE);

  // ------------------------------------------------------------
  // 流式: begin → feed... → finish
  // ------------------------------------------------------------
  //
  // 一个资产的三个 CSV 由 unrar p 管道先后送达, 且共用一块复用缓冲 —— 后一个
  // 到达时前一个的原始字节已被覆盖. 所以每块到达就地解析成中间结构, 全部喂完
  // 再校验 / 折价 / 排序. CSV 不落盘 (见 misc/archive.hpp 里对落盘往返代价的说明).
  //
  // 快照 (行情.csv) 不编码落盘: 其产物全项目无人读取 —— 特征计算只吃 orders,
  // 靠 LimitOrderBook 重建盘口. 但它必须被读进来: 末行是交易所给的当日结算
  // 口径, 是逐笔流唯一的外部真值, 编码后归档就不再打开了 (见 L2_Validator.hpp).
  // 代价只有多解压那一份字节 (归档在 NVMe 上, 多路并发直读无寻道代价),
  // 且解析只碰末行.

  // code_ex 形如 "600519.SH": 沪深决定逐行字段的解法, 一个资产判一次
  void begin(const std::string &code_ex);
  void feed(Member member, const char *data, size_t len);

  // 校验 → 折价 → 排序. 只产出 Ok / Tombstone / Corrupt / Invalid.
  // tag 仅用于日志定位 (形如 "20260803 600519.SH").
  Outcome finish(const std::string &tag);

  // 上一次 finish 返回 Ok 时排好的逐笔, 在下一次 begin 前有效.
  // 全序 (时刻, 优先级 maker → taker → cancel, 源序): 同一 10ms 内的同类记录
  // 保持源文件里的先后, 同一份输入永远编出同一个帧.
  FrameView orders() const;

  // 上一次 finish 的准入校验结果 — 返回 Invalid 时, 调用方靠 flags 把这一对
  // 记进当天的账目 (见 shared/OrdersDay.hpp)
  const ValidationReport &report() const { return report_; }

  // ------------------------------------------------------------
  // Parsing / Conversion (public for testing)
  // ------------------------------------------------------------

  // 行情.csv → 末行摘要. 只从尾部回扫到第一条可解析的数据行, 不碰前面的
  // 四千多行 —— 累计字段在末行已是当日终值, 中间行对校验没有额外信息.
  static bool parse_market_tail(const char *data, size_t len, MarketSummary &summary);

  // Convert CSV structures to binary structures
  // price_base 是订单簿价位窗口的下沿, 落盘的价格都折进该窗口 (见 park_price).
  static Order csv_to_order(const CSVOrder &csv, uint32_t price_base, bool szse);
  static Order csv_to_trade(const CSVTrade &csv, uint32_t price_base, bool szse);

  // 把价格折进订单簿价位窗口 [base+1, base+kPriceIndexRange-1], 窗口外一律停靠
  // 到相应边缘; price 为 0 表示无价格 (市价单), 原样透传.
  //
  // 窗口宽 655.35 元, 而单只资产一天的成交带受涨跌停约束远窄于此, 所以被停靠的
  // 只有那些离盘口极远、根本不可能成交的报价. 对它们而言"具体是 9999 元还是
  // 999999 元"没有信息量, 只有"在买/卖方向上无穷远"这一件事是真的, 而这正是
  // 边缘价位表达的含义. 统一在落盘时折算, 使帧与簿内价位一一对应, 挂单与
  // 它后续的撤单也必然落到同一个价格上.
  static uint32_t park_price(uint32_t price, uint32_t price_base) {
    if (price == 0)
      return 0;
    if (price <= price_base)
      return price_base + 1;
    const uint32_t top = price_base + kPriceIndexRange - 1;
    return price < top ? price : top;
  }

  // 一行 CSV 切成字段, 定长不分配 (逐行一次堆分配在千万行量级上不可接受):
  // 只存前 kCsvFields 个, 返回总字段数. 下游最远只读到第 15 列 (行情.csv 的
  // 最低价), 更后面的列只需要计数 (行完整性门槛, 见 kMarketFieldCount).
  static constexpr size_t kCsvFields = 16;
  static size_t split_csv_line(std::string_view line, std::string_view (&fields)[kCsvFields]);

  // HHMMSSmmm → tick (见 make_tick), 各段钳到上界
  static uint32_t parse_tick(uint32_t time_int);
  static inline uint32_t parse_price_to_fen(std::string_view str);
  static inline uint32_t parse_volume(std::string_view str);

private:
  // ------------------------------------------------------------
  // Reusable Buffers (avoid reallocation)
  // ------------------------------------------------------------

  // 解析/合并中间结果. 一个 worker 顺序处理成千上万个 (资产, 日期),
  // 这几个 vector 只 clear 不释放, 容量涨到峰值后就不再 malloc.
  std::vector<CSVOrder> csv_orders_;
  std::vector<CSVTrade> csv_trades_;
  std::vector<uint64_t> keys_; // 排序键: 时刻 (10ms) | 优先级 | 源序下标 (委托在前, 成交在后), 见 finish
  std::vector<Order> sorted_;  // 帧序, orders() 的产物

  // 当前资产的市场 (begin 定下): 深市 = true. 逐行代码列的后缀必须与之相符.
  bool szse_ = false;

  // 当前资产累计的坏行数 (字段数不足 / 代码字段后缀与本资产市场不符). 非零即
  // 判定源损坏, finish 报 Corrupt 并带上行数, 方便定位到具体资产.
  size_t bad_line_count_ = 0;

  // 当前资产累计的"价格字段用了科学计数法"的记录数 (见 Validator::PriceScientific).
  // parse_numeric_field 不认 'e', 这种价格会被解析成 0 — 高价股整天的逐笔价格失真.
  // 在 parse_order_line/parse_trade_line 里与 bad_lines 同一行内计数 (对仗),
  // 不另起独立扫描, 也不进 CSVOrder (不污染下游).
  size_t sci_price_count_ = 0;

  // 准入校验 (见 L2_Validator.hpp). validator_ 内部的哈希表跨资产复用.
  MarketSummary market_;
  Validator validator_;
  ValidationReport report_;
};

} // namespace L2
