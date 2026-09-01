#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// ============================================================================
// 逐笔数据的准入校验 — 全部判据集中在这一个文件
// ============================================================================
//
// 目的: 从逐笔委托还原订单簿之前, 先判断"这一天这个标的的逐笔流本身是否完整".
// 数据源会缺片 (整段委托丢失 / 成交文件缺失 / 归档成员位腐烂), 而 LOB 引擎对
// 缺片是沉默的 —— 它有一整套 corner case 机制 (OUT_OF_ORDER 预创建 / Level[0]
// 暂存), 缺的订单会被当成"乱序到达"悄悄吸收, 产出一个看起来正常但实际错误的
// 盘口. 所以判断必须在编码期、拿着原始 CSV 做, 不能留给下游.
//
// 校验放在编码期还有一个硬理由: 三秒快照 行情.csv 只存在于归档里, 编码后就
// 没有了. 它是交易所自己给的当日结算口径, 是逐笔流唯一的外部真值.
//
// ----------------------------------------------------------------------------
// 判据的经验依据 (config/sample/L2 的 6 个 asset-day 实测, 沪深各 3 天)
// ----------------------------------------------------------------------------
//
// 委托流完不完整, 是设计这些判据时最大的坑, 也是唯一需要分情况的地方:
//
//   有的数据源上报全部委托 —— 成交/撤单引用的 id 100% 能在委托表里找到, 于是
//   可以要求 ID 账目完全闭合 (超额扣减=0), 并且用两个标量计数器就能精确复现
//   快照的叫买/叫卖总量 (实测收盘时刻精确到股).
//
//   有的数据源不上报"即时全部成交、从未进簿"的主动单 —— 实测 52%~64% 的被引用
//   id 查不到. 这是正常特性不是缺数据, 此时账目本就无法闭合, 只剩"一笔成交的
//   买卖双方 id 不能同时查不到"这一条硬不变量 (实测 0/108664 例).
//
// 判据不按交易所代码分支, 而是按数据自身的形态自适应 (见 kStrictLedgerRatioPct):
// 单侧缺失率实测是 0% 与 52%+ 的两极分布, 中间没有样本, 阈值取 1% 两边都有 50 倍
// 余量. 这样北交所/新三板/换数据源都不需要改判据, 哪天上游开始上报全部委托,
// 严判据会自动生效. 撤单落在哪张表 (委托表 D 记录 / 成交表 C 记录) 同样按记录
// 自身判断 —— 两种编码互斥, 不需要知道是哪个交易所.
//
// 另有一类数据源占位行: 2025 年起沪深两市的逐笔委托与逐笔成交各有一条
// 自然日=0、时间=9:26:00、价量与 id 全为 0 的记录. 不跳过的话
// TradeBothMissing 会把 2025 年整个市场判死. 识别规则直接沿用 LimitOrderBook
// 已验证过的那条 —— 价与量同时为 0. 不能放宽成"量为 0 就跳过": 市价单('1')
// 与本方最优单('U') 是 price=0 而 volume>0 的正常记录 (LOB 的 SPECIAL_MAKER),
// 反过来 volume=0 而 price≠0 则是 LOB 抵扣不动的坏记录, 该由 LobUnusable 抓.
//
// ----------------------------------------------------------------------------
// 成本
// ----------------------------------------------------------------------------
//
// 编码流水线的瓶颈是 producer 那条顺序读 (整包 3.47 GB 进页缓存, 机械盘
// ~29 s/天), worker 侧只用掉其中 5~10 s. 本校验只做一遍哈希扫描 (每笔订单
// 一次哈希操作) 加一行 CSV 解析, 摊到每天不到 0.2 s, 完全藏在那条 I/O 后面.
// 表按当日实际委托数开, 只 clear 不释放, 与 encoder 的中间缓冲同一策略.

namespace L2 {

struct CSVOrder;
struct CSVTrade;

// ============================================================================
// 行情.csv 末行摘要 — 逐笔流的对照真值
// ============================================================================
//
// 末行是 15:00 的收盘快照, 累计字段已是当日终值. 价格统一换算到分 (0.01 元),
// 与 CSVTrade::price 同单位; 成交量/挂单量单位为股.
struct MarketSummary {
  bool valid = false;        // 末行解析成功 (行情.csv 存在且有数据行)
  uint32_t last_price = 0;   // 分 — 末笔成交价
  uint32_t high = 0;         // 分 — 最高价
  uint32_t low = 0;          // 分 — 最低价
  uint64_t cum_volume = 0;   // 股 — 当日累计成交量
  uint64_t cum_turnover = 0; // 元 — 当日成交额
  uint64_t bid_total = 0;    // 股 — 叫买总量 (全簿, 非十档)
  uint64_t ask_total = 0;    // 股 — 叫卖总量 (全簿, 非十档)
};

// ============================================================================
// 判据
// ============================================================================
//
// 低 16 位是门禁项: 命中任意一条就不落盘, 留日志等人查数据, 修好后增量重来.
// 高 16 位是观察项: 只记日志, 不拦 —— 它们要么口径未完全查清, 要么与门禁项
// 重合, 拦下来的误报代价 (静默拒收好数据) 大于收益.
enum Check : uint32_t {
  // -------- 门禁 --------
  // 同一挂单 id 在委托表里出现两次. 实测 0/6.
  DupMakerId = 1u << 0,
  // 撤单引用了不存在的挂单 (沪市走委托表 D 记录, 深市走成交表 C 记录).
  // 实测 0/6, 含沪市 —— 沪市虽然大量成交引用查不到, 撤单却 100% 命中.
  CancelUnresolved = 1u << 1,
  // 一笔成交的买卖双方 id 都查不到. 与委托流完不完整无关的硬不变量, 实测 0/108664.
  TradeBothMissing = 1u << 2,
  // 成交任一侧查不到. 只在委托流完整时判 (见 kStrictLedgerRatioPct).
  TradeSideMissing = 1u << 3,
  // 某挂单被扣减的量超过它挂出的量. 只在委托流完整时判 —— 否则主动单不入表会误报.
  OverConsumed = 1u << 4,
  // 行情.csv 缺失或末行解析不出来 —— 没有对照真值, 后面三条无从判断.
  MarketAbsent = 1u << 5,
  // Σ逐笔成交量 ≠ 快照当日累计成交量. 纯整数比较, 实测 6/6 精确到股.
  // 这条是成交流完整性的判决书, 逐笔成交文件整体缺失也由它兜住.
  VolumeMismatch = 1u << 6,
  // 收盘残余挂单 ≠ 快照叫买/叫卖总量. 只在委托流完整时判, 实测 3/3 精确到股.
  // 它抓的是 ID 类判据抓不到的东西: 整段委托缺失且没有成交引用过它们.
  BookMismatch = 1u << 7,
  // 源数据字段超出 Order 的位宽, 落盘时会被 clamp_to_bound 静默截断.
  // 从 csv_to_order/csv_to_trade 挪进来的 —— 那里截断无声无息, 事后无从分辨.
  // 实测这不是假想风险: 2025-01-02 全市场 5110 个资产里 36 个 (0.70%) 的价格
  // 顶到 14bit 上界 (163.83 元), 且几乎都是全天 100% 的记录 —— 高价股 (茅台、
  // 宁德、比亚迪、大批科创板) 落盘后整只票的价格信息荡然无存.
  FieldOverflow = 1u << 8,
  // LimitOrderBook 无法处理的记录: 数量为 0 (抵扣不动簿) 或定位 id 为 0
  // (找不到要抵扣的挂单). 判据照搬 LimitOrderBook.hpp:896 已验证过的那两条.
  // 挪到编码期是为了让 sequential_worker 那个 order_invalid_cnt>100 → exit(1)
  // 永远不会触发: 编码期跳掉一个资产, 好过特征计算跑到一半整个进程退出.
  LobUnusable = 1u << 9,
  // 当日价格跨度超出 LOB 档位窗口 (kPriceIndexRange), 折不进直接索引数组.
  //
  // 涨跌停决定了正常交易日不会命中
  // 真命中时先查是不是深市市价单 (委托类型 '1') 带的名义价
  PriceSpanTooWide = 1u << 10,

  // -------- 观察 --------
  // Σ(成交价×成交量) 与快照当日成交额相差超过 1 元. 实测偏差 -28~+45 分,
  // 来自快照的元级取整; B 股非整分计价可能放大偏差, 故不设门禁.
  TurnoverMismatch = 1u << 16,
  // 最高/最低/末笔价与快照不符. 实测 6/6 精确, 但与 VolumeMismatch 高度重合
  // (量对了价格几乎不会错), 留作日志线索.
  PriceMismatch = 1u << 17,
};

inline constexpr uint32_t kBlockingChecks = 0x0000FFFFu;

// 竞价交易的收盘时刻 (HHMMSSmmm). 这个时刻之后的逐笔记录属于盘后固定价格
// 交易, 解析阶段就丢弃, 不进 .bin 也不进校验.
//
// 阈值取 15:01:00: 收盘集合竞价的成交实测最晚打到 15:00:01.350, 盘后最早
// 15:05:00.210, 两边各留约 4 分钟余量.
inline constexpr uint32_t kAuctionCloseTime = 150100000;

// 成交额比对容差 (分). 快照的当日成交额是元级整数, 逐笔累加必然有取整残差.
inline constexpr int64_t kTurnoverToleranceFen = 100;

// 判定"委托流完整"的单侧缺失率上限 (百分比). 实测是 0% 与 52%+ 的两极分布,
// 取 1% 两边各有 50 倍余量. 低于此值视为委托流完整, 启用账目闭合类严判据.
inline constexpr int64_t kStrictLedgerRatioPct = 1;

struct ValidationReport {
  uint32_t flags = 0;

  // 命中数量 / 偏差量 — 只为日志定位问题规模, 不参与判定
  size_t dup_maker = 0;
  size_t cancel_unresolved = 0;
  size_t trade_both_missing = 0;
  size_t trade_side_missing = 0;
  size_t over_consumed = 0;  // 委托流不完整时照常统计, 只是不置位
  size_t field_overflow = 0; // 超出 Order 位宽的记录数
  size_t lob_unusable = 0;   // LOB 抵扣不动的记录数
  bool strict_ledger = false;

  // 当日非零价格的上下界 (分) 与由此定出的 LOB 索引基准. price_base 要跟着
  // .bin 一起落进 L2FileHeader, 解码后交给 LOB 还原档位下标.
  uint32_t price_min = 0;
  uint32_t price_max = 0;
  uint32_t price_base = 0;

  int64_t volume_delta = 0;
  int64_t turnover_delta = 0;
  int64_t bid_delta = 0;
  int64_t ask_delta = 0;

  bool blocked() const { return (flags & kBlockingChecks) != 0; }

  // 紧凑单行描述, 只列出命中的项; 无命中返回 "ok"
  std::string describe() const;
};

// ============================================================================
// 校验器 — 每个 encoding worker 持有一份, 内部哈希表跨资产复用
// ============================================================================
class Validator {
public:
  // orders/trades 是 encoder 解析出的中间结构 (未经 clamp, 是源数据原样);
  // market 由 行情.csv 末行解析而来, 无效时只跑 ID 类判据.
  void run(const std::vector<CSVOrder> &orders,
           const std::vector<CSVTrade> &trades,
           const MarketSummary &market,
           ValidationReport &out);

private:
  // 开放寻址表: id → 该挂单的剩余量与方向.
  // 用 UINT64_MAX 作空槽标记而非 0 —— 数据源里 id=0 是真实取值 (占位行).
  static constexpr uint64_t kEmptyId = UINT64_MAX;

  struct Slot {
    uint64_t id;
    int64_t remaining; // 挂出量 - 已扣减量; 负数即超额扣减
    uint8_t side;      // 0=bid 1=ask
  };

  std::vector<Slot> table_;
  uint64_t mask_ = 0;
  unsigned shift_ = 64;

  void reset(size_t expected_makers);
  Slot *lookup(uint64_t id);
  Slot *insert(uint64_t id, bool &existed);
};

} // namespace L2
