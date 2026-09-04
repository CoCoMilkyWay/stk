#pragma once

#include "codec/L2_DataType.hpp"
#include <array>
#include <cstddef>
#include <cstdint>

// 核心理念: 在低信噪比、强竞争的二级市场，端到端的深度模型(数据先验)会先被淘汰, 特征工程因子挖掘(结构性先验)是生存条件，不是选择。

// 在手工原始特征设计阶段:
//  1. 高频特征可以有低频能量(原始特征级别只是数学定义所在的级别), 且不需要手动频域切分(后面会自动化处理)
//  2. 选择合适的归一化, 过滤掉尽可能少的不平稳的最低频信号, 让时序平稳 (保留尽可能多的平稳的低频信息, 这部分是因子主体的主要构成)
//  3. 保证特征序列概率分布在时间/截面上的稳定性(时间上不漂移(因子不衰减), 截面分布一致(可以截面中性化))
// 在符号回归阶段:
//  1. GPU会系统性地对所有手工特征做检测, 得到对应的平稳性, 分布, 频域, 自相关特性
//  2. 用算子池逐个特征测试, 展开得到若干新特征, 然后再次统计新特征的特性, 然后过滤掉不合格的, 自动生成针对原始特征的算子池约束
//  3. 计算展开后的特征池的相关性矩阵(时序相关性(自相关), 截面相关性(交叉相关))
//  4. 根据上面的信息, 符号回归优化Loss(Label), 实现高效因子挖掘和稳健性测试
//
// 完整的因子由两部分构成:
// 频率: 秒 -------------------------------- 分钟 ------------ 小时 ------------- 天
//       <------------------------------------>|<--------------------------->| (更低频信号弃用(不平稳))
//        进出场触发器(高频特征的简单组合)        因子主体(低频特征的复杂组合)
//        非必须. 只是超额                        核心的核心, 盈利主体
//        (为因子主体提供稳定超额, 流动性支持)    (决定因子强度, 周期, 平稳性(分层), 换手)

// ============================================================================
// FORMULA NOTATION CONVENTION (公式符号规范)
// ============================================================================
//
// 【深度 (LOB Depth)】
//   符号结构:  {P, V}_{i,t}^{M,s}
//   下标:
//     i = 档位 (1,2,...,N)
//     t = 时刻
//   上标:
//     M = 固定 (Maker, 与订单流符号一致)
//     s ∈ {B, A}  (B=Bid 买盘, A=Ask 卖盘)
//   例:  P_{1,t}^{M,B} = t 时刻买一价,  V_{5,t}^{M,A} = t 时刻卖五量
//
// 【订单流 (Order Flow)】
//   符号结构:  O_{时间}^{e,s(,c)[\mathrm{cond}]}
//   下标 (时间聚合, 选一):
//     t            = 时刻 (单个 tick)
//     Δt           = 时间分箱 (bin)
//     W            = 滚动窗口
//     ∑_{τ=t₀}^{t} = 开盘累计
//   上标 (按顺序):
//     e ∈ {T, M, C}         事件类型: T=Taker(主动成交), M=Maker(挂单), C=Cancel(撤单)
//     s ∈ {B, A}            侧向: B=Bid 买盘, A=Ask 卖盘
//     c ∈ {XL, L, Mid, S}   大小单分类
//     [\mathrm{cond}]       条件筛选 (如 CA=连续竞价, fleet=闪单)
//   算子:
//     |O|  = 取量 (volume)
//     #O   = 取笔数 (count)
//   例:
//     |O_t^{T,B}|                = t 时刻主动买成交量
//     #O_{\Delta t}^{M,A}        = Δt 内卖方挂单笔数
//     |O_t^{T,B,XL}|             = t 时刻特大单主动买成交量
//     |O_W^{T,\mathrm{CA}}|      = W 窗口连续竞价成交量
//     ∑_{τ=t₀}^{t}|O_τ^{T,B}|    = 开盘到 t 的累计主动买成交量
//

// ============================================================================
// 特征定义: 算子文件自带节点实例 + 落盘列 (Operator/TS/<类别>/<Op>.hpp 末尾), 没有中心表
// ============================================================================
// 算子文件 = 数学 (class) + 文件末尾两种宏 (CMake 扫描汇总, C++ 里不直接展开):
//
//   #define NODE_<Name>(N)  N(<Name>, (OpType), (inputs...), compute_trigger, flush_trigger)
//     Name     节点名 (DAG 成员名), 同一算子可有多个实例 (CI.hpp: Ci_1 / Ci_5 / Ci_10 / Ci_30)
//     OpType   算子类型, 必须加括号 (模板参数里有逗号)
//     inputs   构造参数 (不含输出口), 引用 DAG 成员: tick_data / minute_data / asset_code_ / fund_row_ /
//              上游节点: 单口 Up.out(), 多口 Up.out(Up.port), 源层数组 DepthData.bid_qty 等
//     触发域   Trigger:: 下的名字. compute 在 compute_trigger 域调, flush 在 flush_trigger 域调;
//              采样型算子两者相同, 降频型算子 compute=onTick/flush=onMinute
//     依赖     就是 inputs 里出现的 "Up." — 不需要写别的, 也不需要 #include 上游算子
//
//   #define FIELDS_<LVL>_<Name>(X)  X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method,
//                                     PSD, name_en, name_cn, description, formula, SRC) ...
//     LVL ∈ {L0, L1, DEPTH}: 落盘层. 可无 (纯中间节点), 可多层. 一行 = 一个落盘列.
//     SRC 这一列的值从哪来 (基建按它生成写回 / 截面配置 / 广播):
//       OP(Node) / OP(Node, port)  节点输出口 (Node::Out::value / Node::Out::port); 层必须 == 节点 flush 域
//       FUND(field)                当日基本面行 fund::field 盘中广播 (仅 L1)
//       CS(lvl, src, tf, m)        截面: 源层 lvl (0/1) 的字段 src → cs::Transform::tf → cs::Method::m
//       META / LABEL               基建 / 标签回填 手工写
//     非算子列的 <Name> 是任意名字, 放在写它的地方旁边: META → Operator/TS/Meta/Meta.hpp,
//     LABEL → Operator/TS/Label/LabelReturn.hpp, FUND → Fundamental/FundamentalDaily.hpp, CS → Misc/CSMethods.hpp.
//
// CMake (projects/main/CMakeLists.txt) 扫描 features/{Operator,Fundamental,Misc}/**/*.hpp, 按 inputs 引用
// 分层拓扑排序, 生成 build/generated/features/NodesGenerated.hpp: 全部 #include + NODES(N) +
// LEVEL_{0,1}_FIELDS(X) + DEPTH_FIELDS(X). 基建 (ComputeGraph / *_Sequential / *_Crosssection /
// FeatureStore / Feature.hpp GUI 元数据) 全部由这几张表展开, 不需要手改. 改动后下次 build 自动重新 configure.
//
// 加特征 = 改 (或新建) 一个算子文件. 删特征 = 删几行 / 删文件. 其他地方不动.
// 顺序保证: DAG 按拓扑序声明成一条链, 节点只看得到排在自己前面的节点 → 引用了排后面的节点 = 编译错误.
// 各触发域一个 tick 内的执行顺序: onTaker|onMaker|onCancel → onTick → onDepth; 分钟边界 → onMinute.
// 字段表改动会改变落盘布局: 文件头带表指纹, 读旧文件即断言失败, 需重算.
// 标签 (LabelReturn*) 回填别的时间行, 不走节点表, 在 ComputeGraph.hpp 手工声明.

// ============================================================================
// FEATURE METADATA ENCODING SYSTEM
// ============================================================================

// Data type classification
enum class FeatureDataType : uint8_t {
  TS = 0,  // Time-series (时序)
  CS = 1,  // Cross-sectional (截面)
  LB = 2,  // Label (标签)
  SH = 3,  // Shared (共享值)
  META = 4 // Metadata (backend系统元数据)
};

// Primary category
enum class FeatureCategoryL1 : uint8_t {
  IMBALANCE = 0,  // 失衡
  SHAPE = 1,      // 形状
  ORDER_FLOW = 2, // 订单流
  BEHAVIORAL = 3, // 行为
  RESILIENCE = 4, // 韧性
  LIQUIDITY = 5,  // 流动性
  VOLATILITY = 6, // 波动率
  BASIC = 7,      // 基础
  LABEL = 8,      // 标签/目标
  META = 9        // 元数据/共享变量
};

// Secondary category
enum class FeatureCategoryL2 : uint8_t {
  RAW = 0,        // 原始
  NORMALIZED = 1, // 标准化
  OSCILLATOR = 2, // 震荡器
  DEVIATION = 3,  // 偏离
  RATIO = 4,      // 比率
  RANK = 5,       // 排名
  FUTURE_RET = 6, // 未来收益
  SCORE = 7,      // 评分
  UNIVERSE = 8,   // 全域统计
  BENCHMARK = 9   // 基准/市场
};

// Normalization method
enum class NormMethod : uint8_t {

  // --- identity ---
  NONE = 0, // x

  // --- scale (linear) ---
  ZSCORE = 1,        // (x - mean) / std
  ROBUST_ZSCORE = 2, // (x - median) / MAD
  IQR_ZSCORE = 3,    // (x - Q2) / (Q3 - Q1)

  // --- order based ---
  RANK = 4,        // rank / N
  RANK_ZSCORE = 5, // rank → inverse normal

  // --- bounding ---
  CLIP = 6,   // clip(x, [-k, k])
  WINSOR = 7, // winsorize by percentile

  // --- nonlinear transform ---
  LOG = 8,    // log/log1p
  POWER = 9,  // x^α
  ASINH = 10, // asinh(x)
  TANH = 11,  // tanh(x)

  // --- encoding / embedding ---
  SINCOS = 12, // x → (sin, cos)

  // --- composite (common pipelines) ---
  LOG_ZSCORE = 20,   // log/log1p → zscore
  POWER_ZSCORE = 21, // power → zscore
  ASINH_ZSCORE = 22, // asinh → zscore

  CLIP_ZSCORE = 23,     // zscore → clip
  WINSOR_ZSCORE = 24,   // winsor → zscore
  CLIP_LOG_ZSCORE = 25, // clip → log → zscore
};

// ============================================================================
// ALL LEVELS REGISTRY
// ============================================================================
// Format: X(level_name, level_index, fields_macro)

#define ALL_LEVELS(X)      \
  X(L0, 0, LEVEL_0_FIELDS) \
  X(L1, 1, LEVEL_1_FIELDS)

// ============================================================================
// TIME GRANULARITY CONFIGURATION
// ============================================================================

constexpr size_t TRADE_MINUTES_PER_DAY = 255;                        // 9:15-11:30 (135min) + 13:00-15:00 (120min)
constexpr size_t TRADE_SECONDS_PER_DAY = TRADE_MINUTES_PER_DAY * 60; // 15300 seconds

// Time unit types
enum class TimeUnit : uint8_t {
  MILLISECOND = 0,
  SECOND = 1,
  MINUTE = 2,
  HOUR = 3
};

// Level time configuration
struct LevelTimeConfig {
  TimeUnit unit;
  size_t interval; // Number of units per time index

  constexpr size_t max_capacity() const {
    switch (unit) {
    case TimeUnit::MILLISECOND:
      return (TRADE_SECONDS_PER_DAY * 1000) / interval + 1;
    case TimeUnit::SECOND:
      return TRADE_SECONDS_PER_DAY / interval + 1;
    case TimeUnit::MINUTE:
      return (TRADE_SECONDS_PER_DAY / 60) / interval + 1;
    case TimeUnit::HOUR:
      return (TRADE_SECONDS_PER_DAY / 3600) / interval + 1;
    }
    return TRADE_SECONDS_PER_DAY + 1;
  }
};

// Predefined level configurations
constexpr LevelTimeConfig LEVEL_CONFIGS[2] = {
    {TimeUnit::SECOND, 1}, // L0: 1s
    {TimeUnit::MINUTE, 1}  // L1: 1min
};

// ============================================================================
// TRADING SESSION CONSTANTS
// ============================================================================
// A股交易时段 (含集合竞价):
//
//   时段       时钟时间         分钟数    秒数      L0 范围        L1 范围
//   ──────────────────────────────────────────────────────────────────────
//   上午       09:15 - 11:30    135 min   8100 s    [0, 8099]      [0, 134]
//   午休       11:30 - 13:00    (非交易)
//   下午       13:00 - 15:00    120 min   7200 s    [8100, 15299]  [135, 254]
//   ──────────────────────────────────────────────────────────────────────
//   合计                        255 min   15300 s
//
// 关键边界值:
//   MORNING_SECONDS = 8100   (上午总秒数, 也是下午 L0 起点)
//   MORNING_MINUTES = 135    (上午总分钟数, 也是下午 L1 起点)

constexpr uint16_t MORNING_START_MIN = L2::MORNING_CALL_AUCTION_START_HOUR * 60 + L2::MORNING_CALL_AUCTION_START_MINUTE;                   // 555 (09:15)
constexpr uint16_t MORNING_END_MIN = L2::CONTINUOUS_TRADING_MORNING_END_HOUR * 60 + L2::CONTINUOUS_TRADING_MORNING_END_MINUTE;             // 690 (11:30)
constexpr uint16_t AFTERNOON_START_MIN = L2::CONTINUOUS_TRADING_AFTERNOON_START_HOUR * 60 + L2::CONTINUOUS_TRADING_AFTERNOON_START_MINUTE; // 780 (13:00)
constexpr uint16_t AFTERNOON_END_MIN = L2::CONTINUOUS_TRADING_AFTERNOON_END_HOUR * 60 + L2::CONTINUOUS_TRADING_AFTERNOON_END_MINUTE;       // 900 (15:00)

constexpr size_t MORNING_SECONDS = 8100; // 135 min × 60 = 上午交易秒数
constexpr size_t MORNING_MINUTES = 135;  // 上午交易分钟数

// ============================================================================
// INTERNAL: Compile-time LUT for Clock → L0
// ============================================================================
// 预计算 1440 个 (hour, minute) → L0 base offset 的映射表
// 运行时只需 O(1) 查表 + 加秒数

namespace detail {

// 返回该 (hour, minute) 对应的 L0 base (不含秒)
// 返回 -1 表示盘前, 返回 15299 表示盘后
constexpr int16_t minute_offset(uint8_t hour, uint8_t minute) {
  const uint16_t m = hour * 60 + minute;
  if (m >= MORNING_START_MIN && m < MORNING_END_MIN) // 09:15-11:29
    return static_cast<int16_t>((m - MORNING_START_MIN) * 60);
  if (m >= AFTERNOON_START_MIN && m < AFTERNOON_END_MIN) // 13:00-14:59
    return static_cast<int16_t>(MORNING_SECONDS + (m - AFTERNOON_START_MIN) * 60);
  if (m >= MORNING_END_MIN && m < AFTERNOON_START_MIN) // 11:30-12:59 午休
    return static_cast<int16_t>(MORNING_SECONDS);      // → 映射到下午开盘
  if (m < MORNING_START_MIN)                           // 00:00-09:14 盘前
    return -1;
  return static_cast<int16_t>(TRADE_SECONDS_PER_DAY - 1); // 15:00+ 盘后
}

constexpr auto generate_minute_offset_table() {
  std::array<int16_t, 24 * 60> table{};
  for (size_t i = 0; i < 24 * 60; ++i)
    table[i] = minute_offset(static_cast<uint8_t>(i / 60), static_cast<uint8_t>(i % 60));
  return table;
}

} // namespace detail

// 编译期生成的查表 (1440 entries × 2 bytes = 2.88 KB)
static constexpr auto MINUTE_OFFSET_LUT = detail::generate_minute_offset_table();

// ============================================================================
// CLOCK TIME STRUCTURE
// ============================================================================

struct ClockTime {
  uint8_t hour = 0;
  uint8_t minute = 0;
  uint8_t second = 0;
};

// ============================================================================
// TIME INDEX CONVERSION
// ============================================================================
// 命名规则: X2Y 表示 X → Y
//
// 两级索引体系:
//   L0 (tick)   : 0-15299  秒级索引
//   L1 (minute) : 0-254    分钟级索引
//
// L1 分钟边界 (相对于 L0):
//   上午: L1=0 → L0=[0,59], L1=1 → L0=[60,119], ..., L1=134 → L0=[8040,8099]
//   下午: L1=135 → L0=[8100,8159], ..., L1=254 → L0=[15240,15299]

// -------------------------------- 降采样 --------------------------------
// Clock → L0 → L1

// Clock → L0: 09:15:00→0, 11:29:59→8099, 13:00:00→8100, 14:59:59→15299
inline constexpr size_t Clock_to_L0(uint8_t hour, uint8_t minute, uint8_t second) {
  const int16_t base = MINUTE_OFFSET_LUT[hour * 60 + minute];
  // Branchless: base<0 (盘前) 时右移 15 位得全 1, 取反 AND 后清零
  const size_t clamped = base & ~(base >> 15);
  const size_t result = clamped + second;
  return (result < TRADE_SECONDS_PER_DAY) ? result : (TRADE_SECONDS_PER_DAY - 1);
}

// L0 → L1: 0→0, 59→0, 60→1, 8099→134, 8100→135, 15299→254
inline constexpr size_t L0_to_L1(size_t l0_idx) {
  return (l0_idx < MORNING_SECONDS)
             ? (l0_idx / 60)
             : (MORNING_MINUTES + (l0_idx - MORNING_SECONDS) / 60);
}

// -------------------------------- 升采样 --------------------------------
// L1 → L0 → Clock

// L1 → L0: 0→0, 134→8040, 135→8100, 254→15240 (返回该分钟的起始秒索引)
inline constexpr size_t L1_to_L0(size_t l1_idx) {
  return (l1_idx < MORNING_MINUTES)
             ? (l1_idx * 60)
             : (MORNING_SECONDS + (l1_idx - MORNING_MINUTES) * 60);
}

// L0 → Clock: 0→09:15:00, 8099→11:29:59, 8100→13:00:00, 15299→14:59:59
inline constexpr ClockTime L0_to_Clock(size_t l0_idx) {
  ClockTime t;
  if (l0_idx < MORNING_SECONDS) {
    size_t total = MORNING_START_MIN * 60 + l0_idx;
    t.hour = static_cast<uint8_t>(total / 3600);
    t.minute = static_cast<uint8_t>((total % 3600) / 60);
    t.second = static_cast<uint8_t>(total % 60);
  } else {
    size_t total = AFTERNOON_START_MIN * 60 + (l0_idx - MORNING_SECONDS);
    t.hour = static_cast<uint8_t>(total / 3600);
    t.minute = static_cast<uint8_t>((total % 3600) / 60);
    t.second = static_cast<uint8_t>(total % 60);
  }
  return t;
}

// L1 → Clock: 0→09:15, 134→11:29, 135→13:00, 254→14:59
inline constexpr ClockTime L1_to_Clock(size_t l1_idx) {
  ClockTime t;
  t.second = 0;
  if (l1_idx < MORNING_MINUTES) {
    size_t total = MORNING_START_MIN + l1_idx;
    t.hour = static_cast<uint8_t>(total / 60);
    t.minute = static_cast<uint8_t>(total % 60);
  } else {
    size_t total = AFTERNOON_START_MIN + (l1_idx - MORNING_MINUTES);
    t.hour = static_cast<uint8_t>(total / 60);
    t.minute = static_cast<uint8_t>(total % 60);
  }
  return t;
}

// ============================================================================
// FORMAT UTILITIES
// ============================================================================

inline void format_time(char *buf, size_t buf_size, const ClockTime &t) {
  std::snprintf(buf, buf_size, "%02d:%02d:%02d", t.hour, t.minute, t.second);
}

inline void format_time_hm(char *buf, size_t buf_size, const ClockTime &t) {
  std::snprintf(buf, buf_size, "%02d:%02d", t.hour, t.minute);
}

// ============================================================================
// ENUM TO STRING
// ============================================================================

struct EnumStr {
  const char *en;
  const char *cn;
};

inline constexpr EnumStr to_string(FeatureDataType t) {
  switch (t) {
  case FeatureDataType::TS:
    return {"TS", "时序"};
  case FeatureDataType::CS:
    return {"CS", "截面"};
  case FeatureDataType::LB:
    return {"LB", "标签"};
  case FeatureDataType::SH:
    return {"SH", "共享"};
  case FeatureDataType::META:
    return {"META", "元数据"};
  }
  return {"?", "未知"};
}

inline constexpr EnumStr to_string(FeatureCategoryL1 t) {
  switch (t) {
  case FeatureCategoryL1::IMBALANCE:
    return {"IMBALANCE", "失衡"};
  case FeatureCategoryL1::SHAPE:
    return {"SHAPE", "形状"};
  case FeatureCategoryL1::ORDER_FLOW:
    return {"ORDER_FLOW", "订单流"};
  case FeatureCategoryL1::BEHAVIORAL:
    return {"BEHAVIORAL", "行为"};
  case FeatureCategoryL1::RESILIENCE:
    return {"RESILIENCE", "韧性"};
  case FeatureCategoryL1::LIQUIDITY:
    return {"LIQUIDITY", "流动性"};
  case FeatureCategoryL1::VOLATILITY:
    return {"VOLATILITY", "波动率"};
  case FeatureCategoryL1::BASIC:
    return {"BASIC", "基础"};
  case FeatureCategoryL1::LABEL:
    return {"LABEL", "标签"};
  case FeatureCategoryL1::META:
    return {"META", "元数据"};
  }
  return {"?", "未知"};
}

inline constexpr EnumStr to_string(FeatureCategoryL2 t) {
  switch (t) {
  case FeatureCategoryL2::RAW:
    return {"RAW", "原始"};
  case FeatureCategoryL2::NORMALIZED:
    return {"NORMALIZED", "标准化"};
  case FeatureCategoryL2::OSCILLATOR:
    return {"OSCILLATOR", "震荡器"};
  case FeatureCategoryL2::DEVIATION:
    return {"DEVIATION", "偏离"};
  case FeatureCategoryL2::RATIO:
    return {"RATIO", "比率"};
  case FeatureCategoryL2::RANK:
    return {"RANK", "排名"};
  case FeatureCategoryL2::FUTURE_RET:
    return {"FUTURE_RET", "未来收益"};
  case FeatureCategoryL2::SCORE:
    return {"SCORE", "评分"};
  case FeatureCategoryL2::UNIVERSE:
    return {"UNIVERSE", "全域统计"};
  case FeatureCategoryL2::BENCHMARK:
    return {"BENCHMARK", "基准"};
  }
  return {"?", "未知"};
}

inline constexpr EnumStr to_string(NormMethod t) {
  switch (t) {
  case NormMethod::NONE:
    return {"NONE", "无"};
  case NormMethod::ZSCORE:
    return {"ZSCORE", "Z标准化"};
  case NormMethod::ROBUST_ZSCORE:
    return {"ROBUST_ZSCORE", "稳健Z"};
  case NormMethod::IQR_ZSCORE:
    return {"IQR_ZSCORE", "IQR标准化"};
  case NormMethod::RANK:
    return {"RANK", "排名"};
  case NormMethod::RANK_ZSCORE:
    return {"RANK_ZSCORE", "排名标准化"};
  case NormMethod::CLIP:
    return {"CLIP", "截断"};
  case NormMethod::WINSOR:
    return {"WINSOR", "缩尾"};
  case NormMethod::LOG:
    return {"LOG", "对数"};
  case NormMethod::POWER:
    return {"POWER", "幂变换"};
  case NormMethod::ASINH:
    return {"ASINH", "反双曲正弦"};
  case NormMethod::TANH:
    return {"TANH", "双曲正切"};
  case NormMethod::SINCOS:
    return {"SINCOS", "正余弦编码"};
  case NormMethod::LOG_ZSCORE:
    return {"LOG_ZSCORE", "对数+Z"};
  case NormMethod::POWER_ZSCORE:
    return {"POWER_ZSCORE", "幂+Z"};
  case NormMethod::ASINH_ZSCORE:
    return {"ASINH_ZSCORE", "asinh+Z"};
  case NormMethod::CLIP_ZSCORE:
    return {"CLIP_ZSCORE", "Z+截断"};
  case NormMethod::WINSOR_ZSCORE:
    return {"WINSOR_ZSCORE", "缩尾+Z"};
  case NormMethod::CLIP_LOG_ZSCORE:
    return {"CLIP_LOG_ZSCORE", "截断+对数+Z"};
  }
  return {"?", "未知"};
}

inline constexpr EnumStr to_string(L2::ValidType t) {
  switch (t) {
  case L2::ValidType::ALL:
    return {"ALL", "全部"};
  case L2::ValidType::DATA:
    return {"DATA", "数据"};
  case L2::ValidType::DEPTH:
    return {"DEPTH", "深度"};
  }
  return {"?", "未知"};
}
