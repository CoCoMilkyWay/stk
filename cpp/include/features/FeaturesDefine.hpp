#pragma once

#include "codec/L2_DataType.hpp"
#include <array>
#include <cstddef>
#include <cstdint>

// clang-format off

// ============================================================================
// LEVEL 0: Tick-level Features (瞬时微结构信号, 短窗口: 5-200 ticks)
// ============================================================================
// Format: X(code, width, data_type, cat_l1, cat_l2, norm_method, name_en, name_cn, formula, description)

#define LEVEL_0_FIELDS(X)                                                                                                                                                                                         \
  X(tick_ret_z,         1,             TS,   MOMENTUM,       NORMALIZED, ZSCORE,    "Tick Return Z-score",    "微小对数收益",   "(r-μ)/σ, W=50",               "滚动窗口标准化的tick级对数收益")               \
  X(tobi_osc,           1,             TS,   IMBALANCE,      OSCILLATOR, CLIP,      "TOBI Oscillator",        "订单失衡震荡",   "clip((tobi-μ)/MAD, ±3)",      "top-of-book买卖压力震荡器")                     \
  X(micro_gap_norm,     1,             TS,   MICROSTRUCTURE, NORMALIZED, TANH,      "Micro Gap Normalized",   "微观价差标准化", "tanh((micro-mid)/σ)",         "micro与mid价格标准化偏离")                      \
  X(spread_momentum,    1,             TS,   LIQUIDITY,      DEVIATION,  ZSCORE,    "Spread Momentum",        "价差动量",       "s - EMA(s)",                  "spread的短期变动")                              \
  X(signed_volume_imb,  1,             TS,   VOLUME,         OSCILLATOR, NONE,      "Signed Volume Imbalance","签名成交量失衡", "Σ(sign×size)/Σ|size|",        "近N ticks签名成交量不对称")                     \
  X(cs_spread_rank,     1,             CS,   LIQUIDITY,      RANK,       RANK_NORM, "CS Spread Rank",         "价差截面排名",   "Φ⁻¹(pctl(spread))",           "spread截面rank→inverse normal")                 \
  X(cs_tobi_rank,       1,             CS,   IMBALANCE,      RANK,       RANK_NORM, "CS TOBI Rank",           "失衡截面排名",   "Φ⁻¹(pctl(tobi))",             "tobi截面rank→inverse normal")                   \
  X(cs_liquidity_ratio, 1,             CS,   LIQUIDITY,      RATIO,      ZSCORE,    "CS Liquidity Ratio",     "流动性比率截面", "(top_size/median)/z",         "top-of-book size截面z-score")                   \
  X(next_tick_ret,      1,             LB,   LABEL,          FUTURE_RET, NONE,      "Next Tick Return",       "下tick收益",     "log(mid_{t+1}/mid_t)",        "下一tick对数收益")                              \
  X(next_5tick_ret,     1,             LB,   LABEL,          FUTURE_RET, NONE,      "Next 5-Tick Return",     "未来5tick收益",  "log(mid_{t+5}/mid_t)",        "未来5tick累计对数收益")                         \
  X(asset_valid,        1,             SH,   META,           RAW,        NONE,      "Asset Valid Flag",       "资产有效标志",   "1.0=valid, 0.0=invalid",      "标记asset是否有效")                             \
  X(universe_size,      1,             SH,   META,           UNIVERSE,   NONE,      "Universe Size",          "全域规模",       "count(valid)",                "当前有效合约数量")                              \
  X(market_mid_price,   1,             SH,   META,           BENCHMARK,  NONE,      "Market Mid Price",       "市场基准价格",   "benchmark_mid",               "基准合约mid价格")                               \
  X(_link_to_L1,        1,             META, META,           RAW,        NONE,      "Link to L1",             "L1时间索引",     "L1_time_index",               "L0→L1时间映射")                                 \
  X(_link_to_L2,        1,             META, META,           RAW,        NONE,      "Link to L2",             "L2时间索引",     "L2_time_index",               "L0→L2时间映射")                                 \
  X(bid_price,          L2::LOB_DEPTH, META, META,           RAW,        NONE,      "Bid Prices",             "买盘价格",       "bid_price[0:N]",              "GUI:N档买盘价格")                               \
  X(ask_price,          L2::LOB_DEPTH, META, META,           RAW,        NONE,      "Ask Prices",             "卖盘价格",       "ask_price[0:N]",              "GUI:N档卖盘价格")                               \
  X(bid_volume,         L2::LOB_DEPTH, META, META,           RAW,        NONE,      "Bid Volumes",            "买盘数量",       "bid_volume[0:N]",             "GUI:N档买盘数量")                               \
  X(ask_volume,         L2::LOB_DEPTH, META, META,           RAW,        NONE,      "Ask Volumes",            "卖盘数量",       "ask_volume[0:N]",             "GUI:N档卖盘数量")

// ============================================================================
// LEVEL 1: Minute-level Features (聚合分钟条, 窗口: 1/5/15/60 minutes)
// ============================================================================

#define LEVEL_1_FIELDS(X)                                                                                                                                                                                \
  X(min_ret_z,          1, TS, MOMENTUM,   NORMALIZED, WINSOR,    "Minute Return Z-score",       "分钟收益",       "(r-μ)/σ, 60m rolling",        "分钟对数收益标准化")                                   \
  X(rv_5m_norm,         1, TS, VOLATILITY, NORMALIZED, LOG_NORM,  "Realized Vol 5m",             "5分钟波动率",    "log(σ_5m) rank-norm",         "5分钟波动率标准化")                                    \
  X(vwap_gap_pct,       1, TS, PRICE,      DEVIATION,  ZSCORE,    "VWAP Gap Percent",            "VWAP偏离",       "(close-vwap)/vwap z",         "价格相对VWAP偏离")                                     \
  X(momentum_15m,       1, TS, MOMENTUM,   OSCILLATOR, ZSCORE,    "Momentum 15m",                "15分钟动量",     "Σr/σ, 15m",                   "15分钟累计动量标准化")                                 \
  X(range_squeeze,      1, TS, VOLATILITY, RATIO,      CLIP,      "Range Squeeze",               "Range收窄",      "(H-L)/σ, clip±3",             "盘面窄幅程度")                                         \
  X(cs_min_return_rank, 1, CS, MOMENTUM,   RANK,       RANK_NORM, "CS Minute Return Rank",       "分钟收益截面",   "Φ⁻¹(pctl(ret))",              "分钟收益截面rank")                                     \
  X(cs_min_volume_pct,  1, CS, VOLUME,     RANK,       RANK_NORM, "CS Minute Volume Percentile", "分钟量能百分位", "pctl(log(vol)) rank",         "分钟volume截面排名")                                   \
  X(cs_min_spread_z,    1, CS, LIQUIDITY,  NORMALIZED, ZSCORE,    "CS Minute Spread Z-score",    "分钟价差截面",   "z(spread) cross-sect",        "分钟spread截面z-score")                                \
  X(next_1m_ret,        1, LB, LABEL,      FUTURE_RET, NONE,      "Next 1-Minute Return",        "下1分钟收益",    "log(close_{t+1}/close_t)",    "下一分钟对数收益")                                     \
  X(calmar_score,       1, LB, LABEL,      SCORE,      NONE,      "Calmar Score",                "Calmar评分",     "ret/maxDD",                   "年化收益/最大回撤")                                    \
  X(universe_size,      1, SH, META,       UNIVERSE,   NONE,      "Universe Size",               "全域规模",       "count(valid)",                "当前有效合约数量")                                     \
  X(market_return,      1, SH, META,       BENCHMARK,  NONE,      "Market Return",               "市场收益",       "log(mkt_t/mkt_{t-1})",        "市场基准收益率")

// ============================================================================
// LEVEL 2: Hour-level Features (小时级, 窗口: 1h/3h/6h/24h)
// ============================================================================

#define LEVEL_2_FIELDS(X)                                                                                                                                                                               \
  X(hour_ret_12h_mom,    1, TS, MOMENTUM,   NORMALIZED, ZSCORE,    "Hour Return 12h Momentum",    "12小时动量",     "Σr/z, 12h",                  "12小时动量标准化")                                     \
  X(hour_volatility,     1, TS, VOLATILITY, NORMALIZED, LOG_NORM,  "Hour Volatility 24h",         "24小时波动率",   "log(σ_24h) rank",            "24小时波动率标准化")                                   \
  X(pivot_dev,           1, TS, PRICE,      DEVIATION,  CLIP,      "Pivot Deviation",             "Pivot偏差",      "(close-pivot)/range",        "收盘相对pivot偏差")                                    \
  X(dominant_persist,    1, TS, IMBALANCE,  OSCILLATOR, ZSCORE,    "Dominant Persistence",        "主导持续性",     "EMA(side) norm",             "买卖主导延续性")                                       \
  X(hour_overnight_gap,  1, TS, PRICE,      DEVIATION,  WINSOR,    "Hour Overnight Gap",          "隔夜跳空",       "(open-prev_close)/σ",        "隔夜gap捕捉消息冲击")                                  \
  X(cs_hour_return_beta, 1, CS, MOMENTUM,   RANK,       RANK_NORM, "CS Hour Return Beta",         "小时收益残差",   "residual(r~mkt) rank",       "相对市场回归残差排名")                                 \
  X(cs_hour_liq_adj_ret, 1, CS, MOMENTUM,   RANK,       RANK_NORM, "CS Hour Liq Adj Return",      "流动性调整收益", "ret/√vol rank",              "流动性调整后收益排名")                                 \
  X(cs_hour_range_rank,  1, CS, VOLATILITY, RANK,       RANK_NORM, "CS Hour Range Rank",          "小时Range排名",  "Φ⁻¹(pctl(range))",           "价格区间截面排名")                                     \
  X(next_1h_ret,         1, LB, LABEL,      FUTURE_RET, NONE,      "Next 1-Hour Return",          "下1小时收益",    "log(close_{t+1h}/close_t)",  "下一小时对数收益")                                     \
  X(sharpe_score,        1, LB, LABEL,      SCORE,      NONE,      "Sharpe Score",                "Sharpe评分",     "(μ-rf)/σ",                   "超额收益/波动率")                                      \
  X(universe_size,       1, SH, META,       UNIVERSE,   NONE,      "Universe Size",               "全域规模",       "count(valid)",               "当前有效合约数量")                                     \
  X(market_volatility,   1, SH, META,       BENCHMARK,  NONE,      "Market Volatility",           "市场波动率",     "std(mkt_ret_24h)",           "市场24小时波动率")

// clang-format on

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
  PRICE = 0,          // 价格
  VOLUME = 1,         // 量能
  VOLATILITY = 2,     // 波动率
  MOMENTUM = 3,       // 动量
  LIQUIDITY = 4,      // 流动性
  IMBALANCE = 5,      // 失衡
  MICROSTRUCTURE = 6, // 微结构
  LABEL = 7,          // 标签/目标
  META = 8            // 元数据/共享变量
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
  NONE = 0,      // 无
  ZSCORE = 1,    // z-score标准化
  RANK_NORM = 2, // rank + inverse normal
  CLIP = 3,      // clip到[-3,3]
  TANH = 4,      // tanh激活
  WINSOR = 5,    // winsorize
  LOG_NORM = 6,  // log后标准化
  PCT_RANK = 7   // percentile rank
};

// ============================================================================
// ALL LEVELS REGISTRY
// ============================================================================
// Format: X(level_name, level_index, fields_macro)

#define ALL_LEVELS(X)      \
  X(L0, 0, LEVEL_0_FIELDS) \
  X(L1, 1, LEVEL_1_FIELDS) \
  X(L2, 2, LEVEL_2_FIELDS)

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
constexpr LevelTimeConfig LEVEL_CONFIGS[3] = {
    {TimeUnit::SECOND, 1}, // L0: 1s
    {TimeUnit::MINUTE, 1}, // L1: 1min
    {TimeUnit::HOUR, 1}    // L2: 1hour
};

// ============================================================================
// TRADING SESSION MAPPING - High Performance Non-linear Time Conversion
// ============================================================================
// Chinese stock market trading sessions (including call auctions):
//   Morning:   09:15 - 11:30 (2 hours 15 minutes = 135 minutes)
//   Lunch:     11:30 - 13:00 (non-trading)
//   Afternoon: 13:00 - 15:00 (2 hours = 120 minutes)
// Total trading time: 4 hours 15 minutes = 255 minutes = 15300 seconds

// Trading session boundaries (in minutes since midnight)
constexpr uint16_t MORNING_START_MIN = L2::MORNING_CALL_AUCTION_START_HOUR * 60 + L2::MORNING_CALL_AUCTION_START_MINUTE;                   // 555 (09:15)
constexpr uint16_t MORNING_END_MIN = L2::CONTINUOUS_TRADING_MORNING_END_HOUR * 60 + L2::CONTINUOUS_TRADING_MORNING_END_MINUTE;             // 690 (11:30)
constexpr uint16_t AFTERNOON_START_MIN = L2::CONTINUOUS_TRADING_AFTERNOON_START_HOUR * 60 + L2::CONTINUOUS_TRADING_AFTERNOON_START_MINUTE; // 780 (13:00)
constexpr uint16_t AFTERNOON_END_MIN = L2::CONTINUOUS_TRADING_AFTERNOON_END_HOUR * 60 + L2::CONTINUOUS_TRADING_AFTERNOON_END_MINUTE;       // 900 (15:00)

// Helper: Map clock time to trading seconds (comptime)
// Returns: -1 for pre-market, 0-8099 for morning, 8100-15299 for afternoon, 15299 for post-market (clamped)
constexpr int16_t map_clock_to_trading_seconds(uint8_t hour, uint8_t minute) {
  const uint16_t total_minutes = hour * 60 + minute;

  // Morning session: 09:15-11:30 → 0-8099 seconds (135 minutes)
  if (total_minutes >= MORNING_START_MIN && total_minutes < MORNING_END_MIN) {
    return static_cast<int16_t>((total_minutes - MORNING_START_MIN) * 60);
  }

  // Afternoon session: 13:00-15:00 → 8100-15299 seconds (120 minutes)
  if (total_minutes >= AFTERNOON_START_MIN && total_minutes < AFTERNOON_END_MIN) {
    return static_cast<int16_t>(8100 + (total_minutes - AFTERNOON_START_MIN) * 60);
  }

  // Lunch break: map to afternoon session start
  if (total_minutes >= MORNING_END_MIN && total_minutes < AFTERNOON_START_MIN) {
    return 8100;
  }

  // Pre-market
  if (total_minutes < MORNING_START_MIN) {
    return -1;
  }

  // Post-market: clamp to last valid index (15299, not 15300)
  return 15299;
}

// Constexpr function to generate lookup table at compile time
constexpr auto generate_trading_offset_table() {
  std::array<int16_t, 24 * 60> table{};
  for (size_t i = 0; i < 24 * 60; ++i) {
    const uint8_t hour = i / 60;
    const uint8_t minute = i % 60;
    table[i] = map_clock_to_trading_seconds(hour, minute);
  }
  return table;
}

// Compile-time generated lookup table (1440 entries x 2 bytes = 2.88 KB)
static constexpr auto TRADING_OFFSET_LUT = generate_trading_offset_table();

// ============================================================================
// TIME CONVERSION - O(1) Branchless Lookup
// ============================================================================

// Convert time to trading seconds (0-15299)
// High-performance branchless implementation using compile-time LUT
inline constexpr size_t time_to_trading_seconds(uint8_t hour, uint8_t minute, uint8_t second) {
  const size_t hm_idx = hour * 60 + minute;
  const int16_t base = TRADING_OFFSET_LUT[hm_idx];
  // Branchless clamp: negative → 0, positive → value
  const size_t clamped_base = base & ~(base >> 15); // Sign bit mask: if negative, result is 0
  const size_t result = clamped_base + second;
  // Clamp to max valid index (post-market times can exceed 15299 when second > 0)
  return result < TRADE_SECONDS_PER_DAY ? result : TRADE_SECONDS_PER_DAY - 1;
}

// Convert time to trading milliseconds (0-15299999)
inline constexpr size_t time_to_trading_milliseconds(uint8_t hour, uint8_t minute, uint8_t second, uint8_t millisecond) {
  return time_to_trading_seconds(hour, minute, second) * 1000 + millisecond;
}

// ============================================================================
// ENUM TO STRING MAPPINGS - For metadata query and serialization
// ============================================================================

inline constexpr const char *to_string(FeatureDataType type) {
  switch (type) {
  case FeatureDataType::TS:
    return "TS";
  case FeatureDataType::CS:
    return "CS";
  case FeatureDataType::LB:
    return "LB";
  case FeatureDataType::SH:
    return "SH";
  case FeatureDataType::META:
    return "META";
  }
  return "UNKNOWN";
}

inline constexpr const char *to_string(FeatureCategoryL1 cat) {
  switch (cat) {
  case FeatureCategoryL1::PRICE:
    return "PRICE";
  case FeatureCategoryL1::VOLUME:
    return "VOLUME";
  case FeatureCategoryL1::VOLATILITY:
    return "VOLATILITY";
  case FeatureCategoryL1::MOMENTUM:
    return "MOMENTUM";
  case FeatureCategoryL1::LIQUIDITY:
    return "LIQUIDITY";
  case FeatureCategoryL1::IMBALANCE:
    return "IMBALANCE";
  case FeatureCategoryL1::MICROSTRUCTURE:
    return "MICROSTRUCTURE";
  case FeatureCategoryL1::LABEL:
    return "LABEL";
  case FeatureCategoryL1::META:
    return "META";
  }
  return "UNKNOWN";
}

inline constexpr const char *to_string(FeatureCategoryL2 cat) {
  switch (cat) {
  case FeatureCategoryL2::RAW:
    return "RAW";
  case FeatureCategoryL2::NORMALIZED:
    return "NORMALIZED";
  case FeatureCategoryL2::OSCILLATOR:
    return "OSCILLATOR";
  case FeatureCategoryL2::DEVIATION:
    return "DEVIATION";
  case FeatureCategoryL2::RATIO:
    return "RATIO";
  case FeatureCategoryL2::RANK:
    return "RANK";
  case FeatureCategoryL2::FUTURE_RET:
    return "FUTURE_RET";
  case FeatureCategoryL2::SCORE:
    return "SCORE";
  case FeatureCategoryL2::UNIVERSE:
    return "UNIVERSE";
  case FeatureCategoryL2::BENCHMARK:
    return "BENCHMARK";
  }
  return "UNKNOWN";
}

inline constexpr const char *to_string(NormMethod method) {
  switch (method) {
  case NormMethod::NONE:
    return "NONE";
  case NormMethod::ZSCORE:
    return "ZSCORE";
  case NormMethod::RANK_NORM:
    return "RANK_NORM";
  case NormMethod::CLIP:
    return "CLIP";
  case NormMethod::TANH:
    return "TANH";
  case NormMethod::WINSOR:
    return "WINSOR";
  case NormMethod::LOG_NORM:
    return "LOG_NORM";
  case NormMethod::PCT_RANK:
    return "PCT_RANK";
  }
  return "UNKNOWN";
}
