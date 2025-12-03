#pragma once

#include "codec/L2_DataType.hpp"
#include <array>
#include <cstddef>
#include <cstdint>

// ============================================================================
// LEVEL 0: Tick-level Features (瞬时微结构信号, 短窗口: 5-200 ticks)
// ============================================================================
// Format: X(code, width, data_type, cat_l1, cat_l2, norm_method, name_en, name_cn, formula, description)

#define LEVEL_0_FIELDS(X)                                                                                                                                                                           \
  X(tick_ret_z, 1, TS, MOMENTUM, NORMALIZED, ZSCORE, "Tick Return Z-score", "微小对数收益", "(r-μ_W)/σ_W, r=log(mid_t/mid_{t-1}), W=50", "滚动窗口标准化的tick级对数收益,中性动量/瞬时冲击")        \
  X(tobi_osc, 1, TS, IMBALANCE, OSCILLATOR, CLIP, "TOBI Oscillator", "订单失衡震荡", "clip((tobi-mean_W)/MAD_W, -3, 3), W=50", "top-of-book买卖压力震荡器,对称性好")                                \
  X(micro_gap_norm, 1, TS, MICROSTRUCTURE, NORMALIZED, TANH, "Micro Gap Normalized", "微观价差标准化", "tanh((micro_price-mid)/σ_W), W=50", "micro_price与mid_price的标准化偏离,有界对称")          \
  X(spread_momentum, 1, TS, LIQUIDITY, DEVIATION, ZSCORE, "Spread Momentum", "价差动量", "Δs = s - EMA_α(s), α~20ticks", "spread的短期变动,表示流动性瞬变")                                         \
  X(signed_volume_imb, 1, TS, VOLUME, OSCILLATOR, NONE, "Signed Volume Imbalance", "签名成交量失衡", "Σ(sign_ixsize_i)/Σ|size_i|, N ticks", "近N ticks签名成交量不对称,直接为[-1,1]")               \
  X(cs_spread_rank, 1, CS, LIQUIDITY, RANK, RANK_NORM, "CS Spread Rank", "价差截面排名", "Φ^{-1}(percentile(spread))", "spread在universe中的截面rank→inverse normal")                               \
  X(cs_tobi_rank, 1, CS, IMBALANCE, RANK, RANK_NORM, "CS TOBI Rank", "失衡截面排名", "Φ^{-1}(percentile(tobi))", "tobi在universe中的截面rank→inverse normal")                                       \
  X(cs_liquidity_ratio, 1, CS, LIQUIDITY, RATIO, ZSCORE, "CS Liquidity Ratio", "流动性比率截面", "(top_size/median_H)/z-score", "当前top-of-book size相对历史中位数的截面z-score")                  \
  X(next_tick_ret, 1, LB, LABEL, FUTURE_RET, NONE, "Next Tick Return", "下tick收益", "log(mid_{t+1}/mid_t)", "下一个tick的对数收益,作为预测目标")                                                   \
  X(next_5tick_ret, 1, LB, LABEL, FUTURE_RET, NONE, "Next 5-Tick Return", "未来5tick收益", "log(mid_{t+5}/mid_t)", "未来5个tick的累计对数收益,中期预测目标")                                        \
  X(asset_valid, 1, SH, META, RAW, NONE, "Asset Valid Flag", "资产有效标志", "1.0=valid, 0.0=invalid(inactive/suspended)", "TS/CS共享:标记该asset数据是否有效(停牌/无数据则为0),业务逻辑使用")      \
  X(universe_size, 1, SH, META, UNIVERSE, NONE, "Universe Size", "全域规模", "count(valid_instruments)", "TS/CS共享:当前时刻universe中有效合约数量")                                                \
  X(market_mid_price, 1, SH, META, BENCHMARK, NONE, "Market Mid Price", "市场基准价格", "benchmark_instrument_mid_price", "TS/CS共享:市场基准合约的mid价格")                                        \
  X(_link_to_L1, 1, META, META, RAW, NONE, "Link to L1 Time Index", "L1时间索引", "static_cast<_Float16>(size_t_L1_index)", "Backend元数据:L0时刻对应的L1时间索引,存储为_Float16,导出时转为size_t") \
  X(_link_to_L2, 1, META, META, RAW, NONE, "Link to L2 Time Index", "L2时间索引", "static_cast<_Float16>(size_t_L2_index)", "Backend元数据:L0时刻对应的L2时间索引,存储为_Float16,导出时转为size_t")

// ============================================================================
// LEVEL 1: Minute-level Features (聚合分钟条, 窗口: 1/5/15/60 minutes)
// ============================================================================

#define LEVEL_1_FIELDS(X)                                                                                                                                                                   \
  X(min_ret_z, 1, TS, MOMENTUM, NORMALIZED, WINSOR, "Minute Return Z-score", "分钟收益", "(r-μ_60m)/σ_60m, r=log(close_t/close_{t-1})", "一分钟对数收益标准化,rolling 60m")                 \
  X(rv_5m_norm, 1, TS, VOLATILITY, NORMALIZED, LOG_NORM, "Realized Vol 5m Normalized", "5分钟波动率", "log(σ_5m) rank-normalize", "5分钟实际波动率标准化,减小偏斜")                         \
  X(vwap_gap_pct, 1, TS, PRICE, DEVIATION, ZSCORE, "VWAP Gap Percent", "VWAP偏离", "(close-vwap)/vwap rolling z-score", "close与vwap相对偏离,表示价格是否偏离当期交易价")                   \
  X(momentum_15m, 1, TS, MOMENTUM, OSCILLATOR, ZSCORE, "Momentum 15m", "15分钟动量", "Σr_{1m}/σ_rolling, 15m累计", "15分钟累计动量标准化")                                                  \
  X(range_squeeze, 1, TS, VOLATILITY, RATIO, CLIP, "Range Squeeze", "Range收窄", "(high-low)/(σ_30m+ε), clip[-3,3]", "range/vol,衡量盘面窄幅,收窄为正")                                     \
  X(cs_min_return_rank, 1, CS, MOMENTUM, RANK, RANK_NORM, "CS Minute Return Rank", "分钟收益截面", "Φ^{-1}(percentile(minute_return))", "分钟收益在universe中的截面rank→inverse normal")    \
  X(cs_min_volume_pct, 1, CS, VOLUME, RANK, RANK_NORM, "CS Minute Volume Percentile", "分钟量能百分位", "percentile(log(volume)) rank-normalize", "分钟volume在universe中的截面百分位排名") \
  X(cs_min_spread_z, 1, CS, LIQUIDITY, NORMALIZED, ZSCORE, "CS Minute Spread Z-score", "分钟价差截面", "z-score(spread) cross-sectional", "分钟spread的截面z-score,反映相对交易成本")       \
  X(next_1m_ret, 1, LB, LABEL, FUTURE_RET, NONE, "Next 1-Minute Return", "下1分钟收益", "log(close_{t+1}/close_t)", "下一分钟的对数收益,作为预测目标")                                      \
  X(calmar_score, 1, LB, LABEL, SCORE, NONE, "Calmar Score", "Calmar评分", "annual_return/max_drawdown", "Calmar比率,年化收益与最大回撤之比,风险调整收益指标")                              \
  X(universe_size, 1, SH, META, UNIVERSE, NONE, "Universe Size", "全域规模", "count(valid_instruments)", "TS/CS共享:当前时刻universe中有效合约数量")                                        \
  X(market_return, 1, SH, META, BENCHMARK, NONE, "Market Return", "市场收益", "log(market_close_t/market_close_{t-1})", "TS/CS共享:市场基准收益率")

// ============================================================================
// LEVEL 2: Hour-level Features (小时级, 窗口: 1h/3h/6h/24h)
// ============================================================================

#define LEVEL_2_FIELDS(X)                                                                                                                                                            \
  X(hour_ret_12h_mom, 1, TS, MOMENTUM, NORMALIZED, ZSCORE, "Hour Return 12h Momentum", "12小时动量", "Σr_{1h}^{12}/z-score_{48h}", "12小时动量标准化,捕捉中期趋势")                  \
  X(hour_volatility, 1, TS, VOLATILITY, NORMALIZED, LOG_NORM, "Hour Volatility 24h", "24小时波动率", "log(σ_24h) rank-normalize", "24小时realized vol,log后rank标准化减小偏斜")      \
  X(pivot_dev, 1, TS, PRICE, DEVIATION, CLIP, "Pivot Deviation", "Pivot偏差", "(close-pivot)/price_range, clip", "收盘相对pivot point的偏差,标准化")                                 \
  X(dominant_persist, 1, TS, IMBALANCE, OSCILLATOR, ZSCORE, "Dominant Persistence", "主导持续性", "EMA(dominant_side, α) normalized", "dominant_side的EMA标准化,表示买卖主导延续性") \
  X(hour_overnight_gap, 1, TS, PRICE, DEVIATION, WINSOR, "Hour Overnight Gap", "隔夜跳空", "(open-prev_close)/σ_intraday, winsorize", "当小时起点与前一日收盘gap,捕捉消息型跳空")    \
  X(cs_hour_return_beta, 1, CS, MOMENTUM, RANK, RANK_NORM, "CS Hour Return Beta", "小时收益残差", "residual(r_t ~ r_market) rank-normalize", "小时回报相对市场的回归残差,截面排名")  \
  X(cs_hour_liq_adj_ret, 1, CS, MOMENTUM, RANK, RANK_NORM, "CS Hour Liquidity Adj Return", "流动性调整收益", "hour_ret/sqrt(volume) rank", "小时收益按流动性调整后的截面排名")       \
  X(cs_hour_range_rank, 1, CS, VOLATILITY, RANK, RANK_NORM, "CS Hour Range Rank", "小时Range排名", "Φ^{-1}(percentile(price_range))", "price_range在universe中的截面百分位排名")     \
  X(next_1h_ret, 1, LB, LABEL, FUTURE_RET, NONE, "Next 1-Hour Return", "下1小时收益", "log(close_{t+1h}/close_t)", "下一小时的对数收益,作为预测目标")                                \
  X(sharpe_score, 1, LB, LABEL, SCORE, NONE, "Sharpe Score", "Sharpe评分", "(mean_return-rf)/std_return", "Sharpe比率,超额收益与波动率之比,风险调整收益指标")                        \
  X(universe_size, 1, SH, META, UNIVERSE, NONE, "Universe Size", "全域规模", "count(valid_instruments)", "TS/CS共享:当前时刻universe中有效合约数量")                                 \
  X(market_volatility, 1, SH, META, BENCHMARK, NONE, "Market Volatility", "市场波动率", "std(market_returns_24h)", "TS/CS共享:市场24小时波动率")

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
