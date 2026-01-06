#pragma once

#include "codec/L2_DataType.hpp"
#include <array>
#include <cstddef>
#include <cstdint>

// clang-format off

// ============================================================================
// LEVEL 0: Tick-level Features (秒级)
// ============================================================================
// Format: X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, formula, name_en, name_cn, description)

#define LEVEL_0_FIELDS(X)\
  /* ======== 时序特征 (Time-Series) ======== */\
  X(sec,                1, DATA,  TS,   MICROSTRUCTURE, RAW,        NONE,        "100/00/00", R"(f(t_{L0_{depth}})=t_{sec} \in [0,59])",                                  "Time Sec",                     "时间-秒",        "用于和同周期以上特征做因子组合(早上9点时段, 很多人喜欢在59秒挂单frontrun整数分钟的订单)")\
  X(voi1,               1, DATA,  TS,   IMBALANCE,      RAW,        LOG_ZSCORE,  "100/00/00", R"(\Delta V^B_1 - \Delta V^A_1)",                                           "Vol Order Imba 1-Level",       "订单失衡1档",    "对近端大单挂单(容易成交)非常敏感")\
  X(voi30,              1, DATA,  TS,   IMBALANCE,      RAW,        LOG_ZSCORE,  "100/00/00", R"(\sum_{i=1}^{30} w_i (\Delta V^B_i - \Delta V^A_i))",                     "Vol Order Imba 30-Level",      "订单失衡30档",   "对近端大单挂单(容易成交)非常敏感, 带深度线性衰减加权")\
  X(oir5,               1, DATA,  TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\frac{V^B - V^A}{V^B + V^A})",                                           "Order Imba Ratio 5-Level",     "失衡率5档",      "订单失衡率[-1,1]标准化")\
  X(oir10,              1, DATA,  TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\frac{\sum_{i=1}^{10}(V^B_i - V^A_i)}{\sum_{i=1}^{10}(V^B_i + V^A_i)})", "Order Imba Ratio 10-Level",    "失衡率10档",     "深度失衡率(研报:优于VOI)")\
  X(soir5,              1, DATA,  TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\frac{\sum w_i \cdot \mathrm{SOIR}_i}{\sum w_i})",                       "SOIR 5-Level Weighted",        "逐档失衡5档",    "逐档订单失衡率加权")\
  X(soir5s,             1, DATA,  TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\mathrm{SOIR}_5)",                                                       "SOIR Level-5 Single",          "第5档失衡",      "研报:单档效果优于加权")\
  X(soir10s,            1, DATA,  TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\mathrm{SOIR}_{10})",                                                    "SOIR Level-10 Single",         "第10档失衡",     "深层订单簿失衡")\
  X(soir30s,            1, DATA,  TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\mathrm{SOIR}_{30})",                                                    "SOIR Level-30 Single",         "第30档失衡",     "最深层订单簿信号")\
  X(mpb,                1, DATA,  TS,   MICROSTRUCTURE, DEVIATION,  NONE,        "100/00/00", R"(P_{\mathrm{trade}} - P_{\mathrm{mid}})",                                 "Mid-Price Basis",              "市价偏离度",     "成交均价与中间价偏离(研报最佳)")\
  X(mpc1,               1, DATA,  TS,   MOMENTUM,       RAW,        NONE,        "100/00/00", R"(\frac{M_t - M_{t-1}}{M_{t-1}})",                                         "MPC Lag-1",                    "中间价变化1",    "中间价短期变化率")\
  X(mpc5,               1, DATA,  TS,   MOMENTUM,       RAW,        NONE,        "100/00/00", R"(\frac{M_t - M_{t-5}}{M_{t-5}})",                                         "MPC Lag-5",                    "中间价变化5",    "中间价中期变化率")\
  X(mpc5_max,           1, DATA,  TS,   MOMENTUM,       RAW,        NONE,        "100/00/00", R"(\max_d(\mathrm{MPC5}_d))",                                               "MPC5 Daily Max",               "MPC5日最大",     "日内MPC5极值(IC -9.39%)")\
  X(mpc5_skew,          1, DATA,  TS,   MOMENTUM,       RAW,        NONE,        "100/00/00", R"(\mathrm{skew}(\mathrm{MPC5}_d))",                                        "MPC5 Daily Skew",              "MPC5日偏度",     "日内MPC5偏度(夏普3.07)")\
  /* ======== 截面特征 (Cross-sectional) ======== */\
  X(cs_spread_rank,     1, DATA,  CS,   LIQUIDITY,      RANK,       RANK_ZSCORE, "100/00/00", R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))",                             "CS Spread Rank",               "价差截面排名",   "spread截面rank→inverse normal")\
  X(cs_tobi_rank,       1, DATA,  CS,   IMBALANCE,      RANK,       RANK_ZSCORE, "100/00/00", R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{tobi})))",                               "CS TOBI Rank",                 "失衡截面排名",   "tobi截面rank→inverse normal")\
  X(cs_liquidity_ratio, 1, DATA,  CS,   LIQUIDITY,      RATIO,      ZSCORE,      "100/00/00", R"(\frac{\mathrm{top\_size}/\mathrm{median}}{z})",                          "CS Liquidity Ratio",           "流动性比率截面", "top-of-book size截面z-score")\
  /* ======== 标签 (Labels) ======== */\
  X(next_tick_ret,      1, DATA,  LB,   LABEL,          FUTURE_RET, NONE,        "100/00/00", R"(\log\frac{\mathrm{mid}_{t+1}}{\mathrm{mid}_t})",                         "Next Tick Return",             "下tick收益",     "下一tick对数收益")\
  X(next_5tick_ret,     1, DATA,  LB,   LABEL,          FUTURE_RET, NONE,        "100/00/00", R"(\log\frac{\mathrm{mid}_{t+5}}{\mathrm{mid}_t})",                         "Next 5-Tick Return",           "未来5tick收益",  "未来5tick累计对数收益")\
  /* ======== 元数据 (Metadata) ======== */\
  X(universe_size,      1, DATA,  SH,   META,           UNIVERSE,   NONE,        "100/00/00", R"(\#(\mathrm{valid}))",                                                    "Universe Size",                "全域规模",       "当前有效合约数量")\
  X(_link_to_L1,        1, ALL,   META, META,           RAW,        NONE,        "100/00/00", R"(\mathrm{idx}_{L1})",                                                     "Link to L1",                   "L1时间索引",     "L0→L1时间映射")\
  X(_link_to_L2,        1, ALL,   META, META,           RAW,        NONE,        "100/00/00", R"(\mathrm{idx}_{L2})",                                                     "Link to L2",                   "L2时间索引",     "L0→L2时间映射")\
  X(_depth_valid,       1, ALL,   META, META,           RAW,        NONE,        "100/00/00", R"(\mathbf{1}_{\mathrm{valid}})",                                           "Depth Valid Flag",             "深度有效标志",   "LOB深度缓冲区完整性标记")\
  X(_data_valid,        1, ALL,   META, META,           RAW,        NONE,        "100/00/00", R"(\mathbf{1}_{\mathrm{valid}})",                                           "Data Valid Flag",              "数据有效标志",   "事件驱动稀疏性标记")\

// ============================================================================
// LEVEL 1: Minute-level Features (分钟级)
// ============================================================================

#define LEVEL_1_FIELDS(X)\
  X(min,                1, DATA, TS,   MICROSTRUCTURE, RAW,        NONE,        "00/100/00", R"(f(t_{L1})=t_{min} \in [0,59])",                        "Time Minute",                 "时间-分钟",      "用于和同周期以上特征做因子组合")\
  X(min_ret_z,          1, DATA, TS,   MOMENTUM,       NORMALIZED, WINSOR,      "00/100/00", R"(\frac{r - \mu}{\sigma})",                              "Minute Return Z-score",       "分钟收益",       "分钟对数收益标准化")\
  X(rv_5m_norm,         1, DATA, TS,   VOLATILITY,     NORMALIZED, LOG_ZSCORE,  "00/100/00", R"(\log(\sigma_{5m}))",                                   "Realized Vol 5m",             "5分钟波动率",    "5分钟波动率标准化")\
  X(vwap_gap_pct,       1, DATA, TS,   PRICE,          DEVIATION,  ZSCORE,      "00/100/00", R"(\frac{c - \mathrm{vwap}}{\mathrm{vwap}})",             "VWAP Gap Percent",            "VWAP偏离",       "价格相对VWAP偏离")\
  X(momentum_15m,       1, DATA, TS,   MOMENTUM,       OSCILLATOR, ZSCORE,      "00/100/00", R"(\frac{\sum r}{\sigma})",                               "Momentum 15m",                "15分钟动量",     "15分钟累计动量标准化")\
  X(range_squeeze,      1, DATA, TS,   VOLATILITY,     RATIO,      CLIP,        "00/100/00", R"(\frac{H - L}{\sigma})",                                "Range Squeeze",               "Range收窄",      "盘面窄幅程度")\
  X(cs_min_return_rank, 1, DATA, CS,   MOMENTUM,       RANK,       RANK_ZSCORE, "00/100/00", R"(\Phi^{-1}(\mathrm{pctl}(r)))",                         "CS Minute Return Rank",       "分钟收益截面",   "分钟收益截面rank")\
  X(cs_min_volume_pct,  1, DATA, CS,   VOLUME,         RANK,       RANK_ZSCORE, "00/100/00", R"(\mathrm{pctl}(\log(\mathrm{vol})))",                   "CS Minute Volume Percentile", "分钟量能百分位", "分钟volume截面排名")\
  X(cs_min_spread_z,    1, DATA, CS,   LIQUIDITY,      NORMALIZED, ZSCORE,      "00/100/00", R"(z(\mathrm{spread})_{\mathrm{cs}})",                    "CS Minute Spread Z-score",    "分钟价差截面",   "分钟spread截面z-score")\
  X(next_1m_ret,        1, DATA, LB,   LABEL,          FUTURE_RET, NONE,        "00/100/00", R"(\log\frac{c_{t+1}}{c_t})",                             "Next 1-Minute Return",        "下1分钟收益",    "下一分钟对数收益")\
  X(calmar_score,       1, DATA, LB,   LABEL,          SCORE,      NONE,        "00/100/00", R"(\frac{\mathrm{ret}}{\mathrm{maxDD}})",                 "Calmar Score",                "Calmar评分",     "年化收益/最大回撤")\
  X(universe_size,      1, DATA, SH,   META,           UNIVERSE,   NONE,        "00/100/00", R"(\#(\mathrm{valid}))",                                  "Universe Size",               "全域规模",       "当前有效合约数量")\
  X(market_return,      1, DATA, SH,   META,           BENCHMARK,  NONE,        "00/100/00", R"(\log\frac{\mathrm{mkt}_t}{\mathrm{mkt}_{t-1}})",       "Market Return",               "市场收益",       "市场基准收益率")\
  X(_ohlc_open,         1, DATA, META, META,           RAW,        NONE,        "00/100/00", R"(O)",                                                   "OHLC Open",                   "开盘价",         "GUI:分钟开盘价(分)")\
  X(_ohlc_high,         1, DATA, META, META,           RAW,        NONE,        "00/100/00", R"(H)",                                                   "OHLC High",                   "最高价",         "GUI:分钟最高价(分)")\
  X(_ohlc_low,          1, DATA, META, META,           RAW,        NONE,        "00/100/00", R"(L)",                                                   "OHLC Low",                    "最低价",         "GUI:分钟最低价(分)")\
  X(_ohlc_close,        1, DATA, META, META,           RAW,        NONE,        "00/100/00", R"(C)",                                                   "OHLC Close",                  "收盘价",         "GUI:分钟收盘价(分)")\
  X(_ohlc_volume,       1, DATA, META, META,           RAW,        NONE,        "00/100/00", R"(V)",                                                   "OHLC Volume",                 "成交量",         "GUI:分钟成交量")\
  X(_data_valid,        1, ALL,  META, META,           RAW,        NONE,        "00/100/00", R"(\mathbf{1}_{\mathrm{valid}})",                         "Data Valid Flag",             "数据有效标志",   "事件驱动稀疏性标记")

// ============================================================================
// LEVEL 2: Hour-level Features (小时级)
// ============================================================================

#define LEVEL_2_FIELDS(X)\
  X(hour,                1, DATA, TS,   MICROSTRUCTURE, RAW,        NONE,        "00/00/100", R"(f(t_{L2})=t_{hour} \in \{9,10,11,13,14\})",            "Time Hour",                   "时间-小时",      "用于和同级别特征做因子组合")\
  X(hour_ret_12h_mom,    1, DATA, TS,   MOMENTUM,       NORMALIZED, ZSCORE,      "00/00/100", R"(\frac{\sum_{12h} r}{z})",                              "Hour Return 12h Momentum",    "12小时动量",     "12小时动量标准化")\
  X(hour_volatility,     1, DATA, TS,   VOLATILITY,     NORMALIZED, LOG_ZSCORE,  "00/00/100", R"(\log(\sigma_{24h}))",                                  "Hour Volatility 24h",         "24小时波动率",   "24小时波动率标准化")\
  X(pivot_dev,           1, DATA, TS,   PRICE,          DEVIATION,  CLIP,        "00/00/100", R"(\frac{c - \mathrm{pivot}}{\mathrm{range}})",           "Pivot Deviation",             "Pivot偏差",      "收盘相对pivot偏差")\
  X(dominant_persist,    1, DATA, TS,   IMBALANCE,      OSCILLATOR, ZSCORE,      "00/00/100", R"(\mathrm{EMA}(\mathrm{side}))",                         "Dominant Persistence",        "主导持续性",     "买卖主导延续性")\
  X(hour_overnight_gap,  1, DATA, TS,   PRICE,          DEVIATION,  WINSOR,      "00/00/100", R"(\frac{o - c_{-1}}{\sigma})",                           "Hour Overnight Gap",          "隔夜跳空",       "隔夜gap捕捉消息冲击")\
  X(cs_hour_return_beta, 1, DATA, CS,   MOMENTUM,       RANK,       RANK_ZSCORE, "00/00/100", R"(\epsilon_{r \sim \mathrm{mkt}})",                      "CS Hour Return Beta",         "小时收益残差",   "相对市场回归残差排名")\
  X(cs_hour_liq_adj_ret, 1, DATA, CS,   MOMENTUM,       RANK,       RANK_ZSCORE, "00/00/100", R"(\frac{r}{\sqrt{\mathrm{vol}}})",                       "CS Hour Liq Adj Return",      "流动性调整收益", "流动性调整后收益排名")\
  X(cs_hour_range_rank,  1, DATA, CS,   VOLATILITY,     RANK,       RANK_ZSCORE, "00/00/100", R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{range})))",            "CS Hour Range Rank",          "小时Range排名",  "价格区间截面排名")\
  X(next_1h_ret,         1, DATA, LB,   LABEL,          FUTURE_RET, NONE,        "00/00/100", R"(\log\frac{c_{t+1h}}{c_t})",                            "Next 1-Hour Return",          "下1小时收益",    "下一小时对数收益")\
  X(sharpe_score,        1, DATA, LB,   LABEL,          SCORE,      NONE,        "00/00/100", R"(\frac{\mu - r_f}{\sigma})",                            "Sharpe Score",                "Sharpe评分",     "超额收益/波动率")\
  X(universe_size,       1, DATA, SH,   META,           UNIVERSE,   NONE,        "00/00/100", R"(\#(\mathrm{valid}))",                                  "Universe Size",               "全域规模",       "当前有效合约数量")\
  X(market_volatility,   1, DATA, SH,   META,           BENCHMARK,  NONE,        "00/00/100", R"(\mathrm{std}(r_{\mathrm{mkt},24h}))",                  "Market Volatility",           "市场波动率",     "市场24小时波动率")\
  X(_data_valid,         1, ALL,  META, META,           RAW,        NONE,        "00/00/100", R"(\mathbf{1}_{\mathrm{valid}})",                         "Data Valid Flag",             "数据有效标志",   "事件驱动稀疏性标记")

// ============================================================================
// DEPTH: LOB Snapshot Data (separate storage for orderflow visualization)
// ============================================================================
// Format: X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, formula, name_en, name_cn, description)

#define DEPTH_FIELDS(X)\
  X(_bid_price,    L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", R"(P^B_{0:N})",                    "Bid Prices",        "买盘价格", "GUI:N档买盘价格(分)")\
  X(_ask_price,    L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", R"(P^A_{0:N})",                    "Ask Prices",        "卖盘价格", "GUI:N档卖盘价格(分)")\
  X(_bid_volume,   L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", R"(V^B_{0:N})",                    "Bid Volumes",       "买盘量",   "GUI:N档买盘量(手,100股)")\
  X(_ask_volume,   L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", R"(V^A_{0:N})",                    "Ask Volumes",       "卖盘量",   "GUI:N档卖盘量(手,100股)")\
  X(_mid_price,    1,             DEPTH, META, META, RAW, NONE, "00/00/00", R"(\frac{P^B_1 + P^A_1}{2})",      "Mid Price",         "中间价",   "GUI:实时中间价(分)")\
  X(_depth_valid,  1,             ALL,   META, META, RAW, NONE, "00/00/00", R"(\mathbf{1}_{\mathrm{valid}})",  "Depth Valid Flag",  "深度有效", "LOB深度缓冲区完整性标记")\
  X(_data_valid,   1,             ALL,   META, META, RAW, NONE, "00/00/00", R"(\mathbf{1}_{\mathrm{valid}})",  "Data Valid Flag",   "数据有效", "事件驱动稀疏性标记")

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

  // --- composite (common pipelines) ---
  LOG_ZSCORE = 12,   // log/log1p → zscore
  POWER_ZSCORE = 13, // power → zscore
  ASINH_ZSCORE = 14, // asinh → zscore

  CLIP_ZSCORE = 15,     // zscore → clip
  WINSOR_ZSCORE = 16,   // winsor → zscore
  CLIP_LOG_ZSCORE = 17, // clip → log → zscore
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
constexpr int16_t minute_offset(uint8_t hour, uint8_t minute) {
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
constexpr auto generate_minute_offset_table() {
  std::array<int16_t, 24 * 60> table{};
  for (size_t i = 0; i < 24 * 60; ++i) {
    const uint8_t hour = i / 60;
    const uint8_t minute = i % 60;
    table[i] = minute_offset(hour, minute);
  }
  return table;
}

// Compile-time generated lookup table (1440 entries x 2 bytes = 2.88 KB)
static constexpr auto MINUTE_OFFSET_LUT = generate_minute_offset_table();

// ============================================================================
// TIME CONVERSION - O(1) Branchless Lookup
// ============================================================================

// Convert time to trading seconds (0-15299)
// High-performance branchless implementation using compile-time LUT
inline constexpr size_t tick2index(uint8_t hour, uint8_t minute, uint8_t second) {
  const size_t hm_idx = hour * 60 + minute;
  const int16_t base = MINUTE_OFFSET_LUT[hm_idx];
  // Branchless clamp: negative → 0, positive → value
  const size_t clamped_base = base & ~(base >> 15); // Sign bit mask: if negative, result is 0
  const size_t result = clamped_base + second;
  // Clamp to max valid index (post-market times can exceed 15299 when second > 0)
  return result < TRADE_SECONDS_PER_DAY ? result : TRADE_SECONDS_PER_DAY - 1;
}

// ============================================================================
// INVERSE TIME CONVERSION - Index to Clock Time
// ============================================================================

// Time structure for inverse mapping
struct ClockTime {
  uint8_t hour = 0;
  uint8_t minute = 0;
  uint8_t second = 0;
};

// Convert trading seconds index (0-15299) back to clock time
// Morning: 0-8099 → 09:15:00 - 11:29:59
// Afternoon: 8100-15299 → 13:00:00 - 14:59:59
inline constexpr ClockTime index2tick(size_t index) {
  ClockTime t;

  if (index < 8100) {
    // Morning session: 09:15 + index seconds
    size_t total_seconds = MORNING_START_MIN * 60 + index;
    t.hour = static_cast<uint8_t>(total_seconds / 3600);
    t.minute = static_cast<uint8_t>((total_seconds % 3600) / 60);
    t.second = static_cast<uint8_t>(total_seconds % 60);
  } else {
    // Afternoon session: 13:00 + (index - 8100) seconds
    size_t afternoon_seconds = index - 8100;
    size_t total_seconds = AFTERNOON_START_MIN * 60 + afternoon_seconds;
    t.hour = static_cast<uint8_t>(total_seconds / 3600);
    t.minute = static_cast<uint8_t>((total_seconds % 3600) / 60);
    t.second = static_cast<uint8_t>(total_seconds % 60);
  }

  return t;
}

// Convert trading minute index (0-254) back to clock time
// Morning: 0-134 → 09:15 - 11:29
// Afternoon: 135-254 → 13:00 - 14:59
inline constexpr ClockTime index2minute(size_t index) {
  ClockTime t;
  t.second = 0;

  if (index < 135) {
    // Morning session: 09:15 + index minutes
    size_t total_minutes = MORNING_START_MIN + index;
    t.hour = static_cast<uint8_t>(total_minutes / 60);
    t.minute = static_cast<uint8_t>(total_minutes % 60);
  } else {
    // Afternoon session: 13:00 + (index - 135) minutes
    size_t afternoon_minutes = index - 135;
    size_t total_minutes = AFTERNOON_START_MIN + afternoon_minutes;
    t.hour = static_cast<uint8_t>(total_minutes / 60);
    t.minute = static_cast<uint8_t>(total_minutes % 60);
  }

  return t;
}

// Convert Level 2 hour index (0-3) to clock hour (按小时整数边界分配)
// Level 2 one day has ~4 hour buckets, map each to its clock hour
// 按照数据起始时间落在的时钟小时进行分配:
//   9:15-9:59 -> 9, 10:00-10:59 -> 10, 11:00-11:59 -> 11, 13:00-13:59 -> 13, 14:00-14:59 -> 14
inline constexpr uint8_t index2hour(size_t hour_index) {
  // Simple mapping: Level 2 hour_index corresponds to actual trading hours
  // hour_index 0 -> 9:15 starts -> hour 9
  // hour_index 1 -> ~10:xx starts -> hour 10
  // hour_index 2 -> ~11:xx starts -> hour 11
  // hour_index 3 -> 13:00 starts -> hour 13
  // hour_index 4+ -> 14:xx starts -> hour 14
  constexpr uint8_t hour_map[] = {9, 10, 11, 13, 14};
  return (hour_index < 5) ? hour_map[hour_index] : 14;
}

// ============================================================================
// CROSS-LEVEL INDEX CONVERSION
// ============================================================================

// Convert L0 tick index (0-15299) to L1 minute index (0-254)
// Morning: tick 0-8099 → minute 0-134
// Afternoon: tick 8100-15299 → minute 135-254
inline constexpr size_t tick2minute(size_t tick_idx) {
  if (tick_idx < 8100) {
    return tick_idx / 60;
  } else {
    return 135 + (tick_idx - 8100) / 60;
  }
}

// Convert L2 hour index (0-4) to L1 minute start index
// Hour 0 (9:xx):  → 0   (09:15-09:59)
// Hour 1 (10:xx): → 45  (10:00-10:59)
// Hour 2 (11:xx): → 105 (11:00-11:29)
// Hour 3 (13:xx): → 135 (13:00-13:59)
// Hour 4 (14:xx): → 195 (14:00-14:59)
// Use hour2minute(idx+1) - hour2minute(idx) to get range length
inline constexpr size_t hour2minute(size_t hour_idx) {
  constexpr size_t minute_starts[] = {0, 45, 105, 135, 195, 255};
  return (hour_idx < 5) ? minute_starts[hour_idx] : 255;
}

// ============================================================================
// FORMAT UTILITIES
// ============================================================================

// Format time as string "HH:MM:SS" (for display)
inline void format_time(char *buf, size_t buf_size, const ClockTime &t) {
  std::snprintf(buf, buf_size, "%02d:%02d:%02d", t.hour, t.minute, t.second);
}

// Format time as string "HH:MM" (for display)
inline void format_time_hm(char *buf, size_t buf_size, const ClockTime &t) {
  std::snprintf(buf, buf_size, "%02d:%02d", t.hour, t.minute);
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
  case NormMethod::ROBUST_ZSCORE:
    return "ROBUST_ZSCORE";
  case NormMethod::IQR_ZSCORE:
    return "IQR_ZSCORE";
  case NormMethod::RANK:
    return "RANK";
  case NormMethod::RANK_ZSCORE:
    return "RANK_ZSCORE";
  case NormMethod::CLIP:
    return "CLIP";
  case NormMethod::WINSOR:
    return "WINSOR";
  case NormMethod::LOG:
    return "LOG";
  case NormMethod::POWER:
    return "POWER";
  case NormMethod::ASINH:
    return "ASINH";
  case NormMethod::TANH:
    return "TANH";
  case NormMethod::LOG_ZSCORE:
    return "LOG_ZSCORE";
  case NormMethod::POWER_ZSCORE:
    return "POWER_ZSCORE";
  case NormMethod::ASINH_ZSCORE:
    return "ASINH_ZSCORE";
  case NormMethod::CLIP_ZSCORE:
    return "CLIP_ZSCORE";
  case NormMethod::WINSOR_ZSCORE:
    return "WINSOR_ZSCORE";
  case NormMethod::CLIP_LOG_ZSCORE:
    return "CLIP_LOG_ZSCORE";
  }
  return "UNKNOWN";
}

inline constexpr const char *to_string(L2::ValidType type) {
  switch (type) {
  case L2::ValidType::ALL:
    return "ALL";
  case L2::ValidType::DATA:
    return "DATA";
  case L2::ValidType::DEPTH:
    return "DEPTH";
  }
  return "UNKNOWN";
}
