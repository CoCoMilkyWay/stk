#pragma once

#include "codec/L2_DataType.hpp"
#include <array>
#include <cstddef>
#include <cstdint>

// 核心理念: 在低信噪比、强竞争的二级市场，端到端的深度模型(数据先验)会先被淘汰, 特征工程因子挖掘(结构性先验)是生存条件，不是选择。

// 原始特征级别只是原始特征被定义的级别, 在手工原始特征设计阶段:
//  1. 高频特征可以有低频能量, 且不需要手动频域切分(后面会自动化处理)
//  2. 选择合适的归一化, 过滤掉尽可能少的不平稳的最低频信号, 让时序平稳 (保留尽可能多的平稳的低频信息, 这部分是因子主体的主要构成)
//  3. 保证特征序列概率分布在时间/截面上的稳定性(时间上不漂移(因子能收敛), 截面分布一致(可以截面中性化))
// 在符号回归阶段:
//  1. GPU会系统性地对所有手工特征做检测, 得到对应的平稳性, 分布, 频域, 自相关特性
//  2. 用算子池逐个特征测试, 展开得到若干新特征, 然后再次统计新特征的特性, 然后过滤掉不合格的, 自动生成针对原始特征的算子池约束
//  3. 计算展开后的特征池的相关性矩阵(时序相关性(自相关), 截面相关性(交叉相关))
//  4. 根据上面的信息, 符号回归优化Loss(Label), 实现高效因子挖掘和稳健性测试

// 频率: 秒 -------------------------------- 分钟 ------------ 小时 ------------- 天
//       <------------------------------------>|<----------------------------->| (更低频信号弃用(不平稳))
//        进出场触发器(高频特征的简单组合)           因子主体(低频特征的复杂组合)
//        非必须. 只是超额                          核心的核心, 盈利主体
//        (为因子主体提供稳定超额, 流动性支持)       (决定因子强度, 周期, 平稳性(分层), 换手)

// clang-format off

// ============================================================================
// LEVEL 0: Tick-level Features (秒级)
// ============================================================================
// Format: X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, formula, name_en, name_cn, description)

#define LEVEL_0_FIELDS(X)\
  /* ======== 时序特征 (Time-Series) ======== */\
  X(sec,                1, DATA,  TS,   MICROSTRUCTURE, OSCILLATOR, SINCOS,      "100/00/00", R"(\sin(\frac{2\pi t}{60}))",                                               "Time Sec Phase",               "时间-秒相位",    "用于和同级别以上特征做因子组合(特征值和pdf连续可导, 频谱能量分布集中, 梯度友好)")\
  X(voi1,               1, DEPTH, TS,   IMBALANCE,      RAW,        LOG_ZSCORE,  "100/00/00", R"(\Delta V^B_1 - \Delta V^A_1)",                                           "Vol Order Imba 1-Level",       "订单失衡1档",    "对近端大单挂单(容易成交)非常敏感")\
  X(voi30,              1, DEPTH, TS,   IMBALANCE,      RAW,        LOG_ZSCORE,  "100/00/00", R"(\sum_{i=1}^{30} w_i (\Delta V^B_i - \Delta V^A_i))",                     "Vol Order Imba 30-Level",      "订单失衡30档",   "对近端大单挂单(容易成交)非常敏感, 带深度线性衰减加权")\
  X(oir5,               1, DEPTH, TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\frac{V^B - V^A}{V^B + V^A})",                                           "Order Imba Ratio 5-Level",     "失衡率5档",      "订单失衡率[-1,1]标准化")\
  X(oir10,              1, DEPTH, TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\frac{\sum_{i=1}^{10}(V^B_i - V^A_i)}{\sum_{i=1}^{10}(V^B_i + V^A_i)})", "Order Imba Ratio 10-Level",    "失衡率10档",     "深度失衡率(研报:优于VOI)")\
  X(soir5,              1, DEPTH, TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\frac{\sum w_i \cdot \mathrm{SOIR}_i}{\sum w_i})",                       "SOIR 5-Level Weighted",        "逐档失衡5档",    "逐档订单失衡率加权")\
  X(soir5s,             1, DEPTH, TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\mathrm{SOIR}_5)",                                                       "SOIR Level-5 Single",          "第5档失衡",      "研报:单档效果优于加权")\
  X(soir10s,            1, DEPTH, TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\mathrm{SOIR}_{10})",                                                    "SOIR Level-10 Single",         "第10档失衡",     "深层订单簿失衡")\
  X(soir30s,            1, DEPTH, TS,   IMBALANCE,      RATIO,      NONE,        "100/00/00", R"(\mathrm{SOIR}_{30})",                                                    "SOIR Level-30 Single",         "第30档失衡",     "最深层订单簿信号")\
  X(mpb,                1, DEPTH, TS,   MICROSTRUCTURE, DEVIATION,  NONE,        "100/00/00", R"(P_{\mathrm{trade}} - P_{\mathrm{mid}})",                                 "Mid-Price Basis",              "市价偏离度",     "成交均价与中间价偏离(研报最佳)")\
  X(mpc1,               1, DEPTH, TS,   MOMENTUM,       RAW,        NONE,        "100/00/00", R"(\frac{M_t - M_{t-1}}{M_{t-1}})",                                         "MPC Lag-1",                    "中间价变化1",    "中间价短期变化率")\
  X(mpc5,               1, DEPTH, TS,   MOMENTUM,       RAW,        NONE,        "100/00/00", R"(\frac{M_t - M_{t-5}}{M_{t-5}})",                                         "MPC Lag-5",                    "中间价变化5",    "中间价中期变化率")\
  X(mpc5_max,           1, DEPTH, TS,   MOMENTUM,       RAW,        NONE,        "100/00/00", R"(\max_d(\mathrm{MPC5}_d))",                                               "MPC5 Daily Max",               "MPC5日最大",     "日内MPC5极值(IC -9.39%)")\
  X(mpc5_skew,          1, DEPTH, TS,   MOMENTUM,       RAW,        NONE,        "100/00/00", R"(\mathrm{skew}(\mathrm{MPC5}_d))",                                        "MPC5 Daily Skew",              "MPC5日偏度",     "日内MPC5偏度(夏普3.07)")\
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
  X(min,                1, DATA, TS,   MICROSTRUCTURE, OSCILLATOR, SINCOS,      "00/100/00", R"(\sin(\frac{2\pi t}{60}))",                             "Time Min Phase",              "时间-分钟相位",  "用于和同级别以上特征做因子组合(特征值和pdf连续可导, 频谱能量分布集中, 梯度友好")\
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
  X(hour,                1, DATA, TS,   MICROSTRUCTURE, OSCILLATOR, SINCOS,      "00/00/100", R"(\sin(\frac{2\pi t}{4}))",                              "Time Hour Phase",             "时间-小时相位",  "用于和同级别特征做因子组合(特征值和pdf连续可导, 频谱能量分布集中, 梯度友好")\
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
  X(_bid_price,    L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", R"(P^B_{0:N})",                           "Bid Prices",        "买盘价格", "GUI:N档买盘价格(分)")\
  X(_ask_price,    L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", R"(P^A_{0:N})",                           "Ask Prices",        "卖盘价格", "GUI:N档卖盘价格(分)")\
  X(_bid_volume,   L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", R"(V^B_{0:N})",                           "Bid Volumes",       "买盘量",   "GUI:N档买盘量(手,100股)")\
  X(_ask_volume,   L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", R"(V^A_{0:N})",                           "Ask Volumes",       "卖盘量",   "GUI:N档卖盘量(手,100股)")\
  X(_mid_price,    1,             DEPTH, META, META, RAW, NONE, "00/00/00", R"(\frac{P^B_1 + P^A_1}{2})",             "Mid Price",         "中间价",   "GUI:实时中间价(分)")\
  X(_depth_valid,  1,             ALL,   META, META, RAW, NONE, "00/00/00", R"(\mathbf{1}_{\mathrm{valid}_{depth}})", "Depth Valid Flag",  "深度有效", "LOB深度缓冲区完整性标记")\
  X(_data_valid,   1,             ALL,   META, META, RAW, NONE, "00/00/00", R"(\mathbf{1}_{\mathrm{valid}_{data}})",  "Data Valid Flag",   "数据有效", "事件驱动稀疏性标记")

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
// TRADING SESSION CONSTANTS
// ============================================================================
// A股交易时段 (含集合竞价):
//
//   时段       时钟时间         分钟数    秒数      L0 范围        L1 范围     L2
//   ─────────────────────────────────────────────────────────────────────────────
//   上午       09:15 - 11:30    135 min   8100 s    [0, 8099]      [0, 134]    0,1,2
//   午休       11:30 - 13:00    (非交易)
//   下午       13:00 - 15:00    120 min   7200 s    [8100, 15299]  [135, 254]  3,4
//   ─────────────────────────────────────────────────────────────────────────────
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
// 三级索引体系:
//   L0 (tick)   : 0-15299  秒级索引
//   L1 (minute) : 0-254    分钟级索引
//   L2 (hour)   : 0-4      小时级索引
//
// L1 分钟边界 (相对于 L0):
//   上午: L1=0 → L0=[0,59], L1=1 → L0=[60,119], ..., L1=134 → L0=[8040,8099]
//   下午: L1=135 → L0=[8100,8159], ..., L1=254 → L0=[15240,15299]
//
// L2 小时边界 (相对于 L1):
//   L2=0 (09:xx) : L1=[0, 44]    45 min (09:15-09:59)
//   L2=1 (10:xx) : L1=[45, 104]  60 min (10:00-10:59)
//   L2=2 (11:xx) : L1=[105, 134] 30 min (11:00-11:29)
//   L2=3 (13:xx) : L1=[135, 194] 60 min (13:00-13:59)
//   L2=4 (14:xx) : L1=[195, 254] 60 min (14:00-14:59)

// -------------------------------- 降采样 --------------------------------
// Clock → L0 → L1 → L2

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

// L1 → L2: 0-44→0, 45-104→1, 105-134→2, 135-194→3, 195-254→4
inline constexpr size_t L1_to_L2(size_t l1_idx) {
  return (l1_idx < 105) ? ((l1_idx < 45) ? 0 : 1)
                        : ((l1_idx < 195) ? ((l1_idx < 135) ? 2 : 3) : 4);
}

// -------------------------------- 升采样 --------------------------------
// L2 → L1 → L0 → Clock

// L2 → L1: 0→0, 1→45, 2→105, 3→135, 4→195 (返回该小时的起始分钟索引)
inline constexpr size_t L2_to_L1(size_t l2_idx) {
  constexpr size_t starts[] = {0, 45, 105, 135, 195, 255};
  return (l2_idx < 5) ? starts[l2_idx] : 255;
}

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

// L2 → Clock hour: 0→9, 1→10, 2→11, 3→13, 4→14
inline constexpr uint8_t L2_to_Clock(size_t l2_idx) {
  constexpr uint8_t hour_map[] = {9, 10, 11, 13, 14};
  return (l2_idx < 5) ? hour_map[l2_idx] : 14;
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
  case NormMethod::SINCOS:
    return "SINCOS";
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
