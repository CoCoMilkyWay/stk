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
//       <------------------------------------>|<----------------------------->| (更低频信号弃用(不平稳))
//        进出场触发器(高频特征的简单组合)           因子主体(低频特征的复杂组合)
//        非必须. 只是超额                          核心的核心, 盈利主体
//        (为因子主体提供稳定超额, 流动性支持)       (决定因子强度, 周期, 平稳性(分层), 换手)

// clang-format off
// ============================================================================
// LEVEL 0: Tick-level Features (秒级)
// ============================================================================
// Format: X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, name_en, name_cn, description, formula)

// 后处理算子:
//| 方法                                     | 核心特点           | 适用场景          |
//| -------------------------------------- | -------------- | ------------- |
//| **tsfresh**                            | 广泛统计 + 自动筛选    | 小/中数据集，解释性要求高 |
//| **catch22**                            | 22 个轻量统计特征     | 快速、基线、轻量      |
//| **Kats (by Meta)**                     | 多工具集成 + 时间序列分析 | 预测 + 分析       |
//| **tsai / sktime**                      | 深度学习 + 传统方法统一  | 预测性能最优场景      |
//| **InceptionTime / T-CN / Transformer** | 自动深度表示学习       | 超大数据 & 模式复杂   |
//

#define LEVEL_0_FIELDS(X)\
  /* SD - Structural Depth Features (深度结构特征) */\
  X(ci_1,              1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Cumu Imba 1-Level",               "顶部1档失衡",          "顶部1档订单失衡率",                                    R"(\frac{V_{1,t}^{B} - V_{1,t}^{A}}{V_{1,t}^{B} + V_{1,t}^{A}})")\
  X(ci_5,              1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Cumu Imba 5-Level",               "累计5档失衡",          "累计5档订单失衡率",                                    R"(\frac{\sum_{i=1}^{5}(V_{i,t}^{B} - V_{i,t}^{A})}{\sum_{i=1}^{5}(V_{i,t}^{B} + V_{i,t}^{A})})")\
  X(ci_10,             1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Cumu Imba 10-Level",              "累计10档失衡",         "累计10档订单失衡率",                                   R"(\frac{\sum_{i=1}^{10}(V_{i,t}^{B} - V_{i,t}^{A})}{\sum_{i=1}^{10}(V_{i,t}^{B} + V_{i,t}^{A})})")\
  X(ci_30,             1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Cumu Imba 30-Level",              "累计30档失衡",         "累计30档订单失衡率",                                   R"(\frac{\sum_{i=1}^{30}(V_{i,t}^{B} - V_{i,t}^{A})}{\sum_{i=1}^{30}(V_{i,t}^{B} + V_{i,t}^{A})})")\
  X(ci_all,            1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Cumu Imba All-Level",             "累计全档失衡",         "累计所有档订单失衡率(由逐笔得出, 或者交易所直接提供)",    R"(\frac{\sum_{i=1}^{\infty}(V_{i,t}^{B} - V_{i,t}^{A})}{\sum_{i=1}^{\infty}(V_{i,t}^{B} + V_{i,t}^{A})})")\
  X(cwi_1,             1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Convexity-weighted Imb γ=1",      "凸加权失衡γ=1",        "考虑全量, 但是近端更高权重(按档位)",                    R"(\frac{\sum_{i=1}^{N} w_i V_{i,t}^{B} - \sum_{i=1}^{N} w_i V_{i,t}^{A}}{\sum_{i=1}^{N} w_i (V_{i,t}^{B} + V_{i,t}^{A})}, \quad w_i = \frac{1}{(i+\epsilon)^\gamma}, \quad \gamma = 1)")\
  X(cwi_2,             1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Convexity-weighted Imb γ=2",      "凸加权失衡γ=2",        "考虑全量, 但是近端更高权重(按档位)",                    R"(\frac{\sum_{i=1}^{N} w_i V_{i,t}^{B} - \sum_{i=1}^{N} w_i V_{i,t}^{A}}{\sum_{i=1}^{N} w_i (V_{i,t}^{B} + V_{i,t}^{A})}, \quad w_i = \frac{1}{(i+\epsilon)^\gamma}, \quad \gamma = 2)")\
  X(ddi_1,             1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Distance-discounted Imb λ=0.01",  "距离折扣失衡λ=0.01",   "考虑全量, 但是近端更高权重(按距离)",                    R"(\frac{\sum_{i=1}^{N} e^{-\lambda \Delta p_{i,t}} (V_{i,t}^{\text{bid}} - V_{i,t}^{\text{ask}})}{\sum_{i=1}^{N} e^{-\lambda \Delta p_{i,t}} (V_{i,t}^{\text{bid}} + V_{i,t}^{\text{ask}})}, \quad \Delta p_{i,t} = i \cdot \text{tick}, \quad \lambda = 0.01)")\
  X(ddi_2,             1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Distance-discounted Imb λ=0.02",  "距离折扣失衡λ=0.02",   "考虑全量, 但是近端更高权重(按距离)",                    R"(\frac{\sum_{i=1}^{N} e^{-\lambda \Delta p_{i,t}} (V_{i,t}^{\text{bid}} - V_{i,t}^{\text{ask}})}{\sum_{i=1}^{N} e^{-\lambda \Delta p_{i,t}} (V_{i,t}^{\text{bid}} + V_{i,t}^{\text{ask}})}, \quad \Delta p_{i,t} = i \cdot \text{tick}, \quad \lambda = 0.02)")\
  X(tbr_5,             1, DATA,  TS,   LIQUIDITY,  RATIO,      NONE,        "100/00/00", "Top 5-level Bid Ratio",           "前5档买单占比",        "买单侧是否容易被击穿",                                 R"(\frac{\sum_{i=1}^{N} V_{i,t}^{B}}{\sum_{i=1}^{\infty} V_{i,t}^{B}}, \quad N = 5)")\
  X(tar_5,             1, DATA,  TS,   LIQUIDITY,  RATIO,      NONE,        "100/00/00", "Top 5-level Ask Ratio",           "前5档卖单占比",        "卖单侧是否容易被击穿",                                 R"(\frac{\sum_{i=1}^{N} V_{i,t}^{A}}{\sum_{i=1}^{\infty} V_{i,t}^{A}}, \quad N = 5)")\
  X(b_para_c0,         1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Bid Depth Parabola c0",           "买侧抛物线截距",       "买侧近端流动性",                                      R"(c_{0,t}^{B}, \quad \text{where } V_{i,t}^{B} \sim c_{0,t}^{B} + c_{1,t}^{B} i + c_{2,t}^{B} i^2)")\
  X(b_para_c1,         1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Bid Depth Parabola c1",           "买侧抛物线斜率",       "买方风偏(近端还是远端挂单)",                           R"(c_{1,t}^{B}, \quad \text{where } V_{i,t}^{B} \sim c_{0,t}^{B} + c_{1,t}^{B} i + c_{2,t}^{B} i^2)")\
  X(b_para_c2,         1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Bid Depth Parabola c2",           "买侧抛物线曲率",       "<0:近端有订单块, 容易反转或突破; >0:做市类挂单",        R"(c_{2,t}^{B}, \quad \text{where } V_{i,t}^{B} \sim c_{0,t}^{B} + c_{1,t}^{B} i + c_{2,t}^{B} i^2)")\
  X(a_para_c0,         1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Ask Depth Parabola c0",           "卖侧抛物线截距",       "卖侧近端流动性",                                      R"(c_{0,t}^{A}, \quad \text{where } V_{i,t}^{A} \sim c_{0,t}^{A} + c_{1,t}^{A} i + c_{2,t}^{A} i^2)")\
  X(a_para_c1,         1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Ask Depth Parabola c1",           "卖侧抛物线斜率",       "卖方风偏(近端还是远端挂单)",                           R"(c_{1,t}^{A}, \quad \text{where } V_{i,t}^{A} \sim c_{0,t}^{A} + c_{1,t}^{A} i + c_{2,t}^{A} i^2)")\
  X(a_para_c2,         1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Ask Depth Parabola c2",           "卖侧抛物线曲率",       "<0:近端有订单块, 容易反转或突破; >0:做市类挂单",        R"(c_{2,t}^{A}, \quad \text{where } V_{i,t}^{A} \sim c_{0,t}^{A} + c_{1,t}^{A} i + c_{2,t}^{A} i^2)")\
  X(imba_para_c0,      1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Bid Depth Parabola c0",           "买卖抛物线截距失衡",    "对比买卖近端流动性",                                  R"(\frac{c_{0,t}^{B} - c_{0,t}^{A}}{c_{0,t}^{B} + c_{0,t}^{A}}, \quad V_{i,t}^{\{B,A\}} \sim c_{0,t}^{\{B,A\}} + c_{1,t}^{\{B,A\}} i + c_{2,t}^{\{B,A\}} i^2)")\
  X(imba_para_c1,      1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Bid Depth Parabola c1",           "买卖抛物线斜率失衡",    "对比买卖风偏",                                        R"(\frac{c_{1,t}^{B} - c_{1,t}^{A}}{c_{1,t}^{B} + c_{1,t}^{A}}, \quad V_{i,t}^{\{B,A\}} \sim c_{0,t}^{\{B,A\}} + c_{1,t}^{\{B,A\}} i + c_{2,t}^{\{B,A\}} i^2)")\
  X(imba_para_c2,      1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Bid Depth Parabola c2",           "买卖抛物线曲率失衡",    "对比买卖订单块距离",                                  R"(\frac{c_{2,t}^{B} - c_{2,t}^{A}}{c_{2,t}^{B} + c_{2,t}^{A}}, \quad V_{i,t}^{\{B,A\}} \sim c_{0,t}^{\{B,A\}} + c_{1,t}^{\{B,A\}} i + c_{2,t}^{\{B,A\}} i^2)")\
  X(b_5_c1,            1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 5-level Bid Grad",            "买侧五档梯度",          "买侧梯度(近端斜率)",                                 R"(\frac{1}{N-1}\sum_{i=1}^{N-1}(V_{i+1,t}^{B} - V_{i,t}^{B}), \quad N = 5)")\
  X(a_5_c1,            1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 5-level Ask Grad",            "卖侧五档梯度",          "卖侧梯度(近端斜率)",                                 R"(\frac{1}{N-1}\sum_{i=1}^{N-1}(V_{i+1,t}^{A} - V_{i,t}^{A}), \quad N = 5)")\
  X(imba_5_c1,         1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Top 5-level Grad Ratio",          "买卖五档梯度失衡",       "买卖五档梯度(近端斜率)失衡",                         R"(\frac{\sum_{i=1}^{N-1}(V_{i+1,t}^{B} - V_{i,t}^{B}) - \sum_{i=1}^{N-1}(V_{i+1,t}^{A} - V_{i,t}^{A})}{\sum_{i=1}^{N-1}(V_{i+1,t}^{B} - V_{i,t}^{B}) + \sum_{i=1}^{N-1}(V_{i+1,t}^{A} - V_{i,t}^{A})}, \quad N = 5)")\
  X(b_5_entropy,       1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 5-level Bid ShannonEntropy",   "买侧五档香农熵",        "0:极端集中; ln(N):极端均匀",                        R"(-\sum_{i=1}^{N} \pi_{i,t}^{B} \log(\pi_{i,t}^{B}), \quad \pi_{i,t}^{B} = \frac{V_{i,t}^{B}}{\sum_{j=1}^{N} V_{j,t}^{B}}, \quad N = 5)")\
  X(a_5_entropy,       1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 5-level Ask ShannonEntropy",   "卖侧五档香农熵",        "0:极端集中; ln(N):极端均匀",                        R"(-\sum_{i=1}^{N} \pi_{i,t}^{A} \log(\pi_{i,t}^{A}), \quad \pi_{i,t}^{A} = \frac{V_{i,t}^{A}}{\sum_{j=1}^{N} V_{j,t}^{A}}, \quad N = 5)")\
  X(imba_5_entropy,    1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Top 5-level Entropy Imba",         "五档香农熵失衡",        "五档香农熵失衡",                                    R"(\frac{H_{t}^{B} - H_{t}^{A}}{H_{t}^{B} + H_{t}^{A}}, \quad H_{t}^{\{B,A\}} = -\sum_{i=1}^{N} \pi_{i,t}^{\{B,A\}} \log(\pi_{i,t}^{\{B,A\}}), \pi_{i,t}^{\{B,A\}} = \frac{V_{i,t}^{\{B,A\}}}{\sum_{j=1}^{N} V_{j,t}^{\{B,A\}}}, N = 5)")\
  X(b_30_entropy,      1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 30-level Bid ShannonEntropy",  "买侧三十档香农熵",      "0:极端集中; ln(N):极端均匀",                        R"(-\sum_{i=1}^{N} \pi_{i,t}^{B} \log(\pi_{i,t}^{B}), \quad \pi_{i,t}^{B} = \frac{V_{i,t}^{B}}{\sum_{j=1}^{N} V_{j,t}^{B}}, \quad N = 30)")\
  X(a_30_entropy,      1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 30-level Ask ShannonEntropy",  "卖侧三十档香农熵",      "0:极端集中; ln(N):极端均匀",                        R"(-\sum_{i=1}^{N} \pi_{i,t}^{A} \log(\pi_{i,t}^{A}), \quad \pi_{i,t}^{A} = \frac{V_{i,t}^{A}}{\sum_{j=1}^{N} V_{j,t}^{A}}, \quad N = 30)")\
  X(imba_30_entropy,   1, DATA,  TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Top 5-level Entropy Imba",         "五档香农熵失衡",        "五档香农熵失衡",                                    R"(\frac{H_{t}^{B} - H_{t}^{A}}{H_{t}^{B} + H_{t}^{A}}, \quad H_{t}^{\{B,A\}} = -\sum_{i=1}^{N} \pi_{i,t}^{\{B,A\}} \log(\pi_{i,t}^{\{B,A\}}), \pi_{i,t}^{\{B,A\}} = \frac{V_{i,t}^{\{B,A\}}}{\sum_{j=1}^{N} V_{j,t}^{\{B,A\}}}, N = 30)")\
  X(depth_repre,       1, DATA,  TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Depth Representation",             "多档深度表征空间",      "多档深度的自监督表征学习(使用repre backbone)",        R"(\left\{\begin{aligned}\textbf{Input: }&X_t=\{V_{i,t}^{B},V_{i,t}^{A}\}_{i=1}^{N}\\\textbf{Data Augmentation: }&\tilde X_t=\mathcal{A}(X_t),\ \mathcal{A}=\{\text{volume scaling, noise, level shift}\}\\\textbf{Backbone Encoder: }&z_t=f_{\theta}(\tilde X_t),\ f_{\theta}:\mathbb{R}^{2N}\rightarrow\mathbb{R}^{d}\\\textbf{Projection Head (training only): }&h_t=g_{\phi}(z_t),\ g_{\phi}:\mathbb{R}^{d}\rightarrow\mathbb{R}^{d'}\\\textbf{Training Objective: }&\min_{\theta,\phi}\mathbb{E}\|g_{\phi}(f_{\theta}(\mathcal{A}(X_t)))-g_{\phi}(f_{\theta}(\mathcal{A}(X_{t+\Delta})))\|_2^2\\\textbf{Online Usage: }&\text{depth\_repre}_t=z_t=f_{\theta}(X_t)\end{aligned}\right.)")\
  /* DF - Dynamic Order Flow Features (订单流动态特征) */\
  X(ofi_1,             1, DATA,  TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Order Flow Imba 1-Level",          "订单流失衡1档",         "对近端大单挂单变动非常敏感",                         R"(\Delta V_{1,t}^B - \Delta V_{1,t}^A, \quad \Delta V_{1,t}^B = \begin{cases}0, & p_{1,t}^B < p_{1,t-1}^B \\ V_{1,t}^B - V_{1,t-1}^B, & p_{1,t}^B = p_{1,t-1}^B \\ V_{1,t}^B, & p_{1,t}^B > p_{1,t-1}^B \end{cases}, \quad \Delta V_{1,t}^A = \begin{cases}V_{1,t}^A, & p_{1,t}^A < p_{1,t-1}^A \\ V_{1,t}^A - V_{1,t-1}^A, & p_{1,t}^A = p_{1,t-1}^A \\ 0, & p_{1,t}^A > p_{1,t-1}^A \end{cases})")\
  X(ofi_5,             1, DATA,  TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Order Flow Imba 5-Level",          "订单流失衡5档加权",      "监控5档挂单变化",                                  R"(\Delta V_t^{WB} - \Delta V_t^{WA}, \quad V_t^{WB} = \frac{\sum_{i=1}^N w_i V_{i,t}^B}{\sum_{i=1}^N w_i}, \quad V_t^{WA} = \frac{\sum_{i=1}^N w_i V_{i,t}^A}{\sum_{i=1}^N w_i}, \quad w_i = 1 - \frac{i-1}{N}, \quad N = 5)")\
  /* X(ci_1,              1, DATA,  TS,   ORDERFLOW,  RATIO,      NONE,        "100/00/00", "Hidden-liquidity–adjusted imbalance ", "潜在流动性失衡",          "预测后续时刻的失衡(按照refill/cancel rate)",                                    R"(\frac{V_{1,t}^{B} - V_{1,t}^{A}}{V_{1,t}^{B} + V_{1,t}^{A}})") */\
  /* BH - Behavioral & Strategic Features (行为与策略特征) */\
  /* CD - Clustering & Dependency Features (事件聚集与依赖特征) */\
  /* RS - Resiliency & Replenishment Features (韧性与恢复特征) */\
  /* IC - Impact & Liquidity Cost Features (价格冲击与流动性成本特征) */\
  /* AN - Anomaly & Structural Outlier Features (异常与结构失衡特征) */\
  /* OT - others (其他特征) */\
  X(sec,                1, DATA,  TS,   BASIC,          OSCILLATOR, SINCOS,      "100/00/00", "Time Sec Phase",               "时间-秒相位",    "用于和同级别以上特征做因子组合",                      R"(\sin(\frac{2\pi t}{60}))")\
  /* ======== 截面特征 (Cross-sectional) ======== */\
  X(cs_spread_rank,     1, DATA,  CS,   LIQUIDITY,      RANK,       RANK_ZSCORE, "100/00/00", "CS Spread Rank",               "价差截面排名",   "spread截面rank→inverse normal",                       R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))")\
  X(cs_tobi_rank,       1, DATA,  CS,   IMBALANCE,      RANK,       RANK_ZSCORE, "100/00/00", "CS TOBI Rank",                 "失衡截面排名",   "tobi截面rank→inverse normal",                         R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{tobi})))")\
  X(cs_liquidity_ratio, 1, DATA,  CS,   LIQUIDITY,      RATIO,      ZSCORE,      "100/00/00", "CS Liquidity Ratio",           "流动性比率截面", "top-of-book size截面z-score",                         R"(\frac{\mathrm{top\_size}/\mathrm{median}}{z})")\
  /* ======== 标签 (Labels) ======== */\
  X(next_tick_ret,      1, DATA,  LB,   LABEL,          FUTURE_RET, NONE,        "100/00/00", "Next Tick Return",             "下tick收益",     "下一tick对数收益",                                    R"(\log\frac{\mathrm{mid}_{t+1}}{\mathrm{mid}_t})")\
  X(next_5tick_ret,     1, DATA,  LB,   LABEL,          FUTURE_RET, NONE,        "100/00/00", "Next 5-Tick Return",           "未来5tick收益",  "未来5tick累计对数收益",                               R"(\log\frac{\mathrm{mid}_{t+5}}{\mathrm{mid}_t})")\
  /* ======== 元数据 (Metadata) ======== */\
  X(universe_size,      1, DATA,  SH,   META,           UNIVERSE,   NONE,        "100/00/00", "Universe Size",                "全域规模",       "当前有效合约数量",                                    R"(\#(\mathrm{valid}))")\
  X(_link_to_L1,        1, ALL,   META, META,           RAW,        NONE,        "100/00/00", "Link to L1",                   "L1时间索引",     "L0→L1时间映射",                                       R"(\mathrm{idx}_{L1})")\
  X(_depth_valid,       1, ALL,   META, META,           RAW,        NONE,        "100/00/00", "Depth Valid Flag",             "深度有效标志",   "LOB深度缓冲区完整性标记",                             R"(\mathbf{1}_{\mathrm{valid}})")\
  X(_data_valid,        1, ALL,   META, META,           RAW,        NONE,        "100/00/00", "Data Valid Flag",              "数据有效标志",   "事件驱动稀疏性标记",                                  R"(\mathbf{1}_{\mathrm{valid}})")\

// ============================================================================
// LEVEL 1: Minute-level Features (分钟级)
// ============================================================================

#define LEVEL_1_FIELDS(X)\
  X(min,                1, DATA, TS,   BASIC,          OSCILLATOR, SINCOS,      "00/100/00", "Time Min Phase",              "时间-分钟相位",  "用于和同级别以上特征做因子组合(特征值和pdf连续可导, 频谱能量分布集中, 梯度友好", R"(\sin(\frac{2\pi t}{60}))")\
  X(min_ret_z,          1, DATA, TS,   BASIC,          NORMALIZED, WINSOR,      "00/100/00", "Minute Return Z-score",       "分钟收益",       "分钟对数收益标准化",                                                         R"(\frac{r - \mu}{\sigma})")\
  X(rv_5m_norm,         1, DATA, TS,   VOLATILITY,     NORMALIZED, LOG_ZSCORE,  "00/100/00", "Realized Vol 5m",             "5分钟波动率",    "5分钟波动率标准化",                                                           R"(\log(\sigma_{5m}))")\
  X(vwap_gap_pct,       1, DATA, TS,   VOLATILITY,     DEVIATION,  ZSCORE,      "00/100/00", "VWAP Gap Percent",            "VWAP偏离",       "价格相对VWAP偏离",                                                            R"(\frac{c - \mathrm{vwap}}{\mathrm{vwap}})")\
  X(momentum_15m,       1, DATA, TS,   BASIC,          OSCILLATOR, ZSCORE,      "00/100/00", "Momentum 15m",                "15分钟动量",     "15分钟累计动量标准化",                                                        R"(\frac{\sum r}{\sigma})")\
  X(range_squeeze,      1, DATA, TS,   VOLATILITY,     RATIO,      CLIP,        "00/100/00", "Range Squeeze",               "Range收窄",      "盘面窄幅程度",                                                                R"(\frac{H - L}{\sigma})")\
  X(cs_min_return_rank, 1, DATA, CS,   BASIC,          RANK,       RANK_ZSCORE, "00/100/00", "CS Minute Return Rank",       "分钟收益截面",   "分钟收益截面rank",                                                            R"(\Phi^{-1}(\mathrm{pctl}(r)))")\
  X(cs_min_volume_pct,  1, DATA, CS,   LIQUIDITY,      RANK,       RANK_ZSCORE, "00/100/00", "CS Minute Volume Percentile", "分钟量能百分位", "分钟volume截面排名",                                                          R"(\mathrm{pctl}(\log(\mathrm{vol})))")\
  X(cs_min_spread_z,    1, DATA, CS,   LIQUIDITY,      NORMALIZED, ZSCORE,      "00/100/00", "CS Minute Spread Z-score",    "分钟价差截面",   "分钟spread截面z-score",                                                       R"(z(\mathrm{spread})_{\mathrm{cs}})")\
  X(next_1m_ret,        1, DATA, LB,   LABEL,          FUTURE_RET, NONE,        "00/100/00", "Next 1-Minute Return",        "下1分钟收益",    "下一分钟对数收益",                                                            R"(\log\frac{c_{t+1}}{c_t})")\
  X(calmar_score,       1, DATA, LB,   LABEL,          SCORE,      NONE,        "00/100/00", "Calmar Score",                "Calmar评分",     "年化收益/最大回撤",                                                           R"(\frac{\mathrm{ret}}{\mathrm{maxDD}})")\
  X(universe_size,      1, DATA, SH,   META,           UNIVERSE,   NONE,        "00/100/00", "Universe Size",               "全域规模",       "当前有效合约数量",                                                            R"(\#(\mathrm{valid}))")\
  X(market_return,      1, DATA, SH,   META,           BENCHMARK,  NONE,        "00/100/00", "Market Return",               "市场收益",       "市场基准收益率",                                                              R"(\log\frac{\mathrm{mkt}_t}{\mathrm{mkt}_{t-1}})")\
  X(_ohlc_open,         1, DATA, META, META,           RAW,        NONE,        "00/100/00", "OHLC Open",                   "开盘价",         "GUI:分钟开盘价(分)",                                                          R"(O)")\
  X(_ohlc_high,         1, DATA, META, META,           RAW,        NONE,        "00/100/00", "OHLC High",                   "最高价",         "GUI:分钟最高价(分)",                                                          R"(H)")\
  X(_ohlc_low,          1, DATA, META, META,           RAW,        NONE,        "00/100/00", "OHLC Low",                    "最低价",         "GUI:分钟最低价(分)",                                                          R"(L)")\
  X(_ohlc_close,        1, DATA, META, META,           RAW,        NONE,        "00/100/00", "OHLC Close",                  "收盘价",         "GUI:分钟收盘价(分)",                                                          R"(C)")\
  X(_ohlc_volume,       1, DATA, META, META,           RAW,        NONE,        "00/100/00", "OHLC Volume",                 "成交量",         "GUI:分钟成交量",                                                              R"(V)")\
  X(_data_valid,        1, ALL,  META, META,           RAW,        NONE,        "00/100/00", "Data Valid Flag",             "数据有效标志",   "事件驱动稀疏性标记",                                                          R"(\mathbf{1}_{\mathrm{valid}})")

// ============================================================================
// DEPTH: LOB Snapshot Data (separate storage for orderflow visualization)
// ============================================================================
// Format: X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, name_en, name_cn, description, formula)

#define DEPTH_FIELDS(X)\
  X(_bid_price,    L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", "Bid Prices",        "买盘价格", "GUI:N档买盘价格(分)",       R"(P^B_{0:N})")\
  X(_ask_price,    L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", "Ask Prices",        "卖盘价格", "GUI:N档卖盘价格(分)",       R"(P^A_{0:N})")\
  X(_bid_volume,   L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", "Bid Volumes",       "买盘量",   "GUI:N档买盘量(手,100股)",   R"(V^B_{0:N})")\
  X(_ask_volume,   L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE, "00/00/00", "Ask Volumes",       "卖盘量",   "GUI:N档卖盘量(手,100股)",   R"(V^A_{0:N})")\
  X(_mid_price,    1,             DEPTH, META, META, RAW, NONE, "00/00/00", "Mid Price",         "中间价",   "GUI:实时中间价(分)",        R"(\frac{P^B_1 + P^A_1}{2})")\
  X(_depth_valid,  1,             ALL,   META, META, RAW, NONE, "00/00/00", "Depth Valid Flag",  "深度有效", "LOB深度缓冲区完整性标记",   R"(\mathbf{1}_{\mathrm{valid}_{depth}})")\
  X(_data_valid,   1,             ALL,   META, META, RAW, NONE, "00/00/00", "Data Valid Flag",   "数据有效", "事件驱动稀疏性标记",        R"(\mathbf{1}_{\mathrm{valid}_{data}})")

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
  IMBALANCE = 0,      // 失衡
  SHAPE = 1,          // 形状
  ORDER_FLOW = 2,     // 订单流
  BEHAVIORAL = 3,     // 行为
  RESILIENCE = 4,     // 韧性
  LIQUIDITY = 5,      // 流动性
  VOLATILITY = 6,     // 波动率
  BASIC = 7,          // 基础
  LABEL = 8,          // 标签/目标
  META = 9            // 元数据/共享变量
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

struct EnumStr { const char *en; const char *cn; };

inline constexpr EnumStr to_string(FeatureDataType t) {
  switch (t) {
  case FeatureDataType::TS:   return {"TS",   "时序"};
  case FeatureDataType::CS:   return {"CS",   "截面"};
  case FeatureDataType::LB:   return {"LB",   "标签"};
  case FeatureDataType::SH:   return {"SH",   "共享"};
  case FeatureDataType::META: return {"META", "元数据"};
  } return {"?", "未知"};
}

inline constexpr EnumStr to_string(FeatureCategoryL1 t) {
  switch (t) {
  case FeatureCategoryL1::IMBALANCE:  return {"IMBALANCE",  "失衡"};
  case FeatureCategoryL1::SHAPE:      return {"SHAPE",      "形状"};
  case FeatureCategoryL1::ORDER_FLOW: return {"ORDER_FLOW", "订单流"};
  case FeatureCategoryL1::BEHAVIORAL: return {"BEHAVIORAL", "行为"};
  case FeatureCategoryL1::RESILIENCE: return {"RESILIENCE", "韧性"};
  case FeatureCategoryL1::LIQUIDITY:  return {"LIQUIDITY",  "流动性"};
  case FeatureCategoryL1::VOLATILITY: return {"VOLATILITY", "波动率"};
  case FeatureCategoryL1::BASIC:      return {"BASIC",      "基础"};
  case FeatureCategoryL1::LABEL:      return {"LABEL",      "标签"};
  case FeatureCategoryL1::META:       return {"META",       "元数据"};
  } return {"?", "未知"};
}

inline constexpr EnumStr to_string(FeatureCategoryL2 t) {
  switch (t) {
  case FeatureCategoryL2::RAW:        return {"RAW",        "原始"};
  case FeatureCategoryL2::NORMALIZED: return {"NORMALIZED", "标准化"};
  case FeatureCategoryL2::OSCILLATOR: return {"OSCILLATOR", "震荡器"};
  case FeatureCategoryL2::DEVIATION:  return {"DEVIATION",  "偏离"};
  case FeatureCategoryL2::RATIO:      return {"RATIO",      "比率"};
  case FeatureCategoryL2::RANK:       return {"RANK",       "排名"};
  case FeatureCategoryL2::FUTURE_RET: return {"FUTURE_RET", "未来收益"};
  case FeatureCategoryL2::SCORE:      return {"SCORE",      "评分"};
  case FeatureCategoryL2::UNIVERSE:   return {"UNIVERSE",   "全域统计"};
  case FeatureCategoryL2::BENCHMARK:  return {"BENCHMARK",  "基准"};
  } return {"?", "未知"};
}

inline constexpr EnumStr to_string(NormMethod t) {
  switch (t) {
  case NormMethod::NONE:           return {"NONE",           "无"};
  case NormMethod::ZSCORE:         return {"ZSCORE",         "Z标准化"};
  case NormMethod::ROBUST_ZSCORE:  return {"ROBUST_ZSCORE",  "稳健Z"};
  case NormMethod::IQR_ZSCORE:     return {"IQR_ZSCORE",     "IQR标准化"};
  case NormMethod::RANK:           return {"RANK",           "排名"};
  case NormMethod::RANK_ZSCORE:    return {"RANK_ZSCORE",    "排名标准化"};
  case NormMethod::CLIP:           return {"CLIP",           "截断"};
  case NormMethod::WINSOR:         return {"WINSOR",         "缩尾"};
  case NormMethod::LOG:            return {"LOG",            "对数"};
  case NormMethod::POWER:          return {"POWER",          "幂变换"};
  case NormMethod::ASINH:          return {"ASINH",          "反双曲正弦"};
  case NormMethod::TANH:           return {"TANH",           "双曲正切"};
  case NormMethod::SINCOS:         return {"SINCOS",         "正余弦编码"};
  case NormMethod::LOG_ZSCORE:     return {"LOG_ZSCORE",     "对数+Z"};
  case NormMethod::POWER_ZSCORE:   return {"POWER_ZSCORE",   "幂+Z"};
  case NormMethod::ASINH_ZSCORE:   return {"ASINH_ZSCORE",   "asinh+Z"};
  case NormMethod::CLIP_ZSCORE:    return {"CLIP_ZSCORE",    "Z+截断"};
  case NormMethod::WINSOR_ZSCORE:  return {"WINSOR_ZSCORE",  "缩尾+Z"};
  case NormMethod::CLIP_LOG_ZSCORE:return {"CLIP_LOG_ZSCORE","截断+对数+Z"};
  } return {"?", "未知"};
}

inline constexpr EnumStr to_string(L2::ValidType t) {
  switch (t) {
  case L2::ValidType::ALL:   return {"ALL",   "全部"};
  case L2::ValidType::DATA:  return {"DATA",  "数据"};
  case L2::ValidType::DEPTH: return {"DEPTH", "深度"};
  } return {"?", "未知"};
}
