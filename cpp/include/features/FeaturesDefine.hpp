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

// clang-format off
// ============================================================================
// LEVEL 0: Tick-level Features (秒级)
// ============================================================================
// Format: X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, name_en, name_cn, description, formula)

#define LEVEL_0_FIELDS(X)\
  /* Basic (基础特征) */\
  X(sec,                1, DATA,  TS,   BASIC,      OSCILLATOR, SINCOS,      "100/00/00", "Time Sec Phase",                   "时间-秒相位",           "用于因子组合",                                         R"(\sin(\frac{2\pi t}{60}))")\
  X(spread,             1, DEPTH, TS,   BASIC,      RAW,        NONE,        "100/00/00", "Bid-Ask Spread",                   "买卖价差",              "卖一减买一",                                           R"(P_{1,t}^{M,A} - P_{1,t}^{M,B})")\
  /* Structural Depth Features (深度结构特征) */\
  X(ci_1,               1, DEPTH, TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Cumu Imba 1-Level",                "顶部1档失衡",           "顶部1档订单失衡率",                                    R"(\frac{V_{1,t}^{M,B} - V_{1,t}^{M,A}}{V_{1,t}^{M,B} + V_{1,t}^{M,A}})")\
  X(b_para_c0,          1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Bid Depth Parabola c0",            "买侧抛物线截距",        "买侧近端流动性",                                       R"(c_{0,t}^{M,B}, \quad \text{where } V_{i,t}^{M,B} \sim c_{0,t}^{M,B} + c_{1,t}^{M,B} i + c_{2,t}^{M,B} i^2)")\
  X(b_para_c1,          1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Bid Depth Parabola c1",            "买侧抛物线斜率",        "买方风偏(近端还是远端挂单)",                           R"(c_{1,t}^{M,B}, \quad \text{where } V_{i,t}^{M,B} \sim c_{0,t}^{M,B} + c_{1,t}^{M,B} i + c_{2,t}^{M,B} i^2)")\
  X(b_para_c2,          1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Bid Depth Parabola c2",            "买侧抛物线曲率",        "<0:近端有订单块, 容易反转或突破; >0:做市类挂单",       R"(c_{2,t}^{M,B}, \quad \text{where } V_{i,t}^{M,B} \sim c_{0,t}^{M,B} + c_{1,t}^{M,B} i + c_{2,t}^{M,B} i^2)")\
  X(a_para_c0,          1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Ask Depth Parabola c0",            "卖侧抛物线截距",        "卖侧近端流动性",                                       R"(c_{0,t}^{M,A}, \quad \text{where } V_{i,t}^{M,A} \sim c_{0,t}^{M,A} + c_{1,t}^{M,A} i + c_{2,t}^{M,A} i^2)")\
  X(a_para_c1,          1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Ask Depth Parabola c1",            "卖侧抛物线斜率",        "卖方风偏(近端还是远端挂单)",                           R"(c_{1,t}^{M,A}, \quad \text{where } V_{i,t}^{M,A} \sim c_{0,t}^{M,A} + c_{1,t}^{M,A} i + c_{2,t}^{M,A} i^2)")\
  X(a_para_c2,          1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Ask Depth Parabola c2",            "卖侧抛物线曲率",        "<0:近端有订单块, 容易反转或突破; >0:做市类挂单",       R"(c_{2,t}^{M,A}, \quad \text{where } V_{i,t}^{M,A} \sim c_{0,t}^{M,A} + c_{1,t}^{M,A} i + c_{2,t}^{M,A} i^2)")\
  X(imba_para_c0,       1, DEPTH, TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Depth Parabola c0 Imba",           "买卖抛物线截距失衡",    "对比买卖近端流动性",                                   R"(\frac{|c_{0,t}^{M,B}| - |c_{0,t}^{M,A}|}{|c_{0,t}^{M,B}| + |c_{0,t}^{M,A}|}, \quad V_{i,t}^{M,s} \sim c_{0,t}^{M,s} + c_{1,t}^{M,s} i + c_{2,t}^{M,s} i^2, \quad s \in \{B,A\})")\
  X(imba_para_c1,       1, DEPTH, TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Depth Parabola c1 Imba",           "买卖抛物线斜率失衡",    "对比买卖风偏",                                         R"(\frac{|c_{1,t}^{M,B}| - |c_{1,t}^{M,A}|}{|c_{1,t}^{M,B}| + |c_{1,t}^{M,A}|}, \quad V_{i,t}^{M,s} \sim c_{0,t}^{M,s} + c_{1,t}^{M,s} i + c_{2,t}^{M,s} i^2, \quad s \in \{B,A\})")\
  X(imba_para_c2,       1, DEPTH, TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Depth Parabola c2 Imba",           "买卖抛物线曲率失衡",    "对比买卖订单块距离",                                   R"(\frac{|c_{2,t}^{M,B}| - |c_{2,t}^{M,A}|}{|c_{2,t}^{M,B}| + |c_{2,t}^{M,A}|}, \quad V_{i,t}^{M,s} \sim c_{0,t}^{M,s} + c_{1,t}^{M,s} i + c_{2,t}^{M,s} i^2, \quad s \in \{B,A\})")\
  X(b_5_c1,             1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 5-level Bid Grad",             "买侧五档梯度",          "买侧梯度(近端斜率)",                                   R"(\frac{1}{N-1}\sum_{i=1}^{N-1}(V_{i+1,t}^{M,B} - V_{i,t}^{M,B}), \quad N = 5)")\
  X(a_5_c1,             1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 5-level Ask Grad",             "卖侧五档梯度",          "卖侧梯度(近端斜率)",                                   R"(\frac{1}{N-1}\sum_{i=1}^{N-1}(V_{i+1,t}^{M,A} - V_{i,t}^{M,A}), \quad N = 5)")\
  X(imba_5_c1,          1, DEPTH, TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Top 5-level Grad Ratio",           "买卖五档梯度失衡",      "买卖五档梯度(近端斜率)失衡",                           R"(\frac{|\sum_{i=1}^{N-1}(V_{i+1,t}^{M,B} - V_{i,t}^{M,B})| - |\sum_{i=1}^{N-1}(V_{i+1,t}^{M,A} - V_{i,t}^{M,A})|}{|\sum_{i=1}^{N-1}(V_{i+1,t}^{M,B} - V_{i,t}^{M,B})| + |\sum_{i=1}^{N-1}(V_{i+1,t}^{M,A} - V_{i,t}^{M,A})|}, \quad N = 5)")\
  X(b_5_entropy,        1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 5-level Bid ShannonEntropy",   "买侧五档香农熵",        "0:极端集中; ln(N):极端均匀",                           R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,B} \log(\pi_{i,t}^{M,B}), \quad \pi_{i,t}^{M,B} = \frac{V_{i,t}^{M,B}}{\sum_{j=1}^{N} V_{j,t}^{M,B}}, \quad N = 5)")\
  X(a_5_entropy,        1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 5-level Ask ShannonEntropy",   "卖侧五档香农熵",        "0:极端集中; ln(N):极端均匀",                           R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,A} \log(\pi_{i,t}^{M,A}), \quad \pi_{i,t}^{M,A} = \frac{V_{i,t}^{M,A}}{\sum_{j=1}^{N} V_{j,t}^{M,A}}, \quad N = 5)")\
  X(imba_5_entropy,     1, DEPTH, TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Top 5-level Entropy Imba",         "五档香农熵失衡",        "五档香农熵失衡",                                       R"(\frac{H_{t}^{M,B} - H_{t}^{M,A}}{H_{t}^{M,B} + H_{t}^{M,A}}, \quad H_{t}^{M,s} = -\sum_{i=1}^{N} \pi_{i,t}^{M,s} \log(\pi_{i,t}^{M,s}), \quad \pi_{i,t}^{M,s} = \frac{V_{i,t}^{M,s}}{\sum_{j=1}^{N} V_{j,t}^{M,s}}, \quad N = 5, \quad s \in \{B,A\})")\
  X(b_30_entropy,       1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 30-level Bid ShannonEntropy",  "买侧三十档香农熵",      "0:极端集中; ln(N):极端均匀",                           R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,B} \log(\pi_{i,t}^{M,B}), \quad \pi_{i,t}^{M,B} = \frac{V_{i,t}^{M,B}}{\sum_{j=1}^{N} V_{j,t}^{M,B}}, \quad N = 30)")\
  X(a_30_entropy,       1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Top 30-level Ask ShannonEntropy",  "卖侧三十档香农熵",      "0:极端集中; ln(N):极端均匀",                           R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,A} \log(\pi_{i,t}^{M,A}), \quad \pi_{i,t}^{M,A} = \frac{V_{i,t}^{M,A}}{\sum_{j=1}^{N} V_{j,t}^{M,A}}, \quad N = 30)")\
  X(imba_30_entropy,    1, DEPTH, TS,   IMBALANCE,  RATIO,      NONE,        "100/00/00", "Top 30-level Entropy Imba",        "三十档香农熵失衡",      "三十档香农熵失衡",                                     R"(\frac{H_{t}^{M,B} - H_{t}^{M,A}}{H_{t}^{M,B} + H_{t}^{M,A}}, \quad H_{t}^{M,s} = -\sum_{i=1}^{N} \pi_{i,t}^{M,s} \log(\pi_{i,t}^{M,s}), \quad \pi_{i,t}^{M,s} = \frac{V_{i,t}^{M,s}}{\sum_{j=1}^{N} V_{j,t}^{M,s}}, \quad N = 30, \quad s \in \{B,A\})")\
  X(depth_repre,        1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Depth Representation",             "多档深度表征空间",      "多档深度的自监督表征学习(使用repre backbone)",         R"(\left\{\begin{aligned}\textbf{Input: }&X_t=\{V_{i,t}^{M,B},V_{i,t}^{M,A}\}_{i=1}^{N}\\\textbf{Data Augmentation: }&\tilde X_t=\mathcal{A}(X_t),\ \mathcal{A}=\{\text{volume scaling, noise, level shift}\}\\\textbf{Backbone Encoder: }&z_t=f_{\theta}(\tilde X_t),\ f_{\theta}:\mathbb{R}^{2N}\rightarrow\mathbb{R}^{d}\\\textbf{Projection Head (training only): }&h_t=g_{\phi}(z_t),\ g_{\phi}:\mathbb{R}^{d}\rightarrow\mathbb{R}^{d'}\\\textbf{Training Objective: }&\min_{\theta,\phi}\mathbb{E}\|g_{\phi}(f_{\theta}(\mathcal{A}(X_t)))-g_{\phi}(f_{\theta}(\mathcal{A}(X_{t+\Delta})))\|_2^2\\\textbf{Online Usage: }&\text{depth\_repre}_t=z_t=f_{\theta}(X_t)\end{aligned}\right.)")\
  /* Dynamic Order Flow Features (订单流动态特征) */\
  X(ofi_1,              1, DEPTH, TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Order Flow Imba 1-Level",          "订单流失衡1档",         "对近端大单挂单变动非常敏感",                           R"(\Delta V_{1,t}^{M,B} - \Delta V_{1,t}^{M,A}, \quad \Delta V_{1,t}^{M,B} = \begin{cases}0, & P_{1,t}^{M,B} < P_{1,t-1}^{M,B} \\ V_{1,t}^{M,B} - V_{1,t-1}^{M,B}, & P_{1,t}^{M,B} = P_{1,t-1}^{M,B} \\ V_{1,t}^{M,B}, & P_{1,t}^{M,B} > P_{1,t-1}^{M,B} \end{cases}, \quad \Delta V_{1,t}^{M,A} = \begin{cases}V_{1,t}^{M,A}, & P_{1,t}^{M,A} < P_{1,t-1}^{M,A} \\ V_{1,t}^{M,A} - V_{1,t-1}^{M,A}, & P_{1,t}^{M,A} = P_{1,t-1}^{M,A} \\ 0, & P_{1,t}^{M,A} > P_{1,t-1}^{M,A} \end{cases})")\
  X(ofi_5,              1, DEPTH, TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Order Flow Imba 5-Level",          "订单流失衡5档加权",     "监控5档挂单变化",                                      R"(\Delta V_t^{W,M,B} - \Delta V_t^{W,M,A}, \quad V_t^{W,M,s} = \frac{\sum_{i=1}^N w_i V_{i,t}^{M,s}}{\sum_{i=1}^N w_i}, \quad w_i = 1 - \frac{i-1}{N}, \quad N = 5, \quad s \in \{B,A\})")\
  X(toxic_cr,           1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Toxic Cancel Ratio",               "毒订单流撤单率",        "短窗口撤单占长窗口撤单比例,检测高频撤单行为",          R"(\frac{\sum_{\tau=t-5}^{t}|O_{\tau}^{C}|}{\sum_{\tau=t-60}^{t}|O_{\tau}^{C}|}, \quad |O_{\tau}^{C}|=|O_{\tau}^{C,B}|+|O_{\tau}^{C,A}|)")\
  X(arr_bid,            1, DEPTH, TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Bid Order Arrival Rate",           "买单到达率",            "单位时间买单新增数",                                   R"(\frac{\#O_{\Delta t}^{M,B}}{\Delta t})")\
  X(arr_ask,            1, DEPTH, TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Ask Order Arrival Rate",           "卖单到达率",            "单位时间卖单新增数",                                   R"(\frac{\#O_{\Delta t}^{M,A}}{\Delta t})")\
  X(can_bid,            1, DEPTH, TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Bid Cancellation Rate",            "买单撤单率",            "单位时间买单撤销数",                                   R"(\frac{\#O_{\Delta t}^{C,B}}{\Delta t})")\
  X(can_ask,            1, DEPTH, TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Ask Cancellation Rate",            "卖单撤单率",            "单位时间卖单撤销数",                                   R"(\frac{\#O_{\Delta t}^{C,A}}{\Delta t})")\
  X(trd_buy,            1, DEPTH, TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Active Buy Trade Rate",            "主动买成交率",          "单位时间主动买成交数",                                 R"(\frac{\#O_{\Delta t}^{T,B}}{\Delta t})")\
  X(trd_sell,           1, DEPTH, TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Active Sell Trade Rate",           "主动卖成交率",          "单位时间主动卖成交数",                                 R"(\frac{\#O_{\Delta t}^{T,A}}{\Delta t})")\
  X(net_ord,            1, DEPTH, TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Net Order Flow",                   "净订单流",              "买卖订单流净差",                                       R"((\#O_{\Delta t}^{M,B} - \#O_{\Delta t}^{C,B}) - (\#O_{\Delta t}^{M,A} - \#O_{\Delta t}^{C,A}))")\
  X(foi,                1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Flow Imbalance",                   "订单流失衡",            "买卖订单流不平衡度",                                   R"(\frac{\Delta \#O_t^{B} - \Delta \#O_t^{A}}{\Delta \#O_t^{B} + \Delta \#O_t^{A}}, \quad \Delta \#O_t^{s} = \#O_t^{T,s} - \#O_t^{C,s}, \quad s \in \{B,A\})")\
  X(cc_r,               1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "CC Trade Ratio",                   "连续竞价成交占比",      "连续竞价成交额占全天成交额的比例",                     R"(\frac{|O_t^{T,\mathrm{CA}}|}{|O_t^{T}|})")\
  X(ctr_xl,             1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Cumulative XL Trade Ratio",        "特大单累计成交占比",    "从开盘到t时刻,特大单成交额占总成交额比例",             R"(\frac{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,\mathrm{XL}}|+|O_{\tau}^{T,A,\mathrm{XL}}|)}{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})")\
  X(ctr_l,              1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Cumulative L Trade Ratio",         "大单累计成交占比",      "从开盘到t时刻,大单成交额占总成交额比例",               R"(\frac{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,\mathrm{L}}|+|O_{\tau}^{T,A,\mathrm{L}}|)}{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})")\
  X(ctr_m,              1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Cumulative M Trade Ratio",         "中单累计成交占比",      "从开盘到t时刻,中单成交额占总成交额比例",               R"(\frac{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,\mathrm{M}}|+|O_{\tau}^{T,A,\mathrm{M}}|)}{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})")\
  X(ctr_s,              1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Cumulative S Trade Ratio",         "小单累计成交占比",      "从开盘到t时刻,小单成交额占总成交额比例",               R"(\frac{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,\mathrm{S}}|+|O_{\tau}^{T,A,\mathrm{S}}|)}{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})")\
  X(cnbi,               1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Cumulative Net Buy Ratio",         "累计净买入比率",        "从开盘到t,主动买入成交额减主动卖出成交额(带符号)",     R"(\frac{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|-|O_{\tau}^{T,A}|)}{\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})")\
  X(cnbi_xl,            1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Cumulative Net Buy Ratio XL",      "特大单累计净买入比率",  "特大单对累计净买入失衡的贡献",                         R"(\frac{N_t^{\mathrm{XL}}}{\sum_{c}|N_t^{c}|}, \quad N_t^{c}=\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,c}|-|O_{\tau}^{T,A,c}|), \quad c\in\{\mathrm{XL,L,M,S}\})")\
  X(cnbi_l,             1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Cumulative Net Buy Ratio L",       "大单累计净买入比率",    "大单对累计净买入失衡的贡献",                           R"(\frac{N_t^{\mathrm{L}}}{\sum_{c}|N_t^{c}|}, \quad N_t^{c}=\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,c}|-|O_{\tau}^{T,A,c}|), \quad c\in\{\mathrm{XL,L,M,S}\})")\
  X(cnbi_m,             1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Cumulative Net Buy Ratio M",       "中单累计净买入比率",    "中单对累计净买入失衡的贡献",                           R"(\frac{N_t^{\mathrm{M}}}{\sum_{c}|N_t^{c}|}, \quad N_t^{c}=\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,c}|-|O_{\tau}^{T,A,c}|), \quad c\in\{\mathrm{XL,L,M,S}\})")\
  X(cnbi_s,             1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Cumulative Net Buy Ratio S",       "小单累计净买入比率",    "小单对累计净买入失衡的贡献",                           R"(\frac{N_t^{\mathrm{S}}}{\sum_{c}|N_t^{c}|}, \quad N_t^{c}=\sum_{\tau=t_0}^{t}(|O_{\tau}^{T,B,c}|-|O_{\tau}^{T,A,c}|), \quad c\in\{\mathrm{XL,L,M,S}\})")\
  X(cnbi_am,            1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Net Buy Ratio AM",                 "早盘净买入比率",        "上午有符号净主动成交额",                               R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{AM}}}(|O_{\tau}^{T,B}|-|O_{\tau}^{T,A}|)}{\sum_{\tau\in\mathcal{T}_{\mathrm{AM}}}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})")\
  X(cnbi_pm,            1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Net Buy Ratio PM",                 "尾盘净买入比率",        "下午有符号净主动成交额",                               R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{PM}}}(|O_{\tau}^{T,B}|-|O_{\tau}^{T,A}|)}{\sum_{\tau\in\mathcal{T}_{\mathrm{PM}}}(|O_{\tau}^{T,B}|+|O_{\tau}^{T,A}|)})")\
  X(oa_bcr,             1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "OA Bid Cancel Ratio",              "早盘竞价买方撤单率",    "集合竞价买方撤单额占买方挂单额比例",                   R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{C,B}|}{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{M,B}|}, \quad \mathcal{T}_{\mathrm{OA}}=[09\!:\!15,09\!:\!25))")\
  X(oa_acr,             1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "OA Ask Cancel Ratio",              "早盘竞价卖方撤单率",    "集合竞价卖方撤单额占卖方挂单额比例",                   R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{C,A}|}{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{M,A}|}, \quad \mathcal{T}_{\mathrm{OA}}=[09\!:\!15,09\!:\!25))")\
  X(oa_btr,             1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "OA Bid Trade Ratio",               "早盘竞价买方成交率",    "集合竞价买方成交额占买方挂单额比例",                   R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{T,B}|}{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{M,B}|}, \quad \mathcal{T}_{\mathrm{OA}}=[09\!:\!15,09\!:\!25))")\
  X(oa_atr,             1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "OA Ask Trade Ratio",               "早盘竞价卖方成交率",    "集合竞价卖方成交额占卖方挂单额比例",                   R"(\frac{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{T,A}|}{\sum_{\tau\in\mathcal{T}_{\mathrm{OA}}}|O_{\tau}^{M,A}|}, \quad \mathcal{T}_{\mathrm{OA}}=[09\!:\!15,09\!:\!25))")\
  X(hla_imba,           1, DEPTH, TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Hidden-liquidity-adjusted Imba",   "潜在流动性失衡",        "预测后续时刻的失衡(按照refill/cancel rate)",           R"(\frac{\tilde{V}_{1,t}^{M,B} - \tilde{V}_{1,t}^{M,A}}{\tilde{V}_{1,t}^{M,B} + \tilde{V}_{1,t}^{M,A}}, \quad \tilde{V}_{1,t}^{M,s} = V_{1,t}^{M,s}(1+\rho_t^{s}), \quad \rho_t^{s} = \frac{|O_t^{M,s}| - |O_t^{C,s}|}{|O_t^{M,s}| + |O_t^{C,s}|}, \quad s \in \{B,A\})")\
  /* Behavioral & Strategic Features (行为与策略特征) */\
  X(agg_buy,            1, DEPTH, TS,   BEHAVIORAL, RAW,        NONE,        "100/00/00", "Bid Aggressiveness",               "买单平均侵略性",        "限价买单相对best bid的激进程度",                       R"(\frac{1}{\#O_W^{M,B,\mathrm{lmt}}}\sum_{i\in O_W^{M,B,\mathrm{lmt}}}\log\frac{P_i}{P_{1,\tau_i}^{M,B}}, \quad O_W^{M,B,\mathrm{lmt}}=\{i: \tau_i\in W, s_i=B, \mathrm{type}_i=\mathrm{limit}\})")\
  X(agg_sell,           1, DEPTH, TS,   BEHAVIORAL, RAW,        NONE,        "100/00/00", "Ask Aggressiveness",               "卖单平均侵略性",        "限价卖单相对best ask的激进程度",                       R"(\frac{1}{\#O_W^{M,A,\mathrm{lmt}}}\sum_{i\in O_W^{M,A,\mathrm{lmt}}}\log\frac{P_{1,\tau_i}^{M,A}}{P_i}, \quad O_W^{M,A,\mathrm{lmt}}=\{i: \tau_i\in W, s_i=A, \mathrm{type}_i=\mathrm{limit}\})")\
  X(agg_dif,            1, DEPTH, TS,   BEHAVIORAL, RAW,        NONE,        "100/00/00", "Aggressiveness Diff",              "侵略性差",              "买卖侵略性差值",                                       R"(\bar{a}_W^{B} - \bar{a}_W^{A}, \quad \bar{a}_W^{s}=\frac{1}{\#O_W^{M,s,\mathrm{lmt}}}\sum_{i\in O_W^{M,s,\mathrm{lmt}}}\log\frac{P_i}{P_{1,\tau_i}^{M,s}}, \quad s \in \{B,A\})")\
  X(cpr,                1, DEPTH, TS,   BEHAVIORAL, RATIO,      NONE,        "100/00/00", "Cancel-to-Post Ratio",             "撤挂比",                "撤单量占挂单量比例",                                   R"(\frac{\sum_{\tau\in W}(|O_{\tau}^{C,B}|+|O_{\tau}^{C,A}|)}{\sum_{\tau\in W}(|O_{\tau}^{M,B}|+|O_{\tau}^{M,A}|)})")\
  X(ptc_rt,             1, DEPTH, TS,   BEHAVIORAL, RATIO,      NONE,        "100/00/00", "Pre-Trade Cancel Ratio",           "成交前撤单比",          "成交前T_pre内同向近价撤单占比",                        R"(\frac{\sum_{j\in O_W^{T}}\sum_{\tau=\tau_j-T_{\mathrm{pre}}}^{\tau_j}|O_{\tau}^{C,s_j}|}{\sum_{j\in O_W^{T}}|O_j|}, \quad O_W^{T}=\{j: \tau_j\in W, \mathrm{is\_trade}_j\})")\
  X(fleet_rt,           1, DEPTH, TS,   BEHAVIORAL, RATIO,      NONE,        "100/00/00", "Fleeting Order Ratio",             "闪单占比",              "存活时间<Δ的订单量占比",                               R"(\frac{\sum_{i\in O_W^{M,\mathrm{fleet}}}|O_i|}{\sum_{i\in O_W^{M}}|O_i|}, \quad O_W^{M,\mathrm{fleet}}=\{i\in O_W^{M}: \tau_i^{\mathrm{cxl}}-\tau_i^{\mathrm{post}}<\Delta\})")\
  X(spoof_int,          1, DEPTH, TS,   BEHAVIORAL, RATIO,      NONE,        "100/00/00", "Spoofing Intensity",               "欺骗强度",              "近端大额快速撤单占总撤单比例",                         R"(\frac{\sum_{i\in O_W^{C,\mathrm{spoof}}}|O_i|}{\sum_{i\in O_W^{C}}|O_i|}, \quad O_W^{C,\mathrm{spoof}}=\{i\in O_W^{C}: \tau_i^{\mathrm{cxl}}-\tau_i^{\mathrm{post}}<T_{\mathrm{fast}}, |P_i-P_{1,\tau_i}^{M}|\leq k\cdot\mathrm{tick}, |O_i|\geq q_{\mathrm{large}}\})")\
  X(agg_trd,            1, DEPTH, TS,   BEHAVIORAL, RAW,        NONE,        "100/00/00", "Aggressiveness Trend",             "侵略性趋势",            "子窗口侵略性序列线性回归斜率",                         R"(\hat{\beta}_1, \quad \bar{a}_{\tau}=\hat{\beta}_0+\hat{\beta}_1\tau+\epsilon_{\tau}, \quad \tau\in\{t-W,\ldots,t\})")\
  X(ord_size,           1, DEPTH, TS,   BEHAVIORAL, RAW,        LOG_ZSCORE,  "100/00/00", "Avg Order Size",                   "平均单笔规模",          "窗口内订单平均量",                                     R"(\frac{1}{\#O_W^{M}}\sum_{i\in O_W^{M}}|O_i|)")\
  /* Resiliency & Replenishment Features (韧性与恢复特征) */\
  X(ratio_bid,          1, DEPTH, TS,   RESILIENCE, RATIO,      NONE,        "100/00/00", "Bid Resiliency Ratio",             "买侧韧性比",            "买侧挂单量/消耗量,>1深度增长",                         R"(\frac{|O_W^{M,B}|}{|O_W^{T,B}|+|O_W^{C,B}|})")\
  X(ratio_ask,          1, DEPTH, TS,   RESILIENCE, RATIO,      NONE,        "100/00/00", "Ask Resiliency Ratio",             "卖侧韧性比",            "卖侧挂单量/消耗量,>1深度增长",                         R"(\frac{|O_W^{M,A}|}{|O_W^{T,A}|+|O_W^{C,A}|})")\
  X(imba,               1, DEPTH, TS,   RESILIENCE, RATIO,      NONE,        "100/00/00", "Resiliency Imbalance",             "韧性失衡",              "买卖韧性比差异(正=买侧恢复快)",                        R"(\frac{R^B-R^A}{R^B+R^A}, \quad R^s=\frac{|O_W^{M,s}|}{|O_W^{T,s}|+|O_W^{C,s}|})")\
  X(dev_bid,            1, DEPTH, TS,   RESILIENCE, RAW,        NONE,        "100/00/00", "Bid Depth Deviation",              "买侧深度偏离",          "当前深度vs移动均值偏离度(负=被冲击)",                  R"(\frac{D_t^B-\bar{D}_W^B}{\bar{D}_W^B}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s}, \quad \bar{D}_W^s=\frac{1}{|W|}\sum_{\tau\in W}D_\tau^s)")\
  X(dev_ask,            1, DEPTH, TS,   RESILIENCE, RAW,        NONE,        "100/00/00", "Ask Depth Deviation",              "卖侧深度偏离",          "当前深度vs移动均值偏离度(负=被冲击)",                  R"(\frac{D_t^A-\bar{D}_W^A}{\bar{D}_W^A}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s}, \quad \bar{D}_W^s=\frac{1}{|W|}\sum_{\tau\in W}D_\tau^s)")\
  X(mr_bid,             1, DEPTH, TS,   RESILIENCE, RAW,        NONE,        "100/00/00", "Bid Mean-Reversion Speed",         "买侧均值回归速度",      "深度偏离度变化率(正=恢复中)",                          R"(d_t^B-d_{t-1}^B, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})")\
  X(mr_ask,             1, DEPTH, TS,   RESILIENCE, RAW,        NONE,        "100/00/00", "Ask Mean-Reversion Speed",         "卖侧均值回归速度",      "深度偏离度变化率(正=恢复中)",                          R"(d_t^A-d_{t-1}^A, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})")\
  X(recovery_bid,       1, DEPTH, TS,   RESILIENCE, RAW,        NONE,        "100/00/00", "Bid Recovery Signal",              "买侧恢复信号",          "冲击状态下的正向恢复强度",                             R"(\max(0,\Delta d_t^B)\cdot\mathbf{1}_{d_t^B<0}, \quad \Delta d_t^s=d_t^s-d_{t-1}^s, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})")\
  X(recovery_ask,       1, DEPTH, TS,   RESILIENCE, RAW,        NONE,        "100/00/00", "Ask Recovery Signal",              "卖侧恢复信号",          "冲击状态下的正向恢复强度",                             R"(\max(0,\Delta d_t^A)\cdot\mathbf{1}_{d_t^A<0}, \quad \Delta d_t^s=d_t^s-d_{t-1}^s, \quad d_t^s=\frac{D_t^s-\bar{D}_W^s}{\bar{D}_W^s}, \quad D_t^s=\sum_{i=1}^{N}V_{i,t}^{M,s})")\
  /* Impact & Liquidity Cost Features (价格冲击与流动性成本特征) */\
  X(cost_buy_1,         1, DEPTH, TS,   LIQUIDITY,  RAW,        NONE,        "100/00/00", "Buy Impact Cost 1-Level",          "买方冲击成本1档",       "吃1档卖盘的执行价vs中间价偏离(bps)",                   R"(\frac{\sum_{i=1}^{1} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{1} V_{i,t}^{M,A}} - 1)")\
  X(cost_buy_5,         1, DEPTH, TS,   LIQUIDITY,  RAW,        NONE,        "100/00/00", "Buy Impact Cost 5-Level",          "买方冲击成本5档",       "吃5档卖盘的执行价vs中间价偏离(bps)",                   R"(\frac{\sum_{i=1}^{5} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{5} V_{i,t}^{M,A}} - 1)")\
  X(cost_buy_10,        1, DEPTH, TS,   LIQUIDITY,  RAW,        NONE,        "100/00/00", "Buy Impact Cost 10-Level",         "买方冲击成本10档",      "吃10档卖盘的执行价vs中间价偏离(bps)",                  R"(\frac{\sum_{i=1}^{10} P_{i,t}^{M,A} V_{i,t}^{M,A}}{P_{\mathrm{mid},t} \sum_{i=1}^{10} V_{i,t}^{M,A}} - 1)")\
  X(cost_sell_1,        1, DEPTH, TS,   LIQUIDITY,  RAW,        NONE,        "100/00/00", "Sell Impact Cost 1-Level",         "卖方冲击成本1档",       "吃1档买盘的执行价vs中间价偏离(bps)",                   R"(1 - \frac{\sum_{i=1}^{1} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{1} V_{i,t}^{M,B}})")\
  X(cost_sell_5,        1, DEPTH, TS,   LIQUIDITY,  RAW,        NONE,        "100/00/00", "Sell Impact Cost 5-Level",         "卖方冲击成本5档",       "吃5档买盘的执行价vs中间价偏离(bps)",                   R"(1 - \frac{\sum_{i=1}^{5} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{5} V_{i,t}^{M,B}})")\
  X(cost_sell_10,       1, DEPTH, TS,   LIQUIDITY,  RAW,        NONE,        "100/00/00", "Sell Impact Cost 10-Level",        "卖方冲击成本10档",      "吃10档买盘的执行价vs中间价偏离(bps)",                  R"(1 - \frac{\sum_{i=1}^{10} P_{i,t}^{M,B} V_{i,t}^{M,B}}{P_{\mathrm{mid},t} \sum_{i=1}^{10} V_{i,t}^{M,B}})")\
  /* Anomaly & Structural Outlier Features (异常与结构失衡特征) */\
  X(peak_loc_bid,       1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Bid Peak Location",                "买侧峰位置",            "买侧深度最大值所在档位(前5档)",                        R"(\arg\max_{i \in [1,5]} V_{i,t}^{M,B})")\
  X(peak_loc_ask,       1, DEPTH, TS,   SHAPE,      RAW,        NONE,        "100/00/00", "Ask Peak Location",                "卖侧峰位置",            "卖侧深度最大值所在档位(前5档)",                        R"(\arg\max_{i \in [1,5]} V_{i,t}^{M,A})")\
  X(peak_ratio_bid,     1, DEPTH, TS,   SHAPE,      RATIO,      NONE,        "100/00/00", "Bid Peak Concentration",           "买侧峰集中度",          "买侧最大档位深度/平均档位深度(前5档)",                 R"(\frac{\max_{i \in [1,5]} V_{i,t}^{M,B}}{\frac{1}{5}\sum_{i=1}^{5} V_{i,t}^{M,B}})")\
  X(peak_ratio_ask,     1, DEPTH, TS,   SHAPE,      RATIO,      NONE,        "100/00/00", "Ask Peak Concentration",           "卖侧峰集中度",          "卖侧最大档位深度/平均档位深度(前5档)",                 R"(\frac{\max_{i \in [1,5]} V_{i,t}^{M,A}}{\frac{1}{5}\sum_{i=1}^{5} V_{i,t}^{M,A}})")\
  X(stale_ratio_bid,    1, DEPTH, TS,   BEHAVIORAL, RATIO,      NONE,        "100/00/00", "Bid Stale Order Ratio",            "买侧老单占比",          "存活超T秒大单量占比",                                  R"(\frac{\sum_{i \in O_t^{M,B,\mathrm{stale}}} |O_i|}{\sum_{i \in O_t^{M,B}} |O_i|}, \quad O^{M,s,\mathrm{stale}}=\{i: t-\tau_i^{\mathrm{post}}>T, |O_i|>q_{\mathrm{large}}\})")\
  X(stale_ratio_ask,    1, DEPTH, TS,   BEHAVIORAL, RATIO,      NONE,        "100/00/00", "Ask Stale Order Ratio",            "卖侧老单占比",          "存活超T秒大单量占比",                                  R"(\frac{\sum_{i \in O_t^{M,A,\mathrm{stale}}} |O_i|}{\sum_{i \in O_t^{M,A}} |O_i|}, \quad O^{M,s,\mathrm{stale}}=\{i: t-\tau_i^{\mathrm{post}}>T, |O_i|>q_{\mathrm{large}}\})")\
  /* ======== 截面特征 (Cross-sectional) ======== */\
  X(cs_spread_rank,     1, ALL,   CS,   LIQUIDITY,  RANK,       RANK_ZSCORE, "100/00/00", "CS Spread Rank",                   "价差截面排名",          "spread截面rank→inverse normal",                        R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))")\
  /* ======== 标签 (Labels) ======== */\
  X(next_tick_ret,      1, DATA,  LB,   LABEL,      FUTURE_RET, NONE,        "100/00/00", "Next Tick Return",                 "下tick收益",            "下一tick对数收益",                                     R"(\log\frac{\mathrm{mid}_{t+1}}{\mathrm{mid}_t})")\
  /* ======== 元数据 (Metadata) ======== */\
  X(_link_to_L1,        1, ALL,   META, META,       RAW,        NONE,        "100/00/00", "Link to L1",                       "L1时间索引",            "L0→L1时间映射",                                        R"(\mathrm{idx}_{L1})")\
  X(_depth_valid,       1, ALL,   META, META,       RAW,        NONE,        "100/00/00", "Depth Valid Flag",                 "深度有效标志",          "LOB深度缓冲区完整性标记",                              R"(\mathbf{1}_{\mathrm{valid}})")\
  X(_data_valid,        1, ALL,   META, META,       RAW,        NONE,        "100/00/00", "Data Valid Flag",                  "数据有效标志",          "事件驱动稀疏性标记",                                   R"(\mathbf{1}_{\mathrm{valid}})")\

// ============================================================================
// LEVEL 1: Minute-level Features (分钟级)
// ============================================================================

#define LEVEL_1_FIELDS(X)\
  X(min,                1, DATA,  TS,   BASIC,     OSCILLATOR, SINCOS,       "00/100/00", "Time Min Phase",                   "时间-分钟相位",         "用于因子组合",                                         R"(\sin(\frac{2\pi t}{60}))")\
  X(ci_5,               1, DATA,  TS,   IMBALANCE, RATIO,      NONE,         "00/100/00", "Cumu Imba 5-Level",                "累计5档失衡",           "累计5档订单失衡率(降频)",                              R"(\frac{\sum_{i=1}^{5}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{5}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})")\
  X(ci_10,              1, DATA,  TS,   IMBALANCE, RATIO,      NONE,         "00/100/00", "Cumu Imba 10-Level",               "累计10档失衡",          "累计10档订单失衡率(降频)",                             R"(\frac{\sum_{i=1}^{10}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{10}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})")\
  X(ci_30,              1, DATA,  TS,   IMBALANCE, RATIO,      NONE,         "00/100/00", "Cumu Imba 30-Level",               "累计30档失衡",          "累计30档订单失衡率(降频)",                             R"(\frac{\sum_{i=1}^{30}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{30}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})")\
  X(ci_all,             1, DATA,  TS,   IMBALANCE, RATIO,      NONE,         "00/100/00", "Cumu Imba All-Level",              "累计全档失衡",          "累计所有档订单失衡率(降频)",                           R"(\frac{\sum_{i=1}^{\infty}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{\infty}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})")\
  X(cwi_1,              1, DATA,  TS,   IMBALANCE, RATIO,      NONE,         "00/100/00", "Convexity-weighted Imb γ=1",       "凸加权失衡γ=1",         "考虑全量, 但是近端更高权重(按档位, 降频)",             R"(\frac{\sum_{i=1}^{N} w_i V_{i,t}^{M,B} - \sum_{i=1}^{N} w_i V_{i,t}^{M,A}}{\sum_{i=1}^{N} w_i (V_{i,t}^{M,B} + V_{i,t}^{M,A})}, \quad w_i = \frac{1}{(i+\epsilon)^\gamma}, \quad \gamma = 1)")\
  X(cwi_2,              1, DATA,  TS,   IMBALANCE, RATIO,      NONE,         "00/100/00", "Convexity-weighted Imb γ=2",       "凸加权失衡γ=2",         "考虑全量, 但是近端更高权重(按档位, 降频)",             R"(\frac{\sum_{i=1}^{N} w_i V_{i,t}^{M,B} - \sum_{i=1}^{N} w_i V_{i,t}^{M,A}}{\sum_{i=1}^{N} w_i (V_{i,t}^{M,B} + V_{i,t}^{M,A})}, \quad w_i = \frac{1}{(i+\epsilon)^\gamma}, \quad \gamma = 2)")\
  X(ddi_1,              1, DATA,  TS,   IMBALANCE, RATIO,      NONE,         "00/100/00", "Distance-discounted Imb λ=0.01",   "距离折扣失衡λ=0.01",    "考虑全量, 但是近端更高权重(按距离, 降频)",             R"(\frac{\sum_{i=1}^{N} e^{-\lambda \Delta P_{i,t}} (V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{N} e^{-\lambda \Delta P_{i,t}} (V_{i,t}^{M,B} + V_{i,t}^{M,A})}, \quad \Delta P_{i,t} = i \cdot \text{tick}, \quad \lambda = 0.01)")\
  X(ddi_2,              1, DATA,  TS,   IMBALANCE, RATIO,      NONE,         "00/100/00", "Distance-discounted Imb λ=0.02",   "距离折扣失衡λ=0.02",    "考虑全量, 但是近端更高权重(按距离, 降频)",             R"(\frac{\sum_{i=1}^{N} e^{-\lambda \Delta P_{i,t}} (V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{N} e^{-\lambda \Delta P_{i,t}} (V_{i,t}^{M,B} + V_{i,t}^{M,A})}, \quad \Delta P_{i,t} = i \cdot \text{tick}, \quad \lambda = 0.02)")\
  X(tbr_5,              1, DATA,  TS,   LIQUIDITY, RATIO,      NONE,         "00/100/00", "Top 5-level Bid Ratio",            "前5档买单占比",         "买单侧是否容易被击穿(降频)",                           R"(\frac{\sum_{i=1}^{N} V_{i,t}^{M,B}}{\sum_{i=1}^{\infty} V_{i,t}^{M,B}}, \quad N = 5)")\
  X(tar_5,              1, DATA,  TS,   LIQUIDITY, RATIO,      NONE,         "00/100/00", "Top 5-level Ask Ratio",            "前5档卖单占比",         "卖单侧是否容易被击穿(降频)",                           R"(\frac{\sum_{i=1}^{N} V_{i,t}^{M,A}}{\sum_{i=1}^{\infty} V_{i,t}^{M,A}}, \quad N = 5)")\
  X(cs_spread_rank,     1, DATA,  CS,   LIQUIDITY, RANK,       RANK_ZSCORE,  "00/100/00", "CS Spread Rank",                   "价差截面排名",          "spread截面rank→inverse normal",                        R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))")\
  X(_ohlc_open,         1, DATA,  META, META,      RAW,        NONE,         "00/100/00", "OHLC Open",                        "开盘价",                "GUI:分钟开盘价(分)",                                   R"(O)")\
  X(_ohlc_high,         1, DATA,  META, META,      RAW,        NONE,         "00/100/00", "OHLC High",                        "最高价",                "GUI:分钟最高价(分)",                                   R"(H)")\
  X(_ohlc_low,          1, DATA,  META, META,      RAW,        NONE,         "00/100/00", "OHLC Low",                         "最低价",                "GUI:分钟最低价(分)",                                   R"(L)")\
  X(_ohlc_close,        1, DATA,  META, META,      RAW,        NONE,         "00/100/00", "OHLC Close",                       "收盘价",                "GUI:分钟收盘价(分)",                                   R"(C)")\
  X(_ohlc_volume,       1, DATA,  META, META,      RAW,        NONE,         "00/100/00", "OHLC Volume",                      "成交量",                "GUI:分钟成交量",                                       R"(V)")\
  X(_data_valid,        1, ALL,   META, META,      RAW,        NONE,         "00/100/00", "Data Valid Flag",                  "数据有效标志",          "事件驱动稀疏性标记",                                   R"(\mathbf{1}_{\mathrm{valid}})")

// ============================================================================
// DEPTH: LOB Snapshot Data (separate storage for orderflow visualization)
// ============================================================================
// Format: X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, name_en, name_cn, description, formula)

#define DEPTH_FIELDS(X)\
  X(_bid_price,         L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE,         "00/00/00",  "Bid Prices",        "买盘价格", "GUI:N档买盘价格(分)",       R"(P^{M,B}_{0:N})")\
  X(_ask_price,         L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE,         "00/00/00",  "Ask Prices",        "卖盘价格", "GUI:N档卖盘价格(分)",       R"(P^{M,A}_{0:N})")\
  X(_bid_volume,        L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE,         "00/00/00",  "Bid Volumes",       "买盘量",   "GUI:N档买盘量(手,100股)",   R"(V^{M,B}_{0:N})")\
  X(_ask_volume,        L2::LOB_DEPTH, DEPTH, META, META, RAW, NONE,         "00/00/00",  "Ask Volumes",       "卖盘量",   "GUI:N档卖盘量(手,100股)",   R"(V^{M,A}_{0:N})")\
  X(_mid_price,         1,             DEPTH, META, META, RAW, NONE,         "00/00/00",  "Mid Price",         "中间价",   "GUI:实时中间价(分)",        R"(\frac{P^{M,B}_1 + P^{M,A}_1}{2})")\
  X(_depth_valid,       1,             ALL,   META, META, RAW, NONE,         "00/00/00",  "Depth Valid Flag",  "深度有效", "LOB深度缓冲区完整性标记",   R"(\mathbf{1}_{\mathrm{valid}_{depth}})")\
  X(_data_valid,        1,             ALL,   META, META, RAW, NONE,         "00/00/00",  "Data Valid Flag",   "数据有效", "事件驱动稀疏性标记",        R"(\mathbf{1}_{\mathrm{valid}_{data}})")

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
