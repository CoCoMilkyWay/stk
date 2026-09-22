#pragma once

// =============================================================================
// 因子算子真相表 (X-macro; 语义契约见 factor/Contract.hpp; 成本与误差见 operator.md)
// =============================================================================
//   命名 = 轴_核_窗 (代码里 PascalCase, 文档里 snake_case, 一一对应):
//     轴   Ts 时序 / Cs 截面 —— 顶层只有这两类
//     核   统计量本身 (Mean / Var / Rank / Corr …), 与窗正交
//     窗   Cum 段内 expanding / Roll 最近 d 期 / Ema 指数递推; 无窗后缀 = 逐点
//   同核不同窗只是同一公式挂在不同窗上, 故本表里 *Cum / *Roll 成对相邻.
//
//   分类只有两轴, 两轴的属性集不同 → 两种行格式, 不共用列:
//
//   TS 行  X(Name, 元数, 窗, "参数", GPU策略, "公式")
//     元数     输入序列数 0..3 (窗长 d / 阈值 k 不算元)
//     窗       POINT  逐点 (无状态, TS 的退化)
//              EXPAND 段内 expanding (段 = 交易日 kSegLen 分钟, 段界 reset)
//              ROLL   最近 d 期滚动 (期 = 分钟, 跨段不 reset)
//              EXPO   指数递推 (系数 k, 全程不 reset)
//     GPU策略  POINT 逐格融合 / GATHER 移位取值 / SCAN 分段前缀和差分 /
//              EXTREME van Herk 两趟块内前后缀 (滑窗极值) / HIST 桶直方图 (序统计近似) / RECUR 仿射 scan
//
//   CS 行  X(Name, 元数, "参数", GPU策略, "公式")
//     GPU策略  REDUCE 沿资产归约 / HIST 片上桶直方图 / GROUP 分组归约
//
//   加算子 = 本表一行 + TS(或 CS) 的 Stream / Naive / Gpu 各一个同名 struct; 缺任一侧 → op_check 编译错;
//   流式 struct::kWin / GPU struct::kStrat 与本表不符 → op_check static_assert 错.
//   TsYoY 不入表: = TsDelayRoll + TsDiv 的组合, 且 d 需一年分钟数, 超出块 carry 上限.
//   "退化" 一词的含义见 Contract.hpp: 全并列 (精确) / 相消 (相对 kRelEps) / y = 0 (逐点).
// =============================================================================

// ---- TS: 逐点 (22) ----
#define OP_TS_POINT(X)                                             \
  X(TsAbs, 1, POINT, "", POINT, "|x|")                             \
  X(TsSign, 1, POINT, "", POINT, "sign(x)")                        \
  X(TsLog, 1, POINT, "", POINT, "sign(x)·log1p(|x|)")              \
  X(TsAsinh, 1, POINT, "", POINT, "asinh(x)")                      \
  X(TsTanh, 1, POINT, "", POINT, "tanh(x)")                        \
  X(TsSqrt, 1, POINT, "", POINT, "sign(x)·√|x|")                   \
  X(TsRelu, 1, POINT, "", POINT, "max(0, x)")                      \
  X(TsRecip, 1, POINT, "", POINT, "1/x, x = 0 退化")               \
  X(TsSignedPow, 1, POINT, "k", POINT, "sign(x)·|x|^k")            \
  X(TsClip, 1, POINT, "k", POINT, "clamp(x, −k, k), k ≥ 0")        \
  X(TsTodMask, 0, POINT, "k,k2", POINT, "1[k ≤ t_seg < k2]")       \
  X(TsAdd, 2, POINT, "", POINT, "x + y")                           \
  X(TsSub, 2, POINT, "", POINT, "x − y")                           \
  X(TsMul, 2, POINT, "", POINT, "x · y")                           \
  X(TsDiv, 2, POINT, "", POINT, "x / y, y = 0 退化")               \
  X(TsMax, 2, POINT, "", POINT, "max(x, y)")                       \
  X(TsMin, 2, POINT, "", POINT, "min(x, y)")                       \
  X(TsImb, 2, POINT, "", POINT, "(x − y)/(x + y), 和相消退化")     \
  X(TsShare, 2, POINT, "", POINT, "x/(x + y), 和相消退化")         \
  X(TsLogRatio, 2, POINT, "", POINT, "ln x − ln y, 任一 ≤ 0 退化") \
  X(TsWhere, 3, POINT, "", POINT, "x > 0 ? y : z")                 \
  X(TsClip3, 3, POINT, "", POINT, "clamp(x, y, z), y > z 退化")

// ---- TS: 有窗 (50 = Cum 23 + Roll 26 + Ema 1); 按统计核排, 同核的不同窗相邻 ----
#define OP_TS_WIN(X)                                                                              \
  X(TsDelayRoll, 1, ROLL, "d", GATHER, "x_{t−d}")                                                 \
  X(TsDeltaRoll, 1, ROLL, "d", GATHER, "x_t − x_{t−d}")                                           \
  X(TsSumCum, 1, EXPAND, "", SCAN, "Σ_{s≤t} x_s")                                                 \
  X(TsSumRoll, 1, ROLL, "d", SCAN, "窗和")                                                        \
  X(TsMeanCum, 1, EXPAND, "", SCAN, "Σx / n")                                                     \
  X(TsMeanRoll, 1, ROLL, "d", SCAN, "窗均")                                                       \
  X(TsMeanEma, 1, EXPO, "k", RECUR, "y = k·x + (1−k)·y (0 < k ≤ 1), 首个有效值起, 无效保持")      \
  X(TsVarCum, 1, EXPAND, "", SCAN, "样本方差 (ddof=1), n < 2 或全并列退化")                       \
  X(TsVarRoll, 1, ROLL, "d", SCAN, "窗样本方差 (ddof=1)")                                         \
  X(TsStdCum, 1, EXPAND, "", SCAN, "√TsVarCum")                                                   \
  X(TsStdRoll, 1, ROLL, "d", SCAN, "√TsVarRoll")                                                  \
  X(TsSkewCum, 1, EXPAND, "", SCAN, "m3/m2^1.5 (总体矩), n < 3 或全并列退化")                     \
  X(TsSkewRoll, 1, ROLL, "d", SCAN, "窗 m3/m2^1.5")                                               \
  X(TsKurtCum, 1, EXPAND, "", SCAN, "m4/m2² − 3 (总体矩), n < 4 或全并列退化")                    \
  X(TsKurtRoll, 1, ROLL, "d", SCAN, "窗 m4/m2² − 3")                                              \
  X(TsMaxCum, 1, EXPAND, "", SCAN, "max_{s≤t} x_s (cummax)")                                      \
  X(TsMaxRoll, 1, ROLL, "d", EXTREME, "窗最大")                                                   \
  X(TsMinCum, 1, EXPAND, "", SCAN, "min_{s≤t} x_s")                                               \
  X(TsMinRoll, 1, ROLL, "d", EXTREME, "窗最小")                                                   \
  X(TsArgMaxCum, 1, EXPAND, "", SCAN, "首个 (最旧) 最大值距今期数 0..n−1")                        \
  X(TsArgMaxRoll, 1, ROLL, "d", EXTREME, "首个 (最旧) 最大值距今期数 0..d−1")                     \
  X(TsArgMinCum, 1, EXPAND, "", SCAN, "首个 (最旧) 最小值距今期数")                               \
  X(TsArgMinRoll, 1, ROLL, "d", EXTREME, "首个 (最旧) 最小值距今期数")                            \
  X(TsRankCum, 1, EXPAND, "", HIST, "x_t 在 {x_s, s≤t} 的 pct rank")                              \
  X(TsRankRoll, 1, ROLL, "d", HIST, "x_t 在窗内 pct rank")                                        \
  X(TsMedianRoll, 1, ROLL, "d", HIST, "窗中位数 (桶近似, 偶数不平均)")                            \
  X(TsMadRoll, 1, ROLL, "d", HIST, "median(|x − med|)")                                           \
  X(TsZRoll, 1, ROLL, "d", SCAN, "(x_t − μ)/σ (ddof=1)")                                          \
  X(TsWmaRoll, 1, ROLL, "d", SCAN, "线性加权: w 最旧 = 1 … 最新 = d")                             \
  X(TsProductRoll, 1, ROLL, "d", SCAN, "Π(1 + x) − 1, 任一 1+x ≤ 0 退化")                         \
  X(TsSlopeRoll, 1, ROLL, "d", SCAN, "x 对期序 i = 0..d−1 的 OLS 斜率, n < 2 退化")               \
  X(TsCountGtCum, 1, EXPAND, "k", SCAN, "Σ 1[x_s > k]")                                           \
  X(TsCountGtRoll, 1, ROLL, "d,k", SCAN, "窗内 1[x > k] 计数 (精确, k 不免费)")                   \
  X(TsAgeRoll, 1, ROLL, "d", SCAN, "距上次 x 变动 (精确不等) 的期数, 窗内无变动则 d")             \
  X(TsHhiCum, 1, EXPAND, "", SCAN, "Σx²/(Σx)², Σx 相消退化")                                      \
  X(TsEntropyCum, 1, EXPAND, "", SCAN, "ln Σx − Σ x ln x / Σx (只计 x > 0, 无正样本退化)")        \
  X(TsTopKCum, 1, EXPAND, "k", HIST, "前 k 大之和 / Σx, n < k 或 Σx 相消退化")                    \
  X(TsGiniCum, 1, EXPAND, "", HIST, "Lorenz 基尼系数 (只计 x > 0, 正样本 < 2 退化)")              \
  X(TsPeaksCum, 1, EXPAND, "k", SCAN, "局部峰数: x_{s−1} < x_s > x_{s+1} 且 x_s > k·TsMeanCum_s") \
  X(TsCovCum, 2, EXPAND, "", SCAN, "样本协方差 (ddof=1), n < 2 退化")                             \
  X(TsCovRoll, 2, ROLL, "d", SCAN, "窗样本协方差 (ddof=1)")                                       \
  X(TsCorrCum, 2, EXPAND, "", SCAN, "Pearson, x 或 y 全并列退化")                                 \
  X(TsCorrRoll, 2, ROLL, "d", SCAN, "窗 Pearson")                                                 \
  X(TsCorrLagCum, 2, EXPAND, "k", SCAN, "corr(x_s, y_{s−k}), y 延迟 k 期")                        \
  X(TsBetaCum, 2, EXPAND, "", SCAN, "x 对 y 的 OLS 斜率 cov/var(y), y 全并列退化")                \
  X(TsBetaRoll, 2, ROLL, "d", SCAN, "x 对 y 的窗 OLS 斜率")                                       \
  X(TsResidCum, 2, EXPAND, "", SCAN, "(x_t − x̄) − β(y_t − ȳ)")                                    \
  X(TsResidRoll, 2, ROLL, "d", SCAN, "(x_t − x̄) − β(y_t − ȳ)")                                    \
  X(TsWMeanCum, 2, EXPAND, "", SCAN, "Σ y·x / Σ y (y 为权), Σy 相消退化")                         \
  X(TsWMeanRoll, 2, ROLL, "d", SCAN, "Σ y·x / Σ y (y 为权)")

// ---- CS (20): 截面无窗, 故只有轴_核 ----
#define OP_CS(X)                                                           \
  X(CsMean, 1, "", REDUCE, "截面均值广播")                                 \
  X(CsStd, 1, "", REDUCE, "截面样本标准差广播 (ddof=1), 全并列退化")       \
  X(CsDemean, 1, "", REDUCE, "x − 截面均值")                               \
  X(CsZ, 1, "", REDUCE, "(x − μ)/σ (ddof=1), 全并列退化")                  \
  X(CsRank, 1, "", HIST, "pct rank (并列均秩)")                            \
  X(CsNormRank, 1, "", HIST, "Φ⁻¹(clamp(pct, 1/(N+1), N/(N+1)))")          \
  X(CsMedian, 1, "", HIST, "截面中位数广播")                               \
  X(CsQuantile, 1, "k", HIST, "截面 k 分位广播")                           \
  X(CsWinsor, 1, "k", HIST, "分位缩尾 clamp 到 [q_k, q_{1−k}]")            \
  X(CsWinsorRank, 1, "", HIST, "先缩尾 (k = 0.01) 再 pct rank")            \
  X(CsWinsorZ, 1, "", HIST, "先缩尾 (k = 0.01) 再 z")                      \
  X(CsBucket, 1, "k", HIST, "floor(pct·k) ∈ 0..k−1")                       \
  X(CsResid, 2, "", REDUCE, "x 对 y 截面 OLS (含截距) 残差, y 全并列退化") \
  X(CsBeta, 2, "", REDUCE, "x 对 y 截面 OLS 斜率广播, y 全并列退化")       \
  X(CsCorr, 2, "", REDUCE, "截面 Pearson 广播, x 或 y 全并列退化")         \
  X(CsRankDiff, 2, "", HIST, "pct_rank(x) − pct_rank(y)")                  \
  X(CsGroupMean, 2, "", GROUP, "按 y 分组 (整数 id) 的组均值广播")         \
  X(CsGroupRank, 2, "", GROUP, "按 y 分组的组内 pct rank")                 \
  X(CsCondRank, 2, "k", GROUP, "y 分 k 桶, x 在桶内 pct rank")             \
  X(CsGroupResid, 3, "", GROUP, "按 z 分组: x, y 组内 demean 后 x 对 y 回归残差 (FWL), y 组内全并列退化")

// ---- 全表 ----
#define OP_TS(X) OP_TS_POINT(X) OP_TS_WIN(X)
#define OP_ALL_TS_CS(TS_X, CS_X) OP_TS(TS_X) OP_CS(CS_X)
