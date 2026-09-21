#pragma once

// =============================================================================
// 因子算子真相表 (X-macro; 语义契约见 factor/Contract.hpp; 成本与误差见 operator1.md)
// =============================================================================
//   分类只有两轴, 两轴的属性集不同 → 两种行格式, 不共用列:
//
//   TS 行  X(Name, 元数, 窗, "参数", GPU策略, "公式")
//     元数     输入序列数 0..3 (窗长 d / 阈值 k 不算元)
//     窗       POINT  逐点 (无状态, TS 的退化)
//              EXPAND 段内 expanding (段 = 交易日 kSegLen 分钟, 段界 reset)
//              ROLL   最近 d 期滚动 (期 = 分钟, 跨段不 reset)
//              EXPO   指数递推 (系数 k, 全程不 reset)
//     GPU策略  POINT 逐格融合 / GATHER 移位取值 / SCAN 分段前缀和差分 /
//              DOUBLE doubling sweep (滑窗极值) / HIST 桶直方图 (序统计近似) / RECUR 仿射 scan
//
//   CS 行  X(Name, 元数, "参数", GPU策略, "公式")
//     GPU策略  REDUCE 沿资产归约 / HIST 片上桶直方图 / GROUP 分组归约
//
//   加算子 = 本表一行 + TS(或 CS) 的 Stream / Naive / Gpu 各一个同名 struct; 缺任一侧 → op_check 编译错.
//   Cum* 与 Ts* 是同一统计核在 EXPAND / ROLL 两窗上的实例 (公式只写一份, 见 TS/Stream.hpp).
//   YoY 不入表: = TsDelay + Div 的组合, 且 d 需一年分钟数, 超出块 carry 上限.
// =============================================================================

// ---- TS: 逐点 (22) ----
#define OP_TS_POINT(X)                                       \
  X(Abs, 1, POINT, "", POINT, "|x|")                         \
  X(Sign, 1, POINT, "", POINT, "sign(x)")                    \
  X(Log, 1, POINT, "", POINT, "sign(x)·log1p(|x|)")          \
  X(Asinh, 1, POINT, "", POINT, "asinh(x)")                  \
  X(Tanh, 1, POINT, "", POINT, "tanh(x)")                    \
  X(Sqrt, 1, POINT, "", POINT, "sign(x)·√|x|")               \
  X(Relu, 1, POINT, "", POINT, "max(0, x)")                  \
  X(Recip, 1, POINT, "", POINT, "1/x, |x| < eps 退化")       \
  X(SignedPow, 1, POINT, "k", POINT, "sign(x)·|x|^k")        \
  X(Clip, 1, POINT, "k", POINT, "clamp(x, −k, k)")           \
  X(TodMask, 0, POINT, "k,k2", POINT, "1[k ≤ t_seg < k2]")   \
  X(Add, 2, POINT, "", POINT, "x + y")                       \
  X(Sub, 2, POINT, "", POINT, "x − y")                       \
  X(Mul, 2, POINT, "", POINT, "x · y")                       \
  X(Div, 2, POINT, "", POINT, "x / y, |y| < eps 退化")       \
  X(Max, 2, POINT, "", POINT, "max(x, y)")                   \
  X(Min, 2, POINT, "", POINT, "min(x, y)")                   \
  X(Imb, 2, POINT, "", POINT, "(x − y)/(x + y), 和相对退化") \
  X(Share, 2, POINT, "", POINT, "x/(x + y), 和相对退化")     \
  X(LogRatio, 2, POINT, "", POINT, "ln(x/y), 任一 ≤ 0 退化") \
  X(Where, 3, POINT, "", POINT, "x > 0 ? y : z")             \
  X(Clip3, 3, POINT, "", POINT, "clamp(x, y, z), y > z 退化")

// ---- TS: 段内 expanding (23) ----
#define OP_TS_EXPAND(X)                                                                       \
  X(CumSum, 1, EXPAND, "", SCAN, "Σ_{s≤t} x_s")                                               \
  X(CumMean, 1, EXPAND, "", SCAN, "Σx / n")                                                   \
  X(CumVar, 1, EXPAND, "", SCAN, "样本方差 (ddof=1), n < 2 退化")                             \
  X(CumStd, 1, EXPAND, "", SCAN, "√CumVar")                                                   \
  X(CumSkew, 1, EXPAND, "", SCAN, "m3/m2^1.5 (总体矩), n < 3 退化")                           \
  X(CumKurt, 1, EXPAND, "", SCAN, "m4/m2² − 3 (总体矩), n < 4 退化")                          \
  X(CumMax, 1, EXPAND, "", SCAN, "max_{s≤t} x_s (cummax)")                                    \
  X(CumMin, 1, EXPAND, "", SCAN, "min_{s≤t} x_s")                                             \
  X(CumArgMax, 1, EXPAND, "", SCAN, "首个最大值的段内位置 s")                                 \
  X(CumArgMin, 1, EXPAND, "", SCAN, "首个最小值的段内位置 s")                                 \
  X(CumRank, 1, EXPAND, "", HIST, "x_t 在 {x_s, s≤t} 的 pct rank")                            \
  X(CumHhi, 1, EXPAND, "", SCAN, "Σx²/(Σx)²")                                                 \
  X(CumEntropy, 1, EXPAND, "", SCAN, "ln Σx − Σ x ln x / Σx (只计 x > 0)")                    \
  X(CumTopK, 1, EXPAND, "k", HIST, "前 k 大之和 / Σx, n < k 退化")                            \
  X(CumGini, 1, EXPAND, "", HIST, "Lorenz 基尼系数 (只计 x > 0)")                             \
  X(CumPeaks, 1, EXPAND, "k", SCAN, "局部峰数: x_{s−1} < x_s > x_{s+1} 且 x_s > k·CumMean_s") \
  X(CumCountGt, 1, EXPAND, "k", SCAN, "Σ 1[x_s > k]")                                         \
  X(CumCov, 2, EXPAND, "", SCAN, "样本协方差 (ddof=1), n < 2 退化")                           \
  X(CumCorr, 2, EXPAND, "", SCAN, "Pearson, 任一离散度退化则退化")                            \
  X(CumBeta, 2, EXPAND, "", SCAN, "x 对 y 的 OLS 斜率 cov/var(y)")                            \
  X(CumResid, 2, EXPAND, "", SCAN, "(x_t − x̄) − β(y_t − ȳ)")                                  \
  X(CumWMean, 2, EXPAND, "", SCAN, "Σ y·x / Σ y (y 为权)")                                    \
  X(CumCorrLag, 2, EXPAND, "k", SCAN, "corr(x_s, y_{s−k}), y 延迟 k 期")

// ---- TS: 滚动 d 窗 (26) ----
#define OP_TS_ROLL(X)                                                       \
  X(TsDelay, 1, ROLL, "d", GATHER, "x_{t−d}")                               \
  X(TsDelta, 1, ROLL, "d", GATHER, "x_t − x_{t−d}")                         \
  X(TsSum, 1, ROLL, "d", SCAN, "窗和")                                      \
  X(TsMean, 1, ROLL, "d", SCAN, "窗均")                                     \
  X(TsVar, 1, ROLL, "d", SCAN, "窗样本方差 (ddof=1)")                       \
  X(TsStd, 1, ROLL, "d", SCAN, "√TsVar")                                    \
  X(TsSkew, 1, ROLL, "d", SCAN, "窗 m3/m2^1.5")                             \
  X(TsKurt, 1, ROLL, "d", SCAN, "窗 m4/m2² − 3")                            \
  X(TsMax, 1, ROLL, "d", DOUBLE, "窗最大")                                  \
  X(TsMin, 1, ROLL, "d", DOUBLE, "窗最小")                                  \
  X(TsArgMax, 1, ROLL, "d", DOUBLE, "首个 (最旧) 最大值距今期数 0..d−1")    \
  X(TsArgMin, 1, ROLL, "d", DOUBLE, "首个 (最旧) 最小值距今期数")           \
  X(TsMed, 1, ROLL, "d", HIST, "窗中位数 (桶近似, 偶数不平均)")             \
  X(TsMad, 1, ROLL, "d", HIST, "median(|x − med|)")                         \
  X(TsRank, 1, ROLL, "d", HIST, "x_t 在窗内 pct rank")                      \
  X(TsZ, 1, ROLL, "d", SCAN, "(x_t − μ)/σ (ddof=1)")                        \
  X(TsWma, 1, ROLL, "d", SCAN, "线性加权: w 最旧 = 1 … 最新 = d")           \
  X(TsProduct, 1, ROLL, "d", SCAN, "Π(1 + x) − 1, 任一 1+x ≤ 0 退化")       \
  X(TsSlope, 1, ROLL, "d", SCAN, "x 对期序 i = 0..d−1 的 OLS 斜率")         \
  X(TsCountGt, 1, ROLL, "d,k", SCAN, "窗内 1[x > k] 计数 (精确, k 不免费)") \
  X(EventAge, 1, ROLL, "d", SCAN, "距上次 x 变动的期数, 窗内无变动则 d")    \
  X(TsCov, 2, ROLL, "d", SCAN, "窗样本协方差 (ddof=1)")                     \
  X(TsCorr, 2, ROLL, "d", SCAN, "窗 Pearson")                               \
  X(TsBeta, 2, ROLL, "d", SCAN, "x 对 y 的窗 OLS 斜率")                     \
  X(TsResid, 2, ROLL, "d", SCAN, "(x_t − x̄) − β(y_t − ȳ)")                  \
  X(TsWMean, 2, ROLL, "d", SCAN, "Σ y·x / Σ y (y 为权)")

// ---- TS: 指数递推 (1) ----
#define OP_TS_EXPO(X) \
  X(TsEma, 1, EXPO, "k", RECUR, "y = k·x + (1−k)·y (0 < k ≤ 1), 首个有效值起, 无效保持")

// ---- CS (20) ----
#define OP_CS(X)                                                   \
  X(CsMean, 1, "", REDUCE, "截面均值广播")                         \
  X(CsStd, 1, "", REDUCE, "截面样本标准差广播 (ddof=1)")           \
  X(CsDemean, 1, "", REDUCE, "x − 截面均值")                       \
  X(CsZ, 1, "", REDUCE, "(x − μ)/σ (ddof=1)")                      \
  X(CsRank, 1, "", HIST, "pct rank (并列均秩)")                    \
  X(CsNormRank, 1, "", HIST, "Φ⁻¹(clamp(pct, 1/(N+1), N/(N+1)))")  \
  X(CsMedian, 1, "", HIST, "截面中位数广播")                       \
  X(CsQuantile, 1, "k", HIST, "截面 k 分位广播")                   \
  X(CsWinsor, 1, "k", HIST, "分位缩尾 clamp 到 [q_k, q_{1−k}]")    \
  X(CsWinsorRank, 1, "", HIST, "先缩尾 (k = 0.01) 再 pct rank")    \
  X(CsWinsorZ, 1, "", HIST, "先缩尾 (k = 0.01) 再 z")              \
  X(CsBucket, 1, "k", HIST, "floor(pct·k) ∈ 0..k−1")               \
  X(CsResid, 2, "", REDUCE, "x 对 y 截面 OLS (含截距) 残差")       \
  X(CsBeta, 2, "", REDUCE, "x 对 y 截面 OLS 斜率广播")             \
  X(CsCorr, 2, "", REDUCE, "截面 Pearson 广播")                    \
  X(CsRankDiff, 2, "", HIST, "pct_rank(x) − pct_rank(y)")          \
  X(CsGroupMean, 2, "", GROUP, "按 y 分组 (整数 id) 的组均值广播") \
  X(CsGroupRank, 2, "", GROUP, "按 y 分组的组内 pct rank")         \
  X(CsCondRank, 2, "k", GROUP, "y 分 k 桶, x 在桶内 pct rank")     \
  X(CsGroupResid, 3, "", GROUP, "按 z 分组: x, y 组内 demean 后 x 对 y 回归残差 (FWL)")

// ---- 全表 ----
#define OP_TS(X) OP_TS_POINT(X) OP_TS_EXPAND(X) OP_TS_ROLL(X) OP_TS_EXPO(X)
#define OP_ALL_TS_CS(TS_X, CS_X) OP_TS(TS_X) OP_CS(CS_X)
