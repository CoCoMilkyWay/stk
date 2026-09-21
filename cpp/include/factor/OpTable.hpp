#pragma once

// =============================================================================
// 因子算子真相表 (X-macro): 流式 (factor/stream/*.hpp) / 向量式 (py/factor/ops/*.py) / 对拍 (op_stream + py/factor/check) 三方共同展开.
// =============================================================================
//   行格式  X(Name, "params", "描述")
//     Name    算子名, 三方同名: C++ struct factor::Name / python 函数 ops.<axis>.Name / CLI  op_stream Name
//     params  用到的参数, 逗号分隔, 取自 Param{d, k, k2} (Kernel.hpp): "" 无参; "d" 窗口; "k" 阈值/指数; "k,k2" 两阈值
//     描述    一句话公式
//   分组 = 轴 × 元数 (元数 = 序列操作数, 参数不算元):
//     ELEM  逐值 (无状态)                    Elem.hpp   1/2/3 元
//     CUM   日内 expanding (开盘起累计, 日 reset)  Cum.hpp    1/2 元
//     ROLL  跨日滚动 D 窗 (日频序列)            Roll.hpp   1/2 元
//     CS    截面 (同一时刻跨资产)              Cs.hpp     1/2/3 元
//   加算子 = 本表一行 + 对应 .hpp 一个 struct + 对应 .py 一个函数; 缺任一侧 → op_stream 编译错 / check 报 missing.
//   语义契约 (三方必须一致, 见 Kernel.hpp 头注): NaN 跳过不计 n; n < min_n → NaN; ROLL 窗未满 → NaN; rank 并列均秩.
// =============================================================================

// ---- ELEM 一元 ----
#define OP_ELEM1(X)                  \
  X(Abs, "", "|x|")                  \
  X(Sign, "", "sign(x)")             \
  X(Log, "", "sign(x)·log1p(|x|)")   \
  X(Asinh, "", "asinh(x)")           \
  X(Tanh, "", "tanh(x)")             \
  X(Sqrt, "", "sign(x)·sqrt(|x|)")   \
  X(Relu, "", "max(0, x)")           \
  X(Recip, "", "1/x, x=0 → NaN")     \
  X(SignedPow, "k", "sign(x)·|x|^k") \
  X(Clip, "k", "clamp(x, −k, k)")

// ---- ELEM 二元 ----
#define OP_ELEM2(X)                         \
  X(Add, "", "x + y")                       \
  X(Sub, "", "x − y")                       \
  X(Mul, "", "x · y")                       \
  X(Div, "", "x / y, y=0 → NaN")            \
  X(Max, "", "max(x, y)")                   \
  X(Min, "", "min(x, y)")                   \
  X(Imb, "", "(x − y)/(x + y), 和=0 → NaN") \
  X(Share, "", "x/(x + y), 和=0 → NaN")     \
  X(LogRatio, "", "ln(x/y), 非正 → NaN")

// ---- ELEM 三元 ----
#define OP_ELEM3(X) \
  X(Where, "", "x > 0 ? y : z (x NaN → NaN)")

// ---- CUM 一元 (t = 日内分钟位 0..; 状态 O(1), 标 * 者需存当日全序列) ----
#define OP_CUM1(X)                                                       \
  X(CumSum, "", "Σ_{s≤t} x_s")                                           \
  X(CumMean, "", "Σx/n")                                                 \
  X(CumStd, "", "样本标准差 (ddof=1), n<2 → NaN")                        \
  X(CumVar, "", "样本方差 (ddof=1), n<2 → NaN")                          \
  X(CumSkew, "", "m3/m2^1.5 (总体矩), n<3 或 m2=0 → NaN")                \
  X(CumKurt, "", "m4/m2² − 3 (总体矩), n<4 或 m2=0 → NaN")               \
  X(CumMax, "", "max_{s≤t} x_s")                                         \
  X(CumMin, "", "min_{s≤t} x_s")                                         \
  X(CumArgMax, "", "首个最大值的分钟位 s")                               \
  X(CumArgMin, "", "首个最小值的分钟位 s")                               \
  X(CumRank, "", "* x_t 在 {x_s, s≤t} 的 pct rank")                      \
  X(CumHhi, "", "Σx²/(Σx)², Σx=0 → NaN")                                 \
  X(CumEntropy, "", "−Σ p log p, p = x/Σx (x>0 计入), Σx≤0 → NaN")       \
  X(CumTopK, "k", "* 前 k 大之和 / Σx, n<k 或 Σx=0 → NaN")               \
  X(CumPeaks, "k", "局部峰数: x_{s−1}<x_s>x_{s+1} 且 x_s > k·CumMean_s") \
  X(CumCountGt, "k", "Σ 1[x_s > k]")                                     \
  X(TodMask, "k,k2", "1[k ≤ t < k2] (与 x 无关, 只用 t)")

// ---- CUM 二元 ----
#define OP_CUM2(X)                                                \
  X(CumCov, "", "样本协方差 (ddof=1), n<2 → NaN")                 \
  X(CumCorr, "", "Pearson, n<2 或 任一方差=0 → NaN")              \
  X(CumBeta, "", "x 对 y 的 OLS 斜率 cov/var(y), var(y)=0 → NaN") \
  X(CumResid, "", "x_t − (x̄ + β(y_t − ȳ)), 当前点残差")           \
  X(CumWMean, "", "Σ y·x / Σ y (y 为权), Σy=0 → NaN")

// ---- ROLL 一元 (D 窗, 窗未满 → NaN; 窗内 NaN 跳过) ----
#define OP_ROLL1(X)                                              \
  X(TsDelay, "d", "x_{t−d}")                                     \
  X(TsDelta, "d", "x_t − x_{t−d}")                               \
  X(TsSum, "d", "窗和")                                          \
  X(TsMean, "d", "窗均")                                         \
  X(TsStd, "d", "窗样本标准差 (ddof=1), n<2 → NaN")              \
  X(TsVar, "d", "窗样本方差 (ddof=1), n<2 → NaN")                \
  X(TsSkew, "d", "m3/m2^1.5 (总体矩), n<3 或 m2=0 → NaN")        \
  X(TsKurt, "d", "m4/m2² − 3 (总体矩), n<4 或 m2=0 → NaN")       \
  X(TsMax, "d", "窗最大")                                        \
  X(TsMin, "d", "窗最小")                                        \
  X(TsArgMax, "d", "首个 (最旧) 最大值距今期数 0..d−1")          \
  X(TsArgMin, "d", "首个 (最旧) 最小值距今期数 0..d−1")          \
  X(TsMed, "d", "窗中位数 (偶数取两中位均值)")                   \
  X(TsMad, "d", "median(|x − med|)")                             \
  X(TsRank, "d", "x_t 在窗内 pct rank")                          \
  X(TsZ, "d", "(x_t − μ)/σ (ddof=1), σ=0 → NaN")                 \
  X(TsWma, "d", "线性加权 Σ w_i x_i / Σ w_i, w 最旧=1 … 最新=d") \
  X(TsEma, "k", "y = k·x + (1−k)·y, 首个有效值起, NaN 保持")     \
  X(TsProduct, "d", "Π(1 + x_i) − 1")                            \
  X(TsSlope, "d", "x 对期序 i=0..d−1 的 OLS 斜率")               \
  X(TsCountGt, "d,k", "窗内 1[x > k] 计数")

// ---- ROLL 二元 ----
#define OP_ROLL2(X)                                     \
  X(TsCov, "d", "窗样本协方差 (ddof=1), n<2 → NaN")     \
  X(TsCorr, "d", "窗 Pearson, n<2 或 任一方差=0 → NaN") \
  X(TsBeta, "d", "x 对 y 的 OLS 斜率, var(y)=0 → NaN")  \
  X(TsResid, "d", "x_t − (x̄ + β(y_t − ȳ)), 当前点残差") \
  X(TsWMean, "d", "Σ y·x / Σ y (y 为权), Σy=0 → NaN")

// ---- CS 一元 (前六个 = Method/CS.hpp 已有方法, 含其 均值填充/零填充 口径; 其余 NaN 保持) ----
#define OP_CS1(X)                                                                \
  X(CsRank, "", "cs::Rank: pct rank, 缺失 → 均值填充")                           \
  X(CsNormRank, "", "cs::NormRank: Φ⁻¹((rank+1)/(N+1)), 缺失 → 0")               \
  X(CsWinsorRank, "", "cs::WinsorRank: winsor_mad(3) → z → pct rank → 均值填充") \
  X(CsDemean, "", "cs::Demean: x − mean, 缺失 → 0")                              \
  X(CsZ, "", "cs::Z: (x − mean)/sd (总体), 缺失 → 0")                            \
  X(CsWinsorZ, "", "cs::WinsorZ: winsor_mad(3) → z, 缺失 → 0")                   \
  X(CsMean, "", "截面均值广播")                                                  \
  X(CsMedian, "", "截面中位数广播")                                              \
  X(CsStd, "", "截面样本标准差广播 (ddof=1), n<2 → NaN")                         \
  X(CsBucket, "k", "floor(pct_rank·k) ∈ 0..k−1")

// ---- CS 二元 ----
#define OP_CS2(X)                                                 \
  X(CsResid, "", "x 对 y 截面 OLS (含截距) 残差, 任一 NaN → NaN") \
  X(CsBeta, "", "x 对 y 截面 OLS 斜率广播, var(y)=0 → NaN")       \
  X(CsCorr, "", "截面 Pearson 广播")                              \
  X(CsRankDiff, "", "pct_rank(x) − pct_rank(y)")                  \
  X(CsGroupMean, "", "按 y 分组 (整数 id) 的组均值广播")          \
  X(CsGroupRank, "", "按 y 分组的组内 pct rank")                  \
  X(CsCondRank, "k", "y 分 k 桶, x 在桶内 pct rank")

// ---- CS 三元 ----
#define OP_CS3(X) \
  X(CsGroupResid, "", "按 z 分组: x, y 组内 demean 后 x 对 y 标量回归残差 (FWL, = NeutralRank 核)")

// ---- 全表 ----
#define OP_ALL(X) OP_ELEM1(X) OP_ELEM2(X) OP_ELEM3(X) OP_CUM1(X) OP_CUM2(X) OP_ROLL1(X) OP_ROLL2(X) OP_CS1(X) OP_CS2(X) OP_CS3(X)
