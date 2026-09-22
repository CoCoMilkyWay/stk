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
//   TS 行  X(Name, 元数, 窗, "参数", GPU策略, R"tex(公式)tex", "备注")
//     元数     输入序列数 0..3 (窗长 d / 阈值 k 不算元)
//     窗       POINT  逐点 (无状态, TS 的退化)
//              EXPAND 段内 expanding (段 = 交易日 kSegLen 分钟, 段界 reset)
//              ROLL   最近 d 期滚动 (期 = 分钟, 跨段不 reset)
//              EXPO   指数递推 (系数 k, 全程不 reset)
//     GPU策略  POINT 逐格融合 / GATHER 移位取值 / SCAN 分段前缀和差分 /
//              EXTREME van Herk 两趟块内前后缀 (滑窗极值) / HIST 桶直方图 (序统计近似) / RECUR 仿射 scan
//
//   CS 行  X(Name, 元数, "参数", GPU策略, R"tex(公式)tex", "备注")
//     GPU策略  REDUCE 沿资产归约 / HIST 片上桶直方图 / GROUP 分组归约
//
//   参数列 = 本算子读取的 Param 字段 (Contract.hpp struct Param), 逗号分隔, 空 = 无参数:
//     d   窗长 / 滞后 (期 = 分钟)                         k2  第二阈值 (仅 TodMask 上界)
//     k   阈值 / 指数 / 桶数 / EMA 系数 / 分位 / topk (含义见各行公式)
//
//   【公式符号】继承 features/FeaturesDefine.hpp 的规范 (t = 分钟, D = 交易日, 1[·] 指示, ∑ 显式下标),
//   算子库专用补充 (GUI Factors→Operators 页按 LaTeX 渲染):
//     x, y, z     输入序列, 按元数取前 1..3 个; 每格 Val{v, m}, 所有 ∑ / 计数 / 极值只计有效 (m = 1) 样本
//     a, b        资产 (截面轴). TS 只看本资产, 省略 a; CS 只看同一 t 的截面, 写 x_a 省略 t
//     t_D         段内分钟位置 = t − 当日首分钟 (0..kSegLen−1)
//     W_t         TS 窗 (由"窗"列决定, 公式里统一写 W_t):
//                   EXPAND  W_t = {s : D(s) = D(t), s ≤ t}   ROLL  W_t = {s : t−d < s ≤ t}
//     n           W_t 内有效样本数 (二元: x, y 同格同时有效); N  截面有效资产数
//     μ_t, σ_t    W_t 内均值 / 样本标准差 (ddof=1); 二元加上标 μ^x, μ^y; 截面去掉下标 t
//     m_k         W_t 内 k 阶中心总体矩 (1/n)∑(x_s − μ_t)^k
//     pct(v; S)   v 在样本集 S 的并列均秩 pct rank ∈ [0, 1] (Contract pct_of); 省略 S = 全截面
//     Q_p         截面 p 分位;  Φ⁻¹  标准正态分位;  G(a)  a 所在组 (整数 id 由 y 或 z 给)
//     序统计族 (rank / median / mad / quantile / topk / gini) 三后端同用 kBuckets 桶近似, 桶计数整数, 对拍逐位
//
//   加算子 = 本表一行 + TS(或 CS) 的 Stream / Naive / Gpu 各一个同名 struct; 缺任一侧 → op_check 编译错;
//   流式 struct::kWin / GPU struct::kStrat 与本表不符 → op_check static_assert 错.
//   TsYoY 不入表: = TsDelayRoll + TsDiv 的组合, 且 d 需一年分钟数, 超出块 carry 上限.
//   "退化" 一词的含义见 Contract.hpp: 全并列 (精确) / 相消 (相对 kRelEps) / y = 0 (逐点).
// =============================================================================

// ---- TS: 逐点 (22) ----
#define OP_TS_POINT(X)                                                                                                         \
  X(TsAbs, 1, POINT, "", POINT, R"tex(|x_t|)tex", "")                                                                          \
  X(TsSign, 1, POINT, "", POINT, R"tex(\operatorname{sign}(x_t))tex", "")                                                      \
  X(TsLog, 1, POINT, "", POINT, R"tex(\operatorname{sign}(x_t)\,\ln(1+|x_t|))tex", "")                                         \
  X(TsAsinh, 1, POINT, "", POINT, R"tex(\operatorname{asinh}(x_t))tex", "")                                                    \
  X(TsTanh, 1, POINT, "", POINT, R"tex(\tanh(x_t))tex", "")                                                                    \
  X(TsSqrt, 1, POINT, "", POINT, R"tex(\operatorname{sign}(x_t)\sqrt{|x_t|})tex", "")                                          \
  X(TsRelu, 1, POINT, "", POINT, R"tex(\max(0,\,x_t))tex", "")                                                                 \
  X(TsRecip, 1, POINT, "", POINT, R"tex(1/x_t)tex", "x = 0 退化")                                                              \
  X(TsSignedPow, 1, POINT, "k", POINT, R"tex(\operatorname{sign}(x_t)\,|x_t|^{k})tex", "k = 指数")                             \
  X(TsClip, 1, POINT, "k", POINT, R"tex(\operatorname{clamp}(x_t,\,-k,\,k))tex", "k ≥ 0 对称截断")                             \
  X(TsTodMask, 0, POINT, "k,k2", POINT, R"tex(\mathbf{1}[k \le t_D < k_2])tex", "无序列输入, 只看段内位置 t_D; k/k2 = 分钟界") \
  X(TsAdd, 2, POINT, "", POINT, R"tex(x_t + y_t)tex", "")                                                                      \
  X(TsSub, 2, POINT, "", POINT, R"tex(x_t - y_t)tex", "")                                                                      \
  X(TsMul, 2, POINT, "", POINT, R"tex(x_t \cdot y_t)tex", "")                                                                  \
  X(TsDiv, 2, POINT, "", POINT, R"tex(x_t / y_t)tex", "y = 0 退化")                                                            \
  X(TsMax, 2, POINT, "", POINT, R"tex(\max(x_t,\,y_t))tex", "")                                                                \
  X(TsMin, 2, POINT, "", POINT, R"tex(\min(x_t,\,y_t))tex", "")                                                                \
  X(TsImb, 2, POINT, "", POINT, R"tex(\frac{x_t - y_t}{x_t + y_t})tex", "x + y 相消退化")                                      \
  X(TsShare, 2, POINT, "", POINT, R"tex(\frac{x_t}{x_t + y_t})tex", "x + y 相消退化")                                          \
  X(TsLogRatio, 2, POINT, "", POINT, R"tex(\ln x_t - \ln y_t)tex", "任一 ≤ 0 退化")                                            \
  X(TsWhere, 3, POINT, "", POINT, R"tex(\mathbf{1}[x_t > 0]\,y_t + \mathbf{1}[x_t \le 0]\,z_t)tex", "")                        \
  X(TsClip3, 3, POINT, "", POINT, R"tex(\operatorname{clamp}(x_t,\,y_t,\,z_t))tex", "y > z 退化")

// ---- TS: 有窗 (50 = Cum 23 + Roll 26 + Ema 1); 按统计核排, 同核的不同窗相邻 ----
#define OP_TS_WIN(X)                                                                                                                                                          \
  X(TsDelayRoll, 1, ROLL, "d", GATHER, R"tex(x_{t-d})tex", "看 d 期前那一格, 实际跨 d+1 格: t < d 无效")                                                                      \
  X(TsDeltaRoll, 1, ROLL, "d", GATHER, R"tex(x_t - x_{t-d})tex", "同 DelayRoll: t < d 无效")                                                                                  \
  X(TsSumCum, 1, EXPAND, "", SCAN, R"tex(\sum_{s \in W_t} x_s)tex", "")                                                                                                       \
  X(TsSumRoll, 1, ROLL, "d", SCAN, R"tex(\sum_{s \in W_t} x_s)tex", "")                                                                                                       \
  X(TsMeanCum, 1, EXPAND, "", SCAN, R"tex(\mu_t = \frac{1}{n}\sum_{s \in W_t} x_s)tex", "")                                                                                   \
  X(TsMeanRoll, 1, ROLL, "d", SCAN, R"tex(\mu_t = \frac{1}{n}\sum_{s \in W_t} x_s)tex", "")                                                                                   \
  X(TsMeanEma, 1, EXPO, "k", RECUR, R"tex(e_t = k\,x_t + (1-k)\,e_{t-1})tex", "0 < k ≤ 1; 首个有效值起, x 无效则保持 e_{t-1}; 全程不 reset")                                  \
  X(TsVarCum, 1, EXPAND, "", SCAN, R"tex(\sigma_t^2 = \frac{1}{n-1}\sum_{s \in W_t}(x_s - \mu_t)^2)tex", "n < 2 或全并列退化")                                                \
  X(TsVarRoll, 1, ROLL, "d", SCAN, R"tex(\sigma_t^2 = \frac{1}{n-1}\sum_{s \in W_t}(x_s - \mu_t)^2)tex", "n < 2 或全并列退化")                                                \
  X(TsStdCum, 1, EXPAND, "", SCAN, R"tex(\sigma_t = \sqrt{\sigma_t^2})tex", "同 VarCum")                                                                                      \
  X(TsStdRoll, 1, ROLL, "d", SCAN, R"tex(\sigma_t = \sqrt{\sigma_t^2})tex", "同 VarRoll")                                                                                     \
  X(TsSkewCum, 1, EXPAND, "", SCAN, R"tex(m_3 / m_2^{3/2})tex", "总体矩; n < 3 或全并列退化")                                                                                 \
  X(TsSkewRoll, 1, ROLL, "d", SCAN, R"tex(m_3 / m_2^{3/2})tex", "总体矩; n < 3 或全并列退化")                                                                                 \
  X(TsKurtCum, 1, EXPAND, "", SCAN, R"tex(m_4 / m_2^{2} - 3)tex", "总体矩; n < 4 或全并列退化")                                                                               \
  X(TsKurtRoll, 1, ROLL, "d", SCAN, R"tex(m_4 / m_2^{2} - 3)tex", "总体矩; n < 4 或全并列退化")                                                                               \
  X(TsMaxCum, 1, EXPAND, "", SCAN, R"tex(\max_{s \in W_t} x_s)tex", "cummax")                                                                                                 \
  X(TsMaxRoll, 1, ROLL, "d", EXTREME, R"tex(\max_{s \in W_t} x_s)tex", "")                                                                                                    \
  X(TsMinCum, 1, EXPAND, "", SCAN, R"tex(\min_{s \in W_t} x_s)tex", "cummin")                                                                                                 \
  X(TsMinRoll, 1, ROLL, "d", EXTREME, R"tex(\min_{s \in W_t} x_s)tex", "")                                                                                                    \
  X(TsArgMaxCum, 1, EXPAND, "", SCAN, R"tex(t - \min\{s \in W_t : x_s = \max_{W_t} x\})tex", "首个 (最旧) 最大值距今期数 0..n−1")                                             \
  X(TsArgMaxRoll, 1, ROLL, "d", EXTREME, R"tex(t - \min\{s \in W_t : x_s = \max_{W_t} x\})tex", "首个 (最旧) 最大值距今期数 0..d−1")                                          \
  X(TsArgMinCum, 1, EXPAND, "", SCAN, R"tex(t - \min\{s \in W_t : x_s = \min_{W_t} x\})tex", "首个 (最旧) 最小值距今期数")                                                    \
  X(TsArgMinRoll, 1, ROLL, "d", EXTREME, R"tex(t - \min\{s \in W_t : x_s = \min_{W_t} x\})tex", "首个 (最旧) 最小值距今期数")                                                 \
  X(TsRankCum, 1, EXPAND, "", HIST, R"tex(\mathrm{pct}(x_t;\,\{x_s\}_{s \in W_t}))tex", "桶近似")                                                                             \
  X(TsRankRoll, 1, ROLL, "d", HIST, R"tex(\mathrm{pct}(x_t;\,\{x_s\}_{s \in W_t}))tex", "桶近似")                                                                             \
  X(TsMedianRoll, 1, ROLL, "d", HIST, R"tex(\operatorname{median}_{s \in W_t} x_s)tex", "桶近似, 偶数不平均")                                                                 \
  X(TsMadRoll, 1, ROLL, "d", HIST, R"tex(\operatorname{median}_{s \in W_t}\,|x_s - \operatorname{median}_{W_t} x|)tex", "桶近似")                                             \
  X(TsZRoll, 1, ROLL, "d", SCAN, R"tex(\frac{x_t - \mu_t}{\sigma_t})tex", "ddof=1; n < 2 或全并列退化")                                                                       \
  X(TsWmaRoll, 1, ROLL, "d", SCAN, R"tex(\frac{\sum_{i=1}^{d} i\,x_{t-d+i}}{\sum_{i=1}^{d} i})tex", "线性权: 最旧 = 1 … 最新 = d (只计有效格)")                               \
  X(TsProductRoll, 1, ROLL, "d", SCAN, R"tex(\prod_{s \in W_t}(1 + x_s) - 1)tex", "任一 1 + x ≤ 0 退化")                                                                      \
  X(TsSlopeRoll, 1, ROLL, "d", SCAN, R"tex(\frac{\sum_{i}(i - \bar i)(x_{s_i} - \mu_t)}{\sum_{i}(i - \bar i)^2},\; i = 0..d-1)tex", "x 对窗内期序 i 的 OLS 斜率; n < 2 退化") \
  X(TsCountGtCum, 1, EXPAND, "k", SCAN, R"tex(\sum_{s \in W_t}\mathbf{1}[x_s > k])tex", "k = 阈值")                                                                           \
  X(TsCountGtRoll, 1, ROLL, "d,k", SCAN, R"tex(\sum_{s \in W_t}\mathbf{1}[x_s > k])tex", "k = 阈值; 精确 (k 不免费)")                                                         \
  X(TsAgeRoll, 1, ROLL, "d", SCAN, R"tex(t - \max\{s \in W_t : x_s \ne x_{s-1}\})tex", "距上次 x 变动 (精确不等) 的期数; 窗内无变动则 d")                                     \
  X(TsHhiCum, 1, EXPAND, "", SCAN, R"tex(\frac{\sum_{s \in W_t} x_s^2}{(\sum_{s \in W_t} x_s)^2})tex", "Σx 相消退化")                                                         \
  X(TsEntropyCum, 1, EXPAND, "", SCAN, R"tex(\ln\!\sum_{s \in W_t} x_s - \frac{\sum_{s \in W_t} x_s \ln x_s}{\sum_{s \in W_t} x_s})tex", "只计 x > 0; 无正样本退化")          \
  X(TsTopKCum, 1, EXPAND, "k", HIST, R"tex(\frac{\sum_{\text{top-}k} x_s}{\sum_{s \in W_t} x_s})tex", "k = 前 k 大; n < k 或 Σx 相消退化; 桶近似")                            \
  X(TsGiniCum, 1, EXPAND, "", HIST, R"tex(\operatorname{Gini}_{s \in W_t}(x_s))tex", "Lorenz 基尼; 只计 x > 0, 正样本 < 2 退化; 桶近似")                                      \
  X(TsPeaksCum, 1, EXPAND, "k", SCAN, R"tex(\sum_{s \in W_t}\mathbf{1}[x_{s-1} < x_s > x_{s+1} \,\land\, x_s > k\,\mu_s])tex", "局部峰计数; k = 相对均值倍数")                \
  X(TsCovCum, 2, EXPAND, "", SCAN, R"tex(\frac{1}{n-1}\sum_{s \in W_t}(x_s - \mu^x_t)(y_s - \mu^y_t))tex", "n < 2 退化")                                                      \
  X(TsCovRoll, 2, ROLL, "d", SCAN, R"tex(\frac{1}{n-1}\sum_{s \in W_t}(x_s - \mu^x_t)(y_s - \mu^y_t))tex", "n < 2 退化")                                                      \
  X(TsCorrCum, 2, EXPAND, "", SCAN, R"tex(\frac{\operatorname{cov}_{W_t}(x, y)}{\sigma^x_t\,\sigma^y_t})tex", "Pearson; x 或 y 全并列退化")                                   \
  X(TsCorrRoll, 2, ROLL, "d", SCAN, R"tex(\frac{\operatorname{cov}_{W_t}(x, y)}{\sigma^x_t\,\sigma^y_t})tex", "Pearson; x 或 y 全并列退化")                                   \
  X(TsCorrLagCum, 2, EXPAND, "k", SCAN, R"tex(\operatorname{corr}_{s \in W_t}(x_s,\, y_{s-k}))tex", "k = y 的滞后期")                                                         \
  X(TsBetaCum, 2, EXPAND, "", SCAN, R"tex(\beta_t = \frac{\operatorname{cov}_{W_t}(x, y)}{\operatorname{var}_{W_t}(y)})tex", "x 对 y 的 OLS 斜率; y 全并列退化")              \
  X(TsBetaRoll, 2, ROLL, "d", SCAN, R"tex(\beta_t = \frac{\operatorname{cov}_{W_t}(x, y)}{\operatorname{var}_{W_t}(y)})tex", "x 对 y 的 OLS 斜率; y 全并列退化")              \
  X(TsResidCum, 2, EXPAND, "", SCAN, R"tex((x_t - \mu^x_t) - \beta_t\,(y_t - \mu^y_t))tex", "当前格残差; 同 BetaCum 退化")                                                    \
  X(TsResidRoll, 2, ROLL, "d", SCAN, R"tex((x_t - \mu^x_t) - \beta_t\,(y_t - \mu^y_t))tex", "当前格残差; 同 BetaRoll 退化")                                                   \
  X(TsWMeanCum, 2, EXPAND, "", SCAN, R"tex(\frac{\sum_{s \in W_t} y_s\,x_s}{\sum_{s \in W_t} y_s})tex", "y 为权; Σy 相消退化")                                                \
  X(TsWMeanRoll, 2, ROLL, "d", SCAN, R"tex(\frac{\sum_{s \in W_t} y_s\,x_s}{\sum_{s \in W_t} y_s})tex", "y 为权; Σy 相消退化")

// ---- CS (20): 截面无窗, 故只有轴_核 ----
#define OP_CS(X)                                                                                                                                                                                \
  X(CsMean, 1, "", REDUCE, R"tex(\mu = \frac{1}{N}\sum_{a} x_a)tex", "广播到每个资产")                                                                                                          \
  X(CsStd, 1, "", REDUCE, R"tex(\sigma = \sqrt{\frac{1}{N-1}\sum_{a}(x_a - \mu)^2})tex", "ddof=1, 广播; 全并列退化")                                                                            \
  X(CsDemean, 1, "", REDUCE, R"tex(x_a - \mu)tex", "")                                                                                                                                          \
  X(CsZ, 1, "", REDUCE, R"tex(\frac{x_a - \mu}{\sigma})tex", "ddof=1; 全并列退化")                                                                                                              \
  X(CsRank, 1, "", HIST, R"tex(\mathrm{pct}(x_a))tex", "并列均秩, ∈ [0, 1]")                                                                                                                    \
  X(CsNormRank, 1, "", HIST, R"tex(\Phi^{-1}\!\left(\operatorname{clamp}(\mathrm{pct}(x_a),\,\tfrac{1}{N+1},\,\tfrac{N}{N+1})\right))tex", "GPU 用 normcdfinvf, 对拍容差放宽")                  \
  X(CsMedian, 1, "", HIST, R"tex(\operatorname{median}_{a} x_a)tex", "广播; 桶近似")                                                                                                            \
  X(CsQuantile, 1, "k", HIST, R"tex(Q_k(x))tex", "k = 分位 ∈ (0, 1); 广播; 桶近似")                                                                                                             \
  X(CsWinsor, 1, "k", HIST, R"tex(\operatorname{clamp}(x_a,\,Q_k,\,Q_{1-k}))tex", "k = 缩尾分位; 桶近似")                                                                                       \
  X(CsWinsorRank, 1, "", HIST, R"tex(\mathrm{pct}\big(\operatorname{clamp}(x_a,\,Q_{0.01},\,Q_{0.99})\big))tex", "缩尾分位固定 0.01")                                                           \
  X(CsWinsorZ, 1, "", HIST, R"tex(z\big(\operatorname{clamp}(x_a,\,Q_{0.01},\,Q_{0.99})\big))tex", "缩尾分位固定 0.01; 缩尾后全并列退化")                                                       \
  X(CsBucket, 1, "k", HIST, R"tex(\lfloor k \cdot \mathrm{pct}(x_a) \rfloor)tex", "k = 桶数, 输出 ∈ 0..k−1")                                                                                    \
  X(CsResid, 2, "", REDUCE, R"tex(x_a - \hat\alpha - \hat\beta\,y_a)tex", "x 对 y 截面 OLS (含截距) 残差; y 全并列退化")                                                                        \
  X(CsBeta, 2, "", REDUCE, R"tex(\hat\beta = \frac{\operatorname{cov}_a(x, y)}{\operatorname{var}_a(y)})tex", "广播; y 全并列退化")                                                             \
  X(CsCorr, 2, "", REDUCE, R"tex(\operatorname{corr}_a(x, y))tex", "Pearson 广播; x 或 y 全并列退化")                                                                                           \
  X(CsRankDiff, 2, "", HIST, R"tex(\mathrm{pct}(x_a) - \mathrm{pct}(y_a))tex", "")                                                                                                              \
  X(CsGroupMean, 2, "", GROUP, R"tex(\frac{1}{|G(a)|}\sum_{b \in G(a)} x_b,\; G(a) = \{b : y_b = y_a\})tex", "y = 整数组 id; 组均值广播")                                                       \
  X(CsGroupRank, 2, "", GROUP, R"tex(\mathrm{pct}(x_a;\,\{x_b\}_{b \in G(a)}),\; G(a) = \{b : y_b = y_a\})tex", "y = 整数组 id; 组内 pct rank")                                                 \
  X(CsCondRank, 2, "k", GROUP, R"tex(\mathrm{pct}(x_a;\,B(a)),\; B(a) = \{b : \lfloor k\,\mathrm{pct}(y_b)\rfloor = \lfloor k\,\mathrm{pct}(y_a)\rfloor\})tex", "y 分 k 桶, x 在桶内 pct rank") \
  X(CsGroupResid, 3, "", GROUP, R"tex(\tilde x_a - \hat\beta\,\tilde y_a,\; \tilde x_a = x_a - \bar x_{G(a)},\; G(a) = \{b : z_b = z_a\})tex", "FWL: 按 z 分组, x/y 组内 demean 后 x 对 y 回归残差; y 组内全并列退化")

// ---- 全表 ----
#define OP_TS(X) OP_TS_POINT(X) OP_TS_WIN(X)
#define OP_ALL_TS_CS(TS_X, CS_X) OP_TS(TS_X) OP_CS(CS_X)
