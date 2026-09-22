#pragma once

// =============================================================================
// 因子算子真相表 (X-macro; 语义契约见 factor/Contract.hpp; 成本与误差见 operator.md)
// =============================================================================
//   命名 = 域_核_窗 (代码里 PascalCase, 文档里 snake_case, 一一对应):
//     域   Ts (A 域 = SELF) / Cs (A 域 = ALL | GROUP) —— 前缀只是 A 域的缩写, 不是独立分类
//     核   统计量本身 (Mean / Var / Rank / Corr …), 与窗正交
//     窗   Cum 段内 expanding / Roll 最近 d 期 / Ema 指数递推; 无窗后缀 = 逐点
//
//   【行序 = 全局 idx】全库唯一算子序, code 与 UI 统一按它 (operators.json 的 idx /
//   op_check 输出序 / GUI Operators 表默认序), 序键 (枚举序见 Contract.hpp):
//     元数 → A 域 (Ts 前 Cs 后) → TS 按 T 窗 / CS 按 ALL → GROUP → 核类 → 名字字母序
//   表按 (元数 × 域) 切成 OP_TS0..3 / OP_CS1..3 七块, 块内按上述序键排;
//   OP_ALL 交错拼出 idx 序, OP_TS / OP_CS 各拼本域 (供 GpuRun 的 run_ts / run_cs 按域分派).
//   加算子按序键插行, idx 顺延, 不另设编号.
//
//   行格式统一, 三个分类列 = 三个正交维度 (语义与类型约束见 Contract.hpp【分类】):
//
//   X(Name, 元数, T窗, A域, 核类, "参数", R"tex(公式)tex", "备注")
//     元数   输入序列数 0..3 (窗长 d / 阈值 k 不算元)
//     T窗    POINT  当前点 (无时间状态)
//            EXPAND 段内 expanding (段 = 交易日 kSegLen 分钟, 段界 reset)
//            ROLL   最近 d 期滚动 (期 = 分钟, 跨段不 reset)
//            EXPO   指数加权全历史 (系数 k, 全程不 reset)
//     A域    SELF 只看本资产 / ALL 同一时刻全截面 / GROUP 同一时刻组内 (整数组 id 由 y 或 z 给)
//     核类   MAP 逐元素 / SHIFT 下标平移 / MOMENT 可和分解 (矩族) / EXTREME 极值及 arg 族 /
//            ORDER 序统计 / RECUR 递推; 复合算子标主导 (最重) 一级
//
//   参数列 = 本算子读取的 Param 字段 (Contract.hpp struct Param), 逗号分隔, 空 = 无参数:
//     d   窗长 / 滞后 (期 = 分钟)                         k2  第二阈值 (仅 TodMask 上界)
//     k   阈值 / 桶数 / EMA 系数 / 分位 (含义见各行公式)
//
//   【公式写法】一律单行 (GUI 表格内联渲染, 行高须齐): 不用 \frac (除法写 A / B 加括号)、不用大号
//   \sum \prod \sqrt \lfloor (求和写 \Sigma_下标, 乘积 \Pi_下标, 开方写 ^{1/2}, 取整写 \mathrm{floor}),
//   归约算子一律 \mathrm{名}_下标 (\mathrm 不是 mathop, 下标必落右下, 不会顶到符号正下方把行撑高).
//
//   【公式符号】继承 features/FeaturesDefine.hpp 的规范 (t = 分钟, D = 交易日, 1[·] 指示),
//   算子库专用补充 (GUI Factors→Operators 页按 LaTeX 渲染):
//     x, y, z     输入序列, 按元数取前 1..3 个; 每格 Val{v, m}, 所有 ∑ / 计数 / 极值只计有效 (m = 1) 样本
//     a, b        资产 (截面轴). TS 只看本资产, 省略 a; CS 只看同一 t 的截面, 写 x_a 省略 t
//     t_D         段内分钟位置 = t − 当日首分钟 (0..kSegLen−1)
//     W_t         TS 窗 (由"窗"列决定, 公式里统一写 W_t):
//                   EXPAND  W_t = {s : D(s) = D(t), s ≤ t}   ROLL  W_t = {s : t−d < s ≤ t}
//     Σ_{W_t}     窗内求和 ∑_{s∈W_t} (下标只写窗 / 组, 求和变量恒是 s 或 b); Π_{W_t} 同理为乘积
//     n           W_t 内有效样本数 (二元: x, y 同格同时有效); N  截面有效资产数
//     μ_t, σ_t    W_t 内均值 / 样本标准差 (ddof=1); 二元加上标 μ^x, μ^y; 截面去掉下标 t
//     m_k         W_t 内 k 阶中心总体矩 (1/n)·Σ_{W_t}(x_s − μ_t)^k
//     max, min, cov, var, corr, pct, floor, clamp  带下标者下标 = 取值域 (窗 W_t / 截面 a / 组 G(a))
//     pct(v; S)   v 在样本集 S 的并列均秩 pct rank ∈ [0, 1] (Contract pct_of); 省略 S = 全截面
//     Q_p(S)      样本集 S 的 p 分位 (省略 S = 全截面);  Φ⁻¹  标准正态分位;  G(a)  a 所在组 (整数 id 由 y 或 z 给)
//     序统计族 (rank / quantile / winsor / bucket) 三后端同用 kBuckets 桶近似, 桶计数整数, 对拍逐位
//
//   【核 × 窗 为什么不是满格】格子空不空按"这个统计量在这个时间尺度上有没有意义"定, 不为对称而补:
//     Cum (段内 = 当日) 专收**日内分布形状**: Hhi / Entropy 只有 Cum —— 度量"当日成交怎么摊在各分钟上",
//       跨日滚动窗算集中度没有对应的研报语义.
//     Roll (最近 d 分钟, 跨段) 专收**时序动态**: Quantile / Z / Wma / Product / Slope 只有 Roll ——
//       分位 / 标准分 / 衰减权 / 累乘收益 / 趋势斜率都要求窗长固定, 段内 expanding 的窗长随 t_D 变,
//       出来的值前半段和后半段不可比 (自带一条 t_D 的伪趋势).
//     Delay / Delta 是 SHIFT 核, 只看一格, 天生只有 Roll; MeanEma 是 RECUR 核, 与 EXPO 窗一一绑定.
//     其余归约核 (Sum / Mean / Var / Std / Skew / Kurt / Max / Min / ArgMax / ArgMin / Rank /
//     Cov / Corr / Beta / Resid / WMean) Cum 与 Roll **成对齐全**, 缺一即为 bug.
//
//   【不入表的组合】能由现有行拼出来的一律不给独立算子 (表只收不可约的核):
//     计数   Σ1[x > k]     = TsGt + TsSum{Cum,Roll};  占比同理换 TsMean{Cum,Roll}
//     中位   med           = TsQuantileRoll(k=0.5) / CsQuantile(k=0.5)
//     子集统计  只在某时段/某条件上统计 = TsMask(x, 掩码) 再套窗 (掩码由 TsTodMask / TsGt 造)
//     超前相关  corr(x_s, y_{s+k}) = TsCorr{Cum,Roll}(TsDelayRoll(x, k), y) —— 换边即换符号方向
//     TsYoY    = TsDelayRoll + TsDiv, 且 d 需一年分钟数, 超出块 carry 上限
//
//   加算子 = 本表一行 + TS(或 CS) 的 Stream / Cpu / Gpu 各一个同名 struct; 缺任一侧 → op_check 编译错;
//   流式 TS struct::kWin 与本表 T 窗列不符 → op_check static_assert 错 (A 域/核类是纯语义列, 无实现侧载荷).
//   "退化" 一词的含义见 Contract.hpp: 全并列 (精确) / 相消 (相对 kRelEps) / y = 0 (逐点).
// =============================================================================

// ---- 0 元 × SELF (1) ----
#define OP_TS0(X) \
  X(TsTodMask, 0, POINT, SELF, MAP, "k,k2", R"tex(\mathbf{1}[k \le t_D < k_2])tex", "无序列输入, 只看段内位置 t_D; k/k2 = 分钟界")

// ---- 1 元 × SELF (40 = POINT 8 + EXPAND 13 + ROLL 18 + EXPO 1) ----
#define OP_TS1(X)                                                                                                                                                                     \
  X(TsAbs, 1, POINT, SELF, MAP, "", R"tex(|x_t|)tex", "")                                                                                                                             \
  X(TsClip, 1, POINT, SELF, MAP, "k", R"tex(\operatorname{clamp}(x_t,\,-k,\,k))tex", "k ≥ 0 对称截断")                                                                                \
  X(TsGt, 1, POINT, SELF, MAP, "k", R"tex(\mathbf{1}[x_t > k])tex", "k = 阈值 (严格大于); 套 Sum 窗 = 计数, 套 Mean 窗 = 占比")                                                       \
  X(TsLog, 1, POINT, SELF, MAP, "", R"tex(\operatorname{sign}(x_t)\,\ln(1+|x_t|))tex", "")                                                                                            \
  X(TsRecip, 1, POINT, SELF, MAP, "", R"tex(1/x_t)tex", "x = 0 退化")                                                                                                                 \
  X(TsRelu, 1, POINT, SELF, MAP, "", R"tex(\max(0,\,x_t))tex", "")                                                                                                                    \
  X(TsSign, 1, POINT, SELF, MAP, "", R"tex(\operatorname{sign}(x_t))tex", "")                                                                                                         \
  X(TsSqrt, 1, POINT, SELF, MAP, "", R"tex(\operatorname{sign}(x_t)\,|x_t|^{1/2})tex", "")                                                                                            \
  X(TsEntropyCum, 1, EXPAND, SELF, MOMENT, "", R"tex(\ln(\Sigma_{W_t}\,x_s) - (\Sigma_{W_t}\,x_s \ln x_s) \;/\; (\Sigma_{W_t}\,x_s))tex", "只计 x > 0; 无正样本退化")                 \
  X(TsHhiCum, 1, EXPAND, SELF, MOMENT, "", R"tex(\Sigma_{W_t}\,x_s^2 \;/\; (\Sigma_{W_t}\,x_s)^2)tex", "Σx 相消退化")                                                                 \
  X(TsKurtCum, 1, EXPAND, SELF, MOMENT, "", R"tex(m_4 / m_2^{2} - 3)tex", "总体矩; n < 4 或全并列退化")                                                                               \
  X(TsMeanCum, 1, EXPAND, SELF, MOMENT, "", R"tex(\mu_t = (1/n)\,\Sigma_{W_t}\,x_s)tex", "")                                                                                          \
  X(TsSkewCum, 1, EXPAND, SELF, MOMENT, "", R"tex(m_3 / m_2^{3/2})tex", "总体矩; n < 3 或全并列退化")                                                                                 \
  X(TsStdCum, 1, EXPAND, SELF, MOMENT, "", R"tex(\sigma_t = (\sigma_t^2)^{1/2})tex", "同 VarCum")                                                                                     \
  X(TsSumCum, 1, EXPAND, SELF, MOMENT, "", R"tex(\Sigma_{W_t}\,x_s)tex", "")                                                                                                          \
  X(TsVarCum, 1, EXPAND, SELF, MOMENT, "", R"tex(\sigma_t^2 = \Sigma_{W_t}(x_s - \mu_t)^2 / (n-1))tex", "n < 2 或全并列退化")                                                         \
  X(TsArgMaxCum, 1, EXPAND, SELF, EXTREME, "", R"tex(t - \mathrm{min}\{s \in W_t : x_s = \mathrm{max}_{W_t}\,x\})tex", "首个 (最旧) 最大值距今期数 0..n−1")                           \
  X(TsArgMinCum, 1, EXPAND, SELF, EXTREME, "", R"tex(t - \mathrm{min}\{s \in W_t : x_s = \mathrm{min}_{W_t}\,x\})tex", "首个 (最旧) 最小值距今期数")                                  \
  X(TsMaxCum, 1, EXPAND, SELF, EXTREME, "", R"tex(\mathrm{max}_{W_t}\,x_s)tex", "cummax")                                                                                             \
  X(TsMinCum, 1, EXPAND, SELF, EXTREME, "", R"tex(\mathrm{min}_{W_t}\,x_s)tex", "cummin")                                                                                             \
  X(TsRankCum, 1, EXPAND, SELF, ORDER, "", R"tex(\mathrm{pct}(x_t;\,\{x_s : s \in W_t\}))tex", "桶近似")                                                                              \
  X(TsDelayRoll, 1, ROLL, SELF, SHIFT, "d", R"tex(x_{t-d})tex", "看 d 期前那一格, 实际跨 d+1 格: t < d 无效")                                                                         \
  X(TsDeltaRoll, 1, ROLL, SELF, SHIFT, "d", R"tex(x_t - x_{t-d})tex", "同 DelayRoll: t < d 无效")                                                                                     \
  X(TsKurtRoll, 1, ROLL, SELF, MOMENT, "d", R"tex(m_4 / m_2^{2} - 3)tex", "总体矩; n < 4 或全并列退化")                                                                               \
  X(TsMeanRoll, 1, ROLL, SELF, MOMENT, "d", R"tex(\mu_t = (1/n)\,\Sigma_{W_t}\,x_s)tex", "")                                                                                          \
  X(TsProductRoll, 1, ROLL, SELF, MOMENT, "d", R"tex(\Pi_{W_t}(1 + x_s) - 1)tex", "任一 1 + x ≤ 0 退化")                                                                              \
  X(TsSkewRoll, 1, ROLL, SELF, MOMENT, "d", R"tex(m_3 / m_2^{3/2})tex", "总体矩; n < 3 或全并列退化")                                                                                 \
  X(TsSlopeRoll, 1, ROLL, SELF, MOMENT, "d", R"tex(\Sigma_i (i - \bar i)(x_{s_i} - \mu_t) \;/\; \Sigma_i (i - \bar i)^2,\; i = 0..d-1)tex", "x 对窗内期序 i 的 OLS 斜率; n < 2 退化") \
  X(TsStdRoll, 1, ROLL, SELF, MOMENT, "d", R"tex(\sigma_t = (\sigma_t^2)^{1/2})tex", "同 VarRoll")                                                                                    \
  X(TsSumRoll, 1, ROLL, SELF, MOMENT, "d", R"tex(\Sigma_{W_t}\,x_s)tex", "")                                                                                                          \
  X(TsVarRoll, 1, ROLL, SELF, MOMENT, "d", R"tex(\sigma_t^2 = \Sigma_{W_t}(x_s - \mu_t)^2 / (n-1))tex", "n < 2 或全并列退化")                                                         \
  X(TsWmaRoll, 1, ROLL, SELF, MOMENT, "d", R"tex(\Sigma_{i=1..d}\,i\,x_{t-d+i} \;/\; \Sigma_{i=1..d}\,i)tex", "线性权: 最旧 = 1 … 最新 = d (只计有效格)")                             \
  X(TsZRoll, 1, ROLL, SELF, MOMENT, "d", R"tex((x_t - \mu_t) / \sigma_t)tex", "ddof=1; n < 2 或全并列退化")                                                                           \
  X(TsArgMaxRoll, 1, ROLL, SELF, EXTREME, "d", R"tex(t - \mathrm{min}\{s \in W_t : x_s = \mathrm{max}_{W_t}\,x\})tex", "首个 (最旧) 最大值距今期数 0..d−1")                           \
  X(TsArgMinRoll, 1, ROLL, SELF, EXTREME, "d", R"tex(t - \mathrm{min}\{s \in W_t : x_s = \mathrm{min}_{W_t}\,x\})tex", "首个 (最旧) 最小值距今期数")                                  \
  X(TsMaxRoll, 1, ROLL, SELF, EXTREME, "d", R"tex(\mathrm{max}_{W_t}\,x_s)tex", "")                                                                                                   \
  X(TsMinRoll, 1, ROLL, SELF, EXTREME, "d", R"tex(\mathrm{min}_{W_t}\,x_s)tex", "")                                                                                                   \
  X(TsQuantileRoll, 1, ROLL, SELF, ORDER, "d,k", R"tex(Q_k(\{x_s : s \in W_t\}))tex", "k = 分位 ∈ (0, 1), k = 0.5 即中位; 桶近似, 不插值")                                            \
  X(TsRankRoll, 1, ROLL, SELF, ORDER, "d", R"tex(\mathrm{pct}(x_t;\,\{x_s : s \in W_t\}))tex", "桶近似")                                                                              \
  X(TsMeanEma, 1, EXPO, SELF, RECUR, "k", R"tex(e_t = k\,x_t + (1-k)\,e_{t-1})tex", "0 < k ≤ 1; 首个有效值起, x 无效则保持 e_{t-1}; 全程不 reset")

// ---- 1 元 × 截面 (9 = ALL 9) ----
#define OP_CS1(X)                                                                                                                                                \
  X(CsDemean, 1, POINT, ALL, MOMENT, "", R"tex(x_a - \mu)tex", "")                                                                                               \
  X(CsMean, 1, POINT, ALL, MOMENT, "", R"tex(\mu = (1/N)\,\Sigma_a\,x_a)tex", "广播到每个资产")                                                                  \
  X(CsStd, 1, POINT, ALL, MOMENT, "", R"tex(\sigma = (\Sigma_a (x_a - \mu)^2 / (N-1))^{1/2})tex", "ddof=1, 广播; 全并列退化")                                    \
  X(CsZ, 1, POINT, ALL, MOMENT, "", R"tex((x_a - \mu) / \sigma)tex", "ddof=1; 全并列退化")                                                                       \
  X(CsBucket, 1, POINT, ALL, ORDER, "k", R"tex(\mathrm{floor}(k \cdot \mathrm{pct}(x_a)))tex", "k = 桶数, 输出 ∈ 0..k−1")                                        \
  X(CsNormRank, 1, POINT, ALL, ORDER, "", R"tex(\Phi^{-1}(\operatorname{clamp}(\mathrm{pct}(x_a),\,1/(N+1),\,N/(N+1))))tex", "GPU 用 normcdfinvf, 对拍容差放宽") \
  X(CsQuantile, 1, POINT, ALL, ORDER, "k", R"tex(Q_k(\{x_b\}))tex", "k = 分位 ∈ (0, 1), k = 0.5 即中位; 广播; 桶近似")                                           \
  X(CsRank, 1, POINT, ALL, ORDER, "", R"tex(\mathrm{pct}(x_a))tex", "并列均秩, ∈ [0, 1]")                                                                        \
  X(CsWinsor, 1, POINT, ALL, ORDER, "k", R"tex(\operatorname{clamp}(x_a,\,Q_k,\,Q_{1-k}))tex", "k = 缩尾分位; 桶近似")

// ---- 2 元 × SELF (20 = POINT 10 + EXPAND 5 + ROLL 5) ----
#define OP_TS2(X)                                                                                                                                          \
  X(TsAdd, 2, POINT, SELF, MAP, "", R"tex(x_t + y_t)tex", "")                                                                                              \
  X(TsDiv, 2, POINT, SELF, MAP, "", R"tex(x_t / y_t)tex", "y = 0 退化")                                                                                    \
  X(TsImb, 2, POINT, SELF, MAP, "", R"tex((x_t - y_t) / (x_t + y_t))tex", "x + y 相消退化")                                                                \
  X(TsLogRatio, 2, POINT, SELF, MAP, "", R"tex(\ln x_t - \ln y_t)tex", "任一 ≤ 0 退化")                                                                    \
  X(TsMask, 2, POINT, SELF, MAP, "", R"tex(x_t \;\mathrm{where}\; y_t > 0)tex", "y ≤ 0 退化 (不是补 0): 把下游窗口限在子集上, 配 TodMask / Gt 用")         \
  X(TsMax, 2, POINT, SELF, MAP, "", R"tex(\max(x_t,\,y_t))tex", "")                                                                                        \
  X(TsMin, 2, POINT, SELF, MAP, "", R"tex(\min(x_t,\,y_t))tex", "")                                                                                        \
  X(TsMul, 2, POINT, SELF, MAP, "", R"tex(x_t \cdot y_t)tex", "")                                                                                          \
  X(TsShare, 2, POINT, SELF, MAP, "", R"tex(x_t / (x_t + y_t))tex", "x + y 相消退化")                                                                      \
  X(TsSub, 2, POINT, SELF, MAP, "", R"tex(x_t - y_t)tex", "")                                                                                              \
  X(TsBetaCum, 2, EXPAND, SELF, MOMENT, "", R"tex(\beta_t = \mathrm{cov}_{W_t}(x, y) \;/\; \mathrm{var}_{W_t}(y))tex", "x 对 y 的 OLS 斜率; y 全并列退化") \
  X(TsCorrCum, 2, EXPAND, SELF, MOMENT, "", R"tex(\mathrm{cov}_{W_t}(x, y) \;/\; (\sigma^x_t\,\sigma^y_t))tex", "Pearson; x 或 y 全并列退化")              \
  X(TsCovCum, 2, EXPAND, SELF, MOMENT, "", R"tex(\Sigma_{W_t}(x_s - \mu^x_t)(y_s - \mu^y_t) \;/\; (n-1))tex", "n < 2 退化")                                \
  X(TsResidCum, 2, EXPAND, SELF, MOMENT, "", R"tex((x_t - \mu^x_t) - \beta_t\,(y_t - \mu^y_t))tex", "当前格残差; 同 BetaCum 退化")                         \
  X(TsWMeanCum, 2, EXPAND, SELF, MOMENT, "", R"tex(\Sigma_{W_t}\,y_s x_s \;/\; \Sigma_{W_t}\,y_s)tex", "y 为权; Σy 相消退化")                              \
  X(TsBetaRoll, 2, ROLL, SELF, MOMENT, "d", R"tex(\beta_t = \mathrm{cov}_{W_t}(x, y) \;/\; \mathrm{var}_{W_t}(y))tex", "x 对 y 的 OLS 斜率; y 全并列退化") \
  X(TsCorrRoll, 2, ROLL, SELF, MOMENT, "d", R"tex(\mathrm{cov}_{W_t}(x, y) \;/\; (\sigma^x_t\,\sigma^y_t))tex", "Pearson; x 或 y 全并列退化")              \
  X(TsCovRoll, 2, ROLL, SELF, MOMENT, "d", R"tex(\Sigma_{W_t}(x_s - \mu^x_t)(y_s - \mu^y_t) \;/\; (n-1))tex", "n < 2 退化")                                \
  X(TsResidRoll, 2, ROLL, SELF, MOMENT, "d", R"tex((x_t - \mu^x_t) - \beta_t\,(y_t - \mu^y_t))tex", "当前格残差; 同 BetaRoll 退化")                        \
  X(TsWMeanRoll, 2, ROLL, SELF, MOMENT, "d", R"tex(\Sigma_{W_t}\,y_s x_s \;/\; \Sigma_{W_t}\,y_s)tex", "y 为权; Σy 相消退化")

// ---- 2 元 × 截面 (5 = ALL 3 + GROUP 2) ----
#define OP_CS2(X)                                                                                                                                 \
  X(CsBeta, 2, POINT, ALL, MOMENT, "", R"tex(\hat\beta = \mathrm{cov}_a(x, y) \;/\; \mathrm{var}_a(y))tex", "广播; y 全并列退化")                 \
  X(CsCorr, 2, POINT, ALL, MOMENT, "", R"tex(\mathrm{corr}_a(x, y))tex", "Pearson 广播; x 或 y 全并列退化")                                       \
  X(CsResid, 2, POINT, ALL, MOMENT, "", R"tex(x_a - \hat\alpha - \hat\beta\,y_a)tex", "x 对 y 截面 OLS (含截距) 残差; y 全并列退化")              \
  X(CsGroupMean, 2, POINT, GROUP, MOMENT, "", R"tex((1/|G(a)|)\,\Sigma_{G(a)}\,x_b,\; G(a) = \{b : y_b = y_a\})tex", "y = 整数组 id; 组均值广播") \
  X(CsGroupRank, 2, POINT, GROUP, ORDER, "", R"tex(\mathrm{pct}(x_a;\,\{x_b : b \in G(a)\}),\; G(a) = \{b : y_b = y_a\})tex", "y = 整数组 id; 组内 pct rank")

// ---- 3 元 × SELF (1) ----
#define OP_TS3(X) \
  X(TsWhere, 3, POINT, SELF, MAP, "", R"tex(\mathbf{1}[x_t > 0]\,y_t + \mathbf{1}[x_t \le 0]\,z_t)tex", "")

// ---- 3 元 × 截面 (1) ----
#define OP_CS3(X) \
  X(CsGroupResid, 3, POINT, GROUP, MOMENT, "", R"tex(\tilde x_a - \hat\beta\,\tilde y_a,\; \tilde x_a = x_a - \bar x_{G(a)},\; G(a) = \{b : z_b = z_a\})tex", "FWL: 按 z 分组, x/y 组内 demean 后 x 对 y 回归残差; y 组内全并列退化")

// ---- 拼装: OP_ALL 展开序 = 全局 idx 序; OP_TS / OP_CS 按域 (GpuRun 的 run_ts / run_cs 用);
//      需要按 A 域分派时用 X_##域 token 粘贴 (见 op_check / OperatorsService) ----
#define OP_TS(X) OP_TS0(X) OP_TS1(X) OP_TS2(X) OP_TS3(X)
#define OP_CS(X) OP_CS1(X) OP_CS2(X) OP_CS3(X)
#define OP_ALL(X) OP_TS0(X) OP_TS1(X) OP_CS1(X) OP_TS2(X) OP_CS2(X) OP_TS3(X) OP_CS3(X)
